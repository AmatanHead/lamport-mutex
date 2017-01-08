import abc
import asyncio
import logging

from ..utils.serialize import Serializer
from ..utils.logging import FormatCall as _fc, LoggerAdapter


class BaseServer(metaclass=abc.ABCMeta):
    """Base interface for an RPC server

    RPC server accepts messages from connected clients and executes
    registered handlers. It guarantees that any messages received from
    a single client will be processed in the same order as they are received.
    It also guarantees that messages from a single client will be processed
    one-by-one, e.g. handler for the second message will be called only after
    running handler for the first message and sending a response back to client.

    This interface only implements basic methods for requests handling.
    All serialization and transport processes must be implemented
    in its subclasses.

    Also be aware that this class in not threadsafe.

    Usage of the server class::

        loop = asyncio.get_event_loop()

        # Create a server instance
        server = TCPServer('localhost', 8080, loop=loop)

        # Register handlers
        # (note: handlers should be coroutines or they should
        # return an awaitable object)
        @server.register
        async def add(a, b):
            return a + b

        @server.register
        async def div(a, b):
            return a // b

        # Start a server
        loop.run_until_complete(server.start())

        # Run it
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Stop it
        loop.run_until_complete(server.stop())

    """

    loop = property(lambda self: self._loop)

    def __init__(self, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop

        self._handlers = {}

        self._logger = LoggerAdapter(
            logging.getLogger('lamport_mutex/rpc/server')
        )

    @abc.abstractmethod
    async def start(self):
        """Launch the server

        Can raise any errors depending on implementation.
        """

    @abc.abstractmethod
    async def stop(self):
        """Stop the server

        This function should close all connections, stop all tasks, etc.

        Can raise any errors depending on implementation.

        NB: if there are any handlers running while calling this method,
        theirs output may be lost.
        """

    @abc.abstractproperty
    def is_running(self):
        """Indicates if the server is running

        If the server is starting or stopping,
        this property should be set to ``False``.
        """

    def register(self, handler, name=None):
        """Register a new rpc handler

        :param handler: a coroutine that will be invoked to process every rpc
            with a specific name
        :param name: name of a remote procedure
            (default is ``handler.__name__``)
        :raises NameError: there is another handler registered
            with the same name
        :raises TypeError: passed handler is not a coroutine
        """
        name = name or handler.__name__

        if not asyncio.iscoroutinefunction(handler):
            raise TypeError(
                '%r expected a coroutine, got %r' % (self, type(handler))
            )

        if name in self._handlers:
            raise NameError(
                '%r can\'t register a handler for %r: '
                'there is another handler with the same name' % (self, name)
            )

        self._logger.debug('registering handler', handler_name=name)
        self._handlers[name] = handler

        return handler

    async def _process(self, name, args, kwargs):
        """Internal: process a remote call by invoking an appropriate handler

        If handler returns successfully, this method returns the result of
        handler invocation. If handler raises, or there is no handler
        registered matching passed name, this method returns an exception
        instance so that the underlying transport layer can send this exception
        back to client.

        Generally, this method should not raise exceptions,
        it should return them instead.

        :param name: name of the handler to invoke
        :param args: arguments to pass to the handler
        :param kwargs: keyword arguments to pass to the handler
        :return: result of a handler invocation or an error
        """
        if name not in self._handlers:
            self._logger.warning(
                'got request to call an unknown rp %r',
                _fc(name, args, kwargs), name=name
            )
            return NameError('got request to call an unknown rp')
        try:
            return await self._handlers[name](*args, **kwargs)
        except BaseException as e:
            self._logger.warning(
                'got an error while processing rpc %s',
                _fc(name, args, kwargs), name=name, exc_info=e,
            )
            return e


class TCPServer(BaseServer):
    """RPC server over TCP using pickle

    This server implements TCP layer for the RPC BaseServer.

    For each client, there is a TCP connection that is kept open
    during the entire session. Client writes procedure call requests
    to its socket, one line per request. Server reads those lines
    and interprets them by calling handlers.

    NB: as there is a requirement to have one line per call request, serializer
    ensures that there are no linebreaks in the generated date.
    """

    _serializer = Serializer

    _host = None
    _port = None

    host = property(lambda self: self._host)
    port = property(lambda self: self._port)

    _STOPPED = 0
    _STOPPING = 1
    _STARTING = 2
    _RUNNING = 3

    is_running = property(lambda self: self._state == self._RUNNING)

    def __init__(self, host, port, loop=None):
        super(TCPServer, self).__init__(loop)

        self._host = host
        self._port = port

        self._server = None
        self._connections = {}

        self._client_id = 0

        self._state = self._STOPPED

        # This condition notified whenever client disconnects / its socket
        # closes. The `stop` method won't finish until
        # all clients disconnected.
        self._stop_event = asyncio.Condition()

        self._logger = LoggerAdapter(
            self._logger,
            extra=dict(host=host, port=port)
        )

    async def start(self):
        assert self._state == self._STOPPED, 'already running'

        self._logger.debug('starting')

        self._state = self._STARTING

        try:
            self._server = await asyncio.streams.start_server(
                self._client_connected, self._host, self._port, loop=self._loop
            )
        except Exception:
            self._state = self._STOPPED
            raise

        self._logger.info('started')

        self._state = self._RUNNING

    async def stop(self):
        await self._stop_event.acquire()
        try:
            assert self._state == self._RUNNING, 'not running'

            self._logger.debug('stopping')

            self._state = self._STOPPING

            self._server.close()
            await self._server.wait_closed()
            self._server = None

            for (client_reader, client_writer) in self._connections.values():
                client_writer.write_eof()
                client_writer.close()

            while self._connections:
                await self._stop_event.wait()

            self._logger.info('stopped')

            self._state = self._STOPPED
        finally:
            self._stop_event.release()

    def _client_connected(self, client_reader, client_writer):
        self._client_id += 1
        client_id = self._client_id

        self._logger.subinfo('client connected', client_id=client_id)

        self._connections[client_id] = (client_reader, client_writer)

        task = asyncio.Task(self._handle_client(client_id, client_reader, client_writer))
        task.add_done_callback(
            lambda _: asyncio.ensure_future(self._client_disconnected(client_id), loop=self._loop)
        )

    async def _client_disconnected(self, client_id):
        self._logger.subinfo('client disconnected', client_id=client_id)

        await self._stop_event.acquire()
        try:
            client_reader, client_writer = self._connections.pop(client_id)
            client_writer.close()
        finally:
            self._stop_event.notify_all()
            self._stop_event.release()

    async def _handle_client(self, client_id, client_reader, client_writer):
        while True:
            try:
                data = await client_reader.readline()
            except OSError:
                break
            if not data:
                break

            try:
                call_id, name, args, kwargs = self._serializer.load(data)
            except Exception as e:
                self._logger.warning(
                    'received invalid data from %r', client_id,
                    client_id=client_id, exc_info=e,
                )
                response, call_id = e, None
            else:
                self._logger.subdebug(
                    'processing rpc %s', _fc(name, args, kwargs),
                    name=name, call_id=call_id, client_id=client_id
                )
                response = await self._process(name, args, kwargs)

                if call_id is None:
                    if isinstance(response, BaseException):
                        # TODO: maybe send an error report?
                        self._logger.error(
                            'nr-rpc %r failed', _fc(name, args, kwargs),
                            name=name, call_id=call_id, client_id=client_id
                        )
                    elif response is not None:
                        self._logger.warning(
                            'nr-rpc %r returned not-none value',
                            _fc(name, args, kwargs),
                            name=name, client_id=client_id,
                            response=str(response)
                        )
                    continue

            response = self._serializer.dump((call_id, response)) + b'\n'
            try:
                client_writer.write(response)
                await client_writer.drain()
            except Exception as e:
                self._logger.warning(
                    'failed to send response to client',
                    client_id=client_id, exc_info=e,
                )
                break

    def __repr__(self):
        return '<TCPServer %s:%s at %x>' % (self.host, self.port, id(self))

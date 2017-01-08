import abc
import asyncio
import logging

from ..utils.serialize import Serializer
from ..utils.logging import FormatCall as _fc, LoggerAdapter


class BaseClient(metaclass=abc.ABCMeta):
    """Base interface for an RPC client

    RPC client sends requests to an RPC server. It guarantees that requests
    will be sent in the same order as they were made and responses
    will be processed in the same order as they were received.
    If transport layer preserves package order (e.g. a TCP client/server),
    and a server guarantees that requests are processed in the same order
    as they received, than the statement above means that responses
    will be processed in the same order as the requests were made.

    This interface only implements basic methods for request processing.
    All serialization and transport processes must be implemented
    in its subclasses.

    Also be aware that this class in not threadsafe.

    Usage of the client class::

        loop = asyncio.get_event_loop()

        # Create a client instance
        client = TCPClient('localhost', 8080, loop=loop)

        # Connect to a server
        loop.run_until_complete(client.connect())

        # Call some remote procedures

        future = client.call('add', 2, 2)
        print(loop.run_until_complete(future))  # -> 2

        future = client.call('div', 10, 0)
        print(loop.run_until_complete(future))  # -> ZeroDivisionError()

        # Disconnect from a server
        loop.run_until_complete(client.disconnect())

    """

    loop = property(lambda self: self._loop)

    def __init__(self, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop
        self._call_id = 0

        self._logger = LoggerAdapter(
            logging.getLogger('lamport_mutex/rpc/client')
        )

    @abc.abstractmethod
    async def connect(self):
        """Connect to the server

        Can raise any errors depending on implementation.
        """

    @abc.abstractmethod
    async def disconnect(self):
        """Disconnect from the server

        Can raise any errors depending on implementation.

        NB: if there are any remote calls running while calling this method,
        theirs result may be lost. In this case, all pending futures returned
        from the ``.call()`` method will be marked as finished with an error.
        """

    @abc.abstractproperty
    def is_connected(self):
        """Indicates if the client is connected

        If the server is connecting or disconnecting,
        this property should be set to ``False``.
        """

    @abc.abstractmethod
    def call(self, name, *args, **kwargs):
        """Schedule a call to remote procedure

        This function does not block.

        :param name: name of the remote procedure
        :param args: arguments of the remote procedure
        :param kwargs: keyword arguments of the remote procedure
        :return: future that will be marked as done whenever a response
            is received
        :raises AssertionError: trying to make a remote call while
            the client is not connected
        """

    @abc.abstractmethod
    def call_nr(self, name, *args, **kwargs):
        """Schedule a call to the remote procedure which doesn't require response

        Server will process this remote call as usual, except that
        it will send no response back to client.

        This function does not block.

        :param name: name of the remote procedure
        :param args: arguments of the remote procedure
        :param kwargs: keyword arguments of the remote procedure
        :raises AssertionError: trying to make a remote call while
            the client is not connected
        """


class TCPClient(BaseClient):
    """RPC client over TCP using pickle

    This client implements TCP layer for the RPC BaseClient.

    For each client, there is a TCP connection that is kept open
    during the entire session.

    See also class ``TCPServer``.

    """
    _serializer = Serializer

    _host = None
    _port = None

    host = property(lambda self: self._host)
    port = property(lambda self: self._port)

    _state = None

    _DISCONNECTED = 0
    _DISCONNECTING = 1
    _CONNECTING = 2
    _CONNECTED = 3

    is_connected = property(lambda self: self._state == self._CONNECTED)

    def __init__(self, host, port, loop=None):
        super(TCPClient, self).__init__(loop)

        self._host = host
        self._port = port

        self._reader = None
        self._writer = None

        self._calls = {}

        self._state = self._DISCONNECTED

        # This condition notified whenever client disconnects / its socket
        # closes. The `disconnect` method won't finish until the connection
        # is actually closed (which is when this condition notified).
        self._disconnect_event = asyncio.Condition()

        self._logger = LoggerAdapter(
            self._logger,
            extra=dict(host=host, port=port)
        )

    async def connect(self):
        assert self._state == self._DISCONNECTED, 'already connected'

        self._logger.debug('connecting')

        self._state = self._CONNECTING

        try:
            self._reader, self._writer = await asyncio.streams.open_connection(
                self._host, self._port, loop=self._loop
            )
        except BaseException:
            self._state = self._DISCONNECTED
            raise

        task = asyncio.Task(self._receive())
        task.add_done_callback(
            lambda _: asyncio.ensure_future(
                self._disconnected(), loop=self._loop
            )
        )

        self._logger.subinfo('connected')

        self._state = self._CONNECTED

    async def disconnect(self):
        await self._disconnect_event.acquire()
        try:
            assert self._state == self._CONNECTED, 'not connected'

            self._logger.debug('disconnecting')

            self._state = self._DISCONNECTING

            try:
                self._writer.write_eof()
                await self._writer.drain()
            except OSError:
                pass
            self._writer.close()

            while self._writer is not None:
                await self._disconnect_event.wait()

            self._logger.subinfo('disconnected')
        finally:
            self._disconnect_event.release()

    def call(self, name, *args, **kwargs):
        assert self._state == self._CONNECTED, 'not connected'
        return self._call(name, args, kwargs)

    def call_nr(self, name, *args, **kwargs):
        assert self._state == self._CONNECTED, 'not connected'
        self._call(name, args, kwargs, require_response=False)

    def _call(self, name, args, kwargs, require_response=True):
        if require_response:
            self._call_id += 1
            call_id = self._call_id
            self._logger.subdebug(
                'requesting rpc %r', _fc(name, args, kwargs),
                name=name, call_id=call_id
            )
        else:
            call_id = None
            self._logger.subdebug(
                'requesting nr-rpc %r', _fc(name, args, kwargs),
                name=name
            )

        data = self._serializer.dump((call_id, name, args, kwargs))
        self._writer.write(data + b'\n')

        if require_response:
            future = self._calls[self._call_id] = asyncio.Future()
            return future

    async def _receive(self):
        while True:
            try:
                data = await self._reader.readline()
            except OSError:
                break
            if not data:
                break

            call_id, response = self._serializer.load(data)
            if call_id is None or call_id not in self._calls:
                # TODO: maybe throw an error?
                self._logger.error(
                    'got response to an unknown call', call_id=str(call_id)
                )
            elif isinstance(response, BaseException):
                self._logger.subdebug(
                    'rpc failed',
                    call_id=call_id, exc_info=response
                )
                self._calls.pop(call_id).set_exception(response)
            else:
                self._logger.subdebug(
                    'rpc succeed',
                    call_id=call_id, response=str(response)
                )
                self._calls.pop(call_id).set_result(response)

    async def _disconnected(self):
        if self._state != self._DISCONNECTING:
            self._logger.subinfo('connection lost')

        await self._disconnect_event.acquire()
        try:
            for call_id, call in self._calls.items():
                call.set_exception(
                    EOFError(
                        '%r got no response to call %r '
                        '(connection lost)' % (self, call_id)
                    )
                )

            self._calls = {}

            self._writer.close()

            self._reader = None
            self._writer = None

            self._state = self._DISCONNECTED

            self._disconnect_event.notify_all()
        finally:
            self._disconnect_event.release()

    def __repr__(self):
        return '<TCPClient %s:%s at %x>' % (self.host, self.port, id(self))

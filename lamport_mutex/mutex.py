import asyncio
import logging
import heapq
import os

from .rpc.server import TCPServer
from .rpc.client import TCPClient
from .utils.logging import LoggerAdapter


_logger = LoggerAdapter(logging.getLogger('lamport_mutex'))


class Mutex:
    """Implementation of the Lamport mutual exclusion algorithm

    This class is not threadsafe.
    """

    _server_cls = TCPServer
    _client_cls = TCPClient

    _time = None
    _pid = None

    time = property(lambda self: self._time)
    pid = property(lambda self: self._pid)

    _NOT_INITIALIZED = 0
    _INITIALIZING = 1
    _READY = 2
    _RELEASING = 3
    _RELEASED = 4
    _ACQUIRING = 5
    _ACQUIRED = 6
    _TERMINATING = 7
    _TERMINATED = 8

    _state_str = {
        _NOT_INITIALIZED: 'not_initialized',
        _INITIALIZING: 'initializing',
        _READY: 'ready',
        _RELEASING: 'releasing',
        _RELEASED: 'released',
        _ACQUIRING: 'acquiring',
        _ACQUIRED: 'acquired',
        _TERMINATING: 'terminating',
        _TERMINATED: 'terminated',
    }

    _state = None

    state_str = property(lambda self: self._state_str.get(self._state))

    def __init__(self, *args, others, loop=None, pid=None):
        """Initialize the server

        :param args: star args that will be passed to the server class.
            If using TCPServer as a transport (which is the default),
            ``args`` are expected to be a pair of host and port.
        :param others: list of other processes. Every element of this list
            will be passed as star-args to the client class.
            If using TCPServer as a transport (which is the default),
            ``others`` are expected to be a list of host and port pairs.
        :param loop: asyncio loop to run this mutex in.
        :param pid: id for this process. This may be any value that is
            unique for all processes. It should be serializable and
            comparable with ids of other processes.
            By default ``os.getpid()`` is used.
        """
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop

        if pid is None:
            self._pid = str(os.getpid())
        else:
            self._pid = pid

        # Current logical time
        self._time = 0

        self._server = self._server_cls(*args, loop=self._loop)

        self._server.register(self._rp_status)
        self._server.register(self._rp_start)
        self._server.register(self._rp_terminate)
        self._server.register(self._rp_heartbeat)
        self._server.register(self._rp_request)
        self._server.register(self._rp_release)

        self._client_addresses = others

        self._state = self._NOT_INITIALIZED

        # Map from client's pid to its logical time
        self._client_clocks = {}
        # Map from client's pid to its connection
        self._client_connections = {}

        self._queue = []

        # This event is set when the mutex is fully initialized and ready to
        # process rp calls and acquire/release commands.
        # See the init procedure for more information.
        self._ready_event = asyncio.Event()

        # Condition notified whenever there is an update in the state of
        # the queue or client logical time.
        self._state_update_event = asyncio.Condition()

    # Init routines and handshake protocol

    async def init(self):
        """Initialize the mutex

        Start up the rpc server, connect to all other clients
        and set ``_ready_event``.

        We can't start sending requests until we connect
        to all other processes. Moreover, the algorithm have
        no standard procedure for adding a new process or disconnecting
        an old one. This makes initialization a bit complicated.

        The whole process is done in four steps.

        1) Start the rpc server so all others can connect.

        2) Connect to all other processes.
        If there is no response from a process, we retry for 50 times
        with a little delay and than give up and fail the init procedure.

        3) Request ids from all clients. After getting all responses,
        the mutex is ready to operate (however, it's not that we can start
        sending requests -- the other processes can still be initializing;
        we need someone to send a start signal after ensuring
        that all clients are ready).

        4) Choose one process to ensure that all clients are ready.

        If this process have the least pid of all, than it is the master.

        Master is to query all clients until they report theirs readiness.
        When it ensures that every process is ready, it broadcasts a start
        signal and set the ``_ready_event``.
        After that, initialization is done.

        All other processes are to wait signal from the master.
        Whenever they receive a start signal, they set ``_ready_event``
        and finish initialization.

        """
        assert self._state == self._NOT_INITIALIZED, \
            'cannot initialize mutex in this state'

        try:
            await self._init()
        except Exception as e:
            _logger.error('initializing failed', exc_info=e, stack_info=True)
            await self._tear_down()
            raise

    async def _init(self):
        self._state = self._INITIALIZING

        _logger.debug('initializing')

        # 1) start a server

        await self._server.start()

        # 2) connect to all clients

        clients = [
            self._client_cls(*_, loop=self._loop)
            for _ in self._client_addresses
        ]

        for client in clients:
            for _ in range(50):
                try:
                    await client.connect()
                except OSError:
                    _logger.debug('attempt %s to connect to %r failed', _ + 1,
                                  client)
                    await asyncio.sleep(0.1)
                else:
                    _logger.debug('connected to %r', client)
                    break

            if not client.is_connected:
                _logger.critical('failed to connect to %r', client)
                raise RuntimeError('failed to connect to %r' % client)

        _logger.debug('connected to all clients')

        # 3) collect statuses

        least_pid = self._pid

        for client in clients:
            pid, time, state = await client.call('_rp_status')
            least_pid = min(least_pid, pid)
            self._client_clocks[pid] = time
            self._client_connections[pid] = client

        self._state = self._READY

        _logger.debug('ready to run')

        # 4) choose a master

        if least_pid == self._pid:
            _logger.debug('this process is a master')

            for client in self._client_connections.values():
                state = self._NOT_INITIALIZED

                while state != self._READY:
                    await asyncio.sleep(0.01)
                    _, _, state = await client.call('_rp_status')
                    _logger.debug('client %r have status %r', client, state)

                _logger.debug('client %r is ready', client)

            _logger.debug('all clients are ready to run')

            for client in self._client_connections.values():
                client.call_nr('_rp_start', self._pid)

            self._state = self._RELEASED
            self._ready_event.set()
        else:
            _logger.debug('waiting command from master')
            await self._ready_event.wait()

        # 5) check that no client disconnected during init

        for client in self._client_connections.values():
            if not client.is_connected:
                raise RuntimeError('client disconnected while initialising')

        # 6) lock 'n load!

        _logger.info('initialized')

    async def tear_down(self):
        """Stop the mutex

        Broadcast a terminate message, close all connections
        and unset ``_ready_event``.

        """
        assert self._state in (self._ACQUIRED, self._RELEASED,
                               self._ACQUIRING, self._RELEASING), \
            'cannot tear down mutex in this state'

        await self._tear_down()

    async def _tear_down(self):
        self._state = self._TERMINATING

        _logger.debug('stopping')

        self._ready_event.clear()

        for client in self._client_connections.values():
            if not client.is_connected:
                continue
            try:
                await client.call('_rp_terminate', self._pid)
            except EOFError:
                _logger.debug(
                    'client %r is already dead',
                    client
                )
            except Exception as e:
                _logger.debug(
                    'terminate call to %r was not successful; '
                    'maybe that client is already dead?',
                    client, exc_info=e
                )

            if client.is_connected:
                await client.disconnect()

        if self._server.is_running:
            await self._server.stop()

        self._state = self._TERMINATED

        await self._state_update_event.acquire()
        try:
            self._state_update_event.notify_all()
        finally:
            self._state_update_event.release()

        _logger.info('stopped')

    # Public API

    async def acquire(self):
        """Request the mutex ownership and wait till we're granted access"""
        assert self._state == self._RELEASED, \
            'cannot acquire mutex in this state'

        self._state = self._ACQUIRING

        _logger.debug('acquiring')

        request = (self._time, self._pid)

        heapq.heappush(self._queue, request)

        self._time += 1

        for client in self._client_connections.values():
            client.call_nr('_rp_request', self._pid, self._time, request)

        _logger.debug('requests sent')

        await self._state_update_event.acquire()
        try:
            while not self._can_acquire():
                assert self._state == self._ACQUIRING  # can be terminated
                await self._state_update_event.wait()
        finally:
            self._state_update_event.release()

        self._state = self._ACQUIRED

        _logger.info('acquired')

    async def release(self):
        """Release the mutex and broadcast good news to other processes"""
        assert self._state == self._ACQUIRED, \
            'cannot release mutex in this state'

        self._state = self._RELEASING

        _logger.debug('releasing')

        if not self._queue or self._queue[0][1] != self._pid:
            _logger.critical(
                'got release request but there is no request '
                'from this client on top of the queue '
                '(current queue state is %r)',
                self._queue
            )
            raise RuntimeError(
                'got release message but there is no request '
                'from this client on top of the queue'
            )
        else:
            request = heapq.heappop(self._queue)

        self._time += 1

        for client in self._client_connections.values():
            client.call_nr('_rp_release', self._pid, self._time, request)

        self._state = self._RELEASED

        _logger.info('released')

    # Routines

    def _can_acquire(self):
        if not self._queue:
            return False
        request_time = self._queue[0][0]
        return self._queue[0][1] == self._pid and all(
            time > request_time for time in self._client_clocks.values()
        )

    # Remotes

    async def _rp_status(self):
        return self._pid, self._time, self._state

    async def _rp_start(self, pid):
        if not self._ready_event.is_set():
            _logger.subdebug('got start command from %r', pid)
            self._state = self._RELEASED
            self._ready_event.set()

    async def _rp_terminate(self, pid):
        if self._state != self._TERMINATING:
            _logger.subdebug('got terminate command from %r', pid)
            self._state = self._TERMINATING
            asyncio.ensure_future(self._tear_down(), loop=self._loop)

    async def _rp_heartbeat(self, pid, time):
        if self._state in (self._TERMINATING, self._TERMINATED):
            return

        if not self._ready_event.is_set():
            await self._ready_event.wait()

        await self._state_update_event.acquire()
        try:
            self._time = max(self._time, time) + 1
            self._client_clocks[pid] = time

            self._state_update_event.notify_all()
        finally:
            self._state_update_event.release()

        _logger.subdebug('processed heartbeat from %r', pid)

    async def _rp_request(self, pid, time, request):
        if self._state in (self._TERMINATING, self._TERMINATED):
            return

        if not self._ready_event.is_set():
            await self._ready_event.wait()

        await self._state_update_event.acquire()
        try:
            heapq.heappush(self._queue, request)

            self._time = max(self._time, time) + 1
            self._client_clocks[pid] = time

            self._state_update_event.notify_all()
        finally:
            self._state_update_event.release()

        self._client_connections[pid].call_nr(
            '_rp_heartbeat', self._pid, self._time
        )

        _logger.subdebug('processed request from %r', pid)

    async def _rp_release(self, pid, time, request):
        if self._state in (self._TERMINATING, self._TERMINATED):
            return

        if not self._ready_event.is_set():
            await self._ready_event.wait()

        await self._state_update_event.acquire()
        try:
            if not self._queue or request not in self._queue:
                _logger.critical(
                    'got release message from %r but there is no request '
                    'from this client in the queue '
                    '(current queue state is %r)',
                    pid, self._queue
                )
                raise RuntimeError(
                    'got release message from %r but there is no '
                    'request from this client in the queue' % pid
                )
            else:
                self._queue.remove(request)
                heapq.heapify(self._queue)

            self._time = max(self._time, time) + 1
            self._client_clocks[pid] = time

            self._state_update_event.notify_all()
        finally:
            self._state_update_event.release()

        self._client_connections[pid].call_nr(
            '_rp_heartbeat', self._pid, self._time
        )

        _logger.subdebug('processed release from %r', pid)

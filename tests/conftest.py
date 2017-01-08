import pytest
import logging
import warnings

from lamport_mutex.rpc.server import TCPServer
from lamport_mutex.rpc.client import TCPClient
from lamport_mutex.mutex import Mutex

from lamport_mutex.utils.logging import setup_logging

setup_logging(level=logging.ERROR)

# Set to True to enable warnings from asyncio
asyncio_debug = True

if asyncio_debug:
    # logging.getLogger('asyncio').setLevel(logging.DEBUG)
    warnings.filterwarnings('always', category=ResourceWarning)


@pytest.fixture
def server(event_loop, unused_tcp_port):
    if asyncio_debug:
        event_loop.set_debug(True)
    return TCPServer('localhost', unused_tcp_port, loop=event_loop)


@pytest.fixture
def client_factory():
    def factory(server):
        return TCPClient(server.host, server.port, loop=server.loop)
    return factory


@pytest.fixture
def mutex_factory(event_loop, unused_tcp_port_factory):
    def factory(n):
        ports = [unused_tcp_port_factory() for _ in range(n)]
        return [
            Mutex(
                'localhost', port,
                others=[
                    ('localhost', _port) for _port in ports if _port != port
                ],
                pid=i,
                loop=event_loop
            ) for i, port in enumerate(ports)
        ]

    factory.event_loop = event_loop

    return factory

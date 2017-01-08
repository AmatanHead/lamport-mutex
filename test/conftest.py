import pytest
import logging
import warnings

from lamport_mutex.rpc.server import TCPServer
from lamport_mutex.rpc.client import TCPClient

from lamport_mutex.utils.logging import setup_logging

setup_logging(level=logging.ERROR)

# Set to True to enable warnings from asyncio
asyncio_debug = True

if asyncio_debug:
    logging.getLogger('asyncio').setLevel(logging.INFO)
    warnings.filterwarnings('always', category=ResourceWarning)


@pytest.fixture
def server(event_loop, unused_tcp_port):
    if asyncio_debug:
        event_loop.set_debug(True)
    return TCPServer('localhost', unused_tcp_port, loop=event_loop)


@pytest.fixture
def server_client(server):
    return server, TCPClient(server.host, server.port, loop=server.loop)

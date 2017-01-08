import asyncio
import pytest

from lamport_mutex.rpc.client import TCPClient


@pytest.mark.asyncio
async def test_server_start_stop(server):
    await server.start()

    assert server.is_running

    with pytest.raises(RuntimeError):
        await server.start()

    assert server.is_running

    await server.stop()

    assert not server.is_running

    with pytest.raises(RuntimeError):
        await server.stop()

    assert not server.is_running


@pytest.mark.asyncio
async def test_client_connect_disconnect(server_client):
    server, client = server_client

    await server.start()

    await client.connect()

    assert server.is_running
    assert client.is_connected

    with pytest.raises(RuntimeError):
        await client.connect()

    await client.disconnect()

    assert not client.is_connected

    with pytest.raises(RuntimeError):
        await client.disconnect()

    await server.stop()


@pytest.mark.asyncio
async def test_handlers(server_client):
    server, client = server_client

    @server.register
    async def div(a, b):
        return a // b

    server.register(div, 'div_2')

    with pytest.raises(TypeError):
        server.register(lambda: None, 'name')

    with pytest.raises(NameError):
        server.register(div, 'div_2')

    await server.start()
    await client.connect()

    assert await client.call('div', 10, 2) == 5
    assert await client.call('div', 10, b=2) == 5
    assert await client.call('div', a=10, b=2) == 5

    assert await client.call('div_2', 10, 2) == 5
    assert await client.call('div_2', 10, b=2) == 5
    assert await client.call('div_2', a=10, b=2) == 5

    with pytest.raises(ZeroDivisionError):
        await client.call('div', 1, 0)

    with pytest.raises(NameError):
        await client.call('unknown_callback')

    await client.disconnect()
    await server.stop()


@pytest.mark.asyncio
async def test_nr_handlers(server_client):
    server, client = server_client

    results = []

    @server.register
    async def cb(i):
        results.append(i)

    await server.start()
    await client.connect()

    client.call('cb', 1)
    client.call('cb', 2)
    client.call('cb', 3)

    await asyncio.sleep(0.01)

    assert results == [1, 2, 3]

    await client.disconnect()
    await server.stop()


@pytest.mark.asyncio
async def test_concurrent(server):
    client1 = TCPClient(server.host, server.port, loop=server.loop)
    client2 = TCPClient(server.host, server.port, loop=server.loop)

    @server.register
    async def call(i):
        await asyncio.sleep(0.01)
        return i

    await server.start()
    await client1.connect()
    await client2.connect()

    que1, que2 = [], []
    que1_expected, que2_expected = [], []

    futures = []

    def sched(client, i, que, que_expected):
        fut = client.call('call', i)
        fut.add_done_callback(lambda f: que.append(f.result()))
        futures.append(fut)
        que_expected.append(i)

    sched(client1, 0, que1, que1_expected)
    sched(client1, 1, que1, que1_expected)
    sched(client2, 2, que2, que2_expected)
    sched(client1, 3, que1, que1_expected)
    sched(client2, 4, que2, que2_expected)
    sched(client2, 5, que2, que2_expected)
    sched(client1, 6, que1, que1_expected)
    sched(client1, 7, que1, que1_expected)
    sched(client2, 8, que2, que2_expected)
    sched(client1, 9, que1, que1_expected)
    sched(client2, 10, que2, que2_expected)
    sched(client1, 11, que1, que1_expected)
    sched(client2, 12, que2, que2_expected)
    sched(client1, 13, que1, que1_expected)
    sched(client2, 14, que2, que2_expected)
    sched(client2, 15, que2, que2_expected)

    for future in futures:
        await future

    await asyncio.sleep(0.01)

    assert que1 == que1_expected
    assert que2 == que2_expected

    await client1.disconnect()
    await client2.disconnect()
    await server.stop()


@pytest.mark.asyncio
async def test_sudden_disconnection(server_client):
    server, client = server_client

    @server.register
    async def long_cb():
        await asyncio.sleep(0.01)

    await server.start()
    await client.connect()

    future = client.call('long_cb')

    await server.stop()

    with pytest.raises(EOFError):
        await future

    assert not client.is_connected
    assert not server.is_running

    await server.start()
    await client.connect()

    future = client.call('long_cb')

    await client.disconnect()

    with pytest.raises(EOFError):
        await future

    assert not client.is_connected
    assert server.is_running

    await server.stop()

    await server.start()
    await client.connect()

    await server.stop()

    assert not client.is_connected
    assert not server.is_running

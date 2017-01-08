import asyncio
import pytest


@pytest.mark.asyncio
async def test_single_mutex_no_double_calls(mutex_factory):
    mutex, = mutex_factory(1)

    await mutex.init()

    with pytest.raises(AssertionError):
        await mutex.init()

    with pytest.raises(AssertionError):
        await mutex.release()

    await mutex.acquire()

    with pytest.raises(AssertionError):
        await mutex.acquire()

    await mutex.release()

    with pytest.raises(AssertionError):
        await mutex.release()

    await mutex.tear_down()

    with pytest.raises(AssertionError):
        await mutex.tear_down()

    with pytest.raises(AssertionError):
        await mutex.init()


@pytest.mark.asyncio
async def test_single_mutex_no_parallel_calls(mutex_factory):
    mutex, = mutex_factory(1)

    await mutex.init()

    coro1 = mutex.acquire()
    coro2 = mutex.acquire()

    result = await asyncio.gather(coro1, coro2, return_exceptions=True)

    assert (
        result[0] is None and isinstance(result[1], AssertionError)
        or
        result[1] is None and isinstance(result[0], AssertionError)
    )

    with pytest.raises(AssertionError):
        await mutex.acquire()

    coro1 = mutex.release()
    coro2 = mutex.release()

    result = await asyncio.gather(coro1, coro2, return_exceptions=True)

    assert (
        result[0] is None and isinstance(result[1], AssertionError)
        or
        result[1] is None and isinstance(result[0], AssertionError)
    )

    with pytest.raises(AssertionError):
        await mutex.release()

    await mutex.tear_down()


# tear down leads to all mutex terminated
# sudden disconnection during init

@pytest.mark.asyncio
@pytest.mark.parametrize('n', [2, 5, 10])
async def test_mutex_init_teardown(mutex_factory, n):
    mutexes = mutex_factory(n)
    loop = mutex_factory.event_loop

    await asyncio.gather(*[mutex.init() for mutex in mutexes])

    for mutex in mutexes:
        with pytest.raises(AssertionError):
            await mutex.init()

    await mutexes[0].tear_down()  # should send tear_down to others

    await asyncio.sleep(0.01)

    for mutex in mutexes:
        with pytest.raises(AssertionError):
            await mutex.tear_down()

    all_tasks = asyncio.Task.all_tasks(loop=loop)
    current_task = asyncio.Task.current_task(loop=loop)
    pending = [t for t in all_tasks if t is not current_task]
    await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('n', [2, 5, 10])
async def test_mutex_multi_teardown(mutex_factory, n):
    mutexes = mutex_factory(n)
    loop = mutex_factory.event_loop

    await asyncio.gather(*[mutex.init() for mutex in mutexes])

    for mutex in mutexes:
        with pytest.raises(AssertionError):
            await mutex.init()

    result = await asyncio.gather(*[mutex.tear_down() for mutex in mutexes],
                                  return_exceptions=True)

    assert None in result and all(
        r is None or isinstance(r, AssertionError) for r in result
    )

    all_tasks = asyncio.Task.all_tasks(loop=loop)
    current_task = asyncio.Task.current_task(loop=loop)
    pending = [t for t in all_tasks if t is not current_task]
    await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('_', [1, 2, 3])  # run test for three times
async def test_double_mutex_locks(mutex_factory, _):
    mutex1, mutex2 = mutex_factory(2)

    result = []

    async def inner(mutex, i):
        await mutex.acquire()
        result.append(i)
        await asyncio.sleep(0.1)
        result.append(i)
        await mutex.release()

    await asyncio.gather(mutex1.init(), mutex2.init())
    await asyncio.gather(inner(mutex1, 1), inner(mutex2, 2))
    await asyncio.gather(mutex1.tear_down(), mutex2.tear_down(),
                         return_exceptions=True)

    assert result in [[1, 1, 2, 2], [2, 2, 1, 1]]

    result = []

    async def inner(i):
        await asyncio.sleep(0)
        result.append(i)
        await asyncio.sleep(0.1)
        result.append(i)

    await asyncio.gather(inner(1), inner(2))

    assert result in [
        [1, 2, 1, 2],
        [1, 2, 2, 1],
        [2, 1, 1, 2],
        [2, 1, 2, 1],
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize('_', [1, 2, 3])  # run test for three times
async def test_double_mutex_order_fairness(mutex_factory, _):
    mutex1, mutex2 = mutex_factory(2)

    result = []

    async def inner(acquire_coro, mutex, i):
        await acquire_coro
        result.append(i)
        await asyncio.sleep(0.01)
        result.append(i)
        await mutex.release()

    await asyncio.gather(mutex1.init(), mutex2.init())

    acquire_coro_2 = mutex2.acquire()
    acquire_coro_1 = mutex1.acquire()

    await asyncio.gather(
        inner(acquire_coro_2, mutex2, 2), inner(acquire_coro_1, mutex1, 1)
    )

    await asyncio.gather(mutex1.tear_down(), mutex2.tear_down(),
                         return_exceptions=True)

    assert result == [1, 1, 2, 2]

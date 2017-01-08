#!/usr/bin/env python3

import argparse
import asyncio
import logging
import sys
import os
import shutil
import socket
import contextlib
import time
import fcntl

from lamport_mutex.mutex import Mutex
from lamport_mutex.utils.logging import (
    setup_logging, Colors, SUBDEBUG, SUBINFO, LoggerAdapter
)
from lamport_mutex.version import __version__ as version
from lamport_mutex.rpc.server import TCPServer
from lamport_mutex.rpc.client import TCPClient


_logger = LoggerAdapter(logging.getLogger('lamport_mutex/cli'))


def host_port_type(value):
    data = value.split(':')
    if len(data) == 1:
        host, port = 'localhost', data[0]
    elif len(data) == 2:
        host, port = data
        host = host or 'localhost'
    else:
        raise argparse.ArgumentTypeError(
            'host-port arguments expected to have format '
            '"[host:]port", but %r does not match '
            'that format' % value
        )

    if not port.isdigit():
        raise argparse.ArgumentTypeError(
            'host-port arguments expected to have format '
            '"[host:]port", but %r have '
            'invalid port %r' % (value, port)
        )

    return host, int(port)


def directory_type(value):
    if not os.path.exists(value):
        try:
            os.mkdir(value)
        except OSError as e:
            raise argparse.ArgumentTypeError(
                'cannot create directory %s: %s' % (value, e)
            ) from None
    if not os.path.isdir(value):
        raise argparse.ArgumentTypeError(
            '%s is not a directory' % value
        )

    return value


def filepath_type(value):
    try:
        with open(value, 'w'):
            pass
    except OSError:
        raise argparse.ArgumentTypeError(
            '%s is an invalid file path' % value
        )

    return value


def unused_tcp_port():
    with contextlib.closing(socket.socket()) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def make_parser():
    parser = argparse.ArgumentParser(
        description='run single lamport mutex instance or a stress test'
    )

    parser.add_argument('-v', '--version', action='version', version=version)

    subparsers = parser.add_subparsers(dest='cmd')
    subparsers.required = True

    run = subparsers.add_parser(
        'run', help='run mutex in a normal mode. This mode uses stdin and '
                    'stdout to control the mutex state. Pass in "acquire" '
                    'or "release" commands to stdin and wait for response '
                    'in stdout.',
    )

    run.add_argument('host_port', type=host_port_type,
                     help='host-port pair of this mutex '
                          'in format "[host:]port" (default host is '
                          '"localhost")')

    run.add_argument('other_clients', type=host_port_type, nargs='*',
                     help='host-port pairs of other clients')

    run.add_argument('-V', '--verbose', action='count',
                     help='increase verbosity level')

    run.add_argument('--log', type=argparse.FileType('w'), default=sys.stderr,
                     help='where to write log (default is stderr)')

    run.add_argument('--no-color', action='store_true',
                     help='disable colors in the log')

    stress = subparsers.add_parser(
        'stress', help='run a stress test. In this mode, several processes are '
                       'locking and unlocking a single mutex in loop. '
                       'All logs from subprocesses are written to files in '
                       'a given directory (maybe overwriting existing ones).',
    )

    stress.add_argument('n', type=int, default=3, nargs='?',
                        help='number of concurrent processes to run')

    stress.add_argument('t', type=int, default=60, nargs='?',
                        help='time in seconds to run')

    stress.add_argument('--lock-timeout', type=int, default=5,
                        help='timeout of the locking operation (in seconds). '
                             'If mutex cannot be acquired during this time, '
                             'program logs critical message and '
                             'raises an error.')

    stress.add_argument('--log', type=argparse.FileType('w'),
                        default=sys.stderr,
                        help='where to write log from the main process '
                             '(default is stderr)')

    stress.add_argument('--log-dir', type=directory_type, required=True,
                        help='a directory where to write logs from child '
                             'processes (this will create logs called '
                             '"<pid>.log" in that directory, '
                             'one for each subprocess)')

    stress.add_argument('--clean-log-dir', action='store_true',
                        help='remove all files from the log directory before '
                             'executing subprocesses (warning: all contents '
                             'of the log directory will be removed '
                             'recursively)')

    stress.add_argument('--lock-file', type=filepath_type,
                        required=True,
                        help='this file will be locked once the mutex '
                             'is acquired.')

    stress.add_argument('--no-color', action='store_true',
                        help='disable colors in the log of the main process')

    stress_worker = subparsers.add_parser(
        'stress_worker', help='a helper mode used by stresstest. '
                              'Runs a single mutex which is '
                              'locked and unlocked in loop.',
    )

    stress_worker.add_argument('host_port', type=host_port_type,
                               help='host-port pair of this mutex '
                                    'in format "[host:]port" (default host is '
                                    '"localhost")')

    stress_worker.add_argument('other_clients', type=host_port_type, nargs='*',
                               help='host-port pairs of other clients')

    stress_worker.add_argument('--master-host-port', type=host_port_type,
                               required=True,
                               help='host-port pair of the master process '
                                    'rpc server')

    stress_worker.add_argument('--lock-file', type=filepath_type,
                               required=True,
                               help='this file will be locked once '
                                    'the mutex is acquired.')

    stress_worker.add_argument('--timeout', type=int, default=60,
                               help='time in seconds to run this mutex')

    stress_worker.add_argument('--log-dir', type=directory_type, required=True,
                               help='a directory where to write logs (this '
                                    'will create a log in that directory '
                                    'called "<pid>.log")')

    stress_worker.add_argument('--lock-timeout', type=int, default=5,
                               help='timeout of the locking operation '
                                    '(in seconds). If mutex cannot be acquired '
                                    'during this time, program logs critical '
                                    'message and returns.')

    return parser


def main():
    parser = make_parser()
    namespace = parser.parse_args()

    if namespace.cmd == 'run':
        run(namespace)
    elif namespace.cmd == 'stress':
        stress(namespace)
    elif namespace.cmd == 'stress_worker':
        stress_worker(namespace)


def run(namespace):
    loop = asyncio.get_event_loop()

    host, port = namespace.host_port

    mutex = Mutex(host, port, others=namespace.other_clients, loop=loop)

    if namespace.verbose > 2:
        level = SUBDEBUG
    else:
        level = [logging.INFO, SUBINFO, logging.DEBUG][namespace.verbose]

    setup_logging(mutex, host, port,
                  stream=namespace.log, level=level,
                  use_colors=not namespace.no_color)

    async def inner():
        await mutex.init()
        while True:
            state = mutex.state_str
            message = '(%s) >>> ' % state
            if not namespace.no_color:
                message = Colors.Fg.lightcyan + message + Colors.reset
            sys.stdout.write(message)
            sys.stdout.flush()

            cmd = await loop.run_in_executor(None, sys.stdin.readline)
            cmd = cmd.lower().strip()

            if cmd in ('a', 'acquire'):
                try:
                    await mutex.acquire()
                except AssertionError as e:
                    message = 'acquire failed: %r' % e
                    if not namespace.no_color:
                        message = Colors.Fg.red + Colors.underline + message + Colors.reset
                    print(message)
                else:
                    message = 'acquire done'
                    if not namespace.no_color:
                        message = Colors.Fg.purple + Colors.underline + message + Colors.reset
                    print(message)
            elif cmd in ('r', 'release'):
                try:
                    await mutex.release()
                except AssertionError as e:
                    message = 'release failed: %r' % e
                    if not namespace.no_color:
                        message = Colors.Fg.red + Colors.underline + message + Colors.reset
                    print(message)
                else:
                    message = 'release done'
                    if not namespace.no_color:
                        message = Colors.Fg.purple + Colors.underline + message + Colors.reset
                    print(message)
            elif cmd in ('e', 'exit'):
                break

    try:
        loop.run_until_complete(inner())
    except KeyboardInterrupt:
        pass

    try:
        loop.run_until_complete(mutex.tear_down())
    except AssertionError:
        print('the mutex is already torn down')

    loop.stop()
    loop.close()


def stress(namespace):
    loop = asyncio.get_event_loop()

    setup_logging(stream=namespace.log,
                  use_colors=not namespace.no_color)

    if namespace.clean_log_dir:
        _logger.info('cleaning %r', namespace.log_dir)
        shutil.rmtree(namespace.log_dir)
    if not os.path.exists(namespace.log_dir):
        os.mkdir(namespace.log_dir)

    status_server = TCPServer('localhost', unused_tcp_port(), loop=loop)

    clients = {}
    cycles_d = {}

    @status_server.register
    async def rp_ready(pid):
        _logger.info('client %s is ready', pid)
        clients[pid] = 'ready'

    @status_server.register
    async def rp_progress(pid, cycles, speed):
        clients[pid] = '%s cycles; %0.5f spc' % (cycles, speed)
        cycles_d[pid] = cycles

        den = (namespace.n * sum(x ** 2 for x in cycles_d.values()))
        fairness = sum(cycles_d.values()) ** 2 / den if den else 0

        _logger.info('%r (fairness = %0.5f)', clients, fairness)

    @status_server.register
    async def rp_done(pid):
        _logger.info('client %s done', pid)
        clients[pid] = 'done'

    @status_server.register
    async def rp_fail(pid, info='', error=None):
        if info:
            info = ' (' + info + ')'
        _logger.error('client %s failed%s', pid, info, exc_info=error)
        clients[pid] = 'failed'

    loop.run_until_complete(status_server.start())

    ports = [unused_tcp_port() for _ in range(namespace.n)]

    _logger.info('starting up subprocesses')
    _logger.info('ports are %r', ports)

    processes = []

    for port in ports:
        args = (
            sys.argv[0],
            'stress_worker',
            '--master-host-port', str(status_server.port),
            '--lock-file', namespace.lock_file,
            '--timeout', str(namespace.t),
            '--lock-timeout', str(namespace.lock_timeout),
            '--log-dir', namespace.log_dir,
            str(port),
            *map(str, filter(lambda x: x != port, ports)),
        )

        _logger.info(' '.join(map(str, args)))

        process = loop.run_until_complete(
            asyncio.create_subprocess_exec(
                *args, loop=loop,
                stderr=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
            ),
        )

        processes.append(process)

    for process in processes:
        stdout_data, stderr_data = loop.run_until_complete(
            process.communicate()
        )
        code = loop.run_until_complete(process.wait())
        _logger.info('process exited with code %s', code)
        if stderr_data or stdout_data:
            _logger.info('data from process',
                         stderr_data=stderr_data, stdout_data=stdout_data)

    _logger.info('all processes has exited')

    loop.run_until_complete(status_server.stop())

    loop.stop()
    loop.close()


def stress_worker(namespace):
    pid = os.getpid()

    loop = asyncio.get_event_loop()

    host, port = namespace.host_port

    mutex = Mutex(host, port, others=namespace.other_clients, loop=loop)

    log_file = open(os.path.join(namespace.log_dir, '%s.log' % pid), 'w')

    setup_logging(mutex, host, port,
                  stream=log_file, level=logging.DEBUG, use_colors=False)

    status_client = TCPClient(*namespace.master_host_port, loop=loop)

    loop.run_until_complete(status_client.connect())
    status_client.call_nr('rp_ready', pid)

    async def inner():
        try:
            await mutex.init()
        except BaseException as e:
            _logger.critical(
                'error while initializing the mutex',
                exc_info=e, stack_info=True
            )
            status_client.call_nr('rp_fail', pid, 'initializing failed', e)
            raise

        status_client.call_nr('rp_progress', pid, 0, 0)

        start_time = time.time()
        measure_time = time.time()

        count = 0

        while True:
            if time.time() - start_time > namespace.timeout:
                _logger.info('stopping the mutex')
                return

            try:
                await asyncio.wait_for(mutex.acquire(), namespace.lock_timeout)
            except TimeoutError as e:
                _logger.critical(
                    'timeout while locking the mutex',
                    exc_info=e, stack_info=True
                )
                status_client.call_nr('rp_fail', pid, 'acquiring timeout', e)
                raise
            except AssertionError as e:
                if mutex.state_str in ('terminating', 'terminated'):
                    return
                _logger.critical(
                    'error while releasing the mutex',
                    exc_info=e, stack_info=True
                )
                raise
            except BaseException as e:
                _logger.critical(
                    'error while locking the mutex',
                    exc_info=e, stack_info=True
                )
                status_client.call_nr('rp_fail', pid, 'acquiring failed', e)
                raise

            count += 1

            try:
                with open(namespace.lock_file, 'a') as file:
                    fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    file.write('locked by %r\n' % pid)
                    # time.sleep(random.random() * 0.01)
            except BlockingIOError:
                _logger.critical(
                    'mutex is acquired but there is '
                    'another process locked the file'
                )
                status_client.call_nr('rp_fail', pid, 'file is locked')
                raise

            try:
                await asyncio.wait_for(mutex.release(), namespace.lock_timeout)
            except TimeoutError as e:
                _logger.critical(
                    'timeout while releasing the mutex',
                    exc_info=e, stack_info=True
                )
                status_client.call_nr('rp_fail', pid, 'releasing timeout', e)
                raise
            except AssertionError as e:
                if mutex.state_str in ('terminating', 'terminated'):
                    return
                _logger.critical(
                    'error while releasing the mutex',
                    exc_info=e, stack_info=True
                )
                raise
            except BaseException as e:
                _logger.critical(
                    'error while releasing the mutex',
                    exc_info=e, stack_info=True
                )
                status_client.call_nr('rp_fail', pid, 'releasing failed', e)
                raise

            if count % 100 == 0:
                current_time = time.time()
                speed = (current_time - measure_time) / 100
                measure_time = current_time
                status_client.call_nr('rp_progress', pid, count, speed)

    try:
        loop.run_until_complete(inner())
    except KeyboardInterrupt:
        pass
    except BaseException as e:
        _logger.critical(
            'error while running stresstest',
            exc_info=e, stack_info=True
        )
    finally:
        try:
            try:
                loop.run_until_complete(mutex.tear_down())
            except AssertionError:
                pass
        except BaseException as e:
            _logger.critical(
                'error while destroying the mutex',
                exc_info=e, stack_info=True
            )
            loop.run_until_complete(status_client.call(
                'rp_fail', 'deconstructing failed', e)
            )
            raise
        else:
            loop.run_until_complete(status_client.call('rp_done', pid))
        finally:
            _logger.info('done')
            loop.run_until_complete(status_client.disconnect())

            results = loop.run_until_complete(asyncio.gather(
                *asyncio.Task.all_tasks(), return_exceptions=True
            ))

            for result in results:
                if (isinstance(result, BaseException) and
                        not isinstance(result, AssertionError)):
                    raise result

            loop.stop()
            loop.close()

            log_file.close()

import heapq
import os
import glob
import datetime
import json
import operator
import collections

from .logging import Colors


class Record:
    is_valid = False
    message = extra = location = None
    address = pid = state = logger = level = None
    l_time = -1
    p_time = datetime.datetime(1, 1, 1)
    _parsed_extra = None

    def __init__(self, line):
        self.raw_line = line

        try:
            data = self.parse_line(line)
        except AssertionError:
            self.is_valid = False
        else:
            self.is_valid = True

            head, self.message, self.extra, self.location = data

            self.message = self.message.strip()

            self.address = head[0].strip()
            self.pid = head[1].strip()
            self.p_time = head[2].strip()
            self.l_time = head[3].strip()
            self.state = head[4].strip()
            self.logger = head[5].strip()
            self.level = head[6].strip().upper()

            try:
                self.p_time = datetime.datetime.strptime(
                    self.p_time, '%Y-%m-%d %H:%M:%S.%f'
                )
            except ValueError:
                self.is_valid = False

            try:
                self.l_time = int(self.l_time)
            except ValueError:
                self.is_valid = False

    @property
    def parsed_extra(self):
        if not self.is_valid:
            return None

        if self._parsed_extra is None:
            self._parsed_extra = json.loads(self.extra)

        return self._parsed_extra

    @classmethod
    def parse_line(cls, line):
        data = line.split('\t')
        assert len(data) == 4
        data[0] = cls.parse_head(data[0])
        return data

    @classmethod
    def parse_head(cls, head):
        data = head.split(' / ')
        assert len(data) == 7
        return data


class ClientState:
    def process(self, checker, record):
        raise NotImplementedError


class LastKnownState(ClientState):
    def __init__(self):
        self.state = {}
        self.command = {}
        self.time = {}

    def process(self, checker, record):
        if not record.is_valid:
            return

        if record.pid not in self.state:
            self.state[record.pid] = collections.deque(maxlen=2)
        if record.pid not in self.command:
            self.command[record.pid] = collections.deque(maxlen=2)
        if record.pid not in self.time:
            self.time[record.pid] = collections.deque(maxlen=2)

        self.state[record.pid].append(record.state)
        if record.logger == 'lamport_mutex' and record.message in (
            'released', 'acquiring', 'requests sent', 'acquired', 'releasing',
            'initialized'
        ):
            self.command[record.pid].append(record.message)

        self.time[record.pid].append(record.l_time)


class ReleaseAcquireCount(ClientState):
    def __init__(self):
        self.acquires = collections.defaultdict(int)
        self.releases = collections.defaultdict(int)

    def process(self, checker, record):
        if not record.is_valid:
            return

        if record.logger == 'lamport_mutex' and record.message == 'acquired':
            self.acquires[record.pid] += 1
        if record.logger == 'lamport_mutex' and record.message == 'released':
            self.releases[record.pid] += 1


class Metric:
    name = 'metric'
    ok_color = Colors.Fg.green
    err_color = Colors.Fg.red

    def __init__(self):
        self.errors = []

    def process(self, checker, record):
        raise NotImplementedError

    def report(self, checker, stream, use_colors):
        if not self.errors:
            if use_colors:
                stream.write(self.ok_color)
            stream.write('%r check is ok\n' % self.name)
        else:
            if use_colors:
                stream.write(self.err_color)
            stream.write('%r check has errors\n' % self.name)
            if use_colors:
                stream.write(Colors.reset)

            for message in self.errors[:10]:
                stream.write('  ' + message + '\n')

            if len(self.errors) > 21:
                stream.write(
                    '  (%s more messages suppressed)\n' % (len(self.errors) - 10)
                )
            elif len(self.errors) == 21:
                stream.write('  (1 more message suppressed)\n')

        if use_colors:
            stream.write(Colors.reset)


class ErrorsInLog(Metric):
    name = 'errors in log'

    crits_loggers = set()
    error_loggers = set()
    warning_loggers = set()

    has_invalids = False

    def process(self, checker, record):
        if not record.is_valid:
            self.has_invalids = True
            return

        level = record.level

        if level in ('SUBDEBUG', 'DEBUG', 'SUBINFO', 'INFO'):
            pass
        elif level == 'WARNING':
            self.warning_loggers.add(record.logger)
        elif level == 'ERROR':
            self.error_loggers.add(record.logger)
        elif level == 'CRITICAl':
            self.crits_loggers.add(record.logger)
        else:
            self.errors.append('unknown log level %s' % level)

    def report(self, checker, stream, use_colors):
        errors = []
        if self.has_invalids:
            errors.append('there are records in the log that can\'t be parsed')
        if self.crits_loggers:
            errors.append('%s report critical errors' % ', '.join(self.crits_loggers))
        if self.error_loggers:
            errors.append('%s report errors' % ', '.join(self.error_loggers))
        if self.warning_loggers:
            errors.append('%s report warnings' % ', '.join(self.warning_loggers))
        if errors:
            self.errors = errors + self.errors
        super(ErrorsInLog, self).report(checker, stream, use_colors)


class LogsConsistency(Metric):
    name = 'message / status consistency'

    message_to_state_map = {
        'acquired': 'acquired',
        'releasing': 'releasing',
        'released': 'released',
        'acquiring': 'acquiring',
        'requests sent': 'acquiring',
    }

    def process(self, checker, record):
        if not record.is_valid:
            return

        if record.message in self.message_to_state_map:
            expected = self.message_to_state_map[record.message]
            if expected != record.state:
                self.errors.append(
                    '%s logs %r being in state %r, but state %r is expected' % (
                        record.pid, record.message, record.state, expected
                    )
                )


class LTimeSequenceCorrectness(Metric):
    name = 'events sequence correctness by logical time'

    state_sequence = {
        'released': 'acquiring',
        'acquiring':  'requests sent',
        'requests sent': 'acquired',
        'acquired': 'releasing',
        'releasing': 'released',
        'initialized': 'acquiring',
    }

    def process(self, checker, record):
        if not record.is_valid:
            return

        if record.logger == 'lamport_mutex' and record.message in (
            'released', 'acquiring', 'requests sent', 'acquired', 'releasing'
        ):
            try:
                ls = checker.states['last_known_state'].command[record.pid][0]
            except KeyError:
                self.errors.append(
                    '%s logs %r, but there were no initialization records '
                    'from this process' % (record.pid, record.message)
                )
                return
            if record.message != self.state_sequence[ls]:
                self.errors.append(
                    '%s logs %r after %r, but %r is expected (at %s)' % (
                        record.pid, record.message, ls, self.state_sequence[ls],
                        record.p_time
                    )
                )


class LTimeMonotoneCheck(Metric):
    name = 'logical time is a non-decreasing sequence'

    def process(self, checker, record):
        if not record.is_valid:
            return

        try:
            time = checker.states['last_known_state'].time[record.pid]
        except KeyError:
            self.errors.append(
                '%s logs %r, but there were no initialization records '
                'from this process' % (record.pid, record.message)
            )
            return

        if len(time) >= 2 and time[0] > time[1]:
            self.errors.append('logical time for %s decreased' % record.pid)


class LTimeCorrectnessCheck(Metric):
    name = 'critical sections do not overlap by logical time'

    last_state = (None, -1, 'initialized')

    def process(self, checker, record):
        if not record.is_valid:
            return

        if record.logger == 'lamport_mutex' and record.message in (
            'released', 'acquired'
        ):
            prev_pid, prev_time, prev_state = self.last_state

            if prev_time >= record.l_time or prev_state == record.message:
                self.errors.append(
                    'mutex was %s by %s at %s, and than %s by %s at %s' % (
                        prev_state, prev_pid, prev_time,
                        record.message, record.pid, record.l_time
                    )
                )

            self.last_state = record.pid, record.l_time, record.message


class LTimeScheduleFairness(Metric):
    name = 'request-acquire sequence fairness by logical time'

    def __init__(self):
        super(LTimeScheduleFairness, self).__init__()
        self.requests = collections.deque()

    def process(self, checker, record):
        if not record.is_valid:
            return

        if record.logger == 'lamport_mutex' and record.message == 'acquiring':
            self.requests.append(record.pid)
        if record.logger == 'lamport_mutex' and record.message == 'acquired':
            if not self.requests:
                self.errors.append(
                    '%r acquired the mutex but it has never requested it' % (
                        record.pid
                    )
                )
            pid = self.requests.popleft()
            if pid != record.pid:
                self.errors.append(
                    '%r acquired the mutex out of turn '
                    '(%r should have been acquire it instead)' % (
                        record.pid, pid
                    )
                )


class LockFileCorrectness(Metric):
    name = 'lock file records match logs'

    lock_file = None

    def __init__(self, lock_file):
        super(LockFileCorrectness, self).__init__()
        if lock_file is not None:
            self.lock_file = open(lock_file, 'r')

    def process(self, checker, record):
        if not record.is_valid:
            return

        if self.lock_file is None:
            return

        if record.logger == 'lamport_mutex' and record.message == 'acquired':
            next_line = self.lock_file.readline()
            if not next_line:
                self.errors.append(
                    'mutex locked by %r at %s but there is no record '
                    'about it in the lock file' % (record.pid, record.p_time)
                )
            elif not next_line.startswith('locked by'):
                self.errors.append('the line in the lock file is incorrect')
            else:
                pid = next_line[len('locked by'):].strip()
                if pid != record.pid:
                    self.errors.append(
                        'mutex locked by %r at %s which does not match '
                        'to %r in the lock file' % (
                            record.pid, record.p_time, pid
                        )
                    )


class ReleaseAcquireCountCheck(Metric):
    name = 'acquire / release count is equal'

    def process(self, checker, record):
        pass

    def report(self, checker, stream, use_colors):
        counter = checker.states['release_acquire_count']

        pids = set(counter.acquires.keys()) | set(counter.releases.keys())

        for pid in pids:
            acquires = counter.acquires[pid]
            releases = counter.releases[pid]

            # It is possible that process acquired the mutex and exited
            if acquires - releases not in (0, 1):
                self.errors.append(
                    '%r has %s acquires and %s releases' % (
                        pid, acquires, releases
                    )
                )

        super(ReleaseAcquireCountCheck, self).report(checker, stream, use_colors)


class Fairness(Metric):
    def process(self, checker, record):
        pass

    def report(self, checker, stream, use_colors):
        counter = checker.states['release_acquire_count']
        acquires = counter.acquires.values()

        den = (len(acquires) * sum(x ** 2 for x in acquires))
        fairness = sum(acquires) ** 2 / den if den else 0

        stream.write('fairness is %0.5f\n' % fairness)


class Checker:
    @classmethod
    def ltime_checker(cls, lock_file):
        return cls(
            dict(
                last_known_state=LastKnownState(),
                release_acquire_count=ReleaseAcquireCount(),
            ),
            [
                LogsConsistency(),
                LTimeSequenceCorrectness(),
                ErrorsInLog(),
                LTimeMonotoneCheck(),
                ReleaseAcquireCountCheck(),
                LTimeCorrectnessCheck(),
                LockFileCorrectness(lock_file),
                LTimeScheduleFairness(),
                Fairness(),
            ],
            'l_time'
        )

    def __init__(self, states, metrics, sort_by):
        self.metrics = metrics
        self.states = states
        if callable(sort_by):
            self.sort_by = sort_by
        elif sort_by == 'p_time':
            self.sort_by = operator.attrgetter('p_time')
        elif sort_by == 'l_time':
            self.sort_by = lambda record: (record.l_time, record.pid)
        else:
            raise ValueError(
                'sort_by should be either p_time or l_time, not %r' % sort_by
            )

    def check(self, stream, log_dir):
        assert os.path.isdir(log_dir)

        paths = glob.glob(os.path.join(log_dir, '*.log'))
        files = [open(path, 'r') for path in paths]
        parsed_files = [map(Record, lines) for lines in files]

        if stream:
            stream.write('Running log checker for %s files\n' % len(paths))

        data_stream = heapq.merge(*parsed_files, key=self.sort_by)

        for record in data_stream:
            for state in self.states.values():
                state.process(self, record)
            for metric in self.metrics:
                metric.process(self, record)

    def report(self, stream, use_colors):
        for metric in self.metrics:
            metric.report(self, stream, use_colors)

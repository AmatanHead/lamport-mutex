import logging
import sys
import datetime
import json


SUBDEBUG = 5
SUBINFO = 15


class Colors:
    __init__ = None

    reset = '\033[0m'
    bold = '\033[01m'
    underline = '\033[04m'

    class Fg:
        __init__ = None

        black = '\033[30m'
        red = '\033[31m'
        green = '\033[32m'
        orange = '\033[33m'
        blue = '\033[34m'
        purple = '\033[35m'
        cyan = '\033[36m'
        lightgrey = '\033[37m'
        darkgrey = '\033[90m'
        lightred = '\033[91m'
        lightgreen = '\033[92m'
        yellow = '\033[93m'
        lightblue = '\033[94m'
        pink = '\033[95m'
        lightcyan = '\033[96m'

    class Bg:
        __init__ = None

        black = '\033[40m'
        red = '\033[41m'
        green = '\033[42m'
        orange = '\033[43m'
        blue = '\033[44m'
        purple = '\033[45m'
        cyan = '\033[46m'
        lightgrey = '\033[47m'


class LoggerAdapter(logging.LoggerAdapter):
    """Logging adapter with better method signatures"""

    def __init__(self, logger, extra=None):
        if extra is None:
            extra = {}
        if isinstance(logger, logging.LoggerAdapter):
            extra = dict(logger.extra, **extra)
            super(LoggerAdapter, self).__init__(logger.logger, extra)
        else:
            super(LoggerAdapter, self).__init__(logger, extra)

    def process(self, msg, kwargs):
        kwargs['extra'] = {
            'extra': dict(self.extra, **kwargs.get('extra', {}))
        }
        return msg, kwargs

    def subdebug(self, *args, **kwargs):
        self.log(SUBDEBUG, *args, **kwargs)

    def subinfo(self, *args, **kwargs):
        self.log(SUBINFO, *args, **kwargs)

    def log(self, level, msg, *args, exc_info=None, stack_info=False, **extra):
        super(LoggerAdapter, self).log(
            level, msg, *args,
            exc_info=exc_info, stack_info=stack_info, extra=extra
        )


class Formatter(logging.Formatter):

    head_format = '{host}:{port} / {pid} / {p_time} / {l_time} / ' \
                  '{state} / {name} / {level}'
    line_format = '{head}\t{message}\t{extra}\t{location}'

    log_colors = {
        'lamport_mutex': Colors.Fg.green,
        'lamport_mutex/cli': Colors.Fg.green,
        'lamport_mutex/rpc': Colors.Fg.darkgrey,
        'lamport_mutex/rpc/server': Colors.Fg.darkgrey,
        'lamport_mutex/rpc/client': Colors.Fg.darkgrey,
        'asyncio': Colors.Fg.darkgrey,
    }

    state_colors = {
        'not_initialized': Colors.Fg.orange,
        'initializing': Colors.Fg.yellow,
        'ready': Colors.Fg.green,
        'released': Colors.Fg.purple,
        'acquired': Colors.Fg.purple,
        'terminating': Colors.Fg.orange,
        'terminated': Colors.Fg.red,
    }

    level_colors = {
        SUBDEBUG: Colors.Fg.darkgrey,
        logging.DEBUG: Colors.Fg.darkgrey,
        SUBINFO: Colors.Fg.darkgrey,
        logging.INFO: Colors.reset,
        logging.WARNING: Colors.Fg.yellow,
        logging.ERROR: Colors.Fg.red,
        logging.CRITICAL: Colors.Bg.red + Colors.Fg.black,
    }

    level_names = {
        SUBDEBUG: 'SUBDEBUG',
        SUBINFO: 'SUBINFO',
    }

    def __init__(self, mutex=None, host='NA', port='NA', use_colors=True):
        super(Formatter, self).__init__()

        self._mutex = mutex
        self._host = host
        self._port = port
        self._use_colors = use_colors

    def apply_color(self, data):
        name_color = self.log_colors.get(data['name'].strip(), Colors.reset)
        level_color = self.level_colors.get(data['level_no'], Colors.reset)
        state_color = self.state_colors.get(data['state'].strip(), Colors.reset)

        data['name'] = name_color + data['name'] + Colors.reset
        data['level'] = level_color + data['level'] + Colors.reset
        data['level_no'] = level_color + str(data['level_no']) + Colors.reset
        data['message'] = level_color + data['message'] + Colors.reset
        data['state'] = state_color + data['state'] + Colors.reset
        data['extra'] = Colors.Fg.darkgrey + data['extra'] + Colors.reset
        data['location'] = Colors.Fg.darkgrey + data['location'] + Colors.reset

    def format(self, record):
        extra = getattr(record, 'extra', {})

        if hasattr(record, 'exc_info') and record.exc_info:
            extra['exc_info'] = self.formatException(record.exc_info)

        if hasattr(record, 'exc_text') and record.exc_text:
            extra['exc_text'] = str(record.exc_text)

        if hasattr(record, 'stack_info') and record.stack_info:
            extra['stack_info'] = self.formatStack(record.stack_info)

        level_no = int(record.levelno)

        state = self._mutex.state_str if self._mutex is not None else 'NA'

        data = vars(record)
        data.update(dict(
            extra=json.dumps(extra, separators=(',', ':')),
            level_no=level_no,

            p_time=str(datetime.datetime.fromtimestamp(record.created)),
            l_time=self._mutex.time if self._mutex is not None else 0,

            state=state.ljust(15) if state != 'NA' else state,

            host=self._host,
            port=self._port,

            pid=record.process,

            level=self.level_names.get(level_no, record.levelname),

            name=record.name.ljust(24),

            message=record.getMessage(),

            location='{}:{}'.format(record.filename, record.lineno),
        ))

        if self._use_colors:
            self.apply_color(data)

        head = self.head_format.format(**data)

        data['head'] = head

        line = self.line_format.format(**data)

        return line


def setup_logging(mutex=None,
                  host='NA', port='NA',
                  stream=sys.stderr,
                  level=logging.INFO,
                  use_colors=True,
                  logger_names=None):
    """Configure loggers to work with log analyser specific format

    :param mutex: each record will be annotated with data from this mutex;
        its logical time and its state
    :param host: host of the main rpc server
    :param port: port of the main rpc server
    :param stream: where to write logs
    :param level:
    :param use_colors: if True, all records will be colored
        using ANSI escape codes
    :param logger_names: list of loggers to setup
    """

    if logger_names is None:
        logger_names = [
            'lamport_mutex',
            'lamport_mutex/cli',
            'lamport_mutex/rpc',
            'lamport_mutex/rpc/server',
            'lamport_mutex/rpc/client',
            'asyncio',
        ]

    for logger_name in logger_names:
        logger = logging.getLogger(logger_name)

        if logger.handlers:
            continue

        handler = logging.StreamHandler(stream)
        handler.setFormatter(Formatter(
            mutex=mutex, host=host, port=port, use_colors=use_colors
        ))

        logger.setLevel(level)
        logger.addHandler(handler)


class FormatCall:
    def __init__(self, name, args, kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        s = []
        if self.args:
            s += [', '.join(map(repr, self.args))]
        if self.kwargs:
            s += [', '.join(map(lambda x: '%s=%r' % x, self.kwargs.items()))]

        return '%s(%s)' % (self.name, ', '.join(s))

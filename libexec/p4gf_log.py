#! /usr/bin/env python3.3
"""Utilities for configuring Git Fusion's debug/error/audit log."""

import configparser
from contextlib import contextmanager, ExitStack
import datetime
import fcntl
import inspect
import gzip
import io
import logging
import logging.handlers
import os
import re
import resource
import shutil
import socket
import sys
import syslog
import tempfile
import time
import uuid
import xml.sax.saxutils

import p4gf_bootstrap  # pylint: disable=unused-import
import p4gf_const
from   p4gf_ensure_dir import ensure_parent_dir
from   p4gf_l10n      import _, NTR
import p4gf_util

_config_filename_default    = '/etc/git-fusion.log.conf'
_config_filename_repo_list  = ['{P4GF_HOME}/log.d/{repo}',
                               '/etc/git-fusion.log.d/{repo}']
_configured_path            = None
_general_section            = NTR('general')
_audit_section              = NTR('audit')
_auth_keys_section          = NTR('auth-keys')
_syslog_ident               = NTR('git-fusion')
_syslog_audit_ident         = NTR('git-fusion-auth')
_syslog_auth_keys_ident     = NTR('git-fusion-auth-keys')
_memory_usage               = False
_audit_logger_name          = NTR('audit')
_auth_keys_logger_name      = NTR('auth-keys')
_max_size_mb_name           = NTR('max-size-mb')
_retain_count_name          = NTR('retain-count')
_ssh_params = [
    'SSH_ASKPASS',
    'SSH_ORIGINAL_COMMAND',
    'SSH_CLIENT',
    'SSH_CONNECTION'
]
_http_params = [
    'PATH_INFO',
    'QUERY_STRING',
    'REMOTE_ADDR',
    'SERVER_ADDR',
    'REMOTE_USER',
    'REQUEST_METHOD'
]
XML_FORMAT = "<rec><pid>{process}</pid><req>{request}</req><dt>{time}</dt>" \
             "<lvl>{level}</lvl><nm>{name}</nm><msg>{message}</msg></rec>"
XML_DATEFMT = "%Y-%m-%d %H:%M:%S"

# DEFAULT_LOG_FILES maps sections to possible default log file names, if
# such a default was created for that section. Reconfiguring the logging
# for a repo will rename the given log file to include the repository name
# and user.
DEFAULT_LOG_FILES = dict()

# REPO_LOG_FILES is used to indicate that a log configuration includes the
# use of %(repos)s in the filename configurable, and thus the logger should
# be reconfigured once the repository name is known. Entries are section
# names (e.g. "general"). This avoids reading the configuration file
# needlessly, albeit at the cost of yet another global variable.
REPO_LOG_FILES = set()


def _find_repo_config_file(repo_name):
    """Return path to existing per-repo config file, or None if not found.

    Also allows for repo and user specific configuration.

    """
    assert repo_name
    user_name = os.environ.get(p4gf_const.P4GF_AUTH_P4USER)

    for template in _config_filename_repo_list:
        repo_config_path = template.format(
            P4GF_HOME=p4gf_const.P4GF_HOME, repo=repo_name)
        if user_name:
            user_config_path = os.path.join(repo_config_path, user_name + '.conf')
            if os.path.exists(user_config_path):
                return user_config_path
        repo_config_path = repo_config_path + '.conf'
        if os.path.exists(repo_config_path):
            return repo_config_path
    return None


def _find_global_config_file():
    """Return path to existing global log config file, None if no config file found.

    Returns "/etc/git-fusion.log.conf" unless a test hook has overridden
    with environment variable P4GF_LOG_CONFIG_FILE.
    """
    # Check test-imposed environment var P4GF_LOG_CONFIG_FILE.
    if p4gf_const.P4GF_TEST_LOG_CONFIG_PATH in os.environ:
        path = os.environ[p4gf_const.P4GF_TEST_LOG_CONFIG_PATH]
        if os.path.exists(path):
            return path

    # Check /etc/git-fusion.log.conf .
    if os.path.exists(_config_filename_default):
        return _config_filename_default

    return None


class LongWaitReporter:

    """Keeps track of the duration of a waiting process, logging occassionally."""

    def __init__(self, label, logger, wait_time=60):
        """Construct a new instance with the given label and logger.

        :param label:     included in the log message.
        :param logger:    the logging instance with which to report.
        :param wait_time: how often (in seconds) to report wait.

        """
        self.label = label
        self.logger = logger
        self.last_time = time.time()
        self.wait_time = wait_time

    def been_waiting(self):
        """If it has been a while, report that we have been waiting."""
        new_time = time.time()
        if new_time - self.last_time > self.wait_time:
            self.last_time = new_time
            pid = os.getpid()
            self.logger.warning("{} has been waiting for {}".format(pid, self.label))


class BaseFormatter(logging.Formatter):

    """Formatter that includes exception and stack info, if needed."""

    def __init__(self, fmt=None, datefmt=None):
        """Initialize the log formatter."""
        logging.Formatter.__init__(self, fmt, datefmt)

    def formatMessage(self, record):
        """Format the record as a logging message."""
        # Include the request identifier to make tracking the processing of
        # a pull or push operation easier when moving across processes.
        setattr(record, 'requestId', _get_log_uuid())
        return super(BaseFormatter, self).formatMessage(record)

    def include_extra(self, record, msg):
        """Format the exception and stack info, if available.

        :param record: logging record with exception and stack info.
        :param msg: formatted logging message.

        :return: updated logging message, including exception, stack.

        """
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if msg[-1:] != "\n":
                msg = msg + "\n"
            msg = msg + record.exc_text
        if record.stack_info:
            if msg[-1:] != "\n":
                msg = msg + "\n"
            msg = msg + self.formatStack(record.stack_info)
        return msg


class XmlFormatter(BaseFormatter):

    """A formatter for producing XML.

    This formatter ignores the format and datefmt settings.

    """

    def __init__(self):
        """Initialize the log formatter."""
        BaseFormatter.__init__(self)

    def format(self, record):
        """Format the record as XML."""
        #
        # Because we need absolute control over the entire output of this
        # formatter, we override format(), rather than formatMessage(). As
        # a result, we need to also handle the exception and stack
        # information attributes ourselves (i.e. include_extra()).
        #
        pid = record.process
        lt = time.localtime(record.created)
        dt = time.strftime(XML_DATEFMT, lt)
        lvl = record.levelname
        nm = record.name
        req = _get_log_uuid()
        msg = xml.sax.saxutils.escape(self.include_extra(record, record.getMessage()))
        return XML_FORMAT.format(process=pid, time=dt, level=lvl, name=nm, message=msg,
                                 request=req)


class P4GFSysLogFormatter(BaseFormatter):

    """A formatter for SysLogHandler that inserts category and level into the message."""

    def __init__(self, fmt=None, datefmt=None):
        """Initialize the log formatter."""
        BaseFormatter.__init__(self, fmt, datefmt)

    def format(self, record):
        """Prepend category and level."""
        #
        # Because we need absolute control over the entire output of this
        # formatter, we override format(), rather than formatMessage(). As
        # a result, we need to also handle the exception and stack
        # information attributes ourselves (i.e. include_extra()).
        #
        request_id = _get_log_uuid()
        msg = record.getMessage()
        msg = self.include_extra(record, msg)
        return (NTR("|{req}| {name} {level} {message}").format(
            req=request_id, name=record.name, level=record.levelname, message=msg))


class P4GFSysLogHandler(logging.handlers.SysLogHandler):

    """A SysLogHandler that knows to include an ident string properly.

    The implementation in Python (as recent as 3.3.2) does not use
    the correct syslog API and as such is formatted incorrectly.

    """

    def __init__(self,
                 address=(NTR('localhost'), logging.handlers.SYSLOG_UDP_PORT),
                 facility=syslog.LOG_USER,
                 socktype=socket.SOCK_DGRAM,
                 ident=None):
        """Initialize the log handler."""
        logging.handlers.SysLogHandler.__init__(self, address, facility, socktype)
        self.ident = ident if ident else _syslog_ident

    def emit(self, record):
        """Send a log record to the syslog service."""
        msg = self.format(record)
        syspri = self.mapPriority(record.levelname)
        # encodePriority() expects 1 for "user", shifts it to 8. but
        # syslog.LOG_USER is ALREADY shifted to 8, passing it to
        # encodePriority shifts it again to 64. No. Pass 0 for facility, then
        # do our own bitwise or.
        pri = self.encodePriority(0, syspri) | self.facility

        # Point syslog at our file. Syslog module remains pointed at our
        # log file until any other call to syslog.openlog(), such as those
        # in p4gf_audit_log.py.
        syslog.openlog(self.ident, syslog.LOG_PID)
        syslog.syslog(pri, msg)


def _get_log_uuid():
    """Retrieve the UUID for this request, or generate a new one."""
    if p4gf_const.P4GF_LOG_UUID in os.environ:
        result = os.environ[p4gf_const.P4GF_LOG_UUID]
    else:
        result = str(uuid.uuid1())
        os.environ[p4gf_const.P4GF_LOG_UUID] = result
    return result


def _generate_default_name(section):
    """Generate the default name of the log file.

    :param str section: name of logging configuration section.

    """
    #
    # When not logging to syslog or a specifically named file (which is a
    # bad idea anyway, see GF-2729), then use separate files for each
    # process. Try to incorporate the repo and user names, if available.
    # Otherwise generate a UUID in place of those. Include the date/time in
    # such a way as to make perusing the log files a little easier.
    #
    if p4gf_const.P4GF_LOG_REPO in os.environ:
        repo_name = os.environ[p4gf_const.P4GF_LOG_REPO]
        user_name = os.environ[p4gf_const.P4GF_AUTH_P4USER]
        middle = repo_name + '-' + user_name
    else:
        middle = _get_log_uuid()
    date_str = time.strftime("%Y-%m-%d-%H%M%S")
    fname = "{}_{}_{}_log.xml".format(date_str, middle, os.getpid())
    # P4GF_LOGS_DIR: admins may set up a different log dir.
    # configure-git-fusion.sh will set /fs/... path if configuring the OVA
    # Typically this will be set in p4gf_environment.cfg
    if p4gf_const.P4GF_LOGS_DIR in os.environ:
        log_dir = os.environ[p4gf_const.P4GF_LOGS_DIR]
    else:
        log_dir = os.path.join(p4gf_const.P4GF_HOME, "logs")
    fpath = os.path.join(log_dir, fname)
    # If we were using a default log file, rename it now.
    if section in DEFAULT_LOG_FILES and DEFAULT_LOG_FILES[section] != fpath:
        try:
            shutil.move(DEFAULT_LOG_FILES[section], fpath)
        except FileNotFoundError:
            # Seems that os.path.exists() does not tell the truth.
            pass
    DEFAULT_LOG_FILES[section] = fpath
    return fpath


def _effective_config(parser, section, defaults):
    """Build the effective configuration for a logger.

    Uses a combination of the configparser instance and default options.
    Returns a dict with only the relevant settings for configuring a Logger
    instance.

    It is here the 'handler' over 'filename' and other such precedence
    rules are enforced.

    :param parser: instance of ConfigParser providing configuration.
    :param section: section name from which to take logging configuration.
    :param defaults: dict of default settings.

    """
    assert 'file' not in defaults
    config = defaults.copy()
    fallback = parser.defaults()
    if parser.has_section(section):
        fallback = parser[section]
    config.update(fallback)
    # Allow configuration 'file' setting to take precedence over 'filename'
    # since it is not one of our defaults.
    if 'file' in config:
        config['filename'] = config.pop('file')
    if 'handler' in config:
        val = config['handler']
        if val.startswith('syslog'):
            # Logging to syslog means no format support.
            config.pop('format', None)
            config.pop('datefmt', None)
        # Logging to a handler means no filename
        config.pop('filename', None)
    elif 'filename' in config:
        # perform variable substitution on file path
        fnargs = {}
        fnargs['user'] = os.path.expanduser('~')
        fnargs['tmp'] = tempfile.gettempdir()
        if '%(repo)s' in config['filename']:
            fnargs['repo'] = os.environ.get(p4gf_const.P4GF_LOG_REPO, NTR("norepo"))
            REPO_LOG_FILES.add(section)
        config['filename'] %= fnargs
    else:
        # default for these is syslog - rather than xml file
        if section in [_auth_keys_section, _audit_section]:
            config['handler'] = NTR('syslog')
        else:
            fpath = _generate_default_name(section)
            config['filename'] = fpath
    config.setdefault(NTR('format'), logging.BASIC_FORMAT)
    config.setdefault(NTR('datefmt'), None)
    return config


def _include_process_id(config, fs):
    """Ensure the process identifier is included in the format, unless prohibited.

    :param config: parsed logging configuration.
    :param str fs: original format string.
    :return: format string, with possible modifications.

    """
    # Add the process id to the format unless 'log-process' is 'no'.
    # However an explicitly configured process in format will not be
    # removed if log-process = no.
    log_process = config.pop('log-process', 'yes')
    log_process = False if log_process in ('no', 'No', 'false', 'False') else True
    if (not fs or 'process' not in fs) and log_process:
        if fs:
            fs = '%(process)d ' + fs
        else:
            fs = '%(process)d'
    return fs


def _deconfigure_logger(logger_name):
    """If the given logger has a handler, remove it."""
    logger = logging.getLogger(logger_name)
    # [:] to copy the list so it survives element deletion.
    for h in logger.handlers[:]:
        logger.removeHandler(h)


def _configure_logger(config, section, name=None, ident=None):
    """Configure the named logger (or the root logger if name is None).

    Use provided settings, which likely came from _effective_config().

    :param dict config: logger settings (will be modified).
    :param str section: name of logging section (e.g. general).
    :param str name: name of the logger to configure (defaults to root logger).
    :param str ident: syslog identity, if handler is 'syslog'.

    """
    # pylint: disable=too-many-branches
    _deconfigure_logger(name)
    formatter = None
    if 'handler' in config:
        val = config.pop('handler')
        if val.startswith('syslog'):
            words = val.split(maxsplit=1)
            if len(words) > 1:
                handler = P4GFSysLogHandler(address=words[1], ident=ident)
            else:
                handler = P4GFSysLogHandler(ident=ident)
            formatter = P4GFSysLogFormatter()
        elif val == 'console':
            handler = logging.StreamHandler()
        else:
            sys.stderr.write(_('Git Fusion: unrecognized log handler: {}\n').format(val))
            handler = logging.StreamHandler()
    elif 'filename' in config:
        fpath = config.pop('filename')
        p4gf_util.ensure_parent_dir(fpath)
        handler = logging.FileHandler(fpath, 'a', 'utf-8')
        if fpath.endswith('.xml'):
            formatter = XmlFormatter()
        _rotate_log_file(fpath, section, config)
    else:
        handler = logging.StreamHandler()
    # Always remove these fake logging levels
    fs = config.pop('format', None)
    dfs = config.pop('datefmt', None)
    config.pop(_max_size_mb_name, None)
    config.pop(_retain_count_name, None)
    fs = _include_process_id(config, fs)
    if formatter is None:
        # Build the formatter if one has not already been.
        formatter = BaseFormatter(fs, dfs)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.addHandler(handler)
    logger.setLevel(config.pop('root').upper())

    # Set the logging levels based on the remaining settings
    for key, val in config.items():
        logging.getLogger(key).setLevel(val.upper())


def _read_configuration(filename):
    """Attempt to read the log configuration using the "new" format.

    Massage from the old format into the new as needed (basically
    prepend a section header).

    Return an instance of ConfigParser.
    """
    # Note that we do not make our 'general' section the default since
    # that requires an entirely different API to work with.
    parser = configparser.ConfigParser(interpolation=None)
    with open(filename, 'r') as f:
        text = f.read()
    try:
        try:
            parser.read_string(text, source=filename)
        except configparser.MissingSectionHeaderError:
            text = '[{}]\n{}'.format(_general_section, text)
            parser.read_string(text, source=filename)
    except configparser.Error as e:
        sys.stderr.write(_('Git Fusion: log configuration error, using defaults: {exception}\n')
                         .format(exception=e))
        parser = configparser.ConfigParser()
    return parser


def _apply_default_config(parser):
    """Given a ConfigParser instance, merge with the default logging settings.

    Produce the effective logging configuration and return as a tuple of
    the general ,audit , and auth_keys settings.
    """
    # Configure the general logging
    general_config = NTR({
        # New default is to write to separate files (GF-2729).
        # 'filename': os.environ['HOME'] + '/p4gf_log.txt',
        'format':   '%(asctime)s %(name)-10s %(levelname)-8s %(message)s',
        'datefmt':  '%m-%d %H:%M:%S',
        'root':     'WARNING',
    })
    general_config = _effective_config(parser, _general_section, general_config)

    # Configure the audit logging (defaults to standard syslog)
    audit_config = {'root': NTR('warning')}
    audit_config = _effective_config(parser, _audit_section, audit_config)
    if not ('filename' in audit_config or 'handler' in audit_config):
        audit_config['handler'] = NTR('syslog')

    # Configure the authorized_keys logging (defaults to standard syslog)
    auth_keys_config = {'root': NTR('warning')}
    auth_keys_config = _effective_config(parser, _auth_keys_section, auth_keys_config)
    if not ('filename' in auth_keys_config or 'handler' in auth_keys_config):
        auth_keys_config['handler'] = NTR('syslog')
    return (general_config, audit_config, auth_keys_config)


def _script_name():
    """Return the 'p4gf_xxx' portion of argv[0] suitable for use as a log category."""
    return sys.argv[0].split('/')[-1]


class ExceptionLogger:

    """A handler that records all exceptions to log instead of to console.

    with p4gf_log.ExceptionLogger() as dont_care:
        ... your code that can raise exceptions...
    """

    def __init__(self, exit_code_array=None, category=_script_name(),
                 squelch=True, write_to_stderr_=False, squelch_exceptions=None):
        """Initialize a logger.

        category, if specified, controls where exceptions go if caught.
        squelch controls the return value of __exit__, which in turn
        controls what happens after reporting a caught exception:

        squelch = True: squelch the exception.
            This is what we want if we don't want this exception
            propagating to console. Unfortunately this also makes it
            harder for main() to know if we *did* throw+report+squelch
            an exception.

        squelch = False: propagate the exception.
            This usually results in dump to console, followed by the
            death of your program.

        squelch_exceptions: optional list of exception types which should
            not be logged.

        """
        # pylint:disable=too-many-arguments
        self.__category__ = category
        self.__squelch__ = squelch
        self.__write_to_stderr__ = write_to_stderr_
        self.__squelch_exceptions__ = squelch_exceptions or []
        if exit_code_array:
            self.__exit_code_array__ = exit_code_array
        else:
            self.__exit_code_array__ = [1]
        _lazy_init()

    def __enter__(self):
        """Enter the context."""
        return None

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Record any exception to log. NOP if no exception."""
        # Someone called sys.exit(x). Retain the exit code.
        if isinstance(exc_value, SystemExit):
            self.__exit_code_array__[0] = exc_value.code
            return self.__squelch__

        if exc_type:
            if exc_type not in self.__squelch_exceptions__:
                log = logging.getLogger(self.__category__)
                log.error("Caught exception", exc_info=(exc_type, exc_value, exc_traceback))
            if self.__write_to_stderr__:
                val = exc_value.args[0] if exc_value.args else exc_value
                sys.stderr.write('{}\n'.format(val))

        return self.__squelch__


def caller(depth=1):
    """Return a dict for the caller N frames up the stack."""
    stack = inspect.stack()
    if len(stack) <= depth:
        assert len(stack) > 0
        depth = 1
    frame = stack[depth]
    fname = os.path.basename(frame[1])
    frame_dict = {
        'file':     fname,
        'filepath': frame[1],
        'filebase': os.path.splitext(fname)[0],
        'line':     frame[2],
        'func':     frame[3],
    }
    # Internally sever link to Traceback frame in an attempt to avoid
    # module 'inspect' and its refcount cycles.
    del frame
    return frame_dict


def run_with_exception_logger(func, *args, write_to_stderr=False, squelch_exceptions=None):
    """Wrapper for most 'main' callers, route all exceptions to log."""
    exit_code = [1]
    c = None
    log = None
    with ExceptionLogger(exit_code, write_to_stderr_=write_to_stderr,
                         squelch_exceptions=squelch_exceptions):
        c = caller(depth=2)
        log = logging.getLogger(c['filebase'])
        log.debug("{file}:{line} start --".format(file=c['file'],
                                                  line=c['line']))
        prof_log = logging.getLogger('p4gf_profiling')
        run_with_profiling = prof_log.isEnabledFor(logging.DEBUG3)
        if run_with_profiling:
            # Run the function using the Python profiler and dump the
            # profiling statistics to the log.
            try:
                import cProfile
                prof = cProfile.Profile()
                prof.enable()
            except ImportError:
                log.warning('cProfile not available on this system, profiling disabled')
                run_with_profiling = False
        exit_code[0] = func(*args)
        if run_with_profiling:
            prof.disable()
            buff = io.StringIO()
            import pstats
            ps = pstats.Stats(prof, stream=buff)
            ps.sort_stats(NTR('cumulative'))
            ps.print_stats(100)
            ps.print_callees(100)
            prof_log.debug3("Profile stats for {}:\n{}".format(c['file'], buff.getvalue()))
            buff.close()

    if log and c:
        log.debug("{file}:{line} exit={code} --".format(code=exit_code[0],
                                                        file=c['file'],
                                                        line=c['line']))

    if log and _memory_usage:
        log.warning(memory_usage())

    sys.exit(exit_code[0])


def memory_usage():
    """Format a string that indicates the memory usage of the current process."""
    r = resource.getrusage(resource.RUSAGE_SELF)
    # Linux seems to report in KB while Mac uses bytes.
    factor = 20 if os.uname()[0] == "Darwin" else 10
    mem = r.ru_maxrss / (2 ** factor)
    return NTR('memory usage (maxrss): {: >8.2f} MB').format(mem)


def _print_config(label, config):
    """Print the sorted entries of the config.

    Precede with the label, printed in square brackets ([]) as if a section
    header.
    """
    options = sorted(config.keys())
    print("[{0}]".format(label))
    for opt in options:
        print("{0} = {1}".format(opt, config[opt]))


def configure_for_repo(repo_name):
    """Reconfigure logging now that we have the repository name.

    Use ~/.git-fusion/log.d/{repo} as the logging configuration, if such a
    file exists. Otherwise, the usual configuration will be used.

    If using the default log file, it will be renamed to incorporate the
    repository name, as well as the user name.

    """
    custom_config_path = _find_repo_config_file(repo_name)
    if custom_config_path:
        _lazy_init(config_path=custom_config_path)
    elif _general_section in DEFAULT_LOG_FILES or REPO_LOG_FILES:
        # Hack to get the logging to use a better file name. This may
        # happen when we are using the default logging configuration, or
        # when the filename configurable contains the %(repo)s format
        # string.
        _deconfigure_logger(None)
        # simplify names like "@wait@p4gf_repo@2" and "@wait@p4gf_repo"
        if '@' in repo_name:
            name_idx = repo_name.count('@') - 1
            repo_name = repo_name.split('@')[-name_idx]
        os.environ[p4gf_const.P4GF_LOG_REPO] = repo_name
        global _configured_path
        _configured_path = None
        _lazy_init()


def reset():
    """When a child process is started, this method should be called.

    This is particularly true when using multiprocesssing.

    """
    DEFAULT_LOG_FILES.clear()
    REPO_LOG_FILES.clear()
    _lazy_init()


def _lazy_init(debug=False, config_path=None):
    """Configure logging system if not yet done.

    Use a default set of configuration settings.
    """
    # Re-init if not (yet) inited for requested repo, even if we did a global
    # init earlier (probably from p4gf_env_config at bootstrap time).
    global _configured_path
    if (not _configured_path) or (config_path and _configured_path != config_path):
        try:
            if config_path:
                config_file_path = config_path
            else:
                config_file_path = _find_global_config_file()
            if config_file_path:
                parser = _read_configuration(config_file_path)
            else:
                parser = configparser.ConfigParser()
            general, audit, auth_keys = _apply_default_config(parser)
            if debug:
                _print_config(_general_section, general)
                _print_config(_audit_section, audit)
                _print_config(_auth_keys_section, auth_keys)
            _configure_logger(general, _general_section, ident=_syslog_ident)
            _configure_logger(audit, _audit_section, _audit_logger_name, _syslog_audit_ident)
            _configure_logger(auth_keys, _auth_keys_section, _auth_keys_logger_name,
                              _syslog_auth_keys_ident)
            _configured_path = config_path
        except Exception:   # pylint: disable=broad-except
            # import traceback
            # sys.stderr.write(''.join(traceback.format_exc()))
            # Unable to open log file for write? Some other random error?
            # Printf and squelch.
            sys.stderr.write(_('Git Fusion: Unable to configure log.\n'))


def _get_int_from_dict(map_, key, default):
    """Retrieve the named integer value from the dict.

    :param dict map: the dictionary
    :param str key: the key of the element
    :param int default: default value if missing or not an int
    :return: integer value

    """
    value = map_.get(key, default)
    try:
        value = int(value)
    except ValueError:
        value = default
    return value


def _rotate_log_file(fname, section, config):
    """If logging to a file, consider rotating it as appropriate.

    :param str fname: name of log file to be rotated.
    :param str section: name of the logging "section" (e.g. "general").
    :param dict config: logging configuration for named section.

    """
    if section in DEFAULT_LOG_FILES:
        # For default log file handling, rotating does not make sense.
        return
    if not (_max_size_mb_name in config or _retain_count_name in config):
        # If not rotating the logs, do nothing.
        return
    if os.path.exists(fname):
        # Use sensible defaults for the values, in case they cannot be parsed.
        # 32*16 is 512mb, which is a lot, so should be reasonably sufficient.
        file_size_mb = os.stat(fname).st_size / 1048576
        size_limit_mb = _get_int_from_dict(config, _max_size_mb_name, 32)
        if file_size_mb > size_limit_mb:
            retain_count = _get_int_from_dict(config, _retain_count_name, 16)
            with _log_file_lock(section):
                # remove the rotated file that is at the outer limit
                oldest_fname = fname + '.' + str(retain_count)
                if os.path.exists(oldest_fname):
                    os.unlink(oldest_fname)
                # rotate the old log files (4 -> 5, 3 -> 4, 2 -> 3...)
                for offset in range(retain_count, 1, -1):
                    fname_2 = fname + '.' + str(offset)
                    fname_1 = fname + '.' + str(offset - 1)
                    if os.path.exists(fname_1):
                        os.rename(fname_1, fname_2)
                # rename current log file to have a '.1' extension
                os.rename(fname, fname + '.1')


@contextmanager
def _log_file_lock(name):
    """Acquire exclusive access to the lock for rotating the logs.

    :param str name: name of the logging section (e.g. general)

    """
    path = "{home}/locks/log_{name}.lock".format(home=p4gf_const.P4GF_HOME, name=name)
    p4gf_util.ensure_parent_dir(path)
    fd = os.open(path, os.O_CREAT | os.O_WRONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)
    os.write(fd, bytes(str(os.getpid()), "utf-8"))
    try:
        yield
    finally:
        os.close(fd)


def create_failure_file(prefix=''):
    """Create file P4GF_HOME/logs/2013-07-31T164804.log.txt.

    Attach it to logging category 'failures'.

    Causes all future logs to 'failures' to tee into this file as well as
    wherever the usual debug log goes.

    Each failure deserves its own timestamped file (unless you manage to fail
    twice in the same second, in which case, good on ya, you can have both
    failures in a single file).

    NOP if there's already a handler attached to 'failure': we've probably
    already called create_failure_file().
    """
    logger = logging.getLogger('failures')
    if logger.handlers:
        return

    p4gf_dir = p4gf_const.P4GF_HOME

    date = datetime.datetime.now()
    date_str = date.isoformat().replace(':', '').split('.')[0]
    file_path = p4gf_const.P4GF_FAILURE_LOG.format(
         P4GF_DIR=p4gf_dir, prefix=prefix, date=date_str)
    ensure_parent_dir(file_path)
    logger.addHandler(logging.FileHandler(file_path, encoding='utf8'))
    logger.setLevel(logging.ERROR)
    logger.error('Recording Git Fusion failure log to {}'.format(file_path))


def close_failure_file():
    """If we have a failure log attached to category 'failures', remove it.

    Close the file and compress it.
    """
    logger = logging.getLogger('failures')
    if not logger.handlers:
        return
    handler = logger.handlers[0]
    logger.removeHandler(handler)
    handler.close()

    file_path = handler.baseFilename
    gz_path = handler.baseFilename + '.gz'
    logger.error('Compressing log report to {}'.format(gz_path))

    with ExitStack() as stack:
        fin = stack.enter_context(open(file_path, 'rb'))
        fout = stack.enter_context(gzip.open(gz_path, 'wb'))
        while True:
            b = fin.read(100 * 1024)
            if not len(b):
                break
            fout.write(b)
        fout.flush()

    os.remove(file_path)


def _add_logging_extra(args):
    """Return dict containing some extra logging arguments."""
    epoch = int(time.time())
    server_id = p4gf_util.read_server_id_from_file() or 'no-server-id'
    # add defaults for anything that's missing
    return dict({
        'epoch': epoch,
        'serverId': server_id,
        'userName': 'no-user-name',
        'serverIp': 'no-server-ip',
        'clientIp': 'no-client-ip',
        'command': 'no-command',
        'repo': 'no-repo'
        }, **args)


def get_auth_keys_logger():
    """Return the p4gf_auth_update_authorized_keys logger."""
    return logging.getLogger(_auth_keys_logger_name)


def _extract_extra_data(args, environ=None):
    """Use environment variables to collect extra data for message formatting.

    :param dict args: details from protocol handler.
    :return: extra data for log message formatting.
    :rtype: dict

    """
    if environ is None:
        environ = os.environ
    if 'REMOTE_ADDR' in environ:
        http_env = _http_env_vars(environ)
        extra = _add_extra_http_data(http_env, args)
    else:
        ssh_env = _ssh_env_vars(environ)
        extra = _add_extra_ssh_data(ssh_env, args)
    extra = _add_logging_extra(extra)
    return extra


def _ssh_env_vars(environ):
    """Return the SSH specific environment settings.

    :param dict environ: all known environment variables.
    :return: SSH specific environment variables.
    :rtype: dict

    """
    return {k: v for k, v in environ.items() if k in _ssh_params}


def _http_env_vars(environ):
    """Return the HTTP specific environment settings.

    :param dict environ: all known environment variables.
    :return: HTTP specific environment variables.
    :rtype: dict

    """
    return {k: v for k, v in environ.items() if k in _http_params}


def _add_extra_ssh_data(ssh_env, args):
    """Generate an 'extra' dict based on the SSH connection details.

    :param dict ssh_env: SSH specific environment settings.
    :param dict args: details from protocol handler.
    :return: the given ``args`` with the addition of more details.
    :rtype: dict

    """
    extra = {}
    extra.update(args)
    if 'SSH_CONNECTION' in ssh_env:
        connection = ssh_env['SSH_CONNECTION'].split(' ')
        extra['clientIp'] = connection[0]
        extra['serverIp'] = connection[2]
    return extra


def _add_extra_http_data(environ, args):
    """Generate an 'extra' dict based on the HTTP connection details.

    :param dict environ: environment settings from HTTP handler.
    :param dict args: details from protocol handler.
    :return: collection of HTTP related details for logging.
    :rtype: dict

    """
    extra = {}
    extra.update(args)
    if 'REMOTE_USER' in environ:
        extra['userName'] = environ['REMOTE_USER']
    if 'REMOTE_ADDR' in environ:
        extra['clientIp'] = environ['REMOTE_ADDR']
    if 'SERVER_ADDR' in environ:
        extra['serverIp'] = environ['SERVER_ADDR']
    if environ.get('REQUEST_METHOD') == 'POST':
        m = re.match(r"/([^/]+)/(git-upload-pack|git-receive-pack)",
                     environ['PATH_INFO'])
        if m:
            extra['repo'] = m.group(1)
            extra['command'] = m.group(2)
    else:
        m = re.match("service=(.*)", environ.get('QUERY_STRING', ''))
        if m:
            extra['command'] = m.group(1)
        m = re.match(r"/([^/]+)/(info/refs|HEAD)", environ.get('PATH_INFO', ''))
        if m:
            extra['repo'] = m.group(1)
    return extra


def record_error(line, args, environ=None):
    """Write a line of text to audit log, at priority level 'error'.

    :param str line: message to write to audit log.
    :param dict args: additional details (e.g. user name) for logging.

    """
    # Collect any available information so the error reporting has the
    # details relevant to the request (e.g. client IP, user name).
    if environ is None:
        environ = os.environ
    extra = _extract_extra_data(args, environ)
    logging.getLogger(_audit_logger_name).error(line, extra=_add_logging_extra(extra))


def record_argv(args):
    """Write entire argv and SSH* environment variables to audit log.

    :param dict args: additional details (e.g. user name) for logging.

    """
    ssh_env = _ssh_env_vars(os.environ)
    parts = sys.argv[:]
    parts += ["{}={}".format(k, v) for k, v in ssh_env.items()]
    line = " ".join(parts)
    extra = _add_extra_ssh_data(ssh_env, args)
    extra = _add_logging_extra(extra)
    logging.getLogger(_audit_logger_name).warning(line, extra=extra)


def record_http(args, environ):
    """Write HTTP-related environment variables to audit log.

    :param dict args: additional details (e.g. repo name) for logging.
    :param dict environ: environment from which additional details are retrieved.

    """
    http_env = _http_env_vars(environ)
    line = " ".join("{}={}".format(k, v) for k, v in http_env.items())
    extra = _add_extra_http_data(http_env, args)
    extra = _add_logging_extra(extra)
    logging.getLogger(_audit_logger_name).warning(line, extra=extra)


# pylint:disable=W9903
# non-gettext-ed string
# This is all debug/test code from here on down.
# No translation required.
def main():
    """Parse the command-line arguments and perform the requested operation."""
    desc = """Test wrapper and debugging facility for Git Fusion logging.
    By default, does not read the global log configuration unless the
    --default option is given. Set the P4GF_LOG_CONFIG_FILE environment
    variable to provide the path to a log configuration file.
    """
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument('--default', action='store_true',
                        help="allow reading default log configuration")
    parser.add_argument('--debug', action='store_true',
                        help="print the effective logging configuration (implies --default)")
    parser.add_argument('--http', action='store_true',
                        help="audit log as if HTTP request")
    parser.add_argument('--ssh', action='store_true',
                        help="audit log as if SSH request")
    parser.add_argument('--level', default='INFO',
                        help="log level name (default is 'INFO')")
    parser.add_argument('--name', default='test',
                        help="logger name (default is 'test')")
    parser.add_argument('--msg', default='test message',
                        help="text to write to log")
    args = parser.parse_args()

    if not args.default and not args.debug:
        # Disable loading the default logging configuration since that
        # makes testing log configurations rather difficult.
        global _config_filename_default
        _config_filename_default = '/foo/bar/baz'

    # Perform usual logging initialization.
    _lazy_init(args.debug)

    if args.debug:
        # We're already done.
        return
    elif args.http:
        record_http({}, os.environ)
    elif args.ssh:
        record_argv({'userName': os.environ['REMOTE_USER']})
    else:
        lvl = logging.getLevelName(args.level)
        if not isinstance(lvl, int):
            sys.stderr.write("No such logging level: {}\n".format(args.level))
            sys.exit(1)
        log = logging.getLogger(args.name)
        log.log(lvl, args.msg)

if __name__ == "__main__":
    main()

#! /usr/bin/env python3.3
"""Run subprocesses via a separate Python process to avoid blowing out memory.

That can happen when fork() is called (e.g. on Linux).

When fork() is called on Linux, the entire memory space of the parent
process is copied, and then the subprocess is run. If the parent process
were using 800MB of memory, the child will as well, even though all it may
be doing is running `ps`.

Note that on Darwin this is not a problem.
"""

import atexit
import io
import logging
import multiprocessing
import os
import queue
import signal
import subprocess
import sys
import time
import traceback

from p4gf_const import GIT_BIN as git_bin
from p4gf_const import GIT_BIN_DEFAULT as git_bin_default

import p4gf_bootstrap  # pylint: disable=unused-import
import p4gf_char
from   p4gf_l10n      import _, NTR
import p4gf_log

LOG = logging.getLogger(__name__)
# The child process; call init() to initialize this.
ChildProc = None
ParentProc = None


def translate_git_cmd(cmd):
    """Translate git commands from 'git' to value in GIT_BIN, which defaults to 'git'."""
    if cmd[0] != git_bin_default or git_bin == git_bin_default:      # no translation required
        return cmd
    new_cmd = list(cmd)
    new_cmd[0] = git_bin
    return new_cmd


def install_stack_dumper():
    """Set up stack dumper for debugging.

    To debug a seemingly hung process, send the process the USR1 signal
    and it will dump the stacks of all threads to the log. To set up such
    behavior, call this function within each new Python process.

    """
    def _dumper(signum, _frame):
        """Signal handler that dumps all stacks to the log."""
        collector = io.StringIO()
        print(_('Received signal {signal} in process {pid}')
              .format(signal=signum, pid=os.getpid()),
              file=collector)
        print(_('Thread stack dump follows:'), file=collector)
        for thread_id, stack in sys._current_frames().items():  # pylint: disable=protected-access
            print(_('ThreadID: {thread_id}').format(thread_id=thread_id), file=collector)
            for filename, lineno, name, line in traceback.extract_stack(stack):
                print(_('  File: "{filename}", line {line}, in {function}')
                      .format(filename=filename, line=lineno, function=name),
                      file=collector)
                if line:
                    print('    ' + line.strip(), file=collector)
            print('----------', file=collector)
        print(_('Thread stack dump complete'), file=collector)
        LOG.error(collector.getvalue())

    # Try to use a signal that we're not using anywhere else.
    signal.signal(signal.SIGUSR1, _dumper)


# spawn function derived from code posted on stack overflow:
# http://stackoverflow.com/questions/8425116/indefinite-daemonized-process-spawning-in-python
# Made various updates for Python3.
def double_fork(func, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    """Fork the process (twice, in fact) to run the given function.

    Do the UNIX double-fork magic to isolate the child process from
    the parent, allowing the given function to run indefinitely.

    See Richard Stevens' "Advanced Programming in the UNIX Environment"
    (ISBN 0201563177) for details on this technique.

    """
    # flush before fork rather than later so that buffer contents don't get
    # written twice
    sys.stderr.flush()

    try:
        pid = os.fork()
        if pid > 0:
            # main/parent process
            return
    except OSError as e:
        sys.stderr.write(_('fork #1 failed: {errno} ({error})\n')
                         .format(errno=e.errno, error=e.strerror))
        sys.exit(1)

    # decouple from parent environment
    os.setsid()
    os.umask(0)

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent
            os._exit(0)  # pylint: disable=protected-access
    except OSError as e:
        sys.stderr.write(_('fork #2 failed: {errno} ({error})\n')
                         .format(errno=e.errno, error=e.strerror))
        os._exit(1)  # pylint: disable=protected-access

    # redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    si = open(stdin, 'r')
    so = open(stdout, 'a+')
    se = open(stderr, 'a+')
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    # this is a new process, so install stack dumper
    install_stack_dumper()

    # call the given function
    func()
    os._exit(0)  # pylint: disable=protected-access


def init():
    """Launch the separate Python process for running commands.

    This should be invoked early in the process, before gobs of memory are
    allocated, otherwise the child will consume gobs of memory as well.
    """
    global ChildProc, ParentProc
    if ChildProc and ParentProc != os.getpid():
        ChildProc = None
    if not ChildProc:
        ParentProc = os.getpid()
        ChildProc = ProcessRunner()
        ChildProc.start()
        return True
    return False


@atexit.register
def stop():
    """Stop the child process, if any is running."""
    global ChildProc
    if not ChildProc:
        return
    LOG.debug('stop() invoked, pid={}'.format(os.getpid()))
    ChildProc.stop()
    ChildProc = None


def _log_cmd_result(result, expect_error):
    """
    Record the command results in the log.

    If command completed successfully, record output at DEBUG level so that
    folks can suppress it with cmd:INFO. But if command completed with error
    (non-zero return code), then record its output at ERROR level so that
    cmd:INFO users still see it.
    """
    ec = result['ec']
    out = result['out']
    err = result['err']
    if (not ec) or expect_error:
        # Things going well? Don't care if not?
        # Then log only if caller is REALLY interested.
        log_level = logging.DEBUG
    else:
        # Things going unexpectedly poorly? Log almost all of the time.
        log_level = logging.ERROR
        log = logging.getLogger('cmd.cmd')
        if not log.isEnabledFor(logging.DEBUG):
            # We did not log the command. Do so now.
            log.log(log_level, result['cmd'])
    logging.getLogger('cmd.exit').log(log_level, NTR("exit: {0}").format(ec))
    out_log = logging.getLogger('cmd.out')
    out_log.debug(NTR("out : ct={0}").format(len(out)))
    if len(out) and out_log.isEnabledFor(logging.DEBUG3):
        if isinstance(out, bytes):
            out = out.decode()
        # Replace any nulls that may be in the output (e.g. git-ls-tree)
        # which cause problems when reading the log files, especially in
        # XML format.
        out = out.replace('\x00', ' ')
        out_log.debug3(NTR("out :\n{0}").format(out))
    if len(err):
        logging.getLogger('cmd.err').log(log_level, NTR("err :\n{0}").format(err))


def _validate_popen(cmd):
    """Check that cmd is a list, reporting an error and returning None if it's not.

    Otherwise returns a boolean indicating if the ProcessRunner was initialized
    or not (False if already initialized).
    """
    if not isinstance(cmd, list):
        LOG.error("popen_no_throw() cmd not of list type: {}".format(cmd))
        return None
    logging.getLogger("cmd.cmd").debug(' '.join(cmd))
    if not ChildProc:
        if LOG.isEnabledFor(logging.DEBUG3):
            tb = traceback.format_stack()
            LOG.debug3("ProcessRunner launched at time of popen(): %s", tb)
        return init()
    return False


def _popen_no_throw_internal(cmd_, expect_error, stdin=None, env=None):
    """Internal Popen() wrapper that records command and result to log.

    The standard output and error results are converted to text using
    the p4gf_char.decode() function.
    """
    if _validate_popen(cmd_) is None:
        return None
    cmd = translate_git_cmd(cmd_)
    result = ChildProc.popen(cmd, stdin, env)
    result['cmd'] = ' '.join(cmd_)   # use the untranslated cmd_ for logging
    if 'out' in result:
        result['out'] = p4gf_char.decode(result['out'])
    if 'err' in result:
        result['err'] = p4gf_char.decode(result['err'])
    _log_cmd_result(result, expect_error)
    return result


def popen_binary(cmd_, expect_error=False, stdin=None, env=None):
    """Internal Popen() wrapper that records command and result to log.

    The stdin argument is the input text for the child process.
    The standard output and error results are in binary form.
    """
    if _validate_popen(cmd_) is None:
        return None
    cmd = translate_git_cmd(cmd_)
    result = ChildProc.popen(cmd, stdin, env)
    result['cmd'] = ' '.join(cmd_)
    _log_cmd_result(result, expect_error)
    return result


def popen_no_throw(cmd, stdin=None, env=None):
    """Call popen() and return, even if popen() returns a non-zero returncode.

    The stdin argument is the input text for the child process.

    Prefer popen() to popen_no_throw(): popen() will automatically fail fast
    and report errors. popen_no_throw() will silently fail continue on,
    probably making things worse. Use popen_no_throw() only when you expect,
    and recover from, errors.
    """
    return _popen_no_throw_internal(cmd, True, stdin, env)


def popen(cmd, stdin=None, env=None):
    """Wrapper for subprocess.Popen() that logs command and output to debug log.

    The stdin argument is the input text for the child process.

    Returns three-way dict: (out, err, Popen)
    """
    result = _popen_no_throw_internal(cmd, False, stdin, env)
    if result['ec'] == 0:
        return result
    raise RuntimeError(_('Command failed: {cmd}'
                         '\nexit code: {ec}.'
                         '\nstdout:\n{out}'
                         '\nstderr:\n{err}')
                       .format(ec=result['ec'],
                               cmd=result['cmd'],
                               out=result['out'],
                               err=result['err']))


def wait(cmd_, stdin=None, env=None):
    """Invoke subprocess.wait with the given command list.

    Read standard input for the command from the named file.
    Return the return code of the process.
    """
    if _validate_popen(cmd_) is None:
        return None
    cmd = translate_git_cmd(cmd_)
    result = ChildProc.wait(cmd, stdin, env)
    result['cmd'] = ' '.join(cmd_)
    _log_cmd_result(result, False)
    return result['ec']


def call(cmd_, stdin=None, env=None):
    """Invoke subprocess.call with the given command.

    Read standard input for the command from the named file.
    Return the return code of the process.
    """
    if _validate_popen(cmd_) is None:
        return None
    cmd = translate_git_cmd(cmd_)
    result = ChildProc.call(cmd, stdin, env)
    result['cmd'] = ' '.join(cmd_)
    _log_cmd_result(result, False)
    return result['ec']


def _cmd_runner(event, incoming, outgoing):
    """Invoke subprocess.Popen in a separate process.

    This should never be called directly, but instead launched via
    multiprocessing.Process().
    """
    p4gf_log.reset()
    global LOG
    LOG = logging.getLogger(__name__)
    LOG.debug("_cmd_runner() running, pid={}".format(os.getpid()))
    try:
        while not event.is_set():
            try:
                # Use timeout so we loop around and check the event.
                (cmd, stdin, cwd, wait_, call_, env) = incoming.get(timeout=1)
                # By taking a command list vs a string, we implicitly avoid
                # shell quoting. Also note that we are intentionally _not_
                # using the shell, to avoid security vulnerabilities.
                result = {"out": b'', "err": b''}
                try:
                    stdin_file = None
                    if (wait_ or call_) and stdin:
                        # Special-case: stdin names a file to feed to process.
                        stdin_file = open(stdin)
                    if wait_:
                        p = subprocess.Popen(cmd, cwd=cwd, stdin=stdin_file,
                                             restore_signals=False, env=env)
                        LOG.debug('_cmd_runner() waiting for {}, pid={}'.format(cmd, p.pid))
                        result["ec"] = p.wait()
                    elif call_:
                        p = subprocess.Popen(cmd, stdin=stdin_file,
                                             restore_signals=False, env=env)
                        LOG.debug('_cmd_runner() called {}, pid={}'.format(cmd, p.pid))
                        result["ec"] = p.wait()
                    else:
                        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                                             restore_signals=False, env=env)
                        LOG.debug('_cmd_runner() communicating with {}, pid={}'.format(cmd, p.pid))
                        fd = p.communicate(stdin)
                        # return the raw binary, let higher level funcs decode it
                        result["out"] = fd[0]
                        result["err"] = fd[1]
                        result["ec"] = p.returncode
                except IOError as e:
                    LOG.warning("IOError in subprocess: {}".format(e))
                    result["ec"] = os.EX_IOERR
                    result["err"] = bytes(str(e), 'UTF-8')
                finally:
                    if stdin_file:
                        stdin_file.close()
                outgoing.put(result)
            except queue.Empty:
                pass
        LOG.debug("_cmd_runner() process exiting, pid={}".format(os.getpid()))
    except Exception:  # pylint: disable=broad-except
        LOG.exception("_cmd_runner() died unexpectedly, pid=%s", os.getpid())
        event.set()


class ProcessRunner():

    """Manages a child process which receives commands to be run via the subprocess module.

    Returns the output to the caller.
    """

    def __init__(self):
        self.__event = None
        self.__input = None
        self.__output = None
        self.__stats = {}

    def log_stats(self):
        """Log statistics for git commands run."""
        # sort by time
        LOG.debug("\nProcessRunner statistics:\n" +
                  "\n".join(["\t{:10.10}: {:6} {:6.3f}".format(k, v[0], v[1]) for (k, v)
                            in [(k, self.__stats[k]) for k
                                in sorted(self.__stats.keys(),
                                          key=lambda k: self.__stats[k][1],
                                          reverse=True)]]))

    def start(self):
        """Start the child process and prepare to run commands."""
        self.__event = multiprocessing.Event()  # pylint:disable=no-member
        self.__input = multiprocessing.Queue()  # pylint:disable=no-member
        self.__output = multiprocessing.Queue()  # pylint:disable=no-member
        pargs = [self.__event, self.__input, self.__output]
        p = multiprocessing.Process(target=_cmd_runner,  # pylint:disable=not-callable
                                    args=pargs, daemon=True)
        p.start()
        LOG.debug("ProcessRunner started child {}, pid={}".format(p.pid, os.getpid()))
        if LOG.isEnabledFor(logging.DEBUG3):
            sink = io.StringIO()
            traceback.print_stack(file=sink)
            LOG.debug3("Calling stack trace:\n" + sink.getvalue())
            sink.close()

    def stop(self):
        """Signal the child process to terminate. Does not wait."""
        if self.__event:
            self.__event.set()
            self.__event = None
            self.__input = None
            self.__output = None
        self.log_stats()

    def run_cmd(self, cmd_, stdin, _wait, _call, env):
        """Invoke the given command via subprocess.Popen().

        Return the exit code, standard output, and standard error in a dict.
        """
        if not self.__input:
            LOG.warning("ProcessRunner.run_cmd() called before start()")
            self.start()

        # Make the child process use whatever happens to be our current
        # working directory, which seems to matter with Git.
        cwd = os.getcwd()
        start_time = time.time()
        cmd = translate_git_cmd(cmd_)  # translate the 'git' command if needed
        self.__input.put((cmd, stdin, cwd, _wait, _call, env))
        result = None
        while not self.__event.is_set():
            try:
                result = self.__output.get(timeout=1)
                break
            except queue.Empty:
                pass
        if not result:
            raise RuntimeError(_('Error running: {command}').format(command=cmd))
        if cmd_[0] == "git":
            git_cmd = cmd_[1]
            if git_cmd.startswith("--git-dir") or git_cmd.startswith("--work-tree"):
                git_cmd = cmd_[2]
            elapsed_time = time.time() - start_time
            current = self.__stats.get(git_cmd, (0, 0))
            self.__stats[git_cmd] = (current[0] + 1, current[1] + elapsed_time)
        return result

    def popen(self, cmd, stdin, env=None):
        """Invoke the given command via subprocess.Popen().

        Return the exit code, standard output, and standard error in a dict.
        """
        return self.run_cmd(cmd, stdin, _wait=False, _call=False, env=env)

    def wait(self, cmd, stdin, env=None):
        """Invoke the given command via subprocess.Popen().

        Return the exit code, standard output, and standard error in a dict.
        """
        return self.run_cmd(cmd, stdin, _wait=True, _call=False, env=env)

    def call(self, cmd, stdin, env=None):
        """Invoke the given command via subprocess.Popen().

        Return the exit code, standard output, and standard error in a dict.
        """
        return self.run_cmd(cmd, stdin, _wait=False, _call=True, env=env)

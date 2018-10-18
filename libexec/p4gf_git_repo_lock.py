#! /usr/bin/env python3.3
"""Serialize access to the Git Fusion server's own Git repo.

* Allow multiple readers, or a single writer, of a particular repository.
* Control access to reader/writer "lock" using the flock() system call.
* Incoming readers add a file unique to the repo and their process ID.
    * If a "write" file is present, no read file is created, reader blocks.
    * If the first reader can acquire the p4key lock, perform copy-to-git.
    * If a reader cannot acquire the p4key, skip the copy-to-git phase.
* Incoming writers create a repo-specific "write" file to get exclusive access.
    * First one always succeeds, other writers are blocked.
    * Writer does not proceed until all existing reader locks are gone.
    * Writer then acquires p4key lock, blocking if needed.

"""

from contextlib import contextmanager
import errno
import fcntl
import logging
import os
import time

import p4gf_const
from p4gf_l10n import _
import p4gf_log

LOG = logging.getLogger(__name__)
_RETRY_PERIOD = 0.5
_LOCK_FILE = "lock"
_WRITE_FILE = "write"


class LockBusy(Exception):

    """Raised in the case of a non-blocking lock acquisition."""

    pass


@contextmanager
def read_lock(repo_name):
    """Context manager to acquire and release shared read lock.

    :param repo_name: name of the repository being lock.

    Yields True if the reader had to wait for a writer to finish.

    """
    waited = acquire_read_lock(repo_name)
    try:
        yield waited
    finally:
        remove_read_lock(repo_name)


@contextmanager
def write_lock(repo_name, upgrade=False, blocking=True, append=False):
    """Context manager to acquire and release exclusive write lock.

    :param repo_name: name of the repository being lock.
    :param upgrade: if True, allow 1 reader lock to exist, False to require none.
    :param blocking: if True, wait for readers to finish, otherwise raise LockBusy.
    :param append: if True, add a PID to the write lock (default is False).

    """
    acquire_write_lock(repo_name, upgrade, blocking, append)
    try:
        yield
    finally:
        remove_write_lock(repo_name)


def _active_reader_count(repo_name):
    """Determine the number of readers currently in progress.

    This should only be called when either the write lock acquisition
    process has begun (which prevents additional readers), or when the
    key-based lock is held by the current process.

    """
    entries = os.listdir(_locks_dir_name(repo_name))
    count = len(entries)
    # not exactly concise, but hopefully correct
    if _LOCK_FILE in entries:
        count -= 1
    if _WRITE_FILE in entries:
        count -= 1
    LOG.debug("active reader count %s: %s", count, repo_name)
    return count


def _prune_dead_readers(repo_name):
    """Find and remove any stale reader locks.

    :param repo_name: name of repository for which to find dead locks

    """
    with _git_repo_lock(repo_name):
        entries = os.listdir(_locks_dir_name(repo_name))
        if _LOCK_FILE in entries:
            entries.remove(_LOCK_FILE)
        if _WRITE_FILE in entries:
            entries.remove(_WRITE_FILE)
        for pid in entries:
            if not pid_exists(pid):
                LOG.warning("stale read-lock %s for %s removed", pid, repo_name)
                os.unlink(_read_lock_name(repo_name, pid))


def acquire_read_lock(repo_name):
    """Acquire a shared read lock on the named repository.

    :param repo_name: name of repository for which to acquire lock
    :return: True if read request had waited on a write lock, False otherwise.

    """
    LOG.debug2("read-lock acquiring: %s", repo_name)
    write_fname = _write_lock_name(repo_name)
    read_fname = _read_lock_name(repo_name)
    waited = False
    aliveness_asserted = False
    label = _("acquiring read-lock for {repo_name}").format(repo_name=repo_name)
    wait_reporter = p4gf_log.LongWaitReporter(label, LOG)
    while True:
        with _git_repo_lock(repo_name):
            # need to wait for any pending writer to finish
            if not os.path.exists(write_fname):
                with open(read_fname, "w") as fobj:
                    fobj.write("1")
                break
            # if we have not already done so, check if the writer process
            # is still alive
            if not aliveness_asserted:
                if _check_all_processes(write_fname):
                    LOG.warning("stale write lock for %s removed", repo_name)
                    os.unlink(write_fname)
                    with open(read_fname, "w") as fobj:
                        fobj.write("1")
                    break
                aliveness_asserted = True
        waited = True
        wait_reporter.been_waiting()
        time.sleep(_RETRY_PERIOD)
    LOG.debug("read-lock acquired: %s", repo_name)
    return waited


def remove_read_lock(repo_name):
    """Remove a shared read lock on the named repository.

    :param repo_name: name of repository for which to remove lock

    """
    with _git_repo_lock(repo_name):
        os.unlink(_read_lock_name(repo_name))
    LOG.debug("read-lock released: %s", repo_name)


def acquire_write_lock(repo_name, upgrade=False, blocking=True, append=False):
    """Acquire an exclusive write lock on the named repository.

    :param repo_name: name of repository for which to acquire lock.
    :param upgrade: if True, allow 1 reader lock to exist, False to require none.
    :param blocking: if False, raise LockBusy if unable to get exclusive access.
    :param append: if True, add a PID to the write lock (default is False).

    """
    # indicate our interest in writing to the repository
    LOG.debug2("write-lock acquiring: %s, upgrade=%s, blocking=%s, append=%s",
               repo_name, upgrade, blocking, append)
    write_fname = _write_lock_name(repo_name)
    aliveness_asserted = False
    label = _("acquiring write-lock for {repo_name}").format(repo_name=repo_name)
    wait_reporter = p4gf_log.LongWaitReporter(label, LOG)
    self_pid = str(os.getpid())
    while True:
        with _git_repo_lock(repo_name):
            # If there is no other writer, then take the lock.
            if not os.path.exists(write_fname):
                with open(write_fname, "w") as fobj:
                    fobj.write(self_pid)
                break
            # If multiple "writers" are allowed (as with foreground and
            # background push processes), then append the PID of this
            # process to the lock.
            elif append:
                _add_pid_to_lock_file(write_fname, self_pid)
                LOG.debug("write-lock borrowed: %s by %s", repo_name, self_pid)
                break
            # If we have not already done so, check if the other writer
            # process is still alive.
            elif not aliveness_asserted:
                if _check_all_processes(write_fname):
                    LOG.warning("stale write-lock for %s removed", repo_name)
                    with open(write_fname, "w") as fobj:
                        fobj.write(self_pid)
                    break
                aliveness_asserted = True
        if not blocking:
            raise LockBusy()
        wait_reporter.been_waiting()
        time.sleep(_RETRY_PERIOD)
    LOG.debug2("write-lock pending: %s", repo_name)
    # wait for all of the currently active readers to finish
    allowed_readers = 1 if upgrade else 0
    aliveness_asserted = False
    while _active_reader_count(repo_name) > allowed_readers:
        if not blocking:
            remove_write_lock(repo_name, no_log=True)
            LOG.debug("write-lock cancelled: %s", repo_name)
            raise LockBusy()
        if not aliveness_asserted:
            _prune_dead_readers(repo_name)
            aliveness_asserted = True
        wait_reporter.been_waiting()
        time.sleep(_RETRY_PERIOD)
    LOG.debug("write-lock acquired: %s", repo_name)


def remove_write_lock(repo_name, no_log=False):
    """Remove the exclusive write lock on the named repository.

    Removes only the entry corresponding to the current process.
    If the file becomes empty, it will be removed entirely.

    :param repo_name: name of repository for which to remove lock
    :param bool no_log: if True, do not log a (misleading) message

    """
    write_fname = _write_lock_name(repo_name)
    with _git_repo_lock(repo_name):
        with open(write_fname, 'r+') as fobj:
            # Use a set to be certain that we remove any duplicate entries
            # from the lock file.
            locks = set(fobj.read().splitlines())
            # Allow for this process identifier to be absent from the file
            # in case we are attempting to remove the same lock twice.
            locks.discard(str(os.getpid()))
            if locks:
                fobj.seek(0)
                fobj.write('\n'.join(locks))
                fobj.truncate()
            else:
                # Deleting an open file will work on Unix-like systems.
                os.unlink(write_fname)
    if not no_log:
        LOG.debug("write-lock released: %s", repo_name)


def _read_lock_name(repo_name, pid=None):
    """Return the name of a read lock for the named repo and this process."""
    if pid is None:
        pid = os.getpid()
    return "{0}/locks/{1}/{2}".format(p4gf_const.P4GF_HOME, repo_name, pid)


def _write_lock_name(repo_name):
    """Return the name of the write lock for the named repo."""
    return "{0}/locks/{1}/{2}".format(p4gf_const.P4GF_HOME, repo_name, _WRITE_FILE)


def _locks_dir_name(repo_name):
    """Return the name of the directory which contains the lock files."""
    return "{0}/locks/{1}".format(p4gf_const.P4GF_HOME, repo_name)


@contextmanager
def _git_repo_lock(repo_name):
    """Acquire exclusive access to the locks for the named repository."""
    path = "{0}/locks/{1}/{2}".format(p4gf_const.P4GF_HOME, repo_name, _LOCK_FILE)
    parent_dir = os.path.dirname(path)
    if not os.path.exists(parent_dir):
        try:
            os.makedirs(parent_dir, exist_ok=True)
        except FileExistsError:
            # If the mode does not match, makedirs() raises an error in
            # versions of Python prior to 3.3.6; since umask might alter
            # the mode, we have no choice but to ignore this error.
            pass
    LOG.debug2("git-lock acquiring: %s", repo_name)
    fd = os.open(path, os.O_CREAT | os.O_WRONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)
    os.write(fd, bytes("{}".format(os.getpid()), "utf-8"))
    try:
        LOG.debug2("git-lock acquired: %s", repo_name)
        yield
    finally:
        os.close(fd)
    LOG.debug2("git-lock released: %s", repo_name)


def _check_all_processes(fname):
    """Read the PIDs from the named file and verify if any are alive.

    :param str fname: path of file containing process IDs.
    :return: True if all processes are no longer alive, False otherwise.

    """
    with open(fname) as fobj:
        for pid in fobj:
            if pid_exists(pid):
                LOG.debug("found fresh lock process %s", pid)
                return False
            elif LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("found stale lock process %s", pid)
    return True


def pid_exists(pid):
    """Check if a process by the given pid exists."""
    try:
        os.kill(int(pid), 0)
    except ValueError:
        LOG.error("encountered non-integer PID value: %s", pid)
        return False
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        elif err.errno == errno.EPERM:
            return True
        raise
    return True


def _add_pid_to_lock_file(fname, pid):
    """Ensure the current process identifier appears in the named file."""
    with open(fname, 'r+') as fobj:
        # Use a set to ensure we do not add the same identifer twice.
        locks = set(fobj.read().splitlines())
        length_before_add = len(locks)
        locks.add(pid)
        if len(locks) > length_before_add:
            fobj.seek(0)
            fobj.write('\n'.join(locks))

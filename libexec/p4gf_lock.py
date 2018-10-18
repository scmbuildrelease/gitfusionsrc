#! /usr/bin/env python3.3
"""Acquire and release a lock using p4keys."""

import datetime
import json
import logging
import os
import time
import traceback

import p4gf_const
import p4gf_p4key as P4Key
from p4gf_l10n import _
import p4gf_log
import p4gf_util

LOG = logging.getLogger(__name__)

# time.sleep() accepts a float, which is how you get sub-second sleep durations.
MS = 1.0 / 1000.0

# How often we retry to acquire the lock.
_RETRY_PERIOD = 500 * MS

# Set DEBUG_TRACE to True to have stack traces in the log when acquiring
# and releasing the locks. Having this on by default makes grepping the
# logs very painful, so please do not do so without considering that.
DEBUG_TRACE = False


def _process_start_time():
    """Return start time of current process."""
    date = datetime.datetime.now()
    return date.isoformat(sep=' ').split('.')[0]


class LockBusy(Exception):

    """LockBusy is used to signal that a lock could not be acquired.

    This will happen if another process has already acquired it in an
    incompatible mode.  Nothing that can't be cured by waiting.
    """

    def __init__(self, lock_id):
        """Init the Exception with an explanatory message."""
        super(LockBusy, self).__init__(
            _('Unable to acquire lock: {lock_id}'
              '\nPlease try again later.').format(lock_id=lock_id))


class LockCorrupt(Exception):

    """LockCorrupt is used to signal that a lock's state is corrupted.

    Raised when the lock's p4 key contains unexpected and invalid content.
    An admin needs to repair the lock's p4 key.
    """

    def __init__(self, lock_id):
        """Init the Exception with an explanatory message."""
        super(LockCorrupt, self).__init__(
            _('Lock state corrupted: {lock_id}').format(lock_id=lock_id))


class Lock:

    """Base class for locks.

    Override do_acquire() and do_release() to make something useful.
    """

    def __init__(self, p4, blocking=True):
        """Init the lock."""
        self.p4 = p4
        self.blocking = blocking
        self.has_lock = False

    def __enter__(self):
        """Acquire the lock on enter."""
        return self.acquire()

    def __exit__(self, exc_type, exc_value, _traceback):
        """Release the lock on exit, if it has not already been released."""
        # won't have lock if acquire raised in __enter__
        if self.has_lock:
            self.release()
        return False    # False = do not squelch exception

    def acquire(self):
        """Acquire a lock or raise LockBusy."""
        assert not self.has_lock

        wait_reporter = p4gf_log.LongWaitReporter("accessing p4key-lock", LOG)
        while True:
            if self.do_acquire():
                self.has_lock = True
                LOG.debug2("lock-acquired %s", self)
                if DEBUG_TRACE:
                    LOG.debug3("lock-acquired stack trace:\n%s",
                               "".join(traceback.format_stack()))
                return self

            # lock held by others, attempt to remove stale owners
            if self.remove_stale_owners():
                continue

            # non-blocking case can only raise
            if not self.blocking:
                LOG.debug2("lock-busy %s", self)
                if DEBUG_TRACE:
                    LOG.debug3("lock-busy stack trace:\n%s",
                               "".join(traceback.format_stack()))
                raise LockBusy(self)

            wait_reporter.been_waiting()
            # just wait until lock can be acquired, either due to release or transfer death
            LOG.debug2("lock-waiting %s", self)
            if DEBUG_TRACE:
                LOG.debug3("lock-waiting stack trace:\n%s",
                           "".join(traceback.format_stack()))
            time.sleep(_RETRY_PERIOD)

    def release(self):
        """Release a held lock."""
        if not self.has_lock:
            return

        try:
            LOG.debug2("lock-release %s", self)
            if DEBUG_TRACE:
                LOG.debug3("lock-release stack trace:\n%s",
                           "".join(traceback.format_stack()))
            self.do_release()
            self.has_lock = False
            return self
        except Exception as e:
            LOG.debug("release() exception: %s", e)
            raise LockCorrupt(self)

    def do_acquire(self):
        """Return True if lock can be acquired, else False."""
        # pylint: disable=no-self-use
        raise Exception("Not implemented.")

    def do_release(self):
        """Release the lock."""
        # pylint: disable=no-self-use
        raise Exception("Not implemented.")

    def remove_stale_owners(self):
        """Remove stale lock owners and indicate if any change was made.

        :return: True if any stale owners have been removed, False otherwise.

        """
        # pylint: disable=no-self-use
        return False


class SimpleLock(Lock):

    """Simple lock implemented using a single p4 key.

    Initial lock acquisition increments the p4 key from undefined to 1.

    Subsequent attempts to acquire the lock will increment the p4 key to some
    value greater than 1, indicating the lock is busy.

    If blocking is True, the Lock will repeatedly attempt acquisition until
    successful.

    If blocking is False, the Lock will raise LockBusy on failure to acquire.

    When the lock is released, the p4 key is deleted.
    """

    def __init__(self, p4, lock_key, blocking=True):
        """Init the Lock."""
        super(SimpleLock, self).__init__(p4, blocking)
        self.lock_key = lock_key

    def do_acquire(self):
        """Acquire a lock or raise LockBusy."""
        return P4Key.acquire(self.p4, self.lock_key)

    def do_release(self):
        """Release a held lock."""
        P4Key.delete(self.p4, self.lock_key)

    def __str__(self):
        """For logging purposes."""
        return self.lock_key


class RepoLock(Lock):

    """A per-repo write lock.

    This lock can be locked by a group of cooperating processes. The process
    group is identified by group_id, which is just the pid of the first process
    to acquire the lock.

    The lock is initially acquired by xxx_server.

    The next possible owner is pre_receive_hook.  The pre_receive_hook will
    either not run, or will exit before xxx_server releases the lock, so no
    problem there.

    If pre_receive_hook runs and does not reject the push, post_receive_hook
    will start a new process which needs to assume ownership of the lock.
    There is a race here between the xxx_server process releasing the lock and
    the new background process acquiring it.  The xxx_server process can't
    release the lock until the background process has a chance to acquire it,
    but it also shouldn't get stuck if the background process starts up and
    exits very quickly, before control returns to xxx_server.

    To solve this, post_receive_hook calls set_acquire_pending(True) before
    forking the background process.  This sets a flag in the lock key which
    will be cleared when the background process acquires the lock.  The release
    by xxx_server will be blocked until this flag is cleared.
    """

    def __init__(self, p4, repo_name, blocking=True, group_id=None):
        """Initialize the lock."""
        super(RepoLock, self).__init__(p4, blocking)
        self.lock_key = p4gf_const.P4GF_P4KEY_LOCK_VIEW.format(repo_name=repo_name)
        self.owners_key = p4gf_const.P4GF_P4KEY_LOCK_VIEW_OWNERS.format(repo_name=repo_name)
        self.lock = SimpleLock(p4, self.lock_key, blocking=blocking)

        self.group_id = group_id or str(os.getpid())
        self.server_id = p4gf_util.get_server_id()
        self.process_id = os.getpid()
        self.start_time = _process_start_time()
        self.ignore_pending_acquire = False
        self._post_lock_release_cb = None
        self._transfer_complete_cb = None
        self._acquire_time = None
        self._repo_name = repo_name

    def do_acquire(self):
        """Acquire a lock or raise LockBusy."""
        try:
            with self.lock:
                content = self._add_self(self._read())
                self._write(content)
            self._acquire_time = time.time()
            LOG.debug("p4key-lock acquired: %s", self._repo_name)
            return True
        except LockBusy:
            return False

    def do_release(self):
        """Release a held lock."""
        label = "releasing p4key-lock for {}".format(self._repo_name)
        wait_reporter = p4gf_log.LongWaitReporter(label, LOG)
        while True:
            with self.lock:
                content = self._read()
                if self.ignore_pending_acquire or 'acquire_pending' not in content:
                    content = self._remove_self(content)
                    if content is None:
                        P4Key.delete(self.p4, self.owners_key)
                    else:
                        self._write(content)
                        if self._transfer_complete_cb:
                            self._transfer_complete_cb()
                    if self._post_lock_release_cb:
                        self._post_lock_release_cb()
                    td = time.time() - self._acquire_time
                    LOG.debug("p4key-lock released: %s after %s ms", self._repo_name, td)
                    return
            wait_reporter.been_waiting()
            time.sleep(_RETRY_PERIOD)

    def remove_stale_owners(self):
        """Remove any lock owners that have gone stale."""
        with self.lock:
            content = self._read()
            if 'owners' not in content:
                return False
            fresh_owners = []
            for owner in content['owners']:
                if 'client_name' in owner:
                    result = self.p4.run('clients', '-e', owner['client_name'])
                    LOG.debug3('remove_stale_owners(): clients matching %s: %s',
                               owner['client_name'], result)
                    if len(result) > 0:
                        fresh_owners.append(owner)
                    else:
                        pid = owner.get('process_id', 'unknown')
                        LOG.warning('stale p4key-lock %s for %s removed', pid, self._repo_name)
                else:
                    # client-less owner, cannot safely remove this entry
                    fresh_owners.append(owner)
            if len(fresh_owners) < len(content['owners']):
                if len(fresh_owners) == 0:
                    P4Key.delete(self.p4, self.owners_key)
                else:
                    content['owners'] = fresh_owners
                    self._write(content)
                return True
        return False

    def set_acquire_pending(self):
        """Set lock so that it won't release until it has first been reacquired."""
        assert self.has_lock
        with self.lock:
            content = self._read()
            content['acquire_pending'] = True
            self._write(content)
            self.ignore_pending_acquire = True

    def wholly_owned(self):
        """Return True if this is the one and only owner of the lock."""
        if not self.has_lock:
            return False
        with self.lock:
            content = self._read()
            return 'acquire_pending' not in content and len(content['owners']) == 1

    def set_transfer_complete_cb(self, callback):
        """Register a callback to be invoked after successful transfer.

        Transfer of the lock is considered successful if the lock is released
        while the contents of the owners list is non-empty.

        """
        self._transfer_complete_cb = callback

    def set_lock_release_cb(self, callback):
        """Register a callback to be invoked at the end of lock release.

        This is invoked after they transfer complete callback, if any.

        """
        self._post_lock_release_cb = callback

    def _add_self(self, content):
        """Return content with self added to 'owners' list.

        If content is '0' (i.e. newly locked) return valid content with self
        as the only owner.
        """
        try:
            if not content:
                content = {
                    'server_id': self.server_id,
                    'group_id': self.group_id,
                    'owners': []
                }
            elif (content['server_id'] != self.server_id or
                  content['group_id'] != self.group_id):
                LOG.debug("cannot add lock owner: %s vs %s, %s vs %s (owner %s)",
                          content['server_id'], self.server_id,
                          content['group_id'], self.group_id,
                          content.get('process_id', 'unknown'))
                raise LockBusy(self)

            if 'acquire_pending' in content:
                del content['acquire_pending']
            content['owners'].append({
                'process_id': self.process_id,
                'start_time': self.start_time,
                'client_name': self.p4.client
                })
            return content

        except LockBusy:
            raise
        except:
            LOG.exception("invalid lock content? %s", content)
            raise LockCorrupt(self)

    def _remove_self(self, content):
        """Return content with self removed from 'owners' list."""
        try:
            content['owners'] = [owner for owner in content['owners']
                                 if (owner['process_id'] != self.process_id or
                                     owner['start_time'] != self.start_time)]
            if len(content['owners']):
                return content
            return None
        except:
            raise LockCorrupt(self)

    def _read(self):
        """Read the current content from key."""
        try:
            s = P4Key.get(self.p4, self.owners_key)
            if s == "0":
                return {}
            return json.loads(s)
        except ValueError:
            raise LockCorrupt(self)

    def _write(self, content):
        """Write content to the key."""
        val = json.dumps(content)
        P4Key.set(self.p4, self.owners_key, val)

    def __str__(self):
        """For logging purposes."""
        return self.owners_key


class ReviewsLock(SimpleLock):

    """Write lock shared by all Reviews users.

    p4gf_const.P4GF_REVIEWS_COMMON_LOCK key is used for locking.
    p4gf_const.P4GF_REVIEWS_COMMON_LOCK_OWNER key holds current owner info.
    """

    def __init__(self, p4, blocking=True):
        """Init lock, setting lock key name used for reviews."""
        super(ReviewsLock, self).__init__(
            p4=p4,
            lock_key=p4gf_const.P4GF_REVIEWS_COMMON_LOCK,
            blocking=blocking)
        self.content = {
            'server_id': p4gf_util.get_server_id(),
            'process_id': os.getpid(),
            'start_time': _process_start_time()
        }

    def do_acquire(self):
        """Acquire the lock and set the owner key if successful."""
        if not super(ReviewsLock, self).do_acquire():
            return False
        val = json.dumps(self.content)
        P4Key.set(self.p4, p4gf_const.P4GF_REVIEWS_COMMON_LOCK_OWNER, val)
        return True

    def do_release(self):
        """First delete the owner key, then release the lock."""
        P4Key.delete(self.p4, p4gf_const.P4GF_REVIEWS_COMMON_LOCK_OWNER)
        super(ReviewsLock, self).do_release()

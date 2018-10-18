#! /usr/bin/env python3.3
"""Analyze log file to report on lock contention."""

from datetime import datetime
import functools
import logging
import os
import re
import sys
import xml.sax

import p4gf_const
from p4gf_l10n import _
import p4gf_log
from p4gf_readlog import LogSource, LogContentHandler
import p4gf_util

FILENAME_RE = re.compile(r'(?P<d>\d{4}-\d{2}-\d{2}-\d{6})_(?P<m>.+?)_(?P<p>\d+)_(?P<t>log\.xml)')
STALE_P4KEY_RE = re.compile(r'stale p4key-lock (?P<pid>\d+) for (?P<repo>[^ ]+) removed')
STALE_READ_RE = re.compile(r'stale read-lock (?P<pid>\d+) for (?P<repo>[^ ]+) removed')
STALE_WRITE_RE = re.compile(r'stale write-lock for (?P<repo>[^ ]+) removed')
ACQUIRE_P4KEY_WAIT_RE = re.compile(r'accessing p4key-lock has been waiting for (?P<pid>\d+)')
RELEASE_P4KEY_WAIT_RE = re.compile(r'releasing p4key-lock for (?P<repo>[^ ]+)' +
                                   r' has been waiting for (?P<pid>\d+)')
ACQUIRE_READ_WAIT_RE = re.compile(r'acquiring read-lock for (?P<repo>[^ ]+)' +
                                  r' has been waiting for (?P<pid>\d+)')
ACQUIRE_WRITE_WAIT_RE = re.compile(r'acquiring write-lock for (?P<repo>[^ ]+)' +
                                   r' has been waiting for (?P<pid>\d+)')


class LockExaminer:

    """LockExaminer maintains the state of analyzing the log files."""

    def __init__(self, repo_name):
        """Initialize the LockExaminer instance."""
        self.repo_name = repo_name
        self.p4key_state = dict()
        self.write_lock_state = dict()
        self.read_lock_state = dict()

    def examine_log(self, log_file):
        """Examine the given (XML) log file."""
        callback = functools.partial(self.receive_record)
        with open(log_file, 'rb') as fobj:
            xml.sax.parse(LogSource(fobj), LogContentHandler(callback))

    def print_summary(self):
        """Print a closing summary of the log file analysis."""
        for record in self.p4key_state.values():
            self._warn(record, 'p4key lock not released')
        for record in self.write_lock_state.values():
            self._warn(record, 'write lock not released')
        for record in self.read_lock_state.values():
            self._warn(record, 'read lock not released')

    def receive_record(self, record):
        """Process the given record from the XML log file.

        :param record: parsed log record.

        """
        name = record.nm.lower()
        if name not in ['p4gf_lock', 'p4gf_git_repo_lock']:
            return
        # Lazily escape the record message.
        record.msg = xml.sax.saxutils.unescape(record.msg)
        if "p4key-lock" in record.msg:
            self.handle_p4key(record)
        elif "read-lock" in record.msg:
            self.handle_read_lock(record)
        elif "write-lock" in record.msg:
            self.handle_write_lock(record)

    def handle_p4key(self, record):
        """The record is related to the p4key based locks."""
        # pylint:disable=too-many-branches
        if record.msg.startswith("p4key-lock acquired: {}".format(self.repo_name)):
            self.p4key_state[record.pid] = record
        elif record.msg.startswith("p4key-lock released: {}".format(self.repo_name)):
            if record.pid in self.p4key_state:
                dt = self._calc_time_delta(self.p4key_state, record)
                self._info(record, "p4key lock released after {}".format(dt))
                del self.p4key_state[record.pid]
        elif record.msg.startswith('stale p4key-lock'):
            m = STALE_P4KEY_RE.match(record.msg)
            if m is None:
                self._warn(record, "malformed record message: {}".format(record.msg))
            else:
                pid = m.group('pid')
                repo = m.group('repo')
                if repo == self.repo_name:
                    self._warn(record, 'stale p4key lock removed for {}'.format(pid))
        elif record.msg.startswith('accessing p4key-lock'):
            m = ACQUIRE_P4KEY_WAIT_RE.match(record.msg)
            if m is None:
                self._warn(record, "malformed record message: {}".format(record.msg))
            else:
                pid = m.group('pid')
                self._warn(record, 'access to p4key lock waiting for {}'.format(pid))
        elif record.msg.startswith('releasing p4key-lock'):
            m = RELEASE_P4KEY_WAIT_RE.match(record.msg)
            if m is None:
                self._warn(record, "malformed record message: {}".format(record.msg))
            else:
                pid = m.group('pid')
                repo = m.group('repo')
                if repo == self.repo_name:
                    self._warn(record, 'release of p4key lock waiting for {}'.format(pid))

    def handle_write_lock(self, record):
        """The record is related to the file based write locks."""
        # pylint:disable=too-many-branches
        if record.msg.startswith('write-lock acquired: {}'.format(self.repo_name)):
            self.write_lock_state[record.pid] = record
        elif record.msg.startswith('write-lock released: {}'.format(self.repo_name)):
            if record.pid in self.write_lock_state:
                dt = self._calc_time_delta(self.write_lock_state, record)
                self._info(record, "write lock released after {}".format(dt))
                del self.write_lock_state[record.pid]
            else:
                self._warn(record, "release of write lock without acquisition")
        elif record.msg.startswith('write-lock cancelled: {}'.format(self.repo_name)):
            self._info(record, "write lock acquisition aborted (other readers)")
        elif record.msg.startswith('write-lock borrowed: {}'.format(self.repo_name)):
            self.write_lock_state[record.pid] = record
            self._info(record, "write lock borrowed")
        elif record.msg.startswith('stale write-lock'):
            m = STALE_WRITE_RE.match(record.msg)
            if m is None:
                self._warn(record, "malformed record message: {}".format(record.msg))
            else:
                repo = m.group('repo')
                if repo == self.repo_name:
                    self._warn(record, 'stale write lock removed for {}'.format(repo))
        elif record.msg.startswith('acquiring write-lock'):
            m = ACQUIRE_WRITE_WAIT_RE.match(record.msg)
            if m is None:
                self._warn(record, "malformed record message: {}".format(record.msg))
            else:
                pid = m.group('pid')
                repo = m.group('repo')
                if repo == self.repo_name:
                    self._warn(record, 'acquisition of write lock waiting for {}'.format(pid))

    def handle_read_lock(self, record):
        """The record is related to the file based read locks."""
        if record.msg.startswith('read-lock acquired: {}'.format(self.repo_name)):
            self.read_lock_state[record.pid] = record
        elif record.msg.startswith('read-lock released: {}'.format(self.repo_name)):
            if record.pid in self.read_lock_state:
                dt = self._calc_time_delta(self.read_lock_state, record)
                self._info(record, "read lock released after {}".format(dt))
                del self.read_lock_state[record.pid]
            else:
                self._warn(record, "release of read lock without acquisition")
        elif record.msg.startswith('stale read-lock'):
            m = STALE_READ_RE.match(record.msg)
            if m is None:
                self._warn(record, "malformed record message: {}".format(record.msg))
            else:
                pid = m.group('pid')
                repo = m.group('repo')
                if repo == self.repo_name:
                    self._warn(record, 'stale read lock removed for {}'.format(pid))
        elif record.msg.startswith('acquiring read-lock'):
            m = ACQUIRE_READ_WAIT_RE.match(record.msg)
            if m is None:
                self._warn(record, "malformed record message: {}".format(record.msg))
            else:
                pid = m.group('pid')
                repo = m.group('repo')
                if repo == self.repo_name:
                    self._warn(record, 'acquisition of read lock waiting for {}'.format(pid))

    @staticmethod
    def _calc_time_delta(coll, record):
        """Compute the time between the previous record with the same ID and this one.

        :param dict coll: the mapping of PID to record.
        :param record: the record that follows the previously encountered record
                       of the same process ID.
        :return: time delta of the two records.

        """
        ar = coll[record.pid]
        # Lazily parse the record date/time string.
        at = datetime.strptime(ar.dt, p4gf_log.XML_DATEFMT)
        rt = datetime.strptime(record.dt, p4gf_log.XML_DATEFMT)
        return rt - at

    @staticmethod
    def _info(record, msg):
        """Log an informational message regarding this record."""
        logging.info("%s [%s]: %s", record.dt, record.pid, msg)

    @staticmethod
    def _warn(record, msg):
        """Log a warning message regarding this record."""
        logging.warning("%s [%s]: %s", record.dt, record.pid, msg)


def compare_log_files(filename):
    """Extract a key from the log file name for use in sorting."""
    # rearrange the parts to put the date and pid first
    m = FILENAME_RE.match(filename)
    if m is None:
        raise RuntimeError('filename does not match expected format')
    return "{date}_{pid}_{mid}_{tail}".format(
        date=m.group('d'), pid=m.group('p'),
        mid=m.group('m'), tail=m.group('t'))


def retrieve_log_files(log_dir, repo_name):
    """Retrieve the log files to be processed, in the desired order."""
    # Accept only log files that are XML formatted and contain the repo
    # name in the filename.
    log_files = [fn for fn in os.listdir(log_dir) if fn.endswith('.xml') and repo_name in fn]
    # Sort the files by date and pid so they are processed in a sensible
    # order. For the most part this should not matter, but better to
    # present the results in a time-ordered fashion.
    return sorted(log_files, key=compare_log_files)


def main():
    """Read the log files and report on lock contention."""
    desc = _("""Examine lock related log entries and report. How to use:
1) Configure logging to have `p4gf_git_repo_lock` and `p4gf_lock` set to
`debug` level.
2) Comment out any `handler` and `filename` (or `file`) entries in logging configuration,
   such that one XML formatted log file per process will be created.
3) Run the pull or push operations that are of concern.
4) Run this lock_analyze.py script.
""")
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument('repo', metavar="REPO",
                        help=_("name of repository to be analyzed"))
    parser.add_argument('-d', '--logs', metavar="DIR",
                        help=_("path to log files to be processed"))
    args = parser.parse_args()
    logging.basicConfig(format="%(levelname)-7s %(message)s", stream=sys.stdout,
                        level=logging.INFO)
    if args.logs is None:
        # default args.logs to GFHOME/.git-fusion/logs
        args.logs = os.path.join(p4gf_const.P4GF_HOME, '.git-fusion', 'logs')
    log_files = retrieve_log_files(args.logs, args.repo)
    lexmr = LockExaminer(args.repo)
    for log_file in log_files:
        lexmr.examine_log(os.path.join(args.logs, log_file))
    lexmr.print_summary()


if __name__ == "__main__":
    main()

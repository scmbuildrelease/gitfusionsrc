#! /usr/bin/env python3.3
"""Common functionality for Git receive hooks."""

from contextlib import ExitStack
import itertools
import logging
import os

from p4gf_branch_id import PreReceiveTuple
import p4gf_const
import p4gf_context
import p4gf_create_p4
import p4gf_git_repo_lock
import p4gf_mem_gc
from p4gf_l10n import NTR, log_l10n
import p4gf_lock
import p4gf_log
import p4gf_path
import p4gf_proc
import p4gf_util


LOG = logging.getLogger('p4gf_receive_hook')
_DONT_CARE = "don't care"


class ReceiveHook(object):

    """Basis for pre-receive and post-receive hook code."""

    def __init__(self, label, prl):
        """Initialize the receive hook object.

        :type label: str
        :param label: short name for this hook, for logging purposes.

        :type prl: :class:`p4gf_pre_receive_hook.PreReceiveTupleLists`
        :param prl: pre-receive tuples read from stdin

        """
        self.label = label
        self.prl = prl
        self.context = None

    def before(self):
        """Perform preparatory processing work, if any.

        This function is called immediately before process().

        """
        # pylint:disable=no-self-use
        pass

    def before_p4key_lock(self, repo_name):
        """Perform any work needed before the p4key lock ownership is transferred.

        An example might be to append our PID to the file-based write lock
        while the lock is still held by the foreground process, thus
        preventing any gaps in ownership.

        """
        # pylint:disable=no-self-use
        pass

    def after(self, _ec):
        """Perform post processing repo-related work, if any.

        This function is called immediately after process(), outside of the
        p4key lock. The write lock will be acquired if
        after_requires_write_lock() returns True.

        :param int ec: status code returned from process()

        """
        # pylint:disable=no-self-use
        pass

    def after_requires_write_lock(self):
        """Return True if this hooks requires a write lock for after()."""
        # pylint:disable=no-self-use
        return False

    def cleanup(self):
        """Unconditionally clean up any locks and files.

        Called after after(), regardless of success or failure of the push.
        The repo lock, P4 connection, and context are still valid.

        """
        pass

    def process(self):
        """Do the actual work of processing the pre or post receive.

        :rtype: int
        :return: status code for the process upon exit.

        """
        # pylint:disable=no-self-use
        return 0

    def do_it(self):
        """Perform all of the setup, processing, and clean up.

        :rtype: int
        :return: status code for the process upon exit.

        """
        p4gf_util.log_environ(LOG, os.environ, self.label)
        log_l10n()
        p4gf_proc.install_stack_dumper()
        # Kick off garbage collection debugging, if enabled.
        p4gf_mem_gc.init_gc()

        # Use ExitStack to avoid deeply nested code.
        with ExitStack() as stack:
            stack.enter_context(p4gf_create_p4.Closer())
            p4 = p4gf_create_p4.create_p4_temp_client()
            if not p4:
                return 2
            repo_name = p4gf_path.cwd_to_repo_name()
            p4gf_util.reset_git_enviro()

            # Initialize the external process launcher early, before
            # allocating lots of memory, and just after all other
            # conditions have been checked.
            p4gf_proc.init()

            # Assume that something bad will happen (especially with preflight).
            exit_code = os.EX_SOFTWARE
            try:
                p4gf_log.configure_for_repo(repo_name)
                gid = os.environ[p4gf_const.P4GF_FORK_PUSH]
                self.before_p4key_lock(repo_name)
                with p4gf_lock.RepoLock(p4, repo_name, group_id=gid) as repo_lock:
                    # Work to be done with the p4key lock...
                    self.context = p4gf_context.create_context(repo_name)
                    self.context.p4gf = p4
                    self.context.repo_lock = repo_lock
                    self.context.foruser = os.getenv(p4gf_const.P4GF_FORUSER)
                    stack.enter_context(self.context)
                    self.before()
                    exit_code = self.process()
                if self.after_requires_write_lock():
                    # Work to be done without the p4key lock, but with the
                    # write lock. Note that we release the p4key lock
                    # before acquiring the write lock to avoid deadlock
                    # with the foreground process, which always gets the
                    # repo read/write lock _before_ acquiring the p4key
                    # lock. Hence all this complication with the locks.
                    with p4gf_git_repo_lock.write_lock(repo_name):
                        self.after(exit_code)
                else:
                    # The after() method does not need a write lock...
                    self.after(exit_code)
            finally:
                self.cleanup()
                p4gf_proc.stop()

        # Random tasks after all of the locks have been released.
        msg = NTR("at end of {hook}").format(hook=self.label)
        p4gf_mem_gc.process_garbage(msg)
        p4gf_mem_gc.report_objects(msg)
        return exit_code


class PreReceiveTupleLists:

    """Parse and categorize the PreReceiveTuple elements from stdin."""

    def __init__(self):
        """Initialize an empty tuples collection."""
        self.set_heads = []
        self.del_heads = []
        self.set_tags = []
        self.del_tags = []

    @staticmethod
    def from_stdin(stdin):
        """Create an instance from the given standard input stream."""
        # Read each input line (one for each pushed reference) and convert
        # it to a PreReceiveTuple and append it to the appropriate list.
        prl = PreReceiveTupleLists()
        while True:
            line = stdin.readline()
            if not line:
                break
            LOG.debug('raw pre-receive-tuple: {}'.format(line.strip()))
            prt = PreReceiveTuple.from_line(line)
            is_delete = int(prt.new_sha1, 16) == 0
            is_tag = not prt.ref.startswith('refs/heads/')
            if is_tag:
                if is_delete:
                    prl.del_tags.append(prt)
                else:
                    prl.set_tags.append(prt)
            else:
                if is_delete:
                    prl.del_heads.append(prt)
                else:
                    prl.set_heads.append(prt)
        return prl

    @staticmethod
    def from_dict(indict):
        """Create an instance from the given dict of dicts, as from to_dict."""
        prt = PreReceiveTupleLists()
        acquit = lambda prl: [PreReceiveTuple.from_dict(d) for d in prl]
        prt.set_heads = acquit(indict['set_heads'])
        prt.del_heads = acquit(indict['del_heads'])
        prt.set_tags = acquit(indict['set_tags'])
        prt.del_tags = acquit(indict['del_tags'])
        return prt

    def to_dict(self):
        """Return a dict representation of this object, for easy JSON formatting."""
        dictify_prl = lambda prl: [prt.to_dict() for prt in prl]
        result = dict()
        result['set_heads'] = dictify_prl(self.set_heads)
        result['del_heads'] = dictify_prl(self.del_heads)
        result['set_tags'] = dictify_prl(self.set_tags)
        result['del_tags'] = dictify_prl(self.del_tags)
        return result

    def heads(self):
        """Return all PreReceiveTuple elements for heads."""
        return itertools.chain(self.set_heads, self.del_heads)

    def tags(self):
        """Return all PreReceiveTuple elements for tags."""
        return itertools.chain(self.set_tags, self.del_tags)

    def sets(self):
        """Return all PreReceiveTuple elements for added/changed refs."""
        return itertools.chain(self.set_heads, self.set_tags)

    def deletes(self):
        """Return all PreReceiveTuple elements for deleted refs."""
        return itertools.chain(self.del_heads, self.del_tags)

    def all(self):
        """Return all PreReceiveTuple elements."""
        return itertools.chain(
                  self.set_heads
                , self.del_heads
                , self.set_tags
                , self.del_tags)

    def __str__(self):
        """Return a string representation of the pre-receive tuples."""
        sh = ", ".join([str(head) for head in self.set_heads])
        dh = ", ".join([str(head) for head in self.del_heads])
        st = ", ".join([str(head) for head in self.set_tags])
        dt = ", ".join([str(head) for head in self.del_tags])
        return "set_heads={}; del_heads={}; set_tags={}; del_tags={}".format(sh, dh, st, dt)


def _is_gitref_in_gf(ref, branch_dict, is_lightweight=True):
    """Retrieve the branch object if it is already in Git Fusion.

    Return the branch object if this git reference is already in GF, is not
    deleted, and is_lightweight status matches requested value.

    """
    git_branch_name = ref[len('refs/heads/'):]
    LOG.debug("_is_gitref_in_gf: branch name {} lw={}".format(git_branch_name, is_lightweight))
    branch = None
    for b in branch_dict.values():
        if (b.git_branch_name == git_branch_name and
                (is_lightweight == _DONT_CARE or is_lightweight == b.is_lightweight)
                and not b.deleted):
            branch = b
            break
    return branch


def _packet_filename(repo):
    """Generate the name of the packet file for the given repo.

    :type repo: str
    :param repo: name of repository.

    :return: path and name for packet file.

    """
    file_name = NTR('push-data-{repo}.json').format(repo=repo)
    return os.path.join(p4gf_const.P4GF_HOME, file_name)

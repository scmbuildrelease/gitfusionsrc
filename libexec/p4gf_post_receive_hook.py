#! /usr/bin/env python3.3
"""Translate the incoming Git commits to Perforce changes.

This hook is invoked by git-receive-pack on the remote repository, which
happens when a git push is done on a local repository. It executes on the
remote repository once after all the refs have been updated.

This file must be copied or symlinked into .git/hooks/post-receive

"""

from collections import OrderedDict
import functools
import json
import logging
import os
import sys

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_atomic_lock
import p4gf_branch
from p4gf_branch_id import Assigner
import p4gf_config
import p4gf_const
import p4gf_copy_to_p4
import p4gf_create_p4
from p4gf_fast_push import FastPush
import p4gf_fastexport_marks
import p4gf_git_repo_lock
from p4gf_git_swarm import GSReviewCollection
from p4gf_l10n import _
from p4gf_lfs_row import LFSRow
import p4gf_lock
import p4gf_log
from p4gf_new_depot_branch import NDBCollection
from p4gf_receive_hook import ReceiveHook, _packet_filename, _is_gitref_in_gf,\
    _DONT_CARE, PreReceiveTupleLists
from p4gf_prl_file import PRLFile
import p4gf_path
import p4gf_proc
from p4gf_profiler import with_timer, Timer, Report
from p4gf_push_limits import PushLimits
import p4gf_tag
import p4gf_tempfile
import p4gf_util
import p4gf_version_3
import p4gf_lfs_cache_prune

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_post_receive_hook")


class CopyOnlyHook(ReceiveHook):

    """Perform the copy-to-p4 work (assumes preflight happened)."""

    def __init__(self, label):
        """Initialize the copy-only hook object."""
        # we'll get prl from the packet in before()
        ReceiveHook.__init__(self, label, prl=None)
        self.assigner = None
        self.export_data = None
        self.all_marks = p4gf_fastexport_marks.Marks()
        self.gsreview_coll = None
        self.ndb_coll = None
        self.fast_push = None
        self.lfs_row_list = None
        self.atomic_lock_removed = False

    def before(self):
        """Reset the git references to their previous values."""
        ReceiveHook.before(self)
        self.prl, self.assigner, extras = read_packet(self.context)
        if extras.get("fast_push"):
            fast_push = FastPush.from_post_receive(self.context)
            if fast_push:
                self.fast_push = fast_push
                self.prl = fast_push.prl()
                self.ndb_coll = fast_push.ndb_coll()
                return
        if 'fast-export' in extras:
            self.export_data = {p4gf_branch.BranchRef(k): v for (k, v) in
                                extras.pop('fast-export').items()}
            for prt in self.prl.set_heads:
                marks = self.export_data[prt.ref]['marks']
                self.all_marks.add(prt.ref, marks)
        if 'gsreview' in extras:
            self.gsreview_coll = GSReviewCollection.from_dict(self.context, extras.pop('gsreview'))
        if 'ndb' in extras:
            self.ndb_coll = NDBCollection.from_dict(self.context, extras.pop('ndb'))
        if extras.get("lfs_row_list"):
            self.lfs_row_list = [LFSRow.from_dict(d)
                                 for d in extras.get("lfs_row_list")]

    def before_p4key_lock(self, repo_name):
        """Perform any work needed before the p4key lock ownership is transferred."""
        # Append our pid to the file-based write lock before the transfer
        # of the p4key lock is completed, so there is no gap in the file
        # lock. Once the p4key lock transfer is complete, the foreground
        # process is free to exit, releasing its hold on the write lock.
        p4gf_git_repo_lock.acquire_write_lock(repo_name, append=True)

    def after(self, ec):
        """Update git repository outside of p4key lock, with write lock."""
        p4gf_tag.process_tags(self.context, self.prl.tags())
        LOG.debug('after() performing review post-push processing')
        with Timer('swarm post-copy'):
            if self.gsreview_coll:
                self.gsreview_coll.post_push()
        with Timer('depot branch post-copy'):
            if self.ndb_coll:
                self.ndb_coll.post_push(self.context)
        ReceiveHook.after(self, ec)

    def after_requires_write_lock(self):
        """Return True if this hooks requires a write lock for after()."""
        return True

    def cleanup(self):
        """Remove the atomic lock and packet file."""
        # It may appear to be redundant to ensure the atomic lock is
        # removed here, when it is already (normally) removed in process().
        # However, while do_it() may fail to invoke process() at all, it
        # will invoke cleanup(), so this is our chance to ensure the atomic
        # lock is removed in the event of an error.
        self._remove_atomic_lock()
        p4gf_lfs_cache_prune.prune_lfs_file_cache()

    def _remove_atomic_lock(self):
        """Remove the atomic lock, if it has not already been removed."""
        if not self.atomic_lock_removed:
            p4gf_atomic_lock.lock_update_repo_reviews(
                self.context, action=p4gf_atomic_lock.REMOVE)
            self.atomic_lock_removed = True

    def process(self):
        """Perform the copy to Perforce work."""
        try:
            self._process_unsafe()
        finally:
            # Make sure the atomic lock is removed while we are holding the
            # exclusive p4key lock, lest we allow the foreground process to
            # acquire the p4key lock but fail (noisily) to acquire the
            # atomic lock.
            self._remove_atomic_lock()

    def _process_unsafe(self):
        """Perform the copy to Perforce work, possibly raising an exception."""
        ctx = self.context

        PRLFile(ctx.config.repo_name).delete()
        # Now that the PRL file has been dealt with, remove the write lock.
        p4gf_git_repo_lock.remove_write_lock(ctx.config.repo_name)

        # update the space usage values
        PushLimits(ctx).pre_copy()

        if self.fast_push:
            try:
                self.fast_push.post_receive()
            except Exception:  # pylint: disable=broad-except
                ctx.record_push_failed_p4key(sys.exc_info()[1])
                return 1
        else:
            if self.assigner:
                self._copy_heads()
            _delete_heads(ctx, self.prl)
            ctx.mirror.update_branches(ctx)

        # Update the total disk usage for the repo.
        with Timer('push limits'):
            PushLimits(ctx).post_copy()

        self.context.record_push_success_p4key()
        if not self.fast_push:
            _delete_packet(self.context.config.repo_name)
        return 0

    def _copy_heads(self):
        """For each of the heads being pushed, copy their commits to Perforce."""
        ctx = self.context
        gsreview = self.gsreview_coll
        try:
            branch_dict = ctx.branch_dict()
            for prt in self.prl.set_heads:
                LOG.debug("copy: current branch_dict:\n%s", p4gf_util.dict_to_log(branch_dict))
                LOG.debug("copy %s", prt)
                ref_is_review = gsreview and gsreview.ref_in_review_list(prt.ref)
                LOG.debug("prt.ref %s is_review %s ctx.swarm_reviews %s",
                          prt.ref, ref_is_review, ctx.swarm_reviews)
                g2p = p4gf_copy_to_p4.G2P(ctx, self.assigner, gsreview)
                if self.lfs_row_list:
                    g2p.lfs_row_list = self.lfs_row_list
                commits = self.export_data[prt.ref]['commits']
                g2p.copy(prt, commits, self.all_marks)
        except:
            # If the push fails to process successfully, be sure to
            # revert the disk space key values.
            PushLimits.push_failed(ctx)
            raise


@with_timer('delete refs')
def _delete_heads(ctx, prl):
    """For each of the heads being deleted, remove the branch definition from p4gf_config[2]."""
    heads = prl.del_heads
    if not heads:
        return
    branch_dict = ctx.branch_dict()

    for prt in heads:
        LOG.debug("delete %s", prt)
        branch = _is_gitref_in_gf(prt.ref, branch_dict, is_lightweight=_DONT_CARE)
        if not branch:
            # Branch is not known to Git Fusion branch, so we've nothing
            # more to do. But branch might still be in the Git repo. Let
            # Git sort that out, report its own Git errors if necessary.
            continue

        if branch.more_equal:
            raise RuntimeError(_(
                "Cannot delete {branch}. First branch defined in p4gf_config cannot be deleted.")
                .format(branch=branch.git_branch_name))
        LOG.info("branch %s marked deleted", branch.git_branch_name)
        branch.deleted = True
        with ctx.switched_to_branch(branch):
            branch.deleted_at_change = p4gf_util.head_change_as_string(ctx,submitted=True)


def read_packet(ctx):
    """Read the serialized Git Fusion data from a JSON encoded file.

    :type ctx: :class:`p4gf_context.Context`
    :param ctx: Git Fusion context

    :return: tuple (PreReceiveTupleLists, Assigner, extras dict) for normal push,
             (None, None, package) for fast push,
             or (None, None, None) if no packet file.

    """
    file_name = _packet_filename(ctx.config.repo_name)
    if os.path.exists(file_name):
        with open(file_name) as fobj:
            package = json.load(fobj, object_pairs_hook=OrderedDict)
            if package.get("fast_push"):
                return (None, None, package)
            # read the pre-receive tuples
            prl = PreReceiveTupleLists.from_dict(package.pop('prl'))
            # read the branch dictionary (likely modified by assigner in preflight)
            branch_dict = p4gf_branch.from_dict(package.pop('branch_dict'), ctx.config.p4client)
            ctx.reset_branch_dict(branch_dict)
            # read the branch assignment data
            assign_dict = package.pop('assigner')
            if assign_dict:
                assigner = Assigner.from_dict(assign_dict, branch_dict, prl.set_heads, ctx)
            else:
                assigner = None
            # read the configuration data
            config = p4gf_config.from_dict(package.pop('config'))
            config2 = None
            if 'config2' in package:
                config2 = p4gf_config.from_dict(package.pop('config2'))
            ctx.repo_config.set_repo_config(config, None)
            ctx.repo_config.set_repo_config2(config2, None)
            return (prl, assigner, package)
    return (None, None, None)


def _delete_packet(repo):
    """Remove the JSON packet file upon completion of a successful push.

    :type repo: str
    :param repo: name of the repo

    """
    file_name = _packet_filename(repo)
    if os.path.exists(file_name):
        LOG.debug('removed packet file %s', file_name)
        os.unlink(file_name)
    else:
        LOG.warning('packet file %s missing', file_name)


def forked_execed_main():
    """The main that runs after double-fork + exec."""
    LOG.debug('forked_execed_main() beginning copy-to-p4')
    try:
        p4gf_branch.init_case_handling()
        ec = CopyOnlyHook('post-receive').do_it()
        LOG.debug('forked_execed_main() returned %s', ec)
        p4gf_tempfile.prune_old_files()
        return ec
    finally:
        # The @atexit.register does not seem to fire for double-forked
        # Python processes, so call the profiler report explicitly.
        Report()
        LOG.debug('forked_execed_main() completed')


def forked_main():
    """The main invoked by post-receive hook in a double-forked process.

    To work around OS X fork unsafe issues, do an exec now.
    """
    os.execvp(__file__, sys.argv + ['--do-it-now'])


def main():
    """Do the post-receive work."""
    for h in ['-?', '-h', '--help']:
        if h in sys.argv:
            print(_('Git Fusion post-receive hook.'))
            return 2
    p4gf_version_3.print_and_exit_if_argv()

    # If P4GF_FORK_PUSH is not set, then this is not the genuine push
    # payload and simply a premlinary request made by the HTTP client.
    # In the case of SSH, it will always be set.
    if p4gf_const.P4GF_FORK_PUSH not in os.environ:
        return 0

    # Run this now to avoid the warning of p4gf_proc.init() when ps is
    # invoked in the lock acquisition code (and just because).
    p4gf_proc.init()

    # Indicate that the lock is about to be acquired by the upcoming
    # background process; the main server process will wait until the lock
    # acquisition is completed by the background process.
    LOG.debug('main() setting up forked process')
    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4_temp_client()
        repo_name = p4gf_path.cwd_to_repo_name()
        p4gf_log.configure_for_repo(repo_name)
        group_id = os.environ[p4gf_const.P4GF_FORK_PUSH]
        with p4gf_lock.RepoLock(p4, repo_name, group_id=group_id) as lock:
            lock.set_acquire_pending()

    # Spawn a process to do the work that pre-receive hook could not do
    # before git updated the references. This is an attempt to prevent
    # any timing issues with respect to git.
    LOG.debug('main() starting processing in forked process')
    func = functools.partial(forked_main)
    p4gf_proc.double_fork(func)
    LOG.debug('main() forked process initiated')

    return 0


if __name__ == "__main__":
    if '--do-it-now' in sys.argv:
        p4gf_log.run_with_exception_logger(forked_execed_main, write_to_stderr=True)
    else:
        p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

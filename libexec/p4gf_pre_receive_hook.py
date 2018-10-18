#! /usr/bin/env python3.3
"""Validate the incoming Git commits prior to translation.

This hook is invoked by git-receive-pack on the remote repository, which
happens when a git push is done on a local repository. Just before starting
to update refs on the remote repository, the pre-receive hook is invoked.
Its exit status determines the success or failure of the update.

This file must be copied or symlinked into .git/hooks/pre-receive

"""

import logging
import os
import sys

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_branch
from p4gf_branch_id import Assigner, PreReceiveTuple
import p4gf_config
import p4gf_const
from p4gf_fast_push import FastPush
import p4gf_fastexport_marks
from p4gf_git_swarm import GSReviewCollection
from p4gf_l10n import _
import p4gf_log
from p4gf_new_depot_branch import NDBCollection
import p4gf_object_type
from p4gf_preflight_checker import PreflightException
from p4gf_prl_file import PRLFile
import p4gf_proc
from p4gf_profiler import with_timer, Timer
from p4gf_push_limits import PushLimits
from p4gf_receive_hook import ReceiveHook, PreReceiveTupleLists \
        , _is_gitref_in_gf, _packet_filename
import p4gf_tag
import p4gf_util
import p4gf_version_3
import p4gf_copy_to_p4


LOG = logging.getLogger('p4gf_pre_receive_hook')
# Borrow git's standard message when GF rejects a non-FF push
MSG_HEAD_MOVED = _("""Perforce: Updates were rejected because the remote contains work that you do
Perforce: not have locally. This is usually caused by another repository pushing
Perforce: to the same ref. You may want to first merge the remote changes (e.g.,
Perforce: 'git pull') before pushing again.
Perforce: See the 'Note about fast-forwards' in 'git push --help' for details.
Perforce: remote branch {ref} at {head_sha1}""")


class PreflightHook(ReceiveHook):

    """Perform the preflight work."""

    def process(self):
        """Enforce preconditions before accepting an incoming push.

        If a preflight exception is raised, set the status key.

        :return: status code from process_throw() function.

        """
        try:
            return self.process_throw()
        except PreflightException as pe:
            self.context.record_push_rejected_p4key(pe)
            # Unnecessary, but nice-to-have a quieter log: don't cause a later
            # push/pull to try to roll back this push when we KNOW we told Git
            # it was unacceptable and won't need a rollback.
            PRLFile(self.context.config.repo_name).delete()
            raise

    def process_throw(self):
        """Enforce preconditions before accepting an incoming push.

        :return: status code, but always zero for now.
        :rtype: int

        """
        prl = self.prl
        ctx = self.context

        # Tell server_common about the refs that Git wants to move.
        PRLFile(ctx.config.repo_name).write(prl)

        # Delete the file that signals whether our hooks ran or not.
        fname = os.path.join(ctx.repo_dirs.repo_container, p4gf_const.P4GF_PRE_RECEIVE_FLAG)
        if os.path.exists(fname):
            os.unlink(fname)

        # reject pushes if not fast-forward
        _check_fast_forward(prl)

        # Swarm review creates new Git merge commits. Must occur before
        # branch assignment so that the review reference can be moved to
        # the new merge commit.
        with Timer('swarm pre-copy'):
            gsreview_coll = GSReviewCollection.from_prl(ctx, prl.set_heads)
            if gsreview_coll:
                gsreview_coll.pre_copy_to_p4(prl.set_heads)

        # New depot branches create new fully populated Branch definitions.
        # Must occur before branch assignment so that we can assign
        # incoming commits to these new branches.
        # Modifies PreReceiveTuple refs.
        with Timer('depot branch pre-copy'):
            ndb_coll = NDBCollection.from_prl(ctx, prl.set_heads, gsreview_coll)
            if ndb_coll:
                ndb_coll.pre_copy_to_p4()

        _preflight_check(ctx, prl.set_heads, gsreview_coll)
        self._preflight_tags()
        # do _not_ write changes to space consumption
        PushLimits(self.context).enforce(prl.set_heads)

        fast_push = FastPush.from_pre_receive(
                          ctx           = ctx
                        , prl           = prl
                        , gsreview_coll = gsreview_coll
                        , ndb           = ndb_coll
                        )
        if fast_push:
            fast_push.pre_receive()
            write_packet_fast_push(fast_push)
        else:
            self.prl = prl = _set_old_sha1_for_branch_adds(ctx, prl)
            assigner = _assign_branches(ctx, prl)
            export_data = None
            g2p = None
            if assigner:
                g2p = p4gf_copy_to_p4.G2P(ctx, assigner, gsreview_coll)
                export_data = self._preflight_heads(gsreview_coll, g2p)

            # Write background push packet to file as JSON for consumption in
            # background push processing (see CopyOnlyHook).
            extras = dict()
            if export_data:
                extras['fast-export'] = export_data
            if g2p and g2p.lfs_row_list:
                extras["lfs_row_list"] = [row.to_dict() for row in g2p.lfs_row_list]
            if gsreview_coll:
                # reset the handled state, we will process the reviews again in copy phase
                reviews = gsreview_coll.to_dict()
                for dikt in reviews['reviews']:
                    dikt['handled'] = False
                extras['gsreview'] = reviews
            if ndb_coll:
                extras['ndb'] = ndb_coll.to_dict()
            write_packet(ctx, assigner, prl, extras)

        # If receiving a push over SSH, or the push payload over HTTP,
        # report the push identifier to the user via standard error stream.
        # Any earlier in the process and HTTP will not deliver it, any
        # later and the connection will have already been closed.
        if p4gf_const.P4GF_FORK_PUSH in os.environ:
            sys.stderr.write(_("Commencing push {push_id} processing...\n")
                             .format(push_id=self.context.push_id))
            sys.stderr.flush()

        return 0

    def _preflight_heads(self, gsreview_coll, g2p):
        """For each of the heads being pushed, ensure the commits are valid.

        :param gsreview_coll: Git-Swarm review data
        :param g2p: G2P

        :return: dict of ref to dict of commits and marks from fast-export.

        """
        ctx = self.context
        export_data = dict()
        try:
            branch_dict = ctx.branch_dict()
            all_marks = p4gf_fastexport_marks.Marks()
            for prt in self.prl.set_heads:
                LOG.debug("preflight: current branch_dict %s", branch_dict)
                LOG.debug("preflight %s", prt)
                ref_is_review = gsreview_coll and gsreview_coll.ref_in_review_list(prt.ref)
                LOG.debug("prt.ref %s is_review %s ctx.swarm_reviews %s",
                          prt.ref, ref_is_review, ctx.swarm_reviews)
                commits, marks = p4gf_copy_to_p4.run_fast_export(ctx, prt)
                all_marks.add(prt.ref, marks)
                export_data[prt.ref] = {'commits': commits, 'marks': marks}
                g2p.preflight(prt, commits, all_marks)
        except:

            # If the push fails to process successfully, be sure to
            # revert the disk space key values.
            PushLimits.push_failed(ctx)
            raise
        return export_data

    def _preflight_tags(self):
        """Validate the incoming tags before moving on to copying."""
        # Also need the existing head references, if any
        repo = self.context.repo
        heads = [r for r in repo.listall_references() if r.startswith('refs/heads/')]
        heads.extend(prt.new_sha1 for prt in self.prl.set_heads)
        err = p4gf_tag.preflight_tags(self.context, self.prl.tags(), heads)
        if err:
            raise PreflightException(err)


def _set_old_sha1_for_branch_adds(ctx, prl):
    """Find the true parent of any new references.

    If any pre-receive tuple introduces a new reference (i.e. old SHA1 is all zeros)
    then find the true parent of the new SHA1 and replace the zeros.

    :return: updated list of pre-receive tuples.

    """
    new_prl = PreReceiveTupleLists()
    # duplicate the other tuples that we don't modify here
    new_prl.del_heads = prl.del_heads
    new_prl.set_tags = prl.set_tags
    new_prl.del_tags = prl.del_tags
    branch_dict = ctx.branch_dict()
    for head in prl.set_heads:
        if head.old_sha1 == p4gf_const.NULL_COMMIT_SHA1:
            # This appears to be a new branch reference, so check if it has
            # a parent somewhere in our previously translated history, and
            # use that commit as the true parent of this new branch, so we
            # avoid doing a whole lot of extra work.
            new_head = _find_true_parent(ctx.repo, head, branch_dict, ctx.repo_dirs.GIT_WORK_TREE)
            if p4gf_object_type.ObjectType.commits_for_sha1(ctx, new_head.old_sha1):
                LOG.info('updated pre-receive-tuple %s', new_head)
                head = new_head
        new_prl.set_heads.append(head)
    return new_prl


def _find_true_parent(repo, head, branch_dict, work_tree):
    """Find the closest parent commit for the given branch reference."""
    if not os.path.exists('.git'):
        # repository not yet initialized
        return head
    branch_names = set()
    # Find all non-deleted branches that Git already knows about...
    for branch in branch_dict.values():
        if branch.git_branch_name and not branch.deleted:
            if repo.lookup_branch(branch.git_branch_name):
                branch_names.add(branch.git_branch_name)
    # ...excluding the branch that is being introduced
    branch_names.discard(head.git_branch_name())
    # Turn all of those into exclusions for git-rev-list
    not_branches = ['^{}'.format(br) for br in branch_names]
    cmd = ['git', 'rev-list', '--date-order', '--parents'] + not_branches
    # Start git-rev-list from the new SHA1 that is being introduced.
    cmd.append(head.new_sha1)
    cwd = os.getcwd()
    os.chdir(work_tree)
    # Initialize p4gf_proc now that we've changed the cwd to the git repo
    # (we lack the functionality to change the cwd after the fact).
    p4gf_proc.init()
    result = p4gf_proc.popen(cmd)
    os.chdir(cwd)
    output = result['out'].strip()
    LOG.debug("_find_true_parent() output: %s", output)
    if len(output) == 0:
        return head
    # Extract the last SHA1 from the git-rev-list output, that is the true
    # parent of this new branch.
    sha1s = output[output.rfind('\n')+1:].split()
    LOG.debug("_find_true_parent() first parents: %s", sha1s)
    parent_sha1 = sha1s[1] if len(sha1s) > 1 else sha1s[0]
    return PreReceiveTuple(parent_sha1, head.new_sha1, head.ref)


def _assign_branches(ctx, prl):
    """Assign the commits to branches in Git Fusion.

    :param ctx: Git Fusion context.

    :type prl: :class:`p4gf_pre_receive_hook.PreReceiveTupleLists`
    :param prl: list of pushed PreReceiveTuple elements to be set.

    :return: branch assigner.

    """
    heads = prl.set_heads
    if not heads:
        return None
    branch_dict = ctx.branch_dict()
    LOG.debug2('allowing branch creation: %s', ctx.branch_creation)
    # Assign branches to each of the received commits for pushed branches
    assigner = Assigner(branch_dict, heads, ctx)
    assigner.assign()
    return assigner


@with_timer('preflight')
def _preflight_check(ctx, prl, gsreview_coll):
    """Perform a sanity check before inadvertently creating files.

    :param ctx: Git Fusion context.

    :type prl: :class:`p4gf_pre_receive_hook.PreReceiveTupleLists`
    :param prl: list of pushed PreReceiveTuple elements to be set

    :param gsreview_coll: Git-Swarm review meta data

    """
    LOG.debug('pre-receive preflight check for %s', ctx.config.repo_name)
    branch_dict = ctx.branch_dict()
    for prt in prl:
        branch = _is_gitref_in_gf(prt.ref, branch_dict, is_lightweight=False)
        ref_is_review = gsreview_coll and gsreview_coll.ref_in_review_list(prt.ref)
        if ref_is_review:
            if not ctx.swarm_reviews:
                raise RuntimeError(_(
                    "Swarm reviews are not authorized for this repo."
                    "\nRejecting push of '{ref}'.").format(ref=prt.ref))
        elif not ctx.branch_creation and not branch:
            raise RuntimeError(_(
                "Branch creation is not authorized for this repo."
                "\nRejecting push of '{ref}'.").format(ref=prt.ref))


def _check_fast_forward(prl):
    """Reject non-fast-forward pushes."""
    for prt in prl.set_heads:
        # head sha1 == old sha1?
        branch_head_sha1 = p4gf_util.git_rev_list_1(prt.ref)
        if not branch_head_sha1:
            branch_head_sha1 = '0' * 40
        LOG.debug("check for FF current %s old_sha1 %s",
                  branch_head_sha1, prt.old_sha1)
        if branch_head_sha1 != prt.old_sha1:
            raise RuntimeError(MSG_HEAD_MOVED.format(ref=prt.ref,
                                                     head_sha1=branch_head_sha1))


def write_packet(ctx, assigner, prl, extras=None):
    """Serialize the given Git Fusion data extras JSON.

    :type ctx: :class:`p4gf_context.Context`
    :param ctx: Git Fusion context

    :type assigner: :class:`p4gf_branch_id.Assigner`
    :param assigner: branch assignments

    :type prl: :class:`p4gf_pre_receive_hook.PreReceiveTupleLists`
    :param prl: pre-receive tuples list

    :type extras: dict
    :param extras: additional objects to serialize.

    """
    # serialize one or both config objects
    config1 = ctx.repo_config.repo_config
    config2 = ctx.repo_config.repo_config2
    package = dict()
    package['config'] = p4gf_config.to_dict(config1)
    if config2 is not None:
        package['config2'] = p4gf_config.to_dict(config2)
    package['assigner'] = assigner.to_dict() if assigner else None
    package['prl'] = prl.to_dict()
    # Load up the depot branch info data and integrate with the branch
    # dictionary before writing the packet.
    p4gf_branch.attach_depot_branch_info(ctx.branch_dict(), ctx.depot_branch_info_index())
    package['branch_dict'] = p4gf_branch.to_dict(ctx.branch_dict())
    if extras:
        package.update(extras)

    _write_packet_dict(ctx, package)


def _write_packet_dict(ctx, package_dict):
    """Write the given dict to a packet file.

    Packet file path uses ctx's repo name.
    """
    p4gf_util.write_dict_to_file(package_dict, _packet_filename(ctx.config.repo_name))


def write_packet_fast_push(fast_push):
    """Serialize a very small "this push was handled via FastPush" packet file."""
    package = {"fast_push": True}
    _write_packet_dict(fast_push.ctx, package)


def main():
    """Either do the work now or fork a process to do it later."""
    for h in ['-?', '-h', '--help']:
        if h in sys.argv:
            print(_('Git Fusion pre-receive hook.'))
            return 2
    p4gf_version_3.print_and_exit_if_argv()
    p4gf_branch.init_case_handling()
    prl = PreReceiveTupleLists.from_stdin(sys.stdin)
    # Preflight rejects the push by raising an exception, which is handled
    # in the logging code by printing the message to stderr.
    with Timer('pre-receive'):
        return PreflightHook('pre-receive preflight', prl).do_it()


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True,
                                       squelch_exceptions=[PreflightException])

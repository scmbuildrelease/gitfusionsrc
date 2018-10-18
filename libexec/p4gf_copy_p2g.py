#! /usr/bin/env python3.3
"""Copy change history from Perforce to git.

View must already be defined in Perforce: it must have its
"git-fusion-<view>" client with a Root and a View.

Git repo must already be inited in repo_dirs.GIT_DIR.
The repo can be empty.
"""

import logging
import os

import p4gf_const
import p4gf_copy_to_git
import p4gf_init_host
from   p4gf_l10n import _
import p4gf_proc
import p4gf_util
import p4gf_git

LOG = logging.getLogger(__name__)


def _has_commits(ctx):
    """Return True if this repo has commits on at least one named branch."""
    branch_dict = ctx.branch_dict()
    for v in branch_dict.values():
        if not v.git_branch_name:
            continue
        if p4gf_util.sha1_for_branch(v.git_branch_name):
            return True
    return False


def copy_p2g(ctx, start):
    """Fill git with content from Perforce."""
    ctx.checkpoint("copy_p2g.copy_p2g")
    repo_name = ctx.config.repo_name
    repo_dirs = ctx.repo_dirs
    git_dir = repo_dirs.GIT_DIR
    if not os.path.exists(git_dir):
        LOG.warning("mirror Git repository {} missing, recreating...".format(git_dir))
        # it's not the end of the world if the git repo disappears, just recreate it
        p4gf_init_host.create_git_repo(git_dir, ctx.git_autopack, ctx.git_gc_auto, ctx.p4)
    else:
        # Ensure the pack config settings are set to desired values.
        p4gf_init_host.update_git_config_and_repack(git_dir, ctx.git_autopack, ctx.git_gc_auto)
        # Ensure the pre-receive hook script is in place (it can happen).
        p4gf_init_host.install_hook(git_dir)

    # +++ Waste no time if nothing has changed in Perforce since the last
    #     time we copied.
    if not ctx.any_changes_since_last_seen():
        # But to help out admins and testers who delete the Git Fusion server
        # ~/.git-fusion/views/git/ directory without telling Git Fusion,
        # check for empty Git repo and don't optimize such cases.
        if not p4gf_util.git_empty():
            return

    # If Perforce client view is empty and git repo is empty, someone is
    # probably trying to push into an empty repo/perforce tree. Let them.
    if ctx.union_view_empty() and p4gf_util.git_empty():
        LOG.info(_("Nothing to copy from empty view {repo_name}").format(repo_name=repo_name))
        # Do this only as needed in this case.
        create_empty_repo_branch(ctx, git_dir)
        return

    # We're not empty anymore, we no longer need this to avoid
    # git push rejection of push to empty repo refs/heads/master.
    delete_empty_repo_branch(ctx, repo_dirs.GIT_DIR)

    # Remove branches from git which may have been deleted by another GF instance
    # Get list of new branches add to GF (by git or P4) by another GF instance -
    # for which this local git repo will yet have no branch ref
    copy_new_branches_to_git = synchronize_git_with_gf_branches(ctx)
    if copy_new_branches_to_git:
        LOG.debug("Found new GF branches to add to git:{0}".format(copy_new_branches_to_git))
    else:
        LOG.debug("No new GF branches to add to git:{0}".format(copy_new_branches_to_git))
    start_at = None
    if start is not None:
        # Does this git need to add a new GF branch
        # or does any branch in our branch dict contain a commit?
        has_commits = copy_new_branches_to_git or _has_commits(ctx)
        if has_commits:
            raise RuntimeError(_("Cannot use --start={start} when repo already has commits.")
                               .format(start=start))
    if start:
        start_at = "@{}".format(start)

    p4gf_copy_to_git.copy_p4_changes_to_git(ctx, start_at, "#head", copy_new_branches_to_git)
    ctx.checkout_master_ish()


def synchronize_git_with_gf_branches(ctx):
    """Synchronize git named refs with named branches in p4gf_config2.

    Remove branches from git which have been deleted by another GF instance.
    Return list of git branch names in GF but not now in git.
    Content from Helix for new these branches will be added to git by p4gf_copy_to_git.

    p4gf_config and p4gf_config contains lists of branch definitions.
    Deleted branches acquires "deleted = True" option.
    Branch definitions for a git branch will have git-branch-name set.
    The same git branch may be re-created with a new branch-id.
    GF retains all branch definitions as
    """
    # Get the list of git branches - and clean them somewhat
    cmd = ['git', 'branch']
    result = p4gf_proc.popen(cmd)
    git_branches = result['out'].replace('*', '').splitlines()
    git_branches = [x.strip() for x in git_branches]
    git_branches = [x for x in git_branches if not x.startswith('(no branch)')
            and not x.startswith('(detached from')]
    if not git_branches:       # no git branches - this must be during init repo
        return None
    # Using sets
    bnames_lw_active   = {b.git_branch_name for b in ctx.branch_dict().values()
                              if b.git_branch_name and b.is_lightweight and not b.deleted}
    bnames_lw_deleted  = {b.git_branch_name for b in ctx.branch_dict().values()
                          if b.git_branch_name and b.is_lightweight and b.deleted}
    bnames_fp_active   = {b.git_branch_name for b in ctx.branch_dict().values()
                              if not b.is_lightweight and not b.deleted}
    bnames_fp_deleted  = {b.git_branch_name for b in ctx.branch_dict().values()
                              if not b.is_lightweight and b.deleted}

    # Branches can be deleted and re-created.
    # All deleted branch definitions are retained after marking with deleted=True.
    # Using set difference.
    bnames_fp_deleted_current = bnames_fp_deleted - bnames_fp_active
    bnames_lw_deleted_current = bnames_lw_deleted - bnames_lw_active
    bnames_all_deleted_current = bnames_fp_deleted_current | bnames_lw_deleted_current
    bnames_all_active_current = bnames_fp_active | bnames_lw_active

    cmd = ['git', 'branch', '-D']
    git_branches_deleted = set()
    for b in git_branches:
        branch_name = b.split()[0]     # first item is branch name
        LOG.debug("synchronize: git branch: {}".format(branch_name))
        if branch_name in bnames_all_deleted_current:
            LOG.debug("Removing branch :{0}: from git".format(branch_name))
            p4gf_proc.popen(cmd + [branch_name])
            git_branches_deleted.add(branch_name)
    # Remove the just deleted branche names from our list of current git branches
    git_branches = set(git_branches) - git_branches_deleted
    # Return list of FP and LW branch names in GF but not in git
    new_branches = [b for b in bnames_all_active_current if b not in git_branches]
    return new_branches


def create_empty_repo_branch(ctx, git_dir):
    """Create and switch to branch empty_repo.

    This avoids Git errors when pushing to a brand-new empty repo which
    prohibits pushes to master.

    We'll switch to master and delete this branch later, when there's
    something in the repo and we can now safely detach HEAD from master.
    """
    master_ish = ctx.most_equal()
    with p4gf_git.non_bare_git(git_dir=git_dir):
        for branch in [ master_ish.git_branch_name
                      , p4gf_const.P4GF_BRANCH_EMPTY_REPO]:
            p4gf_proc.popen(['git', '--git-dir=' + git_dir, 'checkout', '-b', branch])


def delete_empty_repo_branch(_ctx, git_dir):
    """Delete branch empty_repo.

    If we are currently on that branch, detach head before switching.

    Only do this if our HEAD points to an actual sha1: we have to have
    at least one commit.
    """
    with p4gf_git.non_bare_git(git_dir=git_dir):
        p4gf_proc.popen_no_throw([ 'git', '--git-dir=' + git_dir
                                 , 'checkout', 'HEAD~0'])
        p = p4gf_proc.popen_no_throw(['git', '--git-dir=' + git_dir
                                     , 'branch', '--list',
                                     p4gf_const.P4GF_BRANCH_EMPTY_REPO])
        if p['out']:
            p = p4gf_proc.popen_no_throw(['git', '--git-dir=' + git_dir
                                         , 'branch', '-D'
                                         , p4gf_const.P4GF_BRANCH_EMPTY_REPO])


def copy_p2g_ctx(ctx, start=None):
    """Using the given context, copy its view from Perforce to Git.

    Common code for p4gf_auth_server.py and p4gf_init_repo.py for setting up
    the eventual call to copy_p2g.
    """
    # cd into the work directory. Not all git functions react well to --work-tree=xxxx.
    os.chdir(ctx.repo_dirs.GIT_WORK_TREE)

    # Fill git with content from Perforce.
    copy_p2g(ctx, start)

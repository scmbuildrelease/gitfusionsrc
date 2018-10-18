#! /usr/bin/env python3.3
"""Clone into an empty repo from the mirror in Perforce."""
import glob
import logging
import os
import re

import p4gf_git
import p4gf_object_type
import p4gf_path
import p4gf_proc
import p4gf_util


LOG = logging.getLogger(__name__)

MAX_TREES_PER_COMMIT = 2


def fast_reclone(ctx):
    """Try to do fast reclone from mirror."""
    # don't try this on a non-empty GF repo
    if not ctx.repo.is_empty:
        LOG.debug("fast_reclone: repo not empty")
        return None, None

    # write empty tree into repo, just in case it's referenced by any
    # commits we'll be copying
    result = p4gf_proc.popen_no_throw(['git', 'write-tree'])
    if result['ec']:
        LOG.debug('fast_reclone: failed to write empty tree: {}'.format(result))
        return None, None

    # copy commits and trees into the repo
    branch_heads, commits, dirs = _get_commits(ctx)
    trees = _get_trees(ctx, dirs)

    return branch_heads, commits + trees


BROKEN_REGEX = re.compile("^(broken link from |missing blob)")


def check_fast_reclone(objects):
    """Run git fsck to see if reclone worked.  If not, remove copied objects.

    Return True if clone is ok, else False.
    """
    # make sure the result is complete and correct
    # if not, clean up the mess we created and bail
    check = p4gf_git.git_fsck()
    if any(BROKEN_REGEX.match(item) for item in check['out'].split('\n')):
        for path in objects:
            os.unlink(path)
        return False
    return True


COMMIT_REGEX = re.compile("/(?P<slashed_sha1>[^-]+)"
                          "-(?P<branch_id>[^,]+)"
                          ",(?P<change_num>\\d+)")


def _get_commits(ctx):
    """Copy commit objects for repo from depot."""
    # sync the commit objects for the repo
    commit_path = p4gf_object_type.commit_depot_path('*', '*', ctx.config.repo_name, '*')
    ctx.p4run('sync', '-p', commit_path)
    # copy each commit into the repo, keeping track of the last commit on each
    # branch, as well as paths to all copied commits for possible later undo
    commits_root = os.path.join(ctx.repo_dirs.p4root,
                                'objects', 'repos', ctx.config.repo_name, 'commits')
    glob_path = os.path.join(commits_root, '*', '*', '*')
    branch_heads = {}
    commits = set()
    dirs = set()
    for path in glob.iglob(glob_path):
        # make sure the path is one of our commit objects
        m = COMMIT_REGEX.search(path[len(commits_root):])
        if not m:
            LOG.debug('_get_commits: no match')
            continue
        # extract info encoded into the path
        sha1 = m.group('slashed_sha1').replace('/', '')
        branch_id = m.group('branch_id')
        change_num = int(m.group('change_num'))
        # if this is a more recent commit on its branch, remember it
        if branch_id not in branch_heads or change_num > branch_heads[branch_id][1]:
            branch_heads[branch_id] = (sha1, change_num)
        # move the file into the git repo
        # a single commit may be duplicated on more than one branch in depot but
        # rename will overwrite if commit was already copied from a different branch
        git_path = os.path.join(ctx.repo_dirs.GIT_DIR, p4gf_util.sha1_to_git_objects_path(sha1))
        # path including only first two digits of sha1
        parent_dir = git_path[:-39]
        if parent_dir not in dirs:
            p4gf_util.ensure_parent_dir(git_path)
            dirs.add(parent_dir)
        os.rename(path, git_path)
        commits.add(git_path)
    return branch_heads, list(commits), dirs


def _get_trees(ctx, dirs):
    """Copy all trees (for all repos) from depot."""
    # sync all tree objects (not just for this repo)
    tree_path = p4gf_path.tree_p4_path('*')
    ctx.p4run('sync', '-p', tree_path)
    # copy each tree into the repo, keeping track of all copied trees for
    # possible later undo
    trees_root = os.path.join(ctx.repo_dirs.p4root, 'objects', 'trees')
    glob_path = os.path.join(trees_root, '*', '*', '*')
    LOG.debug('glob_path: {}'.format(glob_path))
    trees = set()
    for path in glob.iglob(glob_path):
        sha1 = path[len(trees_root):].replace('/', '')
        git_path = os.path.join(ctx.repo_dirs.GIT_DIR, p4gf_util.sha1_to_git_objects_path(sha1))
        # path including only first two digits of sha1
        parent_dir = git_path[:-39]
        if parent_dir not in dirs:
            p4gf_util.ensure_parent_dir(git_path)
            dirs.add(parent_dir)
        os.rename(path, git_path)
        trees.add(git_path)
    return list(trees)

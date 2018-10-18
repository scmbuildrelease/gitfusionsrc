#! /usr/bin/env python3.3
"""GitMirror class."""

from contextlib import ExitStack
import functools
import logging
import os
from   collections                  import namedtuple
import re
import sys

import p4gf_env_config    # pylint: disable=unused-import
import p4gf_atomic_lock
import p4gf_branch
import p4gf_config
import p4gf_const
import p4gf_context
import p4gf_create_p4
from p4gf_fastimport_mark import Mark
import p4gf_git
from p4gf_l10n import _, NTR
from p4gf_lfs_file_spec import lfs_depot_path
import p4gf_lock
import p4gf_log
from p4gf_object_type import ObjectType
import p4gf_object_type_util
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_path
import p4gf_proc
from p4gf_profiler import Timer
import p4gf_progress_reporter as ProgressReporter
import p4gf_pygit2
import p4gf_translate
import p4gf_util

from P4 import OutputHandler, P4Exception

Sha1ChangeBranch = namedtuple('Sha1ChangeBranch', ['sha1', 'change_num', 'branch_id'])

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_gitmirror")
MAX_COMMITS = 10000  # should the user configure 0, ignore it and use this.


def _get_max_commits_per_submit():
    """Read the int(value) from the git_to_perforce section of the config.
    :return: Default if value is not set.
    :raises RunError: if config value is malformed.

    """
    try:
        max_commits = p4gf_config.GlobalConfig.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_MIRROR_MAX_COMMITS_PER_SUBMIT)
        max_commits = int(max_commits)
        if not max_commits or max_commits == 0:
            max_commits = MAX_COMMITS
        return max_commits
    except ValueError:
        raise RuntimeError(_('global p4gf_config: {section}/{key}  is not a valid natural number.'
                             ' Please contact your administrator.')
                           .format(section=p4gf_config.SECTION_GIT_TO_PERFORCE,
                                   key=p4gf_config.KEY_MIRROR_MAX_COMMITS_PER_SUBMIT))

#
# patch up Context class, adding a mirror property with lazy init
# do this here rather than in p4gf_context.py to avoid import cycle
#


def __get_mirror(self):
    """return context's mirror, creating one if necessary."""
    # pylint: disable=protected-access
    if not hasattr(self, '_mirror'):
        self._mirror = GitMirror(self.config.repo_name)
    return self._mirror


def __set_mirror(self, value):
    """replace context's mirror value."""
    self._mirror = value    # pylint: disable=protected-access
p4gf_context.Context.mirror = property(__get_mirror, __set_mirror)


def __repr__with_mirror(self):
    return str(self) + "\n" + repr(self.mirror)
p4gf_context.__repr__ = __repr__with_mirror


_BITE_SIZE = 1000       # How many files to pass in a single 'p4 xxx' operation.
# If this is changed, also change mirror_boundary.t test so it submits this
# exact number of commits.

DESCRIPTION = _("Git Fusion '{repo}' copied to Git.")


class FilterAddFstatHandler(OutputHandler):

    """OutputHandler for p4 fstat, builds list of files that don't already exist."""

    def __init__(self):
        """Init the handler."""
        OutputHandler.__init__(self)
        self.files = []

    def outputMessage(self, m):
        """outputMessage call expected for any files not already added.

        otherwise indicates an error
        """
        try:
            if m.msgid == p4gf_p4msgid.MsgDm_ExFILE:
                self.files.append(m.dict['argc'])
        except Exception:  # pylint: disable=broad-except
            LOG.exception("outputMessage")
        return OutputHandler.REPORT


class MirrorObjects:

    """A collection of commit and tree ObjectType instances."""

    def __init__(self):
        """Init object."""
        self.pending_commits = {}   # commit+branches we need to add+submit
        self.pending_trees = set()  # trees we need to add+submit
        self.last_tree = None       # commit tree for last commit added
        self.pending_blobs = set()  # blobs we need to add+submit

    def add_commit(self, sha1, change_num, ctx, branch_id):
        """Queue commit for adding along with trees it references."""
        key = sha1
        if branch_id:
            key += ',' + branch_id

        assert key not in self.pending_commits

        commit = ObjectType.create_commit(sha1=sha1,
                                          repo_name=ctx.config.repo_name,
                                          change_num=change_num,
                                          branch_id=branch_id)

        self.pending_commits[key] = commit
        self._add_commits(ctx)

        with Timer(EXTRACT_OBJECTS):
            # as commits are added, build set of referenced trees
            if not self.last_tree:
                self.last_tree = _get_snapshot_trees(sha1, self.pending_trees, ctx.repo)
            else:
                self.last_tree = _get_delta_trees(self.last_tree, sha1, self.pending_trees,
                                                  ctx.repo)

    def flush(self, ctx):
        """Add any pending commits and tree objects to the numbered change."""
        # If there are no new commits, there will be no new trees.
        # But there could be commits but no trees if all trees were previously
        # referenced by other commits.
        self._add_commits(ctx, force=True)
        self._add_trees(ctx)
        self._add_blobs(ctx)

    def clear(self):
        """Clear any pending commits and trees.

        Remember any added commits and trees.

        TODO: should maybe not remember anything if submit fails?
        """
        self.pending_commits.clear()
        self.pending_trees.clear()
        self.pending_blobs.clear()
        self.last_tree = None

    def _add_commits(self, ctx, force=False):
        """Add any pending commit objects.

        P4 add any pending commits, if there are _BITE_SIZE commits pending.
        If force is True, p4 add any pending commits, no matter how few.

        """
        if not self.pending_commits:
            return
        if not force and len(self.pending_commits) < _BITE_SIZE:
            return
        LOG.debug("_add_commit: pending_commits = {0}".format(len(self.pending_commits)))
        _try_twice(functools.partial(_add_commits_to_p4, ctx, self.pending_commits))
        self.pending_commits.clear()

    def _add_trees(self, ctx):
        """Add any pending tree objects."""
        if not self.pending_trees:
            return
        _try_twice(functools.partial(_add_trees_to_p4, ctx, self.pending_trees))
        self.pending_trees.clear()
        self.last_tree = None

    def _add_blobs(self, ctx):
        """Add any pending blob objects."""
        if not self.pending_blobs:
            return
        _try_twice(functools.partial(_add_blobs_to_p4, ctx, self.pending_blobs))
        self.pending_blobs.clear()


# timer/counter names
OVERALL = NTR('GitMirror Overall')
EXTRACT_OBJECTS = NTR('extract objects')
P4_FSTAT = NTR('p4 fstat')
P4_ADD = NTR('p4 add')
P4_SUBMIT = NTR('p4 submit')


class GitMirror:

    """Handle git things that get mirrored in Perforce."""

    def __init__(self, repo_name):
        """Object init."""
        self.objects = MirrorObjects()
        self.repo_name = repo_name

        # List of branches that we write to config2:
        # - lightweight branches, whether or not they have a
        #   git-branch-name. Includes any deleted lightweight
        #   branches.
        # - branches based on a stream: we record the stream's
        #   original view to config2, even though the branch's
        #   main definition remains in config not config2.
        self._branch_list_config_2 = None

        self.depot_branch_info_list = None

        # List of depot files already written to local disk by
        # ChangelistDataFile.write().
        # Most are new (require 'p4 add'), some will be updates
        # to existing (require 'p4 sync -k' + 'p4 edit')
        self.changelist_data_file_list = None

    def add_objects_to_p4(self, marks, mark_list, mark_to_branch_id, ctx):
        """Submit Git commit and tree objects associated with the given marks.

        marks:      list of commit marks output by git-fast-import
                    formatted as: :marknumber sha1 branch-id
        mark_list:  MarkList instance that maps mark number to changelist number.
                    Can be None if mark number == changelist number.
        mark_to_branch_id:
                    dict to find branch_id active when mark's commit
                    was added.
                    Can be None if branch_id encoded in mark lines.
        ctx:        P4GF context
        """
        branch_dict = ctx.branch_dict()
        max_commits_per_submit = _get_max_commits_per_submit()
        LOG.debug2("add_objects_to_p4: max_commits_to_submit {0}".format(
            max_commits_per_submit))
        sha1_change_branch = []
        try:
            with ProgressReporter.Indeterminate():
                desc = DESCRIPTION.format(repo=ctx.config.repo_name)
                with Timer(OVERALL):
                    for mark_line in marks:
                        mark = Mark.from_line(mark_line)
                        mark_num = mark.mark
                        if mark_list:
                            change_num = mark_list.mark_to_cl(mark_num)
                        else:
                            change_num = mark_num
                        sha1 = mark.sha1
                        branch_id = mark.branch
                        if (not branch_id) and mark_to_branch_id:
                            branch_id = mark_to_branch_id.get(mark_num)
                        if (branch_id in branch_dict and branch_dict[branch_id].start_at_change and
                                int(change_num) < branch_dict[branch_id].start_at_change):
                            LOG.debug2("add_objects_to_p4: SKIP {}/{} < start_at_change={}".format(
                                branch_id, change_num, branch_dict[branch_id].start_at_change))
                            continue
                        sha1_change_branch.append(Sha1ChangeBranch(sha1=sha1,
                                                                   change_num=change_num,
                                                                   branch_id=branch_id))

                        if len(sha1_change_branch) >= max_commits_per_submit:
                            self.add_commits_in_numbered_changelist(sha1_change_branch, ctx, desc)
                            sha1_change_branch = []

                    # Add the last commits
                    if len(sha1_change_branch):
                        self.add_commits_in_numbered_changelist(sha1_change_branch, ctx, desc)

        finally:
            # Let my references go!
            self.objects.clear()
            sha1_change_branch = []

    def add_commits_in_numbered_changelist(self, sha1_change_branch, ctx, desc):
        """Using a new NumberChangelist, submit the list of commits and trees."""
        with p4gf_util.NumberedChangelist(gfctx=ctx,
                                           description=desc) as nc:
            for scb in sha1_change_branch:
                # add commit object
                self.objects.add_commit(sha1=scb.sha1,
                                change_num=scb.change_num,
                                ctx=ctx,
                                branch_id=scb.branch_id)
            self.flush_and_submit(ctx, nc)

    def flush_and_submit(self, ctx, nc):
        """Flush the pending commits/trees by adding to P4. Then submit."""

        # add the remaining objects to P4
        self.objects.flush(ctx)
        opened = ctx.p4gfrun('opened')
        if opened:
            ProgressReporter.increment(
                _('Submitting new Git commit objects to Perforce'))
            with Timer(P4_SUBMIT):
                r = nc.submit_with_retry()
            if r:
                ObjectType.update_indexes(ctx, r)
        else:
            ProgressReporter.write(
                _('No new Git objects to submit to Perforce'))
            LOG.debug("ignoring empty change list...")

    def add_blob(self, sha1):
        """Add given SHA1 to pending blob list."""
        self.objects.pending_blobs.add(sha1)

    def update_branches(self, ctx):
        """Add/update files for new/changed branches."""
        # update config files for new/changed branches
        with Timer(OVERALL):
            self.add_depot_branch_infos(ctx)
            self.add_branch_config2(ctx)
            write_config1(ctx)
            self._add_config2_branch_defs_to_p4(ctx)
            desc = DESCRIPTION.format(repo=ctx.config.repo_name)
            with p4gf_util.NumberedChangelist(gfctx=ctx, description=desc) as nc:
                depot_branch_infos_added = self._add_depot_branch_infos_to_p4(ctx)
                cldfs_added = self._add_cldfs_to_p4(ctx)
                if depot_branch_infos_added or cldfs_added:
                    ProgressReporter.increment(
                        _('Submitting branch updates to Perforce'))
                    with Timer(P4_SUBMIT):
                        nc.submit()

    def _add_depot_branch_infos_to_p4(self, ctx):
        """Add branch-info files to Perforce for new depot branches.

        If we created any new depot branches, 'p4 add' their branch-info
        file to Perforce. Does not submit.

        If we edited any existing depot branches, 'p4 edit' them.

        Return number of branch-info files added or edited.
        """
        if not self.depot_branch_info_list:
            LOG.debug2('_add_depot_branch_infos_to_p4() nothing to add')
            return 0

        add_path_list = []
        edit_path_list = []
        for dbi in self.depot_branch_info_list:
            local_path = _write_depot_branch_info_local_file(ctx, dbi)
            if dbi.needs_p4add:
                add_path_list.append(local_path)
            else:
                edit_path_list.append(local_path)
            # Mark as written to P4
            dbi.needs_p4add = False
            dbi.needs_p4edit = False

        with Timer(P4_ADD):
            success_list = _p4_add_in_bites(ctx, add_path_list)
        success_list.extend(_p4_sync_k_edit(ctx, edit_path_list))
        LOG.debug3('_add_depot_branch_infos_to_p4() adding {}'.format(success_list))
        self.depot_branch_info_list = None
        return len(success_list)

    def _add_config2_branch_defs_to_p4(self, ctx):
        """Update p4gf_config2 file  with new branches.

        If we defined any new named+lightweight branches, update (or write the
        first revision of) this repo's p4gf_config2 file with all the
        currently defined named+lightweight branches.
        """
        # pylint:disable=too-many-branches, too-many-statements

        # Nothing to write? well maybe we have just deleted the remaining refs
        have_branches = bool(self._branch_list_config_2)

        # What do we want the file to look like? ConfigParser writes only to
        # file, not to string, so we have to give it a file path. Ooh! I know!
        # How about writing to the very file that we have to 'p4 add' or 'p4
        # edit' if its content differs?
        if have_branches:
            for b in p4gf_branch.ordered(self._branch_list_config_2):
                LOG.debug("add branch to config2 {0}".format(b))
                b.add_to_config(ctx.repo_config.repo_config2)

        return ctx.repo_config.write_repo2_if(ctx.p4gf)

    def _add_cldfs_to_p4(self, ctx):
        """Open ChangelistDataFiles for add or edit.

        If we have any ChangelistDataFile local paths in
        changelist_data_file_list, 'p4 add' or 'p4 sync -k' + 'p4 edit' them
        now.

        Return True if we added/edited at least one file, False if not.
        """
        if not self.changelist_data_file_list:
            return False

        # Rather than run 'p4 opened' and then 'p4 sync' + 'p4
        # edit -k' on any files we failed to 'p4 add', we can
        # save a lot of thinking by just blindly 'p4 add'ing and
        # 'p4 edit'ing all files. Yeah it's stupid, yeah it will
        # pollute logs with a lot of warnings.
        with Timer(P4_ADD), ctx.p4gf.at_exception_level(ctx.p4gf.RAISE_NONE):
            ctx.p4gfrun('sync', '-k', self.changelist_data_file_list)
            ctx.p4gfrun('edit', '-k', self.changelist_data_file_list)
            ctx.p4gfrun('add',        self.changelist_data_file_list)

        self.changelist_data_file_list = None
        return True

    def add_depot_branch_infos(self, ctx):
        """Add branch_info files for new depot branches.

        If we created any new depot branches to house lightweight branches,
        record a branch_info file for each new depot branch.
        """
        result = []
        branches = ctx.depot_branch_info_index().by_id.values()
        LOG.debug3('add_depot_branch_infos() branches to consider {}'.format(branches))
        for dbi in branches:
            if dbi.needs_p4add or dbi.needs_p4edit:
                result.append(dbi)
        LOG.debug3('add_depot_branch_infos() branches to add/edit {}'.format(result))
        self.depot_branch_info_list = result

    def add_changelist_data_file_list(self, cldf_list):
        """Remember a list of local file paths to add later.

        We'll eventually add at the same time we add all our other files.
        """
        self.changelist_data_file_list = cldf_list

    def add_branch_config2(self, ctx):
        """Record added branches in p4gf_config2 file.

        If we defined any new lightweight branches, record those mappings
        in p4gf_config2.
        If we have any stream-based branches, record their initial views
        in p4gf_config2.
        """
        self._branch_list_config_2 = [b for b in ctx.branch_dict().values()
                                      if b.is_worthy_of_config2()]

    @staticmethod
    def integ_lfs_files(ctx, lfs_files):
        """Integ lfs files from source files to sha256 mirrors."""
        if not len(lfs_files):
            return
        desc = DESCRIPTION.format(repo=ctx.config.repo_name)
        with p4gf_util.NumberedChangelist(gfctx=ctx, description=desc) as nc:
            for sha256, depot_path in lfs_files.items():
                ctx.p4gfrun('integ', depot_path, lfs_depot_path(ctx, sha256))
            with Timer(P4_SUBMIT):
                nc.submit_with_retry()

    def __str__(self):
        """Return string representation of this Git mirror."""
        return 'GitMirror for {0}'.format(self.repo_name)

# -- module-level ------------------------------------------------------------


def get_last_change_for_commit(commit, ctx, branch_id=None):
    """Given a commit SHA1, find the latest corresponding Perforce change.

    Note that a Git commit may correspond to several Perforce changes.
    """
    return ObjectType.sha1_to_change_num(ctx, commit, branch_id)


def write_depot_branch_info_local_file(ctx, dbi):
    """Write a DepotBranchInfo to a file in the perforce client workspace.

    Wrapper function for the sake of entering OVERALL Timer.
    """
    with Timer(OVERALL):
        return _write_depot_branch_info_local_file(ctx, dbi)


def _write_depot_branch_info_local_file(ctx, dbi):
    """Write a DepotBranchInfo to a file in the perforce client workspace.

    Create a new local file to house the given DepotBranchInfo's spec,
    fill it, and return the local path to it.

    Return the path of the client workspace file suitable for  use with
    p4 add.
    """
    with Timer(EXTRACT_OBJECTS):
        config = dbi.to_config()
        depot_path = dbi.to_config_depot_path()
        local_path = p4gf_util.depot_to_local_path(depot_path,
                                                   ctx.p4gf,
                                                   ctx.client_spec_gf)
        p4gf_util.ensure_dir(p4gf_util.parent_dir(local_path))
        p4gf_util.make_writable(local_path)
        with open(local_path, 'w') as f:
            config.write(f)
        p4gf_config.clean_up_parser(config)
    return local_path


def write_config1(ctx):
    """Update p4gf_config file with new or deleted branches.

    If we defined any new or deleted any existing fully populated branches,
    write this repo's p4gf_config file with the updated branch definitions.

    This function only adds new or modifies (marks deleted)
    existing branch definitions. It cannot remove.

    Return True if written, False if not (nothing to change).
    """
    for b in ctx.branch_dict().values():
        if not b.is_lightweight:
            b.add_to_config(ctx.repo_config.repo_config)
    result = ctx.repo_config.write_repo_if(ctx=ctx)
    map_tuple_list = p4gf_branch.calc_writable_branch_union_tuple_list(
        ctx.p4.client, ctx.branch_dict(), ctx.repo_config.repo_config)
    p4gf_atomic_lock.update_all_gf_reviews(ctx, map_tuple_list)
    return result


def _p4_sync_k_edit(ctx, path_list):
    """'p4 sync -k' then 'p4 edit' the paths.

    Return list of depotFile successfully opened.
    """
    if not path_list:
        return []

    ctx.p4gfrun('sync', '-k', path_list)
    ctx.p4gfrun('edit', '-k', path_list)

    l = p4gf_p4msg.find_msgid(ctx.p4gf, p4gf_p4msgid.MsgDm_OpenSuccess)
    success_list = [x['depotFile'] for x in l]
    return success_list


def _p4_add_in_bites(ctx, path_list):
    """'p4 add' all the files in path_list.

    Return list of depotFile successfully opened.
    """
    success_list = []
    for bite in _spoon_feed(path_list):
        r = ctx.p4gfrun('add', bite)
        success_list.extend(
            [x['depotFile'] for x in r
             if isinstance(x, dict) and 'action' in x and x['action'] == 'add'])

    LOG.debug('_p4_add_in_bites() want={} success={}'
              .format(len(path_list), len(success_list)))
    return success_list


def _add_commits_to_p4(ctx, commits):
    """Run p4 add to create mirror files in .git-fusion."""
    LOG.debug("adding {0} commits to .git-fusion...".
              format(len(commits)))

    # build list of objects to add, extracting them from git
    add_files = [_extract_commit_to_p4(ctx, go)
                 for go in commits.values()]
    add_files = _optimize_objects_to_add_to_p4(ctx, add_files)

    if not len(add_files):
        # Avoid a blank line in output by printing something
        ProgressReporter.write(_('No Git objects to submit to Perforce'))
        LOG.debug("_add_commits_to_p4() nothing to add...")
        return

    _add_objects_to_p4(ctx, add_files)
    ObjectType.reset_cache()


def _extract_commit_to_p4(ctx, go):
    """Extract a commit to the git-fusion perforce client workspace.

    Return the path of the client workspace file suitable for use with
    p4 add.
    """
    ProgressReporter.increment(_('Adding new Git commit objects to Perforce...'))

    # get client path for .git-fusion file
    dst = os.path.join(ctx.gitlocalroot, go.to_p4_client_path())

    if os.path.exists(dst):
        LOG.debug2("reusing existing commit: " + dst)
        return dst

    with Timer(EXTRACT_OBJECTS):
        # Extract the object and write to file
        p4gf_git.write_git_object_from_sha1(ctx.repo, go.sha1, dst)
        LOG.debug2("adding new object: " + dst)

    return dst


# line is: mode SP type SP sha TAB path
# we only want the sha from lines with type "tree"
TREE_REGEX = re.compile("^[0-7]{6} tree ([0-9a-fA-F]{40})\t.*")


def _get_snapshot_trees(commit, trees, view_repo):
    """Get all tree objects for a given commit.

    commit: SHA1 of commit
    trees: set of all trees seen so far
    view_repo: pygit2 repo

    Each tree found is added to the set to be mirrored.
    """
    # get top level tree and make sure we haven't already processed it
    commit_tree = p4gf_pygit2.object_to_sha1(view_repo.get(commit).tree)
    if commit_tree in trees:
        return commit_tree

    # ls-tree doesn't return the top level tree, so add it here
    trees.add(commit_tree)
    LOG.debug2("adding (total={0}) commit tree {1}".format(len(trees), commit_tree))

    # use git ls-tree to find all subtrees in this commit tree
    po = p4gf_proc.popen_no_throw(['git', 'ls-tree', '-rt', commit_tree])['out']
    for line in po.splitlines():
        m = TREE_REGEX.match(line)
        if m:
            trees.add(m.group(1))
            LOG.debug2("adding (total={0} subtree {1}".format(len(trees), m.group(1)))
    return commit_tree

# line is: :mode1 SP mode2 SP sha1 SP sha2 SP action TAB path
# we want sha2 from lines where mode2 indicates a dir
TREE_ENT_RE = re.compile("^:[0-7]{6} 04[0-7]{4} [0-9a-fA-F]{40} ([0-9a-fA-F]{40}) .*")


def _get_delta_trees(commit_tree1, commit2, trees, view_repo):
    """Get all tree objects new in one commit vs another commit.

    commit_tree1: SHA1 of first commit's tree
    commit2: SHA1 of second commit
    trees: set of all trees seen so far
    view_repo: pygit2 repo

    Each tree found is added to the set to be mirrored.
    """
    # get top level tree and make sure we haven't already processed it
    commit_tree2 = p4gf_pygit2.object_to_sha1(view_repo.get(commit2).tree)
    if commit_tree2 in trees:
        return commit_tree2

    # diff-tree doesn't return the top level tree, so add it here
    trees.add(commit_tree2)
    LOG.debug2("adding (total={0}) commit tree {1}".format(len(trees), commit_tree2))

    # use git diff-tree to find all subtrees not in previous commit tree
    po = p4gf_proc.popen_no_throw(['git', 'diff-tree', '-t', commit_tree1, commit_tree2])['out']
    for line in po.splitlines():
        m = TREE_ENT_RE.match(line)
        if m:
            trees.add(m.group(1))
            LOG.debug2("adding (total={0}) subtree {1}".format(len(trees), m.group(1)))
    return commit_tree2


def _add_trees_to_p4(ctx, trees):
    """Run p4 add, submit to create mirror files in .git-fusion."""
    LOG.debug("adding {} trees to .git-fusion..., change={}"
              .format(len(trees), ctx.numbered_change_gf))
    LOG.debug2("trees to add: {}".format(trees))

    # build list of trees to add, extracting them from git
    add_files = []
    for sha1 in list(trees):
        relative_path = 'trees/' + p4gf_path.slashify_sha1(sha1)
        add_files.append(_extract_object_to_p4(ctx, sha1, relative_path))
    add_files = _optimize_objects_to_add_to_p4(ctx, add_files)
    if not len(add_files):
        return
    _add_objects_to_p4(ctx, add_files)


def _add_blobs_to_p4(ctx, blobs):
    """Run p4 add, submit to create mirror files in .git-fusion."""
    LOG.debug("adding {} blobs to .git-fusion..., change={}".format(
        len(blobs), ctx.numbered_change_gf))
    LOG.debug2("blobs to add: {}".format(blobs))

    # build list of blobs to add, extracting them from git
    add_files = []
    for sha1 in list(blobs):
        relative_path = 'blobs/' + p4gf_path.slashify_blob_sha1(sha1)
        add_files.append(_extract_object_to_p4(ctx, sha1, relative_path))
    add_files = _optimize_objects_to_add_to_p4(ctx, add_files)
    if not len(add_files):
        return
    _add_objects_to_p4(ctx, add_files)


def _extract_object_to_p4(ctx, sha1, local_path):
    """Extract an object into the git-fusion perforce client workspace.

    Return the path of the client workspace file suitable for use with p4 add.

    :param ctx: Git Fusion context.
    :param str sha1: SHA1 of the object to extract.
    :param str local_path: trailing path for local file.

    """
    # get client path for .git-fusion file
    dst = os.path.join(ctx.gitlocalroot, 'objects', local_path)
    # An object is likely to already exist, in which case we don't need or
    # want to try to recreate it. We'll just use the existing one.
    if os.path.exists(dst):
        LOG.debug("reusing existing git tree object: " + dst)
        return dst
    with Timer(EXTRACT_OBJECTS):
        # Extract object from repo and write to dst
        p4gf_git.write_git_object_from_sha1(ctx.repo, sha1, dst)
    return dst


def _optimize_objects_to_add_to_p4(ctx, add_files):
    """If many files to add, filter out those which are already added.

    Only do this if the number of files is large enough to justify
    the cost of the fstat
    """
    original_count = len(add_files)
    enough_files_to_use_fstat = 100
    if original_count < enough_files_to_use_fstat:
        return add_files
    with Timer(P4_FSTAT):
        LOG.debug("using fstat to optimize add")
        ctx.p4gf.handler = FilterAddFstatHandler()
        for bite in _spoon_feed(add_files):
            with ctx.p4gf.at_exception_level(ctx.p4gf.RAISE_NONE):
                ctx.p4gf.run("fstat", bite)
        add_files = ctx.p4gf.handler.files
        ctx.p4gf.handler = None
        LOG.debug("{} files removed from add list"
                  .format(original_count - len(add_files)))
        return add_files


def _add_objects_to_p4(ctx, add_files):
    """'p4 add' Git tree and commit objects to Perforce. Does not submit."""
    with Timer(P4_ADD):
        treecount = 0
        commitcount = 0
        for bite in _spoon_feed(add_files):
            result = ctx.p4gfrun('add', '-t', 'binary+F', bite)
            for r in result:
                if isinstance(r, dict) and r["action"] != 'add' or \
                   isinstance(r, str) and r.find("currently opened for add") < 0:
                    # file already exists in depot, perhaps?
                    LOG.debug(r)
                elif isinstance(r, dict):
                    if p4gf_object_type_util.OBJPATH_TREE_REGEX.search(r["depotFile"]):
                        treecount += 1
                    else:
                        commitcount += 1
    LOG.debug("Added {} commits and {} trees"
              .format(commitcount, treecount))


def _spoon_feed(items, bite_size=_BITE_SIZE):
    """Break a list of items in bite size chunks.

    When running p4 commands (e.g. add, fstat) on many files, it works
    better to break it up into several commands with smaller lists of files.
    """
    while items:
        bite = items[:bite_size]
        items = items[bite_size:]
        yield bite


def _try_twice(func):
    """Call func and retry if there's a P4Exception raised.

    At most one retry will be made.
    """
    for i in range(2):
        try:
            return func()
        except P4Exception:
            if i:
                raise


def add_missing_objects(ctx):
    """Ensure all tree objects are submitted to the object cache."""
    # get the SHA1 of every reachable commit in the repo
    if not os.path.exists(ctx.repo_dirs.GIT_WORK_TREE):
        raise RuntimeError(_('not local to this server'))
    os.chdir(ctx.repo_dirs.GIT_WORK_TREE)
    cmd_result = p4gf_proc.popen(['git', 'rev-list', '--all'])
    known_commits = cmd_result['out'].splitlines()

    # find all reachable tree objects in the repo
    last_tree = None
    known_trees = set()
    for sha1 in known_commits:
        if not last_tree:
            last_tree = _get_snapshot_trees(sha1, known_trees, ctx.repo)
        else:
            last_tree = _get_delta_trees(last_tree, sha1, known_trees, ctx.repo)

    # In batches, find those trees that are missing from the cache. The
    # important thing is not to request all cached trees, which could be a
    # huge number, nor to query for each and every tree object in the repo,
    # which would result in many requests to the server.
    last_prefix = ''
    cached_trees = None
    pending_trees = set()
    # Could have some smarts here that switches to a 4-character prefix if
    # the number of results retrieved on the first call is greater than
    # some reasonable number (e.g. 100,000).
    desc = _("Add missing tree objects for {repo_name}").format(repo_name=ctx.config.repo_name)
    with p4gf_util.NumberedChangelist(gfctx=ctx, description=desc) as nc:
        for tree in sorted(list(known_trees)):
            tree_prefix = tree[0:2]
            if last_prefix != tree_prefix:
                cached_trees = trees_for_prefix(ctx, tree_prefix)
                last_prefix = tree_prefix
            if tree not in cached_trees:
                pending_trees.add(tree)
                if len(pending_trees) > _BITE_SIZE:
                    # submit the trees in batches to avoid filling memory
                    _add_trees_to_p4(ctx, pending_trees)
                    pending_trees.clear()
        if pending_trees:
            _add_trees_to_p4(ctx, pending_trees)
            pending_trees.clear()
        opened = ctx.p4gfrun('opened')
        if opened:
            nc.submit()


def count_missing_commits(ctx, known_commits, verbose=False):
    """Count the number of commits missing from the cache.

    :param ctx: context for the repo of interest.
    :param known_commits: list of known commit SHA1s.
    :param bool verbose: True to print individual missing objects.

    :return: number of missing commits.
    :rtype: int

    """
    # retrieve all known cached commit objects
    commit_path = NTR('{0}/repos/{1}/commits/...').format(
        p4gf_const.objects_root(), ctx.config.repo_name)
    p4_result = ctx.p4run('files', '-e', commit_path)
    cached_commits = set()
    for rr in p4_result:
        depot_path = rr.get('depotFile')
        if not depot_path:
            continue
        m = p4gf_object_type_util.OBJPATH_COMMIT_REGEX.search(depot_path)
        if not m:
            continue
        sha1 = m.group(NTR('slashed_sha1')).replace('/', '')
        cached_commits.add(sha1)

    # report how many commits are missing from the cache
    missing_commits = 0
    for commit in known_commits:
        if commit not in cached_commits:
            missing_commits += 1
            if verbose:
                print(_("missing commit {commit}").format(commit=commit))
    return missing_commits


def trees_for_prefix(ctx, prefix):
    """Retrieve the list of cached tree objects for a given prefix (e.g. "e4").

    :param ctx: context for the repo of interest.
    :param str prefix: a SHA1 prefix (e.g. "e4" or "e4/01").

    :return: SHA1 of each cached tree object under the given prefix.
    :rtype: set

    """
    tree_path = NTR('{0}/trees/{1}/...').format(p4gf_const.objects_root(), prefix)
    p4_result = ctx.p4run('files', '-e', tree_path)
    cached_trees = set()
    for rr in p4_result:
        depot_path = rr.get('depotFile')
        if not depot_path:
            continue
        m = p4gf_object_type_util.OBJPATH_TREE_REGEX.search(depot_path)
        if not m:
            continue
        sha1 = m.group(NTR('slashed_sha1')).replace('/', '')
        cached_trees.add(sha1)
    return cached_trees


def count_missing_trees(ctx, known_commits, verbose=False):
    """Count the number of commits missing from the cache.

    :param ctx: context for the repo of interest.
    :param known_commits: list of known commit SHA1s.
    :param bool verbose: True to print individual missing objects.

    :return: number of missing commits.
    :rtype: int

    """
    # find all reachable tree objects in the repo
    last_tree = None
    known_trees = set()
    for sha1 in known_commits:
        if not last_tree:
            last_tree = _get_snapshot_trees(sha1, known_trees, ctx.repo)
        else:
            last_tree = _get_delta_trees(last_tree, sha1, known_trees, ctx.repo)

    # In batches, find those trees that are missing from the cache. The
    # important thing is not to request all cached trees, which could be a
    # huge number, nor to query for each and every tree object in the repo,
    # which would result in many requests to the server.
    missing_trees = 0
    last_prefix = ''
    cached_trees = None
    # Could have some smarts here that switches to a 4-character prefix if
    # the number of results retrieved on the first call is greater than
    # some reasonable number (e.g. 100,000).
    for tree in sorted(list(known_trees)):
        tree_prefix = tree[0:2]
        if last_prefix != tree_prefix:
            cached_trees = trees_for_prefix(ctx, tree_prefix)
            last_prefix = tree_prefix
        if tree not in cached_trees:
            missing_trees += 1
            if verbose:
                print(_("missing tree {tree}").format(tree=tree))
    return missing_trees


def check_missing_objects(ctx, verbose=False):
    """Check for any missing cache objects for the context's repo.

    :param ctx: context for the repo of interest.
    :param bool verbose: True to print individual missing objects.

    :return: number of missing commits and trees.
    :rtype: 2-tuple of ints

    """
    if not os.path.exists(ctx.repo_dirs.GIT_WORK_TREE):
        raise RuntimeError(_('not local to this server'))
    os.chdir(ctx.repo_dirs.GIT_WORK_TREE)
    cmd_result = p4gf_proc.popen(['git', 'rev-list', '--all'])
    known_commits = cmd_result['out'].splitlines()
    missing_commits = count_missing_commits(ctx, known_commits, verbose)
    missing_trees = count_missing_trees(ctx, known_commits, verbose)
    return (missing_commits, missing_trees)


def main():
    """Check for missing cache objects, with the option to add them."""
    # pylint:disable=too-many-branches,too-many-statements
    p4gf_util.has_server_id_or_exit()
    desc = _("Check for missing cache objects.")
    epilog = _("Note that at this time, missing commits cannot be fixed.")
    parser = p4gf_util.create_arg_parser(desc=desc, epilog=epilog)
    parser.add_argument(NTR("repo"), metavar=NTR("R"), nargs="*",
                        help=_("name of the repositories to be checked"))
    parser.add_argument("-a", "--all", action="store_true",
                        help=_("process all known Git Fusion repositories"))
    parser.add_argument("-F", "--fix", action="store_true",
                        help=_("find missing trees and submit to Perforce"))
    parser.add_argument("-v", "--verbose", action="store_true",
                        help=_("print details of missing objects"))
    args = parser.parse_args()

    # Check that either --all, or "repo" was specified.
    if not args.all and len(args.repo) == 0:
        sys.stderr.write(_("Missing view names; try adding --all option.\n"))
        sys.exit(2)
    if args.all and len(args.repo) > 0:
        sys.stderr.write(_("Ambiguous arguments. Choose --all or a view name.\n"))
        sys.exit(2)

    p4gf_proc.init()
    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4_temp_client()
        if not p4:
            sys.exit(2)
        # Sanity check the connection (e.g. user logged in?) before proceeding.
        try:
            p4.fetch_client()
        except P4Exception as e:
            sys.stderr.write(_("P4 exception occurred: {exception}\n").format(exception=e))
            sys.exit(1)
        if args.all:
            repos = p4gf_util.repo_config_list(p4)
            if len(repos) == 0:
                print(_("No Git Fusion repositories found, nothing to do."))
                sys.exit(0)
        else:
            repos = args.repo
        p4gf_create_p4.p4_disconnect(p4)

        for repo in repos:
            repo_name = p4gf_translate.TranslateReponame.git_to_repo(repo)
            print(_("Processing repository {repo_name}... ").format(repo_name=repo_name),
                  end='', flush=True)
            try:
                ctx = p4gf_context.create_context(repo_name)
                with ExitStack() as stack:
                    stack.enter_context(ctx)
                    ctx.repo_lock = p4gf_lock.RepoLock(ctx.p4gf, repo_name, blocking=False)
                    stack.enter_context(ctx.repo_lock)
                    if args.fix:
                        add_missing_objects(ctx)
                    else:
                        (commits, trees) = check_missing_objects(ctx, args.verbose)
                        print(_("missing {commits} commits and {trees} trees")
                              .format(commits=commits, trees=trees), end='')
                print("")
            except p4gf_config.ConfigLoadError:
                print(_("error loading config file\n"))
            except p4gf_lock.LockBusy:
                print(_("unable to acquire lock"))
            except RuntimeError as err:
                # truncate long error messages
                print(str(err).splitlines()[0])


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

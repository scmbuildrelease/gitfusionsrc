#! /usr/bin/env python3.3
"""Code for checking a 'git push' before writing anything to Perforce."""
import functools
import logging
import re
import sys

import P4

import p4gf_branch
from   p4gf_case_conflict_checker   import CaseConflictChecker
import p4gf_config
# no   p4gf_copy_to_p4              avoid circular import
import p4gf_const
from   p4gf_filemode                import FileModeStr
import p4gf_g2p_job                 as     G2PJob
import p4gf_git
from   p4gf_lfs_row                 import LFSRow
from   p4gf_l10n                    import _, NTR
import p4gf_path_convert
from   p4gf_profiler                import Timer
import p4gf_progress_reporter       as     ProgressReporter
import p4gf_protect
import p4gf_pygit2
import p4gf_util

LOG = logging.getLogger(__name__)

# Assume the cached objects will not be mapped into the repository, and
# skip permission checks on those paths since they're submitted as the
# git-fusion-user anyway.
P4GF_DEPOT_OBJECTS_RE = re.compile('//' + p4gf_const.P4GF_DEPOT + '/objects/')
P4GF_DEPOT_BRANCHES_RE = re.compile('//' + p4gf_const.P4GF_DEPOT + '/branches/')

# timer names
CHECK_PROTECTS  = NTR('Check Protects')
CHECK_OVERLAP   = NTR('Check Overlap')


class PreflightChecker:
    """A class that knows how to check a 'git push' for things that
    should not be copied into Perforce.

    Things you might not expect from a preflight checker:
    - user lookup:
        PreflightChecker has to see if the author has permission
        to write files, so PreflightChecker needs a way to look up
        a single Git commit's author.
        Could move to a separate class, passed in as part of __init__.
    - branch definition:
        A side effect of rolling through commits is that we
        have to fully define depot branches to hold those commits,
        and branch views to map the Git work tree to those depot branches.
        Could move elsewhere.
        Not sure if we want to require this as something done BEFORE
        preflight, or done DURING preflight.

    Extracted and decoupled from "classic push" p4gf_copy_to_p4.G2P and reused
    by "fast push" p4gf_fast_push.FastPush. If you change this for classic
    push, test your changes against fast push, too. Such is the price of
    maintaining two parallel code paths.
    """

    def __init__(self, *
            , ctx
            , g2p_user
            , assigner
            , gsreview_coll                 = None
            , already_copied_commit_runner  = None
            , finish_branch_definition      = None
            ):

        self.ctx                = ctx
        self.g2p_user           = g2p_user
        self.assigner           = assigner
        self.gsreview_coll      = gsreview_coll

        # ------------- # Explicitly listing each callback into G2P so I can
                        # find the API between these two snotballs.
                        # These are all "an instace (of G2P) that implements X:

                        # already_copied_commit(self, commit_sha1, branch_id)
        self.already_copied_commit_runner = already_copied_commit_runner

                        # finish_branch_definition(self, commit, branch)
        self._finish_branch_definition = finish_branch_definition

        # ------------- # End callbacks into G2P

                        # set() of depot names of type 'stream'
        self.stream_depots      = set()

                        # PreReceiveTuple whose commits we're checking.
                        # Used when permitting Swarm review merges, but
                        # prohibiting all other merge commits.
                        #
                        # OK to leave None if you don't care about
                        # Swarm reviews.
        self._current_prt       = None

                        # p4gf_fastexport_marks.Marks instance
                        # of all known git-fast-export marks.
        self.fast_export_marks  = None

        self._current_branch    = None
        self._curr_fe_commit    = None   # Current git-fast-export 'commit'.

                        # Behavior control for fast push: what to pass to
                        # Context.switched_to_branch() for set_client.
                        #
                        # When set to False, the Context.p4 client is
                        # untouched, which can save a huge amount of time
                        # in check_commit_for_branch(): 45 seconds drops
                        # to 9 seconds, but works only if you promist to make
                        # no calls to Perforce through that ctx.p4 connection.
                        #
        self.set_client_on_branch_switch = True

                        # list of fully populated Branch instances that overlap
                        # other fully populated branches.
                        #
                        # See _overlapping_branch_list() for calc and cache,
                        # call _invalidate_branch_cache() if you change any
                        # branch definitions after most recent call to
                        # _overlapping_branch_list().
                        #
        self._cached_overlapping_branch_list = None # list[] of FP Branch instances

                        # LFSRow text pointers that are part of this push.
        self.lfs_row_list = []


    #
    # -- callbacks into G2P --------------------------------------------------
    #    Optional. NOP if not running with a full G2P.
    #

    def _already_copied_commit(self, commit_sha1, branch_id):
        """Do we already have a Perforce changelist for this commit,
        this branch_id?
        """
        if not self.already_copied_commit_runner:
            return False
        return self.already_copied_commit_runner.already_copied_commit(
                    commit_sha1, branch_id)

    def finish_branch_definition(self, commit, branch):
        """Inflate a partially-created Branch with a proper depot branch to
        hold its files, and a branch view to map that depot branch to
        the Git work tree.
        """
        assert self._finish_branch_definition
        self._finish_branch_definition.finish_branch_definition(commit, branch)
        self._invalidate_branch_cache()

    #
    # -- end callbacks into G2P ----------------------------------------------
    #

    def check_prt_and_commits(self, prt, commits, marks):
        """Run a preflight check on a set of commits from Git.

        :param prt: pre-receive tuple
        :param commits: FastExport.commits list

        :type marks: :class:`p4gf_fastexport_marks.Marks`
        :param marks: all known fast-export marks

        Raises a PreflightException if anything is out of sorts.

        """
        self._current_prt = prt
        self.fast_export_marks = marks
        self.fast_export_marks.set_head(prt.ref)
        self.check_p4gf_user_write_permission()
        self.check_commits(commits)

    def check_p4gf_user_write_permission(self):
        """Ensure git-fusion-user has permissions to write to depot."""
        gf_client_map = P4.Map()
        gf_client_map.insert("//...", "//client/...")
        utp = p4gf_protect.UserToProtect(self.ctx.p4)
        prot = utp.user_to_protect(p4gf_const.P4GF_USER)
        gf_write_filter = prot.map_for_perm(p4gf_protect.WRITE)
        gf_write_filter = P4.Map.join(gf_write_filter, gf_client_map)
        if not gf_write_filter.includes('//{depot}/...'.format(depot=p4gf_const.P4GF_DEPOT)):
            raise RuntimeError(_('permission denied'))

    def _find_locked_by(self):
        """Return a dict of depot_path => user of any locked files."""
        fstat_flags = NTR('otherLock | otherOpen0 & headType=*+l')
        any_locked_files = {}  # depot_path : user
        for branch_chunk in self.ctx.iter_writable_branch_chunks():
            # Skip any newly defined branches: they're new, won't contain any
            # files yet, and won't get a view definition until later at per-
            # commit preflight time.
            bvl = [b for b in branch_chunk if b.view_lines]
            if not bvl:
                continue
            with self.ctx.switched_to_union(bvl):
                r = self.ctx.p4run('fstat', '-F', fstat_flags, '-m1',
                                   '//{}/...'.format(self.ctx.p4.client),
                                   log_warnings=logging.DEBUG)
                # Collect a dictionary of the locked files from the writable union of branch views
                for lf in r:
                    user = lf['otherOpen'][0] if 'otherOpen' in lf else NTR('<unknown>')
                    any_locked_files[lf['depotFile']] = user
        return any_locked_files

    def check_commits(self, commits):
        """Ensure the entire sequence of commits will (likely) go through
        without any errors related to permissions or locks. Raises an
        exception if anything goes wrong.

        Arguments:
            commits -- commits from FastExport class
        """
        LOG.info('Checking Perforce permissions and locks')
        self.ctx.checkpoint("copy_to_p4._preflight_check")

        # Stop if files are opened in our repo client
        # We expect this to be none, since we have the view lock
        opened = self.ctx.p4.run(['opened', '-m1'])
        if opened:
            raise PreflightException(_('There are files opened by Git Fusion for this repo.'))

        # fetch the repo setting only, without cascading to global config
        is_read_only = self.ctx.repo_config.getboolean(p4gf_config.SECTION_REPO,
                                                       p4gf_config.KEY_READ_ONLY,
                                                       fallback=False)
        if is_read_only:
            raise PreflightException(_("Push to repo {repo_name} prohibited.")
                                     .format(repo_name=self.ctx.config.repo_name))

        # get a list of stream depots for later checks for read-only paths
        depots = self.ctx.p4.run(['depots'])
        self.stream_depots = set([d['name'] for d in depots if d['type'] == 'stream'])
        any_locked_files = self._find_locked_by()
        LOG.debug("any_locked_files {0}".format(any_locked_files))
        case_conflict_checker = None
        if not self.ctx.server_is_case_sensitive:
            case_conflict_checker = CaseConflictChecker(self.ctx)
            case_conflict_checker.read_perforce_paths()

        ui_name = self._curr_ref_ui_name()
        if ui_name:
            progress_msg = _('Checking commits for {ref}...').format(ref=ui_name)
        else:
            progress_msg = _('Checking commits...')

        with ProgressReporter.Determinate(len(commits)):
            for commit in commits:
                ProgressReporter.increment(progress_msg)

                self.g2p_user.get_author_pusher_owner(commit)

                rev = commit['sha1']
                if not self.assigner.is_assigned(commit['sha1']):
                    continue

                self.check_commit(commit)

                for branch_id in self.assigner.branch_id_list(rev):
                    self.check_commit_for_branch(
                                                   commit
                                                 , branch_id
                                                 , any_locked_files
                                                 , case_conflict_checker )

        if case_conflict_checker:
            cc_text = case_conflict_checker.conflict_text()
            if cc_text:
                raise PreflightException(cc_text)

    def check_commit(self, commit):
        """Prior to copying a commit, perform a set of checks to ensure the commit
        will (likely) go through successfully.
        This includes:
            * verifying permission to commit for author p4 user
            * screening for merge commits
            * screening for submodules
            * checking valid filenames
            * checking write permissions for each file
        """
        # pylint: disable=too-many-branches
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug('check_commit() Checking mark={} sha1={} file-ct={} -- {}'
                      .format(  commit['mark']
                              , p4gf_util.abbrev(commit['sha1'])
                              , len(commit['files'])
                              , repr(commit['data'])[:20].splitlines()[0]))

        if not commit['author_p4user']:
            raise PreflightException(_("User '{user}' not permitted to commit")
                                     .format(user=commit['author']['email'].strip('<>')))

        if 'merge' in commit:
            ref_is_review = (self.gsreview_coll and
                             self.gsreview_coll.ref_in_review_list(self._current_prt.ref))
            if not ref_is_review and not self.ctx.merge_commits:
                raise PreflightException(_('Merge commits are not enabled for this repo.'))
            if (not ref_is_review and
                    not self.ctx.branch_creation and self.assigner.have_anonymous_branches):
                msg = _('Git branch creation is prohibited for this repo.')
                p4_branch_names_non_lw = [b.git_branch_name for b in self.ctx.branch_dict().values()
                                          if b.git_branch_name and not b.is_lightweight]
                if len(p4_branch_names_non_lw) > 1:
                    msg += _('\nThis repo has more than one named branch.'
                             '\nTry altering the push order - '
                             'pushing branches with merge ancestors first.')
                raise PreflightException(msg)
            if LOG.isEnabledFor(logging.DEBUG):
                for parent_mark in commit['merge']:
                    parent_sha1 = self.fast_export_marks.get_commit(parent_mark)[:7]
                    LOG.debug("check_commit() merge mark={} sha1={}"
                              .format(parent_mark, parent_sha1))

        if not self.ctx.submodules and 'files' in commit:
            for f in commit['files']:
                if f.get('mode') == '160000':
                    if 'first_commit' in commit and not self._path_added(f.get('path'), commit):
                        LOG.debug2('check_commit() passed {} in {}'.format(
                            f.get('path'), p4gf_util.abbrev(commit['sha1'])))
                        continue
                    raise PreflightException(
                        _('Git submodules not permitted: path={path} commit={commit_sha1}')
                        .format(path=f.get('path'), commit_sha1=p4gf_util.abbrev(commit['sha1'])))

        for f in commit['files']:
            LOG.debug3("check_commit : commit files: " + _log_fe_file(f))
            err = check_valid_filename(f['path'], self.ctx)
            if err:
                raise PreflightException(err)
            if self.ctx.is_lfs_enabled:
                self._check_lfs(commit, f)

        # Warn user about any jobs that appear to not exist
        jobs = G2PJob.lookup_jobs(self.ctx, G2PJob.extract_jobs(commit['data']))
        if jobs:
            for job_id in jobs:
                r = self.ctx.p4run('jobs', '-e', 'job={}'.format(job_id))
                if not r:
                    _print_error(_("Job '{job_id}' doesn't exist").format(job_id=job_id))
        # Create pending changes for any Git-Swarm reviews

    def check_commit_for_branch( self
                               , commit
                               , branch_id
                               , any_locked_files
                               , case_conflict_checker ):
        """
        Prior to copying a commit, perform a set of checks for a specific branch
        to ensure the commit will (likely) go through successfully.
        """
        rev = commit['sha1']
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("check_commit_for_branch() "
                      "Checking branch={} mark={} sha1={} file-ct={} -- {}"
                      .format(  branch_id
                              , commit['mark']
                              , p4gf_util.abbrev(rev)
                              , len(commit['files'])
                              , repr(commit['data'])[:20].splitlines()[0]))

        if self._already_copied_commit(rev, branch_id):
            return

        # following checks assume client has been set for branch
        self.ensure_branch_preflight(commit, branch_id)
        with self.ctx.switched_to_branch(
                  self._current_branch
                , set_client=self.set_client_on_branch_switch
                ):
            if case_conflict_checker:
                case_conflict_checker.read_fast_export_commit(
                    commit, self._current_branch)

            # Empty commits require root-level .p4gf_placeholder to be mapped
            # in the current branch view.
            if not commit['files'] and not self._is_placeholder_mapped():
                raise PreflightException(
                    _("Empty commit {sha1} not permitted. Git Fusion branch views"
                      " must include root to permit empty commits.")
                    .format(sha1=p4gf_util.abbrev(rev)))

            with Timer(CHECK_PROTECTS):
                self._check_protects(commit['author_p4user'], commit['files'])

            with Timer(CHECK_OVERLAP):
                self._check_overlap(commit)

            # fetch the branch setting only, without cascading to repo/global config
            if self._current_branch.is_read_only:
                raise PreflightException(_("Push to branch {branch} prohibited.")
                                         .format(branch=self._current_branch.git_branch_name))
            self._check_stream_writable(commit)
            self._check_stream_in_classic(commit)

            LOG.debug('checking locked files under //{}/...'.format(self.ctx.p4.client))
            if any_locked_files:
                # Convert the git commit paths to depotPaths
                files_in_commit = [self.ctx.gwt_path(f['path']).to_depot()
                                   for f in commit['files']]
                LOG.debug("files_in_commit {0}".format(files_in_commit))
                for f in files_in_commit:
                    if f in any_locked_files:
                        # Collect the names (and clients) of users with locked files.
                        # Report back to the pusher so they can take appropriate action.
                        msg = _('{file} - locked by {user}').format(file=f,
                                                                    user=any_locked_files[f])
                        LOG.info(msg)
                        raise PreflightException(msg)

                    # +++ Spend time extracting Jobs and P4Changelist owner
                    #     here if we actually do need to call
                    #     the preflight-commit hook.
            if self.ctx.preflight_hook.is_callable():
                jobs = G2PJob.extract_jobs(commit['data'])
                jobs2 = G2PJob.lookup_jobs(self.ctx, jobs)
                self.ctx.preflight_hook(
                     ctx                 = self.ctx
                   , fe_commit           = commit
                   , branch_id           = branch_id
                   , jobs                = jobs2
                   )

    def ensure_branch_preflight(self, commit, branch_id):
        """If not already switched to and synced to the correct branch for the
        given commit, do so.

        If this is a new lightweight branch, perform whatever creation we can do
        at preflight time. We don't have commits/marks for any not-yet-submitted
        parent commits, so the depot_branch_info will often lack a correct
        parent or fully populated basis.

        * depot tree, along with a branch-info file
        * branch mapping, along with entry in p4gf_config2 (if not anonymous)

        Return requested branch
        """
        log = LOG.getChild('ensure_branch_preflight')
        branch = self.ctx.branch_dict().get(branch_id)
        # branch should never be None here. p4gf_branch_id.Assigner() must
        # create Branch objects for each assignment.

        if      self._current_branch \
            and self._current_branch.branch_id == branch_id:
            log.debug("sha={} want branch_id={} curr branch_id={} NOP"
                      .format( commit['sha1'][:7]
                             , branch_id[:7]
                             , self._current_branch.branch_id[:7]))
            log.debug("staying on  branch {}"
                      .format(self.ctx.branch_dict().get(branch_id)))

            return branch

        cbid = self._current_branch.branch_id if self._current_branch else 'None'
        log.debug("sha={} want branch_id={} curr branch_id={} switch"
                  .format(commit['sha1'][:7], branch_id[:7], cbid[:7]))

        if not branch.view_lines:
            self.finish_branch_definition(commit, branch)

        elif branch.view_p4map:
            # if this is a stream branch, check for mutation of the stream's
            # view by comparing with the original view saved in p4gf_config2
            if branch.original_view_lines:
                original_view_lines = '\n'.join(branch.original_view_lines)
                view_lines = p4gf_path_convert.convert_view_to_no_client_name(branch.view_lines)
                if not view_lines == original_view_lines:
                    raise PreflightException(
                        _('Unable to push.  Stream view changed from:\n'
                          '{old_view}\nto:\n{new_view}')
                        .format(old_view=original_view_lines, new_view=view_lines))
            # Find existing depot branch for branch view's LHS.
            lhs = branch.view_p4map.lhs()
            branch.depot_branch = self.ctx.depot_branch_info_index()    \
                .find_depot_path(lhs[0])

        log.debug("switching to branch {}".format(branch))

        # By now we should have a branch and a branch.view_lines.
        # First remove current branch's files from workspace
        # Client spec is set to normdir
        self._current_branch = branch
        return branch

    def _is_placeholder_mapped(self):
        """Does this branch map our placeholder file?

        Returns non-False if mapped, None or empty string if not.
        """
        return self.ctx.gwt_path(
            p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER).to_depot()

    def _check_protects(self, p4user, blobs):
        """check if author is authorized to submit files."""
        pc = ProtectsChecker(self.ctx, p4user, self.ctx.authenticated_p4user,
                             self.ctx.foruser)
        pc.filter_paths(blobs)
        if pc.has_error():
            raise PreflightException(pc.error_message())

    def _check_overlap(self, fe_commit):
        """If any of the files in this commit intersect any fully populated branch
        (other than the current branch), then reject this commit.

        Shared/common/overlapping paths in branch views must be read-only from
        Git. Otherwise you end up with a Git push of commit on one Git branch
        inserting  changes into other Git branches behind Git's back.

        To modify shared paths, either do so from Perforce, or create a Git
        Fusion repo with no more than one branch that maps that shared path.
        """
                        # +++ Avoid O(b branches * r rev) checks when
                        #     overlap is impossible because current branch
                        #     overlaps no other branch.
        if self._current_branch not in self._overlapping_branch_list():
            return

        for fe_file in fe_commit['files']:
            gwt_path   = fe_file['path']
            depot_path = self.ctx.gwt_to_depot_path(gwt_path)

            for branch in self._overlapping_branch_list():
                if branch == self._current_branch:
                    continue
                if not branch.intersects_depot_path(depot_path):
                    continue

                LOG.debug("_check_overlap() branch {br1} <> {br2}"
                          " gwt={gwt:<40}   {dp}\n{view}"
                          .format(
                              br1  = p4gf_util.abbrev(self._current_branch.branch_id)
                            , br2  = p4gf_util.abbrev(branch.branch_id)
                            , gwt  = gwt_path
                            , dp   = depot_path
                            , view = "\n".join(branch.view_p4map.as_array())
                            ))

                if self._current_branch.is_new_fp_from_push or branch.is_new_fp_from_push:
                    current_branch_name = self._current_branch.git_branch_name
                    if self._current_branch.is_new_fp_from_push:
                        current_branch_name += '(new)'
                    other_branch_name = branch.git_branch_name
                    if branch.is_new_fp_from_push:
                        other_branch_name += '(new)'
                    human_msg = (_(
                        "Perforce: Cannot commit {sha1} '{gwt_path}' to '{depot_path}'.\n"
                        " You are attempting to push and create a new fully populated branch\n"
                        " with paths which overlap another branch. Contact your admin\n"
                        " to configure non-conflicting destination branch paths.\n"
                        " Branches: '{b1}', '{b2}'")
                        .format( sha1       = p4gf_util.abbrev(fe_commit['sha1'])
                               , gwt_path   = gwt_path
                               , depot_path = depot_path
                               , b1 = current_branch_name
                               , b2 = other_branch_name ))
                else:
                    human_msg = (_(
                        "Cannot commit {sha1} '{gwt_path}' to '{depot_path}'."
                        " Paths that overlap multiple Git Fusion branches are read-only."
                        " Branches: '{b1}', '{b2}'")
                        .format( sha1       = p4gf_util.abbrev(fe_commit['sha1'])
                               , gwt_path   = gwt_path
                               , depot_path = depot_path
                               , b1 = self._current_branch.branch_id
                               , b2 = branch.branch_id ))
                raise PreflightException(human_msg)

    def _check_stream_writable(self, fe_commit):
        """If this is a stream branch, check that all files in the commit are
        writable.  If any of the files is not writable then reject this commit.
        """
        if not self._current_branch.stream_name:
            return
        prefix = self._current_branch.writable_stream_name + '/'
        for fe_file in fe_commit['files']:
            gwt_path   = fe_file['path']
            depot_path = self.ctx.gwt_path(gwt_path).to_depot()
            if depot_path.startswith(prefix):
                continue

            human_msg = (_(
                "Cannot commit {sha1} '{gwt_path}' to '{depot_path}'."
                " Paths not in stream '{stream}' are read-only for branch '{b}'.")
                .format( sha1       = p4gf_util.abbrev(fe_commit['sha1'])
                       , gwt_path   = gwt_path
                       , depot_path = depot_path
                       , stream     = self._current_branch.writable_stream_name
                       , b          = self._current_branch.branch_id ))
            raise PreflightException(human_msg)

    def _check_stream_in_classic(self, fe_commit):
        """If this is a classic branch, check that none of the files in the commit
        are in stream depots and thus not writable.  If any of the files is not
        writable then reject this commit.
        """
        if self._current_branch.stream_name:
            return

        depot_re = re.compile(r'^//([^/]+)/([^/]+)/.*$')
        for fe_file in fe_commit['files']:
            gwt_path   = fe_file['path']
            depot_path = self.ctx.gwt_path(gwt_path).to_depot()
            m          = depot_re.match(depot_path)
            if m:
                depot = m.group(1)
                if depot in self.stream_depots:
                    stream = '//{}/{}'.format(m.group(1), m.group(2))
                    human_msg = (
                        _("Cannot commit {sha1} '{gwt_path}' to '{depot_path}'."
                          " Paths in stream '{stream}' are read-only for branch '{b}'.")
                        .format( sha1       = p4gf_util.abbrev(fe_commit['sha1'])
                               , gwt_path   = gwt_path
                               , depot_path = depot_path
                               , stream     = stream
                               , b          = self._current_branch.branch_id ))
                    raise PreflightException(human_msg)

    def _path_added(self, path, fecommit):
        """Return True if the named path was introduced in the HEAD commit.

        :param self: this object
        :param path: repo path to be evaluated.
        :param fecommit: commit object from fast-export parser.

        """
        # Because git-fast-export includes the entire tree in its output,
        # regardless of whether the requested commit is the first in the
        # branch or not, we need to check the repo itself to be certain if
        # this path was truly introduced in this commit, or simply existed
        # in the tree prior to the "first" commit.
        commit = self.ctx.repo.get(fecommit['sha1'])
        if commit is None:
            # empty repository?
            LOG.debug2("_path_added() commit {} is missing".format(fecommit['sha1']))
            return True
        for parent in commit.parents:
            if p4gf_git.exists_in_tree(self.ctx.repo, path, parent.tree):
                LOG.debug2("_path_added() {} exists in parent tree {}".format(
                    path, p4gf_util.abbrev(p4gf_pygit2.object_to_sha1(parent))))
                return False
        return True

    def _invalidate_branch_cache(self):
        """We have changed our branch_dict (or more likely
        finish_branch_definition()ed a branch within that dict) in a way that
        invalidates any cached calculations that consumed the branch dict.
        """
        self._cached_overlapping_branch_list = None

    def _overlapping_branch_list(self):
        """Return a list of fully populated branches that overlap
        other fully populated branches.

        Caches the result because we check every file revision
        path for overlap, and for huge repos with thousands of
        non-overlapping LW branches, just iterating through the
        branch list starts to waste measurable CPU time.
        """
        if self._cached_overlapping_branch_list is not None:
            return self._cached_overlapping_branch_list

        have_overlap = set()
        for outer in p4gf_branch.iter_fp_non_deleted(self.ctx.branch_dict()):
            outer_lhs = P4.Map()
            outer_lhs.insert(outer.view_p4map.lhs())
            for inner in p4gf_branch.iter_fp_non_deleted(self.ctx.branch_dict()):
                if outer == inner:
                    continue
                overlap = P4.Map.join(outer_lhs, inner.view_p4map)
                        # Any non-exclusionary lines shared between branches?
                for line in overlap.as_array():
                    if line.startswith('-') or line.startswith('"-'):
                        continue
                    # Yep. Non-exclusionary line implies overlap
                    have_overlap.add(outer)
                    have_overlap.add(inner)
                    break

        self._cached_overlapping_branch_list = have_overlap
        return self._cached_overlapping_branch_list

    def _curr_ref_ui_name(self):
        """Return a string suitable for use in progress messages as a
        short hint to what we're processing.

        If we have a PreReceiveTuple with a Git branch/tag name, return
        that name (minus any refs/heads/ prefix). If not, return None.
        """
        if not (self._current_prt and self._current_prt.ref):
            return None
        s = self._current_prt.ref
        prefixes = ["refs/heads/", "refs/tags/"]
        for p in prefixes:
            if s.startswith(p):
                return s[len(p):]
        return s

    def _check_lfs(self, fe_commit, fe_file):
        """If gfe_file is under Git LFS control, require that its
        large file content exist somewhere, either in our upload
        cache (it's new!) or in depot de-dupe storage (already got it).
        """
                        # Deleted files carry no LFS pointer.
        if "sha1" not in fe_file:
            return
                        # Symlinks and non-files carry no LFS pointer.
        if fe_file.get("mode") not in [ FileModeStr.PLAIN
                                      , FileModeStr.EXECUTABLE ]:
            return

                        # Files not under Git LFS control should not carry LFS
                        # pointer information. While legal and permissible,
                        # this is usually a mistake (misconfigured Git client)
                        # and something most users want caught before the push
                        # gets into Helix.
        is_tracked = self.ctx.lfs_tracker.is_tracked_git(
                      commit_sha1 = fe_commit["sha1"]
                    , gwt_path    = fe_file["path"])

        LOG.debug3("_check_lfs() tracked {lfs}  commit {commit_sha1}  gwt {gwt}"
            .format( commit_sha1 = p4gf_util.abbrev(fe_commit["sha1"])
                   , lfs         = 1 if is_tracked else 0
                   , gwt         = fe_file["path"]
                   ))

        if not is_tracked:
            lfs_row = LFSRow.from_gfe(self.ctx, fe_commit, fe_file)
            if lfs_row:
                raise PreflightException(
                        _("Push of Git LFS text pointer not tracked by LFS:"
                          "\ncommit {commit_sha1} path {gwt_path}")
                        .format( commit_sha1 = p4gf_util.abbrev(fe_commit["sha1"])
                               , gwt_path    = fe_file["path"] ))
            return

                        # Files under Git LFS control should carry LFS pointer
                        # information, but sometimes might not, and that's
                        # okay.
        lfs_row = LFSRow.from_gfe(self.ctx, fe_commit, fe_file)
        if not lfs_row:
            return
                        # But if they DO carry LFS pointer information, that
                        # pointer needs to point to a valid LFS large file
                        # either already in Perforce or recently uploaded.
        if not lfs_row.large_file_source:
            LOG.error("LFS text pointer missing content.")
            LOG.error("LFS   commit {}".format(p4gf_util.abbrev(fe_commit["sha1"])))
            LOG.error("LFS   lfs oid {}".format(lfs_row.large_file_oid))
            LOG.error("LFS   ptr {blob_sha1} {blob_mode} {gwt}"
                      .format( blob_sha1 = p4gf_util.abbrev(fe_file["sha1"])
                             , blob_mode = p4gf_util.mode_str(fe_file["mode"])
                             , gwt       = fe_file["path"]))
            LOG.error("LFS   upload  {}".format(lfs_row.to_lfsfs().cache_path(self.ctx)))
            LOG.error("LFS   de-dupe {}".format(lfs_row.to_lfsfs().depot_path(self.ctx)))

            raise PreflightException(_("Push of Git LFS text pointer missing content:"
                        "\ncommit {commit_sha1} path {gwt_path}")
                        .format( commit_sha1 = p4gf_util.abbrev(fe_commit["sha1"])
                               , gwt_path    = fe_file["path"] ))

                        # We have an acceptable LFS text pointer.
                        # Remember it for later.
        self.lfs_row_list.append(lfs_row)

# end class PreflightChecker
# ----------------------------------------------------------------------------


class ProtectsChecker:

    """Handle filtering a list of paths against view and protections."""

    def __init__(self, ctx, author, pusher, foruser):
        """Init P4.Map objects for author, pusher, view and combination."""
        self.ctx = ctx
        self.author = author
        self.pusher = pusher
        self.foruser = foruser

        self.ignore_author_perms = ctx.repo_config.getboolean(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                                              p4gf_config.KEY_IGNORE_AUTHOR_PERMS)

        self.view_map = None
        self.write_protect_author = None
        self.write_protect_pusher = None
        self.write_protect_foruser = None
        self.write_protect_fusion = None
        self.write_filter = None

        self.init_view()
        self.init_write_filter()

        self.author_denied = []
        self.pusher_denied = []
        self.foruser_denied = []
        self.fusion_denied = []
        self.unmapped = []

    def init_view(self):
        """Init view map for client."""
        self.view_map = self.ctx.clientmap

    def init_write_filter(self):
        """Init write filter."""
        self.write_protect_author = self.ctx.user_to_protect(self.author).map_for_perm(
            p4gf_protect.WRITE)
        if self.author != self.pusher or self.ignore_author_perms:
            # If the author and pusher differ, _or_ if we are ignoring
            # author permissions, then we must also included the pusher
            # protections, as well as set write_protect_pusher.
            self.write_protect_pusher = self.ctx.user_to_protect(self.pusher).map_for_perm(
                p4gf_protect.WRITE)
        if self.foruser:
            self.write_protect_foruser = self.ctx.user_to_protect(self.foruser).map_for_perm(
                p4gf_protect.WRITE)
        # Ensure that git-fusion-user can write to the depot
        self.write_protect_fusion = self.ctx.user_to_protect(
            p4gf_const.P4GF_USER).map_for_perm(p4gf_protect.WRITE)
        self.write_filter = functools.reduce(P4.Map.join, [
            vmap for vmap in [
                self.write_protect_author,
                self.write_protect_pusher,
                self.write_protect_foruser,
                self.write_protect_fusion,
                self.view_map,  # view_map has rhs, must come last in join
            ] if vmap])

    def filter_paths(self, blobs):
        """Run list of paths through filter and set list of paths that don't pass."""
        # check against one map for read, one for write
        # if check fails, figure out if it was the view map or the protects
        # that caused the problem and report accordingly
        self.author_denied = []
        self.pusher_denied = []
        self.foruser_denied = []
        self.fusion_denied = []
        self.unmapped = []
        c2d = P4.Map.RIGHT2LEFT

        LOG.debug('filter_paths() write_filter: %s', self.write_filter)
        for blob in blobs:
            gwt_path = self.ctx.gwt_path(blob['path'])
            topath_c = gwt_path.to_client()
            topath_d = gwt_path.to_depot()

            LOG.debug('filter_paths() topath_d: %s', topath_d)
            # for all actions, need to check write access for dest path
            result = "  "   # zum loggen
            if topath_d and P4GF_DEPOT_OBJECTS_RE.match(topath_d):
                LOG.debug('filter_paths() topath_d in //.git-fusion/objects')
                continue
            # do not require user write access to //.git-fusion/branches
            if topath_d and P4GF_DEPOT_BRANCHES_RE.match(topath_d):
                LOG.debug('filter_paths() topath_d in //.git-fusion/branches')
                continue
            if not self.write_filter.includes(topath_c, c2d):
                if not self.view_map.includes(topath_c, c2d):
                    self.unmapped.append(topath_c)
                    result = NTR('unmapped')
                elif not (self.ignore_author_perms or
                          self.write_protect_author.includes(topath_d)):
                    self.author_denied.append(topath_c)
                    result = NTR('author denied')
                elif (self.write_protect_pusher and
                      not self.write_protect_pusher.includes(topath_d)):
                    self.pusher_denied.append(topath_c)
                    result = NTR('pusher denied')
                elif (self.write_protect_foruser and
                      not self.write_protect_foruser.includes(topath_d)):
                    self.foruser_denied.append(topath_c)
                    result = NTR('foruser denied')
                elif not self.write_protect_fusion.includes(topath_d):
                    self.fusion_denied.append(topath_c)
                    result = NTR('Git Fusion denied')
                else:
                    result = "?"
                LOG.error('filter_paths() {:<13} {}, {}, {}'
                          .format(result, blob['path'], topath_d, topath_c))
            elif LOG.isEnabledFor(logging.DEBUG):
                LOG.debug('filter_paths() topath_c in write_filter: %s', topath_c)

    def has_error(self):
        """Return True if any paths not passed by filters."""
        return len(self.unmapped) or len(self.author_denied) \
            or len(self.pusher_denied) or len(self.foruser_denied) \
            or len(self.fusion_denied)

    def error_message(self):
        """Return message indicating what's blocking the push."""
        if len(self.unmapped):
            return _('file(s) not in client view')
        if len(self.author_denied):
            restricted_user = self.author
        elif len(self.pusher_denied):
            restricted_user = self.pusher
        elif len(self.foruser_denied):
            restricted_user = self.foruser
        elif len(self.fusion_denied):
            restricted_user = p4gf_const.P4GF_USER
        else:
            restricted_user = _('<unknown>')
        return _("user '{user}' not authorized to submit file(s) in git commit").format(
            user=restricted_user)

# end ProtectsChecker
# ----------------------------------------------------------------------------


class PreflightException(Exception):

    """This exception is raised when a push was rejected during preflight
    checks.
    """

    pass

# -- module-wide -------------------------------------------------------------

def _log_fe_file(fe_file):
    """Return loggable string for a single fe_commit['files'] element."""
    mode = '      '
    if 'mode' in fe_file:
        mode = fe_file['mode']
    sha1 = '       '
    if 'sha1' in fe_file:
        sha1 = p4gf_util.abbrev(fe_file['sha1'])

    return NTR('{mode} {action} {sha1} {path}') \
           .format( mode   = mode
                  , action = fe_file['action']
                  , sha1   = sha1
                  , path   = fe_file['path'])


def _print_error(msg):
    """Print the given message to the error stream, as well as to the log."""
    sys.stderr.write(msg + '\n')
    LOG.error(msg)


def is_p4d_printable(c):
    """Check if c will be rejected by P4D as non-printable.

    P4D rejects "non-printable" characters with
      ErrorId MsgDm::IdNonPrint = { ErrorOf( ES_DM, 6, E_FAILED, EV_USAGE, 1 )
      "Non-printable characters not allowed in '%id%'." } ;

    Where "character" here means C "char" aka "byte"
    and non-printable means 0x00-0x1f or 0x7f
    """
    if ord(c) < 0x20:
        return False
    if ord(c) == 0x7F:
        return False
    return True


def check_valid_filename(name, ctx):
    """Test the given name for illegal characters.

    Return None if okay, otherwise an error message.
    Illegal characters and sequences include: [...]
    """
    for idx, c in enumerate(name):
        if not is_p4d_printable(c):
            fullname = name[:idx] + "x{ch:02X}".format(ch=ord(c)) + name[idx:]
            return _("Perforce: Non-printable characters not allowed in Perforce: "
                     "character x{ch:02X} in filepath: {filename}").format(
                     filename=fullname, ch=ord(c))
    if '...' in name:
        return _("Perforce: bad filename (...): '{filename}'").format(filename=name)
    if 'P4D/NT' in ctx.server_version:
        if ':' in name:
            return _("Perforce: unsupported filename on windows: {filename}").format(filename=name)
    # This should usually be en_US.UTF-8 which also needs to be defined
    # on the os
    encoding = sys.getfilesystemencoding()
    try:
        name.encode(encoding, "strict")
    except UnicodeEncodeError:
        return _("Perforce: Cannot convert filename to '{encoding}': {filename}").format(
            encoding=encoding, filename=name)
    return None

#! /usr/bin/env python3.3
"""Copy Perforce changes to Git."""

from   collections              import namedtuple, defaultdict
import logging
import pprint
import re
import sys

import pygit2

from p4gf_desc_info             import DescInfo
from p4gf_fastimport            import FastImport
from p4gf_fastimport_mark       import Mark
from p4gf_fastimport_marklist   import MarkList
from p4gf_l10n                  import _, NTR
from p4gf_p2g_changelist_cache  import ChangelistCache
from p4gf_p2g_dag               import P2GDAGIndex
from p4gf_p2g_filelog_cache     import FilelogCache
from p4gf_p2g_print_handler     import PrintHandler
from p4gf_p2g_rev_range         import RevRange
from p4gf_p4changelist          import P4Changelist
from p4gf_p4file                import P4File
from p4gf_gitmirror             import GitMirror
from p4gf_parent_commit_list    import ParentCommitList
from p4gf_object_type           import ObjectType
from p4gf_profiler              import Timer
from p4gf_rev_sha1              import RevSha1

import p4gf_branch
import p4gf_config
import p4gf_const
import p4gf_context
import p4gf_fast_reclone
import p4gf_filelog_action
import p4gf_git
import p4gf_lfs_attributes
import p4gf_log
import p4gf_mem_gc
import p4gf_path
import p4gf_proc
import p4gf_progress_reporter as ProgressReporter
import p4gf_tag
import p4gf_util

from P4 import P4

LOG = logging.getLogger(__name__)
LOG_MEMORY = logging.getLogger('memory')


FastImportResult = namedtuple('FastImportResult',
                              ['marks', 'mark_to_branch_id', 'lfs_files', 'text_pointers'])


class P2G:

    """class to manage copying from Perforce to git."""

    def __init__(self, ctx):
        self.ctx = ctx
        self.fastimport = FastImport(self.ctx)

        self.new_branch_start   = "1"
        self.is_graft           = False
        self.stop_at            = "#head"
        self.current_branch_id  = None
        self.rev_range          = None  # RevRange instance set in copy().
                                        # dict[branch_id] to P4Changelist, but
                                        # no P4File elements in that P4Changelist
        self.branch_id_to_graft_change = None
        self.branch_id_to_graft_num = None

                                    # dict branch_id => ('@nn', end)
                                    # where end is either '@mm' or a commit sha1
                                    #
                                    # _get_branch_start_list() fills for all
                                    # known branches. Then _setup() removes any
                                    # branches with no new changelists.
        self.branch_start_list  = None

        self.changes            = ChangelistCache(self)  # changelists to copy
        self.graft_changes      = None  # graft point changelists to copy (# only)
        self.printed_revs       = None  # RevList produced by PrintHandler
        self.printed_rev_count  = 0

        self.status_verbose     = True

                                    # Where the current head of each branch
                                    # should go once we're done.
                                    # Values are BranchHead tuples.
                                    # Seeded to existing commit sha1s in _setup()
                                    # Updated to pending gfe marks in calls to
                                    # _record_branch_head() as we translate
                                    # changelists.
                                    # Set/updated by _fast_import().
        self._branch_id_to_head = {}

                                    # 1:N 1 p4 changelist number
                                    #     N git-fast-import mark numbers
                                    # Usually 1:1 unless a single Perforce
                                    # changelist touches multiple branches.
                                    # Assigned in _fast_import(), used in
                                    # _mirror()
        self.mark_list          = MarkList()
        self.sha1_to_mark       = dict()

                                    # Cached results for
                                    # filelog_to_integ_source_list()
        self._filelog_cache     = FilelogCache(self)
        self._branch_info_cache = {}# Cached results for _to_depot_branch_set

                                    # Filled and used by _sha1_exists().
                                    # Contains commit, tree, ANY type of sha1.
        self._sha1s_known_to_exist  = set()

                                    # Most recent ghost changelist seen (and
                                    # skipped) for that branch. Values are
                                    # SkippedGhost tuples.
        self._branch_id_to_skipped_ghost = {}

                                    # API into persistent store of
                                    # depotFile#rev ==> blob sha1
        self._rev_sha1          = RevSha1(ctx)

                                    # In-memory graph of not-yet-copied-to-git
                                    # commit/parent relationships.
        self.dag                = P2GDAGIndex(ctx)

    def __str__(self):
        return "\n".join(["\n\nFast Import:\n",
                          str(self.fastimport)
                          ])

    def all_branches(self):
        """Return a list of all known branches.

        Sorted for easier debugging.
        """
        return [b[1] for b in sorted(self.ctx.branch_dict().items())]

    def get_branch_id_to_graft_num(self,
                                   ctx,
                                   start_at):  # "@NNN" Perforce changelist num
        """For a graft. collect the highest P4 CL per git branch"""

        # Are there any PREVIOUS Perforce changelists before the requested
        # start of history? If so, then we'll need to graft our history onto
        # that previous point.
        if start_at != "@1":
            # Possibly (probably) grafting history: we need to know the correct
            # changelist that will be our first real changelist copied to Git.
            begin_change_num = int(start_at[1:])
            r = self.ctx.union_view_highest_change_num(
                    at_or_before_change_num = begin_change_num)
            if not r:
                # Rare surprise: there are no changes at or before the start
                # revision specifier, do not need to graft history.
                return

            LOG.debug("begin_change_num={}".format(begin_change_num))

            # Check each branch for history before that start. That history
            # gets truncated down to a graft-like commit.
            self.branch_id_to_graft_num = {}
            for branch in self.all_branches():
                # Each branch gets its own graft commit (or possibly None).
                with ctx.switched_to_branch(branch):
                    changes_result = ctx.p4run('changes', '-m2',
                                               ctx.client_view_path() + start_at)
                    # Highest changelist that comes before our start is this
                    # branch's graft changelist.
                    max_before = 0
                    for change in changes_result:
                        change_num = int(change['change'])
                        if max_before < change_num < begin_change_num:
                            max_before = change_num
                    if max_before:
                        self.branch_id_to_graft_num[branch.branch_id] = max_before
                        LOG.debug("graft={} for branch={}"
                                  .format(max_before, branch.branch_id))

            if self.branch_id_to_graft_num:
                self.branch_id_to_graft_change = {}
                for branch_id, graft_num \
                        in self.branch_id_to_graft_num.items():
                    # Ignore all depotFile elements, we just want the
                    # change/desc/time/user. depotFiles here are insufficient,
                    # don't include depotFiles from before this change, which
                    # we'll fold in later, during grafting.
                    p4cl = P4Changelist.create_using_describe( self.ctx.p4
                                                             , graft_num
                                                             , 'ignore_depot_files')
                    p4cl.description += (_("\n[grafted history before {start_at}]")
                                         .format(start_at=start_at))
                    self.branch_id_to_graft_change[branch_id] = p4cl

    def _anon_branch_head(self, branch):
        """Return the highest numbered changelist in a branch view that also has
        a corresponding Git commit already existing in the Git repo.
        Returns a 2-tuple (changelist number, commit sha1)

        Return (None, None) if no such changelist or commit.
        """
        commit = ObjectType.last_change_num_for_branches(self.ctx,
                                                         [branch.branch_id],
                                                         must_exist_local=True)
        if commit:
            return (commit.change_num, commit.sha1)
        return (None, None)

    def _get_branch_start_list(self, start_at=None):
        """Store a dictionary[branch_id] => 2-tuple ("@change_num", sha1).

        Where change_num is the highest numbered Perforce changelist already
        copied to Git, sha1 is its corresponding Git commit.

        Calling code starts the Perforce-to-Git copy AFTER the returned tuples.
        """
                        # +++ Split into two passes so that our calls to
                        # ObjectType can be batched by commit sha1.
                        # Avoids O(n^2) behavior for totally artificial tests of
                        # 16,000 branch refs all pointing to the same commit.
        sha1_to_branch_list = defaultdict(list)

        self.branch_start_list = {}
        for v in self.all_branches():
            if start_at and start_at.startswith("@"):
                self.branch_start_list[v.branch_id] = (start_at, start_at)
                continue
            if v.deleted or not v.git_branch_name:
                (ch, sha1) = self._anon_branch_head(v)
                if not (ch and sha1):
                    ch   = 1
                    sha1 = "@1"
                if v.deleted_at_change and v.deleted_at_change <= ch:
                    LOG.debug("_get_branch_start_list (deleted): skipping"
                              " branch_id={} sha1={} ch={} deleted_at_change={}".
                            format(v.branch_id, sha1, ch, v.deleted_at_change))
                    v.any_changes = False
                self.branch_start_list[v.branch_id] = ('@{}'.format(ch), sha1)
                continue
            sha1 = p4gf_util.sha1_for_branch(v.git_branch_name, repo=self.ctx.repo)
            sha1_to_branch_list[sha1].append(v)

                        # +++ Second pass with batched-by-commit-sha1
                        # ObjectType lookups.
        for sha1, branch_list in sha1_to_branch_list.items():
            otl = []
            if sha1:
                otl = ObjectType.commits_for_sha1(self.ctx, sha1)
            branch_id_to_ot = { ot.branch_id : ot for ot in otl }

            for v in branch_list:
                ot = branch_id_to_ot.get(v.branch_id)
                change_num = None
                if ot:
                    change_num = int(ot.change_num)
                if change_num:
                    branch_highest_change_num  = change_num + 1
                    branch_highest_commit_sha1 = sha1
                else:
                    branch_highest_change_num  = 1
                    branch_highest_commit_sha1 = "@1"  # overload this sha1 to refer to @1
                self.branch_start_list[v.branch_id] = (
                    "@{0}".format(branch_highest_change_num), branch_highest_commit_sha1)

        if LOG.isEnabledFor(logging.DEBUG2):
            l = ('{} {}'.format(p4gf_util.abbrev(branch_id),
                                self.branch_start_list[branch_id])
                 for branch_id in sorted(self.branch_start_list.keys()))
            LOG.debug2('_get_branch_start_list() results:\n' + '\n'.join(l))

    def add_to_branch_start_list(self, branch_id, start_cl, start_sha1=None):
        """Add a branch to our list of copy branch start points."""
        _start_sha1 = start_sha1
        if not _start_sha1:
            _start_sha1 = "@1"
        self.branch_start_list[branch_id] = (
            "@{0}".format(start_cl), _start_sha1)

    def _head_change_on_branches(self, branches):
        """Get the head change on the union view of a list of branches.

        If branches is empty or there are no changes on the listed branches
        return 0.

        If branches contains only one branch, use the actual view of the branch
        rather than the 'union' of a single branch.  This gives a more accurate
        result since the 'union' omits exclusion lines in the view.
        """
        if not branches:
            return 0
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("_head_change_on_branches {}".format(
                ", ".join([b.branch_id for b in branches])))
        if len(branches) is 1:
            with self.ctx.switched_to_branch(branches[0]):
                head_change = p4gf_util.head_change_as_string(self.ctx, submitted=True)
        else:
            with self.ctx.switched_to_union(branches):
                head_change = p4gf_util.head_change_as_string(self.ctx, submitted=True)
        if head_change:
            return int(head_change)
        return 0

    def _discover_branches_changed_since_change(self, branches, last_copied_change):
        """Discover which branches have changes since last copy.

        Return True if any branch has changes.
        """
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("_discover_changed_branches_since_change: {}"
                      .format(", ".join([b.branch_id for b in branches])))
        if not branches:
            return False
        half = int((1 + len(branches)) / 2)
        b1 = branches[:half]
        b2 = branches[half:]

        def _discover_half(branches, last_copied_change):
            """Recursively discover changes on half of the branches list."""
            if not branches:
                return False
            if (len(branches) > p4gf_context.MAX_UNION_BRANCH_CT or
                    self._head_change_on_branches(branches) > last_copied_change):
                if len(branches) is 1:
                    if not branches[0].deleted:
                        branches[0].any_changes = True
                    return True
                return self._discover_branches_changed_since_change(branches,
                                                                    last_copied_change)
            for b in branches:
                b.any_changes = False
            return False

        any1 = _discover_half(b1, last_copied_change)
        any2 = _discover_half(b2, last_copied_change)
        return any1 or any2

    def _discover_changed_branches(self, branches):
        """Discover which branches have changes since last copy.

        Return False if no branch has changes.
        Otherwise, set any_changes on each branch and return True.
        """
        if not branches:
            return False

        last_copied_change = self.ctx.read_last_copied_change()
        if not last_copied_change:
            return False
        last_copied_change = int(last_copied_change)

        any_changes = self._discover_branches_changed_since_change(branches, last_copied_change)

        LOG.debug("_discover_changed_branches:{} last_copied_change:{}".
                  format(any_changes, last_copied_change))
        return any_changes

    def _setup(self, _start_at, stop_at):
        """Set RevRange rev_range, figure out which changelists to copy.
        """
        # determine the highest commits for each branch
        # placed into self.branch_start_list
        # If a graft - start loading the newly discovered DBI from the graft point
        # Otherwise it loads from @1
        if _start_at and _start_at.startswith("@") and _start_at != "@1":
            self.new_branch_start = _start_at[1:]
            self.is_graft = True
        self.stop_at = stop_at
        self._get_branch_start_list(_start_at)

        def append(change):
            """append a change to the list."""
            self.changes.update(change)

        for b in self.all_branches():
            if not b.any_changes:
                del self.branch_start_list[b.branch_id]
                continue

            if _start_at:   # possible graft start or init from @1
                start_at = _start_at
            else:
                # passing in the sha1 - not the CLnum
                start_at = self.branch_start_list[b.branch_id][1]
            with self.ctx.switched_to_branch(b):
                self.rev_range = RevRange.from_start_stop(self.ctx, start_at, stop_at)

                # get list of changes to import into git
                num = P4Changelist.get_changelists(self.ctx.p4, self._path_range(), append)

                LOG.debug2("_setup() branch={} range={} change_ct={}"
                           .format( p4gf_util.abbrev(b.branch_id)
                                    , self.rev_range
                                    , num ))

                if not num:
                    del self.branch_start_list[b.branch_id]

        LOG.debug3('_setup() branch_start_list: {}'
                   .format(p4gf_util.debug_list(LOG, self.branch_start_list)))

        # If grafting, get those too.
        # We need to collect the potential graft CL once.

        if self.is_graft:
            self.get_branch_id_to_graft_num(self.ctx, _start_at)

        LOG.debug3('_setup() changes {0}'
                   .format(p4gf_util.debug_list(LOG, self.changes.keys())))

    def _path_range(self):
        """Return the common path...@range string we use frequently."""
        if self.current_branch_id:
            return self._branch_range(self.branch_start_list[self.current_branch_id][0])
        else:
            return self.ctx.client_view_path() + self.rev_range.as_range_string()

    def _branch_range(self, change):
        """For branch return '//<client>/...@N' where N is the highest branch
        changelist number.
        """
        _range = NTR('{begin},{end}').format(begin=change,
                                             end=self.stop_at)
        return self.ctx.client_view_path() + _range

    def _copy_print_view_element(self, printhandler, args, view_element):
        """p4 print all the revs for the given view, git-hash-object them into
        the git repo, add their depotFile, rev, and P4File info to our shared
        RevList.

        view_element is always a Branch.

        Returns a set of change numbers included in output of this print.
        """
        with self.ctx.switched_to_branch(view_element):
            self.current_branch_id = view_element.branch_id

            LOG.debug('_copy_print_view_element() printing for element={}'
                      .format(view_element.to_log()))

            printhandler.change_set = set()
            with p4gf_util.raw_encoding(self.ctx.p4):
                with self.ctx.p4.using_handler(printhandler):
                    with self.ctx.p4.at_exception_level(P4.RAISE_ALL):
                        self.ctx.p4run('print', args, self._path_range())
            printhandler.flush()
            return printhandler.change_set

    def _to_depot_branch_set(self, change_set):
        """Return a list of DepotBranchInfo objects describing storage
        locations that house change_set's p4files' changes.

        Runs 'p4 filelog' to find integration sources to any changelists.

        Can be overly aggressive and return integ sources to files not changed
        by changes in change_set due to view exclusions.
        """
        dbis = set()
        for change_num in change_set:
            if change_num in self._branch_info_cache:
                LOG.debug3("_branch_info_cache hit on {}".format(change_num))
                dbil = self._branch_info_cache[change_num]
            else:
                LOG.debug3("_branch_info_cache miss on {}".format(change_num))
                (dfl, _rl) = self.filelog_to_integ_source_list(change_num)
                dbil = self.ctx.depot_branch_info_index()\
                    .depot_file_list_to_depot_branch_list(dfl)
                self._branch_info_cache[change_num] = dbil
            dbis.update(dbil)
        return dbis

    def _to_branch_view_list(self, depot_branch_set):
        """Return a list of new Branch view instances that map the given
        depot branches into this repo.
        """
        result = []
        for dbi in depot_branch_set:
            l = p4gf_branch.define_branch_views_for(self.ctx, dbi)
            result.extend(l)
        return result

    def _copy_print(self):
        """p4 print all revs and git-hash-object them into the git repo."""
        printhandler = PrintHandler(ctx=self.ctx)
        # if not self.ctx.p4.server_unicode:
        #    old_encoding = self.ctx.p4.encoding
        #    self.ctx.p4.encoding = "raw"
        args = [ '-a'   # all revisions within the specified range
               , '-k'   # suppresses keyword expansion
               ]

        # The union client view is a view into all of these depot branches.
        # We do not want or need to generate new branch views into any of these.
        seen_depot_branch_set = {b.get_or_find_depot_branch(self.ctx)
                                 for b in self.ctx.branch_dict().values()}

        work_queue = []     # list of elements, element is itself
                                    # a list or a branch.
                                    #
        # start with the list of defined repo branches
        for bid, br in sorted(self.ctx.branch_dict().items()):
            if bid in self.branch_start_list:
                work_queue.append(br)

        with ProgressReporter.Indeterminate() \
            , p4gf_git.suppress_gitattributes(self.ctx):
            while work_queue:
                view_element = work_queue.pop(0)
                new_change_set = self._copy_print_view_element(printhandler
                                                               , args
                                                               , view_element)
                # get set of branches not previously seen
                dbi_set = self._to_depot_branch_set(new_change_set)
                dbi_new = dbi_set - seen_depot_branch_set
                seen_depot_branch_set |= set(dbi_new)

                new_branch_view_list = self._to_branch_view_list(dbi_new)
                LOG.debug('_copy_print() new_branch_view_list={}'
                          .format(new_branch_view_list))
                # add these new branches to our dictionary of start CL
                # These are loaded from @1 to capture all of history
                for b in new_branch_view_list:
                    self.add_to_branch_start_list(b.branch_id, self.new_branch_start)
                work_queue.extend(new_branch_view_list)
                p4gf_mem_gc.report_growth(NTR('in P2G._copy_print() work queue'))

            p4gf_mem_gc.report_objects(NTR('after P2G._copy_print() work queue'))

            # If also grafting, print all revs in existence at time of graft.
            if self.branch_id_to_graft_change:
                for branch_id, change in self.branch_id_to_graft_change.items():
                    branch = self.ctx.branch_dict().get(branch_id)
                    with self.ctx.switched_to_branch(branch):
                        args = ['-k']  # suppresses keyword expansion
                        path = self._graft_path(change)
                        LOG.debug("Printing for grafted history: {}".format(path))
                        with p4gf_util.raw_encoding(self.ctx.p4):
                            with self.ctx.p4.using_handler(printhandler):
                                self.ctx.p4run('print', args, path)
                        printhandler.flush()
                    p4gf_mem_gc.report_growth(NTR('in P2G._copy_print() graft change'))

            p4gf_mem_gc.report_objects(NTR('after P2G._copy_print() graft change'))

        self.printed_revs = printhandler.revs
        if self.printed_revs:
            self.printed_rev_count += len(self.printed_revs)
        self._record_printed_revs_to_sha1()

    def _get_sorted_changes(self):
        """return sorted list of changes to be copied to git."""
        if self.branch_id_to_graft_change:
            self.graft_changes = set([int(change.change)
                                      for change
                                      in self.branch_id_to_graft_change.values()])
            sorted_changes = sorted(list(self.changes.keys()) + list(self.graft_changes))
        else:
            sorted_changes = sorted(list(self.changes.keys()))

        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('_get_sorted_changes() returning:\n'
                       + "\n".join([str(ch) for ch in sorted_changes]))
        return sorted_changes

    def _explicit_p4files_for_gfi( self
                                 , change
                                 , branch
                                 , parent_commit_list
                                 ):
        """
        Usually we can git-fast-import just whatever P4Changelist.files holds:
        diff actions against the previous changelist on the current depot branch.
        If this is the case, return None.

        Occasionally Perforce and Git have a different idea of
        "previous changelist/commit". When this happens, return a dict
        of gwt_path to P4File instance for EVERY file that exists,
        undeleted, at branch@change.
        """
                        # What commit does our P4Change use as "before"
                        # for its diff actions on this depot branch?
        p4_diff_before_sha1mark = self.branch_head_mark_or_sha1(branch.branch_id)

                        # What commit will we use as "before" for
                        # git-fast-import?
        gfi_before_sha1mark = parent_commit_list[0] if parent_commit_list else None
        LOG.debug2('_explicit_p4files_for_gfi() p4={} pcl[0]={}'
                   .format(p4_diff_before_sha1mark, gfi_before_sha1mark))

                        # The usual case: they match.
        if p4_diff_before_sha1mark == gfi_before_sha1mark:
            return None

                        # The expensive case: they don't match and now we
                        # have to go fish everything out of Perforce.
        return self._p4file_list(change, branch)

    def _record_printed_revs_to_sha1(self):
        """Persistently store each changelist's depotFile#rev ==> blob sha1
        so that _p4file_to_sha1() can find it later.
        """
        if p4gf_const.READ_ONLY:
            # Must too expensive to perform this work from a replica instance.
            return
        for change_num, p4file_list in self.printed_revs.changes.items():
            self._rev_sha1.record(change_num, p4file_list)

    def _p4file_to_sha1(self, p4file):
        """Find the sha1 of this file that we got when we 'p4 print'ed it then
        git-hash-object-ed it into the Git repo.

        Returns empty string "" if depotFile#rev ==> sha1 is not (yet)
        stored in Perforce. Caller should batch up the failures and call
        _backfill_p4file_to_sha1().
        """
        if p4file.sha1:
            return p4file.sha1

                        # If you change how we print/store p4changelists
                        # and their contained p4file revisions, change
                        # this code to match.
        p4file_list = self.printed_revs.files_for_change(p4file.change)
        hit = [x for x in p4file_list
               if x.depot_path == p4file.depot_path]
        if hit:
            result = hit[0].sha1
            if result:
                return result

        try:
            return self._rev_sha1.lookup( change_num = p4file.change
                                        , depot_path = p4file.depot_path
                                        , rev        = p4file.revision )
        except RevSha1.NotFoundError:
            return ''

    def _p4file_list(self, change, branch):
        """Return a dict of gwt_path to P4File object, one entry for each file
        that exists, undeleted, at this change on this branch.

        Includes all files inherited from branch's fully populated basis,
        if any.
        """
        LOG.debug('_p4file_list() Calculating list of files for {br} @{cl}'
                  .format(br=p4gf_branch.abbrev(branch), cl=change.change))

                        # All file revisions at change, including revisions
                        # we'll not copy due to reasons.
        all_files = branch.p4_files(self.ctx, at_change=change.change)

                        # Don't copy deleted revisions.
        e_files = [f for f in all_files
                   if 'delete' not in f['action']]

                        # Don't copy files that appear as directories.
                        # Perforce allows it(!).
                        # Matrix 1 can't record deletion of symlinks
                        # replaced by directories.
        g_files = p4gf_branch.Branch.strip_conflicting_p4_files(e_files)

        result = []
        for rr in g_files:
            p4file          = P4File.create_from_files(rr)
            p4file.sha1     = self._p4file_to_sha1(p4file)
            p4file.gwt_path = self.ctx.client_path(rr['clientFile']).to_gwt()
            result.append(p4file)
        self._backfill_p4file_to_sha1(result)
        return result

    def _backfill_p4file_to_sha1(self, p4file_list):
        """Update Git Fusion's depotFile#rev ==> sha1 data.

        Lazily updates data with only the revisions that we actually need.

        Occasionally there will be a big delay for 'p4 print' + git-hash-
        object, triggered when a 'git pull' inherits files from some changelist
        that we already copied to/from Git in an earlier pull/push.

        Because we lazy backfill when required, we do not fill in the
        depotFile#rev ==> sha1 data during 'git push'. Why pay a guaranteed
        slower 'git push' for an occasional avoidance of delay in 'git pull'?
        """
        if p4gf_const.READ_ONLY:
            # Must too expensive to perform this work from a replica instance.
            return
        backfill_list = [f for f in p4file_list
                         if f.sha1 == '']
        if not backfill_list:
            return

        rev_path_to_p4file = {p4file.rev_path(): p4file
                              for p4file in backfill_list}
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3("_backfill_p4file_to_sha1()\n"
                       + "\n".join(rev_path_to_p4file.keys()))
        else:
            LOG.debug("_backfill_p4file_to_sha1() {} revisions"
                      .format(len(rev_path_to_p4file.keys())))

                        # Pull out your strainers, it's copypasta time!
                        # _copy_print()
        printhandler = PrintHandler(ctx=self.ctx)
        args = [ '-a'   # all revisions within the specified range
               , '-k'   # suppresses keyword expansion
               ]
                        # _copy_print_view_element()
        printhandler.change_set = set()
        with p4gf_util.raw_encoding(self.ctx.p4):
            with self.ctx.p4.using_handler(printhandler):
                with self.ctx.p4.at_exception_level(P4.RAISE_ALL):
                    self.ctx.p4run('print', args,
                                   list(rev_path_to_p4file.keys()) )
        printhandler.flush()
                        # end copypasta

                        # Copy newly discovered sha1 to p4file_list's elements.
        for printed_p4file in printhandler.revs:
            orig_p4file = rev_path_to_p4file.get(printed_p4file.rev_path())
            if orig_p4file:
                orig_p4file.sha1 = printed_p4file.sha1

                        # Record newly discovered sha1s for future pulls
                        # so we never have to print these file revisions again.
        for change_num, p4file_list in printhandler.revs.changes.items():
            self._rev_sha1.record(change_num, p4file_list)

    def _fast_import_from_p4( self
                            , change, branch
                            , mark_to_branch_id
                            , branch_id_to_temp_name ):
        """Translate a Perforce changelist into a Git commit,
        files into ls-tree and blob objects, all via git-fast-import.

        Called for Perforce changelists that originated in Perforce, or
        originated in Git but not for this repo+branch_id.
        """
        # First commit ever on this branch?
        is_first_commit_on_branch = self.is_branch_empty(branch.branch_id)

        (  parent_commit_list
        , first_parent_branch_id
        , parent_branch_to_cl
        , is_git_orphan ) = self._parent_commit_list(
                                              change
                                            , branch
                                            , is_first_commit_on_branch)

        desc_info = DescInfo.from_text(change.description)
        if desc_info and desc_info.sha1\
                and p4gf_git.object_exists(desc_info.sha1, self.ctx.repo):
            if desc_info.sha1 not in self.sha1_to_mark:
                mark_number = self.mark_list.assign(change.change)
                self.sha1_to_mark[desc_info.sha1] = mark_number
            else:
                LOG.debug('_fast_import_from_p4 skipping duplicate change={} for commit={}'
                          .format(change, desc_info.sha1))
                # Ensure this branch's head points to the existing commit.
                self._record_branch_head( branch_id  = branch.branch_id
                                        , mark       = self.sha1_to_mark[desc_info.sha1]
                                        , change_num = change.change )
                return
        else:
            mark_number = self.mark_list.assign(change.change)
        LOG.debug('_fast_import_from_p4 change={} mark={} branch={}'
                  .format(change, mark_number, branch.to_log()))
        LOG.debug2('_fast_import_from_p4 parent_commit_list={}'
                   .format(p4gf_util.abbrev(parent_commit_list)))
        mark_to_branch_id[mark_number] = branch.branch_id
        # Branching from an existing commit?
        if      is_first_commit_on_branch \
            and first_parent_branch_id    \
            and parent_commit_list:
            # If so, then we must not blindly accept parent commit's
            # list of work tree files as a starting point. Change from
            # one branch view to another becomes changes to work tree
            # file existence or path.
            LOG.debug("_fast_import_from_p4 new branch: first_parent_branch_id={}"
                      .format(first_parent_branch_id))
            LOG.debug3("_fast_import_from_p4 new branch: parent_branch_to_cl={}"
                      .format(parent_branch_to_cl))

            first_branch_from_branch_id     = first_parent_branch_id
            first_branch_from_change_number = \
                parent_branch_to_cl.get(first_parent_branch_id)
        else:
            first_branch_from_branch_id     = None
            first_branch_from_change_number = None

                        # If we can't trust diff calculations,
                        # reset the world to zero and build it from scratch.
        epf = self._explicit_p4files_for_gfi(
                                  change             = change
                                , branch             = branch
                                , parent_commit_list = parent_commit_list )
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3("_fast_import_from_p4() epf={}\nchange.files={}"
                       .format( pprint.pformat(epf)
                              , pprint.pformat(change.files)))
        if epf:
            p4file_list = epf
        else:
            p4file_list = change.files
        if self.ctx.is_lfs_enabled:
            if epf or not parent_commit_list:
                p4file_list = p4gf_lfs_attributes.maybe_create_lfs_attrs(
                    self.ctx, change, p4file_list, branch)

        # create commit and trees
        self.fastimport.add_commit(
              cl           = change
            , p4file_list  = p4file_list
            , mark_number  = mark_number
            , parent_commit_list = parent_commit_list
            , first_branch_from_branch_id     = first_branch_from_branch_id
            , first_branch_from_change_number = first_branch_from_change_number
            , dest_branch  = branch
            , branch_name  = branch_id_to_temp_name[branch.branch_id]
            , deleteall    = (epf is not None)
            , is_git_orphan = is_git_orphan
            )

        # Move this branch's head to this new commit.
        self._record_branch_head( branch_id            = branch.branch_id
                                , mark                 = mark_number
                                , change_num           = change.change
                                , parent_sha1mark_list = parent_commit_list )

    def _sha1_exists(self, sha1):
        """Is there ANY object in the Git repo with the given sha1?
        Could be a commit, tree, blob, whatever.
        """
        with Timer(FI_SHA1_EXISTS):
            exists = sha1 in self._sha1s_known_to_exist
            if not exists:
                exists = p4gf_util.sha1_exists(sha1)
                if exists:
                    self._sha1s_known_to_exist.add(sha1)
            return exists

    def _print_to_git_store(self, sha1, p4_path):
        """p4 print the object directly into the //P4GF_DEPOT/objects/... store."""
        with Timer(FI_PRINT):
            # Fetch Git object from Perforce, write directly to Git.
            git_path = self.ctx.repo_dirs.GIT_DIR + '/' \
                     + p4gf_util.sha1_to_git_objects_path(sha1)
            p4gf_util.ensure_parent_dir(git_path)
            with p4gf_util.raw_encoding(self.ctx.p4gf):
                results = self.ctx.p4gfrun('print', '-o', git_path, p4_path)
                if not results:
                    return None
            LOG.debug3('_print_to_git_store() {} {}'.format(sha1, git_path))
            self._sha1s_known_to_exist.add(sha1)
            return git_path

    def _print_tree_to_git_store(self, sha1):
        """p4 print the tree object directly into the //P4GF_DEPOT/objects/... store.

        Return path to file we printed.
        """
        p4_path = ObjectType.tree_p4_path(sha1)
        return self._print_to_git_store(sha1, p4_path)

    def _print_commit_to_git_store(self, commit):
        """p4 print the commit object directly into the //P4GF_DEPOT/objects/... store.

        Return path to file we printed.
        """
        p4_path = ObjectType.commit_p4_path(self.ctx, commit)
        return self._print_to_git_store(commit.sha1, p4_path)

    def _commit_to_tree_sha1(self, commit_sha1):
        """Return the tree sha1 for a commit."""
        with Timer(FI_TO_TREE):
            obj = self.ctx.repo.get(commit_sha1)
            tree = None
            if obj and obj.type == pygit2.GIT_OBJ_COMMIT:
                tree = p4gf_git.tree_from_commit(obj.read_raw())
            LOG.debug2('_commit_to_tree_sha1({}) => {}'.format(commit_sha1, tree))
            return tree

    def _fast_import_from_gitmirror(self, change, branch):
        """Copy a Git commit object and its Git ls-tree objects directly from
        where they're archived in //P4GF_DEPOT/objects/... into Git.

        Copy file revisions into Git using git-hash-object.

        Return True if able to copy directly from //P4GF_DEPOT/objects/... .
        Return False if not.
        """
        # pylint:disable=too-many-branches
        log = LOG.getChild('fast_import_from_gitmirror')
        with Timer(FI_GITMIRROR):
            # Do we already have a commit object for this changelist, this branch?
            commit_ot = ObjectType.change_num_to_commit(self.ctx,
                                                        change.change,
                                                        branch.branch_id)
            if not commit_ot:
                log.debug2('no commit in Perforce for ch={} branch={}. Returning False.'
                           .format(change.change, p4gf_util.abbrev(branch.branch_id)))
                return False
            if self._sha1_exists(commit_ot.sha1):
                # Already copied. No need to do more.
                log.debug2('{} commit already done, skipping'.format(
                    p4gf_util.abbrev(commit_ot.sha1)))
                self._record_branch_head( branch_id  = branch.branch_id
                                        , sha1       = commit_ot.sha1
                                        , change_num = commit_ot.change_num )
                return True

            # Copy commit from Peforce directly to Git.
            commit_git_path = self._print_commit_to_git_store(commit_ot)

            # Every file we add is suspect unless we complete without error.
            file_deleter = p4gf_util.FileDeleter()
            file_deleter.file_list.append(commit_git_path)

            if self.ctx.is_lfs_enabled:
                self.ctx.lfs_tracker.add_cl(branch=branch, p4change=change)

            # Copy commit's tree and subtrees to Git.
            # blobs should have already been copied during _copy_print()
            tree_sha1 = self._commit_to_tree_sha1(commit_ot.sha1)
            if not tree_sha1:
                log.debug2('{} commit missing tree. Returning False.'
                           .format(p4gf_util.abbrev(commit_ot.sha1)))
                return False

            tree_queue = [tree_sha1]
            while tree_queue:
                tree_sha1 = tree_queue.pop(0)

                # If already in Git, no need to copy it again.
                if self._sha1_exists(tree_sha1):
                    log.debug2('{} tree already done, skipping.'
                               .format(p4gf_util.abbrev(tree_sha1)))
                else:
                    # Copy tree from Perforce directly to Git.
                    intree = ObjectType.tree_exists_in_p4(self.ctx.p4gf, tree_sha1)
                    if not intree:
                        log.debug2('{} tree missing in mirror. Returning False'
                                   .format(p4gf_util.abbrev(tree_sha1)))
                        return False
                    tree_git_path = self._print_tree_to_git_store(tree_sha1)
                    file_deleter.file_list.append(tree_git_path)
                    log.debug2('{} tree copied from mirror.'.format(p4gf_util.abbrev(tree_sha1)))

                # Copy file children, enqueue tree children for future copy.
                for (i_mode, i_type, i_sha1, i_path) in p4gf_util.git_ls_tree(self.ctx.repo,
                                                                              tree_sha1):
                    if 'tree' == i_type:
                        tree_queue.append(i_sha1)
                        continue
                    if '160000' == i_mode:
                        # Submodule/gitlink, nothing to do here
                        continue
                    if not self._sha1_exists(i_sha1):
                        if self.ctx.is_lfs_enabled:
                            if i_path == p4gf_const.GITATTRIBUTES:
                                p4_path = p4gf_path.blob_p4_path(i_sha1)
                                if self._print_to_git_store(i_sha1, p4_path):
                                    log.debug2('.gitattributes from cache: {}'.format(i_sha1))
                                    continue
                            elif self.ctx.lfs_tracker.is_tracked_p4(branch=branch, p4change=change,
                                                                    gwt_path=i_path):
                                p4_path = p4gf_path.blob_p4_path(i_sha1)
                                if self._print_to_git_store(i_sha1, p4_path):
                                    log.debug2('inserting text pointer: {}'.format(i_sha1))
                                    continue
                        log.debug2('{} untree missing. Returning False'.format(
                            p4gf_util.abbrev(i_sha1)))
                        log.debug3('missing entry for {}'.format(i_path))
                        return False

            # Move this branch's head to this new commit.
            self._record_branch_head( branch_id  = branch.branch_id
                                    , sha1       = commit_ot.sha1
                                    , change_num = commit_ot.change_num )
            # Made it to here without error? Keep all we wrought.
            file_deleter.file_list = []
            log.debug2('{} commit copied from mirror. Returning True.'.format(
                p4gf_util.abbrev(commit_ot.sha1)))
            return True

    def _get_changelist(self, changenum):
        """Get changelist object for change number, with no files.

        :param changenum: the changelist specifier; must be an integer

        """
        # All we have is the change number and list of revs.
        # Can't use p4 change -o because that gives formatted time and we want raw.
        # Could use p4 describe, but that sends the potentially large list of files.
        # So use p4 changes, filtered by the first rev in the change, with limit of 1.
        cl = P4Changelist.create_changelist_list_as_dict(self.ctx.p4,
                                                         "@{},@{}".format(changenum, changenum),
                                                         1)[changenum]
        return cl

    def get_changelist_for_branch(self, changenum, branch):
        """Get changelist object for change number.

        If change is a graft point, use branch to create fake changelist object
        containing required files.
        """
        change = self.changes.get(changenum)
        if branch:
            change.files = self.printed_revs.files_for_graft_change(changenum, branch)
        else:
            change.files = self.printed_revs.files_for_change(changenum)
        return change

    def _is_change_num_in_branch_range(self, change_num, branch_id):
        """Copy to Git only those changelists that we've not already copied.

        Copy to Git no changelists if we've decided not to
        copy anything to that branch.
        """
        t = self.branch_start_list.get(branch_id)
        if not t:
            # _setup() stripped out this branch_id because we don't have
            # any new work for this branch. Don't touch this branch at all.
            LOG.debug2('_is_change_num_in_branch_range() no new work for {}'.format(branch_id))
            return False

        start_str = t[0]
        if start_str.startswith('@'):
            start_str = start_str[1:]
        LOG.debug2('_is_change_num_in_branch_range() evaluating {} < {}'.format(
            start_str, change_num))
        return int(start_str) <= int(change_num)

    def _create_branch_id_to_temp_name_dict(self):
        """Give every single Branch view its own unique Git branch name that we can
        use during git-fast-import without touching any real/existing Git branch
        references.

        Return a new dict of branch ID to a temporary Git branch name
        "git-fusion-temp-branch-{}", with either the Git branch name
        (if branch has one) or the branch ID stuffed into the {}.
        """
        return {branch.branch_id: _to_temp_branch_name(branch)
                for branch in self.ctx.branch_dict().values()}

    def _fast_import(self, sorted_changes):
        """Build fast-import script from changes, then run fast-import.

        Assumes a single linear sequence from a single branch:
        * Current Perforce client must be switched to the view for that branch.
        * No merge commits.

        Returns (marks, mark_to_branch_id dict)
        """
        # pylint:disable=too-many-branches
        # pylint:disable=too-many-statements
        LOG.debug('_fast_import()')
        branch_dict = self.ctx.branch_dict()
        branch_id_to_temp_name = self._create_branch_id_to_temp_name_dict()
        LOG.debug3("_fast_import branch_dict={}".format(branch_dict.values()))
        self._fill_head_marks_from_current_heads()
        ##ZZ self._fill_head_marks_from_complete_ghosts()
        mark_to_branch_id = {}
        cache_miss = False
        with ProgressReporter.Determinate(len(sorted_changes)):
            for changenum in sorted_changes:
                ProgressReporter.increment(_('Copying changelists...'))

                        # Never copy ghost changelists to Git.
                if _is_ghost_desc(self.changes.get(changenum).description):
                    self._skip_ghost(self.changes.get(changenum))
                    continue

                is_graft = bool(self.graft_changes) and changenum in self.graft_changes
                LOG.debug2('_fast_import() is_graft={}'.format(is_graft))

                # regular non-graft changes:
                all_branches = self.all_branches()
                if not is_graft:
                    # branch doesn't matter for non-graft changes
                    change = self.get_changelist_for_branch(changenum, None)
                    LOG.info('Copying {}'.format(change))
                    branch_list = [branch for branch in all_branches
                                   if branch.intersects_p4changelist(change)]
                    LOG.debug3('_fast_import() branches={}'
                               .format(", ".join([p4gf_branch.abbrev(b)
                                                  for b in branch_list])))
                    for branch in branch_list:
                        LOG.debug2('_fast_import() non-graft change @{ch} on branch={br}'
                                   .format(br=p4gf_branch.abbrev(branch),
                                           ch=changenum))

                        if not self._is_change_num_in_branch_range(
                                changenum, branch.branch_id):
                            LOG.debug2('_fast_import() change {} on branch {} ignored'.format(
                                changenum, branch.branch_id))
                            continue

                        with self.ctx.switched_to_branch(branch):
                            if not self._fast_import_from_gitmirror(change, branch):
                                cache_miss = True
                                if p4gf_const.READ_ONLY:
                                    # Nothing we can do here, return only what we have.
                                    break
                                self._fast_import_from_p4(change,
                                                          branch,
                                                          mark_to_branch_id,
                                                          branch_id_to_temp_name)
                    if cache_miss and p4gf_const.READ_ONLY:
                        # We're done, get out rather than grabbing
                        # incomplete data that another server might be
                        # pushing into Perforce.
                        break
                    continue

                # special case for graft changes:

                for branch in all_branches:
                    # check if this is branch uses this graft change
                    if branch.branch_id not in self.branch_id_to_graft_change:
                        LOG.debug2('_fast_import() not in graft change, branch={}'.format(branch))
                        continue
                    gchange = self.branch_id_to_graft_change[branch.branch_id]
                    if gchange.change != changenum:
                        LOG.debug2('_fast_import() change not graft change, {}'.format(gchange))
                        continue

                    LOG.debug2('_fast_import() graft change on branch={}'.format(branch))
                    with self.ctx.switched_to_branch(branch):

                        change = self.get_changelist_for_branch(changenum, branch)
                        LOG.info('Copying {}'.format(change))

                        change.description = gchange.description
                        if not self._fast_import_from_gitmirror(change, branch):
                            cache_miss = True
                            if p4gf_const.READ_ONLY:
                                # Nothing we can do here, return only what we have.
                                break
                            self._fast_import_from_p4(change,
                                                      branch,
                                                      mark_to_branch_id,
                                                      branch_id_to_temp_name)
                p4gf_mem_gc.process_garbage(NTR('in P2G._fast_import()'))
                if cache_miss and p4gf_const.READ_ONLY:
                    # We're done, get out rather than grabbing incomplete
                    # data that another server might be pushing into
                    # Perforce.
                    break

        self._fill_head_marks_from_complete_ghosts()

        # run git-fast-import and get list of marks
        LOG.info('Running git-fast-import')
        marks = self.fastimport.run_fast_import()

        # done with these
        # sorted for debugging ease
        p4gf_git.delete_branch_refs(sorted(branch_id_to_temp_name.values()))

        # Record how much we've copied. In the read-only case, we only
        # count the update if we were able to pull from the cache.
        if not cache_miss or not p4gf_const.READ_ONLY:
            self.ctx.write_last_copied_change(sorted_changes[-1])

        self.changes = None
        self._filelog_cache = None
        return FastImportResult(marks=marks,
                                mark_to_branch_id=mark_to_branch_id,
                                lfs_files=self.fastimport.lfs_files,
                                text_pointers=self.fastimport.text_pointers)

    def _mirror(self, fast_import_result):
        """build up list of p4 objects to mirror git repo in perforce then submit them."""
        LOG.info('Copying Git and Git Fusion data to //{}/...'
                 .format(p4gf_const.P4GF_DEPOT))
        for blob in fast_import_result.text_pointers:
            LOG.debug2('_mirror() adding text pointer {}'.format(blob))
            self.ctx.mirror.add_blob(blob)
        self.ctx.mirror.add_objects_to_p4(fast_import_result.marks,
                                          self.mark_list,
                                          fast_import_result.mark_to_branch_id,
                                          self.ctx)
        self.ctx.mirror.update_branches(self.ctx)
        self.ctx.mirror.integ_lfs_files(self.ctx, fast_import_result.lfs_files)
        LOG.getChild("time").debug("\n\nGit Mirror:\n" + str(self.ctx.mirror))
        # Reset to a new, clean, mirror.
        self.ctx.mirror = GitMirror(self.ctx.config.repo_name)

        marks = fast_import_result.marks
        if marks:
            last_commit = marks[len(marks) - 1].strip()
            LOG.debug("Last commit fast-import-ed: " + last_commit)
        else:
            LOG.debug('No commits fast-imported')

    def filelog_to_integ_source_list(self, change_num):
        """From what files does this change integrate?

        Return a 2-tuples of (depotFile_list, erev_list)
        """
        return self._filelog_cache.get(change_num)

    def _calc_filelog_to_integ_source_list(self, change_num):
        """Run 'p4 filelog' to find and return a list of all integration
        source depotFile paths that contribute to this change.

        Return a 3-tuple of (depotFile_list,
                             erev_list,
                             <size of lists>)
        """
        path = self.changes.get_path(change_num)
        r = self.ctx.p4run('filelog', '-m1', '-c', str(change_num), path)
        source_depot_file_list = []
        source_erev_list       = []
        sizeof = 0
        for rr in r:
            # Skip files that aren't integrated to/from somewhere.
            if (   (not rr.get('how' ))
                or (not rr.get('file'))
                or (not rr.get('erev')) ):
                continue
            # double-deref+zip how0,0 and file0,0 double-arrays.
            for how_n, file_n, erev_n in zip(rr['how'], rr['file'], rr['erev']):
                for how_n_m, file_n_m, erev_n_m in zip(how_n, file_n, erev_n):
                    if p4gf_filelog_action.is_from(how_n_m):
                        # erev starts with a # sign ("#3"),
                        # and might actually be a rev range ("#2,#3").
                        # Focus on the end of the range, just the number.
                        erev = erev_n_m.split('#')[-1]
                        source_depot_file_list.append(file_n_m)
                        source_erev_list      .append(erev)
                        sizeof += sys.getsizeof(file_n_m) + sys.getsizeof(erev)

        LOG.debug('filelog_to_integ_source_list() ch={} returning ct={}'
                  .format(change_num, len(source_depot_file_list)))
        LOG.debug3('\n'.join(p4gf_util.to_path_rev_list(source_depot_file_list,
                                                        source_erev_list)))
        if not source_depot_file_list:
            if LOG.isEnabledFor(logging.DEBUG3):
                LOG.debug3('filelog_to_integ_source_list() ch={ch}'
                           ' returning 0, filelog gave us:\n{r}'
                           .format(ch=change_num, r=pprint.pformat(r)))

        sizeof += sys.getsizeof(source_depot_file_list)
        sizeof += sys.getsizeof(source_erev_list)
        return (source_depot_file_list, source_erev_list, sizeof)

    def _parent_commit_list( self
                           , change
                           , current_branch
                           , is_first_commit_on_branch ):
        """ Given a Perforce changelist, return a list of Git commits that
        should be parents of the Git commit we're about to create for this
        changelist.

        Returns a 3-tuple ( [sha1/mark list]
                          , first parent branch id
                          , {branch->changelist dict} )

        Returned list elements are either sha1s of existing commits (str),
        or git-fast-import marks (int).

        Return None if this changelist has no parent in Git.
        Except for the first commit in a repo, this should be very rare.

        change         : P4Changelist
        current_branch : Branch
        """
        pcl = ParentCommitList( self
                              , change
                              , current_branch
                              , is_first_commit_on_branch)
        pcl.calc()

        return ( pcl.parent_commit_list
               , pcl.first_parent_branch_id
               , pcl.branch_id_to_changelist_num
               , pcl.is_git_orphan)

    def _fill_head_marks_from_complete_ghosts(self):
        """Discover forked branch references.

        It's a bit of a hack, but p4gf_fork_populate.py sets the p4key
        git-fusion-index-last-{repo},{branch_id} for each forked branch
        to point to a ghost changelist that populates that branch.

        Find these, and stuff their sha1s into _branch_id_to_head
        so that we can use them later to record the branch ref.
        """
                        # +++ Avoid unnecessary O(n) 'p4 change' calls.
                        #     Only need to do this on the first copy_to_git
                        #     after a p4gf_fork_populate, which means only on
                        #     the first pull into an empty repo.
        if not p4gf_util.git_empty():
            LOG.debug2("_fill_head_marks_from_complete_ghosts()"
                       " skip: Git repo not empty")
            return

        for br in self.ctx.branch_dict().values():
            ot = ObjectType.last_change_num_for_branches(self.ctx, br.branch_id)
            LOG.debug3("_fill_head_marks_from_complete_ghosts() br={} ot={}"
                       .format(p4gf_util.abbrev(br.branch_id), ot))
            if not ot:
                continue
            desc = p4gf_util.first_value_for_key(
                self.ctx.p4run('change', '-o', ot.change_num),
                'Description')
            if not desc:
                continue
            di = DescInfo.from_text(desc)
            if not di:
                continue
            if (   (not di.ghost_of_sha1)
                or (di.push_state != NTR('complete'))):
                LOG.debug3("_fill_head_marks_from_complete_ghosts() skip  :"
                           " br={br} ch={ch} not a complete={c} ghost={g}"
                           .format( br = p4gf_util.abbrev(br.branch_id)
                                  , ch = ot.change_num
                                  , c  = di.push_state
                                  , g  = p4gf_util.abbrev(di.ghost_of_sha1)))
                continue

            LOG.debug3("_fill_head_marks_from_complete_ghosts() record:"
                       " br={br} ch={ch} sha1={s}"
                       .format( br = p4gf_util.abbrev(br.branch_id)
                              , ch = ot.change_num
                              , s  = p4gf_util.abbrev(ot.sha1)))

            self._record_branch_head( branch_id  = br.branch_id
                                    , sha1       = ot.sha1
                                    , change_num = ot.change_num)

    def _fill_head_marks_from_current_heads(self):
        """Read 'git-show-ref' to find each known branch's current head,
        store the head sha1 to use later as the parent of the next
        commit on that branch.

        Upon return, self._branch_id_to_head has an entry for every
        branch_id in branch_dict() with a value of either an existing commit
        sha1, or None if branch ref not yet defined.
        """
        branch_name_list = [b.git_branch_name
                            for b in self.all_branches()
                            if         b.git_branch_name
                               and not b.deleted]
        git_name_to_head_sha1 = p4gf_util.git_ref_list_to_sha1(branch_name_list)
        branch_dict = self.ctx.branch_dict()
        for branch_id, branch in branch_dict.items():
            if branch.git_branch_name in git_name_to_head_sha1:
                sha1 = git_name_to_head_sha1[branch.git_branch_name]
                self._record_branch_head( branch_id  = branch_id
                                        , sha1       = sha1 )

    def is_branch_empty(self, branch_id):
        """Do we have no commits/changelists recorded for this branch?"""
        return not self.branch_head_mark_or_sha1(branch_id)

    def branch_head_mark_or_sha1(self, branch_id):
        """Return the mark or sha1 of the most recently recorded commit/changelist
        for this branch.
        """
        bh = self._branch_id_to_head.get(branch_id)
        LOG.debug3("branch_head_mark_or_sha1() b={} bh={}".format(branch_id, bh))
        if not bh:
            return None
        return _mark_or_sha1(bh)

    def branch_head_to_change_num(self, branch_id):
        """Return integer changelist number of most recent changelist copied to the given branch.

        Return None if branch is empty, no changelists copied ever, including previous pulls/pushes.

        Return 0 if branch contains copied changelists/commits, but we stupidly
        forgot to record their changelist number when building
        self._branch_id_to_head. (That's a bug and we need to fix that.)
        """
        bh = self._branch_id_to_head.get(branch_id)
        if not bh:
            return None
        return int(bh.change_num)

                        # pylint:disable=too-many-arguments
    def _record_branch_head(self, branch_id
                           , mark                 = None
                           , sha1                 = None
                           , change_num           = 0
                           , parent_sha1mark_list = None):
        """Remember that this is the most recent changelist/commit seen on a branch.

        Record its mark_or_sha1 (mark from MarkList if we copied it, sha1 if
        seen from Git).

        Also record corresponding changelist number.
        """
        LOG.debug3('_record_branch_head() mark={mark} sha1={sha1} change={cn} {br} {pl}'
                   .format( br   = p4gf_util.abbrev(branch_id)
                          , mark = mark
                          , sha1 = p4gf_util.abbrev(sha1)
                          , cn   = change_num
                          , pl   = parent_sha1mark_list ))
        mark_int = int(mark) if (mark is not None) else None
        self._branch_id_to_head[branch_id] \
            = BranchHead( mark       = mark_int
                        , sha1       = sha1
                        , change_num = int(change_num))

        if mark is not None:
            self.dag.add(self.dag.to_node(mark, parent_sha1mark_list))

    def _branch_head_marks(self):
        """Iterator/generator yields marks/sha1s, one per branch that has either."""
        for bh in self._branch_id_to_head.values():
            if bh and bh.mark:
                yield bh.mark

    def _skip_ghost(self, ghost_p4change):
        """We're not going to copy ghost_p4change to Git.

        Remember this ghost_p4change's changelist number so that  later when
        processing normal changelists, we can tell if the most recent changelist
        on this branch was a ghost that might warrant closer inspection during
        ParentCommitList calculation.
        """
        change_num = ghost_p4change.change
        LOG.debug2('skipping ghost @{}'.format(change_num))
                        # Do not call Branch.intersects_p4changelist() here. It
                        # requires P4Changelist.files to be populated. P2G no
                        # longer populates P4Changelist.files because that costs
                        # too much memory.
        depot_file_list = p4gf_util.files_in_change_num(self.ctx, change_num)
        for branch_id, branch in self.ctx.branch_dict().items():
            if branch.intersects_depot_file_list(depot_file_list):
                LOG.debug2('skipping ghost @{} on branch {}'
                           .format(change_num, p4gf_util.abbrev(branch_id)))
                di = DescInfo.from_text(ghost_p4change.description)
                ofcn = di.ghost_of_change_num               \
                    if (di and di.ghost_of_change_num) else 0
                self._branch_id_to_skipped_ghost[branch_id] \
                    = SkippedGhost( change_num    = int(change_num)
                                  , of_change_num = int(ofcn) )

    def ghost_for_branch_id(self, branch_id):
        """If the most recent changelist we've seen on this branch was a ghost,
        return a SkippedGhost tuple with the ghost and ghost-of changelist
        numbers as integers.
        If not, return None.
        """
                        # Get most recent ghost on branch.
        skipped_ghost    = self._branch_id_to_skipped_ghost.get(branch_id)
        if not (skipped_ghost and skipped_ghost.change_num):
            return None
        ghost_change_num = skipped_ghost.change_num

                        # Anything on the branch newer than the ghost?
        bh = self._branch_id_to_head.get(branch_id)
        if bh and bh.change_num and ghost_change_num < bh.change_num:
            LOG.debug3("ghost_for_branch_id() branch {br} ghost {gh} < head {h}"
                       .format( br = p4gf_util.abbrev(branch_id)
                              , gh = ghost_change_num
                              , h  = bh.change_num ))
            return None

        LOG.debug2("ghost_for_branch_id() branch {br} ghost {gh} of {of}"
                   .format( br = p4gf_util.abbrev(branch_id)
                          , gh = ghost_change_num
                          , of = skipped_ghost.of_change_num ))
        return skipped_ghost

    @staticmethod
    def _pack():
        """run 'git gc' to pack up the blobs.

        aside from any possible performance benefit, this prevents warnings
        from git about "unreachable loose objects"
        """
        pass  # p4gf_proc.popen_no_throw(["git", "gc"])

    def _set_branch_refs(self, mark_lines):
        """Force each touched, named, branch reference to the head of
        whatever history that we just appended onto that branch.
        """
        # Scan for interesting mark/sha1 lines.
        mark_to_sha1 = {mark: None
                        for mark in self._branch_head_marks()}
        for mark_line in mark_lines:
            ml = Mark.from_line(mark_line)
            if ml.mark in mark_to_sha1:
                mark_to_sha1[ml.mark] = ml.sha1

        # Detach HEAD because we cannot force the current branch to a
        # different commit. This only works if we have a HEAD: empty repos on
        # first clone will reject this command and that's okay.
        with p4gf_git.non_bare_git():
            p4gf_proc.popen_no_throw(['git', 'checkout', 'HEAD~0'])

        for branch_id, bh in self._branch_id_to_head.items():
            head_mark = bh.mark
            head_sha1 = bh.sha1 if bh.sha1 else mark_to_sha1.get(bh.mark)
            branch    = self.ctx.branch_dict().get(branch_id)
            # If our list of branches contained deleted branches to
            # handle tags referencing deleted branches
            # then skip setting the branch ref
            if branch.deleted:
                continue
            if not branch.git_branch_name:  # Anon branches have no ref to set.
                continue
            if head_sha1 is None:
                # Branch not moved by git-fast-import. Might still have been
                # moved by direct copy of commits into .git/objects/...
                LOG.warning("_set_branch_refs() found null head for branch {} mark={}"
                            .format(branch.git_branch_name, head_mark))
                continue
            LOG.debug("_set_branch_refs() {} mark={} sha1={}"
                      .format(branch.git_branch_name, head_mark, head_sha1))
            self._set_branch_ref_if(branch.git_branch_name, head_sha1)

        # Reattach HEAD to a branch.
        self.ctx.checkout_master_ish()

    def _set_branch_refs_fast_reclone(self, heads):
        """Force each touched, named, branch reference to the head of
        whatever history that we just copied into that branch.
        """
        for branch_id, bh in heads.items():
            head_mark = bh[1]
            head_sha1 = bh[0]
            branch = self.ctx.branch_dict().get(branch_id)
            # If our list of branches contained deleted branches to
            # handle tags referencing deleted branches
            # then skip setting the branch ref
            if not branch:
                continue
            if branch.deleted:
                continue
            if not branch.git_branch_name:  # Anon branches have no ref to set.
                continue
            LOG.debug("_set_branch_refs_fast_reclone() {} mark={} sha1={}"
                      .format(branch.git_branch_name, head_mark, head_sha1))
            self._set_branch_ref_if(branch.git_branch_name, head_sha1)

        # Attach HEAD to a branch.
        self.ctx.checkout_master_ish()

    def _set_branch_ref_if(self, git_branch_name, sha1):
        """Change/set a branch ref, but only if it does not already
        exist and point to the requested sha1.
        """
        try:
            ref = self.ctx.repo.lookup_reference(
                    "refs/heads/{}".format(git_branch_name))
            if ref.target.hex == sha1:
                return
        except Exception: # pylint: disable=broad-except
            pass
                        # Could use pygit to set ref.target here, but
                        # requires smarter code that knows how to create
                        # or set depending on whether already exists.
                        # popen() will do until profiling proves we need
                        # pygit2 here.
        p4gf_proc.popen(
            ['git', 'branch', '-f', git_branch_name, sha1])


    def _graft_path(self, graft_change):
        """If grafting, return '//<client>/...@N'.

        N is the graft changelist number.
        """
        return self.ctx.client_view_path(graft_change.change)

    @staticmethod
    def _log_memory(msg):
        """How big is our heap now?"""
        if LOG_MEMORY.isEnabledFor(logging.DEBUG):
            usage = p4gf_log.memory_usage()
            LOG_MEMORY.debug('Memory after {:<24} {}'.format(msg, usage))

    def desc_info_permits_merge(self, change_num):
        """Does the current Perforce changelist's DescInfo block contain a
        "parents:" tag with 2+ parent commit sha1s?

        If not, then this was originally NOT a merge commit, and we should avoid
        turning it into one upon rerepo.

        If this is an old changelist, created before Git Fusion 2013.3
        introduced DescInfo.parents, then we do not know if the original commit
        was a merge or not, and we're not going to spend time digging through
        old Git repos to find out. Return True.
        """
        val = self.ctx.repo_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                       p4gf_config.KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM)

                        # Don't know? Then give up.
        if not val:
            return True
        try:
            req_change_num = int(val)
        except ValueError:
            req_change_num = 0
        if not req_change_num:
            return True

                        # Current changelist before the cutoff?
        if int(change_num) <= req_change_num:
            return True

                        # No DescInfo block? This changelist originated in
                        # Perforce.
        desc_info = DescInfo.from_text(self.changes.get(change_num).description)
        if not desc_info:
            return True
                        # Current changelist description will contain multiple
                        # parent commit sha1s if this was originally a merge
                        # commit.
        return (    desc_info.parents
                and 2 <= len(desc_info.parents ))

    @staticmethod
    def _enforce_start_at(start_at):
        """Code expects start_at to be "@nnn" or None.
        If anything other than that got through, that's a bug
        in calling code or argparser.
        """
        if start_at is not None:
            if not re.match(r"@\d+$", start_at):
                raise ValueError("Bug: start_at='{}', class={}"
                      " must be an @-integer string such as '@123'"
                      .format( start_at, start_at.__class__.__name__ ))

    def copy(self, start_at, stop_at, new_git_branches):
        """copy a set of changelists from Perforce into Git.

        :param start_at: must be one of
            * "@{change_num}" or
            * None
            Anything else rejected.

        """
        # pylint: disable=too-many-branches
        self._log_memory(NTR('start'))
        self._enforce_start_at(start_at)

        # If the Git repository has already been populated by an
        # earlier pull, and there are tags to update, do so now.
        # (avoiding the early-exit logic below...)
        any_changes_since_last_copy = self._discover_changed_branches(self.all_branches())
        repo_empty = p4gf_util.git_empty()
        use_fast_reclone = repo_empty and not any_changes_since_last_copy

        # We must not attempt to add new tags which could refer to new commits
        # which we are just about to copy to this git repo
        if ( not repo_empty
             and p4gf_tag.any_tags_since_last_copy(self.ctx)
             and not any_changes_since_last_copy
             and not new_git_branches):
            p4gf_tag.update_tags(self.ctx)

        # Stop early if nothing to do.
        if (not repo_empty) and (not any_changes_since_last_copy) \
                and not new_git_branches:
            LOG.debug("No changes since last copy.")
            self.fastimport.cleanup()
            return

        # Lazily load the depot branch info data now that we know we have
        # some work to do.
        p4gf_branch.attach_depot_branch_info(
            self.ctx.branch_dict(), self.ctx.depot_branch_info_index())

        # if not any_changes_since_repo and is empty, mark all branches have no changes to copy
        if not any_changes_since_last_copy and not repo_empty:
            for b in self.all_branches():
                b.any_changes = False
        # if repo is empty, mark all branches may have changes to copy
        if repo_empty:
            for b in self.all_branches():
                b.any_changes = True

        # Assume new branches have changes to copy
        if new_git_branches:
            for b in self.all_branches():
                if b.git_branch_name in new_git_branches:
                    b.any_changes = True

        with Timer(OVERALL):
            self._log_memory(NTR('pygit2'))

            with Timer(SETUP):
                self._setup(start_at, stop_at)
                self._log_memory('_setup')

                if      (not len(self.changes.keys())) \
                    and (not self.branch_id_to_graft_num):
                    LOG.debug("No new changes found to copy")
                    return

            with Timer(PRINT):
                LOG.info('Copying file revisions from Perforce')
                self._copy_print()
                self._log_memory('_copy_print')

            if use_fast_reclone:
                use_fast_reclone = self._copy_fast_reclone()

            if not use_fast_reclone:
                sorted_changes = self._copy_normal_reclone(repo_empty)
            else:
                self.fastimport.cleanup()

            with Timer(PACK):
                self._pack()
                self._log_memory('_pack')

        LOG.getChild("time").debug("\n" + str(self))
        if not use_fast_reclone:
            LOG.info('Done. Changelists: {}  File Revisions: {}  Seconds: {}'
                     .format( len(sorted_changes)
                            , self.printed_rev_count
                            , int(Timer(OVERALL).time)))
        p4gf_mem_gc.report_objects(NTR('after P2G.copy()'))
        self._log_memory(NTR('copy() done'))

    def _copy_fast_reclone(self):
        """Attempt to do fast reclone from mirror.

        Return True if this works.
        """
        branch_heads, objects = p4gf_fast_reclone.fast_reclone(self.ctx)
        if not branch_heads:
            return False
        self._log_memory('_generate_tags')
        self._set_branch_refs_fast_reclone(branch_heads)
        success = p4gf_fast_reclone.check_fast_reclone(objects)
        if success:
            p4gf_tag.generate_tags(self.ctx)
        self._log_memory('_set_branch_refs_fast_reclone')
        p4gf_git.git_prune_and_repack()
        return success

    def _copy_normal_reclone(self, repo_empty):
        """Do clone the hard way."""
        sorted_changes = self._get_sorted_changes()
        self._log_memory('_get_sorted_changes')

        with Timer(FAST_IMPORT):
            fast_import_result = self._fast_import(sorted_changes)
            self._log_memory('_fast_import')

        if repo_empty:
            # If we are just now rebuilding the Git repository, also
            # grab all of the tags that have been pushed in the past.
            p4gf_tag.generate_tags(self.ctx)
            self._log_memory('_generate_tags')
        elif p4gf_tag.any_tags_since_last_copy(self.ctx):
            # Now we can add the new tags and not fail on missing objects
            p4gf_tag.update_tags(self.ctx)
            self._log_memory('_update_tags')

        if not p4gf_const.READ_ONLY:
            with Timer(MIRROR):
                self._mirror(fast_import_result)
                self._log_memory('_mirror')

        with Timer(BRANCH_REF):
            self._set_branch_refs(mark_lines=fast_import_result.marks)
            self._log_memory('_set_branch_refs')
        return sorted_changes

# -- module-wide --------------------------------------------------------------

# timer/counter names
OVERALL     = NTR('P4 to Git Overall')
SETUP       = NTR('Setup')
CHANGES     = NTR('p4 changes')
CHANGES1    = NTR('p4 changes -m1 -l')
FILELOG     = NTR('p4 filelog')
PRINT       = NTR('Print')
CALC_PRINT  = NTR('calc print')
FAST_IMPORT = NTR('Fast Import')
MIRROR      = NTR('Mirror')
BRANCH_REF  = NTR('Branch Ref')
PACK        = NTR('Pack')

FI_GITMIRROR   = NTR('FI from Git Mirror')
FI_SHA1_EXISTS = NTR('_sha1_exists')
FI_PRINT       = NTR('_print_to_git_store')
FI_TO_TREE     = NTR('_commit_to_tree_sha1')

                        # Most recently seen commit/changelist for a single
                        # branch. Usually only 1 of mark/sha1 filled in:
BranchHead = namedtuple('BranchHead', [
          'mark'        # of commit being copied as part of this fast-import
                        # Generated by MarkList.
        , 'sha1'        # of existing commit already part of Git
        , 'change_num'  # int changelist number of above commit
        ])

                        # Most recently skipped ghost changelist for a
                        # single branch. Values for P2G.branch_id_to_skipped_ghost
SkippedGhost = namedtuple('SkippedGhost', [
          'change_num'      # int changelist number of ghost changelist.
        , 'of_change_num'   # int changelist number of the changelist of
                            # which this ghost is a copy.
        ])


def _is_ghost_desc(desc):
    """Does this changelist description's tagged info block contain
    tags that appear only for ghost changelists?
    """
    desc_info = DescInfo.from_text(desc)
    return desc_info and desc_info.is_ghost()


def _to_temp_branch_name(branch):
    """Return git-fusion-temp-branch-foo."""
    if branch.git_branch_name:
        return p4gf_const.P4GF_BRANCH_TEMP_N.format(branch.git_branch_name)
    return p4gf_const.P4GF_BRANCH_TEMP_N.format(branch.branch_id)


def copy_p4_changes_to_git(ctx, start_at, stop_at, new_git_branches):
    """copy a set of changelists from perforce into git."""
    p2g = P2G(ctx)
    p2g.copy(start_at, stop_at, new_git_branches)


def _mark_or_sha1(branch_head):
    """For code that read the old commingled list of marks + sha1s,
    re-commingle the decommingled BranchHead tuple.
    """
    return branch_head.mark if branch_head.mark else branch_head.sha1

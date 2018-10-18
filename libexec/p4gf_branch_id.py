#! /usr/bin/env python3.3
"""
Given a list of Git/Perforce branch associations and a list of Git
commits, identify which branch of Perforce file hierarchy should receive
each Git commit.

UML diagram at doc/p4gf_branch_id.uml.pdf

1. Order pushed branches into a sequence:
    1. master-ish
    2. named fully populated
    3. named lightweight

2. For each named branch in the push, in the above sequence,    O(m) pushed branches
   assign commits to that branch:                               x

    2.1. If named branch existed pre-push
         Perform a tree walk, rooted at old ref location,       O(m) x O(n) commits
         calculating which children can reach the old ref
         (set a bool per commit)
         Later, when calculating this branch's path from old to new ref, we'll
         consider only those commits that can reach the old ref location.

       If named branch did not exist pre-push,
         then it doesn't matter what path we choose. Don't bother calculating
         reachability, don't check for it later when calculating path.

    2.2. Starting at new branch ref head and working back       O(m) x O(n) commits in branch path
         through parent links to the start of this branch,
         which is either
         * old ref location if one exists, or
         * any commit already copied to Perforce if this is a new branch ref

         Choose which parent to follow as first of:
          1. unassigned first-parent    *reachable
          2. unassigned any parent      *reachable
          3.   assigned first-parent    *reachable
          4.   assigned any parent      *reachable

          If we had an old ref location, then consider only
          parent commits that can read the old ref location
          (calculated in 2.1 above)

          If this commit is not yet assigned, assign to this branch.
          If commit already assigned, honor the preivous assignment.

   At this point, all *pushed* branches now have complete paths to their roots.
   Paths will often overlap.

   Yes this results in a named branch likely picking up completely unrelated and
   different commits than what the original author intended. We're just trying
   to reuse existing branches here, not recreate unknowable intentions.

3. At this point, there are usually still many commits with no branch assignments.
   Assign them to anonymous branches.

   Iterate through the 'git rev-list --date-order' list,        O(n) commits
   from newest/childmost to oldest/parentmost order.

   For each commit with no branch assignment                    O(p) new anonymous branches
   3.1. Reuse existing, or create new anonymous
        lightweight branch for this commit
   3.2. Repeat 2.2: assign this anonymous branch to             O(p) x O(n) commits in branch path
        this commit and a single chain of parents back
        to the start of pushed history.

   Yes this results in a single anonymous branch that strangely re-appears over
   and over again for distant and unrelated islands of previously unassigned
   commits. That's okay. Better one branch, re-appearing 5 times,
   than 5 branches, each with only one changelist. Either way requires the same
   number of file branch/merge/integ records, but this way creates fewer branches
   of Perforce depot hiearchy: less to render in P4V's Revision Graph, less to
   track in //P4GF_DEPOT/branches/... .

   Now every commit has one branch assignment.

4. For each pushed branch head                                  O(m) pushed branches
   If that head's commit is not assigned to this branch
       add this branch as a second-or-later assignment.

   So that when Git Fusion receives a push with multiple heads on the same
   commit, later code will know to repeat that commit on each of the branches,
   leaving each Perforce depot branch's #head state matching Git's head commit
   state.

5. Thin the memory footprint.                                   O(n) commits
   Discard anything we no longer need.
   Replace our larger Assign instances with much smaller AssignFrozen instances

"""

import configparser
import logging
import os
import re

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_branch
import p4gf_const
import p4gf_histogram
from   p4gf_l10n import _, NTR
import p4gf_log
from   p4gf_object_type import ObjectType
import p4gf_proc
from   p4gf_profiler import Timer
import p4gf_progress_reporter as ProgressReporter
import p4gf_util

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_branch_id")
LOG_GRAPH = LOG.getChild('graph')
LOG_TIME  = LOG.getChild('time')

# Set to None to disable very noisy dump of log.
_DUMP_LOG = (NTR('git'), NTR('log'), NTR('--graph'), NTR('--no-color'), NTR('--format=%H %s'))

TUNNEL_UNLIMITED = -1


class Assigner:

    """Group all the related data in a single object we can pass around and call.

    New for first-push use, set all before calling assign():
        connect_to_previous_branch_sha1
        tunnel_max_ct
        tunnel_assign
    """

    def __init__(self
                , branch_dict
                , pre_receive_list
                , ctx = None):

        self.ctx = ctx

        # Original input: list of PreReceiveTuple elements.
        self.pre_receive_list = pre_receive_list

        # branch_id to Branch object of all branches. Starts out with just the
        # branches from config, but grows as we add new Branch instances for
        # newly pushed branches, or newly discovered anonymous branches.
        self.branch_dict      = branch_dict

        # List of sha1, one for each Git commit being pushed. Order MUST
        # be in topological order with the newest/child commits before
        # all of their older/parent commits.
        self.rev_list         = []

        # All these behavior controls tend to be set either all to True or all
        # to False, True for normal operation, False for Fast Push. We could
        # probably offer a less noisy API. Zig has a mild preference for
        # precise APIs that don't conflate.

        # Behavior control: check incoming commits to see if they're
        # already assigned to some existing branch?
        self.assign_previous = True

        # Behavior control: set to False to ignore current branch references in
        # the repo. When using the Assigner outside of 'git push' pre-receive-
        # hook time, you usually want to drag each new branch head all the way
        # back to the beginning of time, not back to its current location
        # (which is almost always the same as prt.new_sha1).
        self.connect_to_previous_branch_sha1 = True

        # Behavior control: flatten our memory-expensive Assign elements down
        # to much cheaper AssignFrozen elements? Skip if you plan on discarding
        # our data anyway, and save yourself 2% of our assign() time.
        self.flatten_memory = True

        # Behavior control: how many commits in a row is a branch allowed to
        # share with other branches before this branch should give up and
        # terminate? Leave unlimited for maximal branch re-use, minimal branch
        # count.
        self.tunnel_max_ct    = TUNNEL_UNLIMITED
        # Tracking any current tunnelling through shared commits.
        # Tracked only when tunnel_max_ct is not TUNNEL_UNLIMITED.
        self.tunnel_list      = list()

        # Behavior control: actually assign while tunnelling? Causes more
        # commits to be submitted to multiple branches, which is exactly what
        # we want for first-push.
        self.tunnel_assign    = False

        # sha1 to Assign object, one for every pushed commit.
        self.assign_dict      = {}

        # Instrumentation: How long are our branches?
        self.branch_len       = {}
        # Record whether anonymous branches exist
        self.have_anonymous_branches = False

        if LOG.isEnabledFor(logging.DEBUG2):
            for i, prt in enumerate(pre_receive_list):
                LOG.debug2('PRT[{}] {}'.format(i, prt))

    def assign(self):
        """Main entry point. Assign a branch ID to every rev in our rev_list."""
        with Timer(TIMER_OVERALL):
            self._load_commit_dag()
            self._assign_to_loaded_dag()
        self._dump_instrumentation()

    def _assign_to_loaded_dag(self):
        """Run the assigner over our commit DAG."""
        # Zig thinks this _add_assign_for_ref_heads() no longer necessary.
        with Timer(TIMER_BRANCH_HEAD):
            self._add_assign_for_ref_heads()

        with Timer(TIMER_ASSIGN_PREVIOUS):
            self._assign_previous()

        with Timer(TIMER_ASSIGN_BRANCH_NAMED):
            self._assign_branches_named()

        with Timer(TIMER_ASSIGN_BRANCH_ANON):
            self._assign_branches_anon()

        with Timer(TIMER_BRANCH_HEAD):
            self._force_assign_pushed_ref_heads()

        with Timer(TIMER_FREE_MEMORY):
            self._free_memory()

    def is_assigned(self, sha1):
        """Do we have at least one branch assigned for this commit?"""
        return sha1 in self.assign_dict

    def branch_id_list(self, sha1):
        """Return a list of branch_ids assigned to a commit."""
        return self.assign_dict[sha1].branch_id_list()

    def _assign_previous(self):
        """Many commits in assign_dict were already assigned branches in a
        previous push or pull. Remember and honor those.
        """
        if not self.assign_previous:
            return

        # Test-only mode skips this step and that's okay.
        if not self.ctx:
            return

        for assign in self.assign_dict.values():
            otl = ObjectType.commits_for_sha1(self.ctx, assign.sha1)
            for ot in otl:
                self._assign_branch(assign, ot.branch_id)

    def _free_memory(self):
        """Dump everything we no longer need once assignment is complete.

        Replace large Assign instances with tiny AssignFrozen instances.
        """
        if not self.flatten_memory:
            return

        #self.ctx          = None
        #self.branch_dict  = None
        #self.rev_list     = None
        self.assign_dict = {sha1: AssignFrozen(assign.branch_id)
                            for sha1, assign in self.assign_dict.items()}

    def annotate_lines(self, lines):
        """Append "(branch_id1)" string to any line that contains a sha1
        that has a branch assignment.
        """
        re_sha1 = re.compile(NTR('([0-9a-f]{40})'))
        r = []
        for l in lines:
            m = re_sha1.search(l)
            if m:
                sha1 = m.group(1)
                if sha1 in self.assign_dict:
                    annotation = ' ({})'.format(', '
                                .join(self.assign_dict[sha1].branch_id_list()))
                    l += annotation
            r.append(l)
        return r

    def _dump_instrumentation(self):
        """Debugging dump of timing and other info."""
        if _DUMP_LOG and LOG_GRAPH.isEnabledFor(logging.DEBUG3):
            cmd = list(_DUMP_LOG) + [prt.new_sha1 for prt in self.pre_receive_list]
            p = p4gf_proc.popen_no_throw(cmd)
            l = self.annotate_lines(p['out'].splitlines())
            LOG_GRAPH.debug3('Log: {}\n{}'.format(' '.join(cmd), '\n'.join(l)))

        total_seconds = Timer(TIMER_OVERALL).time
        total_rev_ct = len(self.assign_dict)
        LOG_TIME.debug("branches      : {}".format(len(self.branch_dict)))
        LOG_TIME.debug("commits       : {}".format(total_rev_ct))
        LOG_TIME.debug("seconds       : {}".format(int(total_seconds + 0.5)))
        if 1.0 <= total_seconds:
            # Commits per second math becomes unreliable for short runs.
            rev_per_second = total_rev_ct / total_seconds
            LOG_TIME.debug("commits/second: {}".format(int(rev_per_second + 0.5)))

        if self.branch_len:
            histo = p4gf_histogram.to_histogram(self.branch_len.values())
            histo_lines = p4gf_histogram.to_lines(histo)
            LOG_TIME.debug('Branch length histogram: how many branches have N commits?\n'
                           + '\n'.join(histo_lines))

    def _load_commit_dag(self):
        """Load the Git commit tree into memory.

        We just need the parent/child relationships.
        """
        # pylint:disable=too-many-branches
        # A single call to git-rev-list produces both the commit sha1 list
        # that we need AND the child->parent associations that we need. It's
        # screaming fast: 32,000 commit lines in <1 second.
        with Timer(TIMER_RUN_REV_LIST):
            range_list = sorted(set([prt.to_range()
                                     for prt in self.pre_receive_list]))
            cmd        = [ 'git', 'rev-list'
                         , '--date-order', '--parents'] + range_list
            LOG.debug2("DAG: {}".format(' '.join(cmd)))
            d = p4gf_proc.popen(cmd)

        seen_parents = set()

        # Pass 1: Build up a dict of sha1->Assign objects, one per commit.
        with Timer(TIMER_CONSUME_REV_LIST):
            lines = d['out'].splitlines()
            with ProgressReporter.Determinate(len(lines)):
                for line in lines:
                    ProgressReporter.increment(_('Loading commit tree into memory...'))
                    sha1s = line.split()
                    curr_sha1 = sha1s.pop(0)
                    self.rev_list.append(curr_sha1)
                    if LOG.isEnabledFor(logging.DEBUG3):
                        LOG.debug3('DAG: rev_list {} {}'
                                   .format( p4gf_util.abbrev(curr_sha1)
                                          , ' '.join(p4gf_util.abbrev(sha1s))))
                    self.assign_dict[curr_sha1] = Assign(curr_sha1, sha1s)
                    seen_parents.update(sha1s)

        # git-rev-list is awesome in that it gives us only as much as we need
        # for self.rev_list, but unawesome in that this optimization tends to
        # omit paths to branch refs' OLD heads if the old heads are 2+ commits
        # back in time, and that time is ALREADY covered by some OTHER branch.
        # Re-run each pushed branch separately to add enough Assign() nodes
        # to form a full path to its old ref.
        if 2 <= len(self.pre_receive_list):
            for prt in self.pre_receive_list:
                # Skip NEW branch refs: those don't have
                # to connect up to anything.
                if prt.old_sha1 == p4gf_const.NULL_COMMIT_SHA1:
                    continue
                with Timer(TIMER_RUN_REV_LIST):
                    cmd = ['git', 'rev-list'
                           , '--date-order', '--parents', '--reverse', prt.to_range()]
                    LOG.debug2("DAG: {}".format(' '.join(cmd)))
                    d = p4gf_proc.popen(cmd)

                with Timer(TIMER_CONSUME_REV_LIST):
                    for line in d['out'].splitlines():
                        sha1s = line.split()
                        curr_sha1 = sha1s.pop(0)
                        if curr_sha1 in self.assign_dict:
                            break
                        LOG.debug3('DAG: path     {} {}'
                                   .format( p4gf_util.abbrev(curr_sha1)
                                          , ' '.join(p4gf_util.abbrev(sha1s))))
                        self.assign_dict[curr_sha1] = Assign(curr_sha1, sha1s)
                        seen_parents.update(sha1s)

        # Create acting-as-parent-only nodes in dict, too. We don't process
        # these as part of iterating over revs, but we need them when
        # tree walking.
        with Timer(TIMER_CONSUME_REV_LIST):
            parent_only = seen_parents - set(self.assign_dict.keys())
            for curr_sha1 in parent_only:
                if curr_sha1 in self.assign_dict:
                    break
                LOG.debug3('DAG: par only {}'.format(p4gf_util.abbrev(curr_sha1)))
                self.assign_dict[curr_sha1] = Assign(curr_sha1, [])

        # Pass 2: Fill in Assign.children list
        with Timer(TIMER_ASSIGN_CHILDREN):
            with ProgressReporter.Determinate(len(self.assign_dict)):
                for assign in self.assign_dict.values():
                    ProgressReporter.increment(_('Finding child commits...'))
                    for par_sha1 in assign.parents:
                        par_assign = self.assign_dict.get(par_sha1)
                        if par_assign:
                            par_assign.children.add(assign.sha1)
                        else:
                            # Expected and okay: some parents already exist and
                            # are not part of our push/fast-export list.
                            LOG.debug2(
                                "DAG: child {child} -> parent {parent}: parent not part of push"
                                .format(child=assign.sha1[:7], parent=par_sha1[:7]))

    def _branch_id_to_sha1(self):
        """Return a dict of branch_id to sha1 of what the world should look
        like AFTER this push completes.

        Creates new Branch instances and adds them to branch_dict if we
        encounter branch refs not yet in our branch_dict.

        Creates new Assign instances and adds them to assign_dict if the
        referenced sha1 is not part of this push. Happens often: any reference
        that does not move, and sometimes new references too, point to commits
        that we received in some  previous push.

        """
        known = [p4gf_branch.BranchRef('refs/heads/' + bm.git_branch_name)
                 for bm in self.undeleted_branches()
                 if bm.git_branch_name]
        pushed = [prt.ref for prt in self.pre_receive_list]
        LOG.debug2('_assign_branch_id_to_ref_heads() known={} pushed={}'
                   .format(known, pushed))
        ref_list = set(known + pushed)
        ref_to_sha1 = p4gf_util.git_ref_list_to_sha1(ref_list)
        # Use the pre-receive tuples to help with commit-to-branch assignment.
        for prt in self.pre_receive_list:
            ref_to_sha1[prt.ref] = prt.new_sha1

        result = {}
        for ref in sorted(ref_list):
            sha1 = ref_to_sha1[ref]
            if not sha1:
                LOG.debug2("abid() p4gf_util.git_ref_list_to_sha1()"
                           " returned no sha1 for ref={}".format(ref))
                continue

            branch_id = self.ref_to_branch_id(ref)
            if not branch_id:
                # This is a new branch. Create a new Branch instance
                # for it and record its Git name.
                branch = self._create_anon_branch()
                if ref.startswith('refs/heads/'):
                    branch.git_branch_name = ref[len('refs/heads/'):]
                branch_id = branch.branch_id

            a = self.assign_dict.get(sha1)
            if not a:
                # Push assigns branch ref to a commit we already had from
                # some previous push/pull. Must store the assignment so that
                # p4gf_copy_to_p4 will know where to put the branch ref.
                #
                # This Assign object lacks parent info, but that's okay
                # since we're just using it to store a single branch ref
                # assignment, not in deeper branch id calculations.
                a = Assign(sha1)
                self.assign_dict[sha1] = a

            result[branch_id] = sha1

        return result

    def _add_assign_for_ref_heads(self):
        """Make sure that each known or newly pushed branch reference has an
        Assign instance to (eventually) receive that branch assignment.

        _load_commit_dag() creates Assign instances only for pushed refs that
        point to a newly pushed commits. It does not see any old, unpushed refs,
        nor any pushed refs that point to commits that we received in an earlier
        push. Those are is not yet in rev_list or assign_dict.

        This Assign object lacks parent info, but that's okay since we're just
        using it to store a single branch ref  assignment, not in deeper
        branch id calculations.

        The created Assign objects are NOT assigned to any branch yet. That's
        _assign_branches_named()'s job.
        """
        for sha1 in self._branch_id_to_sha1().values():
            a = self.assign_dict.get(sha1)
            if not a:
                a = Assign(sha1)
                self.assign_dict[sha1] = a

    def _force_assign_pushed_ref_heads(self):
        """Force all pushed references to be one of the (possibly multiple) branch
        assignments for their commits. This causes each branch's final head to
        create one final Perforce changelist on that branch, even if this commit
        appears on multiple branches.
        """
        for branch in self._pushed_branch_sequence():
            new_head_sha1 = self._branch_to_pushed_new_head_sha1(branch)
            assign = self.assign_dict.get(new_head_sha1)
            self._assign_branch(assign, branch.branch_id)

    def _assign_branches_named(self):
        """For each pushed branch reference, find a path from its new head location
        to its old head location (if any). Assign commits to the branch as we
        find the path.
        """
        LOG.debug('_assign_branches_named')
        for branch in self._pushed_branch_sequence():
            LOG.debug2('_assign_branches_named branch={}'.format(p4gf_branch.abbrev(branch)))
            if self.connect_to_previous_branch_sha1:
                old_head_sha1 = _branch_to_old_head_sha1(branch)
            else:
                old_head_sha1 = None

            new_head_sha1 = self._branch_to_pushed_new_head_sha1(branch)
            if new_head_sha1 is None:
                raise RuntimeError(_("BUG: _pushed_branch_sequence() returned a branch"
                                     " '{branch}' with no corresponding PreReceiveTuple")
                                   .format(branch=branch.to_log(LOG)))
            if new_head_sha1 == p4gf_const.NULL_COMMIT_SHA1:
                LOG.debug('_assign_branches_named(): skipping branch={}:'
                          ' new sha1 is 0000000'.format(branch.to_log(LOG)))
                return

            if old_head_sha1:
                self._assign_branch_named_old_to_new( branch
                                                    , old_head_sha1=old_head_sha1
                                                    , new_head_sha1=new_head_sha1)
            else:
                self._assign_branch_named_any_to_new( branch
                                                    , new_head_sha1=new_head_sha1)

    def _assign_branch_named_old_to_new(self, branch, old_head_sha1, new_head_sha1):
        """Find a path from this branch's new head location back to its old
        head location.

        Assign this branch to all commits along the path, unless those commits
        already have a branch assignment.
        """
        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2('_assign_branch_named_old_to_new() branch={} {} from {}..{}'
                       .format( p4gf_branch.abbrev(branch.branch_id)
                              , branch.git_branch_name
                              , p4gf_util.abbrev(old_head_sha1)
                              , p4gf_util.abbrev(new_head_sha1)))

        # Note which commits are descendants of the old head.
        # Only such commits are possible choices when creating a path
        # from new head to old.
        self._set_reachable_by(old_head_sha1, branch)

        # Choose only reachable parents to create the path.
        self._assign_path( assign_branch=branch
                         , new_head_sha1=new_head_sha1
                         , reachable_by=branch)

    def _assign_branch_named_any_to_new(self, branch, new_head_sha1):
        """Starting at new head and working back through parent links to
        any root-most newly pushed commit, assign branch to commits along
        the path unless such commits already have a branch assignment.
        """
        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2('_assign_branch_named_any_to_new() branch={} {} from ???????..{}'
                       .format( p4gf_branch.abbrev(branch.branch_id)
                              , branch.git_branch_name
                              , p4gf_util.abbrev(new_head_sha1)))

        # Choose any parents to create the path.
        self._assign_path( assign_branch=branch
                         , new_head_sha1=new_head_sha1
                         , reachable_by=None)

    def _branch_to_pushed_new_head_sha1(self, branch):
        """Return the new head sha1 in branch's corresponding PreReceiveTuple."""
        prt_ref = p4gf_branch.BranchRef('refs/heads/' + branch.git_branch_name)
        for prt in self.pre_receive_list:
            if prt.ref == prt_ref:
                return prt.new_sha1
        return None

    def _assign_path(self, assign_branch, new_head_sha1, reachable_by):
        """Starting at new head and working back through parent links to old head,
        assign branch to commits along the path unless such commits already
        have a branch assignment.
        """
        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2('_assign_path() new_head={new_head_sha1}'
                       ' assign={assign_branch} reachable={reachable_by}'
                       .format(new_head_sha1   = p4gf_util.abbrev(new_head_sha1)
                               , assign_branch = p4gf_branch.abbrev(assign_branch)
                               , reachable_by  = p4gf_branch.abbrev(reachable_by)
                               ))
        curr_assign = self.assign_dict.get(new_head_sha1)
        self._tunnel_reset()

        while True:
            if not curr_assign.branch_id:
                self._assign_branch(curr_assign, assign_branch.branch_id)
                if LOG.isEnabledFor(logging.DEBUG3):
                    LOG.debug3('_assign_path curr={}         assigned {}'
                               .format( p4gf_util.abbrev(curr_assign.sha1)
                                      , p4gf_util.abbrev(assign_branch.branch_id)))
            else:
                        # Normally don't assign if already assigned,
                        # but in tunnel mode, yeah, assign.
                if (    self.tunnel_assign
                    and assign_branch.branch_id not in curr_assign.branch_id):
                    self._assign_branch(curr_assign, assign_branch.branch_id)
                    LOG.debug3('_assign_path curr={} tunnelling through ({})'
                               .format( p4gf_util.abbrev(curr_assign.sha1)
                                      , p4gf_util.abbrev(curr_assign.branch_id_str())))
                else:
                    LOG.debug3('_assign_path curr={} already assigned ({})'
                               .format( p4gf_util.abbrev(curr_assign.sha1)
                                      , p4gf_util.abbrev(curr_assign.branch_id_str())))

            chosen_par_assign = self._best_parent_assign( curr_assign, assign_branch.branch_id
                                                        , reachable_by)
            if not chosen_par_assign:
                if LOG.isEnabledFor(logging.DEBUG3):
                    LOG.debug3('_assign_path curr={} no usable parent. Done.'
                               .format(p4gf_util.abbrev(curr_assign.sha1)))
                self._tunnel_back_out(assign_branch.branch_id)
                break

            if not self._tunnel_check_passes( chosen_par_assign
                                            , assign_branch.branch_id):
                if LOG.isEnabledFor(logging.DEBUG3):
                    LOG.debug3('_assign_path curr={} exceeds tunnel length. Done.'
                               .format(p4gf_util.abbrev(curr_assign.sha1)))
                break

            curr_assign = chosen_par_assign

    def _best_parent_assign(self, child_assign, branch_id, required_reachable_by=None):
        """Return one of child_assign's parent Assign instances.

        First available match in this order:

         Choose which parent to follow as first of:
          1.   assigned parent already assigned to this branch
          2. unassigned first-parent    *reachable
          3. unassigned any parent      *reachable
          4.   assigned first-parent    *reachable
          5.   assigned any parent      *reachable

        If required_reachable_by passed in as non-None, then considers only
        parents with reachable_by==required_reachable_by
        """
        # pylint: disable=too-many-branches

        if not child_assign.parents:
            return None

        # 1. Do we already have one parent assigned to this branch?
        #    Never assign more than one parent to the same branch.
        par_assign_list = None
        for par_sha1 in child_assign.parents:
            par_assign = self.assign_dict.get(par_sha1)
            if not par_assign:
                continue
            if par_assign.branch_id and  branch_id in par_assign.branch_id:
                return par_assign

        # 2. unassigned first-parent    *reachable
        first_par_assign = self.assign_dict.get(child_assign.parents[0])
        # Require reachable (if caller requested)
        if (    first_par_assign
            and required_reachable_by
            and first_par_assign.reachable_by is not required_reachable_by):
            LOG.debug('# par.reachable={} != req={}'
                      .format( first_par_assign.reachable_by
                             , required_reachable_by))
            first_par_assign = None
        # Unassigned? We've got a winner.
        if first_par_assign and not first_par_assign.branch_id:
            return first_par_assign

        # 3. unassigned any parent      *reachable
        par_assign_list = None
        for par_sha1 in child_assign.parents:
            par_assign = self.assign_dict.get(par_sha1)
            if not par_assign:
                continue
            if (    required_reachable_by
                and par_assign.reachable_by is not required_reachable_by):
                continue
            # Unassigned? We've got a winner.
            if not par_assign.branch_id:
                return par_assign
            # No unassigned winner yet?
            # Remember our list of parent Assign for later
            if not par_assign_list:
                par_assign_list = [par_assign]
            else:
                par_assign_list.append(par_assign)

        # 4. assigned first-parent    *reachable
        if first_par_assign:
            return first_par_assign

        # 5. assigned any parent      *reachable
        if par_assign_list:
            return par_assign_list[0]

        return None

    def _set_reachable_by(self, old_head_sha1, reachable_by):
        """Tree-walk a commit and all of its descendants, setting their
        reachable_by pointer to the given branch.

        O(n) commits worst-case (all commits child of old_head_sha1)
        """
        LOG.debug2('_set_reachable_by() old_head_sha1={} reachable_by={}'
                   .format( p4gf_util.abbrev(old_head_sha1)
                          , p4gf_util.abbrev(reachable_by.branch_id)))

        old_head_assign = self.assign_dict.get(old_head_sha1)
        if not old_head_assign:
            LOG.debug3('_set_reachable_by() old_head not in assign_dict. Done.')
            return
        work_queue = [old_head_assign]
        while work_queue:
            curr_assign = work_queue.pop()
            if not curr_assign:
                continue
            curr_assign.reachable_by = reachable_by
            LOG.debug2('curr={} set {}'
                       .format( p4gf_util.abbrev(curr_assign.sha1)
                              , p4gf_util.abbrev(reachable_by.branch_id)))
            for child_sha1 in curr_assign.children:
                # Visit children, but skip ones we've already seen due to some
                # other path (merge commits)
                child_assign = self.assign_dict.get(child_sha1)
                if not child_assign:
                    LOG.debug3('curr={} child={} not found'
                               .format( p4gf_util.abbrev(curr_assign.sha1)
                                      , p4gf_util.abbrev(child_sha1)))
                    continue
                elif child_assign.reachable_by is reachable_by:
                    LOG.debug3('curr={} child={} already set'
                               .format( p4gf_util.abbrev(curr_assign.sha1)
                                      , p4gf_util.abbrev(child_sha1)))
                    continue
                else:
                    LOG.debug3('curr={} child={} enqueued'
                               .format( p4gf_util.abbrev(curr_assign.sha1)
                                      , p4gf_util.abbrev(child_sha1)))
                    work_queue.append(child_assign)

    def _pushed_branch_sequence(self):
        """Return a list of Branch instances, one for each pushed branch reference.

        Order the branches by priority: which branches get first dibs
        on unassigned commits?
        1. master-ish
        2. named fully populated
        3. named lightweight
        """
        masterish       = None
        fully_populated = []
        lightweight     = []
        pushed_git_name_list = [p4gf_branch.BranchRef(prt.ref[len('refs/heads/'):])
                                for prt in self.pre_receive_list
                                if prt.ref.startswith('refs/heads/')]
        for branch in self.undeleted_branches():
            if (   (not branch.git_branch_name)
                or (not branch.git_branch_name in pushed_git_name_list)):
                continue
            elif branch.more_equal:
                masterish = branch
            elif branch.is_lightweight:
                lightweight.append(branch)
            else:
                fully_populated.append(branch)

        if masterish:
            result = [masterish]
        else:
            result = []

        # Why sorted()?  Order within a group doesn't _officially_ matter, but
        # it is a lot easier to write reproducible tests if we have a
        # reproducible order. git_branch_name is easily visible and controllable
        # by test scripts so favor that when available. Fall back to branch_id
        # for anonymous branches that lack a git_branch_name.
        result.extend(sorted(fully_populated, key=lambda br: br.git_branch_name))
        result.extend(sorted(lightweight,     key=lambda br: br.branch_id))
        # LOG.debug('### _pushed_branch_sequence returning={}'.format(result))
        return result

    def _assign_branches_anon(self):
        """Assign each remaining unassigned commit to an anonymous branch.

        Reuse existing anonymous branches before creating a new one.
        """
        # Sort by branch_id solely to force reproducible results.
        anon_branch_list = sorted( (b for b in self.undeleted_branches()
                                    if not b.git_branch_name)
                                  , key=lambda x: x.branch_id
                                  , reverse=True )
        # For each commit with no branch assignment
        for sha1 in self.rev_list:
            assign = self.assign_dict.get(sha1)
            if assign.branch_id:
                continue

            # Record if exist at least one anonymous branch
            # This setting used to support enable-git-branch-creation
            self.have_anonymous_branches = True
            # 3.1 Reuse existing, or create new anonymous
            #     lightweight branch for this commit
            if anon_branch_list:
                branch = anon_branch_list.pop()
            else:
                branch = self._create_anon_branch()

            # 3.2. Repeat 2.2: assign this anonymous branch to
            #     this commit and a single chain of parents back
            #     to the start of pushed history.
            self._assign_path( assign_branch=branch
                             , new_head_sha1=sha1
                             , reachable_by =None)

    def _create_anon_branch(self):
        """Create a new Branch object to hold an anonymous branch.

        View left empty. We haven't completed our branch assignment pass, and
        view depends on parent commit(s)'s assigned branch and its view.

        Store new Branch object in our branch_dict.
        """
        # Test-only mode omits a context
        if self.ctx:
            p4gf = self.ctx.p4gf
        else:
            p4gf = None
        branch                      = p4gf_branch.Branch(p4=p4gf)
        branch.is_lightweight       = True
        self.branch_dict[branch.branch_id] = branch
        return branch

    def ref_to_branch_id(self, ref):
        """Which Git Fusion branch ID contains ref as a Git branch name?

        O(n) scan.
        """
        if ref.startswith('refs/heads/'):
            ref = ref[11:]
        ref = p4gf_branch.BranchRef(ref)
        for bm in self.undeleted_branches():
            # ignore any deleted branch
            if bm.git_branch_name == ref and not bm.deleted:
                return bm.branch_id
        return None

    def _assign_branch(self, assign, branch_id):
        """Add branch_id to assign's list of branches."""
        if assign.add_branch_id(branch_id):
            _increment_bucket(branch_id, self.branch_len)

    def _unassign_branch(self, assign, branch_id):
        """Remove branch_id from assign's list of branches."""
        if assign.remove_branch_id(branch_id):
            _decrement_bucket(branch_id, self.branch_len)

    def undeleted_branches(self):
        """An iterator/generator of all Branch values in branch_dict
        that are not deleted.

        Intentionally duplicating Context.undeleted_branches() here
        rather than requiring a non-None self.ctx
        """
        for branch in self.branch_dict.values():
            if branch.deleted:
                continue
            yield branch

    def to_dict(self):
        """Convert this object to a map that can be serialized via JSON."""
        result = dict()
        result['rev_list'] = self.rev_list
        assign_dict = {k: v.to_dict() for k, v in self.assign_dict.items()}
        result['assign_dict'] = assign_dict
        return result

    @staticmethod
    def from_dict(d, branch_dict, pre_receive_list, ctx):
        """Create an instance based on the given map, as from to_dict."""
        result = Assigner(branch_dict, pre_receive_list, ctx)
        result.rev_list = d['rev_list']
        ad = d['assign_dict']
        result.assign_dict = {k: AssignFrozen.from_dict(ad[k]) for k in ad}
        return result

    def _tunnel_check_passes(self, chosen_par_assign, branch_id):
        """Does this assignment fit within set limits about sharing
        commits with other branches?

        In addition to testing for violation, update internal
        tracking list.
        """
                        # Not tracking tunnel length?
                        # Then don't...er...track tunnel length.
        if self.tunnel_max_ct == TUNNEL_UNLIMITED:
            return True

        is_tunnel = 0 < len(chosen_par_assign.branch_id)

                        # Not tunnelling? Then clear any current tunnel
                        # tracking and permit it.
        if not is_tunnel:
            self._tunnel_reset()
            return True

                        # Tunnel can accept one more?
                        # Then do so and we're done.
        if  len(self.tunnel_list) < self.tunnel_max_ct:
            LOG.debug("tunneling through {} for branch_id={} already assigned={}"
                .format(p4gf_util.abbrev(chosen_par_assign.sha1)
                    , branch_id
                    , chosen_par_assign.branch_id_str()))
            self.tunnel_list.append(chosen_par_assign)
            return True

                        # Tunnel cannot accept any more assignments.
                        # Current tunnel attempt has failed. Un-assign
                        # everything in the tunnel, then reject.
        LOG.debug("backing out, len={} sha1={} branch_id={} tunnel={}"
                  .format(len(self.tunnel_list)
                          , p4gf_util.abbrev(chosen_par_assign.sha1)
                          , branch_id
                          , ", ".join( [p4gf_util.abbrev(a.sha1)
                                        for a in self.tunnel_list])
                          ))
        self._tunnel_back_out(branch_id)
        return False

    def _tunnel_reset(self):
        """Clear any tunnel tracking list: we're starting a new branch."""
        if self.tunnel_list:
            LOG.debug("tunnel reset")
            self.tunnel_list = list()

    def _tunnel_back_out(self, branch_id):
        """Tunnel too long. Remove assignment."""
        for assign in self.tunnel_list:
            self._unassign_branch(assign, branch_id)


# -- class Assign -------------------------------------------------------------

class Assign:

    """A single commit's branch assignment and the work leading up to that
    assignment.
    """

    def __init__(self, sha1=None, parents=None):
        # This commit's sha1
        self.sha1       = sha1

        # Which branches of Perforce file hierarchy receive this commit. Can
        # be multiple if multiple branch heads point to the same commit
        # (only happens for heads right now, we lack the smarts to know when
        # to drag multiple branches back through shared history.)
        #
        # Likely to convert to single string, with heads' multi-branch-id
        # stored in a field that we can leave None for most of the time.
        self.branch_id  = set()

        # List of child commit sha1s that use this commit as a parent.
        self.children   = set()

        # List of parent sha1s, first-parent is [0].
        self.parents    = parents

        # Is this commit a descendant of the current branch's old head commit?
        # If so, then reachable_by points to the current branch instance. If
        # not, then reachable_by is left pointing to either None or some other
        # previous branch instance.
        self.reachable_by   = None

    def add_branch_id(self, branch_id):
        """Assign this commit to the given branch_id.

        Return True if actually added, False if already had
        this branch_id assigned.
        """
        assert isinstance(branch_id, p4gf_branch.BranchId)
        if branch_id in self.branch_id:
            return False
        self.branch_id.add(branch_id)
        return True

    def remove_branch_id(self, branch_id):
        """Un-assign this commit from the given branch_id.

        Return True if actually removed, False if already lacked
        this branch_id.
        """
        assert isinstance(branch_id, p4gf_branch.BranchId)
        if branch_id not in self.branch_id:
            return False
        self.branch_id.remove(branch_id)
        return True

    def branch_id_str(self):
        """Return our branch ID(s) as a single string."""
        if not self.branch_id:
            return None
        return ' '.join(self.branch_id)

    def branch_id_list(self):
        """Return our branch ID(s) as a list.

        Return empty list if no assignments.
        """
        return list(self.branch_id)

    def release_links(self):
        """Sever all ties to parents and children, but retain sha1.

        Called by FastPush as it iterates through our assignments, freeing
        memory by destructively removing Assign elements as it process the
        assigner dict.

        Keep the sha1: before FastPush severs a child's link to its parent(s),
        it (very rarely) needs to its first-parent sha1 to use that for a ghost
        changelist.
        """
        self.branch_id    = None
        self.children     = None
        self.parents      = None
        self.reachable_by = None

    def __repr__(self):
        return '{} {}'.format(p4gf_util.abbrev(self.sha1), self.branch_id_str())


# -- class AssignFrozen -------------------------------------------------------

class AssignFrozen:

    """A single commit's branch assignment.

    A smaller subset of what Assign stores. We don't need to keep around all
    that work-in-progress data once we're done assigning branches. Switching to
    this smaller class frees up memory for later use.
    """

    def __init__(self, branch_id=None):

        # _branch_id is usually a single branch_id str
        # But for the rare case where multiple pushed branch refs point
        # to the same commit, _branch_id is a list of branch_id str.
        #
        self._branch_id = [p4gf_branch.BranchId(b) for b in self._flatten(branch_id)]

    def branch_id_list(self):
        """Return our branch ID(s) as a list."""
        return list(self._branch_id)

    def branch_id_str(self):
        """Return our branch ID(s) as a single string."""
        if not self._branch_id:
            return None
        return ' '.join(self.branch_id_list())

    @staticmethod
    def _flatten(branch_id):
        """Prefer scalar, then list."""
        if not branch_id or isinstance(branch_id, str):
            return branch_id
        # Sorting solely to allow reproducible test results.
        # Order should not matter.
        return sorted(branch_id)

    def to_dict(self):
        """Convert this object to a map that can be serialized via JSON."""
        return {'branch_id': self._branch_id}

    @staticmethod
    def from_dict(d):
        """Create an instance based on the given map, as from to_dict."""
        return AssignFrozen(d['branch_id'])


# -- class PreReceiveTuple ----------------------------------------------------

class PreReceiveTuple:

    """One line from a pre-receive-hook stdin."""

    def __init__(self, old_sha1, new_sha1, ref):
        self.old_sha1 = old_sha1
        self.new_sha1 = new_sha1
        self._ref = None
        self.ref = ref

    @property
    def ref(self):
        """Get ref."""
        return self._ref

    @ref.setter
    def ref(self, value):
        """Set ref."""
        self._ref = p4gf_branch.BranchRef(value)

    def to_range(self):
        """Return old..new if we have an old, or just new if we don't."""
        if (not self.old_sha1) or (self.old_sha1 == p4gf_const.NULL_COMMIT_SHA1):
            return self.new_sha1
        return '{}..{}'.format(self.old_sha1, self.new_sha1)

    def __str__(self):
        """Return string representation of pre-receive info."""
        return 'old={0}, new={1}, ref={2}'.format(
                                               p4gf_util.abbrev(self.old_sha1)
                                             , p4gf_util.abbrev(self.new_sha1)
                                             , self.ref )

    @staticmethod
    def from_line(line):
        """Inflate from a space-delimited line such as what Git feeds us on
        STDIN or what to_line() returns.
        """
        words = line.strip().split()
        return PreReceiveTuple(words[0], words[1], words[2])

    def to_line(self):
        """Format a tuple into something that from_line() can read."""
        return '{} {} {}'.format(self.old_sha1, self.new_sha1, self.ref)

    def git_branch_name(self):
        """Return the "mybranch" portion of "refs/heads/mybranch"."""
        prefix = 'refs/heads/'
        if self.ref.startswith(prefix):
            return p4gf_branch.BranchRef(self.ref[len(prefix):])
        else:
            return None

    def to_dict(self):
        """For easier JSON formatting."""
        return { 'old_sha1' : self.old_sha1
               , 'new_sha1' : self.new_sha1
               , 'ref'      : self.ref
               }

    @staticmethod
    def from_dict(d):
        """For easier JSON parsing."""
        return PreReceiveTuple( old_sha1 = d['old_sha1']
                              , new_sha1 = d['new_sha1']
                              , ref      = d['ref'     ]
                              )




# p4gf_profiler timer names
TIMER_OVERALL                   = NTR('p4gf_branch_id total')
TIMER_RUN_REV_LIST              = NTR('run_rev_list')
TIMER_CONSUME_REV_LIST          = NTR('consume_rev_list')
TIMER_ASSIGN_CHILDREN           = NTR('assign_children')
TIMER_BRANCH_HEAD               = NTR('branch_head')
TIMER_ASSIGN_PREVIOUS           = NTR('assign_previous')
TIMER_ASSIGN_BRANCH_ANON_HEADS  = NTR('assign_branch_anon_heads')
TIMER_ASSIGN_BRANCH_NAMED       = NTR('assign_branch_named')
TIMER_ASSIGN_BRANCH_ANON        = NTR('assign_branch_anon')
TIMER_FREE_MEMORY               = NTR('free_memory')


def _increment_bucket(key, coll):
    """Add 1 to a bucket, creating bucket if necessary."""
    if key in coll:
        coll[key] += 1
    else:
        coll[key] = 1


def _decrement_bucket(key, coll):
    """Subtract 1 from a bucket."""
    if key in coll and 0 < coll[key]:
        coll[key] -= 1


def _branch_to_old_head_sha1(branch):
    """Test-replacable hook for looking up a branch's pre-push sha1."""
    return branch.sha1_for_branch()


# -- test scaffolding ---------------------------------------------------------

class _TestRefHead:

    """A Git reference's symbolic name and sha1 value."""

    def __init__(self):
        self.name            = None
        self.head_sha1       = None

    def __str__(self):
        return "{} {}".format(self.head_sha1, self.name)

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def to_sha1_list(refhead_list):
        """Return the sha1 of each _TestRefHead in a list."""
        return [x.head_sha1 for x in refhead_list]


def _test_return_none(_branch):
    """Test replacement for _branch_to_old_head_sha1() to look more like
    a pre-push repo.
    """
    return None


def _test_dump_result_to_stdout(assigner):
    """Dump all assignments to stdout in a format that a test script would enjoy."""
    # print("Commit count: {}".format(len(assigner.rev_list)))
    fmt = NTR("{sha1:<7.7}\t{branch_id}\t{subject}")
    for rev in assigner.rev_list:
        p = p4gf_proc.popen(['git', 'log', '-1', '--pretty=format:%s', rev])
        subject = p['out'].splitlines()[0]
        branch_id = assigner.assign_dict[rev].branch_id_str()
        print(fmt.format(sha1=rev, branch_id=branch_id, subject=subject))


def _test_dump_all_commits(branch_dict, git_dir, pushed_ref_list):
    """Dump a 'git rev-list' of all commits in git_dir, annotated with the
    name of which branch of Perforce file hierarchy that this commit
    should go into.
    """
    os.environ['GIT_DIR'] = git_dir
    LOG.debug2('pushed_ref_list={}'.format(pushed_ref_list))
    # ### Need a better way to test various "what if I push X and not Y?"
    ref_to_sha1 = p4gf_util.git_ref_list_to_sha1(pushed_ref_list)
    LOG.debug2('ref_to_sha1={}'.format(ref_to_sha1))
    prl = [PreReceiveTuple( old_sha1=p4gf_const.NULL_COMMIT_SHA1
                          , new_sha1=ref_to_sha1[ref]
                          , ref     =ref)
           for ref in pushed_ref_list]
    assigner = Assigner(branch_dict, prl)
    assigner.assign()
    _test_dump_result_to_stdout(assigner)


def main():
    """Log information regarding branches in a repository."""
    desc = _("""Runs branch-id calculation on the branches defined in
repo-config-file within the given git directory
See debug log for results.""")
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument('config_file', metavar='<repo-config-file>')
    parser.add_argument('git_dir', metavar='<git-dir>')
    parser.add_argument('ref', metavar='<ref>', nargs='*')
    args = parser.parse_args()

    with p4gf_log.ExceptionLogger():
        p4gf_proc.init()
        p4gf_branch.init_case_handling()
        _refs = args.ref if args.ref else ['master']
        LOG.debug('config={} dir={}\nrefs={}'.format(args.config_file, args.git_dir, _refs))
        p4gf_branch.use_consecutive_branch_ids()
        _config = configparser.ConfigParser()
        _config.read(args.config_file)
        _branch_dict = p4gf_branch.dict_from_config(_config)
        global _branch_to_old_head_sha1  # pylint:disable=global-variable-undefined
        _branch_to_old_head_sha1 = _test_return_none
        _git_dir = args.git_dir
        _remote_refs = ['refs/heads/' + ref for ref in _refs]
        _test_dump_all_commits(_branch_dict, _git_dir, _remote_refs)


if __name__ == "__main__":
    main()

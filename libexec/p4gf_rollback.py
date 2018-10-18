#! /usr/bin/env python3.3
"""Force a Git Fusion server to forget about recent history.

Used by Perforce Support to repair a Git Fusion server after
something goes wrong.

    p4gf_rollback.py --repo <repo-name> --change-num NNNN

NOT A STANDALONE SCRIPT. Depends on other Git Fusion 14.3 modules. This script
must live in same bin/ directory as other files from current bin/ directory.

Requires P4Python, a P4PORT and P4TICKET from the environment that permit a
connection as a Perforce user with permission to delete files from //.git-
fusion/objects/... and set p4 keys. The easiest way to get all these things
is to run this from the Git Fusion server itself.

TODO: detect overlapping repos and warn on requested obliterate of a file
revision that appears in multiple repos
"""
import logging
import operator
import os
import pprint

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_branch
import p4gf_const
import p4gf_context
import p4gf_desc_info
from   p4gf_l10n             import _, NTR
import p4gf_log
from   p4gf_object_type      import ObjectType
import p4gf_object_type_util
import p4gf_p4key
import p4gf_path
import p4gf_proc
import p4gf_util

                        # Log level reminder from p4gf_util.apply_log_args():
                        # verbose   = INFO
                        # <nothing> = WARNING
                        # quiet     = ERROR
                        #
LOG = logging.getLogger("p4gf_rollback_stdout")
                        # Trailing "_stdout" required here to prevent
                        # p4gf_log.run_with_exception_logger() from dumping
                        # exception stack traces to stdout (aka
                        # getLogger("p4gf_rollback.py"), which is a child of
                        # getLogger("p4gf_rollback")).

                        # Depot path to a single Git commit object, stored in
                        # our gitmirror archive and associated with a single
                        # changelist in a single Git Fusion branch.
                        #
                        # See p4gf_object_type.commit_depot_path()
                        #
GITMIRROR_DEPOT_PATH = NTR("{objects_root}/repos/{repo}/commits/"
                           "{slashed}-{branch_id},{change_num}")

                        # What level to use for --preview/-n comments
                        # and p4 commands. WARNING appears unless --quiet/-q
                        # suppresses all but errors. That's perfect.
LOG_LEVEL_PREVIEW = logging.WARNING


class Rollback:
    """A class that knows how to forget.

    Mostly a place to hang our context and accumulators.
    """
    def __init__(self, *, ctx, change_num, is_preview, is_obliterate):
        self.ctx           = ctx
        self.change_num    = change_num
        self.is_preview    = is_preview
        self.is_obliterate = is_obliterate

                        # Accumulator lists: append each branch's hits,
                        # then delete all at once later.

                        # Git commit objects in our gitmirror that
                        # we need to delete.
        self.del_depot_path_list = []

                        # p4key lookup "index" that stores same Git
                        # commit/Perforce changelist/Git Fusion branch
                        # association as the gitmirror object above.
        self.del_p4key_list = []

                        # p4key "last copied to Git" changelist number,
                        # one per Git Fusion server (usually just 1).
        self.last_copied_p4key_list = []

                        # Did we successfully chdir into GIT_WORK_TREE?
                        # If not, we lack a Git repo. Attempt no
                        # Git operations.
        self.can_git = False

                        # Git branches and the last surviving commit sha1.
                        #
                        # Only filled in for branches with Git branch names,
                        # and not marked as deleted.
                        #
                        # Not filled in for any Git branch that is already
                        # at or before the cutoff point. This includes repos
                        # that have not yet pulled doomed changelists from
                        # Perforce.
                        #
        self.branch_to_surviving_ot = {}

                        # Perforce changelists to delete.
        self.obliterate_change_num_list = []

                        # Perforce depotFile#rev revisions to obliterate.
                        # Intentionally want one element per revision,
                        # so that we can accurately report rev count here.
                        # If you want to optimize this down to something
                        # faster to 'p4 obliterate', do so elsewhere.
        self.obliterate_depot_rev_set = set()

    def rollback(self):
        """Forget all changelists and commits after change_num.

        Remember change_num and earlier.
        """
        if self.is_preview:
            LOG.log(LOG_LEVEL_PREVIEW,
                "Preview mode. Use --execute/-y to remove history.")

        LOG.info("Roll back repo {repo} to changelist: {change_num}"
                 .format( repo       = self.ctx.config.repo_name
                        , change_num = self.change_num
                        ))

        if not self._complete_changelist():
            LOG.warning('Cannot roll back to incomplete change %s', self.change_num)
            return

        self.can_git = self._chdir_git_work_tree()

                        # Accumulate things to change.
        for branch in _ordered(self.ctx.branch_dict().values()):
            self._accumulate_branch(branch)

        self.last_copied_p4key_list = self._last_copied_p4key()
        self._log_results("last-change p4key(s)", self.last_copied_p4key_list)

                        # Sort results so that we get consistent
                        # and comparable previews between runs.
        self.del_depot_path_list    = sorted(self.del_depot_path_list)
        self.del_p4key_list         = sorted(self.del_p4key_list)
        self.last_copied_p4key_list = sorted(self.last_copied_p4key_list)

                        # Change them.
        self._delete_depot_paths()
        self._delete_p4keys()
        self._set_last_copied()
        self._reset_index_last()
        if self.can_git:
            self._move_git_refs()
        if self.is_obliterate:
            self._obliterate_depot_revs()
            self._delete_changelists()

    def _complete_changelist(self):
        """Verify that the selected changelist is a 'complete'."""
        result = self.ctx.p4run('describe', '-s', self.change_num)
        vardict = p4gf_util.first_dict_with_key(result, 'change')
        if not vardict or 'desc' not in vardict:
            LOG.warning("change %s does not exist", self.change_num)
            return False
        di = p4gf_desc_info.DescInfo.from_text(vardict['desc'])
        if not di or p4gf_const.P4GF_DESC_KEY_PUSH_STATE not in di:
            return True
        return di[p4gf_const.P4GF_DESC_KEY_PUSH_STATE] == 'complete'

    def _accumulate_branch(self, branch):
        """Fetch lists of offending gitmirror files and p4keys,
        append to our accumulator data members
        """
        LOG.info("Check branch: {}"
                 .format(p4gf_util.abbrev(branch.branch_id)))

                        # Git commit objects in //.git-fusion/objects/...
        del_change_num_list = self._change_num_list_after(branch)
        self._log_results("Perforce changelist(s)", del_change_num_list)
        if not del_change_num_list:
            return
        del_depot_path_list = self._gitmirror_depot_path_list(
                                  branch
                                , del_change_num_list)
        self._log_results(_("Perforce copies of commit object(s)"
                            " in {root}/...".format(root=p4gf_const.objects_root())),
                          del_depot_path_list)
        self.del_depot_path_list.extend(del_depot_path_list)

                        # p4key index for each above commits.
        del_p4key_list = self._gitmirror_p4key_list(
                                  branch
                                , del_change_num_list)
        self._log_results("Perforce p4key(s) of commit object(s)", del_p4key_list)
        self.del_p4key_list.extend(del_p4key_list)

                        # Where to move each Git branch ref
                        # after rollback?
        if self.can_git:
            self.branch_to_surviving_ot[branch] \
                = self._git_branch_to_ot(branch)
            gbn_list = [b.git_branch_name
                        for b, ot in self.branch_to_surviving_ot.items()
                        if ot]
            self._log_results("Git reference(s)", gbn_list)

        if self.is_obliterate:
                        # Which changelists to obliterate?
            obli_change_num_list = self._change_num_list_to_obliterate(branch)
            self._log_results("changelist(s) to delete", obli_change_num_list)
            self.obliterate_change_num_list.extend(obli_change_num_list)

                        # Which files from those changelists to obliterate?
            obli_depot_rev_list = self._depot_rev_list_to_obliterate(
                                          branch
                                        , obli_change_num_list)
            self._log_results( "depot file revisions(s) to obliterate"
                             , obli_depot_rev_list )
            self.obliterate_depot_rev_set.update(obli_depot_rev_list)

    def _chdir_git_work_tree(self):
        """Our Git commands, including pygit2, assume CWD is GIT_WORK_TREE.

        Return True if we changed directory into repo_dirs.GIT_WORK_TREE
        and pygit2.Repository() believes repo_dirs.GIT_DIR to be a valid
        Git repository.
        """
                        # chdir for Git operations that use CWD.
        try:
            os.chdir(self.ctx.repo_dirs.GIT_WORK_TREE)
        except OSError:
            LOG.exception("GIT_WORK_TREE directory {gwt} not found."
                          " Skip local Git rollback."
                          .format(gwt=self.ctx.repo_dirs.GIT_WORK_TREE))
            return False

                        # Test GIT_DIR for operations that go through
                        # pygit2.Repository(repo_dirs.GIT_DIR).
        try:
            self.ctx.repo
        except Exception:  # pylint: disable=broad-except
            LOG.exception("GIT_DIR directory {gd} not a Git repo."
                          " Skip local Git rollback."
                          .format(gd=self.ctx.repo_dirs.GIT_DIR))
            return False

        return True

    def _delete_depot_paths(self):
        """If we have any depot files to delete, do so in a single changelist."""
        self._log_results( "Delete commit object file(s)"
                         , self.del_depot_path_list
                         , use_preview_level = True
                         , enquote = True )
        if not self.del_depot_path_list:
            return

        description = _("Git Fusion '{repo}' rollback to @{change_num}") \
                      .format( repo       = self.ctx.config.repo_name
                             , change_num = self.change_num )
        try:
            with p4gf_util.NumberedChangelist(
                    gfctx=self.ctx
                  , description = description ) as nc:
                for dp in self.del_depot_path_list:
                    self.p4gfrun('sync', '-k', dp)
                for dp in self.del_depot_path_list:
                    self.p4gfrun('delete', '-k', dp)
                if not self.is_preview:
                    nc.submit()
                    LOG.info("Submitted changelist: {}".format(nc.change_num))
        except Exception:  # pylint: disable=broad-except
            LOG.exception("Could not delete commit object file(s)."
                          " Continuing...")

    def _delete_p4keys(self):
        """
        If we have any p4keys to delete, do so.
        """
        self._log_results( "Delete commit p4key(s)"
                         , self.del_p4key_list
                         , use_preview_level = True )
        for k in self.del_p4key_list:
            try:
                self.p4gfrun('key', '-d', k)
            except Exception:  # pylint: disable=broad-except
                LOG.exception("Could not delete commit p4key {}. Continuing..."
                              .format(k))

    def _set_last_copied(self):
        """Set the "last copied from Perforce to Git Fusion" changelist
        counter to the lesser of (current value, rollback changelist number)

        The number of branches is small enough that I'm not going to split this
        into an accumulate/set pattern.
        """
        self._log_results( "Set last-change-num p4key(s)"
                         , self.last_copied_p4key_list
                         , use_preview_level = True )
        for name in self.last_copied_p4key_list:
            try:
                self.p4gfrun('key', name, str(self.change_num))
            except Exception:  # pylint: disable=broad-except
                LOG.exception("Could not set last-change-num p4key {k} {v}."
                              " Continuing..."
                              .format(k=name, v=str(self.change_num)))

    def _reset_index_last(self):
        """Update the index-last keys to most recent changes on each branch."""
        for branch in self.ctx.branch_dict().values():
            commit = greatest_lesser_change_for_branch(self.ctx, branch, self.change_num)
            name = p4gf_const.P4GF_P4KEY_INDEX_LCN_ON_BRANCH.format(
                repo_name=self.ctx.config.repo_name, branch_id=branch.branch_id)
            if commit:
                value = "{cl},{sha1}".format(cl=commit.change_num, sha1=commit.sha1)
                if self.is_preview:
                    LOG.info("p4 key {name} {value}".format(name=name, value=value))
                else:
                    try:
                        self.p4gfrun('key', name, value)
                    except Exception:  # pylint: disable=broad-except
                        LOG.exception("Could not set index-last p4key {k} {v}."
                                      " Continuing...".format(k=name, v=value))
            else:
                # Make sure the key is set for this branch.
                if p4gf_p4key.is_set(self.ctx, name):
                    if self.is_preview:
                        LOG.info("p4 key -d {name}".format(name=name))
                    else:
                        try:
                            self.p4gfrun('key', '-d', name)
                        except Exception:  # pylint: disable=broad-except
                            LOG.exception("Could not delete index-last p4key {k}."
                                          " Continuing...".format(k=name))

    def _move_git_refs(self):
        """Move (git branch -f) to last surviving Git commit at/before the cutoff,
        or delete (git branch -D) if no surviving commits.
        """
        verbo_list = ["{gbn:<20} {sha1}".format( gbn=b.git_branch_name
                                               , sha1=ot.sha1 )
                      for b, ot in self.branch_to_surviving_ot.items()
                      if ot]
        self._log_results( "Move/delete Git branch reference(s)"
                         , verbo_list
                         , use_preview_level = True )

                        # Detach HEAD (ok if fail due to bare repo)
        self.gitrun(['git', 'checkout', 'HEAD^0'])

        for branch, ot in self.branch_to_surviving_ot.items():
            try:
                if not ot:
                    continue
                elif self._is_ot_to_delete_git_ref(ot):
                    self.gitrun(['git', 'branch', '-D', branch.git_branch_name])
                else:
                    self.gitrun([ 'git', 'branch', '-f', branch.git_branch_name
                                , ot.sha1 ])
            except Exception:  # pylint: disable=broad-except
                LOG.exception("Could not move or delete Git branch reference {}"
                              .format(branch.git_branch_name))

    def _obliterate_depot_revs(self):
        """Permanently destroy all records and content."""
        dfr_list = list(self.obliterate_depot_rev_set)
        dfr_list.sort()
        self._log_results( "Obliterate Perforce depot file revisions(s)"
                         , dfr_list
                         , use_preview_level = True
                         , enquote = True
                         )
        if not dfr_list:
            return
        cmd = ['obliterate']
        if not self.is_preview:
            cmd.append('-y')

                        # Yeah, this list is likely 10,000 elements long.
                        # would sure be nice if we could collapse multiple
                        # contiguous revisions down to a single line
                        # "depot_path#3,8". Not today! No gold-plating!
        cmd.extend(dfr_list)
        try:
            self.p4gfrun(*cmd)
        except Exception:  # pylint: disable=broad-except
            LOG.exception("Could not obliterate. Continuing...")

    def _delete_changelists(self):
        """
        Delete empty changelists.
        """
        cn_list = self.obliterate_change_num_list
        cn_list.sort()
        self._log_results( "Delete Perforce changelist(s)"
                         , cn_list
                         , use_preview_level = True )
        for cn in cn_list:
            try:
                self.p4gfrun('change', '-d', '-f', cn)
            except Exception:  # pylint: disable=broad-except
                LOG.exception("Could not delete changelist {}. Continuing..."
                              .format(cn))

    def p4gfrun(self, *cmd):
        """
        Preview or run a p4 command that changes things.

        Use this only for mutating calls that we must preview.
        """
        if self.is_preview:
            LOG.info("p4 {}".format(" ".join(cmd)))
            return
        LOG.info("p4 {}".format(" ".join(cmd)))
        self.ctx.p4gfrun(*cmd)

    def gitrun(self, cmd):
        """
        Preview or run a git command that changes things.

        Use this only for mutating calls that we must preview.
        """
        if self.is_preview:
            LOG.info(" ".join(cmd))
            return
        LOG.info(" ".join(cmd))
        result = p4gf_proc.popen_no_throw(cmd)
        LOG.debug("git returns: {0}".format(result))
        return result

    def _log_results( self, noun, result_list, *
                    , use_preview_level = False
                    , enquote = False
                    ):
        """
        Common reporting code for "found N thingies"
        """
        msg = NTR("{noun:<16} : {ct}").format( noun = noun
                                             , ct   = len(result_list) )
        if use_preview_level and self.is_preview:
            LOG.log(LOG_LEVEL_PREVIEW, "# {}".format(msg))
        else:
            LOG.info(msg)

        if result_list:
            if enquote:
                l = _enquote_list(result_list)
            else:
                l = result_list
            LOG.debug(pprint.pformat(l))

    def _change_num_list_after(self, branch):
        """Return a list of changelist number on a branch, after our change_num."""
        result = []
        with self.ctx.switched_to_branch(branch):
            r = self.ctx.p4run(
                      'changes'
                    , '-e'
                    , int(self.change_num)+1
                    , self.ctx.client_view_path()
                    )
            for d in r:
                if not isinstance(d, dict) or 'change' not in d:
                    continue
                result.append(d['change'])
        return result

    def _gitmirror_depot_path_list(self, branch, del_change_num_list):
        """Return a list of depot_paths, one for each undeleted gitmirror
        commit object on the given branch, in the given changelist number list.
        """
        result = []
        for del_change_num in del_change_num_list:
            depot_path = GITMIRROR_DEPOT_PATH.format(
                  objects_root  = p4gf_const.objects_root()
                , repo          = self.ctx.config.repo_name
                , slashed       = "..."
                , branch_id     = branch.branch_id
                , change_num    = del_change_num
                )
            r = self.ctx.p4run('files', '-e', depot_path)
            for rr in r:
                if isinstance(rr, dict) and 'depotFile' in rr:
                    result.append(rr['depotFile'])
        return result

    def _gitmirror_p4key_list(self, branch, del_change_num_list):
        """Return a list of p4key names, one for each undeleted gitmirror lookup
        key object on the given branch, in the given changelist number list.
        """
        result = []
        for del_change_num in del_change_num_list:
            p4key_name = p4gf_const.P4GF_P4KEY_INDEX_OT.format(
                  repo_name     = self.ctx.config.repo_name
                , branch_id     = branch.branch_id
                , change_num    = del_change_num
                )
            hit = p4gf_p4key.is_set(self.ctx, p4key_name)
            if hit:
                result.append(p4key_name)
        return result

    def _last_copied_p4key(self):
        """Find the "last copied to this Git Fusion server" p4key for each
        Git Fusion server in the system.

        Return a list of p4keys whose value exceeds the cutoff.
        """
        key_wild = p4gf_const.P4GF_P4KEY_LAST_COPIED_CHANGE.format(
                  repo_name = self.ctx.config.repo_name
                , server_id = "*")
        max_change_num = int(self.change_num)
        key_val = p4gf_p4key.get_all(self.ctx, key_wild).items()
        result = []
        for key, value in key_val:
            got_change_num = int(value)
            if max_change_num < got_change_num:
                result.append(key)
        return result

    def _git_branch_to_ot(self, branch):
        """Return the ObjectType of the most recent surviving
        commit for the given branch.

        Return None if branch is not visible to Git.

        Returns the current branch head if the branch is already
        pointing to some old commit/changelist from before the cutoff.

        WARNING: Does not detect or counteract any branch deletion/creation
        that occur after the cutoff. Branches deleted in soon-to-be-obliterated
        history remain deleted after rollback. Branches created in soon-to-be-
        obliterated history remain created, either empty or containing some
        random reused branch content.
        """
                        # Ignore branches with no Git counterpart.
                        # (branch.sha1_for_branch() checks .git_branch_name,
                        #  but not .deleted)
        if branch.deleted or not branch.git_branch_name:
            if branch.git_branch_name:
                LOG.info("Skip Git ref {gbn}."
                         " Branch {branch_id} marked as deleted."
                         .format( branch_id = branch.branch_id
                                , gbn       = branch.git_branch_name))
            return None

                        # Before the rollback, is the Git head already
                        # positioned before/at the cutoff? If so,
                        # change nothing.
        sha1 = branch.sha1_for_branch()
        if not sha1:
                        # No such reference in local Git repo? Expected.
                        # git-branch-name defined from Perforce or other
                        # another Git Fusion server of this same repo, but
                        # either unpopulated, or not yet pulled to this
                        # Git Fusion server's repo.
            LOG.info("Ignore Git ref {gbn}."
                     " Branch {branch_id} has no sha1 in local repo."
                     .format( branch_id = branch.branch_id
                            , gbn       = branch.git_branch_name))
            return None

                        # Lookup this sha1/branch and find its changelist
                        # number. If that's at/before cutoff, retain it.
        otl = ObjectType.commits_for_sha1( ctx       = self.ctx
                                         , sha1      = sha1
                                         , branch_id = branch.branch_id )
        if otl:
            ot = otl[0]
        if ot and int(ot.change_num) <= int(self.change_num):
            LOG.info("Ignore Git ref {gbn}."
                     " Branch {branch_id} has no sha1 in local repo."
                     .format( branch_id = branch.branch_id
                            , gbn       = branch.git_branch_name))
            return None

                        # If it's missing or after the cutoff, find the
                        # change at/before cutoff.
        with self.ctx.switched_to_branch(branch):
            r = self.ctx.p4run(
                  'changes'
                , '-m1'
                , self.ctx.client_view_path(change_num=self.change_num)
                )
            change_num = p4gf_util.first_value_for_key(r, key='change')
                        # No changelists at/before cutoff?
        if not change_num:
            LOG.info("Delete Git ref {gbn}."
                     " Branch {branch_id} has no changelists before"
                     " @{change_num}."
                     .format( branch_id  = branch.branch_id
                            , gbn        = branch.git_branch_name
                            , change_num = self.change_num ))
            return self._create_ot_to_delete_git_ref(branch)

                        # Find corresponding Git commit sha1.
        ot = ObjectType.change_num_to_commit( ctx        = self.ctx
                                            , change_num = change_num
                                            , branch_id  = branch.branch_id )
        if not ot:
            LOG.info("Delete Git ref {gbn}."
                     " Branch {branch_id} has changelist @{survive_cn} before"
                     " @{change_num}, but no corresponding Git commit."
                     .format( branch_id  = branch.branch_id
                            , gbn        = branch.git_branch_name
                            , change_num = self.change_num
                            , survive_cn = change_num ))
            return self._create_ot_to_delete_git_ref(branch)

                        # Surviving changelist exists as a commit in local
                        # Git repo.
        LOG.info("Move Git ref {gbn}."
                 " Branch {branch_id} has changelist @{survive_cn},"
                 " Git commit {sha1} at/before @{change_num}."
                 .format( branch_id  = branch.branch_id
                        , gbn        = branch.git_branch_name
                        , change_num = self.change_num
                        , survive_cn = change_num
                        , sha1       = ot.sha1 ))
        return ot

    def _create_ot_to_delete_git_ref(self, branch):
        """Return a fake ObjectType instance that tells us to delete this
        branch's Git reference later.

        Use _is_ot_to_delete_git_ref() to detect these fake instances.
        """
        return ObjectType.create_commit(
                  sha1       = p4gf_const.NULL_COMMIT_SHA1
                , repo_name  = self.ctx.config.repo_name
                , change_num = 0
                , branch_id  = branch.branch_id )

    @staticmethod
    def _is_ot_to_delete_git_ref(ot):
        """Is this a fake ObjectType instance created by _create_ot_to_delete_git_ref()?"""
        return ot and ot.sha1 == p4gf_const.NULL_COMMIT_SHA1

    @staticmethod
    def _is_git_changelist(changelist):
        """Is this 'p4 changes -L' dict one with an "Imported from Git"
        block, and thus a changelist that originated in Git?
        """
        di = p4gf_desc_info.DescInfo.from_text(changelist.get('desc'))
        if not di:
            LOG.info("Change @{cn} did not originate in Git."
                       " Do not delete/obliterate."
                     .format(cn=changelist['change']))
        return di is not None

    def _change_num_list_to_obliterate(self, branch):
        """Return a list of Perforce changelist numbers that must be deleted.

        This implies that their revisions must be obliterated.

        Returned list does NOT include any changelists that lack
        the "Imported from Git" tag: we only delete/obliterate history
        that originated in Git.
        """
                        # List all changelists. Fetch full description,
                        # so that we can strip out any changelists
                        # that did not originate in Git.
        with self.ctx.switched_to_branch(branch):
            r = self.ctx.p4run(  'changes'
                               , '-e'
                               , int(self.change_num) + 1
                               , '-l'
                               , self.ctx.client_view_path()
                               )
        change_num_list = [c['change']
                           for c in r
                           if self._is_git_changelist(c)]
        return change_num_list

    def _depot_rev_list_to_obliterate(self, branch, obli_change_num_list):
        """Return a list of "depotFile#rev" depot file revisions to obliterate
        before deleting their containing changelist.

        Omits paths that do not intersect branch. Omitting these paths protects
        against obliterating files that are not ours, but does  risk a failure
        to 'p4 change -df' if any of its files fail to intersect one of our
        branches. This can happen with overlapping repos.
        """
        result = []
        with self.ctx.switched_to_branch(branch):
            for change_num in obli_change_num_list:
                path = self.ctx.client_view_path("={}".format(change_num))
                r = self.ctx.p4run('files', path)
                dfr_list = ["{depotFile}#{rev}".format(
                                      depotFile = rr['depotFile']
                                    , rev       = rr['rev'])
                            for rr in r]
                result.extend(dfr_list)
        return result

# -- module-wide -------------------------------------------------------------

### BELONGS IN p4gf_branch.py
### but leaving it here for now so that we can send out p4gf_rollback.py
### without touching other 14.2 files.


def _ordered(branch_list):
    """
    Return a list of branches, in some reproducible order.
    Don't rely on this order, it's mostly to make debugging
    and displays easier to follow across multiple runs or dumps.
    """
                        # Be permissive in what we accept as input.
                        # Accept a dict, operate on its values.
    if isinstance(branch_list, dict):
        return _ordered(branch_list.values())

    masterish = []      # list of 1 element, or 0 if no masterish
    fp_list   = []
    lw_list   = []
    for b in branch_list:
        if b.more_equal:
                        # Should only be 1 masterish, but  don't let that
                        # invariant violation break a debugging dump.
            masterish.append(b)
        elif b.is_lightweight:
            lw_list.append(b)
        else:
            fp_list.append(b)

    fp_list = sorted(fp_list, key=operator.attrgetter("branch_id"))
    lw_list = sorted(lw_list, key=operator.attrgetter("branch_id"))
    return masterish + fp_list + lw_list

### BELONGS IN p4gf_branch.py
### but leaving it here for now so that we can send out p4gf_rollback.py
### without touching other 14.2 files.


def _enquote_list(l):
    """Return a new list with every space-infected item wrapped in quotes."""
    return [p4gf_path.enquote(e) for e in l]


def greatest_lesser_change_for_branch(ctx, branch, change_num):
    """Find the change for the branch that is no higher than change_num.

    :param ctx: Git Fusion context
    :param branch: branch for which to find highest change
    :param change_num: the high water mark of changes to find

    :return: ObjectType or None

    """
    results = None
    pattern = p4gf_const.P4GF_P4KEY_INDEX_OT.format(
        repo_name=ctx.config.repo_name, change_num='*', branch_id=branch.branch_id)
    LOG.debug("greatest_lesser_change_for_branch() pattern %s", pattern)
    d = p4gf_p4key.get_all(ctx.p4gf, pattern)
    if d:
        for key, value in d.items():
            mk = p4gf_object_type_util.KEY_BRANCH_REGEX.search(key)
            if not mk:
                LOG.debug("ignoring unexpected p4key: %s", key)
                continue
            branch_id = mk.group('branch_id')
            cl = int(mk.group('change_num'))
            if cl <= int(change_num):
                # Ensure we keep the highest change found so far for this branch.
                if results and int(results.change_num) > cl:
                    continue
                LOG.debug("greatest_lesser_change_for_branch() candidate %s, %s, %s",
                          branch_id, cl, value)
                results = ObjectType.create_commit(sha1=value, repo_name=ctx.config.repo_name,
                                                   change_num=cl, branch_id=branch_id)
    LOG.debug("greatest_lesser_change_for_branch() returning %s", results)
    return results


def parse_argv():
    """Convert command line into a usable dict."""
    usage = _("""p4gf_rollback.py [options] --change-num NNN --repo <repo-name>
options:
    --p4port/-p     Perforce server
    --p4user/-u     Perforce user
    --execute/-y    yes, do it (normally just previews/reports)
    --obliterate    delete history from Perforce
    --verbose/-v    write more to console
    --quiet/-q      write nothing but errors to console
""")
    parser = p4gf_util.create_arg_parser(
          help_file    = NTR('p4gf_rollback.help.txt')
        , usage        = usage
        , add_p4_args  = True
        , add_log_args = True
        , add_debug_arg= True
        )
    parser.add_argument('--change-num', metavar="NNN",         required=True)
    parser.add_argument('--repo',       metavar="<repo-name>", required=True)
    parser.add_argument('--execute', '-y', action=NTR("store_true"))
    parser.add_argument('--obliterate', action=NTR("store_true"))

    args = parser.parse_args()
    p4gf_util.apply_log_args(args, LOG)
    LOG.debug("args={}".format(args))
    args.change_num = int(args.change_num)
    return args


def main():
    """Do the thing."""
    args = parse_argv()
    ctx = p4gf_context.create_context(args.repo)
    ctx.create_config_if_missing(False)
    ctx.config.p4user = args.p4user
    ctx.config.p4port = args.p4port
    ctx.connect_cli(LOG)
    p4gf_proc.init()
    p4gf_branch.init_case_handling(ctx.p4gf)
    rollback = Rollback( ctx           = ctx
                       , change_num    = args.change_num
                       , is_preview    = not args.execute
                       , is_obliterate = args.obliterate )
    rollback.rollback()


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

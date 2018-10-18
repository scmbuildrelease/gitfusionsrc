#! /usr/bin/env python3.3
"""Translate Git commits to Perforce changes."""

import os
from   collections                  import defaultdict, namedtuple
from   contextlib                   import ExitStack
import copy
import pprint
import shutil
import logging
import re
import traceback
import sys

import P4

import p4gf_branch
import p4gf_const
from   p4gf_changelist_data_file    import ChangelistDataFile
import p4gf_depot_branch
from   p4gf_desc_info               import DescInfo
# no   p4gf_fast_push               avoid circular import
import p4gf_fastexport
import p4gf_fastexport_marks
from   p4gf_fastimport_mark         import Mark
import p4gf_g2p_job                 as     G2PJob
from   p4gf_g2p_matrix2             import G2PMatrix as G2PMatrix2
import p4gf_g2p_matrix_dump
from   p4gf_g2p_user                import G2PUser
import p4gf_mem_gc
import p4gf_git
import p4gf_gitmirror
import p4gf_p4key                   as     P4Key
from   p4gf_l10n                    import _, NTR
import p4gf_lfs_file_spec
from   p4gf_lfs_row                 import LFSLargeFileSource
import p4gf_log
from   p4gf_object_type             import ObjectType
from   p4gf_p4changelist            import P4Changelist
import p4gf_p4filetype
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_p4spec
import p4gf_path
from   p4gf_preflight_checker       import PreflightChecker, PreflightException
import p4gf_proc
from   p4gf_profiler                import Timer, with_timer
import p4gf_progress_reporter as ProgressReporter
import p4gf_usermap
import p4gf_util
import p4gf_version_3

LOG = logging.getLogger(__name__)

UNICODE_TYPES = ['unicode', 'xunicode', 'utf16', 'xutf16', 'utf8', 'xutf8']

# Yeah, there are some huge functions in here that might benefit from a refactor.
# Or not: sometimes it's easier to follow one giant, mostly linear, function,
# than to hop around from function to function. We'll see. Later.

# p4d treats many failures to open a file for {add, edit, delete, others}
# not as an E_FAILED error, but as an E_INFO "oh by the way I totally failed
# to do what you want.
#
MSGID_CANNOT_OPEN = [ p4gf_p4msgid.MsgDm_LockSuccess
                    , p4gf_p4msgid.MsgDm_LockAlready
                    , p4gf_p4msgid.MsgDm_LockAlreadyOther
                    , p4gf_p4msgid.MsgDm_LockNoPermission
                    , p4gf_p4msgid.MsgDm_LockBadUnicode
                    , p4gf_p4msgid.MsgDm_LockUtf16NotSupp
                    , p4gf_p4msgid.MsgDm_UnLockSuccess
                    , p4gf_p4msgid.MsgDm_UnLockAlready
                    , p4gf_p4msgid.MsgDm_UnLockAlreadyOther
                    , p4gf_p4msgid.MsgDm_OpenIsLocked
                    , p4gf_p4msgid.MsgDm_OpenXOpened
                    , p4gf_p4msgid.MsgDm_IntegXOpened
                    , p4gf_p4msgid.MsgDm_OpenWarnOpenStream
                    , p4gf_p4msgid.MsgDm_IntegMovedUnmapped
                    , p4gf_p4msgid.MsgDm_ExVIEW
                    , p4gf_p4msgid.MsgDm_ExVIEW2
                    , p4gf_p4msgid.MsgDm_ExPROTECT
                    , p4gf_p4msgid.MsgDm_ExPROTECT2
                    ]

# This subset of MSGID_CANNOT_OPEN identifies which errors are "current
# user lacks permission" errors. But the Git user doesn't know _which_
# user lacks permission. Tell them.
MSGID_EXPLAIN_P4USER   = [ p4gf_p4msgid.MsgDm_ExPROTECT
                         , p4gf_p4msgid.MsgDm_ExPROTECT2
                         ]
MSGID_EXPLAIN_P4CLIENT = [ p4gf_p4msgid.MsgDm_ExVIEW
                         , p4gf_p4msgid.MsgDm_ExVIEW2
                         ]

# timer names
OVERALL         = NTR('Git to P4 Overall')
FAST_EXPORT     = NTR('FastExport')
PREFLIGHT       = NTR('Preflight')
COPY            = NTR('Copy')
P4_SUBMIT       = NTR('p4 submit')
MIRROR          = NTR('Mirror Git Objects')
CHUNKIFY        = NTR('Chunkify')
COPY_CHUNKS     = NTR('CopyChunks')
COPY_GSREVIEW   = NTR('CopyGSReview')

N_BLOBS = NTR('Number of Blobs')

# Ravenbrook local change.  Match "Copied from Perforce" followed by any number
# of RFC822-mail-header-like lines containing information about the Perforce
# origin of a commit.  See <https://swarm.workshop.perforce.com/jobs/job000442>.
COPIED_TO_PERFORCE = re.compile(r"(\n[^\n]*" +
                                p4gf_const.P4GF_EXPORT_HEADER +
                                r"[^\n]*(?:\n[^\n]*\w+:[^\n]*)+\n?)")    

GIT_P4_LINE        = re.compile(r"(\n[^\n]*\[git-p4: [^\n]+\n?)")

# Please keep G2P as the first class listed in this file.

class G2P:

    """class to handle batching of p4 commands when copying git to p4."""

    # pylint:disable=too-many-attributes
    def __init__(self, ctx, assigner, gsreview_coll):
        self.ctx = ctx

            # Git to Perforce user lookup.
        usermap = p4gf_usermap.UserMap( ctx.p4gf
                                      , self.ctx.email_case_sensitivity )
        self.g2p_user = G2PUser( ctx     = ctx
                               , usermap = usermap )

            # list of strings [":<p4_change_num> <commit_sha1> <branch_id>"]
            #
            # Fake marks! NOT the marks from git-fast-export!
            #
            # Later code in GitMirror.add_commits() requires the changelist
            # number, NOT the git-fast-export mark number.
        self.marks = []

            # p4gf_fastexport_marks.Marks
        self.fast_export_marks = None

        self.depot_branch_info_index = ctx.depot_branch_info_index()
        self.__branch_id_to_head_changenum = {}
        self.assigner = assigner

            # submitted changelist number (as string) ==> sha1 of commit
        self.submitted_change_num_to_sha1 = {}
        self.submitted_revision_count     = 0

            # Is there some Git-specific data (usually a link to a parent Git
            # commit) that can only be detected by reading the Git commit from
            # our object store, not deduced from Perforce integ history? Reset
            # once per _copy_commit(), latches True if commit needs it.
        self._contains_p4_extra           = False

            # List of Sha1ChangeNum tuples accumulated after each
            # successful 'p4 submit'. Used for debugging _dump_on_failure().
        self._submit_history              = []

            # Git Swarm reviews if any. None, or sometimes empty, if not.
        self.gsreview_coll                = gsreview_coll
        self.swarm_client                 = None

            # Local file paths of ChangelistDataFile files written.
            # Will later pass to GitMirror to write.
        self.changelist_data_file_list     = []

        self._current_branch            = None
        self._curr_fe_commit            = None   # Current git-fast-export 'commit'.
        self._matrix                    = None

            # PreflightChecker instance created in preflight(), reused
            # later in _copy_commit_matrix() and preflight_shelve_gsreviews().
            # Lazy-created in property preflight_checker()
        self._preflight_checker         = None

            # LFS text pointers and where they appear in history/gwt.
            # Filled by PreflightChecker (pre-receive)
            # or from packet file (post-receive).
        self.lfs_row_list               = []

            # Index of above, by commit + gwt path
        self.lfs_row_index              = {}

    def __str__(self):
        """Return a string representation of this G2P object."""
        return 'G2P repo {}'.format(self.ctx.config.repo_name)

    def _dump_on_failure(self, errmsg, is_exception, branch_id=None):
        """Something has gone horribly wrong and we've ended up in
        _revert_and_raise()

        Dump what we know about the push. Maybe it'll help.
        """
        # pylint: disable=too-many-statements
        # pylint: disable=too-many-branches
        log = logging.getLogger('failures')
        if not log.isEnabledFor(logging.ERROR):
            return

        log.error(errmsg)
        with self.ctx.p4.at_exception_level(self.ctx.p4.RAISE_NONE):
            p4gf_log.create_failure_file('push-')

            version = p4gf_version_3.as_string_extended(p4=self.ctx.p4, include_checksum=True)
            log.error(version)

            if is_exception:
                stack_msg = ''.join(traceback.format_exc())
            else:
                stack_msg = ''.join(traceback.format_stack())
            log.error('stack:\n{}'.format(stack_msg))

            log.error('os.environ subset:')
            for k, v in os.environ.items():
                if (   k.startswith('P4')
                    or k in ['LANG', 'PATH', 'PWD', 'HOSTNAME']):
                    log.error('{:<20} : {}'.format(k, v if not k == 'P4PASSWD' else '********'))

            opened = self.ctx.p4.run(['opened'])
            log.error('p4 opened:\n{}'.format(pprint.pformat(opened)))

            have   = self.ctx.p4.run(['have'])
            log.error('p4 have:\n{}'.format(pprint.pformat(have)))

            if branch_id:
                bad_branch = self.ctx.branch_dict().get(branch_id)
                if bad_branch:
                    with self.ctx.switched_to_branch(bad_branch):
                        ch_m5 = self.ctx.p4.run('changes', '-m5', self.ctx.client_view_path())
                        log.error('p4 changes -m5:\n{}'.format(pprint.pformat(ch_m5)))

            client_spec = p4gf_p4spec.fetch_client(self.ctx.p4)
            log.error('p4 client -o:\n{}'.format(pprint.pformat(client_spec)))

            temp_branch = self.ctx.temp_branch(create_if_none=False)
            if temp_branch and temp_branch.written:
                branch_spec = self.ctx.p4.run(['branch', '-o', temp_branch.name])
                log.error('p4 branch -o {}:\n{}'
                          .format(temp_branch.name, pprint.pformat(branch_spec)))
            else:
                log.error('p4 branch not yet written.')

            log.error('git-fast-export commit:\n{}'
                      .format(pprint.pformat(self._curr_fe_commit)))

            log.error('pre-receive tuples:\n{}'
                      .format('\n'.join([str(prt)
                                         for prt in self.assigner.pre_receive_list])))

            cmd = ['git', 'log', '--graph', '--format=%H']
            sha1_list = [prt.new_sha1 for prt in self.assigner.pre_receive_list]
            p = p4gf_proc.popen_no_throw(cmd + sha1_list)
            log_lines = self.assigner.annotate_lines(p['out'].splitlines())
            log_lines = self._annotate_lines(log_lines)
            log.error(' '.join(cmd + sha1_list))
            log.error('\n' + '\n'.join(log_lines))

            if self._matrix:
                log.error('matrix:')
                try:
                    log.error('\n'.join(p4gf_g2p_matrix_dump.dump( self._matrix
                                                                 , wide = True )))
                except Exception as e:  # pylint: disable=broad-except
                    log.error("Matrix dump failed: {}".format(str(e)))

                log.error('matrix integ/branch history:')
                for e in self._matrix.integ_batch_history:
                    log.error(pprint.pformat(e))

            for branch in self.ctx.branch_dict().values():
                log.error('branch view :' + repr(branch))
                log.error('depot branch:' + repr(branch.depot_branch))

            cmd = ['ls', '-RalF', self.ctx.contentlocalroot]
            log.error(' '.join(cmd))
            d = p4gf_proc.popen_no_throw(cmd)
            log.error(d['out'])

            log.error('Recent p4run history count: {}'
                      .format(len(self.ctx.p4run_history)))
            for i, cmd in enumerate(self.ctx.p4run_history):
                log.error('{i:<2}: {cmd}'.format(i=i, cmd=cmd))
            log.error('Recent p4gfrun history count: {}'
                      .format(len(self.ctx.p4gfrun_history)))
            for i, cmd in enumerate(self.ctx.p4gfrun_history):
                log.error('{i:<2}: {cmd}'.format(i=i, cmd=cmd))

        p4gf_log.close_failure_file()

    def _annotate_lines(self, lines):
        """Any line that contains a sha1 gets "@nnn" appended if we hold that sha1
        in our _submit_history. Could have multiple @nnn appended if copied to
        multiple changelists.
        """

        # Inflate our history list into a dict for faster lookup. Pre-assemble
        # annotation strings, including possible multi-changelist sha1s. One
        # less thing to hassle with in the loop below.
        sha1_to_annotation = defaultdict(str)
        for sc in self._submit_history:
            sha1_to_annotation[p4gf_util.abbrev(sc.sha1)] \
                += ' @{}'.format(sc.change_num)

        if self._curr_fe_commit and 'sha1' in self._curr_fe_commit:
            curr_sha1 = p4gf_util.abbrev(self._curr_fe_commit['sha1'])
            sha1_to_annotation[p4gf_util.abbrev(curr_sha1)] += ' <== FAILED HERE'

        re_sha1 = re.compile('([0-9a-f]{7})')

        for l in lines:
            m = re_sha1.search(l)
            if not m:
                yield l
                continue

            sha1 = m.group(1)
            if sha1 not in sha1_to_annotation:
                yield l
                continue

            yield l + sha1_to_annotation[sha1]

    def _parent_branch(self):
        """Return a string suitable for use as a DescInfo "parent-branch:" value.

        If current commit lists a different branch for Git first-parent commit GPARN0,
        return "{depot-branch-id}@{change-num}".

        If not, return None. Don't include in DescInfo
        """
        if not self._matrix:
            return None
                        # If no GPARN0, or if GPARN0 is on same branch, then no
                        # reason to clutter DescInfo with "parent is the
                        # previous commit on this branch."
                        #
        first_parent_col = self._matrix.first_parent_column()
        if not first_parent_col:
            return None
        if first_parent_col.branch == self._current_branch:
            return None

        if first_parent_col.depot_branch:
            par_dbid = first_parent_col.depot_branch.depot_branch_id
        else:
            par_dbid = None
        return (NTR("{depot_branch_id}@{change_num}")
                .format( depot_branch_id = par_dbid
                       , change_num      = first_parent_col.change_num
                       ))

    def _change_description(self, commit):
        """Construct a changelist description from a git commit.

        Keyword arguments:
            commit  -- commit data from Git
        """
        return self.change_desc_info(commit).to_text()

    def change_desc_info_with_branch(self, commit, branch):
        """Calculate a DescInfo for commit on branch.

        Called by FastPush.
        """
        self._current_branch = branch
        return self.change_desc_info(commit)

    def change_desc_info(self, commit):
        """Construct a changelist description from a git commit.

        :param commit: git-fast-export commit
        """
        # pylint:disable=too-many-branches
        di = DescInfo()
        for key in ('author', 'committer'):
            datum = commit[key]
            di[key] = { 'fullname' : datum['user']
                      , 'email'    : datum['email']
                      , 'time'     : datum['date']
                      , 'timezone' : datum['timezone'] }
        di.clean_desc        = commit['data']
        # If this commit was originally cloned from Perforce,
        # Then remove the two consecutive lines which were added during the clone:
        #     Copied from Perforce
        #          Change: N
        #
        copy_from_perforce = re.search(COPIED_TO_PERFORCE, di.clean_desc)
        if copy_from_perforce:
            di.clean_desc = di.clean_desc.replace(copy_from_perforce.group(1),'')
        if self.ctx.git_p4_emulation:
            git_p4_line = re.search(GIT_P4_LINE, di.clean_desc)
            if git_p4_line:
                di.clean_desc = di.clean_desc.replace(git_p4_line.group(1), '')
        di.author_p4         = commit['author_p4user']
        di.pusher            = commit['pusher_p4user']
        di.sha1              = commit['sha1']
        di.push_state        = NTR('complete') if 'last_commit' in commit else NTR('incomplete')
        di.contains_p4_extra = self._contains_p4_extra
        # Always include all of the parent information.
        di.parents = self._parents_for_commit(commit)
        parent_changes = {}
        for par in di.parents:
            otl = self.commit_sha1_to_otl(par)
            sublist = [ot.change_num for ot in otl] if otl else []
            parent_changes[par] = sublist
        di.parent_changes = parent_changes
        if 'files' in commit:
            # Scan for possible submodule/gitlink entries
            di.gitlinks = [(f.get('sha1'), f.get('path')) for f in commit['files']
                           if f.get('mode') == '160000']

        if 'gitlinks' in commit:
            # Hackish solution to removal of submodules, which are indistinquishable
            # from the removal of any other element in Git.
            if not di.gitlinks:
                di.gitlinks = []
            di.gitlinks += commit['gitlinks']

        # Include any submodules that exist unchanged in this commit.
        more_gitlinks = []
        if di.gitlinks:
            seen_gitlink_gwt = {gwt_path for (sha1, gwt_path) in di.gitlinks}
        else:
            seen_gitlink_gwt = set()
        for (gwt_path, sha1) in p4gf_git.submodule_iter(self.ctx.repo, di.sha1):
            if gwt_path not in seen_gitlink_gwt:
                more_gitlinks.append((sha1, gwt_path))
        if more_gitlinks:
            if di.gitlinks is not None:
                di.gitlinks += more_gitlinks
            else:
                di.gitlinks = more_gitlinks

        if self._current_branch.depot_branch:
            di.depot_branch_id = self._current_branch.depot_branch.depot_branch_id

        di.parent_branch = self._parent_branch()

        return di

    def _ghost_change_description(self):
        """Return a string to use as the changelist description for a ghost changelist."""
                        # pylint:disable=line-too-long
        header = _("Git Fusion branch management")
        kv     = { p4gf_const.P4GF_DESC_KEY_GHOST_OF_SHA1       : self._matrix.ghost_column.sha1
                 , p4gf_const.P4GF_DESC_KEY_GHOST_OF_CHANGE_NUM : self._matrix.ghost_column.change_num
                 , p4gf_const.P4GF_DESC_KEY_GHOST_PRECEDES_SHA1 : self._curr_fe_commit['sha1']
                 , p4gf_const.P4GF_DESC_KEY_PUSH_STATE          : NTR('incomplete')
                 }
        parent_branch = self._parent_branch()
        if parent_branch:
            kv[p4gf_const.P4GF_DESC_KEY_PARENT_BRANCH] = parent_branch

        lines = [header, "", p4gf_const.P4GF_IMPORT_HEADER]
        for k in sorted(kv.keys()):
            v = kv[k]
            lines.append(" {}: {}".format(k, v))

        return '\n'.join(lines)

    def _revert_and_raise(self, errmsg, exception, branch_id=None):
        """Cleanup code invoked from exception handlers.

        An error occurred while attempting to submit the incoming change
        to Perforce. As a result, revert all modifications, log the error,
        and raise an exception.

        Do not call this to report an error. Raise an exception.
        """
        # We're already in error-cleanup code. Don't let another error
        # prevent that cleanup.
        with p4gf_util.ignored(Exception):
            # Set the status key whose name is bound to the push id so that
            # it is preserved between pushes and can be requested later.
            msg = _("Push {push_id} failed: {error}").format(
                push_id=self.ctx.push_id, error=errmsg)
            self.ctx.record_push_failed_p4key(errmsg)
            self._dump_on_failure(errmsg=errmsg, is_exception=exception, branch_id=branch_id)

        self._revert_without_raise()

        if not errmsg:
            if exception:
                errmsg = "".join(traceback.format_exception(
                    exception.__class__, value=exception, tb=exception.__traceback__))
            else:
                errmsg = "".join(traceback.format_stack())
        msg = _('import failed: {error}').format(error=errmsg)
        LOG.error(msg)
        if exception:
            raise RuntimeError(msg) from exception
        else:
            raise RuntimeError(msg)

    def _revert_without_raise(self):
        """Revert any pending changes and log a detailed error message."""
        #
        # Do NOT set the status key, the caller will do that.
        # Do NOT raise an exception, the caller will do that.
        #
        # If this p4key is set  - skip the usual cleanup.
        # This disabling is intended for use only by dev.
        value = P4Key.get(self.ctx, p4gf_const.P4GF_P4KEY_DISABLE_ERROR_CLEANUP)
        if not (value == 'True' or value == 'true'):
            try:
                # Undo any pending Perforce operations.
                opened = self.ctx.p4run('opened')
                if opened:
                    self.ctx.p4run(['revert', '-k', self.ctx.client_view_path()])
                self.ctx.p4run('sync', '-kq', '{}#none'.format(self.ctx.client_view_path()))

                # Undo any dirty files laying around in our P4 work area.
                if not self.ctx.contentlocalroot:
                    LOG.error('Bug: who called p4gf_copy_to_p4'
                              ' without ever setting ctx.contentlocalroot?')
                else:
                    shutil.rmtree(self.ctx.contentlocalroot)
                    p4gf_util.ensure_dir(self.ctx.contentlocalroot)
            except RuntimeError as e:
                # Failed to clean up, log that as well, but do not
                # lose the original error message in spite of this.
                LOG.error(str(e))
        else:
            LOG.debug("_revert_and_raise skipping Git Fusion cleanup as {0}={1}".format(
                p4gf_const.P4GF_P4KEY_DISABLE_ERROR_CLEANUP, value))

    def _p4_message_to_text(self, msg):
        """Convert a list of P4 messages to a single string.

        Annotate some errors with additional context such as P4USER.
        """
        txt = str(msg)
        if msg.msgid in MSGID_EXPLAIN_P4USER:
            txt += ' P4USER={}.'.format(self.ctx.p4.user)
        if msg.msgid in MSGID_EXPLAIN_P4CLIENT:
            txt += ' P4CLIENT={}.'.format(self.ctx.p4.client)
        return txt

    def _check_p4_messages(self):
        """If the results indicate a file is locked by another user,
        raise an exception so that the overall commit will fail. The
        changes made so far will be reverted.
        """
        msgs = p4gf_p4msg.find_msgid(self.ctx.p4, MSGID_CANNOT_OPEN)
        if not msgs:
            return

        lines = [self._p4_message_to_text(m) for m in msgs]
        raise RuntimeError('\n'.join(lines))

    def _bulldoze(self):
        """Bulldoze over any locked files (or those opened exclusively) by
        overriding the locks using our admin privileges. Notify the Git
        user if such an action is performed (with file and user information
        included). Returns True if any locks were overridden. The caller
        will need to perform the command again to effect any lasting change.
        """
        exclusive_files = []
        locked_files = []
        # other_users: depotFile => user (used in reporting)
        other_users = dict()
        # other_clients: client => [depotFile...] (used in unlocking)
        other_clients = dict()

        def capture_details(m):
            """Capture details on the locked file."""
            depot_file = m.dict['depotFile']
            if depot_file not in other_users:
                other_users[depot_file] = []
            other_users[depot_file].append(m.dict['user'])
            client = m.dict['client']
            if client not in other_clients:
                other_clients[client] = []
            other_clients[client].append(depot_file)

        # Scan messages for signs of locked files, capturing the details.
        for m in self.ctx.p4.messages:
            if m.msgid == p4gf_p4msgid.MsgDm_OpenXOpened:
                # Will be paired with "also opened by" message
                exclusive_files.append(m.dict['depotFile'])
            elif m.msgid == p4gf_p4msgid.MsgDm_OpenIsLocked:
                locked_files.append(m.dict['depotFile'])
                capture_details(m)
            elif m.msgid == p4gf_p4msgid.MsgDm_AlsoOpenedBy:
                capture_details(m)

        # Unlock any exclusively opened or locked files, and report results.
        if exclusive_files or locked_files:
            if locked_files:
                self.ctx.p4run('unlock', '-f', locked_files)
            if exclusive_files:
                for other_client, depot_files in other_clients.items():
                    client = self.ctx.p4.fetch_client(other_client)
                    host = client.get('Host')
                    user = p4gf_const.P4GF_USER
                    with p4gf_util.UserClientHost(self.ctx.p4, user, other_client, host):
                        # Override the locked option on the other client, if needed.
                        with p4gf_util.ClientUnlocker(self.ctx.p4, client):
                            # don't use self.ctx.p4run() for these, because we're
                            # monkeying with the other user's changelist, not ours
                            # and self.ctx.p4run() will helpfully insert -c changenum
                            # which will cause these commands to fail
                            self.ctx.p4.run('reopen', depot_files)
                            self.ctx.p4.run('revert', '-k', depot_files)

            for depot_file in exclusive_files + locked_files:
                users = ", ".join(other_users[depot_file])
                sys.stderr.write(_("warning: overrode lock on '{depot_file}' by '{users}'\n")
                                 .format(depot_file=depot_file, users=users))
            sys.stderr.write(_('warning: it is advisable to contact them in this regard\n'))
            return True
        return False

    def _revert(self):
        """If an attempt to add/edit/delete a file failed because that file is
        already open for X, then revert it so that we can try again.

        Return list of depot_file paths that we reverted, empty
        if nothing reverted.
        """
        msg_list = p4gf_p4msg.find_msgid(self.ctx.p4, p4gf_p4msgid.MsgDm_OpenBadAction)
        depot_file_list = [m.dict['depotFile'] for m in msg_list]
        if depot_file_list:
            LOG.debug2('_revert(): cannot open file(s) already open for delete'
                       ' from some other branch. Reverting.')
            self.ctx.p4run('revert', depot_file_list)
            # We just reverted a delete, probably a delete integrated from some
            # other branch. Might have just reverted our only link from that
            # branch. Can't trust integ for parent calcs in later p4-to-git.
            self._contains_p4_extra = True
        return depot_file_list

    def _p4run(self, cmd, bulldoze=False, revert=False):
        """Run one P4 command, logging cmd and results."""
        results = self.ctx.p4run(*cmd)
        if bulldoze and self._bulldoze():
            # Having overridden any locks, try again and fall through
            # to the message validator code below.
            results = self.ctx.p4run(*cmd)
        if revert and self._revert():
            results = self.ctx.p4run(*cmd)
        self._check_p4_messages()
        return results

    def _handle_unicode(self, results):
        """Scan the results of a P4 command, looking for files whose type was
        detected as being Unicode. This means they (may) have a byte order
        mark, and this needs to be preserved, which is accomplished by
        storing the file using type 'ctext'.
        """
        for result in results:
            if not isinstance(result, dict):
                continue

            base_mods = p4gf_p4filetype.to_base_mods(result['type'])
            LOG.debug('_handle_unicode() {} has {}'.format(result['depotFile'], base_mods))
            if base_mods[0] in UNICODE_TYPES:
                # Switch UTF16 files to ctext to avoid byte order changing.
                base_mods[0] = 'text'
                # May not have any of (D,F,S) and C ...
                # These other flags may occur if a p4d typemap should add such a flag
                if 'D' in base_mods:
                    base_mods.remove('D')
                if 'F' in base_mods:
                    base_mods.remove('F')
                if 'S' in base_mods:
                    base_mods.remove('S')
                if 'X' in base_mods:
                    base_mods[0] = 'binary'
                elif 'C' not in base_mods:
                    base_mods.append('C')
                filetype = p4gf_p4filetype.from_base_mods( base_mods[0]
                                                         , base_mods[1:])
                self._p4run(['reopen', '-t', filetype, result['depotFile']])

    def preflight_shelve_all_gsreviews(self, fe_commits):
        """Create placeholder shelf for each pending review."""
        if not self.gsreview_coll:
            return
        for fe_commit in fe_commits:
            self.preflight_shelve_gsreviews(fe_commit)

    def preflight_shelve_gsreviews(self, fe_commit):
        """Create a placeholder shelve for each pending review.

        2015-03-11 zig: Do NOT move this toPreflightChecker. This is not
        a preflight "check". It is a "change Perforce, but sneak the
        change in during preflight time.".
        """
        if not self.gsreview_coll:
            return
        self._curr_fe_commit = fe_commit
        gsreview_list = self.gsreview_coll.unhandled_review_list(fe_commit['sha1'])
        LOG.debug3('preflight_shelve_gsreviews() commit={} reviews={}'.format(
            p4gf_util.abbrev(fe_commit['sha1']), gsreview_list))
        for gsreview in gsreview_list:
            # Succeed or fail, never attempt to shelve this Git Swarm
            # review again during preflight of this push.
            gsreview.handled = True
            if gsreview.review_id:
                # An amendment to an existing review, nothing for preflight to do.
                continue
            dest_branch = self.ctx.git_branch_name_to_branch(gsreview.git_branch_name)
            with ExitStack() as stack:
                stack.enter_context(self.ctx.permanent_client(self.swarm_client))
                self._ensure_branch_preflight(fe_commit, dest_branch.branch_id)
                stack.enter_context(self.ctx.switched_to_swarm_client(self._current_branch))
                numbered_changelist = p4gf_util.NumberedChangelist(
                    ctx=self.ctx, description=fe_commit['data'])
                stack.enter_context(numbered_changelist)
                gwt_path = p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER
                local_path = self.ctx.gwt_path(gwt_path).to_local()
                with open(local_path, 'w') as f:
                    f.write('')
                self._p4run(['add', local_path])
                desc = self._change_description(fe_commit)
                self._p4_shelve_for_review(
                    desc=desc, owner=fe_commit['owner'], sha1=fe_commit['sha1'],
                    branch_id=dest_branch.branch_id, gsreview=gsreview, fecommit=fe_commit)
                gsreview.pending = True

    def _create_matrix(self, fe_commit):
        """Factory for G2PMatrix discover/decide/do.

        Allows us to switch to Matrix 2, depending on feature flag.
        """
        return G2PMatrix2(
                          ctx            = self.ctx
                        , current_branch = self._current_branch
                        , fe_commit      = fe_commit
                        , g2p            = self
                        )

    def _copy_commit_matrix_gsreview(self, commit, gsreview):
        """Copy a single Git commit to Perforce, shelve as a pending
        Perforce changelist, as a new or amended Swarm review.
        """

                        # Find destination branch. All we have is the
                        # Git branch name, as a portion of the pushed
                        # Git reference.
        dest_branch = self.ctx.git_branch_name_to_branch(gsreview.git_branch_name)

        LOG.debug('_copy_commit_matrix_gsreview() commit={sha1}'
                  ' gsreview={gsreview} dest_branch={dest_branch}'
                  .format( sha1        = commit['sha1']
                         , gsreview    = gsreview
                         , dest_branch = dest_branch ))

        with self.ctx.permanent_client(self.swarm_client):
            result = self._copy_commit_matrix(
                                     commit              = commit
                                   , branch_id           = dest_branch.branch_id
                                   , gsreview            = gsreview
                                   , finish_func         = self._p4_shelve_for_review )

                        # When writing p4gf_config2, don't write 'review/xxx' as
                        # this branch's ref. Do this NOW, before we write
                        # p4gf_config2, to avoid unnecessary updates to that
                        # file just to change a git-branch-name.
        if gsreview.needs_rename:
            review_branch = self.ctx.git_branch_name_to_branch(gsreview.old_ref_name())
            if review_branch:
                review_branch.git_branch_name = gsreview.new_ref_name()
                LOG.debug('_copy_commit_matrix_gsreview() new {}'.format(review_branch))

        return result

    def _copy_commit_matrix( self
                           , commit
                           , branch_id
                           , gsreview
                           , finish_func ):
        """Copy a single Git commit to Perforce, returning the Perforce
        changelist number of the newly submitted change. If the commit
        resulted in an empty change, nothing is submitted and None is
        returned.
        """
        if LOG.isEnabledFor(logging.INFO):
            sha1 = commit['sha1'][:7]
            desc = repr(commit['data'][:20]).splitlines()[0]
            # Odd spacing here to line up commit sha1 with "Submitted"
            # info message at bottom of this function.
            LOG.info('Copying   commit {}        {} {}'
                     .format(sha1, p4gf_util.abbrev(branch_id), desc))
        if LOG.isEnabledFor(logging.DEBUG) and 'merge' in commit:
            for parent_mark in commit['merge']:
                parent_sha1 = self.fast_export_marks.get_commit(parent_mark)[:7]
                LOG.debug("_copy_commit() merge mark={} sha1={}"
                          .format(parent_mark, parent_sha1))

        self._ensure_branch(commit, branch_id)

        # don't use a temp branch for reviews or it won't be temp any more!
        if gsreview:
            client_view = self.ctx.switched_to_swarm_client(self._current_branch)
        else:
            client_view = self.ctx.switched_to_branch(self._current_branch)
        with client_view:
            numbered_changelist = p4gf_util.NumberedChangelist( ctx = self.ctx
                       , description = commit['data']
                       , change_num  = gsreview.review_id if gsreview else None)
            with numbered_changelist:

                # Debugging a push with a known bad changelist number?
                #
                # if self.ctx.numbered_change.change_num == 50:
                #     logging.getLogger('p4')              .setLevel(logging.DEBUG3)
                #     logging.getLogger('p4gf_g2p_matrix2').setLevel(logging.DEBUG3)
                #     LOG                                  .setLevel(logging.DEBUG3)
                #     LOG.debug3('#################################################')

                try:
                    self._matrix = self._create_matrix(commit)
                    self._matrix.discover()

                    self._matrix.ghost_decide()
                    if self._matrix.ghost_do_it():
                        self._ghost_submit(numbered_changelist)

                        # Rare double-ghost-changelist: branch_delete
                        # was added and submitted above. Now delete and submit.
                        if self._matrix.convert_for_second_ghost_changelist():
                            if self._matrix.ghost_do_it():
                                self._ghost_submit(numbered_changelist)

                            # Create a brand new matrix, re-discover everything,
                            # building on the ghost changelist.
                            # +++ Ideally we could reuse much of the original
                            # +++ matrix. 'p4 integ' previews are expensive.
                        self._matrix = self._create_matrix(commit)
                        self._matrix.discover()

                    self._matrix.decide()
                    self._matrix.do_it()

                except P4.P4Exception as e:
                    self._revert_and_raise(str(e), exception=e, branch_id=branch_id)

                except Exception as e:  # pylint: disable=broad-except
                    self._revert_and_raise(str(e), exception=e, branch_id=branch_id)

                with Timer(P4_SUBMIT):
                    LOG.debug("Pusher is: {}, author is: {}".format(
                        commit['pusher_p4user'], commit['author_p4user']))
                    desc = self._change_description(commit)

                    try:
                        changenum = finish_func( desc      = desc
                                               , owner     = commit['owner']
                                               , sha1      = commit['sha1']
                                               , branch_id = branch_id
                                               , gsreview  = gsreview
                                               , fecommit  = commit )
                    except P4.P4Exception as e:
                        self._revert_and_raise(str(e), exception=e, branch_id=branch_id)

        ### 2014-06-03 zig: Probably unnecessary now that _p4_submit() does this,
        ### keep only if gsreview path needs it, and I doubt that it does,
        ### so I'm commenting out now and if nothing breaks, YAGNI.
        ### if changenum and self._current_branch:
        ###     self.__branch_id_to_head_changenum[self._current_branch.branch_id] = changenum
        return changenum

    @staticmethod
    def _pretty_print_submit_results(submit_result):
        """500-column-wide list-of-dict dumps are not so helpful."""
        r = []
        for sr in submit_result:
            if not isinstance(sr, dict):
                r.append(repr(sr))
                continue

            if (     ('depotFile' in sr)
                 and ('action'    in sr)
                 and ('rev'       in sr) ):
                r.append(NTR('{action:<10} {depotFile}#{rev}')
                         .format( action    = sr['action']
                                , depotFile = sr['depotFile']
                                , rev       = sr['rev']))
                continue

            r.append(' '.join(['{}={}'.format(k, v) for k, v in sr.items()]))
        return r

    def _p4_submit(self, desc, owner, sha1, branch_id, gsreview, fecommit):
        """This is the function called once for each git commit as it is
        submitted to Perforce. If you need to customize the submit or change
        the description, here is where you can do so safely without
        affecting the rest of Git Fusion.

        Since p4 submit does not allow submitting on behalf of another user
        we must first submit as git-fusion-user and then edit the resulting
        changelist to set the 'User' field to the actual author of the change.

        Implements CALL#3507045/job055710 "Allow for a user-controlled
        submit step."

        author_date can be either integer "seconds since the epoch" or a
        Perforce-formatted timestamp string YYYY/MM/DD hh:mm:ss. Probably needs to
        be in the server's timezone.

        branch_id used only for logging.
        """
        # pylint:disable=too-many-arguments, unused-argument
        # Unused argument 'gsreview' cannnot be _ prefixed because calling code passes by keyword.

        # Avoid fetch_change() and run_submit() since that exposes us to the
        # issue of filenames with double-quotes in them (see job015259).
        #
        # Retry the try/catch submit once and only once if and only if
        # a translation error occurs
        # p4 reopen -t binary on the problem files for the second attempt
        #
        # Translation error warning format:
        # 'Translation of file content failed near line 10 file /path/some/file'
        # Set the job number prior to calling submit
        self._add_jobs_to_curr_changelist(sha1=sha1, desc=desc)

        while True:
            try:
                r = self.ctx.numbered_change.submit()
                break
            except P4.P4Exception:
                if p4gf_p4msg.find_msgid(self.ctx.p4, p4gf_p4msgid.MsgServer_NoSubmit):
                    LOG.error('Ignored commit {} empty'.format(p4gf_util.abbrev(sha1)))
                    # Empty changelist is now worthy of a raised exception,
                    # no longer just a silent skip.

                # A p4 client submit may be rejected by our view lock during this copy_to_p4.
                # If so the submit_trigger will unlock its opened files before returning.
                # However, for the small interval between determining to reject the submit
                # and unlocking the files, we may get a lock failure here with our submit.
                # So retry, expecting the trigger to unlock the files.
                # Yes. Forever. With no logging.
                # Nota bene: Ctl-C by the git user will not interrupt this retry loop.
                if p4gf_p4msg.find_msgid(self.ctx.p4, p4gf_p4msgid.MsgDm_LockAlreadyOther):
                    continue
                raise

        changenum = self.ctx.numbered_change.change_num
        LOG.info('Submitted commit {sha1} @{changenum}  {branch_id}'
                 .format( sha1=p4gf_util.abbrev(sha1)
                        , changenum=changenum
                        , branch_id=p4gf_util.abbrev(branch_id)))

        # add count of revs submitted in this change to running total
        submitted_revision_count = 0
        for rr in r:
            if isinstance(rr, dict) and 'rev' in rr:
                submitted_revision_count += 1

        self.submitted_revision_count += submitted_revision_count
        self._submit_history.append(Sha1ChangeNum( sha1       = sha1
                                                 , change_num = changenum ))
        self.submitted_change_num_to_sha1[str(changenum)] = sha1
        self.__branch_id_to_head_changenum[branch_id] = changenum

        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('\n'.join(self._pretty_print_submit_results((r))))

        self._set_changelist_owner(change_num=changenum, owner=owner, desc=desc)
        return changenum

    def _p4_shelve_for_review( self, desc, owner, sha1, branch_id
                             , gsreview, fecommit):
        """Shelve the current numbered pending changelist for Swarm review.

        Revert all open files.

        Do not submit or delete this changelist.

        Create a review data file for this changelist for later
        GitMirror submit.
        """
        # pylint:disable=too-many-arguments, unused-argument
        # Unused argument 'branch_id', cannnot be _ prefixed because calling code passes by keyword.

                        # Succeed or fail, never attempt to shelve this
                        # Git Swarm review again during this push.
        gsreview.handled = True
        change_num = self.ctx.numbered_change.change_num

                        # New review? Assign now.
        is_new_review = (not gsreview.review_id)
        if is_new_review:
            gsreview.review_id    = change_num
            gsreview.needs_rename = True

                        # Tell Git client about the assigned ID. This is our
                        # only chance to get this ID back to the Git user.
            sys.stderr.write(_('\nPerforce: Swarm review assigned:'
                               ' review/{gbn}/{review_id}\n')
                             .format( gbn        = gsreview.git_branch_name
                                    , review_id  = gsreview.review_id ))

        review_status = NTR('new') if is_new_review else NTR('amend')
        if gsreview.pending:
            review_status = NTR('new')
            gsreview.pending = False
        review_suffix_d = { 'review-repo'   : self.ctx.config.repo_name
                          , 'review-id'     : gsreview.review_id
                          , 'review-status' : review_status
                          , 'review-git-branch-name'
                                            : self._current_branch.git_branch_name
                          }

        acp = self.g2p_user.get_acp(fecommit)
        LOG.debug('_p4_shelve_for_review() acp={}'.format(acp))
        if acp.get('author'):
            review_suffix_d['review-author'   ] = acp.get('author')
        if acp.get('committer'):
            review_suffix_d['review-committer'] = acp.get('committer')
        if acp.get('pusher'):
            review_suffix_d['review-pusher'   ] = acp.get('pusher')
        parent_list = self._parents_for_commit(fecommit)
        parent_commits = self.commit_sha1_to_otl(parent_list[0])
        if parent_commits:
            par_cl_num = parent_commits[0].change_num
            review_suffix_d['review-base-change-num'] = par_cl_num

        review_suffix = '\n' + '\n'.join([NTR(' {k}: {v}').format(k=k, v=v)
                                          for k, v in review_suffix_d.items()])

        self._add_jobs_to_curr_changelist(sha1=sha1, desc=desc)
        self.ctx.numbered_change.shelve(replace=not is_new_review)
        self.ctx.p4run('revert', '-k', '//...')

                        # set_changelist_owner() also updates changelist
                        # description, it's our last chance to tell Swarm
                        # that we've got a review.
        desc_r = desc + review_suffix
        self._set_changelist_owner(change_num, owner, desc_r)

                        # Create a data file describing this review
                        # and schedule for later GitMirror add.
        self._create_review_data_file(gsreview)

    def _create_review_data_file(self, _gsreview):
        """Create a new file that explains things about
        the current numbered pending changelist such as a list
        of ancestor Git commits/Perforce changelists,
        which files were deleted in which Git commit.

        GitMirror will eventually add and submit this file along
        with the rest of GitMirror commits and branch-info files:
          //.git-fusion/repos/{repo}/changelists/{change_num}
        """
                        # No need to check for whether or not we have anything
                        # to write. All Git Swarm reviews will have at least ONE
                        # second-parent commit that is the pushed review head,
                        # and not part of the destination.

                        # List of ancestor commits.
        ancestor_commit_otl = list(self._ancestor_commit_otl_iter())

        datafile = ChangelistDataFile(
                              ctx        = self.ctx
                            , change_num = self.ctx.numbered_change.change_num )
        datafile.ancestor_commit_otl = ancestor_commit_otl
        datafile.matrix2 = True
        datafile.write()

        self.changelist_data_file_list.append(datafile.local_path())

    def _ghost_submit(self, numbered_changelist):
        """Submit one or more file actions to create a Ghost changelist: branch
        files that our commit wants to delete, update contents to match what our
        commit wants to edit, and so on.

        Once submitted, swap in a NEW numbered pending changelist to
        house actions for the impending real non-ghost changelist.
        """
                        # Save original description before we
                        # replace it with ghost description.
        orig_changelist = self.ctx.p4.fetch_change(numbered_changelist.change_num)

        desc = self._ghost_change_description()
        self._p4_submit( desc      = desc
                       , owner     = p4gf_const.P4GF_USER
                       , sha1      = self._matrix.ghost_column.sha1
                       , branch_id = self._current_branch.branch_id
                       , gsreview  = None
                       , fecommit  = None
                       )

                        # Create a new numbered pending changelist,
                        # restoring original description.
        numbered_changelist.second_open(orig_changelist['Description'])

    def _ancestor_commit_otl_iter(self):
        """Iterator/generator to produce an ObjectType list of Git commits that
        contribute to the history being merged into the destination branch by
        the current commit.

        Returns Git commits that are ancestors ("are reachable by")
        the current Git fe_commit, but are not ancestors ("are not reachable
        by") the current Git commit's first-parent.

        MUST be a merge commit with exactly 2 parents (which is exactly what
        Swarm reviews create).
        """
        sha1 = self._curr_fe_commit['sha1']
        cmd = ['git', 'rev-list'
                        # Include all history contributing to what's merging in.
              , '{}^2'.format(sha1)
                        # Exclude all history already merged in.
              , '--not', '{}^1'.format(sha1)
              ]
        p = p4gf_proc.popen_no_throw(cmd)
        for par_sha1 in p['out'].splitlines():
            par_otl = self.commit_sha1_to_otl(par_sha1)
            for ot in par_otl:
                yield ot

    def _add_jobs_to_curr_changelist(self, sha1, desc):
        """Run 'p4 change -f' to attach any Jobs mentioned in the
        commit description to the current numbered pending changelist.
        """
        jobs = G2PJob.extract_jobs(desc)
        if not jobs:
            return
        jobs2 = G2PJob.lookup_jobs(self.ctx, jobs)

        changenum = self.ctx.numbered_change.change_num
        change = self.ctx.p4.fetch_change(changenum)
        LOG.debug("Fixing jobs: {}".format(' '.join(jobs)))
        change['Jobs'] = jobs2
        try:
            self.ctx.p4.save_change(change, '-f')
        except P4.P4Exception as e:
            # on error - p4 still saves the client without the invalid Job:
            # and since all we are updating is the job - nothing else to do
            LOG.debug("failed trying to jobs to change {}".format(' '.join(jobs)))
            err = e.errors[0] if isinstance(e.errors, list) and len(e.errors) > 0 else str(e)
            _print_error(_('Commit {commit_sha1} jobs ignored: {error}')
                         .format(commit_sha1=sha1, error=err))

    def _set_changelist_owner(self, change_num, owner, desc):
        """Run 'p4 change -f' to reassign changelist ownership to
        a Perforce user associated with the Git author or pusher,
        not git-fusion-user.
        """
        LOG.debug("Changing change owner to: {}".format(owner))
        change = self.ctx.p4.fetch_change(change_num)
        change['User'] = owner
        change['Description'] = desc
        self.ctx.p4.save_change(change, '-f')
        self._fix_ktext_digests(change['Change'])

    def _fix_ktext_digests(self, change):
        """Update digests for any ktext or kxtext revs in the change.

        This is necessary after setting the author of the change.

        """
        # Use -e to avoid scanning too many rows.
        r = self.ctx.p4run('fstat', '-F', 'headType=ktext|headType=kxtext',
                           '-e', change, '//...')
        ktfiles = ["{}#{}".format(f['depotFile'], f['headRev']) for f in r if 'headRev' in f]
        if ktfiles:
            self.ctx.p4run('verify', '-v', ktfiles)

    def _change_num_to_sha1(self, change_num, branch_id):
        """If change_num is a changelist previously submitted to Perforce on the
        given branch_id, return the sha1 of the commit that corresponds to that
        change.
        """
        # First check to see if change_num was a changelist that we submitted as
        # part of this git push, have not yet submitted its ObjectType mirror to
        # //P4GF_DEPOT/objects/...
        sha1 = self.submitted_change_num_to_sha1.get(str(change_num))
        if sha1:
            return sha1

        # Not one of ours. Have to go to the ObjectType store.
        commit = ObjectType.change_num_to_commit(self.ctx,
                                                 change_num,
                                                 branch_id)
        if commit:
            return commit.sha1
        return None

    def _parents_for_commit(self, commit):
        """For the given Git commit, find the SHA1 values for its parents.

        Return a list of sha1 strings in parent order.
        """
        if 'from' in commit:
            # Use the fast-export information to get the parents
            pl = [self.fast_export_marks.get_commit(commit['from'])]
            if 'merge' in commit:
                for parent in commit['merge']:
                    pl.append(self.fast_export_marks.get_commit(parent))
        else:
            # Make the call to git to get the information we don't have
            LOG.debug3('_parents_for_commit() sha1={}'
                       .format(p4gf_util.abbrev(commit['sha1'])))
            pl = p4gf_util.git_sha1_to_parents( commit['sha1']
                                              , repo=self.ctx.repo)

        return pl

    # Output type for _find_new_depot_branch_parent()
    NewDBIParent = namedtuple('NewDBIParent', [ 'parent_otl'
                                              , 'depot_branch_id_list'
                                              , 'change_num_list'])

    def _find_new_depot_branch_parent(self, commit):
        """Find the parent commits for a new child commit, map those to
        depot branches and changelists on those depot branches, return 'em.
        """
        log = LOG.getChild('find_new_depot_branch_parent')
        branch_dict = self.ctx.branch_dict()

        # Build up two parallel lists of parent depot branch IDs and the
        # Perforce changelist numbers that define the point in time
        # from which we branch off into this new depot branch.
        parent_depot_branch_id_list = []
        parent_changelist_list      = []
        parent_otl                  = []

        commit_sha1 = commit['sha1']
        if log.isEnabledFor(logging.DEBUG):
            desc = commit['data'][:10].replace('\n', '..')
            log.debug("child commit {:7.7}  '{}'".format(commit_sha1, desc))

        parent_list = self._parents_for_commit(commit)
        if log.isEnabledFor(logging.DEBUG):
            log.debug3("child commit {:7.7}  par sha1={}"
                       .format( commit_sha1
                              , ' '.join([s[:7] for s in parent_list])))

        for parent_sha1 in parent_list:
            parent_commits = self.commit_sha1_to_otl(parent_sha1)
            log.debug3("child commit {child_sha1:7.7}  "
                       "par sha1={par_sha1:7.7} otl={par_otl}"
                       .format( child_sha1 = commit_sha1
                              , par_sha1   = parent_sha1
                              , par_otl    = parent_commits ))

                        # Parent commit was already submitted to Perforce as
                        # one or more changelists. Use those.
            if parent_commits:
                parent_otl.extend(parent_commits)
                log.debug3("par_otl extended: {}".format(parent_otl))
                for parent_commit in parent_commits:
                    par_cl_num = parent_commit.change_num
                    p4cl = P4Changelist.create_using_change( self.ctx.p4gf
                                                           , par_cl_num)
                    desc_info = DescInfo.from_text(p4cl.description)
                    par_dbid = desc_info.depot_branch_id if desc_info else None
                    log.debug3(" + par {dbid}@{cl} ot={ot}"
                               .format( ot   = parent_commit
                                      , cl   = par_cl_num
                                      , dbid = p4gf_util.abbrev(par_dbid) ))
                    parent_depot_branch_id_list.append(par_dbid)
                    parent_changelist_list     .append(par_cl_num)
                continue    # to next parent_sha1 in parent_list

                        # Parent commit not yet submitted to Perforce, is part
                        # of this push and we've got a git-fast-export mark for
                        # it. Convert that to the one or more branch views and
                        # their corresponding depot branches.
                        #
            mark               = ':' + self.fast_export_marks.get_mark(parent_sha1)
            assign             = self.assigner.assign_dict.get(parent_sha1)
            par_branch_id_list = assign.branch_id_list() if assign else []
            log.debug3("child commit {child_sha1:7.7}  "
                       "par sha1={par_sha1:7.7} branch_id_list={bl}"
                       .format( child_sha1 = commit_sha1
                              , par_sha1   = parent_sha1
                              , bl         = par_branch_id_list ))
            for par_branch_id in par_branch_id_list:
                par_branch   = branch_dict.get(par_branch_id)
                par_dbid     = par_branch.depot_branch_id()

                log.debug3(" + par {dbid}@{mark} par_branch_id={par_branch_id:7.7}"
                           .format( par_branch_id = par_branch_id
                                  , mark          = mark
                                  , dbid          = p4gf_util.abbrev(par_dbid) ))
                parent_depot_branch_id_list.append(par_dbid)
                parent_changelist_list     .append(mark)

                        # Must fill in output ObjectType list, even if with
                        # temp/fake instances, so that calling code can find
                        # parent branch views from which to derive this depot
                        # branch's branch view.
                parent_otl.append( ObjectType.create_commit(
                                       sha1       = parent_sha1
                                     , repo_name  = self.ctx.config.repo_name
                                     , change_num = mark
                                     , branch_id  = par_branch_id ))
        # end for parent_sha1 in parent_list

        return self.NewDBIParent(
                             parent_otl           = parent_otl
                           , depot_branch_id_list = parent_depot_branch_id_list
                           , change_num_list      = parent_changelist_list )

    def finish_branch_definition(self, commit, branch):
        """Inflate a partially-created Branch with a proper depot branch to
        hold its files, and a branch view to map that depot branch to
        the Git work tree.
        """
        log = LOG.getChild('finish_branch_definition')
        parent_otl = []  # define_new_depot_branch() loads this from Perforce,
                         # define_branch_view() needs it. Omit needless reads.
        log.debug("no mapping (yet) for branch id={}".format(branch.branch_id[:7]))
        depot_branch = self.define_new_depot_branch(commit, parent_otl)
        branch.depot_branch   = depot_branch
        branch.is_dbi_partial = not depot_branch.parent_depot_branch_id_list
        log.debug2("defined new {}".format(depot_branch))
        self.define_branch_view(branch, depot_branch, parent_otl)
        # Prepend client name to view RHS so that
        # Context.switched_to_branch() can use the view.
        branch.set_rhs_client(self.ctx.config.p4client)

    def define_new_depot_branch(self, commit, out_parent_otl):
        """Return a new DepotBranchInfo object. Claims a not-yet-populated subtree
        of depot space, and knows which other depot branches are immediate
        parents.

        Records in our index of all known DepotBranchInfo.

        out_parent_otl : An output list that receives each parent commit
                         ObjectType instance. Pass in a list pointer. Here
                         solely as an optimization to avoid reloading the
                         same ObjectType list again in a few seconds when
                         we need a parent's branch_id/view to calculate a
                         new child branch's view.

                         Returned unchanged if no parents yet for this branch,
                         such as when pushing new history into empty Perforce.

                         ### Return this in a namedtuple, not an output
                         ### parameter.
        """
        log = LOG.getChild('define_new_depot_branch')
        ndpar = self._find_new_depot_branch_parent(commit)

        out_parent_otl.extend(ndpar.parent_otl)

        # Define depot branch with above tuples as parents.
        r = p4gf_depot_branch.new_definition(
                  repo_name = self.ctx.config.repo_name
                , p4        = self.ctx.p4 )
        r.parent_depot_branch_id_list = ndpar.depot_branch_id_list
        r.parent_changelist_list      = ndpar.change_num_list

        commit_sha1 = commit['sha1']
        log.debug('new {} (sha1 {})'.format(r, p4gf_util.abbrev(commit_sha1)))

                        # It is (usually) too early to record an FP basis. Our
                        # parents are likely part of this push and do not yet
                        # have changelist numbers (just ":mark" strings). And
                        # the branch view that uses us? Doesn't have a view yet
                        # (needs our root for that). So leave our basis blank
                        # and we'll fill it in later.
                        # r.fp_basis_change_num = None
                        # r.fp_basis_view       = None

        self.depot_branch_info_index.add(r)
        return r

    def _replace_depot_root(self, orig_p4map, new_depot_root):
        """Build a new branch view mapping, same as our parent's branch
        view mapping, but with the parent's // root replaced with
        new_depot_root.

        Returns a P4.Map() object with the new view mapping.
        """
        # For each original mapping line, if its LHS is one of our
        # depot branch roots, replace it. If not, leave it unchanged.

        # Append trailing slash so that "//rob" does not match "//robert".
        new_root = new_depot_root + '/'

        r = P4.Map()
        client_prefix = '//{}/'.format(self.ctx.config.p4client)
        for lhs, rhs in zip(orig_p4map.lhs(), orig_p4map.rhs()):
            l = p4gf_path.dequote(lhs)
            prefix = ""
            if l[0] in ["-", "+"]:
                prefix = l[0]
                l = l[1:]
            orig_dbi = self.depot_branch_info_index.find_depot_path(l)
            if orig_dbi:
                after_root = l[len(orig_dbi.root_depot_path) + 1:]
            else:       # Replace "//" fully populated root with new_depot_root.
                after_root = l[2:]
            new_lhs = p4gf_path.enquote(prefix + new_root + after_root)
            new_rhs = p4gf_path.dequote(rhs)[len(client_prefix):]
            r.insert(new_lhs, p4gf_path.enquote(new_rhs))
        return r

    def commit_sha1_to_otl(self, sha1):
        """Return a list of zero or more ObjectType instances that correspond to
        the given commit sha1, both from Perforce and from pending GitMirror
        data in our (fake) marks list.

        Yes, you need to check both: commits can be copied multiple times, in
        multiple pushes, to different branch views.

        Return empty list if no results.
        """
        otl = ObjectType.commits_for_sha1(self.ctx, sha1)
        # +++ O(n) scan of split strings
        #
        # Probably should index by sha1 if performance testing shows this to
        # matter, especially for really large pushes with hundreds of
        # thousands of commits.
        copied = False
        for line in self.marks:
            mark = Mark.from_line(line)
            if mark.sha1 != sha1:
                continue
            change_num = mark.mark
            ot = ObjectType.create_commit( sha1       = sha1
                                         , repo_name  = self.ctx.config.repo_name
                                         , change_num = change_num
                                         , branch_id  = mark.branch )
            if ot not in otl:
                if not copied:
                    # Do not modify the list that ObjectType.commits_for_sha1()
                    # gave us. Copy-on-write.
                    otl = copy.copy(otl)
                    copied = True
                otl.append(ot)

        return otl

    def _find_parent_for_new_branch(self, parent_otl):
        """Return one parent Branch instance that would be a suitable base
        from which to derive a new branch's view.

        parent_otl -- ObjectType list that define_new_depot_branch() outputs.

        Return None if no parents (orphan Git branch?).
        """
        for parent_ot in parent_otl:
            if parent_ot.branch_id:
                parent_branch = self.ctx.branch_dict().get(parent_ot.branch_id)
                if parent_branch and parent_branch.view_p4map:
                    return parent_branch
                else:
                    LOG.debug('_find_parent_for_new_branch() skipping:'
                              'view branch_id={} returned branch={}'
                              .format( parent_ot.branch_id[:7]
                                     , parent_branch))
            else:
                LOG.debug('_find_parent_for_new_branch() skipping:'
                          ' ObjectType missing branch_id: {}'
                          .format(parent_ot))
        return None

    def define_branch_view(self, branch, depot_branch, parent_otl):
        """Define a branch view that maps lightweight branch storage into the repo.

        Take a parent's branch view and replace its root with our own.

        If no parent, use the master-ish branch's view.
        """
        log = LOG.getChild('define_branch_view')
        parent_branch = self._find_parent_for_new_branch(parent_otl)
        log.debug('branch={} parent={}'.format(
            p4gf_branch.abbrev(branch), p4gf_branch.abbrev(parent_branch)))
        ObjectType.log_otl(parent_otl, log=log)

        if not parent_branch:
            # No parent? Probably pushing an orphan. Assume it may one day
            # merge into fully-populated Perforce, so base the new view on
            # any old branch view that maps fully-populated Perforce.
            parent_branch = self.ctx.most_equal()

            # Not even ONE branch in our p4gf_config[2]? Bug in config validator
            # or init_repo, should have rejected this push before you got here.
            if not parent_branch:
                raise RuntimeError(_('No Git Fusion branches defined for repo.'))

        branch.view_p4map = self._replace_depot_root(
                                              parent_branch.view_p4map
                                            , depot_branch.root_depot_path)
        branch.view_lines = branch.view_p4map.as_array()

        #rerooted = parent_branch.copy_rerooted(depot_branch)
        #rerooted.strip_rhs_client()
        #branch.view_p4map = rerooted.view_p4map
        #branch.view_lines = rerooted.view_lines

        log.debug('returning: {}'.format(branch.to_log(log)))

    def _ensure_branch_preflight(self, commit, branch_id):
        """If not already switched to and synced to the correct branch for the
        given commit, do so.
        """
        r = self.preflight_checker.ensure_branch_preflight(commit, branch_id)
        self._current_branch = r
        return r

    def submitted_to_branch_this_push(self, branch_id):
        """Have we submitted anything to this branch?"""
        changenum = self.__branch_id_to_head_changenum.get(branch_id)
        if changenum:
            return True
        else:
            return False

    def _is_placeholder_mapped(self):
        """Does this branch map our placeholder file?

        Returns non-False if mapped, None or empty string if not.
        """
        return self.ctx.gwt_path(
            p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER).to_depot()

    def _ensure_branch(self, commit, branch_id):
        """If not already switched to and synced to the correct branch for the
        given commit, do so.

        If this is a new lightweight branch, NOW it is save to create
        a depot_branch_info for this branch, since any parent commits
        now exist as marks.
        """
        # Preflight version does most of what we need.
        branch = self._ensure_branch_preflight(commit, branch_id)

        # If current branch has a partial DBI, now's the first time we can
        # fill it in.
        dbi = branch.depot_branch
        if branch.is_dbi_partial and dbi:
            with self.ctx.switched_to_branch(self._current_branch):
                ndpar = self._find_new_depot_branch_parent(commit)
                dbi.parent_depot_branch_id_list = ndpar.depot_branch_id_list
                dbi.parent_changelist_list      = ndpar.change_num_list
                branch.is_dbi_partial           = False

    def branch_to_p4head_sha1(self, branch):
        """Return the Git commit sha1 of the most recent changelist on branch_id.

        Requires that there are actual submitted changelists in branch_id's view,
        and that the most recent changelist maps to a Git commit already within our Git repo.

        Return None if branch is empty.
        Return 0 if branch holds changelists, but no corresponding Git commit.
        """
                        # Usually we're the source of the most recent changelist.
        change_num = self.__branch_id_to_head_changenum.get(branch.branch_id)

                        # If not, ask Perforce.
        if not change_num:
            with self.ctx.switched_to_branch(branch):
                r = self.ctx.p4run(
                          'changes'
                        , '-m1'
                        , '-s', 'submitted'
                        , '//{client}/...'.format(client=self.ctx.p4.client))
                rr = p4gf_util.first_dict_with_key(r, 'change')
                if not rr:
                    return None     # Branch is empty, no changelists.
                change_num = rr.get('change')

                        # Convert to associated Git commit's sha1.
        sha1 = self._change_num_to_sha1(change_num, branch.branch_id)
        if not sha1:
            return 0    # Changelist found, but lacks Git commit sha1.

        return sha1

    def already_copied_commit(self, commit_sha1, branch_id):
        """Do we already have a Perforce changelist for this commit,
        this branch_id?
        """
        ### Works for already-submitted, not yet for stuff we created as an
        ### earlier part of the current 'git push' but not yet submitted to
        ### the //P4GF_DEPOT/objects/... hierarchy.
        return bool(ObjectType.sha1_to_change_num(self.ctx,
                                                  commit_sha1,
                                                  branch_id))

    def _update_depot_branch_info(self, change_num, commit_sha1, branch_id):
        """In support of lightweight branching, find any depot branch info
        structures that are holding fast-export marks instead of Perforce
        changelist numbers, and update those entries based on the newly
        submitted change.
        """
        log = LOG.getChild('update_depot_branch_info')
        mark        = self.fast_export_marks.get_mark(commit_sha1)
        branch_dict = self.ctx.branch_dict()
        curr_branch = branch_dict.get(branch_id)
        curr_dbi    = curr_branch.depot_branch
        curr_dbid   = curr_dbi.depot_branch_id if curr_dbi else None
        log.debug2('mark :{mark} change_num=@{change_num} on'
                   ' branch={branch_id:7.7} dbid={curr_dbid} commit={commit_sha1:7.7} '
                   .format( mark        = mark
                          , change_num  = change_num
                          , branch_id   = branch_id
                          , curr_dbid   = p4gf_util.abbrev(curr_dbid)
                          , commit_sha1 = commit_sha1 ))
        if not mark:
            return

                        # Why O(n) scan all depot branches?
                        #
                        # Because we don't have a list of "child (depot)
                        # branches that use this (depot) branch as a parent".
                        # If we did, we could replace this O(n) scan with a
                        # more efficient scan through the few (usually 1) child
                        # (depot) branches.
                        #
        for branch in branch_dict.values():
            # reset the mark in case we modified it during our extensive search
            # keep in mind that 'mark' does not change while 'colon_mark' may
            colon_mark = ':' + mark
            depot_branch = branch.depot_branch
            if not depot_branch:
                # If not a newly created branch, nothing for us to do.
                log.debug3("not a newly created branch: {}".format(branch.branch_id))
                continue

            if colon_mark not in depot_branch.parent_changelist_list:
                # Try harder by considering export marks of other branches.
                found = False
                for marc in self.fast_export_marks.find_marks(commit_sha1):
                    colon_mark = ':' + marc
                    if colon_mark in depot_branch.parent_changelist_list:
                        found = True
                        break
                if not found:
                    log.debug3("dbid {dbid:7.7} {colon_mark} not in dbi.par_cll={par_cll}"
                               .format( dbid       = depot_branch.depot_branch_id
                                      , colon_mark = colon_mark
                                      , par_cll    = depot_branch.parent_changelist_list ))
                    continue

            log.debug2("dbid {dbid:7.7} {colon_mark}     in dbi.par_cll={par_cll}"
                       .format( dbid       = depot_branch.depot_branch_id
                              , colon_mark = colon_mark
                              , par_cll    = depot_branch.parent_changelist_list ))

            par_cnl = []
            for (par_cn, par_dbid) in zip( depot_branch.parent_changelist_list
                                         , depot_branch.parent_depot_branch_id_list ):
                if par_cn == colon_mark and par_dbid == curr_dbid:
                    par_cnl.append(change_num)
                    log.debug2("dbid {dbid:7.7} {colon_mark} ==> @{change_num}"
                               " par_dbid={par_dbid:7.7}"
                               .format( dbid       = str(depot_branch.depot_branch_id)
                                      , colon_mark = str(colon_mark)
                                      , change_num = str(change_num)
                                      , par_dbid   = str(par_dbid) ))
                else:
                    par_cnl.append(par_cn)
            depot_branch.parent_changelist_list = par_cnl
            # if this dbi was already mirrored with a mark while processing a previous branch
            # then we need to edit the file on P4
            # If we have not written it yet, the needs_p4add will be true
            if not depot_branch.needs_p4add:
                depot_branch.needs_p4edit = True

    def _copy_commits(self, commits):
        """Copy the given commits from Git to Perforce.

        Arguments:
            commits -- commits from FastExport class

        Returns:
            self.marks will be populated for use in object cache.
        """
        with Timer(COPY):
            last_copied_change_num = 0
            commits_completed = 0
            for commit in commits:
                ProgressReporter.increment(_('Copying changelists...'))
                commit_sha1 = commit['sha1']
                self._curr_fe_commit = commit

                ### Zig hasn't fully thought this through, but skipping
                ### fast-export-ed commits that the branch assigner chose
                ### not to assign to branches seems to bypass a problem
                ### when multiple overlapping branches attempt to re-push
                ### an old p4->git->p4 changelist.
                ###
                ### See also repeat of this in _preflight_check()
                if commit_sha1 not in self.assigner.assign_dict:
                    LOG.debug('_copy_commits() {} no branch_id. Skipping.'
                              .format(p4gf_util.abbrev(commit_sha1)))
                    continue

                for branch_id in self.assigner.assign_dict[commit_sha1] \
                        .branch_id_list():
                    if self.already_copied_commit(commit_sha1, branch_id):
                        LOG.debug('_copy_commits() {} {} '
                                  ' Commit already copied to Perforce. Skipping.'
                                  .format( p4gf_util.abbrev(commit_sha1)
                                         , p4gf_util.abbrev(branch_id)))
                        continue

                    change_num = self._copy_commit_matrix( commit
                                                         , branch_id
                                                         , gsreview            = None
                                                         , finish_func         = self._p4_submit )

                    if change_num is None:
                        LOG.warning("copied nothing for {} on {}"
                                 .format( commit_sha1
                                        , self._current_branch.branch_id ))
                        continue
                    self._update_depot_branch_info(change_num, commit_sha1, branch_id)
                    last_copied_change_num = change_num
                    commits_completed += 1

                    self.add_mark(':{} {} {}'
                                  .format(change_num, commit_sha1,
                                          self._current_branch.branch_id))
                    label = NTR('at g={} p={}').format(commit_sha1[:7], change_num)
                    p4gf_mem_gc.process_garbage(label)
                    p4gf_mem_gc.report_growth(label)
                    # end of for branch_id in assign_dict(sha1)

                    # Once this commit is fully copied and submitted to all
                    # Git Fusion branches, also copy and shelve as pending
                    # changelist for any Git Swarm reviews.
                with Timer(COPY_GSREVIEW):
                    self._copy_commit_gsreviews(commit)
                status_msg = _("Copied {commits_completed} of {total_commits} "
                               "commits for push {push_id}...").format(
                    commits_completed=commits_completed,
                    total_commits=len(commits),
                    push_id=self.ctx.push_id)
                self.ctx.record_push_status_p4key(status_msg, with_push_id = False)

        if last_copied_change_num:
            self.ctx.write_last_copied_change(last_copied_change_num)

    def _copy_commit_gsreviews(self, fe_commit):
        """If current commit is the head commit of one or more Git Swarm reviews,
        copy those to Swarm.
        """
        if not self.gsreview_coll:
            return
        gsreview_list = self.gsreview_coll.unhandled_review_list(
            fe_commit['sha1'])
        LOG.debug3('_copy_commit_gsreviews() commit={} reviews={}'
                   .format( p4gf_util.abbrev(fe_commit['sha1'])
                          , gsreview_list ))
        self._curr_fe_commit = fe_commit
        for gsreview in gsreview_list:
            self._copy_commit_matrix_gsreview(fe_commit, gsreview)

    def _push_start_p4key_name(self):
        """Return the name of a p4key where we record the last known changelist
        number before we start a 'git push'.
        """
        return p4gf_const.P4GF_P4KEY_PUSH_STARTED \
            .format(repo_name=self.ctx.config.repo_name)

    def _record_push_start_p4key(self):
        """Set a p4key with the last known good changelist number before
        this 'git push' started. Gives the Git Fusion administrator a
        place to start if rolling back.
        """
        last_change_num = 0
        r = self.ctx.p4run('changes', '-m1', '-s', 'submitted')
        if r:
            last_change_num = r[0]['change']

        p4key_name = self._push_start_p4key_name()
        p4key_value = NTR('{change} {p4user} {time}') \
                        .format( change = last_change_num
                               , p4user = self.ctx.authenticated_p4user
                               , time   = p4gf_util.gmtime_str_iso_8601())
        P4Key.set(self.ctx, p4key_name, p4key_value)

    def _clear_push_start_p4key(self):
        """Remove any p4key created by _record_push_start_p4key()."""
        P4Key.delete(self.ctx, self._push_start_p4key_name())

    def add_mark(self, mark):
        """add a mark to list."""
        self.marks.append(mark)

    def _ensure_swarm_client(self):
        """Ensure the git-swarm client has been created."""
        name = p4gf_const.P4GF_SWARM_CLIENT.format(repo_name=self.ctx.config.repo_name)
        newmap = p4gf_branch.replace_client_name(
            self.ctx.clientmap, self.ctx.config.p4client, name)
        view = newmap.as_array()
        spec = {
            'LineEnd': NTR('unix'),
            'Host': None,
            'Options': p4gf_const.CLIENT_OPTIONS,
            'Root': self.ctx.contentlocalroot,
            'View': view
        }
        p4gf_p4spec.ensure_spec(self.ctx.p4gf, 'client', name, values=spec)
        self.swarm_client = name

    def _submit_lfs_dedupe(self):
        """p4 add+submit all the the new LFS large file content.
        Do this before commit-by-commit translation so that we can use
        these files as 'p4 copy' sources.

        ### This whole function probably should move to gitmirror,
        and MUST be hoisted to run once per push, not it's current
        call per-PreReceiveTuple.
        """
        if not self.lfs_row_list:
            return

                        # Switch to a temp client that maps our local LFS
                        # upload cache to the depot's de-dupe storage.
        upload_cache_root = p4gf_lfs_file_spec.LFS_CACHE_PATH \
                            .format( repo_lfs   = self.ctx.repo_dirs.lfs
                                   , sha256     = "" )
        lhs = p4gf_lfs_file_spec.LFS_DEPOT_PATH \
                            .format( P4GF_DEPOT = p4gf_const.P4GF_DEPOT
                                   , repo       = self.ctx.config.repo_name
                                   , sha256     = "..." )
        rhs = "//{}/...".format(self.ctx.p4.client)
        view_lines = [ "{} {}".format(lhs, rhs) ]
        LOG.debug("_submit_lfs_dedupe()  view={}".format(view_lines))
        with self.ctx.switched_to_view_lines(view_lines):
                        # Point temp client root to upload cache.
            p4gf_p4spec.set_spec( self.ctx.p4
                                , "client"
                                , spec_id = self.ctx.p4.client
                                , values  = { "Root" : upload_cache_root }
                                )

            desc = p4gf_gitmirror.DESCRIPTION.format(repo=self.ctx.config.repo_name)
            with p4gf_util.NumberedChangelist( ctx         = self.ctx
                                             , description = desc ) as nc:

                        # Add all referenced large files. List will probably
                        # pick up some files that already exist in depot, but
                        # that's okay. We'll revert collisions after the
                        # failure.
                        # Sort just to make debugging a bit more reproducible.
                v = sorted({ row.to_lfsfs().depot_path(self.ctx)
                             for row in self.lfs_row_list
                             if row.large_file_source == LFSLargeFileSource.UPLOAD_CACHE })
                if not v:
                    return
                self.ctx.p4run('add', v)
                        # If nothing opened (already exist from previous de-dupe)
                        # then we're done here. No files to submit.
                df = p4gf_util.first_dict_with_key(
                        self.ctx.p4run('opened', '-m1'), "depotFile")
                if not df:
                    return
                nc.submit_with_retry()

    def _create_lfs_index(self):
        """Create a fast lookup index to find "is this GWT path at this
        commit an LFS text pointer?"
        """
        self.lfs_row_index = { _LFSKey( commit_sha1 = row.commit_sha1
                                      , gwt_path    = row.gwt_path) : row
                               for row in self.lfs_row_list }

    def find_lfs_row(self, commit_sha1, gwt_path):
        """Does this GWT path at this commit contain an LFS text pointer?"""
        return self.lfs_row_index.get(_LFSKey( commit_sha1 = commit_sha1
                                             , gwt_path    = gwt_path ))

    @property
    def preflight_checker(self):
        """Lazy instantiate our PreflightChecker.

        Because the post-receive phasee does not call preflight(), and
        therefore we can't depend on that call bringing the checker
        into existence.
        """
        if not self._preflight_checker:
            self._preflight_checker = PreflightChecker(
                          ctx                           = self.ctx
                        , g2p_user                      = self.g2p_user
                        , assigner                      = self.assigner
                        , gsreview_coll                 = self.gsreview_coll
                        , already_copied_commit_runner  = self
                        , finish_branch_definition      = self
                        )
        return self._preflight_checker

    def preflight(self, prt, commits, marks):
        """Run a preflight check on a set of commits from Git.

        :param prt: pre-receive tuple
        :param commits: FastExport.commits list

        :type marks: :class:`p4gf_fastexport_marks.Marks`
        :param marks: all known fast-export marks

        Raises a PreflightException if anything is out of sorts.

        """
        try:
            self.fast_export_marks = marks
            self.fast_export_marks.set_head(prt.ref)

            if self.gsreview_coll:
                self._ensure_swarm_client()

            with Timer(PREFLIGHT):
                self.preflight_checker.check_prt_and_commits(prt, commits, marks)
                self.lfs_row_list = self.preflight_checker.lfs_row_list
                self.preflight_shelve_all_gsreviews(commits)
        except PreflightException:
            # It's no big deal, just clean up and raise so the caller can
            # set the status and report to the user.
            self._revert_without_raise()
            raise
        except Exception as e:  # pylint: disable=broad-except
            # The sky is falling, make a big noise.
            self._revert_and_raise(str(e), e)
            raise

    def copy(self, prt, commits, marks):
        """Copy a set of commits from Git into Perforce.

        :param prt: pre-receive tuple
        :param commits: FastExport.commits list

        :type marks: :class:`p4gf_fastexport_marks.Marks`
        :param marks: all known fast-export marks

        """
        # pylint: disable=too-many-branches, too-many-statements
        self.ctx.checkpoint("copy_to_p4.copy")
        self.fast_export_marks = marks
        self.fast_export_marks.set_head(prt.ref)
        with Timer(OVERALL):
            try:
                self._record_push_start_p4key()
                if self.gsreview_coll:
                    self._ensure_swarm_client()
                if self.lfs_row_list:
                    self._submit_lfs_dedupe()
                    self._create_lfs_index()
                LOG.debug("copy() begin copying from {} to {} on {}".format(
                    prt.old_sha1, prt.new_sha1, prt.ref))
                for commit in commits:
                    self.g2p_user.get_author_pusher_owner(commit)
                self.marks = []
                try:
                    with ProgressReporter.Determinate(len(commits)):
                        LOG.info('Copying {} commits to Perforce...'.format(len(commits)))
                        self._copy_commits(commits)
                    p4gf_mem_gc.report_objects(NTR('after copying commits'))
                finally:
                    # we want to write mirror objects for any commits that made it through
                    # any exception will still be alive after this
                    if self.marks:
                        with Timer(MIRROR):
                            LOG.info('Copying Git and Git Fusion data to //{}/...'.format(
                                p4gf_const.P4GF_DEPOT))
                            self.ctx.mirror.add_changelist_data_file_list(
                                self.changelist_data_file_list)
                            if self.lfs_row_list:
                                for row in self.lfs_row_list:
                                    self.ctx.mirror.add_blob(row.text_pointer_sha1)
                            self.ctx.mirror.add_objects_to_p4(self.marks, None, None, self.ctx)

                            p4gf_mem_gc.process_garbage(NTR('after mirroring'))
                    else:
                        LOG.warning("no marks to commit for {}".format(prt))

            finally:
                temp_branch = self.ctx.temp_branch(create_if_none=False)
                if temp_branch:
                    temp_branch.delete(self.ctx.p4)

        self._clear_push_start_p4key()
        LOG.getChild("time").debug("\n" + str(self))
        LOG.info('Done. Changelists: {}  File Revisions: {}  Seconds: {}'
                 .format( len(self.submitted_change_num_to_sha1)
                        , self.submitted_revision_count
                        , int(Timer(OVERALL).time)))

# -- end of class G2P --------------------------------------------------------

# -- module-level functions --------------------------------------------------

# Key for _create_lfs_index() and lfs_row_index
_LFSKey = namedtuple("_LFSKey", ["commit_sha1", "gwt_path"])

@with_timer(FAST_EXPORT)
def run_fast_export(ctx, prt):
    """Perform the fast export from Git.

    :param ctx: Git Fusion context
    :param prt: pre-receive tuple to be exported

    :rtype: tuple(FastExport.commits, dict{mark: sha1})
    :return: tuple of commits and mark-to-sha1 mapping

    """
    branch = ctx.git_branch_name_to_branch(prt.ref)
    LOG.info(NTR('Running git-fast-export...'))
    ProgressReporter.increment(_('Running git fast-export...'))
    fe = p4gf_fastexport.FastExport(ctx, prt.old_sha1, prt.new_sha1)
    fe.force_export_last_new_commit = ((branch and (branch.view_lines is None)) or
                                       p4gf_util.sha1_exists(prt.new_sha1))
    if fe.force_export_last_new_commit:
        LOG.debug2('copy() force_export_last_new_commit=True')
    fe.run()
    LOG.debug2('copy() FastExport produced mark_ct={}'.format(len(fe.marks)))
    return (fe.commits, fe.marks)


def copy_git_changes_to_p4(ctx, prt, assigner, gsreview_coll):
    """Copy a set of commits from Git into Perforce.

    :param ctx: Git Fusion context
    :param prt: pre-receive tuple to be exported
    :param assigner: commit-to-branch assignments
    :param gsreview_coll: Git-Swarm review data

    Raises a PreflightException if anything is out of sorts.

    """
    g2p = G2P(ctx, assigner, gsreview_coll)
    commits, marks = run_fast_export(ctx, prt)
    all_marks = p4gf_fastexport_marks.Marks()
    all_marks.add(prt.ref, marks)
    g2p.preflight(prt, commits, all_marks)
    g2p.copy(prt, commits, all_marks)
    return None


def _print_error(msg):
    """Print the given message to the error stream, as well as to the log."""
    sys.stderr.write(msg + '\n')
    LOG.error(msg)


# G2P._submit_history value
Sha1ChangeNum = namedtuple('Sha1ChangeNum', ['sha1', 'change_num'])

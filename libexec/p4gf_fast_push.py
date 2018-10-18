#! /usr/bin/env python3.3
"""Quickly translate Git commits to Perforce changes
using P4D 15.1's new `p4 unzip` bulk importer.
"""
import binascii
import bisect
from   collections                  import defaultdict, deque, namedtuple
import copy
import datetime
from   functools                    import lru_cache
import hashlib
import json
import logging
import os
from   pickle                       import Pickler, Unpickler
from   pprint                       import pformat
import types

import p4gf_branch
import p4gf_branch_id
from   p4gf_case_conflict_checker   import CaseConflictChecker
import p4gf_config
import p4gf_const
from   p4gf_copy_to_p4              import G2P
import p4gf_create_p4
import p4gf_depot_branch
from   p4gf_desc_info               import DescInfo
import p4gf_eta
from   p4gf_fast_push_librarian     import LibrarianStore, lbr_rev_str
from   p4gf_fast_push_rev_history   import RevHistoryStore
from   p4gf_fastexport              import FastExport
from   p4gf_filemode                import FileModeStr, FileModeInt
from   p4gf_g2p_user                import G2PUser
from   p4gf_gfunzip                 import GFUnzip
import p4gf_git
import p4gf_gitmirror
from   p4gf_hex_str                 import md5_str
from   p4gf_l10n                    import _, NTR
from   p4gf_new_depot_branch        import NDBCollection
from   p4gf_object_type             import ObjectType
import p4gf_p4dbschema
import p4gf_p4filetype
import p4gf_p4key                   as     P4Key
from   p4gf_p4typemap               import P4TypeMap
import p4gf_path
from   p4gf_preflight_checker       import PreflightChecker, PreflightException
import p4gf_progress_reporter       as     ProgressReporter
import p4gf_pygit2
from   p4gf_receive_hook            import PreReceiveTupleLists
import p4gf_squee_value
from   p4gf_time_space_recorder     import TimeSpaceRecorder
import p4gf_time_zone
from   p4gf_usermap                 import UserMap
import p4gf_util
import p4gf_version_3

LOG = logging.getLogger('p4gf_fast_push')

MD5_NONE = "0"*32       # Value for deleted revisions

                        # For less typing in LOG format calls
_ab  = p4gf_util.abbrev
_fms = FileModeStr.from_int


class FastPush:
    """Holder of common knowledge used during conversion from Git to
    Perforce.

    Expected call sequence:

    from_pre_receive()  Factory returns a skeletal instance if
                        current push can be fast-pushed.

    pre_receive()       Run preflight check for push acceptability,
                        AND also create giant 'p4 unzip' archive that
                        we'll eventually send to Perforce,
                        AND store temp files and pickled version of
                        ourself to local filesystem.

    from_post_receive() Inflate from pickled version.

    post_receive()      Transmit the giant 'p4 unzip' archive to Perforce.
                        Use 'p4 unzip' response to learn correct changelist
                        numbers, apply to gitmirror data
                        Build gitmirror 'p4 unzip' archive,
                        transmit to Perforce.
                        Bulk-set ObjectType 'p4 keys'
                        Bulk-update changelist descriptions to replace
                        ":123" gfmarks with actual changelist numbers,
                        set "push-state: complete" for branch heads.
    """
    # pylint:disable=too-many-instance-attributes
    #
    # Yeah, this is pretty excessive. Especially since it's a two-phase class
    # where several members are only populated/used in pre-receive, several
    # others only populated/used in post-receive.

    def __init__( self, *
                , ctx
                , prl
                , ndb
                ):
        self.ctx  = ctx
        self._ndb = ndb
        self._prl = prl

                        # branch_id to PopulateCommit or None.
                        #
                        # PopulateCommit records the first commit on each
                        # branch during _assign_changelist_numbers() for use
                        # later when building changelists.
                        #
                        # None records that this is a root/orphan branch,
                        # no populate required.
        self._branch_id_to_popcom = {}
                        # Same values as _branch_id_to_popcom, but indexed by
                        # the gfmark of the changelist that the PopulateCommit
                        # precedes.
        self._gfmark_to_popcom    = {}

                        # branch/commit/changelist assignments
                        #
                        # Memory is expected to be on the order of what
                        # the branch Assigner consumes, so this should
                        # fit in memory as long as we drain the Assigner
                        # at the same rate that we grow this store.
        self._othistory = ObjectTypeHistory()

                        # A partial G2P instance that knows how to do
                        # Git/Perforce user lookup and other things
                        # that we'd rather reuse than copy-and-paste.
                        #
                        # Use sparingly.
        self._g2p       = None

                        # FastExport instance, set in _git_fast_export().
        self._gfe       = None

                        # git-fast-export marks file.
        self._gfe_mark_to_sha1 = {}

                        # The next changelist mark number to use.
                        # Incremented and returned (as string ":123")
                        # in _next_gfmark()
        self._next_change_num = 1

                        # Our BigStore of millions of librarian records,
                        # for blobs. Addressed by sha1. This is how we
                        # de-duplicate file revision blobs.
                        #
                        # Exists only during pre-receive.
        self._lbr_store     = None # LibrarianStore()

                        # Our BigStore of millions of ls-tree sha1s.
                        # We only need to know "yeah, I already added it",
                        # not a full librarian record. So this only needs
                        # a set() API.
                        #
                        # Exists only during pre-receive.
        self._tree_store    = None # TreeStore()

                        # Storage type for all our librarian files.
                        # S_GZ: Store as a *,d/ directory of individual
                        #       gzipped file revisions
        self._lbr_filetype_storage = p4gf_p4dbschema.FileType.S_GZ

                        # Our BigStore of revision history for every single
                        # depot file we create as a translated copy of a
                        # Git file.
                        # This is how we get the next revision number for
                        # each new revision, and also how we calculate
                        # #startRev,endRev ranges for integ sources.
                        #
                        # Exists only during pre-receive.
        self._rev_store     = None # RevHistoryStore()

                        # Our head commit/changelist for each branch.
                        # Just need commit sha1 and change_num, but since we
                        # have a full ObjectType handy, we'll use that.
        self._branch_id_to_curr_head_ot = {}

                        # Which date to use for each changelist?
        self._changelist_date_source = ctx.repo_config.get(
                          p4gf_config.SECTION_GIT_TO_PERFORCE
                        , p4gf_config.KEY_CHANGELIST_DATE_SOURCE
                        , fallback=p4gf_config.VALUE_DATE_SOURCE_P4_SUBMIT
                        ).lower()

                        # A persistent root that we delete only once we're
                        # done. Directory and its contents must survive
                        # pre- and post-receive hook time and into bgpush
                        # time, where we actually 'p4 unzip' these contents
                        # into the server.
        self._persistent_dir = _calc_persistent_dir(ctx.config.repo_name)
        p4gf_util.ensure_dir(self._persistent_dir)

                        # Our blobs-and-trees zip archive.
        self._bat_gfunzip               = None
        self._bat_gfunzip_abspath       = None

                        # Our translated changelists zip archive.
        self._commit_gfunzip              = None
                        # Count of currently open commit_gfunzip's written
                        # db.rev records. To avoid consumig too much Perforce
                        # server memory, Call _rotate_commit_gfunzip() when
                        # this gets too high.
        self._commit_gfunzip_rev_ct       = 0  # of current open gfunzip's revs
                        # gfmark of first db.change in current commit_gfunzip
        self._commit_gfunzip_begin_gfmark = ":1"
                        # List of closed gfunzip archives:
                        # dict{ abspath      : <path>
                        #     , begin_gfmark : ":123" }
        self._commit_gfunzip_list         = []

                        # Our GitMirror zip archive.
        self._gitmirror_gfunzip         = None
        self._gitmirror_gfunzip_abspath = None

                        # Our DescInfo zip archive.
                        # Created/filled/sent entirely in post-receive.
        self._desc_info_gfunzip         = None
        self._desc_info_gfunzip_abspath = None

                        # File that pre-receive gradually fills with
                        # DescInfo struct for each changelist we write, and
                        # which post-receive reads when updating gfmarks to
                        # their final submitted changelist numbers.
        self._desc_info_fp              = None
        self._desc_info_abspath         = None

                        # After 'p4 unzip'ing our commit_gfunzip, what integer
                        # offset did Perforce apply to our gfmark numbers to
                        # produce submitted changelist numbers?
        self._gfmark_offsets            = GFMarkOffsets()

                        # Where do all our branch heads go when the push
                        # completes successfully?
        self._branch_head_otl           = None

                        # PreflightChecker and G2PUser instances used
                        # during pre_receive() to both reject the unworthy
                        # and assign changelist owner.
                        #
                        # Left None during post_receive().
        self._preflight_checker         = None
        self._g2p_user                  = None
        self._case_conflict_checker     = None

                        # 'p4 typemap' filetype assigner P4TypeMap instance.
                        # Used during pre_receive, None during post_receive.
        self._p4typemap                 = None

                        # Instrumentation.
                        # How much time and space does each
                        # translated file revision consume?
        self._rev_timer = TimeSpaceRecorder(
                  report_period_event_ct = 1000
                , log = LOG.getChild("memory.rev")
                , fmt = "   {curr_event_ct:>15,} db.rev total"
                        "   {diff_time:5.2f} seconds"
                        "   {events_per_second:>7,} db.rev/second"
                        "   {events_per_mb:>10,} db.rev/mb"
                        "   {bytes_per_event:>10,} bytes/ct"
                        "   {memory_mb:7.2f} MB total"
                        "   {diff_kb:>15,} KB new"
                        "   {diff_aux_ct:>15,} copied_bytes"
                        "   {aux_per_second:>10,} copied_bytes/second"
                )

        self._eta       = None

                        # How many raw uncompressed bytes of blob data have
                        # we extracted from Git and written to the bat_zip
        self._byte_ct = 0

                        # How many db.revs can we write to a single 'p4 unzip'
                        # commit_gfunzip payload before it's time to rotate to
                        # a new one? 'p4 unzip' does all its work in Perforce
                        # server memory, including some big index structures.
                        # Seems to be about 30K per file revision, mostly due
                        # to our really long depot_paths.
        self._max_rev_per_commit_gfunzip = 10**3

                        # Debugging limit.
                        # How many commits to translate before terminating?
                        # Useful when testing a subset of a huge repo.
        self._max_translate_commit_ct    = 0 # 10000

        LOG.debug("__init__")

    @staticmethod
    def from_pre_receive( *
                        , ctx
                        , prl
                        , gsreview_coll
                        , ndb
                        ):
        """Factory.
        If this push will be a fast push, return a FastPush instance.
        If not, return None.
        """
        if FastPush.can_fast_push(ctx, gsreview_coll):
            return FastPush( ctx = ctx
                           , prl = prl
                           , ndb = ndb
                           )
        # else:
        #     LOG.error("No fast push for you!")
        return None

    @staticmethod
    def from_post_receive(ctx):
        """Factory.
        Inflate from the pickle (not JSON) file that pre_receive() wrote."""
        return FastPush._from_pickled(ctx)

    def pre_receive(self):
        """Perform all the work that we do while the Git client is still
        connected and able to disconnect or terminate us.
        Since we can be terminated, it is unsafe to send changelists
        to Perforce here. Do that in post_receive().

        Run 'git-fast-export', convert results to a zip archive
        containing everything Perforce needs to submit all the translated
        commits (just the translated commits and their files, not any
        gitmirror or other Git Fusion metadata).

        Raise a PreflightException or other error if we cannot translate
        something.

        Create local temp files to transfer knowledge to post_receive().
        """
        LOG.debug("pre_receive() enter")
        try:
            start_dt = datetime.datetime.now()
            self._delete_fast_push_files()
            p4gf_util.ensure_dir(self._persistent_dir)
            self._create_big_stores()
            assigner = self._assign_branches()
            self._create_g2p()
            self._finish_branch_instantiations()
            self._git_fast_export()
            self._assign_changelist_numbers(assigner)
            self._open_bat_gfunzip()
            self._open_commit_gfunzip()
            self._open_desc_info_file(for_write=True)
            self._create_preflight_checker()
            self._create_p4typemap()
            self._translate_commits()
            self._write_dbi_files()
            self._close_desc_info_file()
            self._close_commit_gfunzip()
            self._close_bat_gfunzip()
            self._write_pickle()
            self._log_pre_receive_summary(start_dt)
        except Exception: # pylint: disable=broad-except
            self._delete_fast_push_files()
            LOG.debug("pre_receive() exit with exception")
            raise
        LOG.debug("pre_receive() exit")

    def _log_pre_receive_summary(self, start_dt):
        """Write a one-line summary to log at INFO level about what we did
        and how long it took.

        Intended to match p4gf_copy_to_git.py's G2P.copy() summary line.
        """
        now_dt = datetime.datetime.now()
        duration_seconds = (now_dt - start_dt).total_seconds()
        LOG.info('Done. Changelists: {}  File Revisions: {}  Seconds: {}'
                 .format( len(self._othistory)
                        , self._rev_timer.event_ct
                        , int(duration_seconds)))

    def post_receive(self):
        """
        Perform work that we do while the Git client is no longer
        connected and thus unable to disconnect or terminate us.

        Send the `p4 unzip` archive to Perforce and wait for a
        receipt once Perforce finishes importing it.

        Send gitmirror data of Git commit and tree objects.

        Bulk-update changelist descriptions to replace ":123" gfmark
        placeholders with actual changelist numbers, and convert
        branch heads to "push-state: complete".

        Release atomic push and repo locks.
        """
        LOG.debug("post_receive() enter")
        try:
            self._send_bat_gfunzip()
            self._send_commit_gfunzips()
            self._open_gitmirror_gfunzip()
            self._fill_gitmirror_gfunzip()
            self._close_gitmirror_gfunzip()
            self._send_gitmirror_gfunzip()
            self._fill_branch_head_otl()
            self._submit_config_files()
            self._open_desc_info_gfunzip()
            self._fill_desc_info_gfunzip()
            self._close_desc_info_gfunzip()
            self._send_desc_info_gfunzip()
            self._set_p4keys()
            self._delete_fast_push_files()
        except Exception: # pylint: disable=broad-except
            LOG.exception("FastPush.post_receive() failed.")
            raise
        LOG.debug("post_receive() exit")

    def prl(self):
        """Return a p4gf_receive_hook.PreReceiveTupleLists instance that
        describes the original pre-receive tuples that Git sent to our stdin.
        """
        return self._prl

    def ndb_coll(self):
        """Return the p4gf_new_depot_branch.NDBCollection of newly defined
        branches, based on Git branch ref names and configuration settings.
        """
        return self._ndb

    def _write_pickle(self):
        """Write a pickle file to transmit ourself from pre-receive time
        to post-receive time.

        Modified from p4gf_pre_receive_hook.write_packet().
        """
        d = dict()
        config1 = self.ctx.repo_config.repo_config
        config2 = self.ctx.repo_config.repo_config2
        d['config'] = p4gf_config.to_dict(config1)
        if config2 is not None:
            d['config2'] = p4gf_config.to_dict(config2)
        d['prl'] = self._prl.to_dict()
        d['branch_dict'] = p4gf_branch.to_dict(self.ctx.branch_dict())
        if self._ndb:
            d['ndb'] = self._ndb.to_dict()
        d["othistory"] = self._othistory.to_dict_list()
        d["bat_gfunzip_abspath"] = self._bat_gfunzip_abspath
        d["commit_gfunzip_list"] = self._commit_gfunzip_list
        d["desc_info_abspath"]   = self._desc_info_abspath

        try:
            file_abspath = _pickle_file_abspath(self.ctx.config.repo_name)
            p4gf_util.ensure_parent_dir(file_abspath)
            with open(file_abspath, "wb") as f:
                pickler = Pickler(f)
                pickler.dump(d)
            LOG.info("Fast Push state written: {}".format(file_abspath))
        except TypeError as exc:
            LOG.error(_("Cannot serialize push data: {}")
                      .format(pformat(d)))
            raise RuntimeError(_("Cannot serialize push data.")) from exc

    @staticmethod
    def _from_pickled(ctx):
        """Create and fill in a new FastPush instance from a _write_pickle()
        file.

        Modified from p4gf_post_receive_hook.read_packet()
        """
        file_abspath = _pickle_file_abspath(ctx.config.repo_name)
        if not os.path.exists(file_abspath):
            return None
        with open(file_abspath, "rb") as f:
            unpickler = Unpickler(f)
            d = unpickler.load()
        LOG.info("Fast Push state read   : {}".format(file_abspath))
        #noisy!
        #LOG.debug3("Fast Push state:\n{}".format(pformat(d)))

        # read the pre-receive tuples
        prl = PreReceiveTupleLists.from_dict(d.pop('prl'))
        # read the branch dictionary (likely modified by assigner in preflight)
        branch_dict = p4gf_branch.from_dict(d.pop('branch_dict'), ctx.config.p4client)
        ctx.reset_branch_dict(branch_dict)
        # read the configuration data
        config = p4gf_config.from_dict(d.pop('config'))
        config2 = None
        if 'config2' in d:
            config2 = p4gf_config.from_dict(d.pop('config2'))
        ctx.repo_config.set_repo_config(config, None)
        ctx.repo_config.set_repo_config2(config2, None)
        ndb = None
        if 'ndb' in d:
            ndb = NDBCollection.from_dict(ctx, d.pop('ndb'))
        fp = FastPush( ctx = ctx
                     , prl = prl
                     , ndb = ndb
                     )
                        #pylint:disable=protected-access
        fp._othistory = ObjectTypeHistory.from_dict_list(d["othistory"])
        fp._bat_gfunzip_abspath = d["bat_gfunzip_abspath"]
        fp._commit_gfunzip_list = d["commit_gfunzip_list"]
        fp._desc_info_abspath   = d["desc_info_abspath"]
        return fp

    def _delete_fast_push_files(self):
        """Attempt to delete the views/{repo}/fast_push/ directory.

        Okay if this fails: we'll be leaving behind a potentially large
        directory, but that's just wasteful, not fatal.
        """
        try:
                        # Be very careful with 'rm -rf'. If path doesn't look
                        # right, die rather than delete.
            dir_abs_path = os.path.abspath(self._persistent_dir)
            if not os.path.exists(dir_abs_path):
                LOG.debug2("_delete_fast_push_files() nothing to delete {}"
                           .format(dir_abs_path))
                return
            LOG.debug("_delete_fast_push_files() {}".format(dir_abs_path))
            if (   "/views/" not in dir_abs_path
                or not dir_abs_path.endswith("/fast_push") ):
                LOG.error("Fast Push directory {} does not look"
                          " like something that Git Fusion can"
                          " safely delete."
                          .format(dir_abs_path))
                return
                        # Immediately remove it from our way
            LOG.debug("Fast Push cleanup: delete directory {}"
                      .format(self._persistent_dir))
            p4gf_util.rm_dir_contents(self._persistent_dir)
        except Exception:  # pylint: disable=broad-except
            LOG.exception("Fast Push directory %s could not be deleted.", self._persistent_dir)

    @staticmethod
    def can_fast_push(ctx, gsreview_coll):
        """This code is optimized for a very specific, yet common, case.
        Return True if this is such a case.
        """
        if gsreview_coll:
            LOG.debug2("can_fast_push() 0 gsreview_coll")
        elif not _is_configured(ctx):
            LOG.debug2("can_fast_push() 0 not _is_configured()")
        elif not ctx.server_is_case_sensitive:
            LOG.debug2("can_fast_push() 0 server not case sensitive")
        elif not _server_supports_unzip(ctx):
            LOG.debug2("can_fast_push() 0 not _server_supports_unzip()")
        elif not _is_repo_view_empty(ctx):
            LOG.debug2("can_fast_push() 0 not _is_repo_view_empty()")
        elif ctx.is_lfs_enabled:
            LOG.debug2("can_fast_push() 0 ctx.is_lfs_enabled")
        elif ctx.find_copy_rename_enabled:
            LOG.debug2("can_fast_push() 0 ctx.find_copy_rename_enabled")
        else:
            LOG.debug("can_fast_push() 1")
            return True
        return False

    def _create_big_stores(self):
        """Create our disk-based storage for pre-receive calculations."""
        default = p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MULTIPLE_TABLES
        store_how = self.ctx.repo_config.get(
                          p4gf_config.SECTION_GIT_TO_PERFORCE
                        , p4gf_config.KEY_FAST_PUSH_WORKING_STORAGE
                        , fallback = default)

        LOG.debug("store_how = {}".format(store_how))
        expected = [ p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_DICT
                   , p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MEMORY
                   , p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_SINGLE_TABLE
                   , p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MULTIPLE_TABLES
                   ]
        if store_how not in expected:
            LOG.warning("{key} {value} not in {expected}, using {default}"
                    .format(
                          key      = p4gf_config.KEY_FAST_PUSH_WORKING_STORAGE
                        , value    = store_how
                        , expected = expected
                        , default  = default))
            store_how = default

                        # even for the linux kernel, these fit in memory.
                        #  200MB LibrarianStore.db
                        #   91MB TreeStore.db
                        #
        store_how_smaller = p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_DICT

        d = self._persistent_dir # for less typing
        self._lbr_store  = LibrarianStore(
                                os.path.join(d, "LibrarianStore.db")
                              , store_how_smaller)
        self._tree_store = TreeStore(
                                os.path.join(d, "TreeStore.db")
                              , store_how_smaller)
        self._rev_store  = RevHistoryStore(
                                self.ctx
                              , os.path.join(d, "RevHistoryStore.db")
                              , store_how)

    def _assign_branches(self):
        """Create our branch Assigner and let it do its thing."""
        assigner = p4gf_branch_id.Assigner(
              branch_dict      = self.ctx.branch_dict()
            , pre_receive_list = self._prl.set_heads
                        # Context not required if we're not extending an
                        # existing repo's set of branches. But also used
                        # for uuid-sequential, so supply one anyways to make
                        # testing easier.
            , ctx=self.ctx
            )
                        # First push has no previous branch sha1
                        # to connect up to. Don't bother looking
                        # for those previous branch locations.
        assigner.connect_to_previous_branch_sha1 = False

                        # Do not waste time flattening Assigner internal
                        # elements to save memory. We're going to destroy
                        # those elements almost immediately in
                        # _assign_changelist_numbers().
        assigner.flatten_memory = False

                        # Do not waste time scanning for existing branch
                        # assignments: there won't be any since this is the
                        # first push of new data, and all those 'p4 files -e
                        # <ObjectType commit path> can be expensive for huge
                        # repos.
        assigner.assign_previous = False

                        # Switch Assigner to tunneling mode before
                        # assigning.
        # tunnel_max_ct is tunable, seems to work best in range 10-50,
        # Results from apps-Calendar:
        #
        # tunnel  lw branch  changelist   P4ROOT    _translate_commits()
        # max_ct  ct         ct           MB        seconds
        # ------  ---------  -----------  --------  --------
        #   0     908         7125        1000      101
        #   1     660         7142         835      101
        #   5     373         7840         608       81
        #  10     238         8817         494       80
        #  50     109        11703         431       81
        # 100     102        12000         444       89
        # 500      92        18424         521      101
        assigner.tunnel_max_ct = 10
        assigner.tunnel_assign = True

        assigner.assign()
        return assigner

    def _assign_changelist_numbers(self, assigner):
        """Assign ":123" gfmark strings to every changelist we will create.

        DESTRUCTIVE READ of branch Assigner, draining its .assign_dict
        as it converts each commit/branch association into an ObjectType
        commit/branch/changelist association. We need that memory back.
        """

                        # Why sequence by sorted gfe_mark instead of
                        # Assigner rev-list order?
                        #
                        # Required: changelist numbers must be assigned in the
                        # same order as git-fast-export commits.
                        # _translate_commit() requires all parent commits to
                        # already have changelist and file rev numbers to act
                        # as integ sources.
        sorted_gfe_marks = sorted( self._gfe_mark_to_sha1.keys()
                                 , key=int)
        for gfe_mark in sorted_gfe_marks:
            sha1 = self._gfe_mark_to_sha1[gfe_mark]
            branch_assignment = assigner.assign_dict.pop(sha1)
            for branch_id in branch_assignment.branch_id_list():
                change_gfmark = self._assign_ghost(
                                       ghost_precedes_sha1 = sha1
                                     , branch_id           = branch_id
                                     , branch_assignment   = branch_assignment)
                if not change_gfmark:
                    change_gfmark = self._next_gfmark()

                        # Remember this changelist so we can use it
                        # as a parent of later commit/changelists.
                ot = ObjectType.create_commit(
                          sha1       = sha1
                        , repo_name  = self.ctx.config.repo_name
                        # SURPRISE: change_num here is a gf-mark ":123",
                        # not a real changelist number
                        , change_num = change_gfmark
                        , branch_id  = branch_id
                        )
                self._othistory.add(ot)

                        # Sever the child<->parent links within the Assigner
                        # nodes so that their memory becomes available while.
                        # we iterate, rather than only at the end when we drop
                        # the very last node.
            branch_assignment.release_links()

                        # Because our OTHistory replaces/impersonates a branch
                        # Assigner for PreflightChecker, copy this bool to
                        # OTHistory so that we can reject unworthy attempts to
                        # push anonymous branches when configured to reject
                        # anonymouse branches.
                        # This has nothing to do with OTHistory. We're just
                        # not going to complicate PreflightChecker's API
                        # with Yet Another API for this one bool. Sorry.
        self._othistory.have_anonymous_branches \
                = assigner.have_anonymous_branches

    def _assign_ghost( self, *
                     , ghost_precedes_sha1
                     , branch_id
                     , branch_assignment ):
        """If this the first commit on a branch, then we usually need
        to insert a ghost changelist to populate the branch with a copy
        of this commit's first-parent.

        Allocate a PopulateCommit instance to eventually hold that ghost
        changelist.

        Return the gfmark allocated to the non-ghost that follows this ghost.

        If no ghost required, return None.
        """
                        # Branch already has at least one commit?
                        # Then it's already populated.
        if branch_id in self._branch_id_to_popcom:
            return None
                        # Is this a root/orphan commit? Then no
                        # populate required.
        if not branch_assignment.parents:
            self._branch_id_to_popcom[branch_id] = None
            return None

        ghost_of_sha1 = branch_assignment.parents[0]

                        # Re-repo works better if it can dereference a
                        # ghost changelist to find the original copied-to-Git
                        # non-ghost changelist that we're ghosting.
        ghost_of_otl  = self._othistory.sha1_to_otl(ghost_of_sha1)
        if ghost_of_otl:
            ghost_of_change_num = ghost_of_otl[0].change_num
            parent_branch_id    = ghost_of_otl[0].branch_id
        else:
            ghost_of_change_num = None
            parent_branch_id    = None

        ghost_gfmark          = self._next_gfmark()
        ghost_precedes_gfmark = self._next_gfmark()
        pc = PopulateCommit(
                         branch_id             = branch_id
                       , ghost_of_sha1         = ghost_of_sha1
                       , ghost_of_change_num   = ghost_of_change_num
                       , ghost_of_branch_id    = parent_branch_id
                       , ghost_gfmark          = ghost_gfmark
                       , ghost_precedes_gfmark = ghost_precedes_gfmark
                       , ghost_precedes_sha1   = ghost_precedes_sha1
                       )
        assert pc.ghost_precedes_gfmark not in self._gfmark_to_popcom
        self._branch_id_to_popcom[branch_id]  = pc
        self._gfmark_to_popcom[ghost_precedes_gfmark] = pc
        return ghost_precedes_gfmark

    def _create_g2p(self):
        """Instantiate the G2P kitchen sink class that we use for a desc_info,
        but not for the entire translation.
        """
        assert not self._g2p
        g2p = G2P( ctx            = self.ctx
                 , assigner       = None
                 , gsreview_coll  = None
                 )
                        # Stub out some functions that
                        # G2P.change_desc_info() calls, but which we choose
                        # not to permit because they drag in too many
                        # assumptions about already-submitted changelists, or
                        # cross-branch ancestry. Just the kind of time-
                        # consuming calculation we're trying to avoid.
        def return_none(_self):
            """ NOP replacement. """
            return None

        def commit_sha1_to_otl(_self, sha1):
            """Replacement for G2P.commit_sha1_to_otl()

            Use our own FakeObjectTypeHistory() instead of live
            //.git-fusion/objects/.... or G2P.marks.
            """
                        # SURPRISE: parameter _self is a G2P instance,
                        # which knows nothing of our fake OT history.
                        # Use our own FastPush instance "self",
                        # bound when this _create_g2p() runs.
            return self._othistory.sha1_to_otl(sha1)

                        # Monkey patch/Duck punch instance (not class) method
                        # recipe from
                        #   http://stackoverflow.com/questions/962962
                        #pylint: disable=protected-access
        g2p.commit_sha1_to_otl = types.MethodType(commit_sha1_to_otl, g2p)
        g2p._parent_branch     = types.MethodType(return_none,        g2p)

                        # We don't (yet) have any fast-export mark/sha1 dict.
                        # We'll re-poke this into G2P later, after we
                        # run git-fast-export. For now, have emptiness.
        g2p.fast_export_marks = _FPMarks(gfe_mark_to_sha1 = dict())

        self._g2p = g2p

    def _create_preflight_checker(self):
        """Create the PreflightChecker instance that we use
        during pre_receive().
        """
        usermap = UserMap( self.ctx.p4gf
                         , self.ctx.email_case_sensitivity )
        self._g2p_user = G2PUser( ctx     = self.ctx
                                , usermap = usermap )
        self._preflight_checker = PreflightChecker(
              ctx                           = self.ctx
            , g2p_user                      = self._g2p_user
            , assigner                      = self._othistory # SURPISE!
            , gsreview_coll                 = None
            , already_copied_commit_runner  = None
            , finish_branch_definition      = None
            )
        self._preflight_checker.fast_export_marks = self._g2p.fast_export_marks
                        # Suppress per-commit 'p4 client -i' view swap.
        self._preflight_checker.set_client_on_branch_switch = False

                        # Not a PreflightChecker data member, but passed to
                        # PreflightChecker.check_commit_for_branch().
                        # Might as well create it here along with other
                        # PreflightChecker requirements.
        if not self.ctx.server_is_case_sensitive:
            self._case_conflict_checker = CaseConflictChecker(self.ctx)

    def _create_p4typemap(self):
        """Instantiate the 'p4 typemap' filetype assigner for pre_receive()."""
        self._p4typemap = P4TypeMap(self.ctx)

    def _finish_branch_instantiations(self):
        """Assigner fills in branch_dict, but with skeletal Branch instances
        and no DepotBranchInfo instances. Flesh 'em out.
        """
        assert self._g2p
        for branch in p4gf_branch.ordered(self.ctx.branch_dict().values()):
            self._finish_branch_instantiation(branch)

    def _finish_branch_instantiation(self, branch):
        """Assigner creates skeletal branches with no view mapping or
        depot branch. Fill 'em in.

        Skip dbi parent calculation: all our branches are fully populated
        with no inheritance, no parent, no fully populated basis.
        """
        dbi = branch.depot_branch
        if isinstance(branch.depot_branch, str):
                        # DBID assigned, but no depot branch instantiated yet.
            dbi = p4gf_depot_branch.new_definition(
                      dbid      = branch.depot_branch
                    , repo_name = self.ctx.config.repo_name
                    , p4        = self.ctx.p4gf )
        elif dbi is None and not branch.view_p4map:
                        # No DBID or view assigned yet, just a branch ID.
            dbi = p4gf_depot_branch.new_definition(
                      repo_name = self.ctx.config.repo_name
                    , p4        = self.ctx.p4gf )
        if dbi:
            self.ctx.depot_branch_info_index().add(dbi)
            branch.depot_branch = dbi

        if not branch.view_p4map:
                        # No view mapping assigned yet.
                        # Create a mapping by rerooting "master" to dbi.
            self._g2p.define_branch_view( branch       = branch
                                        , depot_branch = dbi
                                        , parent_otl   = [])
            branch.set_rhs_client(self.ctx.p4.client)

    def _next_gfmark(self):
        """Increment and return a changelist mark number ":1234"."""
        r = ":{}".format(self._next_change_num)
        self._next_change_num += 1
        return r

    def _git_fast_export(self):
        """
        Run git-fast-export to a temp file.

        Does NOT parse the file: that occurs commit-by-commit when
        you iterate over gfe.parse_next_command(), which streams
        commit dicts and avoids building a giant list.

        Memory: FastExport loads the entire output into memory, and keeps
        it there. About 1GB for the linux kernel.
        """
        self._gfe = FastExport(
                  ctx             = self.ctx
                , last_old_commit = None
                , last_new_commit = [prt.new_sha1 for prt in self._prl.sets()]
                )
        self._gfe.run(parse_now = False)
        self._gfe_mark_to_sha1 = self._gfe.marks

                        # Poke the new mark dict into our hacked/patched G2P
                        # so that it will see the newly fast-exported marks.
        self._g2p.fast_export_marks = _FPMarks(self._gfe_mark_to_sha1)

    def _open_bat_gfunzip(self):
        """Create a new object to receive blobs and trees as
        we encounter them later when we translate commits.
        """
        self._bat_gfunzip = self._create_gfunzip("bat_gfunzip")

    def _open_commit_gfunzip(self):
        """Create a new object to receive changelists as
        we encounter them later when we translate commits into changelists.
        """
        dir_index = 1 + len(self._commit_gfunzip_list)
        dir_name = "commit_gfunzip.{:03d}".format(dir_index)
        self._commit_gfunzip = self._create_gfunzip(dir_name)
        self._commit_gfunzip_rev_ct = 0
        self._rev_store.clear_all_contextdata_written()

    def _open_gitmirror_gfunzip(self):
        """Create a new object to receive ObjectType copies
        of Git commits, depot branch-info files, and p4gf_config(2)
        files.
        """
        self._gitmirror_gfunzip = self._create_gfunzip("gitmirror_gfunzip")

    def _open_desc_info_gfunzip(self):
        """Create a new object to receive db.change and db.desc
        updates of already-submitted changelists.
        """
        self._desc_info_gfunzip = self._create_gfunzip("desc_info_gfunzip")

    def _create_gfunzip(self, dir_name):
        """Create a new XxxxZip object to receive data that we'll
        send to Perforce in a 'p4 unzip' command later.
        """
        obj = GFUnzip( self.ctx
                     , os.path.join(self._persistent_dir, dir_name)
                     )
        obj.open()
        return obj

    def _open_desc_info_file(self, for_write = True):
        """Open a JSON file through which we'll send our DescInfo from
        pre-receive to post-receive.
        """
        self._desc_info_abspath = os.path.join( self._persistent_dir
                                              , "desc_info.dat" )
        if for_write:
            flags = "w"
        else:
            flags = "r"
        self._desc_info_fp = open( self._desc_info_abspath
                                 , flags
                                 , encoding = "utf-8" )

    def _close_desc_info_file(self):
        """Close the DescInfo file that pre-receive writes and
        post-receive reads.
        """
        if self._desc_info_fp:
            self._desc_info_fp.close()
            self._desc_info_fp = None

    def _close_bat_gfunzip(self):
        """Flush the blobs-and-trees journal files, copy to zip, close
        the zip.
        """
        self._bat_gfunzip.close()
        self._bat_gfunzip_abspath = self._bat_gfunzip.p4unzip.zip.abspath

    def _close_commit_gfunzip(self):
        """Flush the translated-commits journal files, copy to zip, close
        the zip.
        """
        self._commit_gfunzip.close()
        d = { "abspath"      : self._commit_gfunzip.p4unzip.zip.abspath
            , "begin_gfmark" : self._commit_gfunzip_begin_gfmark
            }
        self._commit_gfunzip_list.append(d)

    def _close_gitmirror_gfunzip(self):
        """Flush the GitMirror journal files, copy to zip, close
        the zip.
        """
        self._gitmirror_gfunzip.close()
        self._gitmirror_gfunzip_abspath \
                = self._gitmirror_gfunzip.p4unzip.zip.abspath

    def _close_desc_info_gfunzip(self):
        """Flush the db.change/db.desc journal file, copy to zip, close
        the zip.
        """
        self._desc_info_gfunzip.close()
        self._desc_info_gfunzip_abspath \
                = self._desc_info_gfunzip.p4unzip.zip.abspath

    def _reset_p4_connection(self):
        """Help P4D defend against memory leaks: reset our connection to
        p4 after every 'p4 unzip' request.

        P4D leaves a lot of memory around after 'p4 unzip', all
        tied to the connection that ran the unzip. Resetting the connection
        frees up that memory on the P4D server.
        """
        p4gf_create_p4.p4_disconnect(self.ctx.p4)
        p4gf_create_p4.p4_connect(self.ctx.p4)

    def _send_bat_gfunzip(self):
        """Network transfer the blobs-and-trees zip archive to Perforce,
        import it into Perforce.
        """
        LOG.debug("_send_bat_gfunzip() {}".format(self._bat_gfunzip_abspath))
        if not os.path.exists(self._bat_gfunzip_abspath):
            raise RuntimeError(_("Blobs-and-trees zip file not found: {}")
                               .format(self._bat_gfunzip_abspath))
        cmd = [ "unzip"
              , "-A"            # include (a)rchived revisions
              , "-f"            # (f)orce: clobber existing revisions
              , "-I"            # skip (i)nteg records (we don't send any)

                                # deep-undoc flag to tell P4D to leave our
                                # lbrRev "1.1" values unchanged. Allows us
                                # to know a blob's full lbr record without
                                # having to run a HUGE O(n) revisions query
                                #   p4 files //.gf/objects/blobs/...
                                #
              , "--retain-lbr-revisions"

              , "--transfer"    # deep-undoc network transfer flag.
              , "-i", self._bat_gfunzip_abspath
              ]
        LOG.debug3("_send_bat_gfunzip() p4 {}".format(" ".join(cmd)))
        r = self.ctx.p4run(*cmd)
        LOG.debug3(pformat(r))
        self._reset_p4_connection()

    def _send_commit_gfunzips(self):
        """Network transfer the translated commits zip archives to Perforce,
        import them into Perforce.
        """
        for d in self._commit_gfunzip_list:
            self._send_commit_gfunzip(
                  commit_gfunzip_abspath = d["abspath"]
                , first_pushed_gfmark    = d["begin_gfmark"]
                )

    def _send_commit_gfunzip(self
            , commit_gfunzip_abspath
            , first_pushed_gfmark
            ):
        """Network transfer a single commits zip archive to Perforce,
        import it into Perforce.
        """
        LOG.debug("_send_commit_gfunzip() {}"
                  .format(commit_gfunzip_abspath))
        if not os.path.exists(commit_gfunzip_abspath):
            raise RuntimeError(_("Commit zip file not found: {path}")
                               .format(path=commit_gfunzip_abspath))
        cmd = [ "unzip"
              , "--transfer"    # deep-undoc network transfer flag.
              , "-i", commit_gfunzip_abspath
              ]
        LOG.debug3("_send_commit_gfunzip() p4 {}".format(" ".join(cmd)))
        r = self.ctx.p4run(*cmd)
        first_pushed_change_num \
            = p4gf_util.first_value_for_key(r, "firstPushedChange")
        last_pushed_change_num \
            = p4gf_util.first_value_for_key(r, "lastPushedChange")
        LOG.debug("_send_commit_gfunzip()"
                  " first_gfmark={}"
                  " firstPushedChange={}"
                  " lastPushedChange={}"
                  .format( first_pushed_gfmark
                         , first_pushed_change_num
                         , last_pushed_change_num))
        self._gfmark_offsets.append(
                  gfmark               = first_pushed_gfmark
                , submitted_change_num = first_pushed_change_num )
        self._reset_p4_connection()

    def _send_gitmirror_gfunzip(self):
        """Network transfer the blobs-and-trees zip archive to Perforce,
        import it into Perforce.
        """
        LOG.debug("_send_gitmirror_gfunzip() {}"
                    .format(self._gitmirror_gfunzip_abspath))
        if not os.path.exists(self._gitmirror_gfunzip_abspath):
            raise RuntimeError(_("GitMirror zip file not found: {}")
                               .format(self._gitmirror_gfunzip_abspath))
        cmd = [ "unzip"
              , "-A"            # include (a)rchived revisions
              , "-f"            # (f)orce: clobber existing revisions
              , "-I"            # skip (i)nteg records (we don't send any)

                                # deep-undoc flag to tell P4D to leave our
                                # lbrRev "1.1" values unchanged. Allows us
                                # to know a blob's full lbr record without
                                # having to run a HUGE O(n) revisions query
                                #   p4 files //.gf/objects/blobs/...
                                #

              , "--transfer"    # deep-undoc network transfer flag.
              , "-i", self._gitmirror_gfunzip_abspath
              ]
        LOG.debug3("_send_gitmirror_gfunzip() p4 {}".format(" ".join(cmd)))
        r = self.ctx.p4run(*cmd)
        LOG.debug3(pformat(r))
        self._reset_p4_connection()

    def _send_desc_info_gfunzip(self):
        """Network transfer the bulk-update 'change -f' zip archive to
        Perforce, import it into Perforce.
        """
        LOG.debug("_send_desc_info_gfunzip() {}"
                  .format(self._desc_info_gfunzip_abspath))
        if not os.path.exists(self._desc_info_gfunzip_abspath):
            raise RuntimeError(_("Changelist desc_info zip file not found: {path}")
                               .format(path=self._desc_info_gfunzip_abspath))
        cmd = [ "unzip"
              , "-f"            # (f)orce: clobber existing revisions
              , "-I"            # skip (i)nteg records (we don't send any)
              , "-R"            # skip (r)ev records (we don't send any)

              , "--transfer"    # deep-undoc network transfer flag.
              , "-i", self._desc_info_gfunzip_abspath
              ]
        LOG.debug3("_send_desc_info_gfunzip() p4 {}".format(" ".join(cmd)))
        r = self.ctx.p4run(*cmd)
        LOG.debug3(pformat(r))
        self._reset_p4_connection()

    def _translate_commits(self):
        """Iterate through all git-fast-export commits, converting to
        Perforce changelists.
        """
        changelist_ct = self._othistory.ct()
        self._eta = p4gf_eta.ETA(total_ct = changelist_ct)
        with ProgressReporter.Determinate(changelist_ct):
            LOG.debug("Total changelists: {:,d}".format(changelist_ct))
            for gfe_commit in self._gfe.parse_next_command():
                self._translate_commit(gfe_commit)

                            # Debugging: early termination for huge repos.
                if self._max_translate_commit_ct:
                    self._max_translate_commit_ct -= 1
                    if self._max_translate_commit_ct <= 0:
                        break

                        # Raise all "accumulate then report at the end"
                        # errors.
                        #
                        # Could move this copypasta into PreflightChecker
                        # along with the rest of CaseConflictChecker
                        # creation/feeding/management.
                        #
        if self._case_conflict_checker:
            cc_text = self._case_conflict_checker.conflict_text()
            if cc_text:
                raise PreflightException(cc_text)


    def _translate_commit(self, gfe_commit):
        """Convert one git-fast-export commit dict to
        one Perforce changelist for each branch to which it
        is assigned.
        """
        sha1 = gfe_commit['sha1']
        otl  = self._othistory.sha1_to_otl(sha1)
        self._g2p_user.get_author_pusher_owner(gfe_commit)
        self._preflight_checker.check_commit(gfe_commit)

        for ot in otl:
            self._preflight_checker.check_commit_for_branch(
                      commit                = gfe_commit
                    , branch_id             = ot.branch_id
                    , any_locked_files      = False
                    , case_conflict_checker = self._case_conflict_checker
                    )
            self._translate_commit_ot(gfe_commit, ot)

        self._copy_trees_to_bat(sha1)

    def _translate_commit_ot(self, gfe_commit, ot):
        """Convert one git-fast-export commit dict to
        one Perforce changelist on the branch specified in ot.
        """
        branch_id = ot.branch_id
        branch    = self.ctx.branch_dict()[branch_id]

        if LOG.isEnabledFor(logging.DEBUG):
            self._eta.increment()
            ProgressReporter.increment(
                    _("Converting changelists...   eta {et} {ed}")
                    .format( et = self._eta.eta_str()
                           , ed = self._eta.eta_delta_str()))
            LOG.debug("_translate_commit_ot() eta={et} {ed}   ot={ot}"
                      .format( ot = ot
                             , et = self._eta.eta_str()
                             , ed = self._eta.eta_delta_str()))

                        # First commit/changelist on a new depot branch?
                        # If, during changelist assignment, we also assigned
                        # a PopulateCommit to precede this changelist on this
                        # branch, then now's the time to populate.
        popcom = self._gfmark_to_popcom.get(ot.change_num)
        if popcom:
            self._populate_branch(popcom, branch)

                        # Before starting this commit/changelist, if the
                        # commit_gfunzip file is getting full, close it
                        # and open a fresh one.
                        #
                        # Do this AFTER any popcom/ghost changelist, just to
                        # simplify our code: unconditionally record
                        # ot.change_num as the first changelist written to
                        # the new gfunzip.
        gfe_files = gfe_commit["files"]
        if not self._commit_gfunzip_can_accept(len(gfe_files)):
            self._rotate_commit_gfunzip(next_gfmark = ot.change_num)
        self._commit_gfunzip_rev_ct += len(gfe_files)

                        # If the commit that git-fast-export used as the
                        # "before" half of its diff is not what this Perforce
                        # branch currently holds, replace git-fast-export's
                        # useless diff with one we generate against the current
                        # branch contents.
        if not self._branch_head_is_gfe_first_parent(gfe_commit, ot):
            gfe_files = self._replace_gfe_with_gdt(gfe_commit, ot)

                        # Mark submodule file delete actions with "gitlink" tag
                        # so that _translate_rev() can skip over them: they are
                        # not tracked as files in Perforce.
                        #
        gitlinks = self._mark_submodule_deletes(gfe_files, ot)
                        #
                        # Stuff that list of gitlinks list into gfe_commit for
                        # transfer to DescInfo. Yeah it's a bit of a hack, but
                        # it's either that or widen a LOT of API to get this
                        # gitlink list through to G2P.change_desc_info().
                        #
                        # Set/clear this each time through: we reuse the same
                        # gfe_commit object for multiple branches, don't want
                        # to repeat some other branch's submodule list.
        if gitlinks:
            gfe_commit["gitlinks"] = gitlinks
        else:
            gfe_commit.pop("gitlinks", None)

                        # Start a new changelist as a db.change record. Can't
                        # write it until after processing file revisions: need
                        # the greatest common depot directory from the file
                        # revisions for the db.change.root field.
        di = self._to_desc_info(gfe_commit, branch)
        change_utc_dt = self._calc_changelist_utc_dt(di)
        self._record_desc_info( gfmark        = ot.change_num
                              , desc_info     = di
                              , change_utc_dt = change_utc_dt
                              , owner         = gfe_commit["owner"]
                              )
        self._write_change_records( gfmark        = ot.change_num
                                  , di            = di
                                  , change_utc_dt = change_utc_dt
                                  )

        integ_src_otl = self._create_integ_src_otl(di, ot.branch_id)

                        # Extract the commit's file revisions from Git and
                        # store directly into the zip archive. Reuse already-
                        # copied revisions as lazy copies. Record newly copied
                        # revisions so we can reuse them later.
        translated_rev_ct = self._translate_revs(
              ot            = ot
            , branch        = branch
            , change_utc_dt = change_utc_dt
            , gfe_files     = gfe_files
            , integ_src_otl = integ_src_otl
            )
        if not translated_rev_ct:
            self._create_empty_placeholder_dbrev(ot, branch, change_utc_dt)

                        # Remove any submodule delete markers we  stored in
                        # gfe_file dicts, in case this gfe_files list is reused
                        # on a second or later Perforce depot branch, where the
                        # diff-against-head might no longer be a submodule
                        # delete.
        if gitlinks:
            self._clear_submodule_deletes(gfe_files)

        self._set_curr_head_ot(ot)

    def _replace_gfe_with_gdt(self, gfe_commit, ot):
        """Convert one commit dict to a single Perforce changelist, running our
        own git-diff-tree to calculate which actions to record in Perforce.
        """
        LOG.debug("_replace_gfe_with_gdt {}".format(ot))

                        # What do we currently have in the Perforce depot
                        # branch? Can be empty tree if this is first commit
                        # to a root/orphan branch.
        curr_head_ot = self._curr_head_ot(ot.branch_id)
        curr_head_sha1 = p4gf_const.EMPTY_TREE_SHA1
        if curr_head_ot:
            curr_head_sha1 = curr_head_ot.sha1

                        # Run git-diff-tree, parse results into a dict
                        # that looks like what FastExport produces, so that
                        # we can funnel that through the same code.
        gdt_files = []
        for gdt in p4gf_git.git_diff_tree(
                  old_sha1              = curr_head_sha1
                , new_sha1              = gfe_commit["sha1"]
                , find_copy_rename_args = None
                ):
            gfe_file = { "action" : gdt.action }

            if gdt.action in ["A", "M", "T"]:
                gfe_file = { "action"    : gdt.action
                           , "mode"      : gdt.new_mode
                           , "sha1"      : gdt.new_sha1
                           , "path"      : gdt.gwt_path
                           }
            elif gdt.action == "D":
                gfe_file = { "action"    : gdt.action
                           , "path"      : gdt.gwt_path
                           }
            elif gdt.action in ["R", "C"]:
                gfe_file = { "action"    : gdt.action
                           , "mode"      : gdt.new_mode
                           , "sha1"      : gdt.new_sha1
                           , "path"      : gdt.gwt_path
                           , "from_path" : gdt.from_path
                           }
            else:
                raise RuntimeError("Unexpected git-diff-tree action '{}'"
                                   .format(gdt.action))
            gdt_files.append(gfe_file)

        return gdt_files

    def _branch_head_is_gfe_first_parent(self, gfe_commit, ot):
        """Is the current head changelist on ot.branch_id the same commit
        sha1 as what git-fast-export used as first-parent for gfe_commit?

        If not, then git-fast-export's list of diffs are inapplicable to
        the current branch head, a new set must be generated.
        """
                        # Which branch did git-fast-export use for its diff?
                        # Can be None if this is first commit on an
                        # orphan/root branch
        gfe_par1_gfe_mark = gfe_commit.get("from")
        gfe_par1_sha1     = self._gfe_mark_to_sha1.get(gfe_par1_gfe_mark, None)

                        # What did we most recently record to this branch?
        curr_head_sha1 = self._curr_branch_head_sha1(ot.branch_id)
        match = gfe_par1_sha1 == curr_head_sha1
        if (                    LOG.isEnabledFor(logging.DEBUG2)
            or ((not match) and LOG.isEnabledFor(logging.DEBUG))
            ):
            level = logging.DEBUG2 if match else logging.DEBUG
            how   = "=="           if match else "!="

            LOG.log( level
                   , "_branch_head_is_gfe_first_parent()  {gfmark:>6}"
                     " {sha1}   {branch_id}  gfe par1={gfe}"
                     " {how} {brhead}=branch head"
                     .format(
                          how       = how
                        , gfmark    = ot.change_num
                        , sha1      = _ab(ot.sha1)
                        , branch_id = _ab(ot.branch_id)
                        , gfe       = _ab(gfe_par1_sha1)
                        , brhead    = _ab(curr_head_sha1)
                        ))
        return match

    def _curr_branch_head_sha1(self, branch_id):
        """Return the given branch's most recently recorded commit sha1,
        if any. Return None if branch is empty.
        """
        curr_head_ot = self._curr_head_ot(branch_id)
        if curr_head_ot:
            return curr_head_ot.sha1
        return None

    def _mark_submodule_deletes(self, gfe_files, ot):
        """Mark gfe_file dict elements of gfe_files that delete a submodule.

        Return a list of (sha1, gwt_path) for each submodule in ot, including
        any in ot's parent deleted in ot.

        Other submodule gfe_file actions (A M T) carry a file mode 160000 to
        tell us it's a submodule action, don't bother copying to a Perforce
        file revision. But D delete? No file mode. So instead, insert a
        'gitlink' key.
        """
                        # What submodules are deleted from .gitmodules?
                        # Usually there is a 1:1 correspondence between
                        # deleting a .gitmodules entry and deleting its
                        # corresponding ls-tree entry, but not always.
                        # So check 'em both. Starting with .gitmodules...

                        # Fetch .gitmodule contents from both the current
                        # commit and whichever parent we're diffing against.
                        # As ConfigParser instances. They'll be empty if no
                        # .gitmodules file for that commit.
        par_sha1 = self._curr_branch_head_sha1(ot.branch_id)
        s1 = self._gitmodules_gwt_paths(par_sha1)
        s2 = self._gitmodules_gwt_paths(ot.sha1)
        submodule_gwt_paths = s1.union(s2)
        par_commit = self.ctx.repo[par_sha1] if par_sha1 else None
        cur_commit = self.ctx.repo[ot.sha1]
        def _filemode(commit, gwt_path):
            """Return file mode or None if not exist."""
            try:
                return commit.tree[gwt_path].filemode
            except (KeyError, AttributeError):
                return None
        deleted_gwt_paths = set()
        for gwt_path in submodule_gwt_paths:
            par_mode = _filemode(par_commit, gwt_path)
            cur_mode = _filemode(cur_commit, gwt_path)
            if par_mode == FileModeInt.COMMIT and not cur_mode:
                deleted_gwt_paths.add(gwt_path)

                        # Check ls-tree enties for any 'D'eleted
                        # paths that were a 16000-mode entry in the previous
                        # commit copied to this Perforce depot branch.
        for gfe_file in gfe_files:
            if (    gfe_file["action"] != "D"
                and gfe_file["path"] not in deleted_gwt_paths):
                continue
            if self._is_submodule_te( commit_sha1 = par_sha1
                                    , gwt_path    = gfe_file["path"] ):
                deleted_gwt_paths.add(gfe_file["path"])

                        # +++ Skip the gfe_file loop in the almost-always
                        #     case that there are no submodule deletes
                        #     to mark.
        if not deleted_gwt_paths:
            return []

        for gfe_file in gfe_files:
            if gfe_file["path"] in deleted_gwt_paths:
                gfe_file["gitlink"] = "# is submodule delete"

        return [ (p4gf_const.NULL_SHA1, gwt_path)
                 for gwt_path in deleted_gwt_paths ]

    @staticmethod
    def _clear_submodule_deletes(gfe_files):
        """Remove any gfe_file["gitlink"] markers inserted by
        _mark_submodule_deletes().

        Do this before re-using gfe_files on a second Perforce depot branch.
        """
        for gfe_file in gfe_files:
            gfe_file.pop("gitlink", None)

    @lru_cache(maxsize = 100)
    def _gitmodules_gwt_paths(self, commit_sha1):
        """Return a set() of submodule gwt_paths from the given commit's
        .gitmodules file.
        Return empty set if no .gitmodules file.
        """
        gitmodules_cp = p4gf_git.parse_gitmodules_for_commit_sha1(
                            self.ctx.repo, commit_sha1)
        gwt_paths     = set()
        if gitmodules_cp:
            for section in gitmodules_cp:
                gwt_path = gitmodules_cp.get( section
                                            , "path"
                                            , raw      = True
                                            , fallback = None )
                if gwt_path:
                    gwt_paths.add(gwt_path)
        return gwt_paths

    def _create_empty_placeholder_dbrev(self, ot, branch, change_utc_dt):
        """Record a single DBRev record that adds or edits a
        .p4gf_empty_changelist_placeholder.
        """
        gwt_path = p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER
        self._ensure_empty_blob()
        gfe_file = { "path" : gwt_path
                   , "action" : "M"
                   , "sha1"   : p4gf_const.EMPTY_BLOB_SHA1
                   , "mode"   : FileModeStr.PLAIN
                   }
        p4d_secs = p4gf_time_zone.utc_dt_to_p4d_secs(change_utc_dt, self.ctx)
        self._translate_rev(
              gfe_file      = gfe_file
            , ot            = ot
            , branch        = branch
            , p4d_secs      = p4d_secs
            , integ_src_otl = []
            )

    def _translate_revs(self, *
            , ot
            , branch
            , change_utc_dt
            , gfe_files
            , integ_src_otl
            ):
        """Write a db.rev record for each file revision in this
        commit.

        Also write a zero-th revision to contextdata.jnl for the first
        new revision of each depot path.

        Return the number of revisions translated.
        """
                        # +++ Convert per-changelist time once per changelist,
                        #     rather than per-revision.
        p4d_secs = p4gf_time_zone.utc_dt_to_p4d_secs(change_utc_dt, self.ctx)

                        ### copy/rename support goes here, fold fuzzy
                        ### match rows here, either before or inside top
                        ### of loop here.
        translated_rev_ct = 0
        for gfe_file in gfe_files:
                        # Skip Git submodules. They are recorded as DescInfo
                        # "gitlinks:" text, not Perforce file revisions.
            if gfe_file.get("mode") == FileModeStr.COMMIT:
                continue

            w = self._translate_rev(
                          gfe_file      = gfe_file
                        , ot            = ot
                        , branch        = branch
                        , p4d_secs      = p4d_secs
                        , integ_src_otl = integ_src_otl
                        )
            translated_rev_ct += w
        return translated_rev_ct

    def _translate_rev(self, *
            , gfe_file
            , ot
            , branch
            , p4d_secs
            , integ_src_otl
            ):
        """Create a single DBRev struct for this file.

        Return 1 if db.rev written, 0 if not.
        """
        gwt_path         = gfe_file['path']
        depot_path       = self._gwt_to_depot_path( gwt_path = gwt_path
                                                  , branch   = branch )
        p4_exists        = self._rev_store.exists_at_head(depot_path)
        prev_head_db_rev = self._rev_store.head_db_rev(depot_path)
        rev_num          = self._rev_store.next_rev_num(depot_path)
        is_contextdata_written \
                         = self._rev_store.is_contextdata_written(depot_path)
        p4action         = _gfe_to_p4_file_action(p4_exists, gfe_file["action"])
        is_delete        = p4gf_p4dbschema.FileAction.DELETE == p4action
                        # Do not delete submodules via 'p4 delete'.
                        # Submodules are not tracked as files within Perforce.
        is_submodule = "gitlink" in gfe_file
        if is_delete and is_submodule:
            return 0
                        # Integrating from other branches? Replace 'p4
                        # add/edit' with 'p4 integ' for the file action.
                        # Writes one db.integ row pair for each integ source
                        # used.
        if integ_src_otl:
            p4action = self._translate_rev_integ(
                  gfe_file        = gfe_file
                , ot              = ot
                , p4action        = p4action
                , dest_depot_path = depot_path
                , integ_src_otl   = integ_src_otl
                , rev_num         = rev_num
                )

                        # Write the db.rev record for this file action.
        new_head_db_rev = self._translate_rev_row(
              gfe_file               = gfe_file
            , ot                     = ot
            , branch                 = branch
            , p4d_secs               = p4d_secs
            , p4action               = p4action
            , depot_path             = depot_path
            , is_delete              = is_delete
            , rev_num                = rev_num
            , prev_head_db_rev       = prev_head_db_rev
            , is_contextdata_written = is_contextdata_written
            )

        if not new_head_db_rev:
            return 0

        self._rev_store.record_head(new_head_db_rev, is_delete)
        self._rev_timer.increment(aux_ct = self._byte_ct)
        return 1

    def _translate_rev_integ(self, *
            , gfe_file
            , ot
            , p4action
            , dest_depot_path
            , integ_src_otl
            , rev_num
            ):
        """Should this revision be integrated from some other branch(es)?
        If so, write those integ records to db.integ, and return the
        integ/branch file action to use instead of whatever add/edit
        action a non-integ rev would use.

        If not integrating from another branch, return p4action unchanged.
        """
                        # Not integrating? Nothing to do.
        if not integ_src_otl:
            return p4action
                        # integ for delete has more restrictions than
                        # we want to encode for fast push. No integ.
        is_delete  = p4gf_p4dbschema.FileAction.DELETE == p4action
        if is_delete:
            return p4action

                        # There are only two outcomes now: copy or branch.
        is_branch = p4action == p4gf_p4dbschema.FileAction.ADD
        if is_branch:
            integ_how_pair = ( p4gf_p4dbschema.IntegHow.BRANCH
                             , p4gf_p4dbschema.IntegHow.BRANCH_R )
            integ_p4action = p4gf_p4dbschema.FileAction.BRANCH
        else:
            integ_how_pair = ( p4gf_p4dbschema.IntegHow.COPY
                             , p4gf_p4dbschema.IntegHow.COPY_R )
            integ_p4action = p4gf_p4dbschema.FileAction.INTEG

        dest_file_mode_int = FileModeInt.from_str(gfe_file["mode"])
        src_commit_sha1s   = set()
        for src_ot in integ_src_otl:

                        # Optional, and either a great or awful idea depending
                        # on what you want to see in integ history.
                        #
                        # Avoid duplicate integs when a source commit becomes
                        # duplicate changelists due to tunneling. Use each
                        # source commit only once.
                        #
            #if src_ot.sha1 in src_commit_sha1s:
            #    continue

            integ_src = self._integ_src(
                  gwt_path            = gfe_file["path"]
                , dest_file_sha1      = gfe_file["sha1"]
                , dest_file_mode_int  = dest_file_mode_int
                , integ_src_ot        = src_ot )
            if not integ_src:
                continue

            self._commit_gfunzip.write_integ_pair(
                  src_depot_path      = integ_src.depot_path
                , src_start_rev_int   = integ_src.start_rev_int
                , src_end_rev_int     = integ_src.end_rev_int
                , dest_depot_path     = dest_depot_path
                , dest_rev_int        = rev_num
                , how                 = integ_how_pair[0]
                , how_r               = integ_how_pair[1]
                , dest_change_num_int = _gfmark_to_int(ot.change_num)
                )
                        # Remember this source commit so that we know we've
                        # integed _something_, and so that we can skip
                        # duplicate changelists for this commit (if we so
                        # choose).
            src_commit_sha1s.add(src_ot.sha1)

        if src_commit_sha1s:
            return integ_p4action
        else:
            return p4action

    def _integ_src(self, *
            , gwt_path
            , dest_file_sha1
            , dest_file_mode_int
            , integ_src_ot
            ):
        """If integ_src_ot can act as an integ source for gwt_path, return
        a _IntegSource instance of integ source stuff:
            depot_path
            start_rev_int
            end_rev_int
        Return None if integ_src_ot is not a worthy integ source.
        """
        try:
            src_commit = self.ctx.repo[integ_src_ot.sha1]
            src_te     = src_commit.tree[gwt_path]

                        # Keep this first implementation simple: exact match
                        # only = branch/copy, no integ/merge.
            if not (    src_te.filemode == dest_file_mode_int
                    and src_te.hex      == dest_file_sha1):
                LOG.debug3("_integ_src()     src commit={commit_sha1}"
                           " sha1/mode mismatch {src_sha1} {src_mode}"
                           " != {dest_sha1} {dest_mode}  {gwt}"
                           .format( commit_sha1 = _ab(integ_src_ot.sha1)
                                  , src_sha1    = _ab(src_te.hex)
                                  , src_mode    = _fms(src_te.filemode)
                                  , dest_sha1   = _ab(dest_file_sha1)
                                  , dest_mode   = _fms(dest_file_mode_int)
                                  , gwt         = gwt_path))
                return None
        except KeyError:
                        # File not in this source commit. No source file from
                        # which to integ.
            LOG.debug3("_integ_src()     src commit={commit_sha1}"
                       " gwt not found   {gwt}"
                       .format( commit_sha1 = _ab(integ_src_ot.sha1)
                              , gwt         = gwt_path))
            return None

                        # Not in source branch mapping? Can't integ
                        # from unmapped space.
        src_branch     = self.ctx.branch_dict().get(integ_src_ot.branch_id)
        src_depot_path = self._gwt_to_depot_path( gwt_path = gwt_path
                                                , branch   = src_branch )
        if not src_depot_path:
            LOG.debug3("_integ_src()     src branch={branch_id}"
                       " gwt not mapped   {gwt}"
                       .format( branch_id = _ab(integ_src_ot.branch_id)
                              , gwt       = gwt_path))
            return None

        src_change_num = _gfmark_to_int(integ_src_ot.change_num)
        src_range = self._rev_store.src_range(src_depot_path, src_change_num)
        if src_range[0] is None or src_range[1] is None:
            LOG.debug3("_integ_src()     src depot_file={df}@{cn}"
                       " no src_range()   {gwt}"
                       .format( df  = src_depot_path
                              , cn  = integ_src_ot.change_num
                              , gwt = gwt_path))
            return None

        LOG.debug3("_integ_src() returning {}#{},{}"
                   .format(src_depot_path, src_range[0], src_range[1]))
        return _IntegSource( depot_path    = src_depot_path
                           , start_rev_int = src_range[0]
                           , end_rev_int   = src_range[1] )

    def _translate_rev_row(self, *
            , gfe_file
            , ot
            , branch
            , p4d_secs
            , p4action
            , depot_path
            , is_delete
            , rev_num
            , prev_head_db_rev
            , is_contextdata_written
            ):
        """Record a single db.rev record for this file.

        Return the DbRev struct that holds what we just recorded.
        """
        change_num  = _gfmark_to_int(ot.change_num)
        if not is_delete:
                        # Add revision to bat_gfunzip (unless it's already there).
            blob_sha1  = gfe_file["sha1"]
            lbr        = self._get_librarian_record(blob_sha1)
            lbr_path   = p4gf_path.blob_p4_path(blob_sha1)
            lbr_rev    = lbr_rev_str()

            debug3(LOG, "_translate_rev() {a} {mode} {sha1:7.7} {gwt}"
                   , a    = gfe_file["action"]
                   , mode = gfe_file["mode"]
                   , sha1 = gfe_file["sha1"]
                   , gwt  = gfe_file["path"]
                   )
            p4filetype = self._calc_file_rev_p4filetype(
                    lbr_record   = lbr
                  , gwt_path     = gfe_file["path"]
                  , branch       = branch
                  , gfe_mode_str = gfe_file["mode"])

            db_rev = p4gf_p4dbschema.DbRev(
                    depot_path              = depot_path
                  , depot_rev               = rev_num
                  , depot_file_type_bits    = p4filetype
                  , file_action_bits        = p4action
                  , change_num              = change_num
                  , date_p4d_secs           = p4d_secs
                  , md5                     = lbr.md5
                  , uncompressed_byte_ct    = lbr.byte_ct
                  , lbr_is_lazy             = p4gf_p4dbschema.RevStatus.LAZY
                  , lbr_path                = lbr_path
                  , lbr_rev                 = lbr_rev
                  , lbr_file_type_bits      = lbr.lbr_file_type
                  )

                        # Adding first rev? Create "rev#0" context.
            if (    1 == rev_num
                and not is_contextdata_written):
                assert not prev_head_db_rev
                prev_head_db_rev = copy.copy(db_rev)
                prev_head_db_rev.rev_num = 0

        else:  # is_delete == True
            debug3( LOG, "_translate_rev() {a}                {gwt}"
                  , a   = gfe_file["action"]
                  , gwt = gfe_file["path"]
                  )

                        # Rare, but it happens: deleting an ls-tree entry that
                        # has no previous db.rev. The previous db.rev was
                        # probably a 160000-mode COMMIT entry, which we never
                        # store as a db.rev.  We usually detect these in
                        # _mark_submodule_deletes() and skip, but if there is
                        # no corresponding change to .gitmodules, the 'D'elete
                        # action makes it all the way to here.
            if not prev_head_db_rev:
                LOG.warning("No previous db.rev for commit={sha1} {a} {gwt},"
                            " probably a 160000 that we never store in db.rev."
                            .format( sha1 = ot.sha1
                                   , a    = gfe_file["action"]
                                   , gwt  = gfe_file["path"]
                                   ))
                return None


            db_rev = copy.copy(prev_head_db_rev)
            db_rev.depot_rev               = rev_num
            db_rev.file_action_bits        = p4action
            db_rev.change_num              = change_num
            db_rev.date_p4d_secs           = p4d_secs
            db_rev.md5                     = MD5_NONE
            db_rev.uncompressed_byte_ct    = -1
            db_rev.lbr_is_lazy             = p4gf_p4dbschema.RevStatus.NOT_LAZY

        if is_contextdata_written:
            context_db_rev = None
        else:
            context_db_rev = prev_head_db_rev
        self._commit_gfunzip.write_rev(db_rev, context_db_rev = context_db_rev)
        return db_rev

    def _ensure_empty_blob(self):
        """Store an empty file for the empty blob sha1.

        Required for .p4gf_empty_changelist_placeholder.

        Return the lbr_rec for the the blob.
        """
                        # Already got one? Nothing to do.
        sha1  = p4gf_const.EMPTY_BLOB_SHA1
        lbr   = self._lbr_store.get(sha1)
        if lbr:
            return lbr

        return self._create_blob_librarian_file_bytes(
                      sha1         = sha1
                    , raw_bytes    = b''
                    )

    def _get_librarian_record(self, sha1):
        """If we already have a librarian record, return that.
        If not, git-cat-file the thing into existence, write it to the
        librarian store, and return a record for the new thing.

        Raises KeyError exception if you try to call us on a "D"elete
        action where we don't have a file sha1.

        ### Will have to deal with 100% match "C"opy/"R"ename actions
        that also omit sha1. We need that sha1.
        """
        lbr_rec = self._lbr_store.get(sha1)
        if not lbr_rec:
            lbr_rec = self._create_blob_librarian_file(sha1)
        return lbr_rec

    def _create_blob_librarian_file(self, sha1):
        """Extract a file blob from the Git repo, write it to the bat_gfunzip,
        and record it in the librarian.
        Return the librarian record for this new file.

        Do not call this for blobs already written to the bat_gfunzip.
            That pointlessly wastes time and space git-cat-file-ing the same
        blob into the zip archive, zip will spew warnings about duplicate
        entries, and Perforce probably won't appreciate a zip archive with
        multiple rev#1 of the same file.
        """
        raw_bytes  = p4gf_git.get_blob(sha1, self.ctx.repo)
        self._byte_ct += len(raw_bytes)
        return self._create_blob_librarian_file_bytes(
                          sha1         = sha1
                        , raw_bytes    = raw_bytes)

    def _create_blob_librarian_file_bytes(self, *
            , sha1
            , raw_bytes):
        """Write raw bytes to the bat_gfunzip, record to librarian.

        Implementation for _create_blob_librarian_file() when you already
        have the bytes (such as for the empty placeholder blob)
        """

        md5_strr   = hashlib.md5(raw_bytes).hexdigest().upper()
        uncompressed_byte_ct = len(raw_bytes)
        p4filetype = self._calc_lbr_p4filetype(raw_bytes)
        self._bat_gfunzip.write_blob_rev_1(
                                 sha1          = sha1
                               , md5           = md5_strr
                               , raw_bytes     = raw_bytes
                               , lbr_file_type = p4filetype )
        r = self._lbr_store.store(
              sha1          = sha1
            , md5           = md5_strr
            , byte_ct       = uncompressed_byte_ct
            , lbr_file_type = p4filetype )
        return r
                # pylint:disable=too-many-arguments
    def _calc_file_rev_p4filetype(
              self
            , lbr_record
            , gwt_path
            , branch
            , gfe_mode_str
            , depot_path = None
            ):
        """Return filetype bits for this file revision.

        Use the librarian's file type bits, plus or minus any
        executable flag from Git.

        :param depot_path: Optional: saves a P4.Map lookup if you already
                           know gwt_path's depot_path.
        """


                        # 1a. p4 typemap has an entry actual depot path?
                        #     +++ Skip if Perforce has no typemap.
        typemap_says = None
        if self._p4typemap.has_typemap():
            if depot_path is None:
                dst_depot_path = self._gwt_to_depot_path(gwt_path, branch)
            else:
                dst_depot_path = depot_path
            typemap_says = self._p4typemap.for_depot_path(dst_depot_path)

                        # 1b. p4 typemap for master-ish location of this file?
            if not typemap_says:
                masterish_depot_path = self._gwt_to_depot_path(
                                            gwt_path, self._masterish())
                typemap_says = self._p4typemap.for_depot_path(
                                            masterish_depot_path)
            typemap_says = _sanitize_p4filetype(typemap_says)

        base_mods = ['', '']
        if typemap_says:
            base_mods = p4gf_p4filetype.to_base_mods(typemap_says)

                        # 2. Git says it's a symlink? Then that trumps
                        #    whatever base was supplied by p4 typemap.
        if gfe_mode_str == FileModeStr.SYMLINK:
            base_mods[0] = "symlink"

                        #    Git says it's not a symlink? Then ignore
                        #    p4 typemap if it asked for one.
        elif base_mods[0] == "symlink":
            base_mods[0] = ''

                        # 3. p4 typemap supplied no base filetype?
                        #    Deduce from file content.
        if not base_mods[0]:
            base_mods[0] = p4gf_p4dbschema.FileType.to_base(
                                lbr_record.lbr_file_type)

        _sanitize_mods(base_mods)
        p4filetype = p4gf_p4dbschema.FileType.from_base_mods(base_mods)

                        # 4. If p4 typemap supplied no storage modifier
                        #    (+C, +F), repeat the storage modifier from
                        #    librarian storage.

                        #    Avoids silly "binary+D" format storing binary
                        #    files in RCS delta format.
        if not p4filetype & p4gf_p4dbschema.FileType.S_MASK:
            lbr_store_bits = lbr_record.lbr_file_type \
                           & p4gf_p4dbschema.FileType.S_MASK
            p4filetype |= lbr_store_bits

                        # 5. Force X bit to match Git.
        if gfe_mode_str == FileModeStr.EXECUTABLE:
            p4filetype |= p4gf_p4dbschema.FileType.EXECUTABLE
        else:
            p4filetype &= ~p4gf_p4dbschema.FileType.EXECUTABLE

        return p4filetype

    def _calc_lbr_p4filetype(self, raw_bytes):
        """Return librarian filetype bits for this blob, based on
        blob content and Git file mode.

        Ignores `p4 typemap` here. That doesn't apply to our lazy copy
        sources under //.git-fusion/objects/...
        """
        r = self._lbr_filetype_storage
        detected = self._detect_p4filetype(raw_bytes)
        # VERY noisy, fills debug log with entire content of every file rev.
        # Use only when debugging a specific, small, problem.
        #LOG.getChild("p4filetype").debug3("_calc_lbr_p4filetype() {:08x} {}"
        #    .format(detected, raw_bytes))
        return r + detected

    @staticmethod
    def _detect_p4filetype(raw_bytes):
        """What kind of file is this?"""
        p4filetype_str = p4gf_p4filetype.detect(raw_bytes)
        if p4filetype_str == "text":
            return p4gf_p4dbschema.FileType.TEXT
        else:
            return p4gf_p4dbschema.FileType.BINARY

    def _populate_branch(self, popcom, branch):
        """This is the first commit on an empty depot branch.
        Fill it with the same content as this commit's first parent.

        Add "populate" change record, and all its rev records,
        to the zip archives.

        Add all these rev#1 revs to our RevHistoryStore.

        Remember the ghost DescInfo for later rewrite.
        """
        LOG.debug("_populate_branch() {}".format(popcom))
        change_num_int = _gfmark_to_int(popcom.ghost_gfmark)
        change_utc_dt  = self._sha1_to_changelist_utc_dt(popcom.ghost_of_sha1)
        rev_ct         = 0
        p4d_secs       = p4gf_time_zone.utc_dt_to_p4d_secs(
                                change_utc_dt, self.ctx)
        LOG.debug("_populate_branch() git-ls-tree walk start")
        for l in p4gf_util.git_ls_tree_r(self.ctx.repo, popcom.ghost_of_sha1):
            if l.mode == FileModeStr.DIRECTORY:
                self._copy_tree_to_bat_if(tree = l.sha1)
            elif l.mode == FileModeStr.COMMIT:
                        # Skip submodules. They're tracked in DescInfo text
                        # not as file revisions.
                pass
            else:
                self._populate_revision(
                        branch         = branch
                      , change_num_int = change_num_int
                      , p4d_secs       = p4d_secs
                      , blob_sha1      = l.sha1
                      , gwt_path       = l.gwt_path
                      , gfe_mode_str   = l.mode
                      )
                rev_ct += 1
        LOG.debug("_populate_branch() git-ls-tree walk complete {} db.rev records".format(rev_ct))

                        # This ghost changelist is not part of our OTHistory,
                        # not copied as a GitMirror commit object, but but
                        # still needed as a "current branch head" when
                        # calculating correct Git diffs.
        ot = ObjectType.create_commit(
                  sha1       = popcom.ghost_of_sha1
                , repo_name  = self.ctx.config.repo_name
                , change_num = popcom.ghost_gfmark
                , branch_id  = popcom.branch_id
                )
                        # Branch starting empty? Nothing to populate? Force a
                        # changelist anyway, with a placholder file. Otherwise
                        # our gfmark/changelist renumbering will have gaps and
                        # off-by-1 errors (or off-by-N for n skipped populates)
        if not rev_ct:
            self._create_empty_placeholder_dbrev(ot, branch, change_utc_dt)

        di = self._ghost_desc_info(popcom)
        desc = di.to_text()
        self._commit_gfunzip.write_changelist(
                  ctx         = self.ctx
                , change_num  = change_num_int
                , client      = self.ctx.p4.client
                , user        = p4gf_const.P4GF_USER
                , date_utc_dt = change_utc_dt
                , description = desc
                )

        self._record_desc_info(
                  gfmark             = popcom.ghost_gfmark
                , desc_info          = di
                , change_utc_dt      = change_utc_dt
                , ghost_of_branch_id = popcom.ghost_of_branch_id
                , owner              = p4gf_const.P4GF_USER
                )

        self._set_curr_head_ot(ot)
        LOG.debug("_populate_branch() complete")

    def _populate_revision(self, *
            , branch
            , change_num_int
            , p4d_secs
            , blob_sha1
            , gwt_path
            , gfe_mode_str
            ):
        """Write a single file revision, part of a single "populate"
        changelist.
        """
        lbr_path   = p4gf_path.blob_p4_path(blob_sha1)
        lbr        = self._get_librarian_record(blob_sha1)
        depot_path = self._gwt_to_depot_path(
                                          gwt_path     = gwt_path
                                        , branch       = branch )
        p4filetype = self._calc_file_rev_p4filetype(
                                          lbr_record   = lbr
                                        , gwt_path     = gwt_path
                                        , branch       = branch
                                        , depot_path   = depot_path
                                        , gfe_mode_str = gfe_mode_str )
        db_rev = p4gf_p4dbschema.DbRev(
                  depot_path            = depot_path
                , depot_rev             = 1
                , depot_file_type_bits  = p4filetype
                , file_action_bits      = p4gf_p4dbschema.FileAction.ADD
                , change_num            = change_num_int
                , date_p4d_secs         = p4d_secs
                , md5                   = md5_str(lbr.md5)
                , uncompressed_byte_ct  = lbr.byte_ct
                , lbr_is_lazy           = p4gf_p4dbschema.RevStatus.LAZY
                , lbr_path              = lbr_path
                , lbr_rev               = lbr_rev_str()
                , lbr_file_type_bits    = lbr.lbr_file_type
                )
        prev_db_rev = copy.copy(db_rev)
        prev_db_rev.depot_rev = 0
        self._commit_gfunzip.write_rev(db_rev, context_db_rev = prev_db_rev)
        self._rev_store.record_head(db_rev, is_delete = False)

    def _branch_id_to_dbid(self, branch_id):
        """Return the depot branch ID that houses a branch."""
        br = self.ctx.branch_dict().get(branch_id)
        if not br:
            return None
        par_dbi = br.depot_branch
        if not par_dbi:
            return None
        return par_dbi.depot_branch_id

    def _ghost_desc_info(self, popcom):
        """Return a DescInfo that can produce a ghost changelist's
        description.
        """
        par_dbid = self._branch_id_to_dbid(popcom.ghost_of_branch_id)
        di = DescInfo()
        di.clean_desc           = _("Git Fusion branch management")
        di.push_state           = NTR("incomplete")
        di.parent_branch        = NTR("{}@{}").format(
                                                par_dbid
                                              , popcom.ghost_of_change_num )
        di.ghost_of_sha1        = popcom.ghost_of_sha1
        di.ghost_of_change_num  = popcom.ghost_of_change_num
        di.ghost_precedes       = popcom.ghost_precedes_sha1
        return di

    def _sha1_to_changelist_utc_dt(self, _sha1):
        """Return an appropriate timestamp to use for a Perforce changelist
        that is a translation of the given Git commit.

        If all you have is the Git commit sha1, this will fetch it
        from Git and convert it.
        Used only for "populate" changelists, were we lack a full
        DescInfo-parsed Git commit with author/commiter time.
        """
                        ### Ideally pygit2.Commit would provide  author and
                        ### commiter time and offset. But it provides only
                        ### commiter. So we have to parse the line ourselves,
                        ### similar to what p4gf_fastexport.get_commit() does.
                        ### Until then, here, have the current wallclock time.
        how = self._changelist_date_source
        if (    how == p4gf_config.VALUE_DATE_SOURCE_GIT_AUTHOR
            or  how == p4gf_config.VALUE_DATE_SOURCE_GIT_COMMITTER):
            LOG.warning("Unimplemented _sha1_to_changelist_utc_dt()")

        return p4gf_time_zone.now_utc_dt()

    def _to_desc_info(self, gfe_commit, branch):
        """Call G2P to create a DescInfo block for this changelist."""

                        # Prevent G2P from doing its own scan through
                        # gfe_commit["files"] searching for submodules. we
                        # already did that and stuffed the results we require
                        # into gfe_commit["gitlinks"].
        save_gfe_files = gfe_commit.pop("files", [])
        result = self._g2p.change_desc_info_with_branch(gfe_commit, branch)
        gfe_commit["files"] = save_gfe_files
        return result

    def _record_desc_info( self, *
                         , gfmark
                         , desc_info
                         , change_utc_dt
                         , ghost_of_branch_id = None
                         , owner
                         ):
        """Remember a changelist's DescInfo block so that we can later
        replace its gfmark placeholders with actual submitted changelist
        numbers.
        """
        d = desc_info.to_dict()
        d["gfmark"] = gfmark
        d["change_utc"] = int(change_utc_dt.timestamp())
        d["owner"] = owner
        if ghost_of_branch_id:
            d["ghost_of_branch_id"] = ghost_of_branch_id
                        # indent=None appears to produce a single line with no
                        # newlines (newlines encoded as literal "\n"). One
                        # record-per-line makes our reader code in
                        # _fill_desc_info_gfunzip() _much_ simpler.
        j = json.dumps(d, indent=None)
        self._desc_info_fp.write(j)
        self._desc_info_fp.write("\n")

    def _fill_desc_info_gfunzip(self):
        """Fill a 'p4 unzip' archive with updated changelist descriptions.
        No revs, just db.change and db.desc records.
        """
                        # Assumes you've already called
                        # _fill_branch_head_otl().
        assert self._branch_head_otl
        branch_head_gfmark_set = {ot.change_num
                                  for ot in self._branch_head_otl}

        with open(self._desc_info_abspath, "r", encoding="utf-8") as f:
            for line in f:
                self._fill_desc_info_one(line, branch_head_gfmark_set)

    def _fill_desc_info_one(self, line, branch_head_gfmark_set):
        """Load one line into a DescInfo, renumber it, write to
        DescInfo gfunzip payload.
        """
        if not line:
            return
        d                   = json.loads(line)
        gfmark              = d.get("gfmark")
        ts                  = d["change_utc"]
        ghost_of_branch_id  = d.get("ghost_of_branch_id")
        change_utc_dt       = p4gf_time_zone.seconds_to_utc_dt(ts)
        change_num_int      = self._gfmark_to_submitted_change_num(gfmark)
        LOG.debug3("_fill_desc_info_one() {} ==> {}"
                   .format(gfmark, change_num_int))
        di = DescInfo.from_dict(d)
        if gfmark in branch_head_gfmark_set:
            di.push_state = NTR("complete")
            LOG.debug3("_fill_desc_info_one() {} ==> {}"
                       "    push-state : complete"
                       .format(gfmark, change_num_int))
        self._renumber_desc_info(di, ghost_of_branch_id)
        self._desc_info_gfunzip.write_changelist(
              ctx         = self.ctx
            , change_num  = change_num_int
            , client      = self.ctx.p4.client
            , user        = d["owner"]
            , date_utc_dt = change_utc_dt
            , description = di.to_text()
            )

    def _renumber_desc_info(self, di, ghost_of_branch_id):
        """Convert all ":123" gfmark strings to "129" submitted changelist
        numbers.
        """
        if di.parent_changes:
            for key_sha1, val_gfmark_list in di.parent_changes.items():
                di.parent_changes[key_sha1] = [
                     self._gfmark_to_submitted_change_num(gfmark)
                     for gfmark in val_gfmark_list]
        if _is_gfmark(di.ghost_of_change_num):
            di.ghost_of_change_num  \
                = str(self._gfmark_to_submitted_change_num(
                        di.ghost_of_change_num))

                        # Only ghost changelists fill in parent-branch during
                        # fast push.
        if di.parent_branch:
            assert ghost_of_branch_id
            par_dbid = self._branch_id_to_dbid(ghost_of_branch_id)
            di.parent_branch = NTR("{}@{}").format(
                                         par_dbid
                                       , di.ghost_of_change_num )

    def _write_change_records(self, *, gfmark, di, change_utc_dt):
        """Record a db.change and db.desc journal record for this
        changelist.

        Owner is git-fusion-user for now. We'll reassign ownership
        later as part of _desc_info_gfunzip.
        """
        change_num = _gfmark_to_int(gfmark)
        self._commit_gfunzip.write_changelist(
                          ctx         = self.ctx
                        , change_num  = change_num
                        , client      = self.ctx.p4.client
                        , user        = p4gf_const.P4GF_USER
                        , date_utc_dt = change_utc_dt
                        , description = di.to_text()
                        )

    # lru_cache() here saves 2.3% Fast Push time, avoids about 75% of calls.
    @lru_cache(maxsize = 10000)
    def _gwt_to_depot_path(self, gwt_path, branch):
        """Optimized version of ctx.gwt_path(gwt).to_depot().

        Avoid creating the convert and goes straight to P4.Map.translate().
        """
        gwt_esc = p4gf_util.escape_path(gwt_path)
        client_path = '//{}/'.format(self.ctx.p4.client) + gwt_esc
        return branch.view_p4map.translate( client_path
                                          , branch.view_p4map.RIGHT2LEFT)

    def _calc_changelist_utc_dt(self, desc_info):
        """Return a changelist time, as a datetime instance in UTC.

        If repo is configured to use Git author or committer date, use that.
        If not, use current Git Fusion server time.
        """
        how = self._changelist_date_source
        if how == p4gf_config.VALUE_DATE_SOURCE_GIT_AUTHOR:
            src = desc_info.author
        elif how == p4gf_config.VALUE_DATE_SOURCE_GIT_COMMITTER:
            src = desc_info.committer
        else:
            return p4gf_time_zone.now_utc_dt()
        return p4gf_time_zone.git_strs_to_utc_dt( src["time"]
                                                , src["timezone"] )

    def _write_dbi_files(self):
        """Write all depot branch-info files to first zip payload."""
        dbi_index = self.ctx.depot_branch_info_index()
        dbi_list  = dbi_index.by_id.values()
        LOG.debug("_write_dbi_files() ct={}".format(len(dbi_list)))
        for dbi in dbi_list:
            self._bat_gfunzip.write_dbi(dbi)

    def _fill_gitmirror_gfunzip(self):
        """Back up any Git data that cannot reliably round-trip through
        Git->Perforce->Git translation.
        Record Git Fusion config and branch data.
        """
        self._copy_othistory_to_gitmirror_gfunzip()
        # branch info
        # config files

    def _copy_othistory_to_gitmirror_gfunzip(self):
        """Record a verbatim copy of every Git commit we translated,
        so that we can rebuild it verbatim.

        Assume that ls-trees were already sent as part of bat_gfunzip.
        """
        LOG.debug("_copy_othistory_to_gitmirror_gfunzip() ot_ct={}"
                  .format(self._othistory.ct()))
        for ot in self._othistory.ot_iter():
            self._copy_ot_to_gitmirror_gfunzip(ot)

    def _copy_ot_to_gitmirror_gfunzip(self, ot):
        """Record that a commit was copied to a branch view under
        some changelist number. Back up the commit's original content
        so that we can reproduce it later without whitespace changes.
        """
        change_num_int = self._gfmark_to_submitted_change_num(ot.change_num)
        self._byte_ct += self._gitmirror_gfunzip.copy_commit(
                            ot, change_num_int)

    def _copy_trees_to_bat(self, tree_sha1):
        """Copy trees to the bat_gfunzip.
        Skip any trees already in the bat_gfunzip.

        Recursive tree walk code similar to p4gf_util.git_iter_tree(), but
        different because we stop the tree walk as soon as we hit any already-
        copied tree. Do not waste CPU time recursing down paths that contain
        nothing new to copy.
        """
        tree_root = p4gf_util.treeish_to_tree(self.ctx.repo, tree_sha1)
        if not tree_root:
            LOG.debug("_copy_trees_to_bat() no tree for {}"
                      .format(p4gf_util.abbrev(tree_sha1)))
            return
        work_queue = deque([tree_root.oid])
        while work_queue:
            tree_oid = work_queue.pop()
            tree     = self.ctx.repo.get(tree_oid)
            copied   = self._copy_tree_to_bat_if(tree)
                        # Skip trees already added.
                        # Recurse into trees that needed a copy.
            if not copied:
                continue
            for child_te in tree:
                if child_te.filemode == FileModeInt.DIRECTORY:
                    work_queue.appendleft(child_te.oid)

    def _copy_tree_to_bat_if(self, tree):
        """If we have not yet recorded this tree to our bat_gfunzip,
        do so now.

        Accepts either a pygit2.Tree or a str tree_sha1.

        Return True if copied, False if already there, no need to copy.
        """
                        # Already stored? Nothing more to do.
        tree_sha1_str = TreeStore.to_sha1_str(tree)
        if self._tree_store.contains(tree):
            debug3(LOG, "_copy_tree_to_bat_if() already has {}"
                  , p4gf_util.abbrev(tree_sha1_str))
            return False
        self._byte_ct += self._bat_gfunzip.copy_tree(tree_sha1_str)
        self._tree_store.add(tree)
        debug3(LOG, "_copy_tree_to_bat_if() copied      {}"
               , p4gf_util.abbrev(tree_sha1_str))
        return True

    def _gfmark_to_submitted_change_num(self, gfmark):
        """Translate ":1" to integer(5), using the offset
        returned by 'p4 unzip' of our commit_gfunzip.
        """
        return self._gfmark_offsets.to_change_num(gfmark)

    def _fill_branch_head_otl(self):
        """For each pushed Git branch, find its commit in our
        ObjectTypeHistory.
        """
        branch_head_otl = []
        for prt in self._prl.set_heads:
            branch = self.ctx.git_branch_name_to_branch(prt.git_branch_name())
            if not branch:
                raise RuntimeError(_("Cannot find branch for {prt}").format(prt=prt))
            ot = self._othistory.sha1_branch_to_ot(
                      sha1      = prt.new_sha1
                    , branch_id = branch.branch_id )
            if not ot:
                raise RuntimeError(_("Cannot find changelist for {prt}")
                                   .format(prt=prt))
            branch_head_otl.append(ot)
        self._branch_head_otl = branch_head_otl

    def _set_p4keys(self):
        """Set all our index and last-copied keys."""
                        # Accumulate each changelist's
                        # "git-fusion-index-branch-" key/value pair.
        key_value   = {}

                        # Accumulate the max changelist number for each branch.
                        # Might as well do this while we're iterating through
                        # OTHistory and save ourselves a separate loop.
        last_copied = {}
        for ot in self._othistory.ot_iter():
                        # Why copy()? OTHistory still has ":123" gfmarks. We
                        # need integer submitted changelist numbers: int(129)
                        # But I really hate code that modifies contents of a
                        # collection as a side effect. So copy() until
                        # profiling says this is too expensive.
            otn = copy.copy(ot)
            otn.change_num             = self._gfmark_to_submitted_change_num(
                                             ot.change_num)
            (key_name, value)          = otn.to_index_key_value()
            key_value[key_name]        = value
            otprev = last_copied.get(otn.branch_id)
            if (not otprev) or (otprev.change_num < otn.change_num):
                last_copied[otn.branch_id] = otn

                        # Now that we have the highest changelist's ObjectType
                        # for each branch, we can add them to our bulk-update
                        # dict.
        for otn in last_copied.values():
            (key_name, value) = otn.to_index_last_key_value()
            key_value[key_name] = value

                        # last-copied-changelist-number, others.
        max_cn_int = max([otn.change_num for otn in last_copied.values()])
        max_cn_key = P4Key.calc_last_copied_change_p4key_name(
                        self.ctx.config.repo_name, p4gf_util.get_server_id())
        key_value[max_cn_key] = str(max_cn_int)

        P4Key.set_many(self.ctx, key_value)

    def _submit_config_files(self):
        """ 'p4 add/edit + submit' our config and config2 file,
        if we have any and they have changes worth writing.

        We cannot easily fit this file into our 'p4 unzip' payloads, because it
        might already exist in Perforce. 'p4 unzip' can only modify existing
        files if we first know the db.rev info for  that existing rev, and I
        don't want to pay a 'p4 fstat' cost just to avoid a later 'p4 submit'.

        Bonus: reuse existing code for Branch.add_to_config() and
        RepoConfig.write_repo2_if().

        Code adapted from GitMirror.add_branch_config2() and
        _add_config2_branch_defs_to_p4()

        Writes config(1) only if new depot branches defined.
        """
        p4gf_gitmirror.write_config1(self.ctx)
        branch_list_config_2 = [b for b in self.ctx.branch_dict().values()
                                if b.is_worthy_of_config2()]
        for b in p4gf_branch.ordered(branch_list_config_2):
            LOG.debug2("building p4gf_config2: add branch\n{}".format(b))
            b.add_to_config(self.ctx.repo_config.repo_config2)
        self.ctx.repo_config.write_repo2_if(self.ctx.p4gf)

    def _curr_head_ot(self, branch_id):
        """Return the most recent OT recorded for this branch_id via
        _set_curr_head_ot().
        """
        return self._branch_id_to_curr_head_ot.get(branch_id)

    def _set_curr_head_ot(self, ot):
        """Return the most recent OT recorded for this branch_id via
        _set_curr_head_ot().
        """
        self._branch_id_to_curr_head_ot[ot.branch_id] = ot

    def _create_integ_src_otl(self, desc_info, dest_branch_id):
        """Return a list of zero or more parent object types, one for each
        parent from some branch other than current branch.

        Return empty list if not merging from any other branch.
        """
        integ_src_otl = []
        for par_sha1 in desc_info.parents:
            otl = self._othistory.sha1_to_otl(par_sha1)
            for ot in otl:
                if ot.branch_id == dest_branch_id:
                    continue
                integ_src_otl.append(ot)
        return integ_src_otl

    @lru_cache(maxsize = 1)
    def _masterish(self):
        """Return the most equal branch in our config."""
        return self.ctx.most_equal()

    def _commit_gfunzip_can_accept(self, rev_ct):
        """Is the current commit_gfunzip archive is getting full?

        To limit Fast Push's memory requirements on the Perforce server,
        limit any individual commit_gfunzip archve to N db.rev records.
        """
                        # Empty zip archives can always accept their first
                        # commit, even if that commit exceeds our max.
        if self._commit_gfunzip_rev_ct == 0:
            return True

        want = self._commit_gfunzip_rev_ct + rev_ct
        return want <= self._max_rev_per_commit_gfunzip

    def _rotate_commit_gfunzip(self, next_gfmark):
        """Close the current commit_unzip and open a new one."""
        self._close_commit_gfunzip()
        self._open_commit_gfunzip()
        self._commit_gfunzip_begin_gfmark = next_gfmark

    def _is_submodule_te(self, commit_sha1, gwt_path):
        """Is the given gwt_path a 160000-mode tree entry at the
        given commit?

        Ignores .gitmodule, we're just looking for 160000-mode tree entries
        here.

        Okay to call with None
        """
        try:
            commit = self.ctx.repo[commit_sha1]
            mode   = commit.tree[gwt_path].filemode
            LOG.debug3("is_submodule_te() {} {} mode={}"
                       .format( p4gf_util.abbrev(commit_sha1)
                              , gwt_path
                              , mode ))
            return mode == FileModeInt.COMMIT
        except (KeyError, AttributeError) as e:
            LOG.debug3("_is_submodule_te() {} {} not found: {}"
                       .format( p4gf_util.abbrev(commit_sha1)
                              , gwt_path
                              , e ))
            # No commit (we permit input commit_sha1=None) or tree entry (we
            # expect paths that do not exist).
        return False


# End class FastPush
# ----------------------------------------------------------------------------

class ObjectTypeHistory:
    """Commit sha1/Branch ID/Changelist number store.

    Each Git commit is assigned to one or more branches, and given a
    gfmark ":123" which acts as a changelist number until `p4 unzip`
    assigns a true changelist number.

    Same data that Git Fusion stores
    under //.git-fusion/objects/{repo}/commits/...
    as part of gitmirror.

    This store fits in memory (really!). It's about the same size as
    the branch Assigner's assign_dict().
    """
    def __init__(self):
        self._store = defaultdict(list)
        self._ct    = 0

                        # Nothing to do with ObjectTypeHistory, here solely
                        # because ObjectTypeHistory impersonates branch
                        # Assigner for PreflightChecker.
        self.have_anonymous_branches = False

    def sha1_to_otl(self, sha1):
        """Return a list of fake ObjectType instances recorded with the
        requested sha1.
        """
        return self._store[sha1]

    def add(self, ot):
        """Remember a fake ObjectType instance."""
        self._store[ot.sha1].append(ot)
        self._ct += 1

    def sha1_branch_to_ot(self, sha1, branch_id):
        """Return exactly one matching OT, or None."""
        l = self._store[sha1]
        for ot in l:
            if ot.branch_id == branch_id:
                return ot
        return None

    def __len__(self):
        return self._ct

    def ct(self):       # pylint:disable=invalid-name
        """How many Romans?"""
        return self._ct

    def ot_iter(self):
        """Iterate through all ObjectType instances."""
        for v in self._store.values():
            yield from v

    def to_dict_list(self):
        """Convert to a flat list of pickle-friendly dicts."""
        return [ ot.to_dict()
                 for ot in self.ot_iter() ]

    @staticmethod
    def from_dict_list(l):
        """Inflate from a list of pickle-friendly dicts."""
        oth = ObjectTypeHistory()
        for d in l:
            oth.add(ObjectType.from_dict(d))
        return oth

    # -- branch Assigner API replacement -------------------------------------

    def is_assigned(self, sha1):
        """Do we have at least one branch assigned for this commit?"""
        return sha1 in self._store

    def branch_id_list(self, sha1):
        """Return a list of branch_ids assigned to a commit."""
        return [ot.branch_id for ot in self._store[sha1]]

# end class ObjectTypeHistory
# ----------------------------------------------------------------------------

class _FPMarks:
    """Replacement for G2P.fast_export_marks.

    Expects an instance of p4gf_fastexport_marks.Marks
    """
    def __init__(self, gfe_mark_to_sha1):
        self.gfe_mark_to_sha1 = gfe_mark_to_sha1

    def get_commit(self, mark):
        """Return the sha1 that corresponds with mark."""
        return self.gfe_mark_to_sha1[mark]

    @staticmethod
    def set_head(_prt_ref):
        """Stub to squelch pylint warnings from on our
        PreflightChecker.check_prt_and_commits(), which never is invoked fakey
        _FPMarks replacement.
        """
        pass

# end class _FPMarks
# ----------------------------------------------------------------------------

# What to use as an integ source. Returned by _integ_src().
_IntegSource = namedtuple("_IntegSource", [ "depot_path"
                                          , "start_rev_int"
                                          , "end_rev_int" ])

# end class _FPMarks
# ----------------------------------------------------------------------------

class TreeStore:
    """A BigStore set of all trees copied to the bat_gfunzip.

    Keys are byte-array not string.
    """
    def __init__(self, file_path, store_how):
                        # pylint:disable=invalid-name
        self._file_path = file_path
        if store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_DICT:
            self._s = set()
        elif store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MEMORY:
            self._s = p4gf_squee_value.SqueeValueDict(file_path)
        elif store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_SINGLE_TABLE:
            self._s = p4gf_squee_value.SqueeValue(file_path)
        else:
            raise RuntimeError(_("Unsupported store_how {store_how}")
                               .format(store_how=store_how))

    def contains(self, sha1):
        """Already stored?"""
        return TreeStore.to_key(sha1) in self._s

    def add(self, sha1):
        """Already stored?"""
        key = TreeStore.to_key(sha1)
        return self._s.add(key)

    @staticmethod
    def to_key(tree):
        """Convert pygit2.Tree or sha1 to the compact form we use as keys."""
        if isinstance(tree, bytes):
            return tree
        elif isinstance(tree, str):
            return binascii.a2b_hex(tree)

        sha1_str = p4gf_pygit2.object_to_sha1(tree)
        return binascii.a2b_hex(sha1_str)

    @staticmethod
    def to_sha1_str(tree):
        """Convert a pygit2.Tree or sha1 to a str form of sha1."""
        if isinstance(tree, str):
            return tree
        elif isinstance(tree, bytes):
            return binascii.b2a_hex(tree)
                        # Will raise an AttributeError if not a
                        # pygit2.Tree or TreeEntry.
        as_str = p4gf_pygit2.object_to_sha1(tree)
        return as_str

# end class TreeStore
# ----------------------------------------------------------------------------

class GFMarkOffsets:
    """A class that tracks multiple gfmark/submitted_change_num offset pairs,
    and can return the correct submitted changelist number for any gfmark.
    """
    def __init__(self):
        self._gf        = []  # int(123) not ":123"
        self._cn_offset = []  # if 127 corresponds to gfmark ":123", then int(4)

    def append(self, gfmark, submitted_change_num):
        """Remember for later calls to to_change_num()."""
        gf = _gfmark_to_int(gfmark)
        cn = int(submitted_change_num)
        cn_offset = cn - gf
        self._gf       .append(gf)
        self._cn_offset.append(cn_offset)
        LOG.getChild("gfmark_offsets").debug("append {} + {} = {}"
                .format(gfmark, cn_offset, submitted_change_num))

    def to_change_num(self, gfmark):
        """Return the submitted changelist number that goes with gfmark."""
        gf = _gfmark_to_int(gfmark)
        index = bisect.bisect_right(self._gf, gf) - 1
        cn_offset = self._cn_offset[index]
        submitted_change_num = gf + cn_offset
        LOG.getChild("gfmark_offsets").debug3("to_change_num {} + {} = {}"
                .format(gfmark, cn_offset, submitted_change_num))
        return submitted_change_num

# end class TreeStore
# ----------------------------------------------------------------------------


# -- module-wide -------------------------------------------------------------

# Struct to hold changelists assigned to first changelist on a new
# depot branch: one for the ghost that populates it, and then one
# to hold the actual Git commit.
PopulateCommit = namedtuple("PopulateCommit",
    [ "branch_id"               # Branch being populated
    , "ghost_of_sha1"           # Git first-parent commit that we'll ghost.
    , "ghost_of_change_num"     # gfmark assigned to parent we're ghosting
    , "ghost_of_branch_id"      # branch that holds @ghost_of_change_num
    , "ghost_gfmark"            # GFMark assigned to ghost that populates this
                                #   branch, to hold a parent commit of "sha1"
    , "ghost_precedes_gfmark"   # GFMark assigned to hold first "real"
                                #   changelist on this branch, to hold
                                #   commit "sha1"
    , "ghost_precedes_sha1"     # First "real" commit on this branch
    ])

# Structure to hold a .gitmodule entry, to help _mark_submodule_deletes()
# detect deletes.
GitmodulesEntry = namedtuple("GitmodulesEntry", ["gwt_path", "sha1"])

def _is_configured(ctx):
    """Is config setting enable-fast-push yes/true?"""
    return ctx.repo_config.getboolean(
                  p4gf_config.SECTION_GIT_TO_PERFORCE
                , p4gf_config.KEY_ENABLE_FAST_PUSH
                , fallback = True )


def _is_repo_view_empty(ctx):
    """No competing changelists in the repo view."""
                    # +++ No changes at all in the union view?
                    #     We know we're empty, no need to check
                    #     each individual branch.
    if ctx.union_view_empty():
        return True

                    # There are changes in the union view, but
                    # those might be excluded by each individual
                    # branch view. Check each individual branch.
    for b in ctx.branch_dict().values():
        with ctx.switched_to_branch(b):
            r = ctx.p4run('changes', '-m1', ctx.client_view_path())
            if r:
                return False

                    # Each branch view empty, therefore repo is empty.
    return True


def _server_supports_unzip(ctx):
    """Is the Perforce server 15.1 or later?"""
    version_string = ctx.server_version
    assert version_string   # Called before ctx.connect()
                            # fills in server_version?

    d = p4gf_version_3.parse_p4d_version_string(version_string)
    year_sub_string = d['release_year'] + '.' + d['release_sub']
    year_sub = float(year_sub_string)
    patch_level = int(d['patchlevel'])

    if year_sub < 2015.1:
        return False
    if year_sub == 2015.1:
                        # @1028542 is not sufficient, lacks @1022853's support
                        # for 'p4 unzip -fIR'
                        # @1038654 was the first internal p15.1 build
                        # to include 'p4 unzip -fIR'.
        return 1038654 <= patch_level
    if 2015.2 <= year_sub:
        return True
    LOG.error("Unknown P4D version: {}".format(version_string))
    return False


def _pickle_file_abspath(repo_name):
    """Generate the name of the packet file for the given repo.

    :type repo: str
    :param repo: name of repository.

    :return: path and name for packet file.
    """
    file_name = NTR('push-data.pickle').format(repo=repo_name)
    file_path = os.path.join(_calc_persistent_dir(repo_name)
                       , file_name)
    return os.path.abspath(file_path)


def _gfe_to_p4_file_action(p4_exists, git_action):
    """Translate git-fast-export actions M/T/D to Perforce
    file actions 'add', 'edit', 'delete'.
    """
                        ### copy/rename support here, too.

    if git_action == 'M' and not p4_exists:
        return p4gf_p4dbschema.FileAction.ADD
    else:
        return { 'A' : p4gf_p4dbschema.FileAction.ADD
               , 'M' : p4gf_p4dbschema.FileAction.EDIT
               , 'D' : p4gf_p4dbschema.FileAction.DELETE }.get(git_action)


def _calc_persistent_dir(repo_name):
    """Return a directory name where we can create files that will
    outlive our process. Not a temporary directory.

    ~/.git-fusion/views/{repo}/fast_push/

    Does not ensure such a directory exists. Do that elsewhere.
    """
    return os.path.join(p4gf_const.P4GF_HOME, "views", repo_name, "fast_push")


def _gfmark_to_int(gfmark):
    """Convert ":123" to int(123)."""
    return int(gfmark[1:])


def _is_gfmark(gfmark):
    """Is this a gfmark string?"""
    return isinstance(gfmark, str) and gfmark.startswith(":")


def _sanitize_p4filetype(p4filetype_str):
    """Git Fusion supports a subset of p4filetypes.
    Replace any unsupported types with "binary".
    """
    if not p4filetype_str:
        return p4filetype_str

    base_mods = p4gf_p4filetype.to_base_mods(p4filetype_str)

                        # Cannot apply type 'symlink' via typemap. Only if Git
                        # says it's a symlink.
    if base_mods[0] == "symlink":
        base_mods[0] = ''

    if (    base_mods[0]
        and base_mods[0] not in p4gf_p4dbschema.FileType.SUPPORTED_P4FILETYPES):
        base_mods[0] = "binary"
        return p4gf_p4filetype.from_base_mods(base_mods[0], base_mods[1:])
    else:
        return p4filetype_str


def _sanitize_mods(base_mods):
    """Fast Push controls the storage format:
    force ON   +C  compressed file per revision (but not here, forced on elsewhere)
    force off  +D  one file with RCS deltas
    force off  +F  uncompressed file per revision (a future version of
                        Git Fusion should use this for JPGs and such)
    force off  +X  archive trigger

    SURPRISE: Modifies collection base_mods in-place!
    """
    for c in ['D', 'F', 'X']:
        if c in base_mods:
            base_mods.pop(base_mods.index(c))
    return base_mods

def debug3(log, msg, *arg, **kwarg):
    """If logging at DEBUG3, do so. If not, do nothing."""
    if log.isEnabledFor(logging.DEBUG3):
        log.debug3(msg.format(*arg, **kwarg))

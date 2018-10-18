#! /usr/bin/env python3.3
"""Config and Context classes."""

from collections import deque
from contextlib import contextmanager
import logging
import os
import pygit2
import time

from P4 import Map

import p4gf_p4cache
from   p4gf_client_pool             import ClientPool
import p4gf_create_p4
import p4gf_branch
from   p4gf_branch_files_cache      import BranchFilesCache
import p4gf_config
import p4gf_const
import p4gf_depot_branch
from   p4gf_g2p_preflight_hook      import PreflightHook
import p4gf_p4key                   as     P4Key
from   p4gf_l10n                    import _, NTR
from   p4gf_lfs_tracker             import LFSTracker
import p4gf_log
from   p4gf_p4result_cache          import P4ResultCache
import p4gf_p4spec
import p4gf_path
import p4gf_path_convert
import p4gf_protect
from   p4gf_temp_p4branch_mapping   import TempP4BranchMapping
import p4gf_util
import p4gf_repo_dirs
import p4gf_git
from p4gf_config_validator import validate_copy_rename_value

LOG = logging.getLogger(__name__)


# Rate for logging memory usage, in seconds
MEMLOG_HEART_RATE = 5
# Flag for dealing with differences between Linux and Darwin resources.
IsDarwin = os.uname()[0] == "Darwin"

# Maximum number of branches permitted in switched_to_union().
MAX_UNION_BRANCH_CT = 300


def client_path_to_local(clientpath, clientname, localrootdir):
    """ return client syntax path converted to local syntax."""
    return localrootdir + clientpath[2 + len(clientname):]


def strip_wild(path):
    """ strip trailing ... from a path.

    ... must be present; no check is made
    """
    return path[:-3]


def to_lines(x):
    """If x is a single string, perhaps of multiple lines, convert to a list of lines."""
    if isinstance(x, str):
        return x.splitlines()
    elif x is None:
        return x
    elif not isinstance(x, list):
        return [x]
    else:
        return x


def create_context(repo_name):
    """Construct a Context for the given repository."""
    cfg = Config()
    cfg.p4user = p4gf_const.P4GF_USER
    cfg.repo_name = repo_name
    return Context(cfg)


def client_spec_to_root(client_spec):
    """Return client root, minus any trailing /."""
    root_dir = p4gf_path.strip_trailing_delimiter(client_spec["Root"])
    return root_dir


class Config:

    """perforce config."""

    def __init__(self):
        self.p4port = None
        self.p4user = None
        self.p4client = None     # client for view
        self.repo_name = None    # git project name


class Context:

    """a single git-fusion view/repo context."""

    # pylint:disable=too-many-public-methods, too-many-instance-attributes
    # Context is our kitchen sink class, can have as many public methods & attributes as it wants.

    def __init__(self, config):
        # pylint:disable=too-many-statements

        self.config = config

        # connected by default:
        self._p4                    = None
        self._p4gf                  = None

        # not connected by default:
        self.p4gf_reviews           = None
        self.p4gf_reviews_non_gf    = None
        self.p4gf_reviews_all_gf    = None

        self.repo_config            = None
        self._create_config_if_missing = True
        self.timezone               = None
        self.server_version         = None
        self.server_is_case_sensitive = None # Becomes True or False when known.
        self.case_folding           = None # Becomes 0 or 1 when known
        self._user_to_protect       = None
        self.repo_dirs              = None
        self._repo_lock             = None
        self._repo                  = None
        self._push_id               = None
        self.is_lfs_enabled         = None
        self.lfs_tracker            = LFSTracker(ctx=self)

        # RAII object to operate on a numbered changelist with p4run and p4gfrun
        # set in p4gf_util by NumberedChangelist
        self.numbered_change        = None
        self.numbered_change_gf     = None

        # Environment variable set by p4gf_auth_server.py.
        self.authenticated_p4user   = os.environ.get(p4gf_const.P4GF_AUTH_P4USER)

        # user to impersonate, optional; used by GitSwarm
        self.foruser                = None

        # gf_branch_name ==> p4gf_branch.Branch
        # Lazy-loaded by branch_dict()
        self._branch_dict           = None

        # Our masterish branch. Lazy found and cached in most_equal().
        self._most_equal            = None

        # Config options
        self.branch_creation        = None
        self.merge_commits          = None
        self.swarm_reviews          = None
        self.submodules             = None
        self.owner_is_author        = None
        self.email_case_sensitivity = None
        self.job_lookup_list        = None
        self.git_autopack           = None
        self.git_gc_auto            = None
        self.checkpoints_enabled    = False
        self.find_copy_rename_args   = []
        self.find_copy_rename_enabled  = False
        self.add_copied_from_perforce = None
        self.git_p4_emulation       = None

        # DepotBranchInfoIndex of all known depot branches that house
        # files from lightweight branches, even ones we don't own.
        # Lazy-loaded by depot_branch_info_index()
        self._depot_branch_info_index = None

        # paths set up by set_up_paths()
        self.gitdepotroot           = "//{}/".format(p4gf_const.P4GF_DEPOT)
        self.gitlocalroot           = None
        self.client_spec_gf         = None
        self.gitrootdir             = None
        self.contentlocalroot       = None
        self.contentclientroot      = None
        self.clientmap              = None
        self.client_exclusions_added = False

        # Seconds since the epoch when we last logged memory usage.
        self._memlog_time        = None

            # A set of temporary Perforce client specs, each mapped to one
            # branch's view. Use these to query Perforce rather than switching
            # ctx.p4 back and forth just to run 'p4 files //client/...' for some
            # random branch other than our current branch.
        self._client_pool           = None

        # A single, shared temporary Perforce branch, useful for integrations.
        self._temp_branch           = None

            # Minimize the number of 'p4 files/fstat //branch-client/...@n' calls.
        self.branch_files_cache     = BranchFilesCache()
        self.branch_fstat_cache     = P4ResultCache(['fstat', '-Ol'])

            # Set by G2PMatrix during a 'git push' to remember the most recent
            # changelist integrated from each branch to each other branch.
            # Instance of IntegratedUpTo
        self.integrated_up_to       = None

            # Last N p4run() commands. Reported in _dump_on_failure()
        self.p4run_history          = deque(maxlen=20)

            # Last N p4run() commands. Reported in _dump_on_failure()
        self.p4gfrun_history        = deque(maxlen=20)

            # Admin-configured option to reject unworthy commits.
        self._preflight_hook        = None

            # don't allow nested __enter__()
        self.entered                = False

        self.server_id              = p4gf_util.get_server_id()

    @property
    def repo_lock(self):
        """The p4key lock associated with the context."""
        return self._repo_lock

    @repo_lock.setter
    def repo_lock(self, repo_lock):
        """Set the p4key lock to be associated with this context."""
        if repo_lock and self._repo_lock:
            # One or the other must be None, otherwise something is wrong;
            # caller should explicitly set the existing lock "None" first.
            raise RuntimeError("cannot set lock twice")
        self._repo_lock = repo_lock

    @property
    def p4(self):
        """Retrieve the P4 connection for repo access."""
        return self._p4

    # There _is_ no p4 setter, this class alone manages the p4 connection.
    # @p4.setter
    # def p4(self, value):
    #     """Set the P4 connection to be used for repo access."""
    #     assert self._p4gf is None
    #     self._p4gf = value

    @property
    def p4gf(self):
        """Retrieve the P4 connection for meta data access."""
        if self._p4gf is None:
            # Create the connection if one was not provided earlier.
            self._p4gf = p4gf_create_p4.create_p4_temp_client()
        return self._p4gf

    @p4gf.setter
    def p4gf(self, value):
        """Set the P4 connection to be used for meta data access."""
        # Either we should allow the connection to be set only one time, or
        # we should disconnect the old one before accepting the new one.
        # The point is to be very conscientious about connections. For now,
        # only allow setting once.
        assert self._p4gf is None
        self._p4gf = value

    def __enter__(self):
        assert not self.entered
        self.entered = True
        self.connect()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        assert self.entered
        self.entered = False
        self.disconnect()
        return False  # False == do not squelch any current exception

    def create_config_if_missing(self, value):
        """Enable or disable automatic creation of the repo config.

        By default, any missing configuration file will be created.

        """
        self._create_config_if_missing = value

    def connect_cli(self, log):
        """Common connect() wrapper that dumps failure to given stdout-oriented log.

        Meant for command-line scripts such as p4gf_rollback.py and
        p4gf_fork_config.py.
        """
        try:
            self.connect()
        except Exception:   # pylint: disable=broad-except
                            # Adding a rather pointless error message here
                            # just so we can more easily test l10n.
            if log:
                log.error(_("Unable to connect."))
                if self.p4 and not self.p4.connected():
                    log.error("P4PORT: {}".format(self.p4.port))
                    log.error("P4USER: {}".format(self.p4.user))
                elif self.p4gf and not self.p4gf.connected():
                    log.error("P4PORT: {}".format(self.p4gf.port))
                    log.error("P4USER: {}".format(self.p4gf.user))
                log.error("repo  : {}".format(self.config.repo_name))
            raise

    def connect(self):
        """Connect the p4 and p4gf connections.

        Note: p4gf_reviews, p4gf_reviews_all_gf and p4gf_reviews_non_gf are not connected!

        If this is the first time connecting, complete context initialization.

        """
        # if previously connected, just reconnect
        if self.p4:
            if not self.p4.connected():
                p4gf_create_p4.p4_connect(self.p4)
            if not self.p4gf.connected():
                p4gf_create_p4.p4_connect(self.p4gf)
            return

        # create connections and use them to complete initialization of context
        self._p4 = p4gf_create_p4.create_p4_repo_client(
            self.config.p4port, self.config.p4user, self.config.repo_name)
        self.config.p4client = self.p4.client
        self.p4gf_reviews = self.__make_p4(user=p4gf_util.gf_reviews_user_name())
        self.p4gf_reviews_non_gf = self.__make_p4(user=p4gf_const.P4GF_REVIEWS__NON_GF)
        self.p4gf_reviews_all_gf = self.__make_p4(user=p4gf_const.P4GF_REVIEWS__ALL_GF)
        msg = "context connecting p4=%s p4gf=%s reviews=%s reviews-non-gf=%s reviews-all-gf=%s"
        LOG.debug(msg, self.p4.connected(), self.p4gf.connected(), self.p4gf_reviews.connected(),
                  self.p4gf_reviews_non_gf.connected(), self.p4gf_reviews_all_gf.connected())

        # Ensure we have a repo config since the rest of the setup relies
        # on reading from a configuration file. In most cases the file
        # already exists, and in a few cases, it is provided before the
        # connections are created.
        if self.repo_config is None:
            self.repo_config = p4gf_config.RepoConfig.from_depot_file(
                self.config.repo_name, self.p4gf,
                create_if_missing=self._create_config_if_missing)
        self.__wrangle_charset()
        self.get_timezone_serverversion_case()
        self.__set_branch_creation()
        self.__set_merge_commits()
        self.__set_swarm_reviews()
        self.__set_submodules()
        self.__set_change_owner()
        self.__set_email_case_sensitivity()
        self.__set_job_lookup_list()
        self.__set_git_autopack()
        self.__set_git_gc_auto()
        self.__set_up_paths()
        self.__set_checkpoints_enabled()
        self.__set_copy_rename_enabled()
        self.__set_add_copied_from_perforce_enabled()
        self.__set_git_p4_emulation()
        self.__set_lfs_enabled()
        # ClientPool needs p4 client root set up
        self._client_pool = ClientPool(self)

    def disconnect(self):
        """Disconnect any P4 connections."""
        if self.p4 is None:
            return
        msg = "context disconnecting p4=%s p4gf=%s reviews=%s reviews-non-gf=%s reviews-all-gf=%s"
        LOG.debug(msg, self.p4.connected(), self.p4gf.connected(), self.p4gf_reviews.connected(),
                  self.p4gf_reviews_non_gf.connected(), self.p4gf_reviews_all_gf.connected())
        if not self.p4gf.connected():
            p4gf_create_p4.p4_connect(self.p4gf)
        self._client_pool.cleanup()
        if self._repo_lock and self._repo_lock.wholly_owned():
            # Now is our chance to clean up any clients left behind by a
            # crashed process.
            self._client_pool.cleanup_all()
        if self._repo_lock:
            # Due to the frequent use of ExitStack, it is possible that our
            # connection will be broken before we release the lock, so
            # explicitly release it now before we lose the connection.
            self._repo_lock.release()
        if self.p4.connected():
            p4gf_create_p4.p4_disconnect(self.p4)
        if self.p4gf.connected():
            p4gf_create_p4.p4_disconnect(self.p4gf)
        if self.p4gf_reviews.connected():
            p4gf_create_p4.p4_disconnect(self.p4gf_reviews)
        if self.p4gf_reviews_non_gf.connected():
            p4gf_create_p4.p4_disconnect(self.p4gf_reviews_non_gf)
        if self.p4gf_reviews_all_gf.connected():
            p4gf_create_p4.p4_disconnect(self.p4gf_reviews_all_gf)
        # Force the connections to be reconstructed next time, otherwise we
        # will attempt to use a temporary client that has been deleted by
        # the above disconnect.
        self._p4 = None
        self._p4gf = None
        self.p4gf_reviews = None
        self.p4gf_reviews_non_gf = None
        self.p4gf_reviews_all_gf = None

    @contextmanager
    def permanent_client(self, client_name):
        """Create a new P4 connection, temporarily suppressing the current one.

        By default the context uses a -ix temporary client, which makes shelving
        impossible. Apparently this affects the connection, rather than the
        specific client. As such, create a new connection for the named client,
        yield to the caller, and then restore the original connection.

        """
        saved_p4 = self.p4
        saved_map = self.clientmap
        saved_cea = self.client_exclusions_added
        saved_root = self.contentclientroot
        self._p4 = self.__make_p4(client=client_name, connect=True)
        try:
            yield
        finally:
            p4gf_create_p4.p4_disconnect(self.p4)
            self._p4 = saved_p4
            self.config.p4client = self.p4.client
            self.clientmap = saved_map
            self.client_exclusions_added = saved_cea
            self.contentclientroot = saved_root

    def last_copied_change_p4key_name(self):
        """Return name of a p4key that holds the highest changelist number
        copied to the our repo, on our Git Fusion server.
        """
        return P4Key.calc_last_copied_change_p4key_name(
            self.config.repo_name, p4gf_util.get_server_id())

    def read_last_copied_change(self):
        """Return the highest changelist number copied to a repo on a server."""
        return P4Key.get(self, self.last_copied_change_p4key_name())

    def write_last_copied_change(self, change_num):
        """Set the highest changelist number copied to a repo on a server."""
        # Do not set a value less than current
        current_last_copied_change = self.read_last_copied_change()
        if int(change_num) > int(current_last_copied_change):
            P4Key.set(self, self.last_copied_change_p4key_name(), change_num)

    def user_to_protect(self, user):
        """Return a p4gf_protect.Protect instance that knows the given user's permissions."""
        # Lazy-create the user_to_protect instance since not all
        # Context-using code requires it.
        if not self._user_to_protect:
            self._user_to_protect = p4gf_protect.UserToProtect(self.p4)
        return self._user_to_protect.user_to_protect(user)

    @property
    def repo(self):
        """Get the pygit2 repo object.

        Lazy-create the pygit2 repo object as needed.
        """
        # pygit2 loads git config once when creating the pygit2 Repository
        # We need non-bare - for the print handler to create blobs -
        # so we unset and set --bare around the Repository call
        if not self._repo:
            with p4gf_git.non_bare_git(self.repo_dirs.GIT_DIR):
                self._repo = pygit2.Repository(self.repo_dirs.GIT_DIR)
            LOG.debug('repo path {0}'.format(self._repo.path))
        return self._repo

    @property
    def push_id(self):
        """Retrieve the push identifier, or None if no push in effect."""
        if not self._push_id:
            key_name = P4Key.calc_repo_push_id_p4key_name(self.config.repo_name)
            self._push_id = P4Key.get(self.p4gf, key_name)
        return self._push_id

    def __make_p4(self, client=None, user=None, connect=False):
        """Create a connection to the Perforce server."""
        if not user:
            user = self.config.p4user
        if not client:
            client = self.config.p4client
        return p4gf_create_p4.create_p4(port=self.config.p4port, user=user,
                                        client=client, connect=connect)

    def __set_branch_creation(self):
        """Configure branch creation."""
        self.branch_creation = self.repo_config.getboolean(
            p4gf_config.SECTION_GIT_TO_PERFORCE,
            p4gf_config.KEY_ENABLE_BRANCH_CREATION)
        LOG.debug('Enable repo branch creation = {0}'.format(self.branch_creation))

    def __set_merge_commits(self):
        """Configure merge commits."""
        self.merge_commits = self.repo_config.getboolean(
            p4gf_config.SECTION_GIT_TO_PERFORCE,
            p4gf_config.KEY_ENABLE_MERGE_COMMITS)
        LOG.debug('Enable repo merge commits = {0}'.format(self.merge_commits))

    def __set_swarm_reviews(self):
        """Configure swarm review."""
        self.swarm_reviews = self.repo_config.getboolean(
            p4gf_config.SECTION_GIT_TO_PERFORCE,
            p4gf_config.KEY_ENABLE_SWARM_REVIEWS)
        LOG.debug('Enable repo swarm review = {0}'.format(self.swarm_reviews))

    def __set_submodules(self):
        """Configure submodule support."""
        self.submodules = self.repo_config.getboolean(
            p4gf_config.SECTION_GIT_TO_PERFORCE,
            p4gf_config.KEY_ENABLE_SUBMODULES)
        LOG.debug('Enable repo submodules = {0}'.format(self.submodules))

    def __set_change_owner(self):
        """Configure change ownership setting."""
        value = self.repo_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                     p4gf_config.KEY_CHANGE_OWNER)
        value = str(value).lower()
        if value not in [p4gf_config.VALUE_AUTHOR, p4gf_config.VALUE_PUSHER]:
            LOG.warning("change-owner config setting has invalid value, defaulting to author")
            value = p4gf_config.VALUE_AUTHOR
        self.owner_is_author = True if value == p4gf_config.VALUE_AUTHOR else False
        LOG.debug('Set change owner to {0}'.format(value))

    def __set_email_case_sensitivity(self):
        """Configure email_case_sensitivity."""
        self.email_case_sensitivity = self.repo_config.getboolean(
            p4gf_config.SECTION_AUTHENTICATION,
            p4gf_config.KEY_EMAIL_CASE_SENSITIVITY)
        LOG.debug('Email case sensitivity = {0}'.format(self.email_case_sensitivity))

    def __set_checkpoints_enabled(self):
        """Configure checkpoints."""
        self.checkpoints_enabled = self.repo_config.getboolean(
            p4gf_config.SECTION_UNDOC,
            p4gf_config.KEY_ENABLE_CHECKPOINTS)
        LOG.debug('Checkpoints enabled = {0}'.format(self.checkpoints_enabled))

    def __set_copy_rename_enabled(self):
        """Configure git copy rename enabled."""
        value = self.repo_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                     p4gf_config.KEY_ENABLE_GIT_FIND_COPIES)
        find_copies_enabled = validate_copy_rename_value(value)
        if not find_copies_enabled:
            raise RuntimeError(
                _('Perforce: Improperly configured enable-git-find-copies value: {value}')
                .format(value=value))

        if find_copies_enabled == '0%':
            find_copies_enabled = False

        value = self.repo_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                     p4gf_config.KEY_ENABLE_GIT_FIND_RENAMES)
        find_renames_enabled = validate_copy_rename_value(value)
        if not find_renames_enabled:
            raise RuntimeError(
                _('Perforce: Improperly configured enable-git-find-renames value: {value}')
                .format(value=value))
        if find_renames_enabled == '0%':
            find_renames_enabled = False

        if (self.repo_config.getboolean(p4gf_config.SECTION_PERFORCE_TO_GIT,
                                        p4gf_config.KEY_GIT_LFS_ENABLE) and
                (find_copies_enabled or find_renames_enabled)):
            LOG.info(_("Option 'git-lfs-enable' enabled.  Options 'enable-git-find-copies'"
                       " and 'enable-git-find-renames' will be ignored."))
            return

        if find_copies_enabled:
            self.find_copy_rename_args.append("--find-copies={}".format(find_copies_enabled))
            self.find_copy_rename_args.append("--find-copies-harder")
            self.find_copy_rename_enabled = True
        if find_renames_enabled:
            self.find_copy_rename_args.append("--find-renames={}".format(find_renames_enabled))
            self.find_copy_rename_enabled = True

        LOG.debug('find_copy_rename_enabled={0} find_copy_rename_args={1}'.
                  format(self.find_copy_rename_enabled, self.find_copy_rename_args))

    def __set_job_lookup_list(self):
        """Configure job-lookup."""
        j = self.repo_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                 p4gf_config.KEY_JOB_LOOKUP)
        if not j:
            self.job_lookup_list = None
        else:
            self.job_lookup_list = [l.strip() for l in str(j).splitlines()]
        LOG.debug('job-lookup = {0}'.format(self.job_lookup_list))

    def __set_git_autopack(self):
        """Configure git_autopack."""
        self.git_autopack = self.repo_config.getboolean(
            p4gf_config.SECTION_REPO_CREATION,
            p4gf_config.KEY_GIT_AUTOPACK)
        LOG.debug('git autopack = {0}'.format(self.git_autopack))

    def __set_git_gc_auto(self):
        """Configure git_gc_auto."""
        try:
            self.git_gc_auto = self.repo_config.getint(
                p4gf_config.SECTION_REPO_CREATION,
                p4gf_config.KEY_GIT_GC_AUTO)
        except ValueError as value:
            raise RuntimeError(
                _('Perforce: Improperly configured git-gc-auto value: must be numeric: {value}')
                .format(value=value))
        if self.git_gc_auto:
            self.git_gc_auto = str(self.git_gc_auto)
        LOG.debug('git gc_auto = {0}'.format(self.git_gc_auto))

    def __set_add_copied_from_perforce_enabled(self):
        """Configure 'Copied from Perforce' block for git originated commits."""
        self.add_copied_from_perforce = self.repo_config.getboolean(
            p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_ENABLE_ADD_COPIED_FROM_PERFORCE)
        LOG.debug('Enable add copied from perforce {0}'.format(self.add_copied_from_perforce))

    def __set_git_p4_emulation(self):
        """Configure 'Copied from Perforce' block for git originated commits."""
        self.git_p4_emulation = self.repo_config.getboolean(
            p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_ENABLE_GIT_P4_EMULATION)
        LOG.debug('Enable git-p4 emulation {0}'.format(self.git_p4_emulation))

    def __set_lfs_enabled(self):
        """Turn on Git LFS support if configured."""
        self.is_lfs_enabled = self.repo_config.getboolean(
            p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_GIT_LFS_ENABLE)
        LOG.debug('Git LFS support enabled {0}'.format(self.is_lfs_enabled))

    def __wrangle_charset(self):
        """figure out if server is unicode and if it is, set charset."""
        if not self.p4.server_unicode:
            return
        # we have a unicode server
        # first, always use utf8 for the gf connection
        # use that connection to fetch the config for the repo
        self.p4gf.charset = 'utf8'
        # then set the repo-specific charset for their connection
        self.p4.charset = self.repo_config.get(p4gf_config.SECTION_REPO_CREATION,
                                               p4gf_config.KEY_CHARSET)
        LOG.debug('repo charset will be: '+self.p4.charset)

    def client_view_path(self, change_num=None):
        """Return "//{client}/..." : the client path for whole view.

        Include ... wildcard.

        Optional change_num, if supplied, appended as "@N"
        Because I'm sick of constructing that over and over.
        """
        if change_num:
            return '{}@{}'.format(self.contentclientroot, change_num)
        return self.contentclientroot

    def client_view_union(self, branches):
        """Return the client view to the union of a list of branches.

        If branches is empty, use all branches in this repo.

        Returns a list of mapping lines.

        ### would be nice if we did not downgrade from MapTuple to (possibly enquoted) string here.

        """
        if MAX_UNION_BRANCH_CT < len(branches):
            raise ValueError("Bug: must chunk up branch_ct={}"
                             " in call to ctx.client_view_union()"
                             .format(len(branches)))
        tuple_list = p4gf_branch.calc_branch_union_tuple_list(self.p4.client, branches)
        return ["{} {}".format(p4gf_path.enquote(mt.lhs), p4gf_path.enquote(mt.rhs))
                for mt in tuple_list]

    def checkout_master_ish(self):
        """Switch Git to the first branch view defined in repo config.

        This is often master, but not always.
        Since P4GF defaults to configuring a 'master' branch, which may
        not exist, try the other configured branches before giving up.

        NOP if no such branch (perhaps an empty repo config?).
        """
        br = self.most_equal()
        if br and br.git_branch_name:
            # Guard against LFS filters that might be installed globally,
            # which is guaranteed to be the case in testing environments.
            with p4gf_git.suppress_gitattributes(self):
                if not p4gf_git.git_checkout(br.git_branch_name, force=True):
                    # Perhaps the most-equal branch does not exist?
                    # Try the others until we achieve success.
                    success = False
                    branches = self.branch_dict()
                    if LOG.isEnabledFor(logging.DEBUG2):
                        LOG.debug2("checkout_master_ish() branches: %s", branches)
                    for br in branches.values():
                        if br.git_branch_name is None:
                            LOG.debug2("checkout_master_ish() branch %s has no name",
                                       br.branch_id)
                        elif p4gf_git.git_checkout(br.git_branch_name, force=True):
                            success = True
                            LOG.debug("checkout_master_ish() fallback %s", br.git_branch_name)
                        else:
                            LOG.warning("unable to checkout branch %s in git", br.git_branch_name)
                    if not success:
                        LOG.warning('Unable to checkout sensible branch for %s',
                                    self.config.repo_name)
                        if self.repo_config and LOG.isEnabledFor(logging.DEBUG2):
                            config_content = p4gf_config.to_text('', self.repo_config.repo_config)
                            LOG.debug2("checkout_master_ish() config: %s", config_content)
                        else:
                            LOG.debug2("checkout_master_ish() repo_config not set")
                elif LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug("checkout_master_ish() preferred %s", br.git_branch_name)
        elif LOG.isEnabledFor(logging.DEBUG):
            LOG.debug('checkout_master_ish(): no most-equal branch for %s',
                      self.config.repo_name)

    def git_branch_name_to_branch(self, git_branch_name):
        """If we have an undeleted branch with the requested name, return it.

        If not, return None.

        O(n) scan.
        """
        assert git_branch_name
        name = git_branch_name
        if name.startswith('refs/heads/'):
            name = name[len('refs/heads/'):]
        for branch in self.branch_dict().values():
            if branch.deleted:
                continue
            if branch.git_branch_name == name:
                return branch
        return None

    def most_equal(self):
        """Return the masterish branch."""
        if not self._most_equal:
            self._most_equal = p4gf_branch.most_equal(self.branch_dict())
        return self._most_equal

    def branch_dict(self):
        """Return all known Git<->Perforce branch associations.

        Lazy-loaded from config file.

        """
        # Do not load the branch-info files, as that can be very expensive,
        # and we only need it when we have actual work to do, which comes
        # much later in the process.
        if not self._branch_dict:
            self._branch_dict = p4gf_branch.config_to_branch_dict(
                      config          = self.repo_config
                    , p4              = self.p4gf
                    , rhs_client_name = self.p4.client
                    )

            LOG.debug('branch_dict() lazy-loaded ct={}'
                      .format(len(self._branch_dict)))
            if LOG.isEnabledFor(logging.DEBUG2):
                LOG.debug2(p4gf_util.dict_to_log(self._branch_dict))

        return self._branch_dict

    def reset_branch_dict(self, branch_dict):
        """Discard existing branch dictionary and use the one given.

        Additionally, resets the branch info index with the given values.

        """
        self._branch_dict = branch_dict
        dbi_index = p4gf_depot_branch.DepotBranchInfoIndex(self)
        for branch in branch_dict.values():
            if branch.depot_branch:
                dbi_index.add(branch.depot_branch)
        self._depot_branch_info_index = dbi_index

    def undeleted_branches(self):
        """A list of all Branch values in branch_dict that are not deleted.

        For easier debugging, the list is sorted.
        """
        return [b[1] for b in sorted(self.branch_dict().items()) if not b[1].deleted]

    def depot_branch_info_index(self):
        """Return all known depot branches that house files for lightweight branches.

        This includes depot branches that other Git Fusion repos created.
        We must stay lightweight even when sharing across repos.

        Most DBI instances here are just a dbid extrapolated from depot_path.
        We'll lazy-load the few we really need later.
        """
        if not self._depot_branch_info_index:
            LOG.debug('depot_branch_info_index() loading from Perforce...')
            self._depot_branch_info_index \
                = p4gf_depot_branch.DepotBranchInfoIndex.from_p4_incomplete(self)
        return self._depot_branch_info_index

    def union_view_highest_change_num(self
            , after_change_num  = 0
            , at_or_before_change_num = 0
            ):
        """Return integer changelist number the most recent changelist
        that possibly intersects any branch view.

        :param after_change_num:
            if set, run 'p4 changes -e <num>' to limit P4D server work
            to just changes after <num>.

        :param at_or_before_change_num:
            if set, run 'p4 changes @<num>', returning the first changelist
            at or before <num>.
            AVOID: This can be computationally expensive
            for P4D to calculate this on large views. Currently permitted
            only for the initial init_repo() of grafted history.

        "union view" here may omit exclusionary/minus-mapping lines,
        so you might get a result that does not ACTUALLY intersect any
        branch.

        Return int(0) if no changes.

        See also union_view_is_empty().
        """
        if after_change_num and at_or_before_change_num:
            raise ValueError("Bug: Do not set both after_change_num"
                             " and at_or_before_change_num")
        result = 0
        for branch_chunk in self.iter_branch_chunks():
            with self.switched_to_union(branch_chunk):
                path = self.client_view_path()
                if after_change_num:
                    cmd = [ "changes", "-m1"
                          , "-e", str(after_change_num)
                          , path ]
                elif at_or_before_change_num:
                    cmd = [ "changes", "-m1"
                          , self.client_view_path(at_or_before_change_num) ]
                else:
                    cmd = [ "changes", "-m1"
                          , path ]
                r = self.p4run(*cmd)
            s = p4gf_util.first_value_for_key(r, "change")
            if s and result < int(s):
                result = int(s)
        return result

    def union_view_empty(self, omit_deleted = False):
        """If there are any files at all in the union view, return True.
        If not, return False.

        :param omit_deleted:
            Pass -e to omit files deleted at head?

        "union view" here may omit exclusionary/minus-mapping lines,
        so you might get a result that does not ACTUALLY intersect any
        branch.

        See also union_view_highest_changelist().
        """
        for branch_chunk in self.iter_branch_chunks():
            with self.switched_to_union(branch_chunk):
                path = self.client_view_path()
                if omit_deleted:
                    cmd = ["files", "-m1", "-e", path]
                else:
                    cmd = ["files", "-m1",       path]
                r = self.p4run(*cmd)
                if r:
                    return False
        return True

    def iter_writable_branch_chunks(self):
        """Iterate through our branch_dict, in small, union-able, subsets
        of 300 elements at a time.
        """
        writable_all = p4gf_branch.writable_branch_list(self.branch_dict()
                               , self.repo_config.repo_config)
        return p4gf_util.iter_chunks( writable_all
                                    , MAX_UNION_BRANCH_CT )

    def iter_branch_chunks(self):
        """Iterate through our branch_dict, in small, union-able, subsets
        of 300 elements at a time.
        """
        return p4gf_util.iter_chunks( list(self.branch_dict().values())
                                    , MAX_UNION_BRANCH_CT )

    def get_timezone_serverversion_case(self):
        """get server's timezone and server version and case sensivity via cached p4 info."""
        info = p4gf_util.get_p4_info_and_configurables(self.p4)
        server_date = info['serverDate']
        self.timezone = server_date.split(" ")[2]
        self.server_version = info['serverVersion']
        case_handling = info['caseHandling']
        self.server_is_case_sensitive = 'sensitive' == case_handling
        self.case_folding = 0 if self.server_is_case_sensitive else 1
        self.p4.case_folding = self.case_folding
        LOG.debug("case sensitive={0}  P4.case_folding={1}".
                  format(self.server_is_case_sensitive, self.p4.case_folding))

    def __set_up_paths(self):
        """set up depot and local paths for both content and P4GF.

        These paths are derived from the client root and client view.
        """
        self.__set_up_p4gf_paths()
        self.repo_dirs = p4gf_repo_dirs.from_p4gf_dir(self.gitrootdir, self.config.repo_name)
        self._set_up_content_paths()

    def _set_up_content_paths(self):
        """set up depot and local paths for both content and P4GF.

        These paths are derived from the client root and client view.
        """
        client = self.p4.fetch_client()
        self.clientmap = Map(client["View"])
        # If the len of the client Views differs from the len of the Map
        # then the P4 disabmbiguator added exclusionary mappings - note this here
        # for reporting a message back to the user.
        self.client_exclusions_added = len(client["View"]) != len(self.clientmap.as_array())

        # local syntax client root, force trailing /
        self.contentlocalroot = client["Root"]
        if not self.contentlocalroot.endswith("/"):
            self.contentlocalroot += '/'

        # client sytax client root with wildcard
        self.contentclientroot = '//' + self.p4.client + '/...'

    def _set_up_content_paths_temp(self, temp_client_name):
        """Set up depot and local paths for both content and P4GF from a TempClient.

        These paths are derived from the client pool's copy of the client view and root.
        If the TempClient values are not set, use setup_content_paths and store the values
        for subsequent calls. This is expected only for TempClients from for_stream().
        """
        # get the TempClient object by name
        temp_client = self._client_pool.get_client_with_name(temp_client_name)

                        # View based temp clients will have the views in the TempClient.
                        # Stream based temp clients will not at time of creation.
                        # This requires another client -o to retrieve the views.
        if not temp_client.client_view_map:
            self._set_up_content_paths()   # get the views and roots from the p4
            temp_client.client_view_map = self.clientmap
            temp_client.client_root     = self.contentlocalroot
            return

        self.clientmap = temp_client.client_view_map
        self.contentlocalroot = temp_client.client_root
        # view can be quite long, report at finer log level
        LOG.debug3("_set_up_content_paths_temp client View: %s and root: %s",
                   self.clientmap, self.contentlocalroot)

        # local syntax client root, force trailing /
        if not self.contentlocalroot.endswith("/"):
            self.contentlocalroot += '/'

        # client sytax client root with wildcard
        self.contentclientroot = '//' + self.p4.client + '/...'

    def __set_up_p4gf_paths(self):
        """Set up depot and local paths for this context.

        These paths are derived from the client root and client view.

        """
        client = self.p4gf.fetch_client()
        self.client_spec_gf = client
        self.gitrootdir = client_spec_to_root(client)
        clientmap_gf = Map(client["View"])
        lhs = clientmap_gf.lhs()
        assert len(lhs) == 1, _('view must contain only one line')
        rpath = clientmap_gf.translate(lhs[0])
        self.gitlocalroot = strip_wild(client_path_to_local(
            rpath, self.p4gf.client, self.gitrootdir))

    def __str__(self):
        return "\n".join(["Git data in Perforce:   " + self.gitdepotroot + "...",
                          "                        " + self.gitlocalroot + "...",
                          "Exported Perforce tree: " + self.contentlocalroot + "...",
                          "                        " + self.contentclientroot,
                          "timezone: " + self.timezone])

    def log_context(self):
        """Dump connection info, client info, directories, all to log category
        'context' as INFO.
        """

        log = logging.getLogger('context')
        if not log.isEnabledFor(logging.INFO):
            return

        # Dump client spec as raw untagged text.
        client_lines_raw = p4gf_p4spec.fetch_client_raw(self.p4, self.p4.client)
        # Strip comment header
        client_lines = [l for l in client_lines_raw if not l.startswith('#')]

        # Dump p4 info, tagged, since that includes more pairs than untagged.
        p4info = p4gf_p4cache.fetch_info(self.p4)
        key_len_max = max(len(k) for k in p4info.keys())
        info_template = NTR('%-{}s : %s').format(key_len_max)

        log.info(info_template, 'P4PORT',     self.p4.port)
        log.info(info_template, 'P4USER',     self.p4.user)
        log.info(info_template, 'P4CLIENT',   self.p4.client)
        log.info(info_template, 'p4gfclient', self.p4gf.client)

        for k in sorted(p4info.keys(), key=str.lower):
            log.info(info_template, k, p4info[k])

        for line in client_lines:
            log.info(line)

    def log_memory_usage(self):
        """Log our memory usage on a regular basis."""
        if LOG.isEnabledFor(logging.DEBUG):
            now = time.time()
            if self._memlog_time and now - self._memlog_time < MEMLOG_HEART_RATE:
                return
            self._memlog_time = now
            LOG.debug(p4gf_log.memory_usage())

    def _convert_path(self, clazz, path):
        """Return a path object that can convert to other formats."""
        return clazz( self.clientmap
                      , self.p4.client
                      , self.contentlocalroot[:-1]  # -1 to strip trailing /
                      , path)

    def depot_path(self, path):
        """Return an object that can convert from depot to other syntax."""
        return self._convert_path(p4gf_path_convert.DepotPath, path)

    def client_path(self, path):
        """Return an object that can convert from client to other syntax."""
        return self._convert_path(p4gf_path_convert.ClientPath, path)

    def gwt_path(self, path):
        """Return an object that can convert from Git work tree to other syntax."""
        return self._convert_path(p4gf_path_convert.GWTPath, path)

    def gwt_to_depot_path(self, gwt_path):
        """Optimized version of ctx.gwt_path(gwt).to_depot().

        Avoid creating the convert and goes straight to P4.Map.translate().

        We call this once for every row in G2PMatrix. This function runs in
        about 60% of ctx.gwt_path(x).to_depot() time. That works out to about 5%
        of total wall clock time for many-file repos such as james.
        """
        gwt_esc = p4gf_util.escape_path(gwt_path)
        client_path = '//{}/'.format(self.p4.client) + gwt_esc
        return self.clientmap.translate(client_path, self.clientmap.RIGHT2LEFT)

    def depot_to_gwt_path(self, depot_path):
        """Optimized version of ctx.depot_path(dp).to_gwt().

        Avoid creating the convert and goes straight to P4.Map.translate().

        We call this once for every row in G2PMatrix. This function runs in
        about 60% of ctx.depot_path(x).to_gwt() time. That works out to about 5%
        of total wall clock time for many-file repos such as james.
        """
        client_path = self.clientmap.translate(depot_path, self.clientmap.LEFT2RIGHT)
        gwt_esc = client_path[3+len(self.p4.client):]
        return p4gf_util.unescape_path(gwt_esc)

    @staticmethod
    def _p4run(p4, numbered_change, run_history, *args, **kwargs):
        """Record the command in history, then perform it."""
        if numbered_change:
            args = numbered_change.add_change_option(*args)
        run_history.append(list(args))
        return p4.run(*args, **kwargs)

    def p4run(self, *args, **kwargs):
        """Run a command, with logging."""
        return self._p4run(self.p4, self.numbered_change, self.p4run_history,
                           *args, **kwargs)

    def p4gfrun(self, *args, **kwargs):
        """Run a command, with logging."""
        return self._p4run(self.p4gf, self.numbered_change_gf, self.p4gfrun_history,
                           *args, **kwargs)

    #
    # switched_to_xxx()
    #
    # Return RAII objects (context managers) to switch p4 connection to a
    # different view on entry and then restore the original client on exit.
    #
    # To make things more efficient, a pool of temporary clients is used,
    # each of which can be set to a different view.  If a client is already
    # set up with the requested view, it will be used.  If no such client exists
    # a new temporary client will be created or an existing temporary repurposed.
    # The returned context manager will switch to the selected temporary
    # client on entry and back to the previous client on exit.
    #
    # No attempt is made to ensure that the same temporary client will be
    # used for different calls with the same view.  Because of this, when using
    # commands which open files (edit, add, delete, etc.) or submit changes, you
    # must be sure to finish what you start before exiting the context manager.
    #

    def switched_to_view_lines(self, view_lines):
        """Return an RAII object to switch p4 connection to a different,
        temporary, client spec, with the requested view lines, then
        restore p4 connection to original client on exit.

        See 'switched_to_xxx()' comment above for more detail.
        """
        return View(self, view_lines=view_lines)

    def switched_to_union(self, branches):
        """Return an RAII object to switch p4 connection to a different,
        temporary, client spec, with the view consisting of the union of all
        listed branches, or if branches is None, all branches in this repo.
        Then restore p4 connection to original client on exit.

        See 'switched_to_xxx()' comment above for more detail.

        :param branches:
            Which branches to union together into the view.
            Required: pass no more than MAX_UNION_BRANCH_CT branches.
            client_view_union() enforces this limit.
        """
        return View(self, view_lines=self.client_view_union(branches))

    def switched_to_branch(self, branch, set_client=True):
        """Return an RAII object to switch p4 connection to a different,
        temporary, client spec, with  a view matching the specified branch,
        then restore p4 connection to original client on exit.

        See 'switched_to_xxx()' comment above for more detail.
        """
        return View(self, branch=branch, set_client=set_client)

    def switched_to_swarm_client(self, branch):
        """Return an RAII object to switch p4 connection to the pre-repo
        permanent swarm client spec, with  a view matching the specified branch,
        then restore p4 connection to original client on exit.

        See 'switched_to_xxx()' comment above for more detail.
        """
        return View(self, branch=branch, use_swarm_client=True)

    def temp_branch(self, create_if_none=True):
        """Retrieve the shared temporary branch associated with this context.

        If create_if_none is False and there is no temporary branch already,
        None will be returned.
        """
        if self._temp_branch is None and create_if_none:
            self._temp_branch = TempP4BranchMapping(p4=self.p4gf)
        return self._temp_branch

    def is_feature_enabled(self, feature):
        """Return whether feature is enabled for this repo.

        Looks in @features section of config, repo first then global.
        If a feature is not set in config it defaults to not enabled.
        """
        return self.repo_config.is_feature_enabled(feature)

    @property
    def preflight_hook(self):
        """Return our preflight hook, creating if necessary."""
        if not self._preflight_hook:
            self._preflight_hook = PreflightHook.from_context(self)
        return self._preflight_hook

    def checkpoint(self, checkpoint_name):
        """Set a timestamp valued counter using serverid, repo, and checkpoint_name."""
        if self.checkpoints_enabled:
            checkpoint_key = p4gf_const.P4GF_P4KEY_CHECKPOINT.format(
                repo_name=self.config.repo_name,
                server_id=self.server_id, name=checkpoint_name)
            now = time.time()
            P4Key.set(self.p4gf, checkpoint_key, now)

    def path_to_key(self, path):
        """If running against a case-insensitive Perforce server,
        then monocase the path so that DIR == dir.

        Returned string is fine to use as a dict key, but do not use
        as input to Perforce or Git. Send the original paths to Perforce
        and Git. Let their own case-preserving-folding code do its work.
        """
                        # server_is_case_sensitive is a tri-state, with
                        # None == "don't know yet." But you should never
                        # get this far without get_timezone_serverversion_case()
                        # setting this to True/False.
        if not self.server_is_case_sensitive:
            return path.lower()
        return path

    def record_push_status_p4key(self, msg, with_push_id=False):
        """Write generic push status message to the current push's status p4key.

        with_push_id is usually left False: write to the generic repo-wide push
        status. Only supply True for push failures, so that we can know "push
        123 failed" long after we've pushed more things on top of it.
        """
        push_id = self.push_id if with_push_id else None
        status_key_name = P4Key.calc_repo_status_p4key_name(self.config.repo_name, push_id)
        P4Key.set(self.p4gf, status_key_name, msg)

    def record_push_failed_p4key(self, exception):
        """Write failure message to the current push's status p4key.

        Set the status key whose name is bound to the push id so that
        it is preserved between pushes and can be requested later.
        """
        msg = _("Push {push_id} failed: {exception}").format(
            push_id=self.push_id, exception=exception)
        self.record_push_status_p4key(msg, with_push_id=False)
        self.record_push_status_p4key(msg, with_push_id=True)

    def record_push_rejected_p4key(self, exception):
        """Write rejected message to the current push's status p4key.

        Set the status key whose name is bound to the push id so that
        it is preserved between pushes and can be requested later.
        """
        msg = _("Push {push_id} rejected: {exception}").format(
            push_id=self.push_id, exception=exception)
        self.record_push_status_p4key(msg, with_push_id=False)
        self.record_push_status_p4key(msg, with_push_id=True)

    def record_push_success_p4key(self):
        """Write success message to the current push's status p4key."""
        msg = _("Push {push_id} completed successfully").format(push_id=self.push_id)
        self.record_push_status_p4key(msg, with_push_id=False)

    def check_branches_map_top_level(self):
        """Check that each branch maps the top level.
        Used to determine that .gitattributes can be mapped."""
        ret = True
        branch_dict = p4gf_branch.dict_from_config(self.repo_config.repo_config, self.p4)
        for b in branch_dict.values():
            b.set_rhs_client(self.p4.client)
        for branch in branch_dict.values():
            if branch.git_branch_name and not branch.deleted:
                gitattributes_path = '//' + self.p4.client + '/.gitattributes'
                if not branch.view_p4map.translate(gitattributes_path,
                        branch.view_p4map.RIGHT2LEFT):
                    ret = False
                    break
        LOG.debug3("check_branches_map_top_level repo:{0} : branch_dict\n{1}".
                format(self.config.repo_name, branch_dict))
        branch_dict = None
        return ret

    def _get_most_recent_change(self, last_seen):
        """Find the most recent change for this repo.

        :param str last_seen: the "last-seen" change for this repo.
        :return: most recent change.
        :rtype: int

        """
        curr_change = 0
        if last_seen == '0':
            last_seen = '1'
        for branch_chunk in self.iter_branch_chunks():
            with self.switched_to_union(branch_chunk):
                path = self.client_view_path()
                # While -s submitted is not required, it is also not wrong,
                # and makes it explicit that we are looking only at
                # submitted changelists. The entire problem with using 'p4
                # counter change' is that it included pending changes.
                r = self.p4run('changes', '-m1', '-e', last_seen, '-s', 'submitted', path)
            s = p4gf_util.first_value_for_key(r, 'change')
            if s and curr_change < int(s):
                curr_change = int(s)
        return curr_change

    def any_changes_since_last_seen(self):
        """Determine if there have been any new changes to this repo."""
        key_name = p4gf_const.P4GF_P4KEY_LAST_SEEN_CHANGE.format(
            repo_name=self.config.repo_name, server_id=p4gf_util.get_server_id())
        last_seen = P4Key.get(self, key_name)
        curr_change = self._get_most_recent_change(last_seen)
        return curr_change != last_seen

    def update_changes_since_last_seen(self):
        """Update the last-seen change for this repository."""
        key_name = p4gf_const.P4GF_P4KEY_LAST_SEEN_CHANGE.format(
            repo_name=self.config.repo_name, server_id=p4gf_util.get_server_id())
        last_seen = P4Key.get(self, key_name)
        curr_change = self._get_most_recent_change(last_seen)
        P4Key.set(self, key_name, str(curr_change))

# -- end class Context --------------------------------------------------------


class View:

    """RAII class for switching client view by temporarily changing current
    P4 connection to use a different client, with the requested view.

    set_client: If false, do not call 'p4 client -i' to change the client view.
    Still set the P4.Map(), just don't touch the client.
    """

    def __init__( self, ctx, *
                , branch     = None
                , view_lines = None
                , set_client = True
                , use_swarm_client = False
                ):
        assert (not branch) or (not view_lines)
        self.ctx                            = ctx
        self.new_lines                      = view_lines
        self.branch                         = branch
        self.save_client                    = self.ctx.p4.client
        self.save_contentclientroot         = self.ctx.contentclientroot
        self.save_contentlocalroot          = self.ctx.contentlocalroot
        self.save_clientmap                 = self.ctx.clientmap
        self.save_client_exclusions_added   = self.ctx.client_exclusions_added
        self.need_restore                   = False
        self.set_client                     = set_client
        self.use_swarm_client               = use_swarm_client
        self.log = LOG.getChild('view')
        self.log.debug2("saved client {}, clientmap {}".format(
            self.save_client, self.save_clientmap))
        self.log.debug2("branch {}, view_lines {}".format(branch, view_lines))

    def __enter__(self):
        # pylint: disable=protected-access
        if not self.set_client:
            self._switch_without_client()
            return

        if self.use_swarm_client:
            self._enter_swarm_client()
            return

        if self.branch:
            if self.branch.stream_name:
                self.log.debug2("__enter__ for stream {}".format(self.branch.stream_name))
                client_name = self.ctx._client_pool.for_stream(self.branch.stream_name)
            else:
                if self.ctx._client_pool.matches_view(self.ctx.p4.client, self.branch.view_lines):
                    self.log.debug2("__enter__ continuing to use client {}; "
                                    "no view change required".format(self.ctx.p4.client))
                    return
                client_name = self.ctx._client_pool.for_view(self.branch.view_lines)
        else:
            if self.ctx._client_pool.matches_view(self.ctx.p4.client, self.new_lines):
                self.log.debug2("__enter__ continuing to use client {}; "
                                "no view change required".format(self.ctx.p4.client))
                return
            client_name = self.ctx._client_pool.for_view(self.new_lines)

        self.need_restore = True
        self.log.debug("__enter__ set need_restore for {}".format(client_name))
        self.ctx.p4.client = client_name
        self.ctx._set_up_content_paths_temp(client_name)
        self.log.debug2("__enter__ switched connection to client {}, clientmap {}".format(
            client_name, self.ctx.clientmap))

    def __exit__(self, _exc_type, _exc_value, _traceback):
        # pylint: disable=protected-access
        if not self.need_restore:
            self.log.debug("__exit__ not need_restore")
            self.log.debug2("__exit__ continuing to use client {}; no view change required".format(
                self.ctx.p4.client))
            return
        if not self.use_swarm_client:
            # done with temp client, return it to the pool
            self.log.debug("__exit__ restoring {}".format(self.ctx.p4.client))
            self.ctx._client_pool.release_client(self.ctx.p4.client)
        else:
            self.log.debug("__exit__ swarm client")

        self.log.debug2("__exit__ restoring connection to client {}, clientmap {}".format(
            self.save_client, self.save_clientmap))
        self.ctx.p4.client                  = self.save_client
        self.ctx.contentclientroot          = self.save_contentclientroot
        self.ctx.contentlocalroot           = self.save_contentlocalroot
        self.ctx.clientmap                  = self.save_clientmap
        self.ctx.client_exclusions_added    = self.save_client_exclusions_added

    def _switch_without_client(self):
        """Switch view without talking to the Perforce server.

        Optimization to avoid per-commit network calls to the Perforce server
        during fast push.

        Context.client_view_path() will produce incorrect results after this,
        since the client isn't switched.
        """
        assert self.branch
        self.ctx.clientmap = self.branch.view_p4map

    def _enter_swarm_client(self):
        """Switch to the swarm client."""
        # pylint: disable=protected-access
        assert self.branch
        swarm_client = p4gf_const.P4GF_SWARM_CLIENT.format(repo_name=self.ctx.config.repo_name)
        assert self.ctx.p4.client == swarm_client
        self.need_restore = True
        new_view_map = p4gf_branch.replace_client_name(self.branch.view_lines,
                                                       self.ctx.config.p4client,
                                                       swarm_client)

        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('_enter_swarm_client() name={} view={}'
                       .format(swarm_client, new_view_map.as_array()))
        else:
            LOG.debug2('_enter_swarm_client() name={}'
                       .format(swarm_client))
        p4gf_p4spec.ensure_spec_values(self.ctx.p4gf,
                                       spec_type='client',
                                       spec_id=swarm_client,
                                       values={'View': new_view_map.as_array(),
                                               'Stream': None})
        self.ctx._set_up_content_paths()
        self.need_restore = True

#! /usr/bin/env python3.3
"""Git Fusion configuration files.

A global configuration file is stored in Perforce:
    //P4GF_DEPOT/p4gf_config

Each individual Git Fusion repo has its own config:
    //P4GF_DEPOT/repos/{repo}/p4gf_config

Files are simple INI format as supported by the configparser module.
"""

from collections import OrderedDict
import configparser
import io
import logging
import sys
import traceback

import p4gf_env_config    # pylint: disable=unused-import
import p4gf_const
import p4gf_create_p4
import p4gf_p4key as P4Key
from   p4gf_l10n import _, NTR
import p4gf_log
import p4gf_path_convert
import p4gf_translate
import p4gf_util

from P4 import P4Exception

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_config")

# No cascade:
#   SECTION_REPO:KEY_READ_ONLY
#   SECTION_GIT_TO_PERFORCE:KEY_READ_PERMISSION_CHECK
#   SECTION_GIT_TO_PERFORCE: KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM
#
# Any of the following options may be overridden in [@repo] of repo config.
#

# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
# [repo-creation]
SECTION_REPO_CREATION      = NTR('repo-creation')
KEY_CHARSET                = NTR('charset')
KEY_GIT_AUTOPACK           = NTR('git-autopack')
KEY_GIT_GC_AUTO            = NTR('git-gc-auto')
KEY_NDPR_ENABLE            = NTR('depot-path-repo-creation-enable')
KEY_NDPR_P4GROUP           = NTR('depot-path-repo-creation-p4group')

# [git-to-perforce]
SECTION_GIT_TO_PERFORCE    = NTR('git-to-perforce')
KEY_CHANGE_OWNER           = NTR('change-owner')
KEY_ENABLE_BRANCH_CREATION = NTR('enable-git-branch-creation')
KEY_ENABLE_MERGE_COMMITS   = NTR('enable-git-merge-commits')
KEY_ENABLE_SWARM_REVIEWS   = NTR('enable-swarm-reviews')
KEY_ENABLE_SUBMODULES      = NTR('enable-git-submodules')
KEY_PREFLIGHT_COMMIT       = NTR('preflight-commit')
KEY_IGNORE_AUTHOR_PERMS    = NTR('ignore-author-permissions')
KEY_READ_PERMISSION_CHECK  = NTR('read-permission-check')
KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM \
                           = NTR('git-merge-avoidance-after-change-num')
KEY_ENABLE_GIT_FIND_COPIES = NTR('enable-git-find-copies')
KEY_ENABLE_GIT_FIND_RENAMES = NTR('enable-git-find-renames')
KEY_ENABLE_FAST_PUSH       = NTR('enable-fast-push')
KEY_JOB_LOOKUP             = NTR('job-lookup')
KEY_NDB_ENABLE             = NTR('depot-branch-creation-enable')
VALUE_NDB_ENABLE_NO        = NTR('no')
VALUE_NDB_ENABLE_EXPLICIT  = NTR('explicit')
VALUE_NDB_ENABLE_ALL       = NTR('all')
KEY_NDB_P4GROUP            = NTR('depot-branch-creation-p4group')
KEY_NDB_DEPOT_PATH         = NTR('depot-branch-creation-depot-path')
VALUE_NDB_DEPOT_PATH_DEFAULT = NTR('//depot/{repo}/{git_branch_name}')
KEY_NDB_VIEW               = NTR('depot-branch-creation-view')
VALUE_NDB_VIEW_DEFAULT     = NTR('... ...')
KEY_FAST_PUSH_WORKING_STORAGE = NTR('fast-push-working-storage')
KEY_USE_SHA1_TO_SKIP_EDIT  = NTR('use-sha1-to-skip-edit')
#                 End block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
    # Python dict.
    # 30% faster than sqlite, but crash if db.rev doesn't fit in memory.
    # And boy howdy does it soak up the memory.
VALUE_FAST_PUSH_WORKING_STORAGE_DICT                  = NTR('memory')
    # Deep undoc: single sqlite table in memory.
    # Useful only when testing performance. Same 'memory' is always better.
VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MEMORY         = NTR('disk.memory')
    # Deep undoc: single sqlite table on disk.
    # Useful only when testing performance. Multiple tables
    # performs better especially for large db.rev counts.
VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_SINGLE_TABLE    = NTR('disk.single')
    # Multiple sqlite tables within a single db file.
    # Reasonable performance and much less memory than Python dict
    # for high db.rev counts.
VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MULTIPLE_TABLES = NTR('disk')
VALUE_DATE_SOURCE_GIT_AUTHOR    = NTR('git-author')
VALUE_DATE_SOURCE_GIT_COMMITTER = NTR('git-pusher')
KEY_MIRROR_MAX_COMMITS_PER_SUBMIT = NTR("gitmirror-max-commits-per-submit")
# [perforce-to-git]
# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
SECTION_PERFORCE_TO_GIT    = NTR('perforce-to-git')
KEY_CHANGELIST_DATE_SOURCE = NTR('changelist-date-source')
VALUE_DATE_SOURCE_P4_SUBMIT     = NTR('perforce-submit-time')
KEY_SUBMODULE_IMPORTS      = NTR('enable-stream-imports')
KEY_CLONE_TO_CREATE_REPO   = NTR('enable-clone-to-create-repo')
KEY_UPDATE_ONLY_ON_POLL    = NTR('update-only-on-poll')
KEY_HTTP_URL               = NTR('http-url')
KEY_SSH_URL                = NTR('ssh-url')
KEY_ENABLE_ADD_COPIED_FROM_PERFORCE = NTR('enable-add-copied-from-perforce')
KEY_ENABLE_GIT_P4_EMULATION = NTR('enable-git-p4-emulation')
# [@features]
SECTION_FEATURES           = NTR('@features')
FEATURE_KEYS = {
}
# [authentication]
SECTION_AUTHENTICATION     = NTR('authentication')
KEY_EMAIL_CASE_SENSITIVITY = NTR('email-case-sensitivity')
KEY_AUTHOR_SOURCE          = NTR('author-source')
# [quota]
SECTION_QUOTA              = NTR('quota')
KEY_COMMIT_LIMIT           = NTR('limit_commits_received')
KEY_FILE_LIMIT             = NTR('limit_files_received')
KEY_SPACE_LIMIT            = NTR('limit_space_mb')
KEY_RECEIVED_LIMIT         = NTR('limit_megabytes_received')

# [undoc]
SECTION_UNDOC              = NTR('undoc')
KEY_ENABLE_CHECKPOINTS     = NTR('enable_checkpoints')
#
# In [@repo] of the per-repo config files only
#
SECTION_REPO               = NTR('@repo')
KEY_DESCRIPTION            = NTR('description')
KEY_FORK_OF_REPO           = NTR('fork-of-repo')
#
# The following may also appear in the branch section...
#
KEY_READ_ONLY              = NTR('read-only')
KEY_FORK_OF_BRANCH_ID      = NTR('fork-of-branch-id')

#
# In the [<branch_id>] section of the per-repo config files only
#
KEY_GIT_BRANCH_NAME        = NTR('git-branch-name')
KEY_VIEW                   = NTR('view')
KEY_STREAM                 = NTR('stream')
KEY_ORIGINAL_VIEW          = NTR('original-view')
KEY_ENABLE_MISMATCHED_RHS  = NTR('enable-mismatched-rhs')
KEY_GIT_BRANCH_DELETED     = NTR('deleted')
KEY_GIT_BRANCH_DELETED_CHANGE  = NTR('deleted-at-change')
KEY_GIT_BRANCH_START_CHANGE    = NTR('start-at-change')
KEY_DEPOT_BRANCH_ID        = NTR('depot-branch-id')
KEY_GIT_LFS_ENABLE         = NTR('git-lfs-enable')
KEY_GIT_LFS_INITIAL_TRACK  = NTR('git-lfs-initial-track')
#                 End block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------
KEY_DEPOT_ROOT             = NTR('depot-root')

# When a feature is ready to turn on all the time, add to this list.
#
# Eventually we'll want to completely remove the flag and any code that tests
# for it, but that's an intrusive code change that risks introducing bugs. Only
# do that near the start of a dev cycle.
#
_FEATURE_ENABLE_FORCE_ON = [
]

# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
VALUE_AUTHOR                = NTR('author')
VALUE_PUSHER                = NTR('pusher')
VALUE_YES                   = NTR('yes')
VALUE_NO                    = NTR('no')
VALUE_NONE                  = NTR('none')
VALUE_GIT_EMAIL             = NTR('git-email')
#                 End block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------
VALUE_GIT_BRANCH_NAME       = NTR('master')
VALUE_GIT_USER              = NTR('git-user')
VALUE_GIT_EMAIL_ACCT        = NTR('git-email-account')

# For p4gf_config files that we write, keep our sections in a stable
# order to avoid unnecessary changes due to nothing more than dict() reordering.
#
# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
SECTION_LIST = [ SECTION_REPO_CREATION
               , SECTION_GIT_TO_PERFORCE
               , SECTION_PERFORCE_TO_GIT
               , SECTION_FEATURES
               , SECTION_AUTHENTICATION
               , SECTION_QUOTA
               , SECTION_UNDOC
               , SECTION_REPO
               ]
#                 End block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------


class ConfigLoadError(RuntimeError):

    """Something went wrong reading a config file."""

    def __init__(self, path, e=None):
        """Init error."""
        msg = _("Missing or empty config file '{path}'.").format(path=path)
        if e:
            msg += "\n{}".format(e)
        LOG.debug(msg)
        RuntimeError.__init__(self, msg)


class ConfigParseError(RuntimeError):

    """Something went wrong parsing a config file."""

    def __init__(self, path, e):
        """Init error."""
        msg = _("Unable to parse config file '{path}'.").format(path=path)
        if e:
            msg += "\n{}".format(e)
        LOG.error(msg)
        RuntimeError.__init__(self, msg)


class ConfigCreateError(Exception):

    """Something went wrong creating a config file from a client or stream."""

    pass


class GlobalConfig:

    """Access to only the global config."""

    _instance = None
    _needs_write = False

    def __init__(self, p4):
        """Ensure the single instance is created."""
        self.init(p4)

    @staticmethod
    def init(p4):
        """Initialize the single instance of the global config ConfigParser.

        If the global config instance not yet been initialized, reads and
        updates any existing config file or creates a new one with default
        settings.
        """
        if GlobalConfig._instance is None:
            defaults = default_config_global()
            try:
                GlobalConfig._instance = _read_config_depot(p4, depot_path_global())
                _transition_old_global(GlobalConfig._instance)
                # ensure the config has everything we expect
                _apply_defaults(GlobalConfig._instance, defaults)
            except ConfigLoadError:
                GlobalConfig._instance = defaults
        return GlobalConfig._instance

    @staticmethod
    def instance(p4=None):
        """Return single instance of global config ConfigParser.

        If not yet initialized and p4 is supplied, initializes before returning.
        """
        if not GlobalConfig._instance and p4:
            GlobalConfig.init(p4)
        return GlobalConfig._instance

    @staticmethod
    def get(section, option, **kwargs):
        """Retrieve a string configuration setting from the named section.

        If the configuration setting was 'none' or the empty string, then
        None is returned, unless fallback is passed. If fallback is passed,
        it will be used in place of missing values.

        """
        value = GlobalConfig._instance.get(section, option, **kwargs)
        if 'fallback' not in kwargs.keys() and _is_none_value(value):
            value = None
        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2("GlobalConfig.get() [{}]/{} => {}".format(section, option, value))
        return value

    @staticmethod
    def getboolean(section, option, **kwargs):
        """Retrieve a boolean configuration setting from the global config.

        Similar in behavior to ConfigParser.getboolean() except that any value
        that is not an accepted boolean value will result in the fallback value
        being returned rather than raising ValueError.

        """
        value = GlobalConfig._instance.get(section, option, **kwargs)
        # pylint: disable=maybe-no-member
        if value and value.lower() in configparser.ConfigParser.BOOLEAN_STATES:
            value = configparser.ConfigParser.BOOLEAN_STATES[value.lower()]
        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2("getboolean() global:[{}]/{} => {}".format(section, option, value))
        return value

    @staticmethod
    def set(section, option, value):
        """Set a config option in the global config (used for testing)."""
        if not GlobalConfig._instance.has_section(section):
            GlobalConfig._instance.add_section(section)
        GlobalConfig._instance.set(section, option, value)

    @staticmethod
    def write_if(p4):
        """If the config changed, write to Perforce.

        If it still matches what's already in Perforce, do nothing.

        Returns True if the file is actually written, else False.
        """
        # Ensure the merge avoidance value is set to something at some
        # point, like just before writing the global config file to the
        # depot, but avoid doing so on every single request.
        sec = GlobalConfig._instance[SECTION_GIT_TO_PERFORCE]
        if sec[KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM] == '0':
            # '0' is not a valid value, and we use that to indicate
            # this option has not been set yet.
            sec[KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM] = _get_merge_avoid_seed(p4)

        depot_path = depot_path_global()
        got_file_content = _print_config_file(p4, depot_path)
        want_file_content = to_text(_comment_header_global(), GlobalConfig._instance)
        if got_file_content == want_file_content:
            return False
        # Let add_depot_file() figure out if the file is new or not
        added = p4gf_util.add_depot_file(p4, depot_path, want_file_content)
        if not added:
            # Apparently it already exists
            p4gf_util.edit_depot_file(p4, depot_path, want_file_content)
        return True


class RepoConfig:

    """Cascading config files."""

    # pylint: disable=too-many-public-methods

    def __init__(self, repo_name=None, p4=None):
        """Initialize empty RepoConfig."""
        self.repo_name = repo_name
        self._repo_config = configparser.ConfigParser(interpolation=None)
        self._repo_config2 = configparser.ConfigParser(interpolation=None)
        self.repo_config_source = None
        self.repo_config2_source = None
        # if p4 provided, ensure that global config is read
        if p4:
            GlobalConfig.init(p4)

        # from_depot_file() sets to file rev# that we printed so that
        # refresh_if() can tell if there's a reason to refresh.
        self._depot_revision = None
        self._depot_revision2 = None

    @property
    def repo_config(self):
        """Return the repository configuration instance."""
        return self._repo_config

    @property
    def repo_config2(self):
        """Return the supplemental repository configuration instance."""
        return self._repo_config2

    def copy_excluding_branches(self):
        """Return a copy of this RepoConfig excluding branch sections."""
        copy = RepoConfig(self.repo_name)
        copy.repo_config_source = "copy_excluding_branches: "+str(self.repo_config_source)
        copy.repo_config2_source = "copy_excluding_branches: "+str(self.repo_config2_source)
        for s in non_branch_section_list(self._repo_config):
            for k, v in self._repo_config[s].items():
                copy.set(s, k, v)
        return copy

    def set_repo_config(self, config, source):
        """Replace repo_config, calling clean_up_parser if needed.

        If config is None, repo_config is set to a new empty ConfigParser.
        """
        clean_up_parser(self._repo_config)
        if config:
            self._repo_config = config
            self.repo_config_source = source
        else:
            self._repo_config = configparser.ConfigParser(interpolation=None)
            self.repo_config_source = None

    def set_repo_config2(self, config, source):
        """Replace repo_config2, calling clean_up_parser if needed.

        If config is None, repo_config2 is set to a new empty ConfigParser.
        """
        clean_up_parser(self._repo_config2)
        if config:
            self._repo_config2 = config
            self.repo_config2_source = source
        else:
            self._repo_config2 = configparser.ConfigParser(interpolation=None)
            self.repo_config2_source = None

    def load_local(self, path):
        """Load repo_config from a local config file.

        Raises ConfigLoadError if the file is missing or
        ConfigParseError if the file is invalid.
        """
        self.set_repo_config(_read_config_local(path), "local: "+path)
        _transition_old_repo(self._repo_config)
        LOG.debug3("read config1:\n{}".format(to_text("", self._repo_config)))
        return self

    def load_local2(self, path):
        """Load repo_config2 from a local config file.

        Raises ConfigLoadError if the file is missing or
        ConfigParseError if the file is invalid.
        """
        self.set_repo_config2(_read_config_local(path), "local: "+path)
        LOG.debug3("read config2:\n{}".format(to_text("", self._repo_config2)))
        return self

    @staticmethod
    def _rev_number(tagged_dict):
        """If 'p4 print' returned a tagged dict with a rev number,
        return that as an integer. If not, return None.
        """
        if tagged_dict and "rev" in tagged_dict:
            return int(tagged_dict["rev"])
        return None

    def load_depot(self, p4):
        """Load repo_config from its depot file.

        Raises ConfigLoadError if the file is missing or
        ConfigParseError if the file is invalid.
        """
        path = depot_path_repo(self.repo_name)
        (config, d) = _read_config_depot_tagged(p4, path)
        self.set_repo_config(config, path)
        self._depot_revision = self._rev_number(d)
        _transition_old_repo(self._repo_config)
        return self

    def load_depot2(self, p4):
        """Load repo_config2 from its depot file.

        Suppresses ConfigLoadError if the file is missing but
        raises ConfigParseError if the file is invalid.
        """
        path = depot_path_repo2(self.repo_name)
        try:
            (config, d) = _read_config_depot_tagged(p4, path)
            self.set_repo_config2(config, path)
            self._depot_revision2 = self._rev_number(d)
        except ConfigLoadError:
            self._depot_revision2 = None
        return self

    def set_defaults(self):
        """Load repo_config with defaults."""
        self._repo_config = default_config_repo(self.repo_name)
        self.repo_config_source = "defaults"
        return self

    @staticmethod
    def make_default(repo_name, p4):
        """Initialize repo_config with default values.

        Tries to load repo_config2 from depot, ignoring any errors.
        """
        return RepoConfig(repo_name, p4).set_defaults().load_depot2(p4)

    @staticmethod
    def from_local_file(repo_name, p4, path):
        """Initialize repo_config from a local config file.

        Tries to load repo_config2 from depot file if one exists.

        Raises ConfigLoadError if the local file is missing or
        ConfigParseError if either file is invalid.
        """
        config = RepoConfig(repo_name, p4).load_depot2(p4).load_local(path)
        LOG.debug3("read config1:\n{}".format(to_text("", config.repo_config)))
        return config

    @staticmethod
    def from_local_files(repo_name, p4, path, path2):
        """
        Initialize from local config and config2 files.

        Raises ConfigLoadError if either file is missing or
        ConfigParseError if either file is invalid.
        """
        config = RepoConfig(repo_name, p4).load_local(path)
        if path2:
            config.load_local2(path2)
        LOG.debug3("read config1:\n{}".format(to_text("", config.repo_config)))
        return config

    def _needs_refresh(self, p4):
        """Do either config or config2 have a new revision in Perforce
        that's later than our current contents?
        """
        current = None
        current2 = None
        try:
            r = p4.run( 'files'
                      , depot_path_repo(self.repo_name)
                      , depot_path_repo2(self.repo_name))
            for d in r:
                if "depotFile" in d and "rev" in d:
                    if d["depotFile"] == depot_path_repo(self.repo_name):
                        current = int(d["rev"])
                    elif d["depotFile"] == depot_path_repo2(self.repo_name):
                        current2 = int(d["rev"])
        except Exception: # pylint: disable=broad-except
            pass
        needs =   current  != self._depot_revision \
               or current2 != self._depot_revision2
        LOG.debug3("_needs_refresh() needs={}  config have={} need={}"
                   "   config2 have={} need={}"
                   .format( needs
                          , self._depot_revision,  current
                          , self._depot_revision2, current2))
        return needs

    def refresh_if(self, p4, create_if_missing=False):
        """If the Perforce copy of our config file has changed since the
        last time we ran from_depot_file(), run it again.
        """
        if self._needs_refresh(p4):
            self._from_depot_file(self.repo_name, p4, create_if_missing)

    @staticmethod
    def from_depot_file(repo_name, p4, create_if_missing=False):
        """Initialize from a config file stored in the depot.

        If create_if_missing is False, will raise ConfigLoadError if the file is
        missing or invalid.
        """
        config = RepoConfig(repo_name, p4)
        config._from_depot_file(repo_name, p4, create_if_missing) # pylint: disable=protected-access
        return config

    def _from_depot_file(self, repo_name, p4, create_if_missing):
        """Load config and config2 from repo."""
        self.load_depot2(p4)
        try:
            self.load_depot(p4)
            LOG.debug3("read config1:\n{}".format(to_text("", self.repo_config)))
        except ConfigLoadError:
            if create_if_missing:
                LOG.debug('config file not found for {}, creating default'.format(repo_name))
                self.set_defaults()
                LOG.debug3('created config1:\n{}'.format(to_text("", self.repo_config)))
            else:
                raise

    @staticmethod
    def from_template_client(repo_name, p4, client_spec, client_name):
        """Initialize using a Perforce client to define the view.

        The client view becomes the view for the master branch.

        :param repo_name: name of the repository.
        :param p4: used to get config instance and generate UUID.
        :param client_spec: client specification containing valid view.
        :param client_name: used for reporting.

        """
        config = RepoConfig(repo_name, p4)
        try:
            view = client_spec.get('View')
            client_less = p4gf_path_convert.convert_view_to_no_client_name(view)
            repo_config = default_config_repo(repo_name)
            sec = p4gf_util.uuid(p4)
            repo_config.add_section(sec)
            repo_config.set(sec, KEY_GIT_BRANCH_NAME, VALUE_GIT_BRANCH_NAME)
            repo_config.set(sec, KEY_VIEW, client_less)
            config.set_repo_config(repo_config, "client:" + client_name)
            return config

        except:
            msg = _("Error initializing config from client {client_name}") \
                .format(client_name=client_name)
            LOG.exception(msg)
            raise ConfigCreateError(msg)

    @staticmethod
    def from_stream(repo_name, p4, stream_name):
        """Initialize using a Perforce stream to define the view.

        View is used to create a single for Git branch: master.

        Raises ConfigCreateError if the stream is missing or not suitable.
        """
        config = RepoConfig(repo_name, p4)
        try:
            repo_config = default_config_repo(repo_name)
            sec = p4gf_util.uuid(p4)
            repo_config.add_section(sec)
            repo_config.set(sec, KEY_GIT_BRANCH_NAME, VALUE_GIT_BRANCH_NAME)
            repo_config.set(sec, KEY_STREAM, stream_name)
            config.set_repo_config(repo_config, "stream:"+stream_name)
            return config

        except:
            msg = _("Error initializing config from stream '{stream_name}'") \
                .format(stream_name=stream_name)
            LOG.exception(msg)
            raise ConfigCreateError(msg)

    def set(self, section, option, value):
        """Set an option in the repo config, creating the section first if necessary."""
        if not self._repo_config.has_section(section):
            self._repo_config.add_section(section)
        self._repo_config.set(section, option, value)

    def _get(self, section, option, **kwargs):
        """Retrieve a configuration setting from the named section.

        First looks in the named section of the repository configuration,
        then in the [@repo] section of the repository configuration,
        finally, in the named section of the global configuration.
        The global settings are always populated with sensible defaults
        so a value will be returned.

        Empty values are ok but missing sections or options will raise.

        Pass fallback=whatever to avoid raising configparser.NoOptionError.

        Any other keyworded arguments will be ignored.
        """
        kwargs_no_fallback = {k: v for k, v in kwargs.items() if k != 'fallback'}
        if section == SECTION_FEATURES:
            try:
                return self._repo_config.get(section, option, **kwargs_no_fallback)
            except (configparser.NoSectionError, configparser.NoOptionError):
                return GlobalConfig.instance().get(section, option, **kwargs)

        if section == SECTION_REPO:
            return self._repo_config.get(section, option, **kwargs)

        if section in self.branch_sections():
            try:
                return self._repo_config.get(section, option, **kwargs_no_fallback)
            except configparser.NoOptionError:
                return self._repo_config.get(SECTION_REPO, option, **kwargs)

        # section must be a global config section
        try:
            return self._repo_config.get(SECTION_REPO, option, **kwargs_no_fallback)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return GlobalConfig.instance().get(section, option, **kwargs)

    def get(self, section, option, **kwargs):
        """Retrieve a string configuration setting from the named section.

        If the configuration setting was 'none' or the empty string, then
        None is returned, unless fallback is passed. If fallback is passed,
        it will be used in place of missing values.

        """
        value = self._get(section, option, **kwargs)
        if 'fallback' not in kwargs.keys() and _is_none_value(value):
            value = None
        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2("get() {}:[{}]/{} => {}".format(self.repo_name,
                                                       section, option, value))
        return value

    def getint(self, section, option, **kwargs):
        """Retrieve an integer configuration setting for the named repo.

        If the configuration setting is 'none', '' or None, then returns None.
        Any other non-integer value will result in raising ValueError.
        """
        value = self._get(section, option, **kwargs)
        if value is None or _is_none_value(value):
            value = None
        else:
            value = int(value)
        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2("getint() {}:[{}]/{} => {}".format(self.repo_name,
                                                          section, option, value))
        return value

    def getboolean(self, section, option, **kwargs):
        """Retrieve a boolean configuration setting for the named repo.

        Similar in behavior to ConfigParser.getboolean() except that any value
        that is not an accepted boolean value will result in the fallback value
        being returned rather than raising ValueError.
        """
        value = self._get(section, option, **kwargs)
        # pylint: disable=maybe-no-member
        if value and value.lower() in configparser.ConfigParser.BOOLEAN_STATES:
            value = configparser.ConfigParser.BOOLEAN_STATES[value.lower()]
        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2("getboolean() {}:[{}]/{} => {}".format(self.repo_name,
                                                              section, option, value))
        return value

    def sections(self):
        """Return a list of sections in the repo config."""
        return self._repo_config.sections()

    def branch_sections(self):
        """Return a list of section names, one for each branch mapping section.

        Not every returned section name is guaranteed to be a correct and
        complete branch definition. Use p4gf_branch.Branch.from_config() to
        figure that out.
        """
        return [s for s in self._repo_config.sections()
                if is_branch_section_name(s)]

    def branch_sections2(self):
        """Return a list of branch section names from repo_config2.

        Not every returned section name is guaranteed to be a correct and
        complete branch definition. Use p4gf_branch.Branch.from_config() to
        figure that out.  Of course, all sections in repo_config2 should be
        valid branch definitions.
        """
        return [s for s in self._repo_config2.sections()
                if is_branch_section_name(s)]

    def section_for_branch(self, branch_name):
        """Return the name of the section containing the named branch or None."""
        for section in self._repo_config.sections():
            if section == SECTION_REPO:
                continue
            if KEY_GIT_BRANCH_NAME not in self._repo_config.options(section):
                continue
            if branch_name != self._repo_config.get(section, KEY_GIT_BRANCH_NAME):
                continue
            return section
        return None

    def remove_section(self, section):
        """Remove the section from the repo config."""
        return self._repo_config.remove_section(section)

    def has_option(self, section, option):
        """Check if an option is defined.

        Return True if the given section exists, and contains the given option.
        Otherwise return False.
        """
        return self._repo_config.has_option(SECTION_REPO, option) or \
            self._repo_config.has_option(section, option)

    def is_feature_enabled(self, feature):
        """Check if a feature is enabled in a repo's config.

        Default to False if not set.
        """
        if feature in _FEATURE_ENABLE_FORCE_ON:
            return True
        return self.getboolean(SECTION_FEATURES, feature)

    def _copy_repo_config_remove_original_view(self):
        """Return a copy of the repo_config and if exist remove any KEY_ORIGINAL_VIEW.

        KEY_ORIGINAL_VIEW should never be written to repo_config.
        The copy is for writing to Perforce.
        """

        config_copy = from_dict(self._repo_config)
        for section in config_copy.sections():
            # remove_option returns False if no option
            config_copy.remove_option(section, KEY_ORIGINAL_VIEW)
        return config_copy

    def write_repo_if(self, p4=None, ctx=None, client=None):
        """If the config has changed, write to Perforce.

        If it still matches what's already in Perforce, do nothing.
        """
        if LOG.isEnabledFor(logging.DEBUG3):
            tb = traceback.format_stack()
            LOG.debug3("write_repo_if called from:\n{}".format("\n".join(tb)))
        p4 = p4 or ctx.p4gf
        depot_path = depot_path_repo(self.repo_name)
        got_file_content = _print_config_file(p4, depot_path)
        # get a copy of the config with KEY_ORIGINAL_VIEW options removed
        repo_config_copy = self._copy_repo_config_remove_original_view()
        want_file_content = to_text(_comment_header_repo(), repo_config_copy)
        if got_file_content == want_file_content:
            return False
        LOG.debug3("writing config1 with:\n{}".format(want_file_content))
        # Let add_depot_file() figure out if the file is new or not
        added = p4gf_util.add_depot_file(p4, depot_path, want_file_content,
                                         client_spec=client, filetype='text')
        if not added:
            # Apparently it already exists
            p4gf_util.edit_depot_file(p4, depot_path, want_file_content, client)
        return True

    def write_repo_local(self, path):
        """Unconditionally write to the file."""
        # get a copy of the config with KEY_ORIGINAL_VIEW options removed
        repo_config_copy = self._copy_repo_config_remove_original_view()
        file_content = to_text(_comment_header_repo(), repo_config_copy)
        with open(path, 'w') as f:
            f.write(file_content)

    def write_repo2_if(self, p4, client=None):
        """If the config has changed, write to Perforce.

        If it still matches what's already in Perforce, do nothing.
        """
        depot_path = depot_path_repo2(self.repo_name)
        got_file_content = _print_config_file(p4, depot_path)
        want_file_content = to_text("", self._repo_config2)
        if not want_file_content.strip():
            LOG.debug('write_repo2_if nothing to write')
            return False
        if got_file_content == want_file_content:
            return False
        LOG.debug3("writing config2 with:\n{}".format(want_file_content))
        # Let add_depot_file() figure out if the file is new or not
        added = p4gf_util.add_depot_file(p4, depot_path, want_file_content,
                                         client_spec=client, filetype='text')
        if not added:
            # Apparently it already exists
            p4gf_util.edit_depot_file(p4, depot_path, want_file_content, client)
        return True

    def write_repo2_local(self, path):
        """Unconditionally write to the file."""
        file_content = to_text("", self._repo_config2)
        with open(path, 'w') as f:
            f.write(file_content)

    def create_default_for_context(self, ctx, charset):
        """If the config does not exist in p4, create using context's client.

        The client must already exist (p4gf_context.create_p4_client() must
        have already succeeded) so that we can read the view from that client
        and record it as the view mapping for git branch 'master'.
        """
        if p4gf_util.depot_file_exists(ctx.p4gf, depot_path_repo(self.repo_name)):
            return

        client_view = ctx.clientmap.as_array()
        client = ctx.p4.fetch_client()
        if 'Stream' in client:
            self._repo_config = default_config_repo_for_stream(
                ctx.p4gf, self.repo_name, client['Stream'])
        else:
            self._repo_config = default_config_repo_for_view(
                ctx.p4gf, self.repo_name, client_view)
        if charset:
            self._repo_config.set(SECTION_REPO, KEY_CHARSET, charset)
        self.write_repo_if(ctx=ctx)


def depot_path_global():
    """Return path to the global config file."""
    return p4gf_const.P4GF_CONFIG_GLOBAL.format(P4GF_DEPOT=p4gf_const.P4GF_DEPOT)


def depot_path_repo(repo_name):
    """Return the path to a repo's config file."""
    return p4gf_const.P4GF_CONFIG_REPO.format(P4GF_DEPOT=p4gf_const.P4GF_DEPOT,
                                              repo_name=repo_name)


def depot_path_repo2(repo_name):
    """Return the path to a repo's lightweight branch config file."""
    return p4gf_const.P4GF_CONFIG_REPO2.format(P4GF_DEPOT=p4gf_const.P4GF_DEPOT,
                                               repo_name=repo_name)


def _parse_config(contents, file_path):
    """Parse the contents into a ConfigParser instance.

    Uses file_path only if needed to format an error message.

    Raises a ConfigParseError if parsing fails.
    """
    try:
        config = configparser.ConfigParser(interpolation=None)
        config.read_string(contents)
        return config

    except configparser.Error as e:
        raise ConfigParseError(file_path, e)


def _read_config_local(file_path):
    """Read the named local file into a ConfigParser instance.

    Raises a ConfigLoadError or ConfigParseError if reading or parsing fails.
    """
    try:
        with open(file_path, 'r') as f:
            contents = f.read()

    except OSError as e:
        raise ConfigLoadError(file_path, e)

    return _parse_config(contents, file_path)


def _read_config_depot(p4, file_path):
    """Create a ConfigParser and load a Perforce file into it.

    p4 print a config file, parse it into a ConfigParser instance,
    return that ConfigParser instance.

    Raises a ConfigLoadError or ConfigParseError if reading or parsing fails.
    """
    (config, _d) = _read_config_depot_tagged(p4, file_path)
    return config

def _read_config_depot_tagged(p4, file_path):
    """Create a ConfigParser and load a Perforce file into it.

    p4 print a config file, parse it into a ConfigParser instance,
    return that ConfigParser instance.

    Raises a ConfigLoadError or ConfigParseError if reading or parsing fails.

    Returns
    * ConfigParser instance
    * 'p4 print' tagged dict
    """
    try:
        (contents, d) = _print_config_file_tagged(p4, file_path)
        if contents is None:
            raise ConfigLoadError(file_path)
    except P4Exception as e:
        raise ConfigLoadError(file_path, e=e)

    return (_parse_config(contents, file_path), d)


def _apply_defaults(config, defaults):
    """Ensure the given config has all of the desired options.

    :param config: instance of ConfigParser, modified in place.
    :param defaults: ConfigParser of default values.
    """
    for section in defaults.sections():
        if not config.has_section(section):
            config.add_section(section)
        for option in defaults.options(section):
            if not config.has_option(section, option):
                config.set(section, option, defaults.get(section, option))


def _rename_config_section(parser, old_sect, new_sect):
    """Move all options from one section to another.

    :param parser: instance of ConfigParser, modified in place.
    :param old_sect: name of old section; will be removed.
    :param new_sect: name of new section, populated from old section.
    """
    parser.add_section(new_sect)
    for option in parser.options(old_sect):
        parser.set(new_sect, option, parser.get(old_sect, option))
    parser.remove_section(old_sect)


def _transition_old_global(config):
    """Transition old settings to the new names in the given config.

    :param config: instance of ConfigParser, modified in place.
    """
    if not config.has_section(SECTION_REPO_CREATION):
        old_sect = NTR('p4gf-repo-creation')
        if config.has_section(old_sect):
            _rename_config_section(config, old_sect, SECTION_REPO_CREATION)
        else:
            config.add_section(SECTION_REPO_CREATION)
    if not config.has_section(SECTION_GIT_TO_PERFORCE):
        config.add_section(SECTION_GIT_TO_PERFORCE)
    if not config.has_option(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_BRANCH_CREATION):
        value = config.get(SECTION_REPO_CREATION, 'enable-branch-creation',
                           fallback=VALUE_YES)
        config.set(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_BRANCH_CREATION, value)
        config.remove_option(SECTION_REPO_CREATION, 'enable-branch-creation')
    if not config.has_option(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_MERGE_COMMITS):
        value = config.get(SECTION_REPO_CREATION, 'enable-branch-creation',
                           fallback=VALUE_YES)
        config.set(SECTION_GIT_TO_PERFORCE, KEY_ENABLE_MERGE_COMMITS, value)
        config.remove_option(SECTION_REPO_CREATION, 'enable-branch-creation')
    # Remove the old settings so validation can pass.
    config.remove_option(SECTION_REPO_CREATION, 'enable-branch-creation')
    return config


def _transition_old_repo(config):
    """Transition old settings to the new names in the given config.

    :param config: instance of ConfigParser, modified in place.
    """
    if config.has_section(SECTION_REPO):
        if config.has_option(SECTION_REPO, 'enable-branch-creation'):
            value = config.get(SECTION_REPO, 'enable-branch-creation')
            config.remove_option(SECTION_REPO, 'enable-branch-creation')
            if not config.has_option(SECTION_REPO, KEY_ENABLE_BRANCH_CREATION):
                config.set(SECTION_REPO, KEY_ENABLE_BRANCH_CREATION, value)
            if not config.has_option(SECTION_REPO, KEY_ENABLE_MERGE_COMMITS):
                config.set(SECTION_REPO, KEY_ENABLE_MERGE_COMMITS, value)
    return config


def create_file_repo_from_config(ctx, repo_name, config):
    """Create the config file for a repo from the given config."""
    file_content = file_content_repo(config)
    depot_path = depot_path_repo(repo_name)
    _add_file(ctx.p4gf, depot_path, file_content)


def _add_file(p4, depot_path, file_content):
    """Aadd a config file to Perforce using the Git Fusion object client."""
    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3('_add_file {}'.format("".join(traceback.format_stack())))
    return p4gf_util.add_depot_file(p4, depot_path=depot_path, file_content=file_content)


def _print_config_file(p4, depot_path):
    """Return a config file's content as a string."""
    (s, _d) = _print_config_file_tagged(p4, depot_path)
    return s

def _print_config_file_tagged(p4, depot_path):
    """Return a config file's content as a string.

    Returns a tuple of
    * config content as string
    * 'p4 print' tagged dict with printed file's Perforce info (rev number!)
    """
    (b, d) = p4gf_util.print_depot_path_raw_tagged(p4, depot_path)
    if b:
        s = b.decode()    # as UTF-8
    else:
        s = None
    return (s, d)


def write_repo_if(p4, client, repo_name, config):
    """If the config has changed, write to Perforce.

    If it still matches what's already in Perforce, do nothing.
    """
    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3('write_repo_if {}'.format("".join(traceback.format_stack())))
    depot_path = depot_path_repo(repo_name)
    got_file_content = _print_config_file(p4, depot_path)
    want_file_content = file_content_repo(config)
    if got_file_content == want_file_content:
        return
    # Let add_depot_file() figure out if the file is new or not
    added = p4gf_util.add_depot_file(p4, depot_path, want_file_content, client)
    if not added:
        # Apparently it already exists
        p4gf_util.edit_depot_file(p4, depot_path, want_file_content, client)


def file_content_repo(config):
    """Convert a config object to file content that we'd write to Perforce."""
    return to_text(_comment_header_repo(), config)


def _comment_header_global():
    """Return the header text to go at the top of a new global config file."""
    header = p4gf_util.read_bin_file(NTR('p4gf_config.global.txt'))
    if header is False:
        sys.stderr.write(_("no 'p4gf_config.global.txt' found\n"))
        header = _("# Missing p4gf_config.global.txt file!")
    return header


def _comment_header_repo():
    """Return the header text to go at the top of a new per-repo config file."""
    header = p4gf_util.read_bin_file(NTR('p4gf_config.repo.txt'))
    if header is False:
        sys.stderr.write(_("no 'p4gf_config.repo.txt' found\n"))
        header = _('# Missing p4gf_config.repo.txt file!')
    return header


def _get_p4_charset():
    """Retreive the value for P4CHARSET, or return 'utf8' if not set."""
    p4 = p4gf_create_p4.create_p4(connect=False)
    charset = p4.env('P4CHARSET')
    if (not charset) or (charset == ''):
        charset = 'utf8'
    return charset


def _get_merge_avoid_seed(p4):
    """Return the current "change" counter, as a string.

    Return "1" if no changelists yet.
    """
    counter = P4Key.get_counter(p4, 'change')
    if counter == "0":
        return "1"
    return counter


def _is_none_value(value):
    """Return True if the value represents 'none' in some manner."""
    return str(value).lower() == VALUE_NONE or value == ''


def default_config_global():
    """Return a ConfigParser instance loaded with default values."""
    # pylint: disable=too-many-statements
    config = configparser.ConfigParser(interpolation=None)

    config.add_section(SECTION_REPO_CREATION)
    config.set(SECTION_REPO_CREATION, KEY_CHARSET, _get_p4_charset())
    config.set(SECTION_REPO_CREATION, KEY_GIT_AUTOPACK, VALUE_YES)
    config.set(SECTION_REPO_CREATION, KEY_GIT_GC_AUTO, VALUE_NONE)
    config.set(SECTION_REPO_CREATION, KEY_NDPR_ENABLE, VALUE_NO)
    config.set(SECTION_REPO_CREATION, KEY_NDPR_P4GROUP, VALUE_NONE)

    config.add_section(SECTION_GIT_TO_PERFORCE)
    # Zig thinks it's clearer to address the section, not the configparser.
    # Maybe do this for other sections with 3+ entries.
    # (Zig also things tabular code should stay tabular, but he'll let
    #  pep8 fans overrule him here.)
    sec = config[SECTION_GIT_TO_PERFORCE]
    sec[KEY_CHANGE_OWNER] = VALUE_AUTHOR
    sec[KEY_ENABLE_BRANCH_CREATION] = VALUE_YES
    sec[KEY_ENABLE_SWARM_REVIEWS] = VALUE_YES
    sec[KEY_ENABLE_MERGE_COMMITS] = VALUE_YES
    sec[KEY_ENABLE_SUBMODULES] = VALUE_YES
    sec[KEY_PREFLIGHT_COMMIT] = ''
    sec[KEY_IGNORE_AUTHOR_PERMS] = VALUE_NO
    sec[KEY_READ_PERMISSION_CHECK] = ''
    sec[KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM] = '0'
    sec[KEY_ENABLE_GIT_FIND_COPIES] = VALUE_NO
    sec[KEY_ENABLE_GIT_FIND_RENAMES] = VALUE_NO
    sec[KEY_ENABLE_FAST_PUSH] = VALUE_YES
    sec[KEY_JOB_LOOKUP] = VALUE_NONE
    sec[KEY_NDB_ENABLE] = VALUE_NDB_ENABLE_NO
    sec[KEY_NDB_P4GROUP] = VALUE_NONE
    sec[KEY_NDB_DEPOT_PATH] = VALUE_NDB_DEPOT_PATH_DEFAULT
    sec[KEY_NDB_VIEW] = VALUE_NDB_VIEW_DEFAULT
    sec[KEY_CHANGELIST_DATE_SOURCE] = VALUE_DATE_SOURCE_P4_SUBMIT
    sec[KEY_MIRROR_MAX_COMMITS_PER_SUBMIT] = '10000'
    sec[KEY_USE_SHA1_TO_SKIP_EDIT] = VALUE_YES

    config.add_section(SECTION_PERFORCE_TO_GIT)
    sec = config[SECTION_PERFORCE_TO_GIT]
    sec[KEY_SUBMODULE_IMPORTS] = VALUE_NO
    sec[KEY_CLONE_TO_CREATE_REPO] = VALUE_YES
    sec[KEY_UPDATE_ONLY_ON_POLL] = VALUE_NO
    sec[KEY_GIT_LFS_ENABLE] = VALUE_NO
    sec[KEY_GIT_LFS_INITIAL_TRACK] = VALUE_NONE
    sec[KEY_HTTP_URL] = VALUE_NONE
    sec[KEY_SSH_URL] = VALUE_NONE
    sec[KEY_ENABLE_ADD_COPIED_FROM_PERFORCE] = VALUE_YES
    sec[KEY_ENABLE_GIT_P4_EMULATION] = VALUE_NO

    config.add_section(SECTION_FEATURES)
    for key in FEATURE_KEYS.keys():
        config.set(SECTION_FEATURES, key, "False")

    config.add_section(SECTION_AUTHENTICATION)
    config.set(SECTION_AUTHENTICATION, KEY_EMAIL_CASE_SENSITIVITY, VALUE_NO)
    config.set(SECTION_AUTHENTICATION, KEY_AUTHOR_SOURCE, VALUE_GIT_EMAIL)

    config.add_section(SECTION_QUOTA)
    config.set(SECTION_QUOTA, KEY_COMMIT_LIMIT, '0')
    config.set(SECTION_QUOTA, KEY_FILE_LIMIT, '0')
    config.set(SECTION_QUOTA, KEY_SPACE_LIMIT, '0')
    config.set(SECTION_QUOTA, KEY_RECEIVED_LIMIT, '0')

    config.add_section(SECTION_UNDOC)
    config.set(SECTION_UNDOC, KEY_ENABLE_CHECKPOINTS, VALUE_NO)

    return config


def default_config_repo(repo_name):
    """Create a ConfigParser loaded with default values for a single repo.

    :param repo_name: name of the repository.

    Default values for a repo include a placeholder description in the
    [@repo] section.
    """
    config = configparser.ConfigParser(interpolation=None)
    config.add_section(SECTION_REPO)
    config.set(SECTION_REPO, KEY_DESCRIPTION, _("Created from '{repo_name}'")
               .format(repo_name=repo_name))
    return config


def default_config_repo_for_view(p4, repo_name, view):
    """Return a ConfigParser instance loaded with default values for a repo.

    Uses single view as the view for a single Git branch: master.
    """
    client_less = p4gf_path_convert.convert_view_to_no_client_name(view)
    return default_config_repo_for_view_plain(p4, repo_name, client_less)


def default_config_repo_for_view_plain(p4, repo_name, view):
    """Construct a ConfigParser using the client-less view.

    Return a ConfigParser instance loaded with default values for a
    single repo, using single view as the view for a single for Git
    branch: master.
    """
    config = default_config_repo(repo_name)
    sec = p4gf_util.uuid(p4)
    config.add_section(sec)
    config.set(sec, KEY_GIT_BRANCH_NAME, VALUE_GIT_BRANCH_NAME)
    config.set(sec, KEY_VIEW, view)
    return config


def default_config_repo_for_stream(p4, repo_name, stream_name):
    """Return a ConfigParser instance loaded with default values for a repo.

    Uses a stream to define the view for a single for Git branch: master.
    """
    config = default_config_repo(repo_name)
    sec = p4gf_util.uuid(p4)
    config.add_section(sec)
    config.set(sec, KEY_GIT_BRANCH_NAME, VALUE_GIT_BRANCH_NAME)
    config.set(sec, KEY_STREAM, stream_name)
    return config


def to_text(comment_header, config):
    """Produce a single string with a comment header and a ConfigParser.

    This text is suitable for writing to file.
    """
    out = io.StringIO()
    out.write(comment_header)
    config.write(out)
    file_content = out.getvalue()
    out.close()
    return file_content


def to_dict(config):
    """Convert the given ConfigParser object to an OrderedDict.

    Note that it is critical to maintain ordering of sections because we
    treat the first branch differently.

    It is not critical but still nice to not rewrite the config file with
    only reordering of options within sections.
    """
    result = OrderedDict()
    for section in config:
        if section == configparser.DEFAULTSECT:
            continue
        sect = OrderedDict()
        for option in config[section]:
            sect[option] = config[section][option]
        result[section] = sect
    return result


def from_dict(dikt):
    """Convert the given map to a ConfigParser instance."""
    config = configparser.ConfigParser(interpolation=None)
    config.read_dict(dikt)
    return config


def is_branch_section_name(section_name):
    """Check if named section name is a branch section."""
    if section_name.startswith('@'):
        return False
    if section_name in SECTION_LIST:
        return False
    return True


def branch_section(config, branch_id):
    """Return the config section name for the BranchId.

    Create the section if it does not already exist.

    branch_id must be of type BranchId (CaseHandlingString)
    keys in config are of type str
    """
                        # +++ The O(n) loop below causes O(n^2 branch count)
                        # slowness when called from an O(n) outer loop, such as
                        # when writing a config2 file out at the  end of a
                        # push or pull.
                        #
                        # Behave O(1) when case matches and there's a hit,
                        # which is most of the time.
                        #
    as_str = str(branch_id)
    if config.has_section(as_str):
        return as_str

    for s in config.sections():
        if branch_id == s:
            return s
    s = as_str
    config.add_section(s)
    return s


def branch_section_list(config):
    """Return a list of section names, one for each branch mapping section.

    Not every returned section name is guaranteed to be a correct and complete
    branch definition. Use p4gf_branch.Branch.from_config() to figure that out.
    """
    return [s for s in config.sections()
            if is_branch_section_name(s)]


def non_branch_section_list(config):
    """Return a list of section names, one for each non-branch section."""
    return [s for s in config.sections()
            if not is_branch_section_name(s)]


def clean_up_parser(config):
    """Break the reference cycles in the ConfigParser instance.

    This is necessary so the object can be garbage collected properly. The
    config instance should not be used after calling this function.
    """
    # Remove the reference cycle in each section
    sections = config.sections()
    # The default section is a special case
    sections.append(config.default_section)
    for section in sections:
        config._proxies[section]._parser = None     # pylint: disable=protected-access


def create_from_12x_gf_client_name(p4, gf_client_name):
    """Upgrade from Git Fusion 12.x.

    Given the name of an existing Git Fusion 12.x client spec
    "git-fusion-{repo-name}", copy its view into a Git Fusion 13.1 p4gf_config
    file, add and submit that file to Perforce.

    NOP if that p4gf_config file already exists.
    """
    # Extract repo_name from client spec's name.
    assert gf_client_name.startswith(p4gf_const.P4GF_CLIENT_PREFIX)
    repo_name = gf_client_name[len(p4gf_const.P4GF_CLIENT_PREFIX):]

    # NOP if repo's p4gf_config already exists and is not deleted at head.
    depot_path = depot_path_repo(repo_name)
    if p4gf_util.depot_file_exists(p4, depot_path):
        return

    # Extract View lines from client spec, use them to create a new config.
    client_spec = p4.fetch_client(gf_client_name)
    view = client_spec['View']
    config = default_config_repo_for_view(p4, repo_name, view)

    # Write config to Perforce.
    config_file_content = file_content_repo(config)
    depot_path = depot_path_repo(repo_name)
    _add_file(p4, depot_path, config_file_content)


def get_view_lines(config, key):
    """Get a view value and return it as a list of view lines.

    Common code to deal with blank first lines (happens a lot
    in human-authored configs) and to force a list.
    """
    view_lines = config.get(key)
    return to_view_lines(view_lines)


def to_view_lines(view_lines):
    """Convert view_lines to a list of strings, with no blank lines.

    It is common for human-authored configs to contain blank lines, so strip
    them out.
    """
    if isinstance(view_lines, str):
        view_lines = view_lines.splitlines()
    # Common: first line blank, view starts on second line.
    if view_lines and not len(view_lines[0].strip()):
        del view_lines[0]
    return view_lines


def configurable_features():
    """Return sorted list of configurable features.

    This list does not include any features which are forced on.
    Suitable for producing @features output.
    """
    return sorted([key for key in FEATURE_KEYS.keys() if key not in _FEATURE_ENABLE_FORCE_ON])


def _set_option(p4, arg_value, repo_name):
    """Set the option described by the argument value.

    :type arg_value: str
    :param arg_value: [section]/option=value

    :type repo_name: str
    :param repo_name: name of repository or None for global

    :return: ConfigParser instance.

    """
    assert '/' in arg_value, _('malformed --set argument, missing /, see --help')
    section, rest = arg_value.split('/', 1)
    assert '=' in rest, _('malformed --set argument, missing =, see --help')
    option, value = rest.split('=', 1)
    if repo_name:
        cfg = RepoConfig.from_depot_file(repo_name, p4)
        cfg.set(section, option, value)
        client = p4.fetch_client()
        cfg.write_repo_if(p4, client)
        LOG.info('set {}/{}={} in {}/p4gf_config'.format(section, option, value, repo_name))
        return cfg.repo_config
    else:
        cfg = GlobalConfig.instance(p4)
        GlobalConfig.set(section, option, value)
        GlobalConfig.write_if(p4)
        LOG.info('set {}/{}={} in global p4gf_config'.format(section, option, value))
        return cfg


def _unset_option(p4, arg_value, repo_name):
    """Remove the option described by the argument value.

    :type arg_value: str
    :param arg_value: [section]/option

    :type repo_name: str
    :param repo_name: name of repository or None for global

    :return: ConfigParser instance.

    """
    assert '/' in arg_value, _('malformed --unset argument, missing /, see --help')
    section, option = arg_value.split('/', 1)
    if repo_name:
        cfg = RepoConfig.from_depot_file(repo_name, p4)
        cfg.repo_config.remove_option(section, option)
        client = p4.fetch_client()
        cfg.write_repo_if(p4, client)
        LOG.info('removed {}/{} in {}/p4gf_config'.format(section, option, repo_name))
        return cfg.repo_config
    else:
        cfg = GlobalConfig.instance(p4)
        cfg.remove_option(section, option)
        GlobalConfig.write_if(p4)
        LOG.info('removed {}/{} in global p4gf_config'.format(section, option))
        return cfg


def main():
    """Parse the command-line arguments and print a configuration."""
    p4gf_util.has_server_id_or_exit()
    p4 = p4gf_create_p4.create_p4_temp_client()
    if not p4:
        sys.exit(1)
    desc = _("""Display the effective global or repository configuration.
All comment lines are elided and formatting is normalized per the
default behavior of the configparser Python module.
The default configuration options will be produced if either of the
configuration files is missing.
""")
    epilog = """Configuration settings in a repository configuration file
can be modified by using the --set argument. This takes the form of the
section name, a slash (/), then the option name, then an equals sign (=),
and then the value. For example: --set @repo/charset=utf8
"""
    parser = p4gf_util.create_arg_parser(desc=desc, epilog=epilog)
    parser.add_argument(NTR('repo'), metavar=NTR('R'), nargs='?', default='',
                        help=_('name of the repository, or none to operate on global.'))
    parser.add_argument('-s', '--set', help=_('option to set in a config'))
    parser.add_argument('-u', '--unset', help=_('option to remove from config'))
    parser.add_argument('-F', '--file', type=open,
                        help=_('file from which config settings are read'))
    args = parser.parse_args()
    repo_name = None
    if args.repo:
        repo_name = p4gf_translate.TranslateReponame.git_to_repo(args.repo)
        LOG.debug('translated {} => {}'.format(args.repo, repo_name))
    if args.set:
        try:
            cfg = _set_option(p4, args.set, repo_name)
        except AssertionError as ae:
            parser.error(str(ae))
    elif args.unset:
        try:
            cfg = _unset_option(p4, args.unset, repo_name)
        except AssertionError as ae:
            parser.error(str(ae))
    elif args.file:
        if not repo_name:
            sys.stderr.write(_('Use of --file without named repository!') + '\n')
            sys.exit(1)
        cfg = RepoConfig.from_depot_file(repo_name, p4, create_if_missing=True)
        cfg.repo_config.read_file(args.file)
        client = p4.fetch_client()
        cfg.write_repo_if(p4, client)
        LOG.info('replaced {}/p4gf_config'.format(repo_name))
        cfg = cfg.repo_config
    elif repo_name:
        cfg = RepoConfig.from_depot_file(repo_name, p4, create_if_missing=False).repo_config
    else:
        cfg = GlobalConfig.instance(p4)
    cfg.write(sys.stdout)


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

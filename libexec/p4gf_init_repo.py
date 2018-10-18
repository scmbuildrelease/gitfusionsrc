#! /usr/bin/env python3.3
"""Configure and populate a new Git Fusion repo.

See p4gf_init_repo.help.txt for usage details.

Create config data to map <view> to a Git Fusion client, local filesystem
location for .git and workspace data.

<view> must be an existing Perforce client spec. Its view mapping is copied
into the git repo's config. After p4gf_init_repo.py completes, Git Fusion
no longer uses or needs this <view> client spec, you can delete it or use
it for your own purposes. We just needed to copy its view mapping once.
Later changes to this view mapping are NOT propagated to Git Fusion. <view>
cannot be a stream client.

p4gf_init_repo.py creates a new Perforce client spec 'git-fusion-<view>'.
This is the client for this view, which Git Fusion uses for all operations
within this repo/view.

p4gf_init_repo.py initializes an empty git repo for this view.

NOP if a view with this name already exists.
"""

from contextlib import contextmanager, ExitStack
import logging
import os
import re
import sys

import P4

import p4gf_env_config    # pylint: disable=unused-import
import p4gf_branch
import p4gf_config
from   p4gf_config_validator import Validator
import p4gf_const
import p4gf_copy_p2g
import p4gf_context   # Intentional mis-sequence avoids pylint Similar lines in 2 files
import p4gf_create_p4
import p4gf_group
import p4gf_init_host
import p4gf_init
from   p4gf_l10n             import _, NTR, log_l10n
import p4gf_lock
import p4gf_log
import p4gf_p4spec
import p4gf_proc
import p4gf_read_permission
import p4gf_streams
import p4gf_util
import p4gf_version_3
import p4gf_repo_dirs
import p4gf_translate
import p4gf_atomic_lock
import p4gf_delete_repo_util

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_init_repo")


class InitRepoMissingView(RuntimeError):

    """Missing required template client."""

    pass


class InitRepoBadView(RuntimeError):

    """Git Fusion view malformed or using unsupported features."""

    pass


class InitRepoMissingConfigFile(RuntimeError):

    """Git Fusion repo config file does not exist."""

    pass


class InitRepoBadConfigFile(RuntimeError):

    """Git Fusion repo config file does not exist."""

    pass


class InitRepoBadStart(RuntimeError):

    """Start value not an integer or no changelists at or after value."""

    pass


class InitRepoBadCharset(RuntimeError):

    """Invalid charset."""

    pass


class InitRepoInvalidPrefix(RuntimeError):

    """Repo name starts with an invalid prefix: 'git-fusion' or '.git-fusion'."""

    pass


class InitRepoReadOnly(RuntimeError):

    """This Git Fusion instance is configured for read-only."""

    pass

# 'depot-path-repo-creation-enable' regex to match "depot/repo/branch"
RE_NDPR = re.compile(r'^([^/]+)/([^/]+)/([^/]+)')

# possible statuses for config file (too bad we don't have sum types in Python...)
CONFIG_EXISTS = 0       # exists, not necessarily at expected rev
CONFIG_NEW = 1          # exists but repo never initialized
CONFIG_NONE = 2         # doesn't exist, repo never initialized
CONFIG_MISSING = 3      # doesn't exist, but repo was previously initialized
CONFIG_DELETED = 4      # deleted @head, repo may have been previously initialized
CONFIG_READDED = 5      # exists but deleted then readded since initialization

INVALID_REPO_NAME_PREFIXES = ['git-fusion', '.git-fusion']
VIRT_STREAM_SUFFIX = "_p4gfv"


class InitRepo:

    """Initialize a repo or determine that it has already been initialized.

    Must call set_repo_config() before using.
    May optionally call set_config_file_path() and/or set_charset().
    """

    def __init__(self, p4, repo_lock):
        """set up a InitRepo object.

        :param p4:        P4API object.
        :param repo_lock: P4KeyLock mutex that prevents other processes from
                          touching this repo, whether on this Git Fusion server
                          or another.
        """
        self.p4 = p4
        self.repo_lock = repo_lock
        self.repo_name = None
        self.repo_name_git = None
        self._repo_config = None
        self._context = None
        self.charset = None
        self.stream_name = None
        self.config_file_path = None
        self.noclone = True
        self.start = None
        self.fail_when_read_only = False
        self.permit_empty_git_branch_name = False
        self.config_needs_validation = True
        self._skip_disconnect = False

    @property
    def context(self):
        """Get the context used by this initializer, creating one if needed."""
        assert self.repo_name
        if self._context is None:
            self._context = p4gf_context.create_context(self.repo_name)
            self._context.repo_config = self.repo_config
            self._context.repo_lock = self.repo_lock
        return self._context

    @context.setter
    def context(self, context):
        """Set the context to be used by this initializer."""
        assert self._context is None
        self._skip_disconnect = True
        self._context = context

    @property
    def repo_config(self):
        """Get the repo configuration property."""
        return self._repo_config

    @repo_config.setter
    def repo_config(self, config):
        """Set the repo configuration property."""
        self._repo_config = config
        # Force our config over whatever the context had produced.
        if self._context:
            self._context.repo_config = config

    def set_repo_name(self, repo_name):
        """Set the repo name."""
        assert self.repo_config is None
        self.repo_name = repo_name
        self.repo_name_git = p4gf_translate.TranslateReponame.repo_to_git(repo_name)
        return self

    def set_repo_config(self, repo_config):
        """Set the repo config."""
        self.repo_config = repo_config
        self.repo_name = repo_config.repo_name
        self.repo_name_git = p4gf_translate.TranslateReponame.repo_to_git(self.repo_name)
        return self

    def set_config_file_path(self, path):
        """Set the path to the config file."""
        self.config_file_path = path
        return self

    def set_charset(self, charset):
        """Set the charset for the repo."""
        self.charset = charset
        return self

    def set_noclone(self, noclone):
        """Set true to skip populating the repo.."""
        self.noclone = noclone
        return self

    def set_start(self, start):
        """Set the starting changelist from which the repo will be populated."""
        self.start = start
        return self

    def set_permit_empty_git_branch_name(self, val):
        """Set whether config validation allows empty/missing git_branch_name."""
        self.permit_empty_git_branch_name = val
        return self

    def set_fail_when_read_only(self, val):
        """If in read-only mode, fail the init if val is True.

        If the initialization involves creating the configuration file for
        the repository, and the flag is True, then reject the operation.

        """
        self.fail_when_read_only = val

    def full_init(self, repo_name_p4client=None):
        """Initialize the repo."""
        # Ensure we have a sane environment.
        p4gf_init.init(self.p4)

        self._get_config()

        # Initialize the repository if necessary.
        print(_("Initializing '{repo_name}'...").format(repo_name=self.repo_name))
        self.init_repo(repo_name_p4client)
        print(_("Initialization complete."))

        if not self.noclone:
            try:
                start_at = int(self.start.lstrip('@')) if self.start else 1
                self._copy_p2g_with_start(start_at)
            except ValueError:
                raise RuntimeError(_('Invalid --start value: {start}')
                                   .format(start=self.start))
            except IndexError:
                raise RuntimeError(_("Could not find changes >= '{start_at}'")
                                   .format(start_at=start_at))

    def _validate_repo_name(self):
        """Raise an exception if the repo name is invalid."""
        # disallow repo names starting with 'git-fusion' or '.git-fusion'
        for prefix in INVALID_REPO_NAME_PREFIXES:
            if self.repo_name.startswith(prefix):
                raise InitRepoInvalidPrefix(
                    _("repo name '{repo_name}' may not start with '{prefix}'")
                    .format(repo_name=self.repo_name, prefix=prefix))

    def is_init_needed(self):
        """Return True if the repo and/or client needs work done.

        The repository configuration does not need to be loaded, yet.

        """
        LOG.debug("is_init_needed() repo_name {0}".format(self.repo_name))
        self._validate_repo_name()

        repo_dirs = p4gf_repo_dirs.from_p4gf_dir(p4gf_const.P4GF_HOME, self.repo_name)

        (status, _, _) = self._discover_config_file()
        if status != CONFIG_EXISTS:
            LOG.debug("is_init_needed() repo_name {0} config missing".format(self.repo_name))
            return True

        LOG.debug("repo initialization not needed for {}".format(self.repo_name))
        # Remove the old rc file that we no longer use.
        rc_path = os.path.join(repo_dirs.repo_container, NTR('.git-fusion-rc'))
        if os.path.exists(rc_path):
            os.unlink(rc_path)
        return False

    def init_repo(self, repo_name_p4client=None, handle_imports=True):
        """Create repo if necessary, without copying from Perforce.

        :param repo_name_p4client: name of actual p4 client on which to base this new repo;
                                   if None - will be determined from repo_name if needed
        :param handle_imports: if True, process stream imports as submodules
        :return: True if repo created, False otherwise.

        """
        # repo_name is the internal repo_name with special chars already translated
        LOG.debug("init_repo : repo_name {0}".format(self.repo_name))
        assert self.repo_config  # Must be set by caller: we do not create one.
        self._validate_repo_name()

        repo_dirs = p4gf_repo_dirs.from_p4gf_dir(p4gf_const.P4GF_HOME, self.repo_name)
        client_root = repo_dirs.p4root
        p4gf_util.ensure_dir(client_root)

        # Check for cases where repo was created earlier and has changed in some way.
        # pylint: disable=unused-variable
        (status, head_rev, head_change) = self._discover_config_file()
        if status == CONFIG_MISSING:
            # just set counter to remember that this repo is gone
            self._set_server_repo_config_rev(0)
            raise InitRepoMissingConfigFile(
                p4gf_const.MISSING_P4GF_CONFIG_MSG_TEMPLATE.format(repo_name=self.repo_name))
        if status == CONFIG_DELETED:
            self._clean_deleted_config(self.repo_name)
            # set counter so we won't try deleting it again
            self._set_server_repo_config_rev(0)
            raise InitRepoMissingConfigFile(
                p4gf_const.DELETED_P4GF_CONFIG_MSG_TEMPLATE.format(repo_name=self.repo_name))
        if status == CONFIG_READDED:
            self._clean_readded_config(self.repo_name, repo_dirs)

        # Either nothing has been initialized, or it has been partially initialized.
        created_repo = False
        if status == CONFIG_NONE:
            # No such config, so we must initialize everything now.
            if p4gf_const.READ_ONLY and self.fail_when_read_only:
                raise InitRepoReadOnly(_("Cannot initialize repo in read-only instance."))
            self._init_config(repo_name_p4client, handle_imports)
            created_repo = True
        else:
            # Repo config file already checked into Perforce?  Use that.
            self._repo_from_config(handle_imports)
            # Set the p4gf_config rev to the current p4gf_config rev
            self._set_server_repo_config_rev(head_rev)

        # Ensure everything else has been set up properly.
        self._create_perm_groups()
        with self.connector() as ctx:
            self.repo_config.create_default_for_context(ctx, self.charset)
            if created_repo:
                map_tuple_list = p4gf_branch.calc_writable_branch_union_tuple_list(
                    ctx.p4.client, ctx.branch_dict(), self.repo_config)
                p4gf_atomic_lock.update_all_gf_reviews(ctx, map_tuple_list)
            if ctx.client_exclusions_added:
                _print_stderr(_("The referenced client view contains implicit exclusions.\n"
                                "The Git Fusion config will contain these as explicit exclusions."))
            _warn_if_ndpr_collision(ctx)
            if p4gf_init_host.is_init_needed(repo_dirs):
                p4gf_init_host.init_host(repo_dirs, ctx)
        if created_repo:
            LOG.debug("repository creation for %s complete", self.repo_name)
        return created_repo

    def _init_config(self, repo_name_p4client, handle_imports):
        """Create the P4 client for this repo, as well as the p4gf_config file."""
        assert self.repo_config
        self._preflight_init_repo(repo_name_p4client, handle_imports)
        if self.charset:
            self.repo_config.set(p4gf_config.SECTION_REPO, p4gf_config.KEY_CHARSET, self.charset)
        if self.stream_name:
            section_name = self.repo_config.branch_sections()[0]
            self.repo_config.set(section_name, p4gf_config.KEY_STREAM, self.stream_name)
        with self.connector() as ctx:
            self.repo_config.write_repo_if(ctx=ctx)
        # Set the p4gf_config rev at the point of creation for this GF/repo
        # (anything other than zero should be sufficient for our needs).
        self._set_server_repo_config_rev(1)

    def _validate_config(self, config):
        """Return True if the RepoConfig is valid."""
        if not self.config_needs_validation:
            LOG.debug("skipping validation of p4gf_config - no new Rev")
            return True
        validator = Validator(config, self.p4)
        validator.set_require_git_branch_name(not self.permit_empty_git_branch_name)
        return validator.is_valid()

    def _get_config(self):
        """Read or create repo_config if necessary."""
        if self.repo_config:
            return
        # If local config file specified, validate it and store in
        # Perforce now. Even if client exists (aka repo was already
        # inited), this is one way for an admin to modify an existing
        # repo's config.
        if self.config_file_path:
            try:
                self.repo_config = p4gf_config.RepoConfig.from_local_file(
                    self.repo_name, self.p4, self.config_file_path)
            except p4gf_config.ConfigLoadError as e:
                raise InitRepoMissingConfigFile(_("error: {exception}")
                                                .format(exception=e))
            except p4gf_config.ConfigParseError as e:
                raise InitRepoBadConfigFile(_("error: {exception}")
                                            .format(exception=e))
            if not self._validate_config(self.repo_config):
                raise InitRepoBadConfigFile(_('error: invalid config file {path}')
                                            .format(path=self.config_file_path))
            self.repo_config.write_repo_if(self.p4)
        elif self.charset and not Validator.valid_charset(self.charset):
            raise InitRepoBadCharset(_("error: invalid charset: {charset}")
                                     .format(charset=self.charset))
        else:
            self.repo_config = p4gf_config.RepoConfig.make_default(self.repo_name, self.p4)

    def _preflight_init_repo(self, repo_name_p4client, handle_imports):
        """Ensure that Git Fusion can initialize a repo from given the data.

        :param repo_name_p4client: name of actual p4 client on which to base this new repo;
                                   if None - will be determined from repo_name if needed
        :param handle_imports: if True, process stream imports as submodules

        Raises an exception if anything goes wrong.

        """
        LOG.debug("_preflight_init_repo(): repo_name_git: {} repo_name: {} repo_name_p4client: {}".
                  format(self.repo_name_git, self.repo_name, repo_name_p4client))

        nop4client = None
        # Client exist with the same name as this Git Fusion repo?
        # Build a new config, check it into Perforce, and use that.
        if not repo_name_p4client:
            repo_name_p4client = p4gf_translate.TranslateReponame.git_to_p4client(
                self.repo_name_git)
        if p4gf_p4spec.spec_exists(self.p4, 'client', repo_name_p4client):
            if _can_clone_to_create_repo_from_p4():
                self._repo_from_template_client(repo_name_p4client)
                return
            else:
                nop4client = _("Repo creation by cloning from a P4 client name is disabled" +
                               " on this Git Fusion server.\n")

        # creating repo from stream?
        # note that we can't pass '//depot/stream' because git would be confused
        # but it's ok to pass 'depot/stream' and add the leading '//' here
        stream_name = '//' + self.repo_name_git
        if p4gf_p4spec.spec_exists(self.p4, 'stream', stream_name):
            if _can_clone_to_create_repo_from_p4():
                self._repo_from_stream_pre(stream_name, handle_imports)
                return
            else:
                nop4client = _("Repo creation by cloning from a P4 stream name is disabled" +
                               " on this Git Fusion server.\n")

        # No p4gf_config or p4 client exists for this repo name.
        # Check if this repo_name conforms to that of depot-path-repo
        # and is depot-path-repo-creation-enable is set for this user.
        if p4gf_read_permission.can_create_depot_repo(self.p4, self.repo_name):
            self._repo_from_depot_repo()
            return

        # We don't have, and cannot create, a config for this repo.
        # Say so and give up.
        if not nop4client:
            nop4client = _("p4 client '{p4client}' does not exist\n").format(
                p4client=repo_name_p4client)
            raise InitRepoMissingView(
                p4gf_const.NO_REPO_MSG_TEMPLATE.format(
                    repo_name=self.repo_name, repo_name_p4client=repo_name_p4client,
                    nop4client=nop4client))
        else:
            raise InitRepoMissingView(
                p4gf_const.NO_REPO_FROM_CLIENT_MSG_TEMPLATE.format(
                    repo_name=self.repo_name, repo_name_p4client=repo_name_p4client,
                    nop4client=nop4client))

    def _discover_config_file(self):
        """Discover the status of the repo config file."""
        server_rev = self._get_server_repo_config_rev()
        config_path = p4gf_config.depot_path_repo(self.repo_name)
        (head_rev, head_action, head_change) = p4gf_util.depot_file_head_rev_action(
            self.p4, config_path)

        was_initialized = server_rev != '0'
        deleted = head_action and 'delete' in head_action
        exists = head_action and 'delete' not in head_action
        readded = was_initialized and exists and p4gf_util.depot_file_is_re_added(
            self.p4, config_path, server_rev, head_rev)
        if readded:
            return (CONFIG_READDED, head_rev, head_change)
        if exists:
            if was_initialized:
                if head_rev == server_rev:
                    self.config_needs_validation = False
                return (CONFIG_EXISTS, head_rev, head_change)
            else:
                return (CONFIG_NEW, head_rev, head_change)
        if deleted:
            # The p4gf_config file has been deleted and we will not resurrect from
            # a p4 client of the same name.
            return (CONFIG_DELETED, head_rev, head_change)

        if was_initialized:
            # The p4gf_config file has apparently been obliterated.
            return (CONFIG_MISSING, head_rev, head_change)

        return (CONFIG_NONE, head_rev, head_change)

    def _repo_from_template_client(self, repo_name_p4client):
        """Create a new repo configuration from a template client."""
        # repo_name_p4client is the p4client
        # repo_name is the gfinternal repo name
        # repo_name differs from repo_name_p4client if latter contains special chars
        #           or was configured with --p4client argument

        if not p4gf_p4spec.spec_exists(self.p4, 'client', repo_name_p4client):
            raise InitRepoMissingView(_("Template client {p4client} does not exist.")
                                      .format(p4client=repo_name_p4client))

        template_spec = p4gf_p4spec.fetch_client(self.p4, repo_name_p4client, routeless=True)
        if 'Stream' in template_spec:
            if 'StreamAtChange' in template_spec:
                raise InitRepoBadView(_("StreamAtChange not supported"))
            self._repo_from_stream_pre(template_spec['Stream'])
            return
        if 'ChangeView' in template_spec:
            raise InitRepoBadView(_("ChangeView not supported"))
        self.repo_config = p4gf_config.RepoConfig.from_template_client(
            self.repo_name, self.p4, template_spec, repo_name_p4client)
        if not self._validate_config(self.repo_config):
            raise InitRepoBadConfigFile(_("Invalid config file for {repo_name}")
                                        .format(repo_name=self.repo_name))

        # Seed a new client using the view's view as a template.
        LOG.info("Git Fusion repo %s does not exist, creating from existing client %s",
                 self.repo_name, repo_name_p4client)

    def _repo_from_stream_pre(self, stream_name, handle_imports=True):
        """Create a new repo config from the named stream, with initial validation.

        Create a new Perforce client spec <client_name> using existing Perforce
        stream spec <stream_name> as a template (just use its View).

        Returns one of the INIT_REPO_* constants.
        """
        # stream_name      is the name of a stream, e.g. '//depot/stream'
        # repo_name        is the gfinternal repo name

        if not p4gf_p4spec.spec_exists(self.p4, 'stream', stream_name):
            raise InitRepoMissingView(_("Stream {stream_name} does not exist.")
                                      .format(stream_name=stream_name))

        self.repo_config = p4gf_config.RepoConfig.from_stream(self.repo_name, self.p4, stream_name)
        if not self._validate_config(self.repo_config):
            raise InitRepoBadConfigFile(_("Invalid config file for {repo_name}")
                                        .format(repo_name=self.repo_name))

        # Seed a new client using the stream's view as a template.
        LOG.info("Git Fusion repo %s does not exist, creating from existing Perforce stream %s",
                 self.repo_name, stream_name)
        self._repo_from_stream(stream_name, self.repo_config, handle_imports)

    def _repo_from_stream(self, stream_name, repo_config, handle_imports=True):
        """Create a new repo from the named stream, based on the given config.

        Updates self.stream_name with the possibly new stream name, if handling imports.

        """
        imports_enabled = repo_config.getboolean(p4gf_config.SECTION_PERFORCE_TO_GIT,
                                                 p4gf_config.KEY_SUBMODULE_IMPORTS)
        if handle_imports and imports_enabled:
            # Create virtual stream with excluded paths, use that for client.
            stream = self.p4.fetch_stream(stream_name)
            stream_paths = p4gf_streams.stream_import_exclude(stream['Paths'])
            desc = (_("Created by Perforce Git Fusion for work in '{repo}'.")
                    .format(repo=p4gf_translate.TranslateReponame.repo_to_git(self.repo_name)))
            spec_values = {
                'Owner': p4gf_const.P4GF_USER,
                'Parent': stream_name,
                'Type': 'virtual',
                'Description': desc,
                'Options': 'notoparent nofromparent',
                'Paths': stream_paths,
                'Remapped': ['.gitmodules-{} .gitmodules'.format(self.repo_name)]
            }
            stream_name += VIRT_STREAM_SUFFIX
            p4gf_p4spec.set_spec(self.p4, 'stream', spec_id=stream_name, values=spec_values)
            LOG.debug('virtual stream {} created for {}'.format(stream_name, self.repo_name))
        self.stream_name = stream_name

    def _repo_from_depot_repo(self):
        """Create the repo client from the {depot}/{repo}/{branch} repo_name."""
        viewline = NTR('//{0}/... ...').format(self.repo_name_git)
        config = p4gf_config.default_config_repo_for_view_plain(self.p4, self.repo_name, viewline)
        self.repo_config.set_repo_config(config, None)

    def _repo_from_config(self, handle_imports):
        """Create a new Git Fusion repo client spec for this repo.

        The branch_id section should not matter since we now support
        all listed branches, but we need to initially set the client view
        to _something_, so pick one from config.branch_section_list[0].

        :param handle_imports: if True, and config is based on a stream, handle import paths.

        """
        # Ignore self.repo_config and read the file from the depot.
        self.repo_config = p4gf_config.RepoConfig.from_depot_file(self.repo_name, self.p4)
        if not self._validate_config(self.repo_config):
            raise InitRepoBadConfigFile(_("Invalid config file for {repo_name}")
                                        .format(repo_name=self.repo_name))
        section_name = self.repo_config.branch_sections()[0]
        branch = p4gf_branch.Branch.from_config(self.repo_config.repo_config, section_name, self.p4)

        # If config is stream-based and we are to handle imports, delegate
        # to the stream code that can deal with that appropriately. Yes,
        # this code may be called more than once for a particular repo, as
        # the initial attempt may have failed (e.g. due to improper
        # configuration) and this second attempt must finish the task.
        def is_virt_stream(name):
            """Determine if stream is a virtual one."""
            return name.endswith(VIRT_STREAM_SUFFIX)
        if branch.stream_name and handle_imports and not is_virt_stream(branch.stream_name):
            self._repo_from_stream(branch.stream_name, self.repo_config, handle_imports)
            # Modify and save the configuration file, because no one else will.
            self.repo_config.set(section_name, p4gf_config.KEY_STREAM, self.stream_name)
            with self.connector() as ctx:
                self.repo_config.write_repo_if(ctx=ctx)
            return

    def _create_perm_groups(self):
        """Create the pull and push permission groups, initially empty."""
        p4gf_group.create_repo_perm(self.p4, self.repo_name, p4gf_group.PERM_PULL)
        p4gf_group.create_repo_perm(self.p4, self.repo_name, p4gf_group.PERM_PUSH)

    def _copy_p2g_with_start(self, start):
        """Invoked 'p4gf_init_repo.py --start=NNN': copy changes from @NNN to #head."""
        with self.connector() as ctx:
            LOG.debug("connected to P4, p4gf=%s", ctx.p4gf)
            # Check that there are changes to be copied from any branch.
            r = ctx.union_view_highest_change_num(after_change_num=int(start))
            if r:
                # Copy any recent changes from Perforce to Git.
                print(_("Copying changes from '{start}'...").format(start=start))
                p4gf_copy_p2g.copy_p2g_ctx(ctx, start)
                print(_('Copying completed.'))
            else:
                msg = _("No changes above '{start}'.").format(start=start)
                if int(start) == 1:
                    LOG.debug(msg)
                else:
                    LOG.info(msg)
                    raise IndexError(msg)

    def _clean_deleted_config(self, repo_name):
        """Clean up a deleted repo."""
        client_name = p4gf_util.repo_to_client_name(repo_name)
        try:
            def args():
                """Fake argparse object."""
                return None
            args.verbose = False
            args.delete = True
            p4gf_delete_repo_util.delete_non_client_repo_data(
                args, self.p4, client_name, p4gf_delete_repo_util.DeletionMetrics())
        except P4.P4Exception as e:
            if str(e) == p4gf_const.NO_SUCH_CLIENT_DEFINED.format(client_name):
                pass
            else:
                raise e

    def _clean_readded_config(self, repo_name, repo_dirs):
        """Clean up a repo that's been deleted and readded."""
        # There has been a delete and recreation of the p4gf_config since this
        # GF instance initialized it's client and repo_dirs.  Cleanup and force
        # reinitialization.
        #
        # Reinitialization will store the current p4gf_config rev number
        # in P4GF_P4KEY_REPO_SERVER_CONFIG_REV

        # Remove this GF's client repo_dirs
        p4gf_util.remove_tree(repo_dirs.repo_container, contents_only=False)
        # Delete the serverid/repo specific GF client
        client_name = p4gf_util.repo_to_client_name(repo_name)
        p4gf_util.p4_client_df(self.p4, client_name)

    def _get_server_repo_config_rev(self):
        """Get the config file rev for the repo from the p4key."""
        key = p4gf_const.P4GF_P4KEY_REPO_SERVER_CONFIG_REV.format(
            repo_name=self.repo_name, server_id=p4gf_util.get_server_id())
        return p4gf_util.get_p4key(self.p4, key)

    def _set_server_repo_config_rev(self, value):
        """Get the config file rev for the repo from the p4key."""
        assert value is not None
        key = p4gf_const.P4GF_P4KEY_REPO_SERVER_CONFIG_REV.format(
            repo_name=self.repo_name, server_id=p4gf_util.get_server_id())
        p4gf_util.set_p4key(self.p4, key, value)

    @contextmanager
    def connector(self):
        """Optionally disconnect the context."""
        if self._skip_disconnect:
            ctx = self.context
            if ctx.p4 is None or not ctx.p4.connected():
                ctx.connect()
            yield ctx
        else:
            try:
                self.context.connect()
                yield self.context
            finally:
                self.context.disconnect()


def _print_stderr(msg):
    """Write text to stderr.

    Appends its own trailing newline so that you don't have to.
    """
    sys.stderr.write(msg + '\n')


def _warn_if_ndpr_collision(ctx):
    """Warn if branch does not map expected Perforce path.

    If the repo name looks like a match for a 'depot-path-repo-creation-
    enable' auto-created repo, but the repo's view isn't one we'd expect for
    such a repo, print a warning to stderr to tell the Git client that they
    are not working with the depot path they expect.
    """
    repo_name = p4gf_translate.TranslateReponame.p4client_to_git(
        ctx.config.repo_name)
    if not RE_NDPR.search(repo_name):
        return
    expect_depot_path = "//{}/...".format(repo_name)
    branch = ctx.most_equal()
    if not branch:
        return
    lhs = branch.view_p4map.lhs()
    matched = (1 == len(lhs)) and (lhs[0] == expect_depot_path)
    if not matched:
        msg = _(
            "Warning: repo '{repo_name}' branch {gbn}"
            " does not map to Perforce path {expect}"
            "\nRepo will work fine, just reads/writes a different"
            " portion of Perforce than you might expect.") \
            .format(repo_name=repo_name,
                    gbn=branch.git_branch_name,
                    expect=expect_depot_path)
        _print_stderr(msg)


def _can_clone_to_create_repo_from_p4():
    """Return whether clone from p4 client, stream, or depot_path is enabled."""
    return p4gf_config.GlobalConfig.getboolean(
        p4gf_config.SECTION_PERFORCE_TO_GIT,
        p4gf_config.KEY_CLONE_TO_CREATE_REPO)


def _parse_argv():
    """Convert argv into a usable dict. Dump usage/help and exit if necessary."""
    help_txt = p4gf_util.read_bin_file('p4gf_init_repo.help.txt')
    if help_txt is False:
        help_txt = _("Missing '{help_filename}' file!").format(
            help_filename=NTR('p4gf_init_repo.help.txt'))
    parser = p4gf_util.create_arg_parser(
        desc=_('Configure and populate Git Fusion repo.'),
        epilog=None,
        usage=_('p4gf_init_repo.py [options] <repo-name>'),
        help_custom=help_txt)
    parser.add_argument('--start',   metavar="")
    parser.add_argument('--noclone', action=NTR('store_true'))
    parser.add_argument('--config')
    parser.add_argument('--p4client')
    parser.add_argument(NTR('repo_name'),      metavar=NTR('repo-name'))
    parser.add_argument('--charset')
    parser.add_argument('--enablemismatchedrhs', action=NTR('store_true'))
    args = parser.parse_args()
    if args.noclone and args.start:
        raise RuntimeError(_('Cannot use both --start and --noclone'))
    if args.config and args.charset:
        raise RuntimeError(_('Cannot use both --config and --charset'))
    if args.config and args.p4client:
        raise RuntimeError(_('Cannot use both --config and --p4client'))
    LOG.debug("args={}".format(args))
    return args


def _argv_to_repo_name(args_repo_name):
    """Convert a repo name to our internal repo name.

    Take a user-supplied repo name, which might carry slashes and other
    characters prohibited as Perforce client names, and convert to our
    internal repo name.
    """
    # !!! repo_name_git    the untranslated repo name
    # !!! repo_name        the translated repo name
    repo_name_git = p4gf_util.argv_to_repo_name(args_repo_name)
    # strip leading '/' to conform with p4gf_auth_server behavior
    if repo_name_git[0] == '/':
        repo_name_git = repo_name_git[1:]
    return p4gf_translate.TranslateReponame.git_to_repo(repo_name_git)


def main():
    """Set up repo for a view."""
    p4gf_util.has_server_id_or_exit()
    args = _parse_argv()
    p4gf_version_3.log_version_extended(include_checksum=True)
    log_l10n()
    if args.enablemismatchedrhs:
        # Git Fusion should never modify the customer's config file, and
        # use of this option resulted in the config file losing all of the
        # comments and formatting the customer had put in place.
        sys.stderr.write(_('The --enablemismatchedrhs option is deprecated,'
                           ' please use enable-mismatched-rhs config file'
                           ' option instead.\n'))
        sys.exit(1)
    repo_name_p4client = None
    if args.p4client:
        repo_name_p4client = p4gf_util.argv_to_repo_name(args.p4client)
    repo_name = _argv_to_repo_name(args.repo_name)
    p4gf_util.reset_git_enviro()

    p4 = p4gf_create_p4.create_p4_temp_client()
    if not p4:
        raise RuntimeError(_('error connecting to Perforce'))

    LOG.debug("connected to P4 at %s", p4.port)
    p4gf_proc.init()

    try:
        with ExitStack() as stack:
            stack.enter_context(p4gf_create_p4.Closer())
            p4gf_version_3.version_check()
            p4gf_branch.init_case_handling(p4)
            repo_lock = p4gf_lock.RepoLock(p4, repo_name)
            stack.enter_context(repo_lock)
            ctx = p4gf_context.create_context(repo_name)
            ctx.p4gf = p4
            ctx.repo_lock = repo_lock
            initer = InitRepo(p4, repo_lock).set_repo_name(repo_name)
            initer.context = ctx
            initer.set_config_file_path(args.config)
            initer.set_charset(args.charset)
            initer.set_noclone(args.noclone)
            initer.set_start(args.start)
            stack.enter_context(ctx)
            initer.full_init(repo_name_p4client)
    except P4.P4Exception as e:
        _print_stderr(_('Error occurred: {exception}').format(exception=e))
        sys.exit(1)


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

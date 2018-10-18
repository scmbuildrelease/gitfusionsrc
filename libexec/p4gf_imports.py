#! /usr/bin/env python3.3
"""Stream Imports.

Functions to handle processing of stream imports.
"""
import sys
from collections import namedtuple
import logging
import os
import re
import socket

import p4gf_branch_id
import p4gf_config
import p4gf_const
import p4gf_context
import p4gf_copy_p2g
import p4gf_copy_to_p4
import p4gf_create_p4
import p4gf_git
from p4gf_init_repo import InitRepo
from p4gf_l10n import _
import p4gf_lock
import p4gf_object_type
import p4gf_p4spec
import p4gf_pygit2
import p4gf_streams
import p4gf_usermap
import p4gf_util

LOG = logging.getLogger(__name__)

# Pylint does not understand namedtuple.
ImportPaths = namedtuple('ImportPaths', ['import_paths', 'parent'])  # pylint:disable=invalid-name

CLIENT_LESS_REGEX = re.compile(r'"?//[^/]+/(.*)')

HTTP_URL_REGEX = re.compile(r'http[s]?://([^/])+/([^/]+)')
SSH_URL_REGEX = re.compile(r'[^@]+@([^:]+):(.+)')


class ReadOnlyException(Exception):

    """Git Fusion instance is configured for read-only, cannot create anything."""

    pass


def _validate_submodule_url(key, url, args):
    """Validate within reason.

    Handle ssh and http handling of errors differently.
    """
    LOG.debug("_validate_submodule_url {0}".format(url))
    have_error = False
    try:
        u = url.format(**args)
    except (ValueError, KeyError):
        have_error = True
    if not url.endswith('{repo}'):
        have_error = True

    if key == p4gf_config.KEY_HTTP_URL:
        m = not have_error and HTTP_URL_REGEX.match(u)
        if not m:
            LOG.error(_('Stream imports require a valid http-url'
                        ' be configured. Contact your administrator.'))
            return None    # http_auth_server will report the error
    else:
        m = not have_error and SSH_URL_REGEX.match(u)
        if not m:
            msg = _('Stream imports require a valid ssh-url'
                    ' be configured. Contact your administrator.')
            LOG.error(msg)
            raise RuntimeError(msg)
    return u


def _submodule_url(repo_config):
    """Retrieve the appropriate repo URL for the given context."""
    # Check for a standard HTTP environment variable since I am unaware of
    # any equivalent for SSH (that is also set by our T4 test suite).
    using_http = 'REMOTE_ADDR' in os.environ
    key = p4gf_config.KEY_HTTP_URL if using_http else p4gf_config.KEY_SSH_URL
    url = repo_config.get(p4gf_config.SECTION_PERFORCE_TO_GIT, key)
    if url is None:
        LOG.error('Git Fusion configuration missing ssh-url/http-url values.')
    else:
        args = {}
        args['user'] = 'git'  # fallback value
        for env_key in ['LOGNAME', 'USER', 'USERNAME']:
            if env_key in os.environ:
                args['user'] = os.environ[env_key]
                break
        args['host'] = socket.gethostname()
        args['repo'] = repo_config.repo_name
        url = _validate_submodule_url(key, url.strip(), args)
    LOG.debug('_submodule_url() => %s', url)
    return url


class StreamImports:

    """Class to process stream imports."""

    def __init__(self, ctx):
        """Initialize StreamImports object."""
        self.ctx = ctx
        self.paths_parent = self._has_import_paths()

    def missing_submodule_import_url(self):
        """Return True if repo has stream imports but no configured ssh-url."""
        return self.paths_parent and self.paths_parent.import_paths and\
            not _submodule_url(self.ctx.repo_config)

    def process(self):
        """Ensure stream imports are processed appropriately.

        The parent repository has already been initialized and populated.
        This function only applies to clients with a stream that contains
        import(ed) paths. For all other clients this will be a NOP.
        """
        if not self.paths_parent or not self.paths_parent.import_paths:
            LOG.debug2('StreamImport.process() client %s has no parent paths or import paths',
                       self.ctx.config.repo_name)
            return
        parent = self.paths_parent.parent
        if not parent:
            LOG.debug2('StreamImport.process() client %s has no parent',
                       self.ctx.config.repo_name)
            return

        if not p4gf_const.READ_ONLY:
            # Prohibit developers from pushing submodule changes to this stream repo.
            LOG.debug('StreamImports.process() config %s to prohibit submodule changes',
                      self.ctx.config.repo_name)
            self.ctx.repo_config.set(p4gf_config.SECTION_REPO,
                                     p4gf_config.KEY_ENABLE_SUBMODULES,
                                     'no')
            self.ctx.repo_config.write_repo_if(self.ctx.p4gf)
        if LOG.isEnabledFor(logging.DEBUG3):
            try:
                for branch in self.ctx.repo.listall_branches():
                    ref = self.ctx.repo.lookup_branch(branch)
                    LOG.debug3('process_imports() %s => %s', branch, p4gf_pygit2.ref_to_sha1(ref))
            except KeyError:
                pass
            except ValueError:
                pass
        try:
            # Attach HEAD to a branch so our commits persist.
            self.ctx.checkout_master_ish()
            self._import_submodules()
        except Exception as e:   # pylint:disable=broad-except
            msg = str(e).replace('\\t', '\t').replace('\\n', '\n')
            sys.stderr.write(_("Perforce: error {exception}\n").format(exception=msg))
            emsg = _("Perforce: submodule imports failed.\n"
                     "  Contact your administrator to correct this problem.\n" +
                     "  Then:\n" +
                     "     git pull\n" +
                     "     git submodule update --init --recursive\n")
            sys.stderr.write(emsg)
            LOG.exception("submodule imports failed")

    def _has_import_paths(self):
        """Determine whether this repo defines stream import views.

        Return a named tuple with import_paths and parent.
        """
        imports_enabled = self.ctx.repo_config.getboolean(p4gf_config.SECTION_PERFORCE_TO_GIT,
                                                          p4gf_config.KEY_SUBMODULE_IMPORTS)
        if not imports_enabled:
            return None
        # check if this client has a virtual stream
        # (need to force the stream-ish definition of the client, if one exists)
        branch = self.ctx.most_equal()
        p4 = self.ctx.p4
        LOG.debug2('process_imports() branch %s', branch)
        with self.ctx.switched_to_branch(branch):
            client = p4.fetch_client()
        LOG.debug2('has_import_paths() checking %s for a stream', client['Client'])
        if 'Stream' not in client:
            LOG.debug2('has_import_paths() %s is not a stream client', client['Client'])
            return None
        virt_stream = client['Stream']
        virtual = p4.fetch_stream(virt_stream)
        if 'Parent' not in virtual or virtual['Parent'] == 'none':
            LOG.debug2('has_import_paths() %s has no parent', virt_stream)
            return None
        if virtual['Type'] != 'virtual':
            LOG.debug2('has_import_paths() %s created prior to submodules support', virt_stream)
            return None
        parent = p4gf_util.first_dict(p4.run('stream', '-ov', virtual['Parent']))
        LOG.debug3('has_import_paths() parent stream=%s', parent)
        v_paths = virtual['Paths']
        p_paths = parent['Paths']
        import_paths = p4gf_streams.match_import_paths(v_paths, p_paths)
        if not import_paths:
            # May not have import paths now, but perhaps it did before and they were
            # removed, in which case we need to do more work.
            if self._has_submodules():
                # Signal that something must be done with the lingering submodules.
                import_paths = [[None, None, None]]
            else:
                LOG.debug2('has_import_paths() %s has no exclude paths', virt_stream)
                return None
        return ImportPaths(import_paths=import_paths, parent=parent)

    def _has_submodules(self):
        """Detect if repo has submodules managed by Git Fusion."""
        modules = p4gf_git.parse_gitmodules(self.ctx.repo)
        LOG.debug('has_submodules() checking for submodules in %s', self.ctx.config.p4client)
        for section in modules.sections():
            if modules.has_option(section, p4gf_const.P4GF_MODULE_TAG):
                LOG.debug2('has_submodules() found submodule %s', section)
                return True
        LOG.debug2('has_submodules() no submodules found in %s', self.ctx.config.p4client)
        return False

    def _import_submodules(self):
        """For stream clients, create a submodule for each import."""
        # pylint:disable=too-many-statements, too-many-branches
        view = self.paths_parent.parent['View']  # the parent stream's 'View'
        change_view = self.paths_parent.parent.get('ChangeView')  # the parent stream's 'ChangeView'
        import_paths = self.paths_parent.import_paths

        # have already split this function several times...
        usermap = p4gf_usermap.UserMap(self.ctx.p4gf, self.ctx.email_case_sensitivity)
        user_3tuple = usermap.lookup_by_p4user(p4gf_const.P4GF_USER)
        if not user_3tuple:
            LOG.error('Missing Perforce user %s', p4gf_const.P4GF_USER)
            return
        client_name = self.ctx.config.p4client
        LOG.debug('processing imports for %s', client_name)
        LOG.debug3('_import_submodules() view=%s, change_view=%s, import_paths=%s',
                   view, change_view, import_paths)
        change_views = p4gf_streams.stream_imports_with_changes(view, change_view, import_paths)
        LOG.debug2('_import_submodules() change_views=%s', change_views)
        if not change_views and LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2('_import_submodules() view=%s change_view=%s import_paths=%s',
                       view, change_view, import_paths)
        # initialize and populate the submodules
        old_head = p4gf_pygit2.head_ref(self.ctx.repo)
        for depot_path, change_num, local_path in change_views:
            # avoid double-nesting by excluding the local path from the client path
            client_path = "//{}/...".format(client_name)
            LOG.debug('_import_submodules() for %s => %s', depot_path, client_path)
            stream_name = depot_path[:-4]
            if p4gf_p4spec.spec_exists(self.ctx.p4, 'stream', stream_name):
                # convert stream name to repo name by pruning leading slashes
                repo_name = p4gf_streams.repo_name_from_depot_path(stream_name)
                config = None
                LOG.debug('initializing stream import for %s', depot_path)
            else:
                # create a repo configuration file for this 1-line view
                repo_name = p4gf_streams.repo_name_from_depot_path(depot_path)
                client_less_path = CLIENT_LESS_REGEX.match(client_path).group(1)
                if client_path and client_path[0] == '"':
                    client_less_path = '"' + client_less_path
                repo_view = depot_path + " " + client_less_path
                LOG.debug('creating config for %s', repo_name)
                config = p4gf_config.default_config_repo_for_view_plain(self.ctx.p4,
                                                                        repo_name,
                                                                        repo_view)
            # prepare to initialize the repository
            #
            # Note that we skip the temp client counting mechanism in this
            # case because it is rather difficult to avoid.
            p4 = p4gf_create_p4.create_p4_temp_client(skip_count=True)
            if not p4:
                LOG.error('unable to create P4 instance for %s', repo_name)
                return
            if p4gf_const.READ_ONLY:
                try:
                    repo_config = p4gf_config.RepoConfig.from_depot_file(repo_name, p4)
                    subtxt = p4gf_context.create_context(repo_name)
                    subtxt.p4gf = p4
                    ir = InitRepo(p4, None).set_repo_config(repo_config)
                    ir.context = subtxt
                    ir.init_repo(handle_imports=False)
                    with subtxt:
                        # populate the submodule
                        self._copy_submodule(subtxt, local_path, change_num, user_3tuple)
                except p4gf_config.ConfigLoadError:
                    raise ReadOnlyException(_("Read-only instance cannot initialize repositories."))
            else:
                with p4gf_lock.RepoLock(p4, repo_name) as repo_lock:
                    if config:
                        p4gf_config.create_file_repo_from_config(self.ctx, repo_name, config)
                    LOG.debug('initializing repo for %s', repo_name)
                    repo_config = p4gf_config.RepoConfig.from_depot_file(repo_name, p4,
                                                                         create_if_missing=True)
                    subtxt = p4gf_context.create_context(repo_name)
                    subtxt.p4gf = p4
                    subtxt.repo_lock = repo_lock
                    ir = InitRepo(p4, repo_lock).set_repo_config(repo_config)
                    ir.context = subtxt
                    ir.init_repo(handle_imports=False)
                    with subtxt:
                        # populate the submodule
                        self._copy_submodule(subtxt, local_path, change_num, user_3tuple)
            if p4.connected():
                p4gf_create_p4.p4_disconnect(p4)
        # Remove any submodules controlled by Git Fusion that no longer match
        # any of the current import paths.
        self._deport_submodules(import_paths, user_3tuple)

        if not p4gf_const.READ_ONLY:
            # The process() method above configures 'enable-git-submodules'
            # in the parent repo to disable submodule updates. This is written to p4gf_config,
            # but self.ctx.submodules is not set to False, and remains = True.
            # This so the import of the submodule itself is not rejected.
            # However, if the import fails, the next pull attempt would
            # fail now that 'enable-git-submodules' has been set to False.
            # So .. bypass the submodules protection for the fake push which
            # maybe be creating the imported submodule itself in the parent repo.

            submodules = self.ctx.submodules
            self.ctx.submodules = True    # temporary
            self._ensure_commits_copied(old_head)
            self.ctx.submodules = submodules

    def _ensure_commits_copied(self, old_head):
        """Ensure the Git commits we just created are copied back to Perforce.

        Do this by faking a 'push' from the client. Roll the HEAD reference
        ('master') back to the old SHA1, assign the commits to Perforce branches,
        then move the reference back to the latest commit and copy everything to
        the depot as usual.
        """
        new_head = p4gf_pygit2.head_ref(self.ctx.repo)
        old_head_sha1 = p4gf_pygit2.ref_to_sha1(old_head)
        new_head_sha1 = p4gf_pygit2.ref_to_sha1(new_head)
        if new_head_sha1 == old_head_sha1:
            return
        p4gf_pygit2.set_branch(self.ctx.repo, new_head, old_head_sha1)
        prt = p4gf_branch_id.PreReceiveTuple(old_head_sha1, new_head_sha1, new_head.name)

        LOG.debug('Copying modules to depot: %s', prt)
        assigner = p4gf_branch_id.Assigner(self.ctx.branch_dict(), [prt], self.ctx)
        assigner.assign()
        p4gf_pygit2.set_branch(self.ctx.repo, new_head, new_head_sha1)
        with p4gf_git.head_restorer():
            p4gf_copy_to_p4.copy_git_changes_to_p4(self.ctx, prt, assigner, None)

    def _deport_submodules(self, import_paths, user_3tuple):
        """Find any submodules that Git Fusion controls which should be removed.

        Arguments:
            import_paths -- current set of import paths in stream.
            user_3tuple -- (p4user, email, fullname) for Git Fusion user
        """
        # parse the .gitmodules file into an instance of ConfigParser
        modules = p4gf_git.parse_gitmodules(self.ctx.repo)
        LOG.debug('_deport_submodules() checking for defunct submodules in %s',
                  self.ctx.config.p4client)
        # find those sections whose 'path' no longer matches any of the imports
        for section in modules.sections():
            # we can only consider those submodules under our control
            if modules.has_option(section, p4gf_const.P4GF_MODULE_TAG):
                path = modules.get(section, 'path', raw=True, fallback=None)
                if not path:
                    LOG.warning(".gitmodules entry %s has %s but no 'path'",
                             section, p4gf_const.P4GF_MODULE_TAG)
                    continue
                LOG.debug('_deport_submodules() considering import %s', path)
                # append the usual suffix for easier comparison
                view_path = path + '/...'
                present = False
                for impath in import_paths:
                    if impath[0] == view_path:
                        present = True
                        break
                if not present:
                    # removal happens for each submodule separately because
                    # merging the writes into a single tree is tricky
                    if p4gf_git.remove_submodule(self.ctx.repo, path, user_3tuple):
                        LOG.debug('_deport_submodules() removed submodule %s', path)

    def _copy_submodule(self, subtxt, local_path, change_num, user_3tuple):
        """Copy from Perforce to Git the submodule changes.

        Arguments:
            subtxt -- context for submodule repo.
            local_path -- path within parent repo where submodule will go.
            user_3tuple -- (p4user, email, fullname) for Git Fusion user

        Returns the new SHA1 of the parent repo and an error string, or None
        if successful.
        """
        cwd = os.getcwd()
        os.chdir(subtxt.repo_dirs.GIT_WORK_TREE)
        repo_name = subtxt.config.repo_name
        LOG.debug('_copy_submodule() marking submodule %s as read-only', repo_name)
        subtxt.repo_config.set(p4gf_config.SECTION_REPO, p4gf_config.KEY_READ_ONLY, 'yes')
        if self.ctx.is_lfs_enabled:
            LOG.debug('_copy_submodule() marking submodule %s as git-lfs-enable', repo_name)
            subtxt.is_lfs_enabled = self.ctx.is_lfs_enabled
            subtxt.repo_config.set(p4gf_config.SECTION_REPO,
                    p4gf_config.KEY_GIT_LFS_ENABLE, 'yes')
            initial_tracking = self.ctx.repo_config.get(
                    p4gf_config.SECTION_PERFORCE_TO_GIT,
                    p4gf_config.KEY_GIT_LFS_INITIAL_TRACK)
            if initial_tracking:
                subtxt.repo_config.set(p4gf_config.SECTION_REPO,
                    p4gf_config.KEY_GIT_LFS_INITIAL_TRACK, initial_tracking)
        subtxt.repo_config.write_repo_if(subtxt.p4gf)
        LOG.debug('_copy_submodule() copying changes for %s', repo_name)
        p4gf_copy_p2g.copy_p2g_ctx(subtxt)
        # if available, use the requested change to get the corresponding SHA1 of the submodule
        commit_ot = None
        latest_change = subtxt.union_view_highest_change_num(at_or_before_change_num=change_num)
        if latest_change:
            LOG.debug('_copy_submodule() latest change: %s', latest_change)
            commit_ot = p4gf_object_type.ObjectType.change_num_to_commit(
                subtxt, latest_change, None)
        if commit_ot:
            sub_sha1 = commit_ot.sha1
            LOG.debug('_copy_submodule() using commit %s', sub_sha1)
        else:
            # otherwise use the latest commit
            sub_sha1 = p4gf_pygit2.head_commit_sha1(subtxt.repo)
            LOG.debug('_copy_submodule() using HEAD: %s', sub_sha1)
        os.chdir(cwd)
        url = _submodule_url(subtxt.repo_config)
        if local_path.endswith('...'):
            local_path = local_path[:-3]
        local_path = local_path.rstrip('/')
        if p4gf_git.add_submodule(self.ctx.repo, repo_name, local_path, sub_sha1, url, user_3tuple):
            LOG.debug('_copy_submodule() added submodule %s to %s as %s',
                      local_path, repo_name, user_3tuple[0])

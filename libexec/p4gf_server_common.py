#! /usr/bin/env python3.3
"""Common functions for all P4GF server implementations."""

from collections import namedtuple
from contextlib import contextmanager, ExitStack
import functools
import logging
import os
import random
import re
import signal
import sys
import time

import p4gf_atomic_lock
from P4 import Map, P4Exception
import p4gf_branch
import p4gf_config
import p4gf_const
import p4gf_context
import p4gf_copy_p2g
import p4gf_create_p4
import p4gf_mem_gc
import p4gf_git
import p4gf_git_repo_lock
from p4gf_git_swarm import GSReviewCollection
import p4gf_group
import p4gf_init_host
import p4gf_imports
import p4gf_init
from p4gf_init_repo import InitRepo, InitRepoMissingView, InitRepoReadOnly
import p4gf_p4key as P4Key
from p4gf_l10n import _, NTR
import p4gf_lock
import p4gf_log
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_p4spec
from p4gf_prl_file import PRLFile
import p4gf_proc
from p4gf_profiler import Timer, with_timer
import p4gf_protect
from p4gf_repolist import RepoList
import p4gf_translate
import p4gf_util
from p4gf_util import CommandError
import p4gf_version_3
import p4gf_read_permission
import p4gf_lock_status

LOG = logging.getLogger(__name__)

CMD_GIT_UPLOAD_PACK   = "git-upload-pack"      # aka fetch/pull/clone
CMD_GIT_RECEIVE_PACK  = "git-receive-pack"     # aka push
CMD_LFS_OBJECTS       = "objects"              # aka Git LFS pre-push metadata

COMMAND_TO_PERM = {
    CMD_GIT_UPLOAD_PACK:  p4gf_group.PERM_PULL,
    CMD_GIT_RECEIVE_PACK: p4gf_group.PERM_PUSH,
    # All LFS requests must be treated as "pull" until we can determine
    # exactly where in the depot (repo and branch) the files are going.
    CMD_LFS_OBJECTS:      p4gf_group.PERM_PULL
}


class ServerCommonException(Exception):

    """base class for exceptions that don't require logging."""

    pass


class BadRequestException(ServerCommonException):

    """bad args or similar."""

    pass


class PerforceConnectionFailed(ServerCommonException):

    """trouble with p4 connection."""

    pass


class SpecialCommandException(ServerCommonException):

    """requested repo is actually a special command."""

    pass


class RepoNotFoundException(ServerCommonException):

    """requested repo does not exist."""

    pass


class RepoInitFailedException(ServerCommonException):

    """problem initializing a repo."""

    pass


class MissingSubmoduleImportUrlException(ServerCommonException):

    """repo has Stream imports but no configured ssh-url."""

    pass


class ReadOnlyInstanceException(ServerCommonException):

    """Git Fusion instance is configured to reject pushes."""

    pass


class TerminatingException(ServerCommonException):

    """A terminating signal was received during pull/push processing."""

    pass


class ExceptionAuditLogger:

    """Print errors to standard channels, then propagate."""

    def __init__(self):
        """Initialize the logger."""
        pass

    def __enter__(self):
        """On enter do nothing."""
        return self

    def __exit__(self, exc_type, exc_value, _traceback):
        """On exit, log exceptions in some cases."""
        # Skip calls to exit().
        if exc_type == SystemExit:
            return False

        # Skip logging of certain known exception types
        if isinstance(exc_value, ServerCommonException):
            return False

        if exc_value:
            msg = "{}".format(exc_value)
            # Improve the readability of the error message for the client (GF-2311).
            msg = msg.replace('\\t', '\t').replace('\\n', '\n')
            sys.stderr.write(msg + '\n')
            if hasattr(exc_value, "usage") and exc_value.usage:
                print(exc_value.usage)

        return False  # False = do not squelch. Propagate


def check_lfs_enabled_maps_top_level(ctx):
    """If LFS is enabled for this repo, views must map top level in all branches. """

    if ctx.is_lfs_enabled and not ctx.check_branches_map_top_level():
        raise RuntimeError(
           _('Perforce: Improperly configured branch views.'
             '\n  LFS is enabled for this repo, but at least one branch does'
             '\n  not map the top level directory for .gitattributes).'))


class Server(object):

    """base class for Git Fusion servers."""

    def __init__(self):
        """Init the Server object."""
        self.p4 = None
        self.user = None
        self.foruser = None
        self.repo_name_git = None
        self.command = None
        self.git_caller = None
        self.skip_perms_check = False
        self.poll_only = False
        self.repo_name = None
        self.repo_perm = None
        self.git_dir = None
        self._repo_config = None
        self._should_remove_atomic_lock = False

    def before(self):
        """override to do setup before process."""
        pass

    def after(self):
        """override to do cleanup after process."""
        pass

    def push_received(self):
        """Return True if the push payload is being received.

        Some protocols, such as HTTP, receive multiple requests during a
        push operation, only one of which is the actual push payload. We
        need to know when that is the case, and the server implementations
        must override this method to provide that information.

        """
        # pylint:disable=no-self-use
        return False

    def record_access(self):
        """Record the access of the repository in the audit log."""
        pass

    def record_error(self, msg):
        """Record the given error message to the audit log.

        :param str msg: the error message to record.

        """
        pass

    @property
    def repo_config(self):
        """Fetch or create the repo configuration file, if not already loaded."""
        if self._repo_config is None:
            self._repo_config = p4gf_config.RepoConfig.from_depot_file(
                self.repo_name, self.p4, create_if_missing=True)
        return self._repo_config

    @with_timer('Setup')
    def _setup(self):
        """do setup; no lock required."""
        LOG.debug(p4gf_log.memory_usage())
        p4gf_util.has_server_id_or_exit(log=LOG)

        p4gf_util.reset_git_enviro()
        #
        # Initialize the external process launcher early, before allocating
        # lots of memory, and just after all other conditions have been
        # checked.
        #
        # Do this _after_ changing to the git working tree, as it seems to
        # be rather difficult to correct this later using the 'cwd'
        # argument to the subprocess.Popen() constructor, at least on
        # Linux systems.
        #
        # Also do this before creating any P4 connections, as that seems to
        # effect whether temporary clients are automatically deleted or not.
        #
        p4gf_proc.init()

        self.p4 = p4gf_create_p4.create_p4_temp_client()
        if not self.p4:
            raise PerforceConnectionFailed()
        LOG.debug("connected to P4: %s", self.p4)

        p4gf_group.invalidate_groups_i()
        p4gf_branch.init_case_handling(self.p4)
        self._get_repo_name_and_foruser()

        self._check_readiness()
        self._check_lock_perm()
        self._check_protects()
        self.check_special_command()

        self._check_valid_command()
        self._check_gf_depot_permissions()
        self.check_user_exists()
        self._check_perms()
        self._init_system()

        self.check_lfs_enabled()
        self._write_motd()

    def _init_system(self):
        """Initialize the Git Fusion system."""
        # Create Git Fusion server depot, user, config. NOPs if already created.
        p4gf_init.init(self.p4)

    @with_timer('Server process')
    def process(self):
        """process the request."""
        exit_code = 1
        with ExitStack() as stack:
            stack.enter_context(ExceptionAuditLogger())
            stack.enter_context(p4gf_create_p4.Closer())
            stack.enter_context(run_before_after(self))
            stack.enter_context(gc_debug())
            stack.enter_context(log_start_end(self))
            self._setup()

            ctx = p4gf_context.create_context(self.repo_name)
            ctx.p4gf = self.p4
            ctx.foruser = self.foruser
            stack.enter_context(ctx)
            ctx.log_context()
            self._init_host(ctx)
            # N.B. this temporarily takes both exclusive locks, if needed
            # N.B. for the LFS server, this function does nothing
            repo_created = self._init_repo(ctx)
            check_lfs_enabled_maps_top_level(ctx)

            # Change into the git working directory. Not all git commands
            # react well to the --work-tree option.
            self.git_dir = ctx.repo_dirs.GIT_DIR
            os.chdir(ctx.repo_dirs.GIT_WORK_TREE)
            stack.enter_context(raise_on_sigterm())

            try:
                exit_code = self.process_request(ctx, repo_created)
            except TerminatingException:
                # The design of the "raise on sigterm" handler is to raise
                # an exception when SIGTERM is received, which should cause
                # all of the "process" functions to clean up and remove any
                # locks. We then quietly exit to avoid causing grief with
                # the T4 tests running in ElectricCommander.
                LOG.warning('terminating signal received, exiting quietly')

        return exit_code

    def process_request(self, ctx, repo_created):
        """Handle the incoming request, now that everything is set up.

        :param ctx: Git Fusion context.
        :param bool repo_created: True if repo was just created now.

        :return: exit status code (usually zero).

        """
        try:
            rollback_prl(ctx, blocking=False)
        except p4gf_git_repo_lock.LockBusy:
            # This will happen often, do not fail fast, just log and move on.
            LOG.info("repo %s busy at this time, no rollback attempted", self.repo_name)
        if CMD_GIT_UPLOAD_PACK in self.command or self.poll_only:
            # This is a pull request.
            update_only_on_poll = self.repo_config.getboolean(
                p4gf_config.SECTION_PERFORCE_TO_GIT,
                p4gf_config.KEY_UPDATE_ONLY_ON_POLL)
            if p4gf_const.READ_ONLY:
                # Retrieve whatever is available in the object cache.
                exit_code = self._process_readonly(ctx)
            elif update_only_on_poll and not self.poll_only:
                # The antithesis of polling: no update at all, serve
                # whatever is available in the repo right now.
                exit_code = self._call_git(ctx)
            else:
                # Normal fetch or a poll.
                exit_code = self._process_upload(ctx, repo_created)
        else:
            # This is a push request.
            if p4gf_const.READ_ONLY:
                raise ReadOnlyInstanceException(_('Push to read-only instance prohibited'))
            exit_code = self._process_receive(ctx, repo_created)
        return exit_code

    def _process_upload(self, ctx, _repo_created):
        """Service the git-upload-pack (fetch) request.

        :param ctx: Git Fusion context.
        :param repo_created: True if repo was created in this request.

        :return: exit code of the git command, or OK if poll_only is True.

        """
        log = LOG.getChild('upload')
        # Acquire the git read lock _before_ getting the p4key lock.
        with p4gf_git_repo_lock.read_lock(self.repo_name) as waited_on_writer:
            # Attempt to acquire the p4key lock to gain exclusive access to
            # the Git Fusion repository so that we may perform the Perforce
            # to Git translation. Failing that, we will skip the p4-to-git
            # phase and simply let git-upload-pack return the currently
            # available data to the client.
            log.debug('read commencing for {}'.format(self.repo_name))
            if not waited_on_writer:
                repo_lock = p4gf_lock.RepoLock(self.p4, self.repo_name, blocking=False)
                try:
                    repo_lock.acquire()
                    ctx.repo_lock = repo_lock
                    lock_acquired = True
                except p4gf_lock.LockBusy:
                    lock_acquired = False
                try:
                    if lock_acquired:
                        # Set the lock(s) to "blocking" so that the release
                        # will succeed despite any momentary contention for
                        # access to the owners key.
                        repo_lock.blocking = True
                        repo_lock.lock.blocking = True
                        log.debug('p2g commencing for {}'.format(self.repo_name))
                        ctx.checkpoint("server_common:acquire_lock")
                        # Upgrade to a writer lock when copying to git; this is
                        # safe as we already have the p4key lock and at worst
                        # would have to wait for any newly arrived readers to
                        # finish their requests. Those other readers would fail
                        # to acquire the p4key lock and thus would not take
                        # this path through the code, hence no deadlock. Since
                        # we have the read lock and the p4key lock, any other
                        # writer will be put on hold until we are done.
                        # If not polling, do not block on getting the write lock.
                        write_lock = p4gf_git_repo_lock.write_lock(
                            self.repo_name, upgrade=True, blocking=self.poll_only)
                        try:
                            with ExitStack() as stack:
                                stack.enter_context(Timer('with Lock'))
                                stack.enter_context(write_lock)
                                # We now have the exclusive write lock and the p4key
                                # lock and can perform the p4-to-git translation.
                                self._copy_p2g(ctx)
                                ctx.update_changes_since_last_seen()
                        except p4gf_git_repo_lock.LockBusy:
                            # Oh well, this fetch will possibly be missing the latest changes.
                            # But at least we responded quickly, without waiting on anything.
                            LOG.debug("skipping p4-to-git due to contention on {}".format(
                                self.repo_name))
                finally:
                    if lock_acquired:
                        # Give up the p4key lock _before_ invoking git to respond to client.
                        ctx.checkpoint("server_common:releasing_lock")
                        # Disconnect while we have the lock so temp clients are removed.
                        ctx.disconnect()
                        repo_lock.release()
        if self.poll_only:
            # Record the event of accessing the repo for the audit log.
            self.record_access()
            code = os.EX_OK
        else:
            log.debug('delegating to git for {}'.format(self.repo_name))
            code = self._call_git(ctx)
        return code

    def _process_receive(self, ctx, _repo_created):
        """Service the git-receive-pack (push) request.

        :param ctx: Git Fusion context.
        :param repo_created: True if repo was created in this request.

        :return: exit code of the git command.

        """
        def cleanup(repo_lock, msg):
            """Clean up after error."""
            # Do not wait to release the lock, just do it immediately. We
            # cannot make any assumptions about what git-receive-pack and
            # our hooks are doing right now, we can only exit as quickly
            # and cleanly as possible.
            repo_lock.ignore_pending_acquire = True
            # When something bad happens, remove the atomic lock.
            self._maybe_remove_atomic_lock(ctx)
            # Record the failure in the push status key.
            ctx.record_push_failed_p4key(msg)

        with ExitStack() as stack:
            repo_lock = stack.enter_context(self._acquire_both_write_locks())
            self._should_remove_atomic_lock = True
            ctx.repo_lock = repo_lock
            # We now have the exclusive write lock and the p4key lock and
            # can perform the p4-to-git and git-to-p4 translations.
            stack.enter_context(Timer('with Lock'))
            ctx.checkpoint("server_common:acquire_lock")

            try:
                self._increment_push_counter(ctx)
                # In the push case, engage the atomic view lock.
                # Do this BEFORE copying Perforce to Git, to avoid a race
                # condition with new Perforce changelists coming in after we
                # finish the copy.
                p4gf_atomic_lock.lock_update_repo_reviews(ctx, action=p4gf_atomic_lock.ADD)
                self._copy_p2g(ctx)
                # Ensure the atomic lock is removed if the p4key lock is
                # released without successfully transferring it to the
                # background process (hence the two callbacks).
                release_cb = functools.partial(self._maybe_remove_atomic_lock, ctx)
                repo_lock.set_lock_release_cb(release_cb)
                repo_lock.set_transfer_complete_cb(self._lock_transfer_cb)
                # The git-to-p4 translation will happen in post-receive.
                _set_pre_receive_flag(ctx)
                stack.enter_context(Timer('call git'))
                ec = self._call_git(ctx)
                if ec:
                    msg = _('git-receive-pack returned {error_code}').format(error_code=ec)
                    LOG.error('{} for {}'.format(msg, self.repo_name))
                    cleanup(repo_lock, msg)
                    sys.stderr.write(msg + "\n")
                elif _detect_pre_receive_flag(ctx):
                    # Treat the "nop" as a success, but record in the log exactly
                    # what happened for easier debugging when a push seems to fail.
                    LOG.info('push {} had nothing to do (pre-receive not invoked)'.format(
                        ctx.push_id))
                    ctx.record_push_success_p4key()
                    ctx.update_changes_since_last_seen()
                ctx.checkpoint("server_common:releasing_lock")
                return ec
            except:  # pylint:disable=bare-except
                LOG.exception('receive failed')
                cleanup(repo_lock, sys.exc_info()[1])
                raise

    def _process_readonly(self, ctx):
        """Service the git-upload-pack (fetch) request on a read-only instance.

        :param ctx: Git Fusion context.

        :return: exit code of the git command.

        """
        with p4gf_git_repo_lock.read_lock(self.repo_name):
            # Upgrade to a writer lock when writing to the git repo.
            with p4gf_git_repo_lock.write_lock(self.repo_name, upgrade=True):
                # Ensure our repository is up to date with the mirror.
                try:
                    stream_imports = p4gf_imports.StreamImports(ctx)
                    if stream_imports.missing_submodule_import_url():
                        raise MissingSubmoduleImportUrlException
                    p4gf_copy_p2g.copy_p2g_ctx(ctx)
                    stream_imports.process()
                except p4gf_imports.ReadOnlyException as e:
                    raise ReadOnlyInstanceException(str(e))
                except InitRepoReadOnly as e:
                    raise ReadOnlyInstanceException(str(e))
                p4gf_git.set_bare(True)
        code = self._call_git(ctx)
        return code

    @contextmanager
    def _acquire_both_write_locks(self):
        """Acquire both the write lock and p4key lock without causing deadlock.

        In particular, acquire the locks in the established order (file
        lock first, then p4key lock), releasing the file lock if the p4key
        lock is busy, and waiting briefly before trying again. This allows
        the background push process to operate without holding the file
        lock, and yet have an incoming push wait on that background push to
        complete. Without this, the incoming push would temporarily block
        the background push, get both the file and p4key locks, and then
        fail to acquire the atomic lock, and reject the push (which is a
        bad user experience).

        """
        try:
            repo_lock = p4gf_lock.RepoLock(self.p4, self.repo_name, blocking=False)
            while True:
                # Acquire the write lock first, then try to get the p4key
                # lock. If that lock is busy, release the write lock and
                # sleep for a brief time before trying again.
                p4gf_git_repo_lock.acquire_write_lock(self.repo_name)
                try:
                    repo_lock.acquire()
                    # Set the lock(s) to "blocking" so that the release
                    # will succeed despite any momentary contention for
                    # access to the owners key.
                    repo_lock.blocking = True
                    repo_lock.lock.blocking = True
                    break
                except p4gf_lock.LockBusy:
                    p4gf_git_repo_lock.remove_write_lock(self.repo_name)
                    # sleep randomly between 1/4 and 1 second
                    time.sleep(random.randrange(1, 4) / 4.0)
            yield repo_lock
        finally:
            repo_lock.release()
            p4gf_git_repo_lock.remove_write_lock(self.repo_name)

    def _get_repo_name_and_foruser(self):
        """Extract foruser from url if present; get translated repo name."""
        foruser_patt = re.compile('@foruser=([^ @]+)')
        m = foruser_patt.search(self.repo_name_git)
        if m:
            self.foruser = m.group(1)
            repo_name_git = self.repo_name_git[:m.start(0)] + self.repo_name_git[m.end(0):]
            LOG.debug('foruser: %s', self.foruser)
        else:
            repo_name_git = self.repo_name_git
        # translate '/' ':' ' ' .. etc .. for internal repo_name
        # and figure out if repo is  'repo' or 'repo.git'
        self.repo_name = p4gf_translate.TranslateReponame.url_to_repo(repo_name_git, self.p4)
        p4gf_log.configure_for_repo(self.repo_name)
        LOG.debug("public repo_name: %s   internal repo_name: %s",
                  repo_name_git, self.repo_name)

    def _check_readiness(self):
        """Check that P4GF is ready for accepting connections from clients."""
        prevent_session = P4Key.get_all(self.p4, p4gf_const.P4GF_P4KEY_PREVENT_NEW_SESSIONS + '*')
        trigger_version = P4Key.get(self.p4, p4gf_const.P4GF_P4KEY_TRIGGER_VERSION)

        # Check if the "prevent further access" p4key has been set, and raise an
        # error if the p4key is anything other than zero.
        if prevent_session:
            every_instance = prevent_session.get(p4gf_const.P4GF_P4KEY_PREVENT_NEW_SESSIONS, '0')
            key_name = p4gf_const.P4GF_P4KEY_PREVENT_NEW_SESSIONS + '-' + p4gf_util.get_server_id()
            our_instance = prevent_session.get(key_name, '0')
            if every_instance != '0' or our_instance != '0':
                raise RuntimeError(_('Git Fusion is shutting down. Please contact your admin.'))

        # Check that GF submit trigger is installed and has a compatible version.
        trigger_version_p4key = trigger_version.split(":")[0].strip() if trigger_version else '0'
        if trigger_version_p4key != p4gf_const.P4GF_TRIGGER_VERSION:
            LOG.error("Incompatible trigger version: {0} should be {1} but got {2}".format(
                p4gf_const.P4GF_P4KEY_TRIGGER_VERSION,
                p4gf_const.P4GF_TRIGGER_VERSION, trigger_version_p4key))
            if trigger_version_p4key == '0':
                raise RuntimeError(_('Git Fusion submit triggers are not installed.'
                                     ' Please contact your admin.'))
            else:
                raise RuntimeError(_('Git Fusion submit triggers need updating.'
                                     ' Please contact your admin.'))

    def _raise_p4gf_perm(self):
        """User-visible permission failure."""
        p4gf_util.raise_gfuser_insufficient_perm(p4port=self.p4.port)

    def _check_lock_perm(self):
        """Permission check: see if git-fusion-user has adequate permissions to use locks."""
        # try deleting a p4 key to see if we can.  If so, we have enough rights
        # to be able to use locks.  P4 checks perms before checking if the key
        # actually exists, so it doesn't need to be a key that actually exists.
        try:
            P4Key.delete(self.p4, p4gf_const.P4GF_P4KEY_PERM_CHECK)
        except P4Exception:
            # expect a protect error if we don't have access to use keys
            if p4gf_p4msg.contains_protect_error(self.p4):
                self._raise_p4gf_perm()
                return
            if p4gf_p4msg.find_msgid(self.p4, p4gf_p4msgid.MsgDm_NoSuchKey):
                return
            raise

    def _check_protects(self):
        """Check that the protects table does not deny the Git Fusion user.

        It must either be empty or grant the Git Fusion user sufficient privileges.
        Return False if this is not the case.
        """
        if not p4gf_version_3.p4d_supports_protects(self.p4):
            self._raise_p4gf_perm()

    def check_lfs_enabled(self):
        """Validate repo configuration if processing an LFS request.

        If we're processing a Git LFS request, but the current repo is not
        configured to allow Git LFS requests, reject.

        Cannot check until after you load the repo config.
        """
        pass

    def _check_gf_depot_permissions(self):
        """Verify P4GF_USER has admin access to //P4GF_DEPOT/...

        Fetch the admin permisions and raise perm exception
        if any of the test files are not mapped.
        The set of tested files are those required the given repo_name
        """
        # init admin filter
        gf_client_map = Map()
        gf_client_map.insert("//{0}/...".format(p4gf_const.P4GF_DEPOT), "//client/...")
        gf_admin_filter = p4gf_protect.UserToProtect(self.p4).user_to_protect(
            p4gf_const.P4GF_USER).map_for_perm(p4gf_protect.ADMIN)
        gf_admin_filter = Map.join(gf_admin_filter, gf_client_map)

        # Exhaustive list of RW paths required by GF for a given repo
        files_to_test = ["//{0}/branch_info/foo",
                         "//{0}/branches/{1}/foo",
                         "//{0}/objects/repos/{1}/foo",
                         "//{0}/objects/trees/foo",
                         "//{0}/p4gf_config",
                         "//{0}/repos/{1}/p4gf_config",
                         "//{0}/repos/{1}/p4gf_config2",
                         "//{0}/users/p4gf_usermap"
                         ]
        for f in files_to_test:
            f = f.format(p4gf_const.P4GF_DEPOT,  # pylint: disable=too-many-format-args
                         self.repo_name)
            if not gf_admin_filter.includes(f):
                LOG.error("check_gf_depot_permissions FAILED test {0}".format(f))
                self._raise_p4gf_perm()

    def check_special_command(self):
        """If repo is actually a special command, run it and raise SpecialCommandException."""
        if not self.repo_name.startswith('@'):
            return
        SpecialCommandHandler.create(self).run()
        raise SpecialCommandException()

    def _check_valid_command(self):
        """Verify requested command is valid."""
        if self.command not in COMMAND_TO_PERM:
            LOG.debug("command %s not in %s", self.command, COMMAND_TO_PERM)
            raise BadRequestException(_("Unrecognized service\n"))

    def _check_user_exists(self, user):
        """Check that the user actually exists."""
        if not p4gf_p4spec.spec_exists(self.p4, 'user', user):
            form = _('User {user} does not exist in Perforce. Please contact your admin.')
            msg = form.format(user=user)
            self.record_error(msg)
            raise RuntimeError(msg)
        if not p4gf_p4spec.spec_values_match(self.p4, 'user', user, {'Type': 'standard'}):
            form = _("Perforce: User '{user}' has invalid 'Type'."
                     " User Type must be 'standard'. Please contact your admin.")
            msg = form.format(user=user)
            self.record_error(msg)
            raise RuntimeError(msg)

    def check_user_exists(self):
        """Check that the user(s) actually exist."""
        self._check_user_exists(self.user)
        if self.foruser:
            self._check_user_exists(self.foruser)

    def _raise_perms_error(self):
        """Raise an exception for insufficient permissions."""
        msg = _("User '{user}' not authorized for '{command}' on '{repo}'.").format(
            user=self.repo_perm.p4user_name, command=self.command, repo=self.repo_name)
        # if user permissions prevent the pull provide verbose message.
        if self.repo_perm.user_read_permission_checked and self.repo_perm.error_msg:
            msg += self.repo_perm.error_msg
        self.record_error(msg)
        raise CommandError(msg)

    def _check_perms(self):
        """Check that user has permission to run the command.

        If not, raise an exception.
        We use the translated internal repo name here for perm authorization
        """
        if self.skip_perms_check:
            return
        required_perm = COMMAND_TO_PERM[self.command]
        # first, check foruser if set.  This will leave self.repo_perm set
        # to RepoPerm for foruser, so any error can be reported
        if self.foruser:
            LOG.debug("_check_perms for {}".format(self.foruser))
            if not self.check_permissions(required_perm, user=self.foruser):
                self._raise_perms_error()
        # if that worked, check for authenticated user, resetting self.repo_perm
        if not self.check_permissions(required_perm):
            self._raise_perms_error()

    def check_permissions(self, required_perm, repo_name=None, user=None):
        """Check that user has permission to run the command.

        We use the translated internal repo name here for perm authorization.

        :type required_perm: str
        :param required_perm: either p4gf_group.PERM_PULL or p4gf_group.PERM_PUSH

        :type repo_name: str
        :param repo_name: name of repository, or None to use default.

        :rtype: bool
        :return: True if access permitted, False otherwise.

        """
        user = user or self.user
        if repo_name is None:
            repo_name = self.repo_name
        self.repo_perm = p4gf_group.RepoPerm.for_user_and_repo(
            self.p4, user, repo_name, required_perm)
        LOG.debug2('check_permissions() require %s perm %s', required_perm, self.repo_perm)
        if self.repo_perm.can(required_perm):  # check group permissions
            if required_perm == p4gf_group.PERM_PULL:
                # if group grants permissions - then check for user read perms
                if p4gf_read_permission.user_has_read_permissions(
                        self.p4, self.repo_perm, required_perm):
                    LOG.debug2('check_permissions() read accept perm %s', self.repo_perm)
                    return True
            else:  # PERM_PUSH
                LOG.debug2('check_permissions() push accept perm %s', self.repo_perm)
                return True
        if p4gf_read_permission.can_create_depot_repo(self.p4, repo_name):
            LOG.debug2('check_permissions() create accept perm %s', self.repo_perm)
            return True
        LOG.debug2('check_permissions() reject perm %s', self.repo_perm)
        return False

    def _write_motd(self):
        """If there is a .git-fusion/motd.txt file, return it on stderr."""
        motd = self._read_motd()
        if motd:
            sys.stderr.write(motd)

    @staticmethod
    def _read_motd():
        """If there is a message of the day file, return its contents.

        If not, return None.
        """
        p4gf_dir = p4gf_const.P4GF_HOME
        motd_file_path = p4gf_const.P4GF_MOTD_FILE.format(P4GF_DIR=p4gf_dir)
        if not os.path.exists(motd_file_path):
            return None
        with open(motd_file_path, 'r') as f:
            content = f.read()
        return content

    def _init_repo(self, ctx):
        """Create Git Fusion per-repo client view mapping and config.

        :return: True if repo created, False otherwise.

        """
        LOG.debug("ensuring repo {} is initialized".format(self.repo_name))
        repo_initer = InitRepo(self.p4, None).set_repo_name(self.repo_name)
        if repo_initer.is_init_needed():
            # Set the context first, and then the config, using the init
            # repo setters so that they are coordinated.
            repo_initer.context = ctx
            # Lazily fetch/create the repo configuration.
            repo_initer.set_repo_config(self.repo_config)
            with ExitStack() as stack:
                # Temporarily ignore SIGTERM while we take out several locks.
                stack.enter_context(ignore_sigterm())
                LOG.debug("_init_repo() initializing repo {}".format(self.repo_name))
                # Hold all the locks while initializing this repository and
                # its configuration, as well as possibly modifying the
                # group membership.
                stack.enter_context(p4gf_git_repo_lock.write_lock(self.repo_name))
                # If in read-only mode, do not block trying to acquire the
                # p4key lock and exit immediately with an error message.
                blocking = not p4gf_const.READ_ONLY
                repo_lock = p4gf_lock.RepoLock(self.p4, self.repo_name, blocking=blocking)
                try:
                    stack.enter_context(repo_lock)
                except p4gf_lock.LockBusy:
                    raise ReadOnlyInstanceException(_("Repo currently busy, try again later."))
                stack.enter_context(Timer('with Lock'))
                try:
                    repo_initer.repo_lock = repo_lock
                    # Do not permit initialization from scratch during
                    # pull/push when in read-only instance mode.
                    repo_initer.set_fail_when_read_only(True)
                    repo_created = repo_initer.init_repo()
                except InitRepoMissingView as e:
                    LOG.debug('InitRepoMissingView')
                    raise RepoNotFoundException(str(e))
                except RuntimeError as e:
                    LOG.exception('repo initialization failed')
                    raise RepoInitFailedException(str(e))
                # If authorization came from default, not explicit group
                # membership, copy that authorization to a group now. Could
                # not do this until after p4gf_init_repo() has a chance to
                # create not-yet-existing groups.
                if self.repo_perm:
                    self.repo_perm.write_if(self.p4)
                return repo_created
        if self.repo_perm and self.repo_perm.needs_write():
            # Not convinced we need the lock for modifying the group,
            # but since this will be rare, it is an acceptable cost.
            with p4gf_lock.RepoLock(self.p4, self.repo_name):
                self.repo_perm.write_if(self.p4)
        return False

    def _init_host(self, ctx):
        """Initialize the Git Fusion host for this repo, if needed."""
        if p4gf_init_host.is_init_needed(ctx.repo_dirs):
            with p4gf_git_repo_lock.write_lock(self.repo_name):
                p4gf_init_host.init_host(ctx.repo_dirs, ctx)

    @with_timer('copy to Git')
    def _copy_p2g(self, ctx):
        """Copy any recent changes from Perforce to Git."""
        try:
            # Since we fetch the configuration before we acquire the
            # exclusive lock, there is a chance that another pull/push has
            # modified/created that configuration.
            #   Now we have the exclusive lock, preventing submit to
            # config/config2, Refresh if a new file revision for config or
            # config2 submitted since we originally loaded but before we
            # acquired the lock.
            self.repo_config.refresh_if(self.p4, create_if_missing=True)
            ctx.repo_config = self.repo_config

            stream_imports = p4gf_imports.StreamImports(ctx)
            if stream_imports.missing_submodule_import_url():
                raise MissingSubmoduleImportUrlException

            p4gf_copy_p2g.copy_p2g_ctx(ctx)

            stream_imports.process()

            # Now is also an appropriate time to clear out any stale Git
            # Swarm reviews. We're pre-pull, pre-push, time when we've
            # got exclusive write access to the Git repo,
            GSReviewCollection.delete_refs_for_closed_reviews(ctx)
            p4gf_git.set_bare(True)

        except:
            # Dump failure to log, BEFORE cleanup, just in case
            # cleanup ALSO fails and throws its own error (which
            # happens if we're out of memory).
            LOG.exception('copy p2g failed')
            p4gf_lock_status.print_lock_status(ctx.p4gf, p4gf_util.get_server_id(), LOG.error)
            raise

    def _increment_push_counter(self, ctx):
        """Increment the push counter value now that the push is underway."""
        # Increment if we are getting the actual push payload.
        if self.push_received():
            id_key_name = P4Key.calc_repo_push_id_p4key_name(self.repo_name)
            push_id = P4Key.increment(ctx.p4gf, id_key_name)
            msg = _("Push {push_id} started").format(push_id=push_id)
            ctx.record_push_status_p4key(msg)

    def _lock_transfer_cb(self):
        """Lock ownership was transferred successfully."""
        # Lock was successfully transferred to the background process, do
        # _not_ release the atomic lock in this "foreground" process.
        LOG.debug2('canceling repo lock removal')
        self._should_remove_atomic_lock = False

    def _maybe_remove_atomic_lock(self, ctx):
        """Remove the atomic lock for the repo."""
        if self._should_remove_atomic_lock:
            LOG.debug('removing atomic-lock for %s', self.repo_name)
            # In the event of a preflight error, it is highly likely the
            # connection has been dropped, so temporarily reconnect.
            with ExitStack() as stack:
                if not ctx.p4gf.connected():
                    p4gf_create_p4.p4_connect(ctx.p4gf)
                    stack.callback(p4gf_create_p4.p4_disconnect, ctx.p4gf)
                p4gf_atomic_lock.lock_update_repo_reviews(ctx, action=p4gf_atomic_lock.REMOVE)

    def _call_git(self, ctx):
        """Delegate to the appropriate Git command defined in git_caller.

        Call git (e.g. git-upload-pack, git-receive-pack) while keeping reviews updated.

        Returns the exit code of the Git command.

        """
        # Record the event of transferring information (i.e. audit log).
        self.record_access()

        retval = None

        # Detach git repo's HEAD before calling original git, otherwise we
        # won't be able to push the current branch (if any).
        if not p4gf_git.is_bare_git_repo():
            p4gf_git.checkout_detached_head()

        # Flush stderr before returning control to Git. Otherwise Git's own
        # output might interrupt ours.
        sys.stderr.flush()

        # Keep the idle P4 connection open during call to git since we have
        # to keep the temporary client (and likewise the lock) alive.
        LOG.debug('_call_git() delegating to git for {}'.format(ctx.config.repo_name))

        # Ignore the SIGTERM signal during this particularly delicate stage
        # in the process. Allowing this signal to get through to Git could
        # possibly result in the pre-receive hook running but not the post-
        # receive hook, which may leave the git references ahead of the
        # last translated commit. This is very difficult to detect and
        # prevent after the fact. For instance, HTTP push seemingly always
        # ends up with Git references that are ahead of Perforce changes.
        # The same happens with a push that introduces a change rejected by
        # a trigger.
        with ignore_sigterm():
            retval = self.git_caller(ctx)  # pylint:disable=not-callable

        LOG.debug('_call_git() returning {}'.format(retval))
        return retval


# functions and log messages for tag or branch rollback in _rollback_prt().
RollbackHow = namedtuple("RollbackHow",
    [ "log_nop"
    , "log_force",  "func_force"
    , "log_delete", "func_delete"
    ])
_ROLLBACK_BRANCH = RollbackHow(
      "git branch no change required"
    , "git branch -f {ref:<20} {old_sha1}", p4gf_git.force_branch_ref
    , "git branch -D {ref:<20}",            p4gf_git.delete_branch_ref )
_ROLLBACK_TAG = RollbackHow(
      "git tag no change required"
    , "git tag -f {ref:<20} {old_sha1}", p4gf_git.force_tag_ref
    , "git tag -d {ref:<20}",            p4gf_git.delete_tag_ref )


def _rollback_prt(prt, how):
    """Roll one tag or branch ref back, with logging."""
    curr_sha1 = p4gf_util.git_rev_list_1(prt.ref)
    if curr_sha1 is None:
        curr_sha1 = p4gf_const.NULL_COMMIT_SHA1
    prefix = "rollback: prt={prt:<60} curr={curr_sha1} "\
             .format(prt=prt, curr_sha1=p4gf_util.abbrev(curr_sha1))
    # Surely we have a "strip /refs/xxx/ prefix" utility function somewhere?
    short_ref = prt.ref
    for p in ["refs/heads/", "refs/tags/"]:
        if short_ref.startswith(p):
            short_ref = short_ref[len(p):]
            break
    if curr_sha1 == prt.old_sha1:
        LOG.warning(prefix + how.log_nop)
    elif prt.old_sha1 == p4gf_const.NULL_COMMIT_SHA1:
        LOG.warning(prefix + how.log_delete.format(ref=short_ref))
        how.func_delete(short_ref)
    else:
        LOG.warning(prefix + how.log_force.format(ref=short_ref, old_sha1=prt.old_sha1))
        how.func_force(short_ref, prt.old_sha1)


def rollback_prl(ctx, blocking):
    """If a prl file exists, then a previous push has failed. Git might have
    refs that point to history not yet copied to Perforce. Roll those
    refs back to where they were before the previous, failed, push.

    NOP if no prl file: post-receive took over and (should have) finished
    the translation to Perforce.

    :param blocking: if True, will block until performing rollback,
                      if false, raise LockBusy.

    Returns with Git repo checked out to master-ish branch (if any),
    since rollback_prl() has to detach head to change any branch refs.
    """
    prlfile = PRLFile(ctx.config.repo_name)
    prl = prlfile.read()
    if not prl:
        return
    with p4gf_git_repo_lock.write_lock(ctx.config.repo_name, upgrade=False, blocking=blocking):
        # When we can get the lock, then log as an error, otherwise it
        # means we had lock contention, which is not an error.
        LOG.warning("rollback: checking Git refs after previous push failed: {}"
                    .format(ctx.config.repo_name))
        # Re-fetch prl AFTER we hold the lock. Can't trust pre-lock data
        # that some other process might have changed out from under us.
        prl = prlfile.read()
        if not prl:
            return
        if not p4gf_git.is_bare_git_repo():
            p4gf_git.checkout_detached_head()
        for prt in prl.heads():
            _rollback_prt(prt, _ROLLBACK_BRANCH)
        for prt in prl.tags():
            _rollback_prt(prt, _ROLLBACK_TAG)
        prlfile.delete()
        if not p4gf_git.is_bare_git_repo():
            ctx.checkout_master_ish()


@contextmanager
def run_before_after(server):
    """Wrap something with calls to before() and after()."""
    server.before()
    try:
        yield
    finally:
        server.after()


@contextmanager
def log_start_end(server):
    """Log the start and end of the processing of a request."""
    LOG.info('process-start of {} for {} on {}'.format(
        server.command, server.user, server.repo_name_git))
    try:
        yield
    finally:
        LOG.info('process-end of {} for {} on {}'.format(
            server.command, server.user, server.repo_name_git))


@contextmanager
def gc_debug():
    """Yield to caller with garbage collection debugging, if enabled."""
    p4gf_mem_gc.init_gc()
    try:
        yield
    finally:
        p4gf_mem_gc.process_garbage(NTR('at end of server process'))


@contextmanager
def raise_on_sigterm():
    """Raise an exception if the SIGTERM signal is received."""
    def raise_exception(_signum, _frame):
        """Raise an exception."""
        LOG.exception("SIGTERM received. Raising exception.")
        # Avoid receiving another SIGTERM again while we are trying to exit
        # cleanly already, so ignore any further SIGTERM signals.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise TerminatingException('SIGTERM received')
    LOG.debug("raise_on_sigterm() will raise exception on SIGTERM")
    term_handler = signal.signal(signal.SIGTERM, raise_exception)
    try:
        yield
    finally:
        if term_handler:
            signal.signal(signal.SIGTERM, term_handler)
        else:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)


@contextmanager
def ignore_sigterm():
    """Temporarily ignore the SIGTERM signal."""
    if 'REMOTE_ADDR' in os.environ:
        LOG.debug("ignore_sigterm() not ignoring SIGTERM because http")
        # For HTTP, secretly do nothing as we really need to exit when
        # Apache sends us a SIGTERM, lest it employs SIGKILL after we fail
        # to politely shut down in a timely manner.
        yield
    else:
        LOG.debug("ignore_sigterm() ignoring SIGTERM signal")
        term_handler = signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            yield
        finally:
            if term_handler:
                signal.signal(signal.SIGTERM, term_handler)
            else:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)


class StatusReporter:

    """Reports current repo status occassionally."""

    def __init__(self, p4, repo_name, wait_push_id, active_push_id):
        """Construct a new instance with the given wait time."""
        self.last_time = time.time()
        self.p4 = p4
        self.repo_name = repo_name
        self.wait_push_id = wait_push_id
        self.active_push_id = active_push_id
        self.last_status = None

    def maybe_report(self):
        """If it has been a while, report the current status."""
        new_time = time.time()
        # check for updated status every 5 seconds
        if new_time - self.last_time > 5:
            self.last_time = new_time
            msg = _status_for_push(self.p4, self.repo_name, self.wait_push_id, self.active_push_id)
            if msg != self.last_status:
                self.last_status = msg
                sys.stderr.write("{}\n".format(msg))
                sys.stderr.flush()


class SpecialCommandHandler:

    """Base class for handlers of special commands."""

    def __init__(self, server):
        """Init the handler."""
        self.server = server
        self.repo_name = server.repo_name
        self.p4 = server.p4
        self.user = server.user
        self.foruser = server.foruser

    @staticmethod
    def create(server):
        """Factory method for SpecialCommandHandler.

        :param server: instance of Server.
        :return: instance of SpecialCommandHandler.

        """
        special_names = [
            (p4gf_const.P4GF_UNREPO_HELP,     SpecialCommandHelp),
            (p4gf_const.P4GF_UNREPO_INFO,     SpecialCommandInfo),
            (p4gf_const.P4GF_UNREPO_LIST,     SpecialCommandList),
            (p4gf_const.P4GF_UNREPO_FEATURES, SpecialCommandFeatures),
            (NTR("@mirror_wait"),             SpecialCommandMirrorWait)  # deprecated special
        ]
        command = server.repo_name
        for (s, f) in special_names:
            if s == command:
                return f(server)

        special_patterns = [
            (p4gf_const.P4GF_UNREPO_WAIT,     SpecialCommandWaitRepo),
            (p4gf_const.P4GF_UNREPO_PROGRESS, SpecialCommandProgressRepo),
            (p4gf_const.P4GF_UNREPO_FEATURES, SpecialCommandFeatureRepo),
            (p4gf_const.P4GF_UNREPO_STATUS,   SpecialCommandStatusRepo),
            (p4gf_const.P4GF_UNREPO_CONFIG,   SpecialCommandConfigRepo)
        ]
        for (s, f) in special_patterns:
            if re.compile("^" + s + "@").match(command):
                repo_name = command[len(s)+1:]
                try:
                    repo_name, wait_id, active_id = _parse_push_identifiers(server.p4, repo_name)
                except RuntimeError as e:
                    # use the normal process to report the error
                    return SpecialCommandError(server, s, str(e))
                return f(server, repo_name, wait_id, active_id)

        # Did not match any known command, fall back to listing supported commands.
        return SpecialCommandHandler(server)

    def run(self):
        """Print the list of special commands."""
        # pylint:disable=no-self-use
        special_cmds = " ".join(p4gf_const.P4GF_UNREPO)
        sys.stderr.write(
            _("Git Fusion: unrecognized special command.\n"
              "Valid commands are: {commands}\n")
            .format(commands=special_cmds))


class SpecialCommandError(SpecialCommandHandler):

    """Reports an error in parsing the special command."""

    def __init__(self, server, command, error_msg):
        """Init the handler."""
        SpecialCommandHandler.__init__(self, server)
        self.command = command
        self.error_msg = error_msg

    def run(self):
        """Print the error message and write an error to the log."""
        LOG.warning("{} unable to proceed: {}".format(self.command, self.error_msg))
        sys.stderr.write(self.error_msg + "\n")


class SpecialCommandHelp(SpecialCommandHandler):

    """Processes the '@help' special command."""

    def run(self):
        """Dump the contents of the help.txt file, if it exists."""
        # pylint:disable=no-self-use
        help_text = p4gf_util.read_bin_file('help.txt')
        if help_text is False:
            sys.stderr.write(_("file 'help.txt' not found\n"))
        else:
            sys.stderr.write(help_text)


class SpecialCommandInfo(SpecialCommandHandler):

    """Processes the '@info' special command."""

    def run(self):
        """Print version information to stderr."""
        sys.stderr.write(p4gf_version_3.as_string_extended(p4=self.p4, include_checksum=True))


class SpecialCommandList(SpecialCommandHandler):

    """Processes the '@list' special command."""

    def run(self):
        """Print a list of known repositories to stderr."""
        def _merge_perm(p1, p2):
            """Return the lower permission of p1 and p2."""
            if p1 == NTR('pull') or p2 == NTR('pull'):
                return NTR('pull')
            return NTR('push')

        def _merge_lists(rl1, rl2):
            """merge two repo lists.

            Result contains only repos that are in both lists.
            perm for each repo in result is the lower of that from the two lists.
            """
            result = []
            for r1 in rl1:
                for r2 in rl2:
                    if r1[0] != r2[0]:
                        continue
                    result.append([
                        r1[0],
                        _merge_perm(r1[1], r2[1]),
                        r1[2],
                        r1[3]
                    ])
            result.sort(key=lambda tup: tup[0])
            return result

        def _format_repo(r):
            """format info for a single repo."""
            # pylint can't see the nested {width}
            # pylint:disable=unused-format-string-argument
            return ("{name:<{width}} {perm} {charset:<10} {desc}"
                    .format(width=width,
                            name=p4gf_translate.TranslateReponame.repo_to_git(r[0]),
                            perm=r[1],
                            charset=r[2],
                            desc=r[3]))

        try:
            self.server.check_user_exists()

            repos = RepoList.list_for_user(self.p4, self.user).repos
            if self.foruser:
                repos2 = RepoList.list_for_user(self.p4, self.foruser).repos
                repos = _merge_lists(repos, repos2)
            if len(repos):
                width = max(len(r[0]) for r in repos)
                sys.stderr.write("\n".join([_format_repo(r) for r in repos]) + "\n")
            else:
                sys.stderr.write(_('no repositories found\n'))
        except RuntimeError:
            LOG.exception('repo list retrieval failed')
            sys.stderr.write(_('no repositories found\n'))


class SpecialCommandFeatures(SpecialCommandHandler):

    """Processes the '@features' special command."""

    def run(self):
        """Print a list of all available features."""
        # pylint:disable=no-self-use
        sys.stderr.write(_('Available features:\n'))
        for k in p4gf_config.configurable_features():
            sys.stderr.write("{} : {}\n".format(k, p4gf_config.FEATURE_KEYS[k]))


class SpecialCommandHandlerRepo(SpecialCommandHandler):

    """Base class for special commands targeting a single repo.

    Some (but not all) of these commands also take wait_id and active_id.
    """

    def __init__(self, server, repo_name, wait_id, active_id):
        """Init the handler."""
        SpecialCommandHandler.__init__(self, server)
        self.repo_name = repo_name
        self.wait_push_id = wait_id
        self.active_push_id = active_id


class SpecialCommandProgressRepo(SpecialCommandHandlerRepo):

    """Processes the '@progress@repo' special command."""

    def run(self):
        """Wait for a repo lock and report changes to the repo status."""
        if self.server.check_permissions(p4gf_group.PERM_PULL, self.repo_name):
            _wait_for_push(self.p4, self.repo_name, self.wait_push_id, self.active_push_id,
                           progress=True)
        else:
            LOG.warning("@progress unable to report status for {} to {}".format(
                self.repo_name, self.user))
            sys.stderr.write(_('Status not available due to permissions\n'))


class SpecialCommandWaitRepo(SpecialCommandHandlerRepo):

    """Processes the '@wait@repo' special command."""

    def run(self):
        """Wait for repo lock to be released."""
        if self.server.check_permissions(p4gf_group.PERM_PULL, self.repo_name):
            _wait_for_push(self.p4, self.repo_name, self.wait_push_id, self.active_push_id)
        else:
            LOG.warning("@wait unable to report status for {} to {}".format(
                self.repo_name, self.user))
            sys.stderr.write(_('Status not available due to permissions\n'))


class SpecialCommandFeatureRepo(SpecialCommandHandlerRepo):

    """Processes the '@features@repo' special command."""

    def run(self):
        """Report which features are enabled for a repo."""
        config = p4gf_config.RepoConfig.from_depot_file(self.repo_name, self.p4)
        sys.stderr.write(_("Enabled features for repo '{repo_name}':\n")
                         .format(repo_name=self.repo_name))
        for k in p4gf_config.configurable_features():
            sys.stderr.write("{} : {}\n".format(
                k, config.is_feature_enabled(k)))


class SpecialCommandStatusRepo(SpecialCommandHandlerRepo):

    """Processes the '@status@repo' special command."""

    def run(self):
        """Show the status of a push operation."""
        if self.server.check_permissions(p4gf_group.PERM_PULL, self.repo_name):
            msg = _status_for_push(self.p4, self.repo_name, self.wait_push_id, self.active_push_id)
            sys.stderr.write("{}\n".format(msg))
        else:
            LOG.warning("@status denied for {} on {}".format(self.user, self.repo_name))
            sys.stderr.write(_('Permission denied for @status\n'))


class SpecialCommandConfigRepo(SpecialCommandHandlerRepo):

    """Dump the configuration of the named repository."""

    def run(self):
        """Dump the repository configuration."""
        if self.server.check_permissions(p4gf_group.PERM_PULL, self.repo_name):
            try:
                config = p4gf_config.RepoConfig.from_depot_file(self.repo_name, self.p4)
                config_content = p4gf_config.to_text('', config.repo_config)
                sys.stderr.write(config_content)
            except p4gf_config.ConfigLoadError as err:
                sys.stderr.write(_('Unable to load configuration: {}\n').format(err))
        else:
            LOG.warning("@config denied for {} on {}".format(self.user, self.repo_name))
            sys.stderr.write(_('Permission denied for @config\n'))


class SpecialCommandMirrorWait(SpecialCommandHandler):

    """Processes the '@mirror_wait' special command."""

    def run(self):
        """Warn user that @mirror_wait is no longer implemented."""
        # pylint:disable=no-self-use
        sys.stderr.write(_('@mirror_wait is no longer implemented, try @wait instead\n'))


def _status_for_push(p4, repo_name, wait_push_id, active_push_id):
    """Return the status for the requested push, or something reasonable.

    :param p4: instance of P4API.
    :param repo_name: name fo the repository for which to retrieve status.
    :param wait_push_id: push identifier of interest.
    :param active_push_id: identifier of the active push.

    """
    # check for the push-specific status key, if any
    n_key_name = P4Key.calc_repo_status_p4key_name(repo_name, wait_push_id)
    result = P4Key.get(p4, n_key_name)
    if result and result != '0':
        return result
    # otherwise, retrieve the current status, if appropriate
    if wait_push_id == active_push_id:
        key_name = P4Key.calc_repo_status_p4key_name(repo_name)
        result = P4Key.get(p4, key_name)
        if result and result != '0':
            return result
    # and if that didn't work, just return some generic message
    if wait_push_id != '0':
        # try to keep this text the same as in record_push_success_p4key()
        return _('Push {push_id} completed successfully').format(
            push_id=wait_push_id)
    return _('No status available')


def _parse_push_identifiers(p4, repo_name):
    """Return a tuple of adjusted repository name, requested push id, and active push id.

    :param p4: instance of P4API.
    :param repo_name: name fo the repository for which to retrieve status; may contain
                      the requested push identifier (e.g. repo_name@push_id).

    Returns a 3-tuple of repository name, requested push identifer, and active push id.
    If no push identifier was requested, the active push identifier is substituted.
    The returned repository name will also have been translated.

    """
    if '@' in repo_name:
        (repo_name, wait_push_id) = repo_name.rsplit('@', 1)
        try:
            if int(wait_push_id) < 1:
                raise ValueError()
        except ValueError:
            raise RuntimeError(_("Push identifier must be a positive integer."))
    else:
        wait_push_id = None
    repo_name = p4gf_translate.TranslateReponame.url_to_repo(repo_name, p4)
    key_name = P4Key.calc_repo_push_id_p4key_name(repo_name)
    active_push_id = P4Key.get(p4, key_name)
    if wait_push_id is None:
        wait_push_id = active_push_id
    elif int(wait_push_id) > int(active_push_id):
        raise RuntimeError(_("Push {push_id} unrecognized\n")
                           .format(push_id=wait_push_id))
    return repo_name, wait_push_id, active_push_id


def _wait_for_push(p4, repo_name, wait_push_id, active_push_id, progress=False):
    """Wait for the completion of a push on a particular repository.

    :param p4: P4API instance.
    :param str repo_name: name of repository.
    :param int wait_push_id: push identifier on which to wait.
    :param int active_push_id: identifier of active push.
    :param bool progress: True to display progress of push periodically.

    """
    if wait_push_id == active_push_id:
        LOG.debug("checking for active lock on {}".format(repo_name))
        # quick check to see if the lock is held or not
        lock_key_name = p4gf_const.P4GF_P4KEY_LOCK_VIEW_OWNERS.format(repo_name=repo_name)
        lock_value = P4Key.get(p4, lock_key_name)
        if lock_value and lock_value != '0':
            LOG.debug("waiting for lock on {}".format(repo_name))
            # wait for either the lock status or the active push id to change
            sys.stderr.write(_("Waiting for push {push_id}...\n".format(push_id=wait_push_id)))
            sys.stderr.flush()
            key_name = P4Key.calc_repo_push_id_p4key_name(repo_name)
            label = "@wait on {}".format(repo_name)
            wait_reporter = p4gf_log.LongWaitReporter(label, LOG)
            status_reporter = StatusReporter(p4, repo_name, wait_push_id, active_push_id)
            while lock_value and lock_value != '0' and wait_push_id == active_push_id:
                time.sleep(1)
                lock_value = P4Key.get(p4, lock_key_name)
                active_push_id = P4Key.get(p4, key_name)
                wait_reporter.been_waiting()
                if progress:
                    status_reporter.maybe_report()
            LOG.debug("active push changed or lock released for {}".format(
                repo_name))
    # retrieve the status while we're here
    msg = _status_for_push(p4, repo_name, wait_push_id, active_push_id)
    sys.stderr.write("{}\n".format(msg))


def _set_pre_receive_flag(ctx):
    """Write a file that pre-receive hook should delete when it runs."""
    fname = os.path.join(ctx.repo_dirs.repo_container, p4gf_const.P4GF_PRE_RECEIVE_FLAG)
    with open(fname, 'w') as fobj:
        fobj.write(str(os.getpid()))


def _detect_pre_receive_flag(ctx):
    """Check for the file that indicates whether pre-receive ran or not.

    :return: True if file still exists, indicating pre-receive did _not_ run.

    """
    fname = os.path.join(ctx.repo_dirs.repo_container, p4gf_const.P4GF_PRE_RECEIVE_FLAG)
    return os.path.exists(fname)

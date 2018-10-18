#! /usr/bin/env python3.3
"""Functions for enforcing push limits on Git Fusion repositories.

See the doc/design-doc/Push_Limits.md specification for details.
"""

from contextlib import ExitStack
import logging
import sys

import P4
import pygit2

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_config
import p4gf_const
import p4gf_context
import p4gf_copy_p2g
import p4gf_create_p4
import p4gf_git
from p4gf_l10n import _, NTR
import p4gf_lock
import p4gf_p4key
import p4gf_proc
import p4gf_pygit2
import p4gf_translate
import p4gf_util

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_push_limits")


class PushLimitException(Exception):

    """This exception is raised when a push limit has been exceeded."""

    pass


class PushLimits:

    """Class for handling push limits."""

    def __init__(self, ctx):
        self.ctx            = ctx
        self.p4             = ctx.p4gf              # P4 instance for retreiving config files
        self.repo           = ctx.repo              # pygit2.Repository instance
        self.repo_name      = ctx.config.repo_name

        # read some config settings
        self.commit_limit   = self._quota_int(p4gf_config.KEY_COMMIT_LIMIT)
        self.file_limit     = self._quota_int(p4gf_config.KEY_FILE_LIMIT)
        self.space_limit    = self._quota_int(p4gf_config.KEY_SPACE_LIMIT)
        self.received_limit = self._quota_int(p4gf_config.KEY_RECEIVED_LIMIT)
        # do _not_ compute the disk usage until it is truly needed
        self._space_total   = None

        self._space_remaining_mb = None

    def enforce(self, prl):
        """Enforce the push limits, if any are defined.

        :param prl: list of pre-receive tuples.

        """
        self._enforce_commits_and_files(prl)
        self._enforce_disk_usage()
        with self._space_lock():
            self._enforce_overall_usage()

    def pre_copy(self):
        """Update the value for the pending_mb key for the given repo."""
        # Do nothing if limits are not enabled. Yes, this means that if
        # limits are later enabled, the next push will behave as if the
        # entire repo was received as one push, but that is unavoidable.
        if self.space_limit or self.received_limit or self.space_remaining:
            with self._space_lock():
                previous_total = self.get_total_mb()
                space_total = self.space_total
                if space_total == 0:
                    # unable to enforce limits, already logged
                    return
                recieved_mb = space_total - previous_total
                self._set_key(p4gf_const.P4GF_P4KEY_PENDING_MB, recieved_mb)
                self._adjust_space_pending_mb(recieved_mb)

    def post_copy(self):
        """Update the values for the total_mb and pending_mb keys for this repo.

        This function should be invoked after the translation of the Git commits
        to Perforce changes has been completed successfully.

        """
        # Do nothing if limits are not enabled.
        if self.space_limit or self.received_limit or self.space_remaining:
            with self._space_lock():
                previous_total = self.get_total_mb()
                space_total = self.space_total
                if space_total == 0:
                    # unable to enforce limits, already logged
                    return
                recieved_mb = space_total - previous_total
                self._adjust_space_pending_mb(-recieved_mb)
                self.space_remaining -= recieved_mb
            self._set_key(p4gf_const.P4GF_P4KEY_TOTAL_MB, space_total)
            self._set_key(p4gf_const.P4GF_P4KEY_PENDING_MB, 0)

    @staticmethod
    def push_failed(ctx):
        """Reset the key values in the event that a push failed."""
        # pylint: disable=W0212
        # Access to a protected member
        limits = PushLimits(ctx)
        pending_mb = limits.get_pending_mb()
        limits._set_key(p4gf_const.P4GF_P4KEY_PENDING_MB, 0)
        with limits._space_lock():
            limits._adjust_space_pending_mb(-pending_mb)

    def _enforce_commits_and_files(self, prl):
        """Enforce the file and commits push limits, if any are defined.

        :param prl: list of pre-receive tuples.

        Raises a PushLimitException if a limit has been exceeded.

        """
        # Do we require any push commit or file limit enforcement?
        if not (self.commit_limit or self.file_limit):
            return
        # Yes, need to count at least the commits, and maybe the files, too.
        commit_total = 0
        file_total = 0
        count_files = self.file_limit is not None
        for prt in prl:
            commit_count, file_count = self._count_commits(prt.old_sha1, prt.new_sha1, count_files)
            commit_total += commit_count
            file_total += file_count

        if self.commit_limit:
            LOG.debug('enforce() found {} commits'.format(commit_total))
            if commit_total > self.commit_limit:
                raise PushLimitException(
                    _("Push to repo {repo_name} rejected, commit limit exceeded")
                    .format(repo_name=self.repo_name))

        if self.file_limit:
            LOG.debug('enforce() found {} files'.format(file_total))
            if file_total > self.file_limit:
                raise PushLimitException(
                    _("Push to repo {repo_name} rejected, file limit exceeded")
                    .format(repo_name=self.repo_name))

    def _enforce_overall_usage(self):
        """Enforce the overall disk usage limits, if any are defined."""
        remaining_mb = self.space_remaining
        if remaining_mb <= 0:
            return
        LOG.debug('_enforce_overall_usage() remaining {}'.format(remaining_mb))
        pending_mb = self.get_pending_mb()
        previous_total = self.get_total_mb()
        space_total = self.space_total
        if space_total == 0:
            # unable to enforce limits, already logged
            return
        recieved_mb = space_total - pending_mb - previous_total
        space_pending_mb = self._get_space_pending_mb()
        if (space_pending_mb + recieved_mb) > remaining_mb:
            # Remove the newly introduced, unreferenced, commits so that
            # the next push has a chance of succeeding.
            p4gf_proc.popen(['git', '--git-dir=' + self.repo.path, 'prune'])
            raise PushLimitException(
                _("Push to repo {repo_name} rejected, remaining space exhausted")
                .format(repo_name=self.repo_name))

    def _enforce_disk_usage(self):
        """Enforce the total and received megabytes push limits, if any are defined.

        Raises a PushLimitException if a limit has been exceeded.
        """
        if not (self.space_limit or self.received_limit):
            return

        pending_mb = self.get_pending_mb()

        if self.space_limit:
            LOG.debug('enforce() measured {0:.2f}M disk usage'.format(self.space_total))
            if (self.space_total + pending_mb) > self.space_limit:
                # Remove the newly introduced, unreferenced, commits so
                # that the next push has a chance of succeeding.
                p4gf_proc.popen(['git', '--git-dir=' + self.repo.path, 'prune'])
                raise PushLimitException(
                    _("Push to repo {repo_name} rejected, space limit exceeded")
                    .format(repo_name=self.repo_name))

        if self.received_limit:
            previous_total = self.get_total_mb()
            space_total = self.space_total
            if space_total == 0:
                # unable to enforce limits, already logged
                return
            recieved_mb = space_total - pending_mb - previous_total
            LOG.debug('enforce() measured {0:.2f}M received'.format(recieved_mb))
            if recieved_mb > self.received_limit:
                # Remove the newly introduced, unreferenced, commits so
                # that the next push has a chance of succeeding.
                p4gf_proc.popen(['git', '--git-dir=' + self.repo.path, 'prune'])
                raise PushLimitException(
                    _("Push to repo {repo_name} rejected, received limit exceeded")
                    .format(repo_name=self.repo_name))

    def _quota_int(self, key):
        """Read a value from the quota section of the config.

        :type key: str
        :param key: key to config value with quota section.
        :return: None if value is not set.
        :raises PushLimitException: if config value is malformed.

        """
        try:
            return self.ctx.repo_config.getint(p4gf_config.SECTION_QUOTA, key)
        except ValueError:
            raise PushLimitException(
                _('p4gf_config: {section}/{key} not a valid natural number')
                .format(section=p4gf_config.SECTION_QUOTA, key=key))

    def _set_key(self, key, value):
        """Set a key."""
        if '{repo_name}' in key:
            key = key.format(repo_name=self.repo_name)
        p4gf_p4key.set(self.p4, key, str(value))

    def _get_key(self, key, value_type=None, default=None):
        """Get a key.

        If value_type is specified, convert result to type.
        If key is empty/missing, default will be returned.

        Will raise TypeError if key contains a value that can't convert to type.

        """
        if '{repo_name}' in key:
            key = key.format(repo_name=self.repo_name)
        value = p4gf_p4key.get(self.p4, key) or default

        # will raise TypeError if can't convert to requested type
        if value_type and value is not None:
            value = value_type(value)
        return value

    def _count_commits(self, since=None, until=None, count_files=False):
        """Count the number commits from since to until.

        :param since: earliest commit SHA1 to visit.
        :param until: latest commit SHA1 to visit.
        :param count_files: if True, also count the files added.

        Returns the number of commits and files introduced within the range of
        commits. The number of files will be zero unless count_files is true.

        """
        if int(since, 16) == 0:
            since = None
        if int(until, 16) == 0:
            until = None
        start = self.repo.get(until) if until else p4gf_pygit2.head_commit(self.repo)
        if start is None:
            raise RuntimeError(_("Missing starting commit"))
        sort = pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_TIME
        commit_count = 0
        file_count = 0
        for commit in self.repo.walk(start.oid, sort):
            if since and p4gf_pygit2.object_to_sha1(commit) == since:
                break
            commit_count += 1
            if count_files:
                if commit.parents:
                    for parent in commit.parents:
                        file_count += self._compare_commit_with_parent(parent, commit)
                else:
                    file_count += self._count_files_for_commit(commit)
        return commit_count, file_count

    def _compare_commit_with_parent(self, commit_a, commit_b):
        """Find the number of added files between two commits."""
        file_count = 0

        def count_files(old, new):
            """Count the files in a tree."""
            # pylint:disable=unused-variable,undefined-variable
            nonlocal file_count
            if old is None and new.filemode != 0o040000:
                file_count += 1

        p4gf_git.diff_trees(self.repo, commit_a.tree, commit_b.tree, count_files)
        return file_count

    def _count_files_for_commit(self, commit):
        """For the given commit, return number of files found."""
        file_count = 0

        def count_files(_entry):
            """Count the files in a tree."""
            # pylint:disable=unused-variable,undefined-variable
            nonlocal file_count
            file_count += 1

        p4gf_git.visit_tree(self.repo, commit.tree, count_files)
        return file_count

    @property
    def space_total(self):
        """Return the disk usage in megabytes of the repository as a float."""
        # Allow _space_total to be 0, that is perfectly legal.
        if self._space_total is None:
            # self._space_total = self._get_disk_usage()
            # $ /usr/bin/du -sk .git
            # 337524    .git
            result = p4gf_proc.popen_no_throw(['/usr/bin/du', '-sk', self.repo.path])
            if result['ec'] == 0:
                self._space_total = int(result['out'].split()[0]) / 1024
            else:
                LOG.error("Push limits not enforced: unable to get disk usage for {}".format(
                    self.repo.path))
                self._space_total = 0
        return self._space_total

    def get_pending_mb(self):
        """Retrieve the value for the pending_mb key.

        Returns a floating point number, possibly zero.
        """
        return self._get_key(p4gf_const.P4GF_P4KEY_PENDING_MB, float, 0)

    def get_total_mb(self):
        """Retrieve the value for the total_mb key.

        Returns a floating point number, possibly zero.
        """
        return self._get_key(p4gf_const.P4GF_P4KEY_TOTAL_MB, float, 0)

    @property
    def space_remaining(self):
        """Retrieve the value for the git-fusion-space-remaining-mb key.

        :return: a floating point number, possibly zero.

        """
        if self._space_remaining_mb is None:
            self._space_remaining_mb = self._get_key(
                p4gf_const.P4GF_P4KEY_ALL_REMAINING_MB, float, 0)
        return self._space_remaining_mb

    def _get_space_pending_mb(self):
        """Retrieve the value for the git-fusion-space-pending-mb key.

        :return: a floating point number, possibly zero.
        """
        return self._get_key(p4gf_const.P4GF_P4KEY_ALL_PENDING_MB, float, 0)

    @space_remaining.setter
    def space_remaining(self, value):
        """Update the value for the git-fusion-space-remaining-mb key.

        :param float value: new value for remaining space (in MB).

        """
        # Only adjust the value if the key has been set to something non-zero.
        if self.space_remaining > 0:
            # Do _not_ let the remaining value go to zero, otherwise it disables the check!
            new_remaining_mb = max(0.001, value)
            self._space_remaining_mb = new_remaining_mb
            LOG.debug('space_remaining.setter() remaining: {}'.format(new_remaining_mb))
            self._set_key(p4gf_const.P4GF_P4KEY_ALL_REMAINING_MB, new_remaining_mb)

    def _adjust_space_pending_mb(self, adjustment):
        """Retrieve the value for the git-fusion-space-pending-mb key.

        :type adjustment: float
        :param adjustment: value (positive or negative) to add to key value.

        """
        space_pending_mb = self._get_space_pending_mb()
        new_pending_mb = max(0.0, space_pending_mb + adjustment)
        LOG.debug('_adjust_space_pending_mb() remaining: {}'.format(new_pending_mb))
        self._set_key(p4gf_const.P4GF_P4KEY_ALL_PENDING_MB, new_pending_mb)

    def _space_lock(self):
        """Acquire the space lock.

        :rtype: :class:`p4gf_lock.P4KeyLock`
        :return: the space lock.
        """
        return p4gf_lock.SimpleLock(self.p4, p4gf_const.P4GF_P4KEY_LOCK_SPACE)


def main():
    """Update the disk usage p4 keys for one or more repositories."""
    desc = _("Set/reset the total and pending p4 keys.")
    epilog = _("Without the -y/--reset option, only displays current values.")
    parser = p4gf_util.create_arg_parser(desc, epilog=epilog)
    parser.add_argument('-a', '--all', action='store_true',
                        help=_('process all known Git Fusion repositories'))
    parser.add_argument('-y', '--reset', action='store_true',
                        help=_('perform the reset of the p4 keys'))
    parser.add_argument(NTR('repos'), metavar=NTR('repo'), nargs='*',
                        help=_('name of repository to be updated'))
    args = parser.parse_args()

    # Check that either --all, or 'repos' was specified.
    if not args.all and len(args.repos) == 0:
        sys.stderr.write(_('Missing repo names; try adding --all option.\n'))
        sys.exit(2)
    if args.all and len(args.repos) > 0:
        sys.stderr.write(_('Ambiguous arguments. Choose --all or a repo name.\n'))
        sys.exit(2)

    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4_temp_client()
        if not p4:
            sys.exit(2)
        # Sanity check the connection (e.g. user logged in?) before proceeding.
        try:
            p4.fetch_client()
        except P4.P4Exception as e:
            sys.stderr.write(_('P4 exception occurred: {exception}').format(exception=e))
            sys.exit(1)

        if args.all:
            repos = p4gf_util.repo_config_list(p4)
            if len(repos) == 0:
                print(_('No Git Fusion repositories found, nothing to do.'))
                sys.exit(0)
        else:
            repos = args.repos
        p4gf_create_p4.p4_disconnect(p4)

        for repo in repos:
            repo_name = p4gf_translate.TranslateReponame.git_to_repo(repo)
            print(_("Processing repository {repo_name}... ").format(repo_name=repo_name), end='')
            ctx = p4gf_context.create_context(repo_name)
            with ExitStack() as stack:
                stack.enter_context(ctx)
                ctx.repo_lock = p4gf_lock.RepoLock(ctx.p4gf, repo_name, blocking=False)
                stack.enter_context(ctx.repo_lock)
                limits = PushLimits(ctx)
                if args.reset:
                    # Copy any Perforce changes down to this Git repository.
                    p4gf_copy_p2g.copy_p2g_ctx(ctx)
                    # Attempt to trim any unreferenced objects.
                    p4gf_proc.popen(['git', '--git-dir=' + ctx.repo.path, 'prune'])
                    limits.post_copy()
                # Display current key values and disk usage.
                pending_mb = limits.get_pending_mb()
                total_mb = limits.get_total_mb()
                current_mb = limits.space_total
                print(
                    _('{total_mb:.2f}M total, {pending_mb:.2f}M pending, '
                      '{current_mb:.2f}M current')
                    .format(total_mb=total_mb,
                            pending_mb=pending_mb,
                            current_mb=current_mb), end='')
            print("")


if __name__ == "__main__":
    main()

#! /usr/bin/env python3.3
"""Deletes Git Fusion repositories and Perforce artifacts.

During testing, we often create and destroy Git Fusion repositories.
As such, we need an easy way to clean up and try again, without
destroying the entire Perforce server and starting from scratch. In
particular, this script will:

* delete client git-fusion-<space> workspace files
* delete client git-fusion-<space>

If the --all option is given, all git-fusion-<view> clients are
found and deleted, in addition to the following:

* delete object client workspace files
* obliterate //P4GF_DEPOT/objects/...

Invoke with -h for usage information.

"""

import binascii
import logging
import os
import sys

import P4
import p4gf_env_config    # pylint: disable=unused-import
import p4gf_config
import p4gf_const
import p4gf_context
import p4gf_create_p4
import p4gf_p4key as P4Key
from   p4gf_l10n import _, NTR, log_l10n
import p4gf_log
import p4gf_lock
from   p4gf_object_type import ObjectType
import p4gf_util
import p4gf_translate
import p4gf_branch
from p4gf_init_repo import InitRepo
from p4gf_delete_repo_util import delete_non_client_repo_data, DeletionMetrics, \
        check_repo_exists_and_get_repo_config, get_p4gf_localroot, \
        delete_p4key, delete_group

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_delete_repo")

OBLITERATE_MSG = _("""
* To improve obliterate performance, only the Perforce server metadata for //{gfdepot}/objects/...
* has been removed. The archive data under the Perforce server root directory for
* //{gfdepot}/objects/...  may safely be removed.
* Contact your Perforce administrator to request their deletion.
""")


def _print_stderr(msg):
    """Write text to stderr.

    Appends its own trailing newline so that you don't have to.
    """
    sys.stderr.write(msg + '\n')


def print_verbose(args, msg):
    """If args.verbose, print msg, else NOP."""
    if args.verbose:
        print(msg)


def _tree_scanner(blob):
    """Generator function that returns a series of SHA1's for each tree found
    in the given tree blob. If no trees found, returns nothing.
    The object header should not be part of the input.
    """
    # Format: [mode string] [name string]\0[20-byte-SHA1-value]... (no line seperator)
    # Mask of mode string for trees is 040000 (that is, second digit is a 4)
    # Unsure if tree entries _always_ have a file mode of 040000 (stored as '40000'),
    # so allow for leading zero when checking mode.
    idx = 0
    end = len(blob)
    while idx < end:
        nindex = blob.index(b'\x00', idx) + 21
        # Check entry mode, first non-zero digit is a 4
        if (blob[idx] == 48 and blob[idx + 1] == 52) or blob[idx] == 52:
            yield binascii.hexlify(blob[nindex - 20:nindex]).decode()
        idx = nindex


def _find_commit_files(path, client_name):
    """Generator function that walks a directory tree, returning each commit
    file found for the given client.

    Arguments:
        path -- root of directory tree to walk.
        client_name -- name of client for which to find commits.
    """
    for root, _dirs, files in os.walk(path):
        for fyle in files:
            fpath = os.path.join(root, fyle)
            # Convert the object file path to an ObjectType, but don't
            # let those silly non-P4GF objects stop us.
            ot = ObjectType.commit_from_filepath(fpath)
            if ot and ot.applies_to_view(client_name):
                yield fpath


def _tree_mirror_path(root, sha1):
    """Construct a path to the object file."""
    return os.path.join(root, 'trees', sha1[:2], sha1[2:4], sha1[4:])


def _fetch_tree(root, sha1):
    """Fetches the Git tree object as raw text, or returns None if the file is missing."""
    path = _tree_mirror_path(root, sha1)
    if os.path.exists(path):
        return p4gf_util.local_path_to_git_object(path)
    LOG.warning('Missing file for tree object {}'.format(path))
    return None


def _find_gitmodules(p4, stream_name):
    """Retrieve the depot path of the .gitmodules file for this stream.

    :param p4: instance of P4
    :param stream_name: name of the virtual stream

    Returns None if the .gitmodules file was not in the stream view.

    """
    parent = p4gf_util.first_dict(p4.run('stream', '-ov', stream_name))
    for line in parent['View']:
        if '.gitmodules' in line:
            # return everything up to the ' .gitmodules' at the end of the line
            return line[:-12]
    return None


def _init_repo(p4, repo_config):
    """Recreate the repo client so the usual delete repo may operate as usual."""
    repo_created = False
    repo_initer = InitRepo(p4, None).set_repo_config(repo_config)
    if repo_initer.is_init_needed():
        repo_initer.set_fail_when_read_only(True)
        repo_created = repo_initer.init_repo(handle_imports=False)
    return repo_created


def delete_client_local(args, p4, client_name, metrics):
    """Delete the named Perforce client and its workspace.

    :param args: parsed command line arguments
    :param p4: Git user's Perforce client
    :param client_name: name of client to be deleted
    :param metrics: DeletionMetrics for collecting resulting metrics

    Very little else is removed since this is presumed to be a read-only
    instance, and as such, submodules, config files, streams, keys, etc
    are not removed from the Perforce server.

    """
    p4.user = p4gf_const.P4GF_USER
    repo_name = p4gf_util.client_to_repo_name(client_name)
    has_main = __name__ == "__main__"
    check_repo_exists_and_get_repo_config(
            args, p4, client_name, has_main)
    delete_non_client_repo_data(args, p4, client_name, metrics, read_only=True)
    if args.delete:
        if __name__ == "__main__":
            server_id_dict = p4gf_util.serverid_dict_for_repo(p4, repo_name)
            if server_id_dict:
                print(_('You must delete this repo from these other Git Fusion instances'))
                for k, v in server_id_dict.items():
                    print(_("  {server_id} on host {host}")
                          .format(server_id=k, host=v))


def get_stream_name_from_repo_config(p4, repo_config):
    """Return the stream name if configured in the p4gf_config."""
    section_name = repo_config.branch_sections()[0]
    LOG.debug("get_stream_name_from_repo_config: section_name {0}".format(section_name))
    branch = p4gf_branch.Branch.from_config(repo_config.repo_config, section_name, p4)
    return branch.stream_name


def delete_client(args, p4, client_name, metrics, prune_objs=True):
    """Delete the named Perforce client and its workspace.

    Raise P4Exception if the client is not present, or the client configuration
    is not set up as expected.

    Keyword arguments:
    args        -- parsed command line arguments
    p4          -- Git user's Perforce client
    client_name -- name of client to be deleted
    metrics     -- DeletionMetrics for collecting resulting metrics
    prune_objs  -- if True, delete associated objects from cache

    """
    p4.user = p4gf_const.P4GF_USER
    repo_name = p4gf_util.client_to_repo_name(client_name)
    has_main = __name__ == "__main__"
    repo_config = check_repo_exists_and_get_repo_config(
            args, p4, client_name, has_main)
    # The repo client is required only to remove gitmodules from a stream.
    # Since the repo client does not exist we re-construct it only if we have a repo_config.
    # For any repo, the first call to p4gf_delete_repo can use the repo_config, but
    # then deletes it afterward.
    # For the second+ calls to p4gf_delete_repo for other GF instances
    # with a deleted p4gf_config, it is neither possible
    # nor necessary to remove a stream's gitmodules.
    git_modules = None
    stream_name = None
    if repo_config:
        repo_stream_name = get_stream_name_from_repo_config(p4, repo_config)
        if repo_stream_name and repo_stream_name.endswith('_p4gfv'):
            stream_name = repo_stream_name
            git_modules = _find_gitmodules(p4, stream_name)
            if git_modules and args.delete:
                ctx = p4gf_context.create_context(repo_name)
                ctx.repo_config = repo_config
                with ctx:
                    # Temporarily map in the stream so we can delete the file(s).
                    ctx.p4gfrun('client', '-f', '-s', '-S', stream_name, ctx.p4.client)
                    _init_repo(p4, repo_config)
                    ctx.p4.run('sync', '-f', git_modules)
                    ctx.p4.run('delete', git_modules)
                    ctx.p4.run('submit', '-d', "Delete .gitmodules for {0}".format(
                        repo_name), git_modules)
    # Delete the no-client data for this repo
    delete_non_client_repo_data(args, p4, client_name, metrics, prune_objs)
    if stream_name:
        if args.delete:
            p4.run('stream', '-d', stream_name)
        else:
            print(NTR('p4 stream -d {}').format(stream_name))

    if args.delete:
        if __name__ == "__main__":
            server_id_dict = p4gf_util.serverid_dict_for_repo(p4, repo_name)
            if server_id_dict:
                print(_('You must delete this repo from these other Git Fusion instances'))
                for k, v in server_id_dict.items():
                    print(_("  {server_id} on host {host}")
                          .format(server_id=k, host=v))


def _delete_cache(args, p4, metrics):
    """Delete all of the Git Fusion cached objects."""
    if not args.no_obliterate:
        print_verbose(args, _('Obliterating object cache...'))
        print(OBLITERATE_MSG.format(gfdepot=p4gf_const.P4GF_DEPOT))
        r = p4.run('obliterate', '-hay', '//{}/objects/...'.format(p4gf_const.P4GF_DEPOT))
        results = p4gf_util.first_dict_with_key(r, 'revisionRecDeleted')
        if results:
            metrics.files += int(results['revisionRecDeleted'])


def _repo_config_exists(p4, repo_name):
    """Return true is repo p4gf_config exists."""
    config_path = p4gf_config.depot_path_repo(repo_name)
    return p4gf_util.depot_file_exists(p4, config_path)


def delete_clients(args, p4, metrics):
    """Delete all of the Git Fusion clients, except the object cache clients."""
    repos = p4gf_util.repo_config_list(p4)
    if not repos:
        print(_('No Git Fusion clients found.'))
        return
    for repo in repos:
        client = p4gf_util.repo_to_client_name(repo)
        try:
            delete_client(args, p4, client, metrics, False)
        except P4.P4Exception as e:
            sys.stderr.write(str(e) + '\n')
            sys.exit(1)


def _remove_local_root(localroot):
    """Remove the contents of the P4GF local workspace.

    Disregard whether the root is a symbolic link.
    Save and re-write the server-id file after removing contents.
    """
    LOG.debug2("_remove_local_root(): {}".format(localroot))
    # get the server_id
    server_id = p4gf_util.get_server_id()
    p4gf_util.remove_tree(localroot)
    # re-write server_id
    p4gf_util.write_server_id_to_file(server_id)


def _lock_all_repos(p4):
    """Quickly acquire locks on all Git Fusion repositories.

    Fail immediately (raise LockBusy) if any repos are currently locked.
    Waiting would only increase the chance of getting blocked on another repo,
    so scan and fail fast instead.

    Return a list of the P4KeyLock instances acquired.
    """
    locks = []
    repos = p4gf_util.repo_config_list(p4)
    if not repos:
        print(_('No Git Fusion clients found.'))
    else:
        for repo in repos:
            lock = p4gf_lock.RepoLock(p4, repo, blocking=False)
            lock.acquire()
            # If that didn't raise an error, then add to the list of locks acquired.
            locks.append(lock)
    return locks


def _release_locks(locks):
    """Release all of the given locks, reporting any errors to the log."""
    for lock in locks:
        try:
            lock.release()
        except Exception:  # pylint: disable=broad-except
            LOG.exception("Error releasing lock %s", lock.p4key_name())


def _prevent_access(p4):
    """Prevent further access to Git Fusion while deleting everything.

    Return the previous value of the p4key so it can be restored later.
    """
    old_value = P4Key.get(p4, p4gf_const.P4GF_P4KEY_PREVENT_NEW_SESSIONS)
    P4Key.set(p4, p4gf_const.P4GF_P4KEY_PREVENT_NEW_SESSIONS, 'true')
    return old_value


def delete_all_local(args, p4, metrics):
    """Remove "everything" as if from a read-only Git Fusion instance.

    :param args: parsed command line arguments
    :param p4: Git user's Perforce client
    :param metrics: for counting delete actions.

    Similar to deleting everything from the master server, except that very
    little is removed from the Perforce server (e.g. counters and files).
    In short, only the client and local directories are removed.

    """
    p4.user = p4gf_const.P4GF_USER
    print(_('Connected to {P4PORT}').format(P4PORT=p4.port))
    client_name = p4gf_util.get_object_client_name()
    localroot = get_p4gf_localroot(p4)
    if not args.delete:
        if localroot:
            if args.no_obliterate:
                print(NTR('p4 sync -f {}...#none').format(localroot))
            else:
                print(NTR('p4 client -f -d {}').format(client_name))
                print(NTR('rm -rf {}').format(localroot))
    else:
        if localroot:
            if not args.no_obliterate:
                # Need this in order to use --gc later on
                p4gf_util.p4_client_df(p4, client_name)
                metrics.clients += 1
                print_verbose(args, _("Deleting client '{client_name}'s workspace...")
                              .format(client_name=client_name))
                _remove_local_root(localroot)


def delete_all(args, p4, metrics):
    """Remove all Git Fusion clients, as well as the object cache.

    Keyword arguments:
        args -- parsed command line arguments
        p4   -- Git user's Perforce client
    """
    # pylint:disable=too-many-branches
    p4.user = p4gf_const.P4GF_USER
    group_list = [p4gf_const.P4GF_GROUP_PULL, p4gf_const.P4GF_GROUP_PUSH]
    print(_('Connected to {P4PORT}').format(P4PORT=p4.port))
    print_verbose(args, _('Scanning for Git Fusion clients...'))
    client_name = p4gf_util.get_object_client_name()
    locks = _lock_all_repos(p4)
    if args.delete:
        was_prevented = _prevent_access(p4)
    else:
        was_prevented = None
    delete_clients(args, p4, metrics)
    # Retrieve the names of the initialization/upgrade "lock" p4keys.
    p4keys = [
        p4gf_const.P4GF_P4KEY_ALL_PENDING_MB,
        p4gf_const.P4GF_P4KEY_ALL_REMAINING_MB
    ]
    # Key patterns NOT published in p4gf_const because they have trailing *
    # wildcards and it's not worth cluttering p4gf_const for this one use.
    p4key_patterns = ['git-fusion-init-started*',
                      'git-fusion-init-complete*',
                      'git-fusion-upgrade-started*',
                      'git-fusion-upgrade-complete*',
                      'git-fusion-index-*']
    for p4key_pattern in p4key_patterns:
        d = P4Key.get_all(p4, p4key_pattern)
        p4keys.extend(sorted(d.keys()))
    localroot = get_p4gf_localroot(p4)
    if not args.delete:
        if localroot:
            if args.no_obliterate:
                print(NTR('p4 sync -f #none'))
            else:
                print(NTR('p4 client -f -d {}').format(client_name))
                print(NTR('rm -rf {}').format(localroot))
        if not args.no_obliterate:
            print(NTR('p4 obliterate -hay //{}/objects/...').format(p4gf_const.P4GF_DEPOT))
        for p4key in p4keys:
            print(NTR('p4 key -d {}').format(p4key))
        for group in group_list:
            print(NTR('p4 group -a -d {}').format(group))
    else:
        if localroot:
            if not args.no_obliterate:
                # Need this in order to use --gc later on
                # client should not exist; this is likely a NOOP
                p4gf_util.p4_client_df(p4, client_name)
                metrics.clients += 1
                print_verbose(args, _("Deleting client '{client_name}'s workspace...")
                              .format(client_name=client_name))
                _remove_local_root(localroot)
        _delete_cache(args, p4, metrics)
        print_verbose(args, _('Removing initialization p4keys...'))
        for p4key in p4keys:
            delete_p4key(p4, p4key, metrics)
        for group in group_list:
            delete_group(args, p4, group, metrics)
    _release_locks(locks)
    if was_prevented is not None:
        if was_prevented != '0':
            P4Key.set(p4, p4gf_const.P4GF_P4KEY_PREVENT_NEW_SESSIONS, was_prevented)
        else:
            P4Key.delete(p4, p4gf_const.P4GF_P4KEY_PREVENT_NEW_SESSIONS)


def main():
    """Process command line arguments and call functions to do the real
    work of cleaning up the Git mirror and Perforce workspaces.
    """
    # pylint:disable=too-many-branches, too-many-statements
    log_l10n()
    # Set up argument parsing.
    desc = _("""Deletes Git Fusion repositories and workspaces. When you
include the -a or --all option, Git Fusion finds and deletes the following
for all repos on the current server disregarding specified views:
1) All git-fusion-view clients. 2) Client git-fusion--p4 workspace files.
3) Objects in //.git-fusion/objects/...
""")
    epilog = _("""It is recommended to run 'p4gf_delete_repo.py' without
the '-y' flag to preview changes that will be made to the depot before
using the '-y' flag for permanent removal. Use -a or --all to permanently
delete all repo data for all repos on the Perforce server; be aware that
this may take some time, depending on the number and size of the objects.
Use -N, --no-obliterate to quickly delete most of the repo's data and
continue working. This minimizes the impact to server performance.
""")
    parser = p4gf_util.create_arg_parser(desc, epilog=epilog)
    parser.add_argument('-a', '--all', action='store_true',
                        help=_('remove all known Git mirrors on the current server'))
    parser.add_argument('-y', '--delete', action='store_true',
                        help=_('perform the deletion'))
    parser.add_argument('-v', '--verbose', action='store_true',
                        help=_('print details of deletion process'))
    parser.add_argument('-N', '--no-obliterate', action='store_true',
                        help=_('with the --all option, do not obliterate object cache'))
    parser.add_argument(NTR('views'), metavar=NTR('view'), nargs='*',
                        help=_('name of view to be deleted'))
    args = parser.parse_args()
    p4gf_util.has_server_id_or_exit()

    # Check that either --all, or 'views' was specified.
    if not args.all and len(args.views) == 0:
        sys.stderr.write(_('Missing view names; try adding --all option.\n'))
        sys.exit(2)
    if args.all and len(args.views) > 0:
        sys.stderr.write(_('Ambiguous arguments. Choose --all or a view name.\n'))
        sys.exit(2)

    # Check that --no-obliterate occurs only with --all
    if not args.all and args.no_obliterate:
        sys.stderr.write(_('--no-obliterate permitted only with the --all option.\n'))
        sys.exit(2)

    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4_temp_client()
        if not p4:
            return 2
        # Sanity check the connection (e.g. user logged in?) before proceeding.
        p4gf_branch.init_case_handling(p4)
        try:
            p4.fetch_client()
        except P4.P4Exception as e:
            sys.stderr.write(_('P4 exception occurred: {exception}').format(exception=e))
            sys.exit(1)

        metrics = DeletionMetrics()
        if args.all:
            try:
                if p4gf_const.READ_ONLY:
                    delete_all_local(args, p4, metrics)
                else:
                    delete_all(args, p4, metrics)
            except (p4gf_lock.LockBusy, P4.P4Exception) as e:
                sys.stderr.write("{exception}\n".format(exception=e))
                sys.exit(1)
        else:
            # Delete the client(s) for the named view(s).
            for git_view in args.views:
                repo_name = p4gf_translate.TranslateReponame.git_to_repo(git_view)
                client_name = p4gf_util.repo_to_client_name(repo_name)
                try:
                    if p4gf_const.READ_ONLY:
                        delete_client_local(args, p4, client_name, metrics)
                    else:
                        with p4gf_lock.RepoLock(p4, repo_name, blocking=False):
                            delete_client(args, p4, client_name, metrics)
                except (p4gf_lock.LockBusy, P4.P4Exception) as e:
                    sys.stderr.write("{exception}\n".format(exception=e))
                    sys.exit(1)
        if not args.delete:
            print(_('This was report mode. Use -y to make changes.'))
        else:
            print(_('Deleted {num_files:d} files, {num_groups:d} groups, '
                    '{num_clients:d} clients, and {num_keys:d} p4keys.')
                  .format(num_files=metrics.files, num_groups=metrics.groups,
                          num_clients=metrics.clients, num_keys=metrics.p4keys))
            if args.all:
                print(_('Successfully deleted all repos\n'))
            else:
                print(_('Successfully deleted repos:\n{repos}')
                      .format(repos="\n".join(args.views)))

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

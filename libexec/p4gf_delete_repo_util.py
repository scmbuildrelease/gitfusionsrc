#! /usr/bin/env python3.3
"""Delete a Git Fusion repo."""

import logging
import os
import sys

import P4
import p4gf_env_config    # pylint: disable=unused-import
import p4gf_config
import p4gf_const
import p4gf_context
import p4gf_p4key as P4Key
from   p4gf_l10n import _, NTR
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_p4spec
import p4gf_util
import p4gf_repo_dirs

LOG = logging.getLogger(__name__)


class DeletionMetrics:

    """DeletionMetrics captures the number of Perforce objects removed."""

    def __init__(self):
        self.clients = 0
        self.groups = 0
        self.files = 0
        self.p4keys = 0


def raise_if_homedir(homedir, repo_name, rm_list):
    """If any path in rm_list is user's home directory, fail with error
    rather than delete the home directory."""
    for e in rm_list:
        if e == homedir:
            raise P4.P4Exception(
                _("One of view '{repo_name}'s directories is user's home directory!")
                .format(repo_name=repo_name))


def print_verbose(args, msg):
    """If args.verbose, print msg, else NOP."""
    if args.verbose:
        print(msg)


def delete_p4key(p4, name, metrics):
    """Attempt to delete p4key. Report and continue on error."""
    try:
        P4Key.delete(p4, name)
        metrics.p4keys += 1
    except P4.P4Exception:
        if p4gf_p4msg.find_msgid(p4, p4gf_p4msgid.MsgDm_NoSuchKey):
            LOG.info('failed to delete p4key {name}'.format(name=name))


def get_p4gf_localroot(p4):
    """Calculate the local root for the object client."""
    if not p4gf_util.is_temp_object_client_name(p4.client):
        LOG.debug("incorrect object client {0} should be {1}".format(
            p4.client,
            p4gf_const.P4GF_OBJECT_CLIENT_UNIQUE.format(server_id=p4gf_util.get_server_id(),
                                                        uuid='{uuid}')))
        raise RuntimeError(_('incorrect p4 client'))
    client = p4.fetch_client()
    rootdir = client["Root"]
    if rootdir.endswith(os.sep):
        rootdir = rootdir[:-1]
    client_map = P4.Map(client["View"])
    lhs = client_map.lhs()
    if len(lhs) > 1:
        # not a conforming Git Fusion client, ignore it
        return None
    rpath = client_map.translate(lhs[0])
    localpath = p4gf_context.client_path_to_local(rpath, p4.client, rootdir)
    localroot = p4gf_context.strip_wild(localpath)
    localroot = localroot.rstrip('/')
    return localroot


def _find_client_commit_objects(args, p4, repo_name):
    """Finds the object cache commit files associated only with the given view.
    These are objects that can be deleted from the cache without affecting
    other Git Fusion views. This does not return the eligible tree objects.

    Arguments:
        args -- parsed command line arguments
        p4 -- P4API object, client for object cache, already connected
        repo_name -- name of view for which files are to be pruned from cache

    Returns:
        List of cached commit objects to be deleted.
    """

    # Bring the workspace up to date and traverse that rather than
    # fetching large numbers of small files from Perforce.
    repo_commit_objects_path = "{0}/repos/{1}/...".format(p4gf_const.objects_root(), repo_name)
    repos_path = "{0}/repos/...".format(p4gf_const.P4GF_DEPOT)
    with p4.at_exception_level(P4.P4.RAISE_NONE):
        # Raises an exception when there are no files to sync?
        p4.run('sync', '-q', repo_commit_objects_path)
        p4.run('sync', '-q', repos_path)

# TBD Optimization:
# Rather than delete batches of files based on workspace file discovery
# we could do the following -- ??could overwhelm the server or be slower??
#   r = p4.run('delete', repo_commit_objects_path)
#   count = sum([int('depotFile' in rr and rr['action'] == 'delete') for rr in r])
#   r = p4.run("submit", "-d",
#            "Deleting {0} commit objects for repo '{1}'".format(count, repo_name))
#   return count

    root = os.path.join(get_p4gf_localroot(p4), 'objects')
    print_verbose(args, _("Selecting cached commit objects for '{repo_name}'...")
                  .format(repo_name=repo_name))
    paths = [os.path.join(root, 'repos', repo_name, '...')]
    return paths


def _delete_files(p4, files, repo_name=None):
    """Delete a set of files, doing so in chunks."""
    if repo_name:
        msgstr = _("Deleting {num_commits} commit objects for repo '{repo_name}'.")
    else:
        msgstr = _("Deleting {num_commits} commit objects for all repos.")
    total = 0
    bite_size = 1000
    while len(files):
        to_delete = files[:bite_size]
        files = files[bite_size:]
        result = p4.run("delete", to_delete)
        count = sum([int('depotFile' in row and row['action'] == 'delete') for row in result])
        total += count
        if count:
            for d in to_delete:
                if os.path.isfile(d):
                    os.remove(d)
            result = p4.run("submit", "-d", msgstr.format(num_commits=count, repo_name=repo_name))
    return total


def delete_group(args, p4, group_name, metrics):
    """Delete one group, if it exists and it's ours."""
    LOG.debug("delete_group() {}".format(group_name))
    r = p4.fetch_group(group_name)
    if r and r.get('Owners') and p4gf_const.P4GF_USER in r.get('Owners'):
        print_verbose(args, _("Deleting group '{group_name}'...").format(group_name=group_name))
        p4.run('group', '-a', '-d', group_name)
        metrics.groups += 1
    else:
        print_verbose(args, _("Not deleting group '{group}':"
                              " Does not exist or '{user}' is not an owner.")
                      .format(group=group_name, user=p4gf_const.P4GF_USER))


def check_for_other_gf_instances(p4, repo_name, has_main=False):
    """If the <serverid>-repo-client exists on another GF instance
    display message informing admin to delete on another GF instance
    where the client exists. Exit non-0.
    Otherwise return for normal processing.
    There are still problems with this approach yet to be addressed.
    """
    if not has_main:
        return
    server_id_dict = p4gf_util.serverid_dict_for_repo(p4, repo_name)
    if server_id_dict:
        print(_("Repo '{repo_name}' has not been accessed via this Git Fusion instance")
              .format(repo_name=repo_name))
        print(_('You must delete this repo from one of these Git Fusion instances'))
        for k, v in server_id_dict.items():
            print(_("  {server_id} on host {host}").format(server_id=k, host=v))
        sys.exit(1)


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


def repo_exists_on_gf_server(p4, repo_name):
    """Return true or false if repo exists on this GF server."""
    return get_server_repo_config_rev(p4, repo_name) != '0'


def get_server_repo_config_rev(p4, repo_name):
    """Get the config file rev for the repo from the p4key."""
    key = p4gf_const.P4GF_P4KEY_REPO_SERVER_CONFIG_REV.format(
        repo_name=repo_name, server_id=p4gf_util.get_server_id())
    return p4gf_util.get_p4key(p4, key)


def check_repo_exists_and_get_repo_config(args, p4, client_name, has_main=False):
    """Return repo_config or None if not exists."""
    print_verbose(args, _("Checking for client '{client_name}'...")
                  .format(client_name=client_name))
    # check for old style repo client name with no serverid
    old_client_name = p4gf_const.P4GF_CLIENT_PREFIX + p4gf_util.client_to_repo_name(client_name)
    old_client_exists = p4gf_p4spec.spec_exists(p4, 'client', old_client_name)
    repo_name = p4gf_util.client_to_repo_name(client_name)
    repo_exists_on_server = repo_exists_on_gf_server(p4, repo_name)
    no_repos_exist = False
    if not (old_client_exists or repo_exists_on_server):
        check_for_other_gf_instances(p4, repo_name, has_main)
        no_repos_exist = True
    # Do we have a repo config file to delete?
    config_file = p4gf_config.depot_path_repo(repo_name) + '*'
    config_file_exists = p4gf_util.depot_file_exists(p4, config_file)
    repo_config = None
    if config_file_exists:
        repo_config = p4gf_config.RepoConfig.from_depot_file(repo_name, p4)
    elif no_repos_exist:
        raise P4.P4Exception(p4gf_const.NO_SUCH_REPO_DEFINED.format(repo_name))
    return repo_config


# pylint: disable=too-many-arguments
def delete_non_client_repo_data(args, p4, client_name, metrics, prune_objs=True, read_only=False):
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
    # pylint:disable=too-many-branches, too-many-statements, too-many-locals
    group_list = [p4gf_const.P4GF_GROUP_REPO_PULL, p4gf_const.P4GF_GROUP_REPO_PUSH]
    repo_name = p4gf_util.client_to_repo_name(client_name)
    p4.user = p4gf_const.P4GF_USER
    old_client_name = p4gf_const.P4GF_CLIENT_PREFIX + p4gf_util.client_to_repo_name(client_name)
    old_client_exists = p4gf_p4spec.spec_exists(p4, 'client', old_client_name)
    config_file = p4gf_config.depot_path_repo(repo_name) + '*'
    config_file_exists = p4gf_util.depot_file_exists(p4, config_file)

    repo_dirs = p4gf_repo_dirs.from_p4gf_dir(p4gf_const.P4GF_HOME, repo_name)

    homedir = os.path.expanduser('~')
    raise_if_homedir(homedir, repo_name, repo_dirs.repo_container)

    # Scan for objects associated only with this view so we can remove them.
    objects_to_delete = []
    if prune_objs:
        objects_to_delete = _find_client_commit_objects(args, p4, repo_name)

    if read_only:
        p4key_list = [  p4gf_const.P4GF_P4KEY_REPO_SERVER_CONFIG_REV
                        .format( repo_name = repo_name
                               , server_id = p4gf_util.get_server_id())
                      , p4gf_const.P4GF_P4KEY_LAST_COPIED_TAG
                        .format( repo_name = repo_name
                               , server_id = p4gf_util.get_server_id())
                      , p4gf_const.P4GF_P4KEY_LAST_SEEN_CHANGE
                        .format( repo_name = repo_name
                               , server_id = p4gf_util.get_server_id())
                      ]

    else:
        # What p4keys shall we delete?
        p4key_set = set()
        server_id = p4gf_util.get_server_id()
        p4key_set.add(P4Key.calc_last_copied_change_p4key_name(repo_name, server_id))
        p4key_set.add(P4Key.calc_repo_status_p4key_name(repo_name))
        p4key_set.add(P4Key.calc_repo_push_id_p4key_name(repo_name))

        pattern_list = [ "git-fusion-index-last-{},*".format(repo_name)
                       , "git-fusion-index-branch-{},*".format(repo_name)
                       , p4gf_const.P4GF_P4KEY_REPO_SERVER_CONFIG_REV
                            .format( repo_name = repo_name
                                   , server_id = p4gf_util.get_server_id())
                       , p4gf_const.P4GF_P4KEY_LAST_COPIED_TAG
                            .format( repo_name = repo_name
                                   , server_id = p4gf_util.get_server_id())
                       , p4gf_const.P4GF_P4KEY_LAST_SEEN_CHANGE
                            .format( repo_name = repo_name
                                   , server_id = p4gf_util.get_server_id())
                       , p4gf_const.P4GF_P4KEY_REV_SHA1
                            .format( repo_name  = repo_name
                                   , change_num = '*')
                       , p4gf_const.P4GF_P4KEY_TOTAL_MB.format(repo_name=repo_name)
                       , p4gf_const.P4GF_P4KEY_PENDING_MB.format(repo_name=repo_name)
                       , P4Key.calc_repo_status_p4key_name(repo_name, '*')
                       ]
        for pattern in pattern_list:
            p4key_set.update(P4Key.get_all(p4, pattern).keys())
        p4key_list = sorted(p4key_set)

    if not args.delete:
        if old_client_exists:
            print(NTR('p4 client -f -d {}').format(old_client_name))
        print(NTR('rm -rf {}').format(repo_dirs.repo_container))
        for p4key in p4key_list:
            print(NTR('p4 key -d {}').format(p4key))
        if not read_only:
            print(_('Deleting {num_objects} objects from //{depot}/objects/...')
                  .format(num_objects=len(objects_to_delete),
                          depot=p4gf_const.P4GF_DEPOT))
            for group_template in group_list:
                group = group_template.format(repo=repo_name)
                print(NTR('p4 group -a -d {}').format(group))
            if config_file_exists:
                print(NTR('p4 sync -f {}').format(config_file))
                print(NTR('p4 delete  {}').format(config_file))
                print(NTR('p4 submit -d "Delete repo config for {repo_name}" {config_file}')
                      .format(repo_name=repo_name, config_file=config_file))
    else:
        if p4gf_p4spec.spec_exists(p4, 'client', client_name):
            LOG.info('deleting client {0}'.format(client_name))
            p4gf_util.p4_client_df(p4, client_name)
            metrics.clients += 1
        if old_client_exists:
            p4gf_util.p4_client_df(p4, old_client_name)
            metrics.clients += 1
        print_verbose(args, _("Deleting repo {repo_name}'s directory {dir}...")
                      .format(repo_name=repo_name, dir=repo_dirs.repo_container))
        p4gf_util.remove_tree(repo_dirs.repo_container, contents_only=False)
        metrics.files += _delete_files(p4, objects_to_delete, repo_name)
        for p4key in p4key_list:
            delete_p4key(p4, p4key, metrics)
        if not read_only:
            for group_template in group_list:
                delete_group(args, p4, group_template.format(repo=repo_name), metrics)
            if config_file_exists:
                p4.run('sync', '-fq', config_file)
                desc = _("Delete repo config for '{repo_name}'").format(repo_name=repo_name)
                with p4gf_util.NumberedChangelist(p4=p4, description=desc) as nc:
                    nc.p4run("delete", config_file)
                    nc.submit()

#! /usr/bin/env python3.3
"""Functions for operating on a .gitattributes file."""

import p4gf_config
import p4gf_const
from p4gf_p4file import P4File
import p4gf_pygit2
import p4gf_util

def generate_initial_lfs_attrs(ctx):
    """Generate the initial .gitattributes file for an LFS-enabled repo.

    Does nothing if the git-lfs-initial-track configurable is not set.

    :param ctx: Git Fusion context.
    :param changelist: the P4Changelist.
    :param branch: the Branch to contain this file.

    :return: contents of .gitattributes file

    """
    # Check if we have an initial-tracking setting or not.
    initial_tracking = ctx.repo_config.get(p4gf_config.SECTION_PERFORCE_TO_GIT,
                                           p4gf_config.KEY_GIT_LFS_INITIAL_TRACK)
    if not initial_tracking:
        return None

    attr_form = '{0} filter=lfs diff=lfs merge=lfs -crlf'
    attrs = []
    for ext in initial_tracking.splitlines():
        stripped = ext.strip()
        if stripped:
            attrs.append(attr_form.format(stripped))
    if not attrs:
        return None
    return ('\n'.join(attrs) + '\n').encode('utf-8')


def maybe_create_lfs_attrs(ctx, changelist, p4file_list, branch):
    """Create the initial .gitattributes file for an LFS-enabled repo.

    Does nothing if the git-lfs-initial-track configurable is not set.

    :param ctx: Git Fusion context.
    :param changelist: the P4Changelist.
    :param p4file_list: list of P4File.
    :param branch: the Branch to contain this file.

    :return: modified p4file_list

    """
    # Check if we have an initial-tracking setting or not.
    initial_tracking = generate_initial_lfs_attrs(ctx)
    if not initial_tracking:
        return p4file_list

    # Check if a .gitattributes file already exists or not.
    with ctx.switched_to_branch(branch):
        depot_path = ctx.gwt_to_depot_path(p4gf_const.GITATTRIBUTES)
        if not depot_path:
            return p4file_list
    for p4file in p4file_list:
        if p4file.depot_path == depot_path:
            # A .gitattributes already exists, nothing to do.
            return p4file_list

    # If a .gitattributes file ever existed in Perforce but was deleted by
    # the time we got to this changelist, honor that deletion. Do not insert.
    r = ctx.p4run('files', "{}@{}".format(depot_path, changelist.change))
    if p4gf_util.first_dict_with_key(r, "depotFile"):
        return p4file_list

    # "Print" the attributes file into the repository.
    sha1 = add_attributes_to_repo(initial_tracking, ctx.repo)
    if sha1 is None:
        return p4file_list

    # Save the SHA1 in the gitmirror list of pending blobs to cache.
    ctx.mirror.add_blob(sha1)

    # Construct a P4File and add to the list of files for this change.
    vardict = {
        'depotFile': depot_path,
        'action': 'add',
        'rev': 1,
        'type': 'text',
        'change': changelist.change
    }
    p4file = P4File.create_from_print(vardict)
    p4file.sha1 = sha1
    p4file_list.append(p4file)
    return p4file_list


def add_attributes_to_repo(initial_tracking, repo):
    """Generate a .gitattributes file for LFS and add to the repository.

    :param initial_tracking: value for git-lfs-initial-track setting.
    :param repo: pygit2.Repository instance.

    :return: SHA1 of newly created blob.

    """
    oid = repo.create_blob(initial_tracking)
    return p4gf_pygit2.oid_to_sha1(oid)

#! /usr/bin/env python3.3
"""Functions to support storing and reconstituting Git tags."""

import logging
import os
import re
import sys
import zlib

import P4
import pygit2

import p4gf_const
import p4gf_git
import p4gf_p4key as P4Key
from p4gf_l10n import _, NTR
from p4gf_object_type import ObjectType
import p4gf_proc
from p4gf_profiler import with_timer
import p4gf_pygit2
import p4gf_util

LOG = logging.getLogger(__name__)
_BITE_SIZE = 1000  # How many files to pass in a single 'p4 xxx' operation.


def _client_path(ctx, sha1):
    """Construct the client path for the given tag object.

    For example, 5716ca5987cbf97d6bb54920bea6adde242d87e6 might return as
    objects/repos/foobar/tags/57/16/ca5987cbf97d6bb54920bea6adde242d87e6
    """
    return os.path.join("objects", "repos", ctx.config.repo_name, "tags",
                        sha1[:2], sha1[2:4], sha1[4:])


def _add_tag(ctx, name, sha1, edit_list, add_list):
    """Add a tag to the object cache.

    If adding another lightweight tag that refers to the same object,
    edit the file rather than add.
    """
    LOG.debug("_add_tag() adding tag {}".format(name))
    fpath = os.path.join(ctx.gitlocalroot, _client_path(ctx, sha1))
    if os.path.exists(fpath):
        # Overwriting an existing tag? Git prohibits that.
        # But, another lightweight tag of the same object is okay.
        # Sanity check if this is a lightweight tag of an annotated
        # tag and reject with a warning.
        with open(fpath, 'rb') as f:
            contents = f.read()
        try:
            zlib.decompress(contents)
            return
        except zlib.error:
            pass
        # it's a lightweight tag, just append the name
        with open(fpath, 'ab') as f:
            f.write(b'\n')
            f.write(name.encode('UTF-8'))
        edit_list.append(fpath)
    else:
        obj = ctx.repo.get(sha1)
        if obj.type == pygit2.GIT_OBJ_TAG:
            LOG.debug("_add_tag() annotated tag {}".format(name))
            p4gf_git.write_git_object_from_sha1(ctx.repo, sha1, fpath)
        else:
            # Lightweight tags can be anything: commit, tree, blob
            LOG.debug("_add_tag() lightweight tag {}".format(name))
            p4gf_util.ensure_parent_dir(fpath)
            with open(fpath, 'wb') as f:
                f.write(name.encode('UTF-8'))
        add_list.append(fpath)


def _remove_tag(ctx, name, sha1, edit_list, delete_list):
    """Remove the tag from the object cache.

    If removing one of several lightweight tags which reference the same object,
    the corresponding file will be edited rather than deleted.
    """
    LOG.debug("_remove_tag() removing tag {}".format(name))
    fpath = os.path.join(ctx.gitlocalroot, _client_path(ctx, sha1))
    if not os.path.exists(fpath):
        # Already gone (or never stored), nothing else to do.
        return
    with open(fpath, 'rb') as f:
        contents = f.read()
    try:
        zlib.decompress(contents)
        # Must be an annotated tag...
        delete_list.append(fpath)
    except zlib.error:
        tag_names = contents.decode('UTF-8').splitlines()
        tag_names.remove(name)
        if tag_names:
            contents = '\n'.join(tag_names).encode('UTF-8')
            os.chmod(fpath, 0o644)
            with open(fpath, 'wb') as f:
                f.write(contents)
            edit_list.append(fpath)
        else:
            delete_list.append(fpath)


def _get_tag_target(repo, sha1):
    """Return the pygit2 object referred to by the tag given by the SHA1."""
    obj = repo.get(sha1)
    if obj.type == pygit2.GIT_OBJ_TAG:
        # Get the tag's target object
        obj = repo.get(obj.target)
    return obj


def _is_reachable(sha1, heads):
    """Return True if the commit is reachable from one of the heads.

    :param sha1: SHA1 of the commit to find.
    :param heads: list of commit references for heads (non-tags)

    :return: True if commit is reachable, False otherwise.

    """
    LOG.debug2('_is_reachable() checking for {}'.format(sha1))
    for head_sha1 in heads:
        # ### newer pygit2.Repository.merge_base(oid, oid) would do this for us,
        # ### too bad we're not updating any time soon...
        cmd = ['git', 'merge-base', '--is-ancestor', sha1, head_sha1]
        result = p4gf_proc.popen_no_throw(cmd)
        if LOG.isEnabledFor(logging.DEBUG2):
            reachable = result['ec'] == 0
            LOG.debug2('_is_reachable() {} is reachable {}'.format(sha1, reachable))
        if result['ec'] == 0:
            return True
    return False


@with_timer('preflight tags')
def preflight_tags(ctx, tags, heads):
    """Validate the incoming tags.

    :param ctx: P4GF context with initialized pygit2 Repository.
    :param tags: list of PreReceiveTuple objects for tags
    :param heads: list of commit references for heads (non-tags)

    Warnings are printed to stderr so the user knows about them.

    Returns None if successful and an error string otherwise.

    """
    if not tags:
        LOG.debug("preflight_tags() no incoming tags to process")
        return None

    LOG.debug("preflight_tags() beginning...")
    tags_path = "objects/repos/{repo}/tags".format(repo=ctx.config.repo_name)
    with ctx.p4gf.at_exception_level(P4.P4.RAISE_NONE):
        # Raises an exception when there are no files to sync?
        ctx.p4gfrun('sync', '-q', "//{}/{}/...".format(ctx.p4gf.client, tags_path))

    regex = re.compile(r'[*@#,]|\.\.\.|%%')
    for prt in tags:
        tag = prt.ref[10:]
        # Screen the tags to ensure their names won't cause problems
        # sometime in the future (i.e. when we create Perforce labels).
        # Several of these characters are not allowed in Git tag names
        # anyway, but better to check in case that changes in the future.
        # In particular git disallows a leading '-', but we'll check for it
        # anyway Otherwise allow internal '-'
        if regex.search(tag) or tag.startswith('-'):
            return _("illegal characters (@#*,...%%) in tag name: '{tag}'").format(tag=tag)
        if prt.old_sha1 == p4gf_const.NULL_COMMIT_SHA1:
            if prt.new_sha1 == p4gf_const.NULL_COMMIT_SHA1:
                # No idea how this happens, but it did, so guard against it.
                sys.stderr.write(_('Ignoring double-zero pre-receive-tuple line'))
                continue
            # Adding a new tag; if it references a commit, check that it
            # exists; for other types, it is too costly to verify
            # reachability from a known commit, so just ignore them.
            obj = _get_tag_target(ctx.repo, prt.new_sha1)
            is_commit = obj.type == pygit2.GIT_OBJ_COMMIT
            if is_commit and not _is_reachable(p4gf_pygit2.object_to_sha1(obj), heads):
                # Do not fail in preflight but allow the push to proceed.
                # Later this tag is ignored without error and not added to the object cache.
                # The tag ref however has already been added to the git repo, but
                # will be removed later in process_tags.
                msg = _("Tag '{tag}' of unknown commit {sha1:7.7} not stored in "
                        "Perforce nor in git.\n"
                        "You must push a branch containing the target commit either prior to"
                        " or with the push of the tag.\n").format(tag=tag, sha1=prt.new_sha1)
                LOG.debug(msg)
                sys.stderr.write(msg)
            if obj.type == pygit2.GIT_OBJ_TREE:
                msg = _("Tag '{tag}' of tree will not be stored in Perforce\n").format(tag=tag)
                sys.stderr.write(msg)
                continue
            if obj.type == pygit2.GIT_OBJ_BLOB:
                msg = _("Tag '{tag}' of blob will not be stored in Perforce\n").format(tag=tag)
                sys.stderr.write(msg)
                continue

            fpath = os.path.join(ctx.gitlocalroot, _client_path(ctx, prt.new_sha1))
            if os.path.exists(fpath):
                # Overwriting an existing tag? Git prohibits that.
                # But, another lightweight tag of the same object is okay.
                # Sanity check if this is a lightweight tag of an annotated
                # tag and reject with a warning.
                with open(fpath, 'rb') as f:
                    contents = f.read()
                try:
                    zlib.decompress(contents)
                    msg = _("Tag '{tag}' of annotated tag will not be stored in Perforce\n")
                    sys.stderr.write(msg.format(tag=tag))
                except zlib.error:
                    pass

        elif prt.new_sha1 != p4gf_const.NULL_COMMIT_SHA1:
            # Older versions of Git allowed moving a tag reference, while
            # newer ones seemingly do not. We will take the new behavior as
            # the correct one and reject such changes.
            return _('Updates were rejected because the tag already exists in the remote.')


@with_timer('process tags')
def process_tags(ctx, tags):
    """Add or remove tags objects from the Git Fusion mirror.

    :param ctx: P4GF context with initialized pygit2 Repository.
    :param tags: list of PreReceiveTuple objects for tags

    """
    # pylint:disable=too-many-branches
    if not tags:
        LOG.debug("process_tags() no incoming tags to process")
        return

    # Re-sync the tags since preflight_tags() synced with a different temp client.
    tags_path = "objects/repos/{repo}/tags".format(repo=ctx.config.repo_name)
    with ctx.p4gf.at_exception_level(P4.P4.RAISE_NONE):
        # Raises an exception when there are no files to sync?
        ctx.p4gfrun('sync', '-q', "//{}/{}/...".format(ctx.p4gf.client, tags_path))

    # Decide what to do with the tag references.
    tags_to_delete = []
    tags_to_add = []
    tags_to_edit = []
    for prt in tags:
        tag = prt.ref[10:]
        if prt.old_sha1 == p4gf_const.NULL_COMMIT_SHA1:
            if prt.new_sha1 == p4gf_const.NULL_COMMIT_SHA1:
                # No idea how this happens, but it did, so guard against it.
                continue
            # Adding a new tag; if it references a commit, check that it
            # exists; for other types, it is too costly to verify
            # reachability from a known commit, so just ignore them.
            obj = _get_tag_target(ctx.repo, prt.new_sha1)
            is_commit = obj.type == pygit2.GIT_OBJ_COMMIT
            if is_commit and not ObjectType.commits_for_sha1(ctx, p4gf_pygit2.object_to_sha1(obj)):
                LOG.debug("Tag '{}' of unknown commit {:7.7} not stored."
                          " Removing ref from git repo.".format(tag, prt.new_sha1))
                _remove_tag_ref(tag, prt.new_sha1)
                continue
            if obj.type == pygit2.GIT_OBJ_TREE:
                continue
            if obj.type == pygit2.GIT_OBJ_BLOB:
                continue
            _add_tag(ctx, tag, prt.new_sha1, tags_to_edit, tags_to_add)
        elif prt.new_sha1 == p4gf_const.NULL_COMMIT_SHA1:
            # Removing an existing tag
            _remove_tag(ctx, tag, prt.old_sha1, tags_to_edit, tags_to_delete)

    # Seemingly nothing to do.
    if not tags_to_add and not tags_to_edit and not tags_to_delete:
        LOG.debug("process_tags() mysteriously came up empty"
                  " - probably a tag of a non-existing commit.")
        return

    # Add and remove tags as appropriate, doing so in batches.
    LOG.info("adding {} tags, removing {} tags, editing {} tags from Git mirror".format(
        len(tags_to_add), len(tags_to_delete), len(tags_to_edit)))
    desc = _("Git Fusion '{repo}' tag changes").format(repo=ctx.config.repo_name)
    with p4gf_util.NumberedChangelist(gfctx=ctx, description=desc) as nc:
        while len(tags_to_add):
            bite = tags_to_add[:_BITE_SIZE]
            tags_to_add = tags_to_add[_BITE_SIZE:]
            ctx.p4gfrun('add', '-t', 'binary+F', bite)
        while len(tags_to_edit):
            bite = tags_to_edit[:_BITE_SIZE]
            tags_to_edit = tags_to_edit[_BITE_SIZE:]
            ctx.p4gfrun('edit', '-k', bite)
        while len(tags_to_delete):
            bite = tags_to_delete[:_BITE_SIZE]
            tags_to_delete = tags_to_delete[_BITE_SIZE:]
            ctx.p4gfrun('delete', bite)
        nc.submit()
        if nc.submitted:
            _write_last_copied_tag(ctx, nc.change_num)
    LOG.debug("process_tags() complete")


def _calc_last_copied_tag_p4key_name(repo_name, server_id):
    """Return a name of a p4key that holds the changelist number of the most recently
    updated tag on the given Git Fusion server.
    """
    return p4gf_const.P4GF_P4KEY_LAST_COPIED_TAG.format(repo_name=repo_name, server_id=server_id)


def _last_copied_tag_p4key_name(ctx):
    """Return the name of a p4key that holds the latest tag changelist
    number for this Git Fusion server.
    """
    return _calc_last_copied_tag_p4key_name(ctx.config.repo_name, p4gf_util.get_server_id())


def _read_last_copied_tag(ctx):
    """Return the changelist number for the most recent tags change for this
    Git Fusion server.
    """
    return P4Key.get(ctx.p4gf, _last_copied_tag_p4key_name(ctx))


def _write_last_copied_tag(ctx, change_num):
    """Update the changelist number for the most recent tags change for this
    Git Fusion server.
    """
    return P4Key.set(ctx.p4gf, _last_copied_tag_p4key_name(ctx), change_num)


def tags_exist_in_cache(ctx):
    """Return boolean whether tags exist in object cache.
    """
    tags_path = '{root}/repos/{repo}/tags/...'.format(
        root=p4gf_const.objects_root(), repo=ctx.config.repo_name)
    r = ctx.p4run('files', '-e', '-m1',  tags_path)
    for rr in r:
        if not isinstance(rr, dict):
            continue
        if 'depotFile' in rr:
            return True
    return False


def overwrite_last_copied_tag(p4, repo_name, change_num):
    """Set the changelist number for the most recent tags change.

    :type p4: :class:`P4API`
    :param p4: P4 API instance

    :type repo_name: str
    :param repo_name: name fo the Git Fusion repository

    :type change_num: int || str
    :param change_num: changelist number to assign to tags key.

    """
    keyname = _calc_last_copied_tag_p4key_name(repo_name, p4gf_util.get_server_id())
    LOG.debug('overwrite_last_copied_tag() assigning {} to {}'.format(change_num, keyname))
    return P4Key.set(p4, keyname, change_num)


def any_tags_since_last_copy(ctx):
    """Return the first tags change not yet copied between Git and Perforce.

    If a tags change exists and is > last copied tags change, return it.
    Else return None.
    """
    last_copied_change = _read_last_copied_tag(ctx)
    if not last_copied_change:
        return None
    tags_path = '{root}/repos/{repo}/tags/...'.format(
        root=p4gf_const.objects_root(),
        repo=ctx.config.repo_name)
    head_change = p4gf_util.head_change_as_string(ctx, submitted=True, view_path=tags_path)

    if head_change and int(head_change) > int(last_copied_change):
        pass  # return this head_change
    else:
        head_change = None
    LOG.debug('any_tags_since_last_copy() found new tags: {}'.format(head_change))
    return head_change


def _create_tag_ref(repo, name, sha1):
    """Create a single tag reference in the repository."""
    if not name or not sha1:
        LOG.warning("_create_tag_ref() invalid params: ({}, {})".format(name, sha1))
        return
    if repo.get(sha1) is None:
        LOG.warning("_create_tag_ref() unknown object: {}".format(sha1))
        return
    tag_file = os.path.join('.git', 'refs', 'tags', name)
    p4gf_util.ensure_parent_dir(tag_file)
    with open(tag_file, 'w') as f:
        f.write(sha1)


def _remove_tag_ref(name, sha1):
    """Remove a single tag reference from the repository."""
    if not name or not sha1:
        LOG.warning("_remove_tag_ref() invalid params: ({}, {})".format(name, sha1))
        return
    tag_file = os.path.join('.git', 'refs', 'tags', name)
    if os.path.exists(tag_file):
        os.unlink(tag_file)


def _install_tag(repo, fname):
    """Given the path of a tag copied from Perforce object cache, copy
    the tag to the repository, with the appropriate name and SHA1.
    There may be multiple lightweight tags associated with the same
    SHA1, in which case multiple tags will be created.

    Arguments:
        repo -- pygit2 repository
        fname -- clientFile attr for sync'd tag
    """
    sha1 = fname[-42:].replace('/', '')
    LOG.debug("_install_tag() examining {}...".format(sha1))
    with open(fname, 'rb') as f:
        contents = f.read()
    try:
        zlib.decompress(contents)
        # Must be an annotated tag...
        blob_path = os.path.join('.git', 'objects', sha1[:2], sha1[2:])
        p4gf_util.ensure_parent_dir(blob_path)
        os.link(fname, blob_path)
        tag_obj = repo.get(sha1)
        tag_name = tag_obj.name
        LOG.debug("_install_tag() annotated tag {}".format(tag_name))
        _create_tag_ref(repo, tag_name, sha1)
    except zlib.error:
        # Lightweight tags are stored simply as the tag name, but
        # there may be more than one name for a single SHA1.
        tag_names = contents.decode('UTF-8')
        for name in tag_names.splitlines():
            LOG.debug("_install_tag() lightweight tag {}".format(name))
            _create_tag_ref(repo, name, sha1)


def _uninstall_tag(repo, fname):
    """Given the path of a tag copied from Perforce object cache, remove
    the tag from the repository.

    Arguments:
        repo -- pygit2 repository
        fname -- clientFile attr for sync'd tag
    """
    sha1 = fname[-42:].replace('/', '')
    LOG.debug("_uninstall_tag() examining {}...".format(sha1))
    with open(fname, 'rb') as f:
        contents = f.read()
    try:
        zlib.decompress(contents)
        # Must be an annotated tag...
        tag_obj = repo.get(sha1)
        tag_name = tag_obj.name
        LOG.debug("_uninstall_tag() annotated tag {}".format(tag_name))
    except zlib.error:
        # Lightweight tags are stored simply as the tag name
        tag_name = contents.decode('UTF-8')
        LOG.debug("_uninstall_tag() lightweight tag {}".format(tag_name))
    # Remove the tag reference
    tag_refs = os.path.join('.git', 'refs', 'tags')
    if not os.path.exists(tag_refs):
        return
    tag_file = os.path.join(tag_refs, tag_name)
    if os.path.exists(tag_file):
        os.unlink(tag_file)


def _read_tags(ctx, depot_path, rev=None):
    """Return the set of (lightweight) tag names read from the given file.

    :type ctx: :class:`p4gf_context.Context`
    :param ctx: Git Fusion context

    :type depot_path: str
    :param depot_path: file path within depot

    :type rev: int
    :param rev: if not None, specifies file revision to retrieve

    """
    if rev:
        cmd = ['sync', '-f', "{}#{}".format(depot_path, rev)]
    else:
        cmd = ['sync', '-f', depot_path]
    r = ctx.p4gfrun(cmd)
    r = p4gf_util.first_dict(r)
    with open(r['clientFile'], 'rb') as f:
        contents = f.read()
    tag_names = contents.decode('UTF-8').splitlines()
    return set(tag_names)


@with_timer('update tags')
def update_tags(ctx):
    """Based on the recent changes to the tags, update our repository
    (remove deleted tags, add new pushed tags).
    """
    last_copied_change = _read_last_copied_tag(ctx)
    tags_prefix = '{root}/repos/{repo}/tags/'.format(
        root=p4gf_const.objects_root(), repo=ctx.config.repo_name)
    tags_path = '{prefix}...'.format(prefix=tags_prefix)
    num = 1 + int(last_copied_change)
    r = ctx.p4gfrun('changes', '-s', 'submitted', '-e', num, tags_path)
    changes = sorted(r, key=lambda k: int(k['change']))
    for change in changes:
        LOG.debug2('update_tags() processing {}'.format(change['change']))
        d = ctx.p4gfrun('describe', change['change'])
        d = p4gf_util.first_dict(d)
        for d_file, rev, action in zip(d['depotFile'], d['rev'], d['action']):
            LOG.debug2('update_tags() processing {} - {}'.format(d_file, action))
            if not d_file.startswith(tags_prefix):
                LOG.info('non-tag file {} in tag-related Git Fusion change {}'.format(
                    d_file, change['change']))
                continue
            if action == 'add':
                r = ctx.p4gfrun('sync', '-f', "{}#{}".format(d_file, rev))
                r = p4gf_util.first_dict(r)
                _install_tag(ctx.repo, r['clientFile'])
            elif action == 'delete':
                r = ctx.p4gfrun('sync', '-f', "{}#{}".format(d_file, int(rev) - 1))
                r = p4gf_util.first_dict(r)
                _uninstall_tag(ctx.repo, r['clientFile'])
            elif action == 'edit':
                # get the tags named in the file prior to this change
                tags_before = _read_tags(ctx, d_file, int(rev) - 1)
                # get the tags named in the file after this change
                tags_after = _read_tags(ctx, d_file)
                # remove old (lightweight) tags and add new ones
                sha1 = d_file[-42:].replace('/', '')
                for old_tag in tags_before - tags_after:
                    _remove_tag_ref(old_tag, sha1)
                for new_tag in tags_after - tags_before:
                    _create_tag_ref(ctx.repo, new_tag, sha1)
            else:
                LOG.error("update_tags() received an unexpected change action: " +
                          "@{}, '{}' on {}".format(change['change'], action, d_file))
    _write_last_copied_tag(ctx, changes[-1]['change'])


@with_timer('generate tags')
def generate_tags(ctx):
    """Regenerate the original tags into the (rebuilt) Git repository.

    This should only be called when the repository was just rebuilt
    from Perforce, otherwise it will do a bunch of work for nothing.
    """
    # Fetch everything under //.git-fusion/objects/repos/<repo>/tags/...
    tags_path = NTR('objects/repos/{repo}/tags').format(repo=ctx.config.repo_name)
    with ctx.p4gf.at_exception_level(P4.P4.RAISE_NONE):
        client_path = "//{}/{}/...".format(ctx.p4gf.client, tags_path)
        ctx.p4gfrun('sync', '-f', '-q', client_path)

    # Walk the tree looking for tags, reconstituting those we encounter.
    tags_root = os.path.join(ctx.gitlocalroot, tags_path)
    for walk_root, _dirs, files in os.walk(tags_root):
        for name in files:
            fname = os.path.join(walk_root, name)
            _install_tag(ctx.repo, fname)

    # Update the tag change p4key to avoid repeating our efforts.
    head_change = any_tags_since_last_copy(ctx)
    if head_change:
        LOG.debug("generate_tags:  head_change  {}".  format(head_change))
        _write_last_copied_tag(ctx, head_change)

#! /usr/bin/env python3.3
"""Functions for operating on Git repositories."""

from collections import deque, namedtuple
from contextlib import contextmanager
import configparser
import hashlib
import io
import logging
import os
import shutil
import zlib

import pygit2

import p4gf_char
import p4gf_const
from p4gf_l10n import _, NTR
import p4gf_path
import p4gf_proc
import p4gf_pygit2
import p4gf_util

LOG = logging.getLogger(__name__)

BARE_FALSE = 0
BARE_TRUE = 1
BARE_DONT_CARE = 2
# how many times we re-try setting core.bare before raise()
CONFIG_NUM_ATTEMPTS = 50


def is_bare_git_repo(git_dir=None):
    """Determine if this Git repository is already bare or not."""
    try:
        path = pygit2.discover_repository(git_dir if git_dir else '.')
        repo = pygit2.Repository(path)
        return repo.is_bare
    except KeyError:
        return False
    except ValueError:
        return False


@contextmanager
def non_bare_git(git_dir=None):
    """Set the git repo to non-bare and re-set bare.

    Do not set/reset if already non_bare.

    """
    was_bare = is_bare_git_repo(git_dir)
    if was_bare:
        LOG.debug3("non_bare_git: enter: set bare False")
        # set non_bare
        set_bare(False, git_dir)
    else:
        LOG.debug3("non_bare_git: already non-bare - do nothing")
    try:
        yield
    finally:
        if was_bare:
            # re-set bare
            LOG.debug3("non_bare_git: exit: set bare True")
            set_bare(True, git_dir)


@contextmanager
def bare_git(git_dir=None):
    """Set the git repo to bare and re-set non-bare.

    Do not set/reset if already bare.

    """
    was_bare = is_bare_git_repo(git_dir)
    if not was_bare:
        LOG.debug3("bare_git: enter set bare True")
        # set bare
        set_bare(True, git_dir)
    else:
        LOG.debug3("bare_git: already bare - do nothing")
    try:
        yield
    finally:
        if not was_bare:
            # re-set non-bare
            LOG.debug3("bare_git: exit: set bare False")
            set_bare(False, git_dir)


def set_bare(is_bare, git_dir=None):
    """Reconfigure a repo for --bare or not-bare.

    :param is_bare: True to make repository bare, False to make it non-bare.
    :param git_dir: The .git directory, or current directory if None.

    """
    if git_dir:
        path = pygit2.discover_repository(git_dir)
    else:
        path = pygit2.discover_repository('.')
    repo = pygit2.Repository(path)
    # If the git config lock exists due to another process making this call
    # then that lock should not persist longer than the length of one
    # process call. We'll retry numerous times, raising an exception if
    # unsuccessful.
    num_attempts = CONFIG_NUM_ATTEMPTS
    while num_attempts:
        try:
            repo.config['core.bare'] = 'true' if is_bare else 'false'
            break
        except pygit2.GitError as ge:
            num_attempts -= 1
            if not num_attempts:
                LOG.error("after %s attempts, cannot set git config core.bare: %s",
                          CONFIG_NUM_ATTEMPTS, ge)
                raise RuntimeError(_("cannot set git config core.bare: {exception}")
                                   .format(exception=ge))


def git_checkout(sha1, force=False):
    """Switch to the given sha1.

    Returns True if the checkout was successful (exit status of 0),
    and False otherwise.
    """
    with non_bare_git():
        if force:
            result = p4gf_proc.popen_no_throw(['git', 'checkout', '-f', sha1])
        else:
            result = p4gf_proc.popen_no_throw(['git', 'checkout', sha1])
    return result['ec'] == 0


def checkout_detached_head():
    """Detach HEAD so that we have no current branch.

    Now we can modify any branch without triggering 'can't ___ current branch' errors.

    """
    # no_throw because brand new repos have no commits at all, so
    # even HEAD~0 is an invalid reference.
    if not p4gf_util.git_empty():
        with non_bare_git():
            p4gf_proc.popen_no_throw(['git', 'checkout', 'HEAD~0'])


def delete_branch_ref(ref_name):
    """Delete one Git branch reference."""
    p4gf_proc.popen_no_throw(['git', 'branch', '-D', ref_name])


def delete_branch_refs(ref_names):
    """Delete several Git branch references at once."""
    for chunk in p4gf_util.iter_chunks(ref_names, 300):
        p4gf_proc.popen_no_throw(['git', 'branch', '-D'] + chunk)


def force_branch_ref(ref_name, sha1):
    """Create or change one Git branch reference."""
    p4gf_proc.popen_no_throw(['git', 'branch', '-f', ref_name, sha1])


def delete_tag_ref(ref_name):
    """Delete one Git tag reference."""
    p4gf_proc.popen_no_throw(['git', 'tag', '-d', ref_name])


def force_tag_ref(ref_name, sha1):
    """Create or change one Git tag reference."""
    p4gf_proc.popen_no_throw(['git', 'tag', '-f', ref_name, sha1])


@contextmanager
def head_restorer():
    """Restore the current working directory's HEAD.

    Also restores the working tree to the sha1 it had when created.

    with p4gf_util.head_restorer():
        ... your code that can raise exceptions...
    """
    sha1 = p4gf_util.git_rev_list_1('HEAD')
    if not sha1:
        logging.getLogger("head_restorer").debug(
            "get_head_sha1() returned None, will not restore")
    try:
        yield
    finally:
        if sha1:
            with non_bare_git():
                p4gf_proc.popen(['git', 'reset', '--hard', sha1])
                p4gf_proc.popen(['git', 'checkout', sha1])


def _setup_temp_repo():
    """Create a temporary repo for extracting pack files.

    Set up a temporary Git repository in which to house pack files
    for unpacking into another repository.

    Returns the path to the new .git/objects/pack directory.

    """
    tmpname = 'p4gf_git_tmp'
    tmprepo = os.path.join(os.path.dirname(os.getcwd()), tmpname)
    if os.path.exists(tmprepo):
        shutil.rmtree(tmprepo)
    pygit2.init_repository(tmprepo)
    packdir = os.path.join(tmprepo, '.git', 'objects', 'pack')
    return (tmprepo, packdir)


def unpack_objects():
    """Ensure there are no pack files in the current repository.

    Find all existing pack objects in the Git repository, unpack them,
    and then remove the now defunct pack and index files.

    Returns True if successful, False otherwise.

    """
    pack_dir = os.path.join(".git", "objects", "pack")
    if not os.path.exists(pack_dir):
        return True
    pack_files = [os.path.join(pack_dir, f) for f in os.listdir(pack_dir) if f.endswith('.pack')]
    if pack_files:
        tmprepo, tmp_pack = _setup_temp_repo()
        if not tmp_pack:
            return False
        cmd = ['git', 'unpack-objects', '-q']
        for pack in pack_files:
            fname = os.path.basename(pack)
            newpack = os.path.join(tmp_pack, fname)
            os.rename(pack, newpack)
            index = pack[:-4] + "idx"
            os.rename(index, os.path.join(tmp_pack, fname[:-4] + "idx"))
            ec = p4gf_proc.wait(cmd, stdin=newpack)
            if ec:
                raise RuntimeError(_("git-unpack-objects failed with '{error}'")
                                   .format(error=ec))
        shutil.rmtree(tmprepo)
    return True


def cat_file_to_local_file(sha1, p4filetype, local_file, view_repo):
    """Perform the equivalent of the git-cat-file command on the given object.

    Write content to given local_file path.

    Assumes .git/objects hierarchy stored completely loose, no packed objects.

    """
    blob_bytes = get_blob(sha1, view_repo)
    if p4filetype and 'symlink' in p4filetype:
        os.symlink(blob_bytes, local_file)
    else:
        with open(local_file, 'wb') as fout:
            fout.write(blob_bytes)


def get_commit(sha1, view_repo):
    """Retrieve the text of a Git commit given its SHA1.

    Returns an empty string if the commit file cannot be found.

    """
    blob = cat_file(sha1, view_repo)
    blob = blob[blob.index(b'\x00') + 1:]
    return p4gf_char.decode(blob)


def get_blob(sha1, view_repo):
    """Retrieve the raw blob data contents without the git header."""
    blob = cat_file(sha1, view_repo)
    blob = blob[blob.index(b'\x00') + 1:]
    return blob


def object_exists(sha1, view_repo):
    """Check if a Git object exists in the repository.

    :type sha1: str
    :param sha1: object identifier to query.

    :type view_repo: :class:`pygit2.Repository`
    :param view_repo: pygit2 repository instance.

    :rtype: bool

    """
    return sha1 in view_repo if sha1 else False


def object_path(sha1):
    """Get the path to a Git object, returning None if it does not exist.

    Caller should invoke unpack_objects() first.

    """
    # Files may be named de/adbeef... or de/ad/beef... in .git/objects directory
    base = os.path.join(".git", "objects")
    if not os.path.exists(base):
        LOG.error("Git objects directory missing!")
    path = os.path.join(base, sha1[:2], sha1[2:])
    if not os.path.exists(path):
        path = os.path.join(base, sha1[:2], sha1[2:4], sha1[4:])
        if not os.path.exists(path):
            return None
    return path


def tree_from_commit(blob):
    """Extract the tree SHA1 from the given commit blob.

    For the given commit object data (an instance of bytes), extract the
    corresponding tree SHA1 (as a str). The object header should not be
    part of the input.

    """
    if not isinstance(blob, bytes):
        LOG.error("tree_from_commit() expected bytes, got {}".format(type(blob)))
        return None
    if len(blob) == 0:
        LOG.error("tree_from_commit() expected non-zero bytes")
        return None
    idx = 0
    end = len(blob)
    try:
        while idx < end and blob[idx:idx + 5] != b'tree ':
            idx = blob.index(b'\n', idx + 1) + 1
    except ValueError:
        return None
    nl = blob.index(b'\n', idx + 1)
    return blob[idx + 5:nl].decode()


def exists_in_tree(repo, path, tree=None):
    """Return True if the named path exists in the given tree.

    :param repo: pygit2.Repository instance.
    :param path: slash-separated path and filename to find.
    :param tree: tree is to be considered, or None for HEAD.

    Returns False if the repo is empty, or the path does not exist.

    """
    if repo.is_empty:
        return False
    if tree is None:
        tree = p4gf_pygit2.head_commit(repo).tree
    leading_path, filename = os.path.split(path)
    tree = find_tree(repo, leading_path, tree)
    return filename in tree


def find_tree(repo, path, tree):
    """Locate the tree object for the given path.

    Arguments:
        repo -- pygit2.Repository instance.
        path -- path for which to locate tree.
        tree -- initially the root tree object.

    Returns the pygit2 Tree object, or None if not found.

    """
    if not path or tree is None:
        return tree
    head, tail = os.path.split(path)
    if head:
        tree = find_tree(repo, head, tree)
        if tree:
            tree = p4gf_pygit2.tree_object(repo, tree)
    return p4gf_pygit2.tree_object(repo, tree[tail]) if tree and tail in tree else None


def make_tree(repo, path, tree):
    """Build a new tree structure from the given path.

    Given a tree object that represents the path, build up the parent
    trees, using whatever existing structure already exists in the
    repository. The result will be a tree object suitable for use in
    creating a Git commit.

    Arguments:
        repo -- pygit2.Repository instance.
        path -- path for which to build the tree structure.
        tree -- tree object to represent the path.

    Returns the pygit2 Tree object.

    """
    head, tail = os.path.split(path)
    if not head:
        head_obj = p4gf_pygit2.head_commit(repo)
        rtree = None if head_obj is None else head_obj.tree
        tb = repo.TreeBuilder(rtree) if rtree else repo.TreeBuilder()
        tb.insert(tail, tree.oid, pygit2.GIT_FILEMODE_TREE)
        return repo.get(tb.write())
    else:
        ptree = find_tree(repo, head, tree)
        tb = repo.TreeBuilder(ptree) if ptree else repo.TreeBuilder()
        tb.insert(tail, tree.oid, pygit2.GIT_FILEMODE_TREE)
        ptree = repo.get(tb.write())
        return make_tree(repo, head, ptree)


def _add_to_gitmodules(repo, repo_name, path, url, tb):
    """Update the .gitmodules file and insert the entry in the tree.

    Given the path and URL for a Git repository, add a new submodule
    section to the .gitmodules file in this repository. The changes
    are made directly into the repository, without touching the working
    directory.

    The newly generated file blob will be inserted into the given
    instance of pygit2.TreeBuilder.

    Arguments:
        repo -- pygit2.Repository instance.
        repo_name -- name of the Git Fusion repository (e.g. depot_0xS_foo).
        path -- full path of the submodule.
        url -- URL used to access submodule.
        tb -- pygit2.TreeBuilder to insert entry into tree.

    """
    header = '[submodule "{}"]'.format(path)
    frm = "{header}\n\t{tag} = {repo}\n\tpath = {path}\n\turl = {url}\n"
    section = frm.format(header=header, tag=p4gf_const.P4GF_MODULE_TAG, repo=repo_name,
                         path=path, url=url)
    blob = None
    if not repo.is_empty:
        try:
            head_obj = p4gf_pygit2.head_commit(repo)
            entry = head_obj.tree['.gitmodules']
        except KeyError:
            entry = None
        if entry:
            # modify file and hash to object store
            blob = repo[entry.oid]
            text = blob.data.decode('UTF-8')
            if header in text:
                # TODO: update the existing information?
                oid = entry.oid
            else:
                text = text + '\n' + section
                oid = repo.create_blob(text.encode('UTF-8'))
    if blob is None:
        # generate file and hash to object store
        oid = repo.create_blob(section.encode('UTF-8'))
    sha1 = p4gf_pygit2.oid_to_sha1(oid)
    tb.insert('.gitmodules', sha1, pygit2.GIT_FILEMODE_BLOB)


def add_submodule(repo, repo_name, path, sha1, url, user):
    """Add the named submodule to the repository.

    Adds or modifies the .gitmodules file at the root of the tree.

    :param repo: pygit2.Repository instance.
    :param repo_name: name of the Git Fusion repository (e.g. depot_0xS_foo).
    :param path: full path of the submodule.
    :param sha1: SHA1 of the submodule.
    :param url: URL used to access submodule.
    :param user: one of the p4gf_usermap 3-tuples.

    Returns True if the repository was modified, False otherwise.

    """
    # pylint:disable=too-many-arguments
    leading_path, sub_name = os.path.split(path)
    head_obj = p4gf_pygit2.head_commit(repo)
    tree = None if head_obj is None else head_obj.tree
    tree = find_tree(repo, leading_path, tree)
    tb = repo.TreeBuilder(tree.oid) if tree else repo.TreeBuilder()
    action = 'Updating' if tree and sub_name in tree else 'Adding'
    tb.insert(sub_name, sha1, pygit2.GIT_FILEMODE_COMMIT)
    tree = repo.get(tb.write())
    if leading_path:
        tree = make_tree(repo, leading_path, tree)
    # This unfortunately wastes the previously built tree but simplicity
    # wins over complexity, as does working code.
    tb = repo.TreeBuilder(tree.oid)
    _add_to_gitmodules(repo, repo_name, path, url, tb)
    tree = repo.get(tb.write())
    # Are we actually changing anything?
    head_obj = p4gf_pygit2.head_commit(repo)
    if not repo.is_empty and tree.oid == head_obj.tree.oid:
        # Nope, nothing changed
        return False
    author = pygit2.Signature(user[2], user[1])
    message = _('{action} submodule {path}').format(action=action, path=path)
    parents = [] if repo.is_empty else [head_obj.oid]
    repo.create_commit('HEAD', author, author, message, tree.oid, parents)
    return True


def _empty_configparser():
    """What some parse_gitmodules functions return if no .gitmodules file.
    """
    return configparser.ConfigParser(interpolation=None)

def parse_gitmodules(repo):
    """Read the .gitmodules file and return an instance of ConfigParser.

    Arguments:
        repo -- pygit2.Repository instance.

    Returns an instance of ConfigParser which contains the contents of the
    .gitmodules file. If no such file was found, the parser will be empty.

    """
    r = parse_gitmodules_for_pygit2_commit(repo, p4gf_pygit2.head_commit(repo))
    if r is not None:
        return r
    return _empty_configparser()

def parse_gitmodules_for_pygit2_commit(repo, pygit2_commit):
    """Parse the .gitmodules file for a given commit and return
    as a ConfigParser object.

    Return None if no .gitmodules file found.
    (+++Avoid needless object construction, save 1% Fast Push time.).
    """
    if (not pygit2_commit) or repo.is_empty:
        return None

    try:
        entry = pygit2_commit.tree['.gitmodules']
        blob = repo[entry.oid]
        text = blob.data.decode('UTF-8')
        parser = configparser.ConfigParser(interpolation=None)
        parser.read_string(text, source='.gitmodules')
        return parser
    except KeyError:
        pass
    return None


def parse_gitmodules_for_commit_sha1(repo, commit_sha1):
    """Parse the .gitmodules file for a given commit and return
    as a ConfigParser object.

    Return None if no .gitmodules file found.
    """
    pygit2_commit = None
    try:
        pygit2_commit = repo[commit_sha1]
    except (KeyError, TypeError):
        pass
    return parse_gitmodules_for_pygit2_commit(repo, pygit2_commit)


def _remove_from_gitmodules(repo, path, tb):
    """Update the .gitmodules file and return the new SHA1.

    Arguments:
        repo -- pygit2.Repository instance.
        repo_name -- name of the Git Fusion repository (e.g. depot_0xS_foo).
        path -- full path of the submodule.
        sha1 -- SHA1 of the submodule.
        url -- URL used to access submodule.
        user -- one of the p4gf_usermap 3-tuples.

    """
    modules = parse_gitmodules(repo)
    section_to_remove = None
    for section in modules.sections():
        # we can only consider those submodules under our control
        if modules.has_option(section, p4gf_const.P4GF_MODULE_TAG):
            mpath = modules.get(section, 'path', raw=True, fallback=None)
            if path == mpath:
                section_to_remove = section
                break
    if not section_to_remove:
        return
    modules.remove_section(section_to_remove)
    out = io.StringIO()
    sections = modules.sections()
    count = len(sections)
    pos = 0
    for section in sections:
        out.write('[{name}]\n'.format(name=section))
        for key, value in modules.items(section):
            out.write('\t{key} = {value}\n'.format(key=key, value=value))
        pos += 1
        if pos < count:
            out.write('\n')
    oid = repo.create_blob(out.getvalue().encode('UTF-8'))
    sha1 = p4gf_pygit2.oid_to_sha1(oid)
    tb.insert('.gitmodules', sha1, pygit2.GIT_FILEMODE_BLOB)


def remove_submodule(repo, path, user):
    """Remove the submodule whose path matches the given path.

    :param repo: pygit2.Repository instance.
    :param path: path of submodule to be removed.
    :param user: one of the p4gf_usermap 3-tuples.

    Returns True if the repository was modified, False otherwise.

    """
    if repo.is_empty:
        return False
    leading_path, sub_name = os.path.split(path)
    head_obj = p4gf_pygit2.head_commit(repo)
    tree = find_tree(repo, leading_path, head_obj.tree)
    if not tree:
        return False
    tb = repo.TreeBuilder(tree.oid)
    tb.remove(sub_name)
    tree = repo.get(tb.write())
    if leading_path:
        tree = make_tree(repo, leading_path, tree)
    # This unfortunately wastes the previously built tree but simplicity
    # wins over complexity, as does working code.
    tb = repo.TreeBuilder(tree.oid)
    _remove_from_gitmodules(repo, path, tb)
    tree = repo.get(tb.write())
    # Are we actually changing anything?
    head_obj = p4gf_pygit2.head_commit(repo)
    if tree.oid == head_obj.tree.oid:
        # Nope, nothing changed
        return False
    author = pygit2.Signature(user[2], user[1])
    message = _('Removing submodule {path}').format(path=path)
    repo.create_commit('HEAD', author, author, message, tree.oid, [head_obj.oid])
    return True


def submodule_iter(repo, commit_sha1):
    """Generator that yields (sha1, gwt_path) tuples for every submodule."""
    tree = repo[commit_sha1].tree
                        # +++ Skip the expensive tree walk if there are
                        #     no submodules to find.
                        #
                        # There is a risk that a malformed repo might have
                        # submodule tree entries but no .gitmodule entry.
                        # Such repos will not have their submodules properly
                        # detected or copied to Perforce.
    if ".gitmodules" not in tree:
        return
    te_queue = deque([('', te) for te in tree])
    while te_queue:
        (par_gwt_path, tree_entry) = te_queue.popleft()
        if 0o160000 == tree_entry.filemode:
            gwt_path = p4gf_path.join(par_gwt_path, tree_entry.name)
            yield (gwt_path, tree_entry.hex)

        elif 0o040000 == tree_entry.filemode:
            gwt_path = p4gf_path.join(par_gwt_path, tree_entry.name)
            for child_tree_entry in repo.get(tree_entry.hex):
                te_queue.append((gwt_path, child_tree_entry))


REPO_OBJECT_TYPES = {
    pygit2.GIT_OBJ_BLOB: 'blob',
    pygit2.GIT_OBJ_COMMIT: 'commit',
    pygit2.GIT_OBJ_TREE: 'tree',
    pygit2.GIT_OBJ_TAG: 'tag',
}


def cat_file(sha1, view_repo):
    """Perform the equivalent of the git-cat-file command on the given object.

    Returns an empty bytes object if the file cannot be found.

    Operates in memory, so please do not call this for blobs of unusual size.
    Use only for commit and tree and other small objects.

    Does not require unpack objects.

    """
    if not view_repo:
        LOG.error("cat_file : view_repo object not defined. Stopping")
        raise RuntimeError(_("view_repo object not defined. Stopping"))

    if sha1 == p4gf_const.EMPTY_TREE_SHA1:
        LOG.debug2('cat_file of empty tree')
        return b''

    git_object = view_repo.get(sha1)
    if not git_object:
        LOG.error("cat_file sha1='{0}' not in repository".format(sha1))
        return b''

    object_type = REPO_OBJECT_TYPES[git_object.type]
    data = git_object.read_raw()
    size = len(data)
    header = (object_type + ' ' + str(size) + '\0').encode()
    data = b"".join([header, data])
    return data


def write_git_object_from_sha1(view_repo, object_sha1, target_path):
    """Utility to extract commit,tree,blob,tag objects from a git (packed/unpacked) object-store,
    and write git object to file path.
    The target_path will be written with a re-constructed git object extracted via pygit2
    :param: view_repo     pygit2 repository
    :param: object_sha1   sha1 in git object store
    :param: target_path   path of new git object
    """
    data = cat_file(object_sha1, view_repo)

    p4gf_util.ensure_parent_dir(target_path)
    try:
        with open(target_path, 'wb') as compressed:
            compress = zlib.compressobj()
            compressed.write(compress.compress(data))
            compressed.write(compress.flush())
    except Exception as e:
        LOG.exception("Failed to write sha1='%s' from repo to path '%s'",
                      object_sha1, target_path)
        raise RuntimeError(_("Failed to write sha1='{sha1}' from repo to path {path}.\n{exception}")
                           .format(sha1=object_sha1, path=target_path, exception=e))


def cat_file_compressed(view_repo, object_sha1):
    """Utility to extract a object from Git as raw uncompressed bytes,
    then compress it for storage in Perforce.
    Must produce the same bits as write_git_object_from_sha1().

    Returns the compressed bytes in memory instead of writing to file.

    Intended for commit/tree objects, not huge blobs.
    """
    raw_data = cat_file(object_sha1, view_repo)
    compressed_bytes = zlib.compress(raw_data)
    return compressed_bytes


def diff_trees(repo, tree1, tree2, func):
    """Diff two Git trees via pygit2, our own comparisons, and
    recursive gwt-walk.

    See also git_diff_tree() for a version that calls 'git diff-tree'.
    """
    # pylint: disable=too-many-branches
    iter1 = iter(tree1)
    iter2 = iter(tree2)
    entry1 = next(iter1, None)
    entry2 = next(iter2, None)

    def visit_left():
        """Process left side of tree."""
        # pylint: disable=used-before-assignment
        # Using variable 'entry1' before assignment
        nonlocal entry1
        func(entry1, None)
        if entry1.filemode == 0o040000:
            diff_trees(repo, repo[entry1.oid], [], func)
        entry1 = next(iter1, None)

    def visit_right():
        """Process right side of tree."""
        # pylint: disable=used-before-assignment
        # Using variable 'entry2' before assignment
        nonlocal entry2
        func(None, entry2)
        if entry2.filemode == 0o040000:
            diff_trees(repo, [], repo[entry2.oid], func)
        entry2 = next(iter2, None)

    while True:
        if entry1 is None and entry2 is None:
            break
        if entry1 is None:
            visit_right()
        elif entry2 is None:
            visit_left()
        elif entry1.name < entry2.name:
            visit_left()
        elif entry1.name > entry2.name:
            visit_right()
        else:
            if entry1.oid != entry2.oid:
                func(entry1, entry2)
                if entry1.filemode == 0o040000 and entry2.filemode == 0o040000:
                    diff_trees(repo, repo[entry1.oid], repo[entry2.oid], func)
                elif entry1.filemode == 0o040000:
                    diff_trees(repo, repo[entry1.oid], [], func)
                elif entry2.filemode == 0o040000:
                    diff_trees(repo, [], repo[entry2.oid], func)
            entry1 = next(iter1, None)
            entry2 = next(iter2, None)


def visit_tree(repo, tree, func):
    """Visit every file in the tree, recursing into trees."""
    for entry in tree:
        if entry.filemode == 0o040000:
            visit_tree(repo, repo[entry.oid], func)
        else:
            func(entry)


class DiffTreeItems:


    """An interator for managing output from git diff-tree.

    For A/M/T actions, returns a 3-tuple:
        (parts, gwt_path, None)

    For C/R actions, returns a 3-tuple:
        (parts, from_gwt_path, to_gwt_path)

    "parts" is a space-delimited string, usually something like this:

    "<mode-old> <mode-new> <sha1-old> <sha1-new> <action>"

    ":100644 100644 03676fbb5340edde0c04b7180ba2b71d7086dc5b"
    " 3f90e54c25822e765302d06ee2f6dce46f13d6dc R081
    """

    def __init__(self, item_list):
        self.item_list = item_list
        # split('\0') on the git diff-tree results
        # returns an empty string as the last item
        if len(self.item_list[-1]) == 0:
            del self.item_list[-1]
        self.index = 0
        self.maxx = len(self.item_list) - 1

    def __iter__(self):
        return self

    def __next__(self):
        if self.index > self.maxx:
            raise StopIteration
        l1 = self.item_list[self.index]
        l2 = self.item_list[self.index+1]
        l3 = None
        action = l1.split()[4][0]  # get the first char of the 5th field
        if action in ['C', 'R']:
            l3 = self.item_list[self.index+2]
        self.index += 2
        if l3:
            self.index += 1
        return (l1, l2, l3)


def git_diff_tree(old_sha1, new_sha1, find_copy_rename_args=None):
    """Run 'git diff-tree -r --name-status <a> <b>' and return the results
    as a list of GitDiffTreeResult <action, gwt_path> tuples.

    See also diff_trees() for a more pygit2/in-process implementation.

    Ideally we would run git diff-tree -r WITHOUT -z or --name-status,
    which would include old/new mode and sha1. But that munges the
    gwt_path, any non-printing chars in the file path are converted
    and the path enquoted. Run 'git ls-tree -r -z' to extract mode and sha1.

    Extracted from Matrix 2
    """
    cmd = ['git', 'diff-tree', '-r', '-z']  # -z = machine-readable \0-delimited output
    if find_copy_rename_args:               # use copy_rename argugments if set
        cmd.extend(find_copy_rename_args)
    cmd.extend([old_sha1, new_sha1])
    d = p4gf_proc.popen(cmd)
                                    # pylint:disable=anomalous-backslash-in-string
                                    # Anomalous backslash in string: '\0'
                                    # Known bug in pylint 0.26.0
                                    #            fixed in 0.27.0
    dtis = DiffTreeItems(d['out'].split('\0'))
    # for pair in p4gf_util.pairwise(d['out'].split('\0')):
    for tupe in dtis:
        # tupe format matches that of output from git diff-tree
        # C/R tupe format: (parts, from_path, path)
        # A/M/D tupe format: (parts, path, None)
        parts = tupe[0].split()
        if parts and parts[0] == ':160000' or parts[1] == '160000':
            # Skip over submodules, cannot process them
            continue
        action = parts[4][0]
        if action in ['C', 'R']:
            # C/R tupe format: (parts, from_path, path)
            yield GitDiffTreeResult(
                      action    = action
                    , gwt_path  = tupe[2]
                    , from_path = tupe[1]
                    , old_mode  = parts[0]
                    , new_mode  = parts[1]
                    , old_sha1  = parts[2]
                    , new_sha1  = parts[3]
                    )
        else:
            # A/M/D tupe format: (parts, path, None)
            yield GitDiffTreeResult(
                      action    = action
                    , gwt_path  = tupe[1]
                    , from_path = None
                    , old_mode  = parts[0]
                    , new_mode  = parts[1]
                    , old_sha1  = parts[2]
                    , new_sha1  = parts[3]
                )


# "struct" for git-diff-tree --name-status result rows.
GitDiffTreeResult = namedtuple('GitDiffTreeResult', [
          'action'
        , 'gwt_path'
        , 'from_path'
        , 'old_mode'
        , 'new_mode'
        , 'old_sha1'
        , 'new_sha1'
        ])


def git_gc_auto(git_dir=None):
    """Invoke git gc --auto to pack objects after copy_p4_to_git only if poll_only."""
    if git_dir:
        cmd = ['git', '--git-dir=' + git_dir, 'gc', '--auto']
    else:
        cmd = ['git', 'gc', '--auto']
    result = p4gf_proc.popen_no_throw(cmd)
    LOG.info("poll_only: git gc --auto returns: {0}".format(result))


def is_valid_git_branch_name(git_branch_name):
    """Determine if the given name refers to a valid git branch."""
    cmd = ['git',  'check-ref-format', '--allow-onelevel',  git_branch_name]
    result = p4gf_proc.popen_no_throw(cmd)
    return not result['ec']


@contextmanager
def suppress_gitattributes(ctx):
    """Temporarily overide gitattributes with .git/info/attributes
    with settings which suppress git's line ending and filter conversions.
    This so that we can 'git hash-object' blobs into the repo.

    There is no pygit2 API for 'git hash-object --no-filters'.
    N.B. pygit2 seems to ignore filter settings in create_blob_fromdisk().
    """
    info_attributes_path = ctx.repo_dirs.GIT_DIR + '/info/attributes'
    with open(info_attributes_path, 'w') as f:
        f.write('* -text -eol -filter\n')
    try:
        yield
    finally:
        os.remove(info_attributes_path)


def git_hash_object(local_path):
    """Return the sha1 that 'git-hash-obect <local_path>' would return.

    Do it without the process overhead of launching git-hash-object.

    Does not write object to .git store.

    Unlike Git's own git-hash-object, git_hash_object() is smart enough
    to not dereference a symlink file. Returns hash of symlink itself.
    """
    if os.path.islink(local_path):
        return git_hash_object_symlink(local_path)
    else:
        return git_hash_object_not_symlink(local_path)

# Header that goes at the top of each blob.
# {} is uncompressed byte count of content.
# Header, uncompressed, along with uncompressed content, is all sha1ed.
# Compress header+content together and store as object under that sha1.
_BLOB_HEADER = NTR('blob {}\0')


def git_hash_object_not_symlink(local_path):
    """Return the sha1 that 'git-hash-obect <local_path>' would return.

    Do it without the process overhead of launching git-hash-object.

    Does not write object to .git store.
    """
    # Push the the "blobNNN\0" header, uncompressed,
    # through the sha1 calculator.
    sha1                    = hashlib.sha1()  # pylint: disable=maybe-no-member
    uncompressed_byte_count = os.lstat(local_path).st_size
    header                  = _BLOB_HEADER.format(uncompressed_byte_count)
    sha1.update(header.encode())

    # Pump file content, uncompressed, through the sha1 calculator.
    with open(local_path, 'rb') as f:
        chunksize = 4096
        while True:
            chunk = f.read(chunksize)
            if chunk:
                sha1.update(chunk)
            else:
                break

    return sha1.hexdigest()


def git_hash_object_symlink(local_path):
    """Return sha1 of a symlink's internal data (the path stored in the link file).

    !!! CAN RETURN INCORRECT RESULTS !!!

    Python's os.readlink() returns a unicode string, not the raw bytes from the
    symlink file. We convert back to bytes using default encoding, which may or
    may not round-trip to the original bytes.

    Use only where incorrect results are annoying but not disastrous.
    """
    assert os.path.islink(local_path)

    # os.readlink() converts the symlink file's content to a Unicode string,
    # _possibly_ using the special roundtrippable os.fsdecode()/os.fsencode()
    # encode. Use os.fsencode() and hope that restores the string back to the
    # symlink's original bytes.
    symlink_content         = os.readlink(local_path)
    data                    = os.fsencode(symlink_content)

    uncompressed_byte_count = len(data)
    header                  = _BLOB_HEADER.format(uncompressed_byte_count)

    sha1                    = hashlib.sha1()  # pylint: disable=maybe-no-member
    sha1.update(header.encode())
    sha1.update(data)
    return sha1.hexdigest()


def git_fsck(obj=None):
    """Invoke git fsck to verify the object database."""
    cmd = ['git', 'fsck']
    if obj:
        cmd.append(str(obj))
    result = p4gf_proc.popen_no_throw(cmd)
    LOG.info("git-fsck returned: {}".format(result))
    return result


def git_prune_and_repack():
    """Invoke git prune and git repack clear any danglers from the object database."""
    cmd = ['git', 'prune']
    result = p4gf_proc.popen_no_throw(cmd)
    LOG.info("git-prune returned: {}".format(result))
    cmd = ['git', 'repack']
    result = p4gf_proc.popen_no_throw(cmd)
    LOG.info("git-repack returned: {}".format(result))
    return result

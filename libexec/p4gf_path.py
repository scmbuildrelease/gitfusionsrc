#! /usr/bin/env python3.3
"""Path utilities broken out of p4gf_util's kitchen sink."""

import os

import p4gf_const
from   p4gf_l10n    import NTR


def _cwd_to_dot_git():
    """If cwd or one of its ancestors is a .git return that .git directory.

    If not, return None.
    """
    path = os.getcwd()
    while path:
        (path2, tail) = os.path.split(path)
        if path2 == path:
            # Give up once split() stops changing the path: we've hit root.
            break
        path = path2
        if tail == '.git':
            return os.path.join(path, tail)
    return None


def cwd_to_repo_name():
    """Derive the repo name from the current working directory's path.

    It's the 'foo' in 'foo/.git/'.
    """
    # Fall back to using directory name as repo name.
    path = _cwd_to_dot_git()
    if path:
        git_path = os.path.dirname(path)
        repo_path = os.path.dirname(git_path)
        repo_name = os.path.basename(repo_path)
        return repo_name
    return None


def find_ancestor(path, ancestor):
    """Walk up the path until you find a dir named 'ancestor', returning the path.

    Return None if no ancestor called 'ancestor'.
    """
    path = path
    while path:
        (path2, tail) = os.path.split(path)
        if path2 == path:
            # Give up once split() stops changing the path: we've hit root.
            break

        if tail == ancestor:
            return path

        path = path2
    return None


def strip_trailing_delimiter(path):
    """Remove trailing /."""
    if path.endswith('/'):
        return path[:-1]
    return path


def strip_leading_delimiter(path):
    """Remove initial /.

    NOP if starts with // depot path prefix.
    """
    if path.startswith("//"):
        return path
    if path.startswith('/'):
        return path[1:]
    return path


def join(a, b):
    """Return "a/b".

    If either a or b is empty, return only a or b (whichever non-empty).
    """
    if a and b:
        return strip_trailing_delimiter(a) + '/' + strip_leading_delimiter(b)
    elif b:
        return b
    else:
        return a


def join_non_empty(grout, a, b):
    """Return a + grout + b, using grout only if both a and b."""
    if a and b:
        return a + grout + b
    if a:
        return a
    return b


def force_trailing_delimiter(path):
    """Make sure path ends with /."""
    if not path.endswith('/'):
        return path + '/'
    else:
        return path


def dequote(path):
    """Strip leading and trailing double-quotes if both present, NOP if not."""
    if (2 <= len(path)) and path.startswith('"') and path.endswith('"'):
        return path[1:-1]
    return path


def enquote(path):
    """Path with space char requires double-quotes, all others pass through unchanged."""
    if ' ' not in path:
        return path
    # Already enquoted? return unchanged.
    if 2 <= len(path) and path[0] == path[-1] == '"':
        return path

    return '"' + path + '"'


def dir_path_iter(file_path):
    """Iterator/generator that yields each directory path.

    //depot/main/bob/file.txt ==> //depot/main/bob
                                  //depot/main
                                  //depot

    """
    rf = file_path.rfind('/', 0, len(file_path))
    while 0 < rf:
                        # Special case for initial "//" to not produce "/".
        if rf == 1 and file_path[0] == '/':
            break
        yield file_path[:rf]
        rf = file_path.rfind('/', 0, rf - 1)


def greatest_common_dir(path_list):
    """Return the longest path ending in "/" that is a prefix for every path in the list.

    Returned string includes that trailing "/".

    Return empty string if no common directory.
    """
    common = os.path.commonprefix(list(path_list))

    # Stop at internal wildcards.
    for wildcard in ["*", "%%", "..."]:
        i = common.find(wildcard)
        if 0 <= i:
            common = common[:i]

    i = common.rfind("/")
    if i < 0:
        return ""
    else:
        # +1 to include trailing /. Need that
        # to avoid matching "dir2/" with "dir/".
        return common[:i+1]


def slashify_sha1(sha1):
    """Convert a SHA1 to the path form for use in Perforce.

    For instance, 60eaf72224a34f592636271fa957b6c4acaee5f3
    becomes 60/ea/f72224a34f592636271fa957b6c4acaee5f3
    which can then be used to build a file path.
    """
    if sha1 == '*':
        return '*/*/*'
    return sha1[:2] + "/" + sha1[2:4] + "/" + sha1[4:]


def slashify_blob_sha1(sha1):
    """Convert a SHA1 to the path form for use in Perforce.

    This splits the value into four levels, suitable for storing a large
    number of objects, such as blobs.

    For instance, 60eaf72224a34f592636271fa957b6c4acaee5f3
    becomes 60/ea/f7/22/24a34f592636271fa957b6c4acaee5f3
    which can then be used to build a file path.
    """
    if sha1 == '*':
        return '*/*/*/*/*'
    return os.path.join(sha1[:2], sha1[2:4], sha1[4:6], sha1[6:8], sha1[8:])


def tree_p4_path(tree_sha1):
    """Return depot path to a tree."""
    return (NTR('{objects_root}/trees/{slashed}')
            .format(objects_root=p4gf_const.objects_root(),
                    slashed=slashify_sha1(tree_sha1)))


def blob_p4_path(blob_sha1):
    """Return depot path to a blob."""
    return (NTR('{objects_root}/blobs/{slashed}')
            .format(objects_root=p4gf_const.objects_root(),
                    slashed=slashify_blob_sha1(blob_sha1)))

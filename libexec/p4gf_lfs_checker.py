#! /usr/bin/env python3.3
"""Class to check git LFS settings.

This could easily be subclassed with minor tweaks for any git attribute.

"""

from collections import namedtuple
import copy
import fnmatch
import logging
import os
import re

import pygit2

import p4gf_branch
import p4gf_git
from p4gf_l10n import _
import p4gf_lfs_attributes
import p4gf_util

LOG = logging.getLogger(__name__)

CommitAttribute    = namedtuple('CommitAttribute', ['tree', 'dirpath_attributes_dict'])
P4ChangeAttribute  = namedtuple('P4ChangeAttribute', ['p4change', 'dirpath_attributes_dict'])

# The dirpath_attributes_dict:
#   directory_path : [list of lfs patterns]
#                    [] may be empty
#  If a .gitattributes directory exists collect all 'filter=lfs' patterns into [...]
#  [] will be empty if either no .gitattributes file exists or exists with no filter=lfs patterns.

# single * matches anything but /
SINGLE_STAR_RE = re.compile(r'([^*])\.\*([^*])')
SINGLE_STAR_REPL = r'\1[^/]*\2'
# leading **/...
LEADING_DOUBLE_STAR_RE = re.compile(r'^\.\*\.\*\\/')
LEADING_DOUBLE_STAR_REPL = r'([^/]*/)?'
# internal ../**/...
INTERNAL_DOUBLE_STAR_RE = re.compile(r'\/\.\*\.\*\/')
INTERNAL_DOUBLE_STAR_REPL = r'.*'
# trailing .../**
TRAILING_DOUBLE_STAR_RE = re.compile(r'\/\.\*\.\*$')
TRAILING_DOUBLE_STAR_REPL = r'/.+'
# no / matches basename
NO_SLASH_RE = r'(.*/)?'

# pattern doesn't match path
# tracking inherited from parent dir
NO_MATCH = 0
# pattern matches path, filter=lfs
# tracking turned on
TRACK = 1
# pattern matches path, -filter=lfs
# tracking turned off
UNTRACK = 2


def path_matches_pattern(path, pattern):
    """Return True if path matches the LFS filter pattern.

    See gitignore and gitattributes documentation for more on this.
    """
    # gitignore documentation to the contrary notwithstanding, a pattern ending
    # with '/' matches nothing when used for gitattributes.
    if pattern.endswith('/'):  # directory pattern
        return False

    # Git documentation quoted here:
    #
    # Git treats the pattern as a shell glob suitable for consumption by
    # fnmatch(3) with the FNM_PATHNAME flag: wildcards in the pattern will not
    # match a / in the pathname.
    #
    # However, Python's fnmatch does not support FNM_PATHNAME, so use fnmatch to
    # create a regex from pattern and then modify it as needed.
    regex = fnmatch.translate(pattern)
    if '/' in pattern:
        regex = re.sub(SINGLE_STAR_RE, SINGLE_STAR_REPL, regex)
        regex = re.sub(LEADING_DOUBLE_STAR_RE, LEADING_DOUBLE_STAR_REPL, regex)
        regex = re.sub(INTERNAL_DOUBLE_STAR_RE, INTERNAL_DOUBLE_STAR_REPL, regex)
        regex = re.sub(TRAILING_DOUBLE_STAR_RE, TRAILING_DOUBLE_STAR_REPL, regex)
    else:
        regex = NO_SLASH_RE + regex
    return bool(re.match(regex, path))


def is_attr_set_in_dir(dirpath_attributes_dict, dpath, path):
    """Return NO_MATCH/TRACK/UNTRACK for this path in this directory.

    :param dict dirpath_attributes_dict:
    :param str dpath: directory possibly containing .gitattributes
    :param str path: gwt_path to check

    """
    if dpath in dirpath_attributes_dict:
        for pattern in dirpath_attributes_dict[dpath]:
            if path_matches_pattern(path, pattern[1]):
                if pattern[0]:
                    return TRACK
                else:
                    return UNTRACK
    return NO_MATCH


def parse_gitattributes_line(elements):
    """Return LFS pattern if any for a .gitattributes file line, else None.

    :param list elements: contents of the line split by whitespace.

    """
    for element in elements[:0:-1]:
        if element == 'filter=lfs':
            return [True, elements[0]]
        if element == '-filter=lfs':
            return [False, elements[0]]
    return None


def parse_gitattributes(content):
    """Return a list of all the LFS patterns from a .gitattributes file.

    Return an empty list if any error occurs parsing the content.

    :param str  content: content of .gitattributes file

    """
    try:
        patterns = [parse_gitattributes_line(elements) for elements
                    in [line.split() for line
                        in [line.strip() for line
                            in content.decode("utf-8").splitlines()
                            ]
                        if line and not line.startswith('#')
                        ]
                    ]
        return [x for x in patterns if x][::-1]  # Nones removed, reversed
    except (UnicodeDecodeError, RuntimeError, ValueError):
        return []


class LFSChecker:

    """Class to determine whether lfs attribute is set for paths."""

    def __init__(self, ctx=None, gitdir=None):
        """Initialize the class.

        :param Context ctx:
        :param str gitdir: path of git repo's --git-dir
               if set, use to create pygit2.Repository

        Both ctx and gitdir cannot be set.  To use p4 -> git direction, ctx
        must be set.

        """
        self.ctx = ctx
        self.gitdir = gitdir
        self.attribute = 'filter=lfs'
        # {commit_sha1: (tree_sha1, { path: [pattern], ...}), ...}
        self.commit_gitattributes_dict = {}
        # nested dictionary [branch][change#] -> list of patterns
        self.change_gitattributes_dict = {}
        if not bool(ctx) ^ bool(gitdir):
            raise RuntimeError(_("Either ctx or gitdir (but not both) must be set in LFSChecker"))
        if gitdir:
            self.repo = pygit2.Repository(gitdir)
        else:
            self.repo = self.ctx.repo

    def _commit_attribute_for_commit(self, commit_sha1):
        """Return the CommitAttribute for the commit.

        Insert a new CommitAttribute into the commit_gitattributes_dict if
        none there yet.
        """
        if commit_sha1 not in self.commit_gitattributes_dict:
            commit = self.repo.revparse_single(commit_sha1)
            if not commit:
                raise RuntimeError(_('Rev not in git repo: {commit_sha1}')
                                   .format(commit_sha1=commit_sha1))
            self.commit_gitattributes_dict[commit_sha1] = CommitAttribute(commit.tree, {})

        return self.commit_gitattributes_dict[commit_sha1]

    def _load_gitattributes_for_path(self, commit_attribute, gwt_path):
        """Ensure the .gitattributes files for dir containing a path have been loaded.

        For dir containing gwt_path, and all parent dirs, add an entry to
        commit_attribute keyed by the dir's path and containing all LFS
        patterns found in .gitattributes files in that dir.

        If a dir has no .gitattributes file, insert the path with an empty
        pattern list so it will not be processed again.

        Return the path of the dir containing gwt_path.
        """
        # get the containing dir
        path_dir = os.path.dirname(gwt_path)
        if path_dir in commit_attribute.dirpath_attributes_dict:
            return path_dir
        tree, attributes = commit_attribute
        dpath = path_dir
        while True:
            ga_path = os.path.join(dpath, '.gitattributes')
            # if we've already processed this one, skip it and its parents
            if dpath in attributes:
                break
            if ga_path in tree:
                entry = tree[ga_path]
                attributes[dpath] = parse_gitattributes(p4gf_git.get_blob(
                    entry.id, self.repo))
            else:
                attributes[dpath] = []
            # if not at top, move up the directory tree
            if dpath == '':
                break
            dpath = os.path.dirname(dpath)
        return path_dir

    def is_tracked_git(self, *,  commit_sha1, gwt_path):
        """Return boolean for whether gwt_path has lfs attribute set.

        :param: str commit_sha1: for this commit
        :param: str gwt_path: does this path have the attribute set

        """
        commit_attribute = self._commit_attribute_for_commit(commit_sha1)
        path_dir = self._load_gitattributes_for_path(commit_attribute, gwt_path)

        while True:
            track = is_attr_set_in_dir(commit_attribute.dirpath_attributes_dict,
                                       path_dir, gwt_path)
            if track == TRACK:
                return True
            if track == UNTRACK:
                return False
            if path_dir == '':    # top of tree .. all done .. so no match
                return False
            path_dir = os.path.dirname(path_dir)  # move up the directory tree

    def add_cl(self, *, branch, p4change):
        """Find .gitattributes files on branch@p4change and cache any lfs lines."""
        # pylint:disable=too-many-branches
        LOG.debug('add_cl {} on {}'.format(p4change.change, branch.branch_id))
        # if first change on this branch, add dict change# -> P4ChangeAttribute
        branch_name = branch.git_branch_name
        if branch_name in self.change_gitattributes_dict:
            # not the first change on this branch
            if p4change.change in self.change_gitattributes_dict[branch_name]:
                raise RuntimeError(_("Change {change} seen more than once in branch {branch}")
                                   .format(change=p4change.change, branch=branch_name))
            # find the parent change and its .gitattributes
            prev_change = max(self.change_gitattributes_dict[branch_name].keys())
            init_dict = self.change_gitattributes_dict[branch_name][prev_change]\
                .dirpath_attributes_dict
        else:
            # first change on this branch
            self.change_gitattributes_dict[branch_name] = {}

            # first, use git-lfs-initial-track config option, if present
            init_dict = {}
            initial_content = p4gf_lfs_attributes.generate_initial_lfs_attrs(self.ctx)
            if initial_content:
                init_dict[''] = parse_gitattributes(initial_content)

            # then add in any .gitattributes in effect prior to this change
            r = self.ctx.p4run(
                'files',
                '//.../.gitattributes@{}'.format(int(p4change.change) - 1))
            for change_file in r:
                depot_path = change_file['depotFile']
                gwt_dir = _get_gwt_path(self.ctx, depot_path, branch, p4change.change)
                init_dict[gwt_dir] = parse_gitattributes(
                    p4gf_util.print_depot_path_raw(self.ctx.p4, depot_path, p4change.change))

            prev_change = p4change.change - 1
            self.change_gitattributes_dict[branch_name][prev_change] = \
                P4ChangeAttribute(prev_change, init_dict)

        # get .gitattributes delta for p4change
        change_dict = {}
        for change_file in p4change.files:
            depot_path = change_file.depot_path
            if not depot_path.endswith('/.gitattributes'):
                continue
            is_delete = change_file.action == 'delete'
            gwt_dir = _get_gwt_path(self.ctx, depot_path, branch, p4change.change)
            # If top level .gitattributes in first change doesn't exist in P4,
            # that means we inserted it with contents of initial tracking.
            # Since we took care of that above, skip it here.
            if (len(self.change_gitattributes_dict[branch_name]) == 1 and
                    gwt_dir == '' and
                    not is_delete and
                    not p4gf_util.depot_file_exists(self.ctx.p4, depot_path)):
                continue
            # When .gitattributes is deleted, we want to remove the path from
            # the dict rather than set it to an empty pattern list.
            if is_delete:
                if gwt_dir in change_dict:
                    del init_dict[gwt_dir]
            else:
                change_dict[gwt_dir] = parse_gitattributes(
                    p4gf_util.print_depot_path_raw(self.ctx.p4, depot_path, p4change.change))

        # if nothing changed, just ref the previous dict, saving a bit of memory
        # otherwise apply this change's delta to the previous changes attrs
        if change_dict:
            updated_dict = copy.deepcopy(init_dict)
            updated_dict.update(change_dict)
        else:
            updated_dict = init_dict

        self.change_gitattributes_dict[branch_name][p4change.change] = \
            P4ChangeAttribute(p4change.change, updated_dict)

    def is_tracked_p4(self, *, branch, p4change, gwt_path):
        """Return boolean for whether gwt_path has lfs attribute set.

        p4change is a Helix P4Change instance not yet written to git-fast-import.

        :param str branch: branch  for detecting .gitattributes files
        :param str p4change:   changenum for detecting .gitattributes files
        :param str gwt_path:   path to which lfs track detection is applied

        """
        branch_name = branch.git_branch_name
        change_attribute = self.change_gitattributes_dict[branch_name][p4change.change]
        path_dir = os.path.dirname(gwt_path)  # get the containing dir

        while True:
            track = is_attr_set_in_dir(change_attribute.dirpath_attributes_dict,
                                       path_dir, gwt_path)
            if track == TRACK:
                return True
            if track == UNTRACK:
                return False
            if path_dir == '':    # top of tree .. all done .. so no match
                return False
            path_dir = os.path.dirname(path_dir)  # move up the directory tree


def _get_gwt_path(ctx, depot_path, branch, change_num):
    """Return the Git working tree path of the given depot path."""
    all_files = branch.p4_files(ctx, at_change=change_num, depot_path=depot_path)
    g_files = p4gf_branch.Branch.strip_conflicting_p4_files(all_files)
    for file in g_files:
        if file['depotFile'] == depot_path:
            if 'delete' in file['action']:
                return ''
            return os.path.dirname(file['gwt_path'])
    return ''

#! /usr/bin/env python3.3
"""Detect and report case conflicts.

NOPs for case-sensitive Perforce server P4D.

Keeps track of all Git Work Tree paths seen in either Perforce or in Git,
and flags any conflicts.
"""
from   collections import defaultdict
import functools
import logging
import os

import p4gf_branch
from   p4gf_filemode                import FileModeInt
from   p4gf_l10n                    import _, NTR
import p4gf_util

LOG = logging.getLogger(__name__)


def tree_occurrence_count(repo, tree, mono):
    """Return the count of occurrences of mono in tree.

    mono is a reversed list of monocased path elements
    """
    name = mono.pop()
    count = 0
    for te in tree:
        if monocase(te.name) == name:
            if mono:
                if te.filemode == FileModeInt.DIRECTORY:
                    count = count + tree_occurrence_count(repo, repo[te.id], list(mono))
            else:
                count = count + 1
    return count


def conflict_text_one_sub(path_list):
    """Return an error message for a list of conflicting Path objects."""
    line_list = [_("Case conflicts:"), _HEADER]
    line_list.extend(
       [_FORMAT.format( sha1       = p4gf_util.abbrev(path.commit_sha1)
                      , gwt        = path.gwt_path
                      , depot_path = path.depot_path
                      )
        for path in path_list])
    return '\n'.join(line_list)


class BranchCaseConflictChecker:

    """Check a single branch for conflicts."""

    def __init__(self, ctx):
        self.ctx = ctx

        # per monocased GWT path, a list of Path objects
        # if GWT path exists in p4, the first entry will be for that
        # following will be each reference to the GWT path seen in fast-export
        # if a 'D' is seen for the GWT path, the list is reset
        self.mono_to_path_list = defaultdict(list)

    def add_path(self, path):
        """Add a Path.

        Used for paths reported by p4 files or for git-fast-export 'M'.
        """
        self.mono_to_path_list[monocase(path.gwt_path)].append(path)

    def delete_path(self, path):
        """Delete a Path.

        Used when a git-fast-export 'D' is seen.
        """
        self.mono_to_path_list[monocase(path)].clear()

    def is_conflict(self, mono, path_list):
        """Test if there is a case conflict for GWT path mono.

        mono is the monocased GWT path
        path_list is a list of Path objects.
        """
        path_set = {path.gwt_path for path in path_list}
        if len(path_set) <= 1:
            # can be none if file was deleted or renamed
            return False
        if all([p.depot_path is None for p in path_list]):
            # if all references to path come from git, the conflict must be real
            return True

        # When p4 has a path and git-fast-export reports a 'M' path that
        # conflicts, we need to use git-ls-tree to see if there's actually a
        # 'D' that we're not seeing, making this not a real conflict.
        #
        # Check the tree at each commit that mentions this path and see if at
        # any time there was a case conflict.
        repo = self.ctx.repo
        for path in path_list:
            if not path.commit_sha1:
                continue
            mono_parts = []
            head, tail = os.path.split(mono)
            while head:
                mono_parts.append(tail)
                head, tail = os.path.split(head)
            mono_parts.append(tail)
            tree = repo.get(path.commit_sha1).tree
            if tree_occurrence_count(repo, tree, mono_parts) != 1:
                return True
        return False

    def conflict_text_one(self, mono):
        """Return a string suitable for use as a user-visible error message.

        Return None if no conflicts.

        Intended for dump to 'git push' user's stderr.
        2+ lines for each conflict, showing all the conflicting cases
        and from where they originated.
        """
        paths = self.mono_to_path_list[mono]
        if not self.is_conflict(mono, paths):
            return ''
        return conflict_text_one_sub(paths)

    def conflict_text(self):
        """Return a string suitable for use as a user-visible error message.

        Return None if no conflicts.

        Intended for dump to 'git push' user's stderr.
        2+ lines for each conflict, showing all the conflicting cases
        and from where they originated.
        """
        texts = [self.conflict_text_one(mono)
                 for mono in sorted(self.mono_to_path_list.keys())]
        text = '\n'.join([t for t in texts if t != ''])
        if not text:
            LOG.debug2("conflict_text() no conflicts found.")
            return ''
        LOG.debug("conflict_text() conflicts found: -{}-".format(text))
        return text


class CaseConflictChecker:

    """Accumulate every Git work tree path seen, either from Git or Perforce.

    Return any case conflicts.
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.branches = defaultdict(functools.partial(BranchCaseConflictChecker, ctx))

    def read_perforce_paths(self):
        """Run 'p4 files' against every branch (including lightweight)
        and accumulate all known Perforce paths.
        """
        for branch in self.ctx.branch_dict().values():
                        # This is called so early during preflight that a new
                        # branch does not yet have depot branch or view lines.
                        # But then it also has no Perforce files, so it is
                        # correct to skip.
            if not branch.view_lines:
                continue

            with self.ctx.switched_to_branch(branch):
                r = self.ctx.p4run('files', self.ctx.client_view_path())
                for rr in r:
                        # Fetch the gwt path then skip if we've already seen it.
                    depot_path = _depot_path(rr)
                    if not depot_path:
                        continue
                    gwt_path = self.ctx.depot_to_gwt_path(depot_path)

                    p = Path.from_p4files(rr, branch, gwt_path)
                    if not p:
                        continue

                    self.branches[branch.branch_id].add_path(p)
                    LOG.debug3("p4: {}".format(p))

    def read_fast_export_commit(self, fe_commit, branch):
        """Record the paths seen in this commit."""
        # process any 'D' commands first to properly handle renames
        for fe_file in fe_commit['files']:
            if fe_file['action'] != 'D':
                continue
            gwt_path = fe_file['path']
            self.branches[branch.branch_id].delete_path(gwt_path)
        # then process any 'M' commands
        for fe_file in fe_commit['files']:
            if fe_file['action'] == 'D':
                continue
            gwt_path = fe_file['path']
            p = Path.from_fe_file(fe_file, fe_commit, branch)
            self.branches[branch.branch_id].add_path(p)

    def conflict_text(self):
        """Return a string suitable for use as a user-visible error message.

        Return None if no conflicts.

        Intended for dump to 'git push' user's stderr.
        2+ lines for each conflict, showing all the conflicting cases
        and from where they originated.
        """
        texts = [b.conflict_text() for b in self.branches.values()]
        return '\n'.join(t for t in texts if t)


class Path:

    """Describe as much as we can about where we saw this path,
    so that we can be precise about where any conflict originates.
    """

    def __init__(self):
        self.branch              = None

                        # Set only if coming from a pushed fe_commit.
        self.commit_sha1         = None

        self.gwt_path    = None
        self.depot_path  = None
        self.from_where  = None

    def __repr__(self):
        """Debugging/programmer string."""
        fmt = '{gwt:<40} {sha1:7} {branch_id:7} {depot_path}'
        return fmt.format( gwt        = self.gwt_path
                         , sha1       = p4gf_util.abbrev(self.commit_sha1)
                         , branch_id  = p4gf_branch.abbrev(self.branch)
                         , depot_path = self.depot_path
                         )

    @staticmethod
    def from_p4files(p4files_dict, branch, gwt_path):
        """From a single 'p4 files' result dict.

        If not a dict, or not a dict with a depotFile, return None.

        +++ Use Context.depot_to_gwt_path() for path translation, which
            assumes that ctx is switched to branch.
        """
        depot_path = _depot_path(p4files_dict)
        if not depot_path:
            return None

        path = Path()
        path.branch     = branch
        path.gwt_path   = gwt_path
        path.depot_path = depot_path
        return path

    @staticmethod
    def from_fe_file(fe_file, fe_commit, branch):
        """From a single 'git fast-export' commit['files'][n] dict."""
        path = Path()
        path.branch      = branch
        path.commit_sha1 = fe_commit['sha1']
        path.gwt_path    = fe_file['path']
        return path


def _depot_path(p4files_dict):
    """Return the 'depotFile' element from a 'p4 files' result list element.

    Return None if this isn't a dict or it's a dict without a depotFile:
    probably a message element mixed in with the dicts.
    """
    try:
        return p4files_dict.get('depotFile')
    except (TypeError, KeyError):
        return None


def monocase(s):
    """Monocase a string."""
    return s.lower()



                        # User-visible formatted string for a single Path
                        # contributing to a conflict.
_FORMAT = NTR("{sha1:7} {gwt:40} {depot_path}")
_HEADER = _FORMAT.format( sha1 = _("commit")
                        , gwt  = _("git path")
                        , depot_path = _("Perforce depot path"))

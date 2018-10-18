#! /usr/bin/env python3.3
"""Check if a Git work tree path is under Git LFS control."""

import p4gf_lfs_checker


class LFSTracker(object):

    """Check if a Git work tree path is under Git LFS control."""

    def __init__(self, *, ctx):
        """Create a LFSChecker to do the checking."""
        self.ctx = ctx
        # delay init of checker until it's needed
        # by then context will have its repo object ready
        self._checker = None

    @property
    def checker(self):
        """Lazy init the checker."""
        if not self._checker:
            self._checker = p4gf_lfs_checker.LFSChecker(self.ctx)
        return self._checker

    def add_cl(self, *, branch, p4change):
        """Find .gitattributes files on branch@p4change and cache any lfs lines."""
        self.checker.add_cl(branch=branch, p4change=p4change)

    def is_tracked_git(self, *, commit_sha1, gwt_path):
        """Check if the given GWT path is under Git LFS control as of the commit.

        Commit is a Git commit from git-fast-export, not yet written to Helix.
        """
        return self.checker.is_tracked_git(commit_sha1=commit_sha1, gwt_path=gwt_path)

    def is_tracked_p4(self, *, branch, p4change, gwt_path):
        """Check if the given GWT path is under Git LFS control as of the P4Change.

        p4change is a Helix P4Change instance not yet written to
        git-fast-import.
        """
        return self.checker.is_tracked_p4(branch=branch, p4change=p4change, gwt_path=gwt_path)

#! /usr/bin/env python3.3
"""A collection of useful directory and file paths."""
import os
from   p4gf_l10n      import _


class RepoDirs:

    """Paths to various directories and files."""

    def __init__(self):
        self.p4gf_dir       = None # P4GF_HOME
        self.repo_container = None # P4GF_HOME/views/<repo>
        self.GIT_WORK_TREE  = None # P4GF_HOME/views/<repo>/git         # pylint: disable=invalid-name
        self.GIT_DIR        = None # P4GF_HOME/views/<repo>/git/.git    # pylint: disable=invalid-name
        self.p4root         = None # P4GF_HOME/views/<repo>/p4
                                   #    (client git-fusion--<serverid>-<repo>'s Root)
        self.lfs            = None # P4GF_HOME/views/<repo>/lfs


def from_p4gf_dir(p4gf_dir, repo_name):
    """Return a dict of calculated paths where a repo's files should go.

    Does not check for existence.
    """
    if not p4gf_dir:
        raise RuntimeError(_('Empty p4gf_dir'))
    if not repo_name:
        raise RuntimeError(_('Empty repo_name'))

    repo_container           = os.path.join(p4gf_dir, "views", repo_name)
    repo_dirs                = RepoDirs()
    repo_dirs.p4gf_dir       = p4gf_dir
    repo_dirs.repo_container = repo_container
    repo_dirs.GIT_WORK_TREE  = os.path.join(repo_container, "git")          # pylint: disable=invalid-name
    repo_dirs.GIT_DIR        = os.path.join(repo_container, "git", ".git")  # pylint: disable=invalid-name
    repo_dirs.p4root         = os.path.join(repo_container, "p4")
    repo_dirs.lfs            = os.path.join(repo_container, "lfs")
    return repo_dirs

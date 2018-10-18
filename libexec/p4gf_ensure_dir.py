#! /usr/bin/env python3.3
"""Directory operations that both p4gf_util and p4gf_log need."""

# These are imported into p4gf_util, you usually want to import that
# instead of p4gf_ensure_dir.

import os


def parent_dir(local_path):
    """Return the path to local_path's immediate parent."""
    return os.path.dirname(local_path)


def ensure_dir(local_dir_path):
    """If dir_path does not already exist, create it."""
    try:
        # Why not test existence first? Because os.path.exists() lies.
        os.makedirs(local_dir_path, exist_ok=True)
    except FileExistsError:
        # If the mode does not match, makedirs() raises an error in
        # versions of Python prior to 3.3.6; since umask might alter
        # the mode, we have no choice but to ignore this error.
        pass


def ensure_parent_dir(local_path):
    """Create local_path's immediate parent directory if it does not already exist."""
    ensure_dir(parent_dir(local_path))

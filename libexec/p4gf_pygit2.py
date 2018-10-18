#! /usr/bin/env python3.3
"""Compatibility wrapper for pygit2.

There are many api changes, especially from 0.18 -> 0.19

To make it easier to keep up with changes, wrap our use of pygit2.
"""

import pygit2

from p4gf_l10n import _

if pygit2.__version__ == '0.22.0':

    def create_blob_fromdisk(repo, path):
        """Create a blob from a file not in the work tree, return sha1."""
        return str(repo.create_blob_fromdisk(path))

    def oid_to_sha1(oid):
        """Sha1 string from oid."""
        return str(oid)

    def ref_to_sha1(ref):
        """Sha1 string from ref."""
        return str(ref.get_object().id)

    def object_to_sha1(obj):
        """Sha1 string from object."""
        return str(obj.id)

    def head_ref(repo):
        """Retrieve the pygit2 Reference for the HEAD of the repo."""
        return repo.head

    def head_commit(repo):
        """Retrieve the pygit2 Commit object for the HEAD of the repo."""
        if repo.is_empty:
            return None
        return repo.head.get_object()

    def head_commit_sha1(repo):
        """Return the sha1 of the Commit object for the HEAD of the repo."""
        return str(repo.head.get_object().id)

    def ref_to_target(ref):
        """Return the pygit2 Oid of the target of the ref.

        Returns name of target for  "symbolic" references.
        """
        return ref.target

    def set_branch(repo, ref, commit):
        """Set the branch ref to point to commit."""
        ref.set_target(repo.get(commit).id)

    def tree_object(repo, tree_entry):
        """Return the tree entry's object."""
        return repo.get(tree_entry.id)

else:
    raise RuntimeError(_("Unsupported pygit2 version {version}")
                       .format(version=pygit2.__version__))

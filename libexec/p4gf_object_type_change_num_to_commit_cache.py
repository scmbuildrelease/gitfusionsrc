#! /usr/bin/env python3.3
"""ChangeNumToCommitCache."""

import random


class ChangeNumToCommitCache:

    """Maintains a limited size cache of (branch+change_num)-to-commit mappings
    for different branches.
    """

    MAX_SIZE = 10000

    def __init__(self):
                        # dict[branch-id] to dict[change_num] to commit sha1
        self._branches = {}
        self._count = 0

    def clear(self):
        """Remove all elements from this cache."""
        self._branches = {}
        self._count = 0

    def _remove_one(self):
        """Remove a randomly selected element."""
        assert self._count == ChangeNumToCommitCache.MAX_SIZE
        self._count -= 1
        i = random.randrange(ChangeNumToCommitCache.MAX_SIZE)
        for j in self._branches.values():
            if len(j) <= i:
                i -= len(j)
                continue
            key = list(j.keys())[i]
            del j[key]

    def append(self, change_num, branch_id, sha1):
        """Add an entry mapping change_num on branch_id to commit sha1."""
        if self._count == ChangeNumToCommitCache.MAX_SIZE:
            self._remove_one()

        if branch_id not in self._branches:
            self._branches[branch_id] = {}
        self._branches[branch_id][change_num] = sha1
        self._count += 1

    def get(self, change_num, branch_id):
        """Return matching (branch_id, commit_sha1) if in cache, else None.

        If branch_id is None, return first matching element, or None.
        """
        if not branch_id:
            return self._get_any_branch(change_num)
        branch_commits = self._branches.get(branch_id)
        if not branch_commits:
            return None
        sha1 = branch_commits.get(change_num)
        if not sha1:
            return None
        return branch_id, sha1

    def _get_any_branch(self, change_num):
        """Return first matching (branch_id,commit_sha1) or None."""
        for branch_id, changes in self._branches.items():
            if change_num in changes:
                return branch_id, changes[change_num]
        return None

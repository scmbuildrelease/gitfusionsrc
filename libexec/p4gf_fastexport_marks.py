#! /usr/bin/env python3.3
"""Mapping of fast-export marks to commit SHA1s for all references.

The Marks class exists to solve one basic problem: with the advent of
background push, the preflight of all pushed references is performed before
any copying is done. As a result, the depot branch info values are
populated with mark numbers, and during the copy phase, a parent change may
have been processed in an earlier branch but the "change" is still listed
as a mark number in the branch info (because the marks are different for
the same commit across branches).

"""

import collections
import logging

LOG = logging.getLogger(__name__)


class Marks:

    """Collection of export marks for all branches."""

    def __init__(self):
        """Construct an instance of Marks."""
        # reference to consider first when searching for commits/marks
        self._preferred_head = None
        # mapping of mark to commit for preferred head
        self._preferred_marks = None
        # mapping of commit to mark for preferred head
        self._preferred_commits = None
        # mapping of ref to mapping of mark to commit
        self._mark_to_commit = collections.OrderedDict()
        # mapping of ref to mapping of commit to mark
        self._commit_to_mark = collections.OrderedDict()

    def set_head(self, ref):
        """Set the preferred branch to search before consulting others.

        :type ref: str
        :param ref: name of branch reference (e.g. 'refs/heads/master').

        """
        LOG.debug2('switching from {} to {}'.format(self._preferred_head, ref))
        self._preferred_head = ref
        self._preferred_marks = self._mark_to_commit.get(ref)
        self._preferred_commits = self._commit_to_mark.get(ref)

    def add(self, ref, marks):
        """Add the given marks associated with the named branch.

        The order in which the marks are added is remembered, so that the
        find functions will scan the refs in the order in which they were
        added. This is useful if the marks are added in the pushed order.

        :type ref: str
        :param ref: name of branch reference (e.g. 'refs/heads/master').

        :type marks: dict
        :param marks: mapping of fast-export marks to commit SHA1s.

        """
        LOG.debug2('adding {} marks for {}'.format(len(marks), ref))
        self._mark_to_commit[ref] = marks
        self._commit_to_mark[ref] = {v: k for k, v in marks.items()}

    def get_commit(self, mark):
        """Retreive the commit associated with the mark on the preferred branch.

        If the preferred branch has not been set, will scan the first added
        branch only.

        :type mark: str
        :param mark: mark value for which to find commit.

        :rtype: str or None
        :return: commit SHA1, or None if not found.

        """
        if self._preferred_marks is None:
            results = self.find_commits(mark)
            return results[0] if results else None
        elif mark in self._preferred_marks:
            sha1 = self._preferred_marks[mark]
            LOG.debug2('found preferred ({2}) commit {0} for mark {1}'.format(
                sha1, mark, self._preferred_head))
            return sha1
        return None

    def find_commits(self, mark):
        """Search for all marks associated with the given SHA1.

        Scans across all known branches in the order in which they were
        added to this colleciton, returning the list of all matching marks.

        :type mark: str
        :param mark: mark value for which to find commit.

        :rtype: list of str
        :return: list of commits.

        """
        results = []
        for ref, marks in self._mark_to_commit.items():
            if mark in marks:
                sha1 = marks[mark]
                LOG.debug2('found commit {0} for mark {1} from ref {2}'.format(sha1, mark, ref))
                results.append(sha1)
        return results

    def get_mark(self, sha1):
        """Retreive the mark associated with the commit on the preferred branch.

        If the preferred branch has not been set, will scan the first added
        branch only.

        :type sha1: str
        :param sha1: commit for which to find mark.

        :rtype: str or None
        :return: mark value, or None if not found.

        """
        if self._preferred_commits is None:
            results = self.find_marks(sha1)
            return results[0] if results else None
        elif sha1 in self._preferred_commits:
            mark = self._preferred_commits[sha1]
            LOG.debug2('found preferred ({2}) mark {1} for commit {0}'.format(
                sha1, mark, self._preferred_head))
            return mark
        return None

    def find_marks(self, sha1):
        """Search for all marks associated with the given SHA1.

        Scans across all known branches in the order in which they were
        added to this colleciton, returning the list of all matching commits.

        :type sha1: str
        :param sha1: commit for which to find mark.

        :rtype: list of str
        :return: list of marks.

        """
        results = []
        for ref, commits in self._commit_to_mark.items():
            if sha1 in commits:
                mark = commits[sha1]
                LOG.debug2('found mark {1} for commit {0} from ref {2}'.format(sha1, mark, ref))
                results.append(mark)
        return results

#! /usr/bin/env python3.3
"""Can the commit we're about to write to git-fast-import reach
the target branch's previous head commit?

An entire O(n) copied commit data structure just to answer the one
question above.

P2GDAGIndex is the main entry point, knows how to find stuff.

P2GDAGNode is an internal node in our own DAG.

Only build up in-memory data for stuff in the git-fast-import script.
No need for stuff already in the Git repo: use
  git merge-base --is-ancestor <ancestor> <child>
to test reachability within existing Git commits.

Uses int/str to differentiate between mark (int) and sha1 (str).

Marks, including parent marks, MUST be integers. No ":1234" strings.
Parent sha1s MUST be 40-char strs.
"""
from   collections import deque
import logging

import p4gf_proc

LOG = logging.getLogger(__name__)

class P2GDAGNode(object):
    """A single in-memory relationship between a git-fast-import mark int(1234)
    and its zero or more parent commits (either pointers to other P2GDAGNode
    instances, or str "sha1" of existing commits).
    """
    def __init__(self, mark, parent_list = None):
        LOG.debug3("node.__init__() m={} pl={}".format(mark, parent_list))
                        # int
        self.mark      = mark

                        # Because the VAST majority of commits have 2 or fewer
                        # parents, avoid list overhead and just directly point
                        # to parents 1 and 2 as pointers or sha1 strs.
                        #
                        # Parents 3+ get dumped into a list.
                        #
                        # Either
                        # * pointer to other P2GDAGNode instance
                        #   if not yet in Git, or
                        # * str(sha1)
                        #   if already in Git
                        #
        self._par1     = None
        self._par2     = None
        self._par3plus = None

        if not parent_list:
            return

        par_ct = len(parent_list)
        if 1 <= par_ct:
            self._par1     = parent_list[0]
        if 2 <= par_ct:
            self._par2     = parent_list[1]
        if 3 <= par_ct:
            self._par3plus = parent_list[2:]

    def __str__(self):
        return self.mark

    def __repr__(self):
        return str(self.mark) + " ".join(_par_str(p) for p in self.parents())

    def parents(self):
        """Iterator/generator for our parents, to mask the internal
        complexity of our par1/par2/par3+ optimization.
        """
        for p in [self._par1, self._par2]:
            if p is None:
                return
            else:
                yield p
        if self._par3plus:
            yield from self._par3plus


# -- end class P2GDAGNode ----------------------------------------------------

class P2GDAGIndex(object):
    """Is some desired ancestor sha1/mark actually a reachable from some
    child sha1/mark?

    Internally implemented as a fast-lookup dict of sha1/mark-to-P2GDAGNode,
    and the algorithms to check reachability between nodes.
    """

    def __init__(self, ctx):
        self.ctx = ctx

        self.mark_to_node = {}

    def to_node(self, child_mark, parent_sha1mark_list):
        """Return a new P2GDAGNode with correct parent pointers.

        Result is NOT yet add()ed to our index. Because the whole point of this
        code is to detect when the proposed child commit's ancestry is messed
        up and needs a bit more work before  recording to git-fast-import.
        """
        LOG.debug3("to_node() m={} pl={}".format(child_mark, parent_sha1mark_list))

                        # Enforce "mark is int, sha1 is str" invariant.
        assert isinstance(child_mark, int)

                        # Expand marks into their actual P2GDAGNode pointers.
        plist = []
        if parent_sha1mark_list:
            for p in parent_sha1mark_list:
                if is_mark(p):
                    pp = self._get(p)
                else:
                    assert isinstance(p, str)
                    assert 40 == len(p)
                    pp = p
                plist.append(pp)

        return P2GDAGNode(child_mark, plist)

    def add(self, node):
        """Record a child=>parent(s) relationship.

        NOP if already recorded. Because that lets me not track that
        in outer code where I'm just assigning an already-seen commit
        for a second-or-later branch assignment.
        """
        LOG.debug3("add() child:{} par:{}"
                   .format(node.mark, list(node.parents())))
        if node.mark not in self.mark_to_node:
            self.mark_to_node[node.mark] = node

    def _get(self, mark):
        """Find the P2GDAGNode with a given mark. Must have been
        previously add()ed.
        """
        node = self.mark_to_node.get(mark)
        if not node:
            raise RuntimeError("BUG: mark {} not yet add()ed.".format(mark))
        return node

    @staticmethod
    def is_ancestor(parent_sha1mark, child_node):
        """Return true if parent sha1/mark is reachable as an ancestor
        of proposed child.
        """
        if is_mark(parent_sha1mark):
            return _is_ancestor_mark(parent_sha1mark, child_node)
        else:
            return _is_ancestor_sha1(parent_sha1mark, child_node)

# -- end class P2GDAGIndex ---------------------------------------------------

def _is_ancestor_mark(parent_mark, child_node):
    """Return true if not-yet-copied-to-Git parent_mark is reachable
    as an ancestor of proposed child.

    Child must have been previously add()ed.
    """
    work_node_queue = deque(child_node.parents())
    seen_set        = set()
    while work_node_queue:
        p = work_node_queue.popleft()
        if not isinstance(p, P2GDAGNode):  # sha1 cannot be a mark
            continue
        if p.mark == parent_mark:       # found it
            return True
        if p.mark in seen_set:          # +++ skip ancestors we've seen
            continue                    # previously via some other path.

                    # "recurse" up the ancestry.
        work_node_queue.append(p.parents())
        seen_set.add(p.mark)
    return False


def _is_ancestor_sha1(parent_sha1, child_node):
    """Return true if already-copied-to-Git parent_sha1 is reachable
    as an ancestor of proposed child.
    """
    work_node_queue = deque(child_node.parents())
    seen_set        = set()
    while work_node_queue:
        p = work_node_queue.popleft()

        if isinstance(p, P2GDAGNode):
                    # Still in P2GDAGNode hierarchy of not-yet-copied commits.
                    # DAGwalk until we hit bedrock of already-copied-to-git
                    # sha1.

                    # +++ Skip ancestors we've seen previously via
                    #     some other path.
            if p.mark in seen_set:
                continue

                    # "recurse" up the ancestry.
            work_node_queue.append(p.parents())
            seen_set.add(p.mark)
            continue

        # not a P2GDAGNode? ==> str sha1
                    # Found the dividing line between not-yet-copied
                    # P2GDAGNode mark commits, and sha1 commits. Can we
                    # reach the requested parent_sha1 from this point on
                    # the dividing line?

                    # +++ Common case is the old branch head sits right on
                    #     the dividing line. Avoid the cost of
                    #     out-of-process 'git merge-base' here.
        if p == parent_sha1:
            return True

                    # +++ Skip ancestors we've seen previously via
                    #     some other path.
        elif p in seen_set:
            continue

        elif _git_is_ancestor(parent_sha1, p):
                    # Child can reach p, p can reach parent_sha1, so by the
                    # transitive property of reachability, child can reach
                    # parent_sha1.
            return True

                    # Not yet found, keep DAGwalking the ancestry.
        seen_set.add(p)
        continue

                    # Ran out of ancestry and never reached parent_sha1.
    return False


def _git_is_ancestor(parent_sha1, child_sha1):
    """Ask Git if one already-copied-to-Git commit is an ancestor of
    another.
    """
    # Make sure these arguments are strings, or p4gf_proc will have problems (GF-2800).
    cmd = ['git', 'merge-base', '--is-ancestor', str(parent_sha1), str(child_sha1)]
    p = p4gf_proc.popen_no_throw(cmd)
    return p['ec'] == 0


def is_mark(sha1mark):
    """Is this a ':1234' mark?"""
    return isinstance(sha1mark, int)


def _par_str(sha1_or_dagnode):
    """P2GDAGNode.__repr__() helper to convert one of P2GDAGNode's
    parent list elements to something printable as a string.

    Pointers to another node converted to that node's mark.
    sha1 strings returned unchanged.
    """
    if isinstance(sha1_or_dagnode, P2GDAGNode):
        return str(sha1_or_dagnode.mark)
    else:
        return str(sha1_or_dagnode)

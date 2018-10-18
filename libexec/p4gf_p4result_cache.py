#! /usr/bin/env python3.3
"""P4ResultCache."""

from collections import deque, namedtuple
import logging

import p4gf_util

LOG = logging.getLogger(__name__)

_MAX = 10


class P4ResultCache:

    """Generic cache of 'p4 xxx' results for some branch at some changelist.

    G2PMatrix runs enough duplicate 'p4 fstat //branch-client/...@nn' in close
    temporal proximity that it could benefit from a small bounded cache.
    """

    def __init__(self, cmd):
        self.cache = deque(maxlen=_MAX)    # of CacheLine
        self._hit_ct  = 0
        self._miss_ct = 0
        self._cmd     = cmd

    def get(self, ctx, branch, change_num):
        """Fetch files in branch at change and return result list."""
        result_list = self._find(branch, change_num)
        if not result_list:
            self._miss_ct += 1
            LOG.debug2('{cmd} {branch}@{change} miss {ct}'
                       .format( branch  = p4gf_util.abbrev(branch.branch_id)
                              , change  = change_num
                              , ct      = self._miss_ct
                              , cmd     = self._cmd ))
            result_list = self._fetch(ctx, branch, change_num)
            self._insert(branch, change_num, result_list)
        else:
            self._hit_ct += 1
            LOG.debug2('{cmd} {branch}@{change} hit  {ct}'
                       .format( branch  = p4gf_util.abbrev(branch.branch_id)
                              , change  = change_num
                              , ct      = self._hit_ct
                              , cmd     = self._cmd ))
        return result_list

    def _fetch(self, ctx, branch, change_num):
        """Run 'p4 files' and return results."""
        with ctx.switched_to_branch(branch):
            return ctx.p4run(self._cmd + [ctx.client_view_path(change_num)])

    def _find(self, branch, change_num):
        """Find a CacheLine with matching path and return its result_list.

        Or return None if not found.
        """
        # Never cache results for temp branch views that lack a permanent
        # branch_id: a branch_id of None is used for multiple branch views.
        if not branch.branch_id:
            return None

        for cl in self.cache:
            if (    cl.branch           == branch.branch_id
                and cl.change_num       == change_num):
                return cl.result_list
        return None

    def _insert(self, branch, change_num, result_list):
        """Add a CacheLine for path + result_list.

        Assumes we don't already have such a line.
        """
        self.cache.appendleft(CacheLine( branch      = branch.branch_id
                                       , change_num  = change_num
                                       , result_list = result_list ))

CacheLine = namedtuple('CacheLine', ['branch', 'change_num', 'result_list'])

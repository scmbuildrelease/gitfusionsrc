#! /usr/bin/env python3.3
"""TreeCache."""

import binascii
import logging
import random
import bisect

import p4gf_object_type_util as util
import p4gf_path

LOG = logging.getLogger(__name__)


class TreeCache:

    """Keeps track of which tree objects exist in perforce."""

    MAX_SIZE = 10000

    def __init__(self):
        self._tree_cache = [None]*TreeCache.MAX_SIZE
        self._tree_cache_size = 0

    def clear(self):
        """Remove all elements from this cache."""
        self._tree_cache = [None]*TreeCache.MAX_SIZE
        self._tree_cache_size = 0

    def _tree_cache_insert(self, j, bsha1):
        """Insert an entry in the cache of tree sha1 values.

        If the cache is already full, remove one entry at random first.
        """
        # NOP if already in list
        if j < self._tree_cache_size and self._tree_cache[j] == bsha1:
            return

        # just insert if not at capacity
        if self._tree_cache_size < TreeCache.MAX_SIZE:
            self._tree_cache = self._tree_cache[:j] \
                             + [bsha1] \
                             + self._tree_cache[j:TreeCache.MAX_SIZE-1]
            self._tree_cache_size += 1
            return

        # remove a random entry and insert new entry
        i = random.randrange(TreeCache.MAX_SIZE)
        if i == j:
            self._tree_cache[i] = bsha1
        elif i < j:
            self._tree_cache = self._tree_cache[:i] \
                             + self._tree_cache[i+1:j] \
                             + [bsha1] + self._tree_cache[j:]
        else:
            self._tree_cache = self._tree_cache[:j] \
                             + [bsha1] \
                             + self._tree_cache[j:i] \
                             + self._tree_cache[i+1:]

    def tree_exists(self, p4, sha1):
        """Return true if sha1 identifies a tree object in
        the //P4GF_DEPOT/objects/... hierarchy.
        """
        # convert to binary rep for space savings
        bsha1 = binascii.a2b_hex(sha1)
        # test if already in cache
        j = bisect.bisect_left(self._tree_cache, bsha1, hi=self._tree_cache_size)
        if j < self._tree_cache_size and self._tree_cache[j] == bsha1:
            LOG.debug2('tree cache hit for {}'.format(sha1))
            return True
        # not in cache, check server
        LOG.debug2("tree cache miss for {}".format(sha1))
        LOG.debug("fetching tree objects for {}".format(sha1))
        path = p4gf_path.tree_p4_path(sha1)
        r = [f for f in util.run_p4files(p4, path)]
        if len(r) != 1:
            return False
        m = util.OBJPATH_TREE_REGEX.search(r[0])
        if not m:
            return False
        found_sha1 = m.group('slashed_sha1').replace('/', '')
        if sha1 != found_sha1:
            return False
        self._tree_cache_insert(j, bsha1)
        return True

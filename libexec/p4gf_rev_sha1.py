#! /usr/bin/env python3.3
"""Stores blob checksums for all file revisions in Perforce keys."""

import binascii
import logging
import zlib

from   p4gf_const      import P4GF_P4KEY_REV_SHA1
import p4gf_p4key      as     P4Key
from   p4gf_l10n       import _

NOT_CACHED    = "NOT_CACHED"
P4KEY_NOT_SET = "P4KEY_NOT_SET"

LOG = logging.getLogger(__name__)

# Why one p4key per changelist?
#
# O(n) changelists : We already have per-changelist p4keys for ObjectType
#       indexes. So sticking with O(n) changelists doesn't change our big O.
# O(n * m) changelist * file revisions would be expensive for repos
#       with m=1,000+ files.
# O(1) per repo was a thought, but that p4key value would become _really_
#      O(n * m) long, writing new O(n * m) values after each pull or push.
#      We'd kill the keys table and journal file.


class RevSha1:

    """File revision ==> Git blob sha1 lookup.

    Creates a p4key, one per <repo, changelist>, which
    contains a list of file revisions / sha1 associations:

        git-fusion-rev-blob-{repo}-{change_num}

        0e4f6bdb053c18e001562f6068088bffbe11b9c9 [tab] //depot/f#2
        63ac313b51fca96ffb7c08b9d77b6c99c67e30d1 [tab] //depot/g#7
        c5fa40f94be620a8318a0ecb1417fc5266ee59db [tab] //depot/h#1

    Written during 'git pull' after we're done printing all revisions for the pull.
    Written during 'git push' SOMETIME I DON'T KNOW WHEN.

    Read during 'git pull' when we need to refer to a file revision
    that's already in Git from some previous push/pull, but which needs to be
    included in a file list for git-fast-import.

    Internally caches everything it reads.
    Does not cache writes because so far no calling code needs that.
    """

    def __init__(self, ctx):
        self.ctx = ctx

                        # Nested dict of dicts:
                        #
                        # str(change_num) => dict
                        #                    depot_path#rev => str(sha1)
        self._cache = {}

    def record(self, change_num, p4file_list):
        """Store in Perforce the sha1 for each depot_path#rev.

        p4file_list elements must be P4File instances with their sha1 already
        filled in. p4gf_p2g_print_handler.py already does this for you.

        p4file_list elements must all be from the same changelist. It is a
        programming error to mix revisions from multiple changelists in a single
        call to record(). If grafting, call record() once for each contributing
        pre-graft change.
        """
        LOG.debug("record() change={change_num} rev ct={ct}"
                  .format( change_num = change_num
                         , ct         = len(p4file_list) ))
        if LOG.isEnabledFor(logging.DEBUG3):
            for f in p4file_list:
                LOG.debug3("{:7.7} {}".format(f.sha1, f.rev_path()))

                        # Unlikely, but just in case: fetch previous value
                        # and add to it.
        depot_path_rev_to_sha1 = self._read_p4key(change_num)
        if depot_path_rev_to_sha1 == P4KEY_NOT_SET:
            depot_path_rev_to_sha1 = {}
        for f in p4file_list:
            depot_path_rev_to_sha1[f.rev_path()] = f.sha1
        self._write_p4key(change_num, depot_path_rev_to_sha1)

    def lookup(self, change_num, depot_path, rev):
        """Find this file revision's blob sha1.

        Never returns None. Raises exception if cannot find.
        """
        LOG.debug3("lookup() requesting {sha1:7.7} @{change_num:<5} {depot_path}#{rev}"
                   .format( change_num = change_num
                          , depot_path = depot_path
                          , rev        = rev
                          , sha1       = '' ))
        r = self._get_cached(change_num, depot_path, rev)
        if r == NOT_CACHED:
            self._load_cache(change_num)
            r = self._get_cached(change_num, depot_path, rev)
        LOG.debug3("lookup() returning  {sha1:7.7} @{change_num:<5} {depot_path}#{rev}"
                   .format( change_num = change_num
                          , depot_path = depot_path
                          , rev        = rev
                          , sha1       = r ))
        return r

    def p4key_name(self, change_num):
        """One p4key per <repo, changelist>."""
        return P4GF_P4KEY_REV_SHA1.format(
                  repo_name  = self.ctx.config.repo_name
                , change_num = change_num )

    def _get_cached(self, change_num, depot_path, rev):
        """Return cached sha1 for depot_path#rev at changelist.

        Return NOT_CACHED if changelist not yet cached.

        Never returns None: raises exception if changelist cached but
        depot_path#rev not listed in cached value.

        """
        cache = self._cache.get(str(change_num))
        if cache is None:
            return NOT_CACHED
        dfrev = "{}#{}".format(depot_path, rev)
        sha1 = cache.get(dfrev)
        if sha1 is None:
            raise RevSha1.NotFoundError(_(
                "Rev-to-sha1 lookup failed:"
                " p4key {p4key_name} lacks line for {depot_path}#{rev}.")
                .format( depot_path   = depot_path
                       , rev          = rev
                       , p4key_name = self.p4key_name(change_num)))
        return sha1

    def _load_cache(self, change_num):
        """Read a p4key value, parse it into a useful dict, store that dict in our cache."""
        val_dict = self._read_p4key(change_num)
        if val_dict == P4KEY_NOT_SET:
            raise RevSha1.NotFoundError(_(
                "Rev-to-sha1 lookup failed:"
                " p4key {p4key_name} not defined.")
                .format(p4key_name=self.p4key_name(change_num)))
        self._cache[str(change_num)] = val_dict

    def _read_p4key(self, change_num):
        """Read a p4key value previously record()ed.

        Parse into a dict depot_path#rev ==> sha1

        Returned dict can be empty: Perforce changelists can contain zero
        file actions after 'p4 obliterate'.

        Return P4KEY_NOT_SET if p4key not set.

        Never returns None.
        """
        val = P4Key.get(self.ctx, self.p4key_name(change_num))
        if (not val) or (val == '0'):
            return P4KEY_NOT_SET
        try:
            # Assume compressed format, catch error if not
            val = zlib.decompress(binascii.a2b_base64(val)).decode()
        except (binascii.Error, zlib.error, ValueError):
            # Uncompressed string format
            pass
        return _parse_p4key_val(val)

    def _write_p4key(self, change_num, val_dict):
        """Convert a dict of depot_path#rev ==> sha1 to a single string,
        write that to this changelist's p4key.
        """
        val = _format_p4key_val(val_dict)
        if len(val) > 1024:
            # For sufficiently large values, compress and base64 encode to
            # save space (about 40% at 1kb, and generally increasing with
            # input size).
            val = binascii.b2a_base64(zlib.compress(val.encode()))
        P4Key.set(self.ctx, self.p4key_name(change_num), val)

    class NotFoundError (RuntimeError):

        """Why lookup() failed to return a sha1."""

        def __init__(self, why):
            RuntimeError.__init__(self, why)
            LOG.debug(why)


# -- end of class RevSha1 -----------------------------------------------------

def _parse_p4key_val(val):
    """Convert a p4key value string to a dict that we use."""
    result = {}
    for line in val.splitlines():
        w = line.split('\t')
        result[w[1]] = w[0]
    return result


def _format_p4key_val(val_dict):
    """Convert a list of P4File instances to a single p4key value string."""
    l = ["{sha1}\t{depot_path_rev}".format(sha1=v, depot_path_rev=k)
         for k, v in val_dict.items()]
    return '\n'.join(l)

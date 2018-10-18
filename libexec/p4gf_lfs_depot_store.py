#! /usr/bin/env python3.3
"""Permanent storage of large file content."""
import p4gf_util

class LFSDepotStore(object):
    """Permanent storage of large file content.

    Cannot store content with this class: to do that, run your own
    `p4 copy`/`p4 add` + `p4 submit`.

    """
    def __init__(self, *, ctx):
        self.ctx = ctx

    def p4print(self, lfsfs):
        """Return a byte array that contains the large file content
        addressed by the LFSFileSpec.

        It is a programming error to call this on a non-existent or
        deleted-at-head depot file.
        """
        return p4gf_util.print_depot_path_raw(
                    self.ctx.p4gf, lfsfs.depot_path(self.ctx))

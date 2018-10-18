#! /usr/bin/env python3.3
"""Git LFS file address"""
import hashlib
import os
import re
import p4gf_const
import p4gf_git
from   p4gf_l10n import _
import p4gf_util

LFS_CACHE_PATH = "{repo_lfs}/sha256/{sha256}"
LFS_DEPOT_PATH = "//{P4GF_DEPOT}/objects/repos/{repo}/lfs/sha256/{sha256}"

_TEXT_POINTER_MAX_BYTE_COUNT = 3*1000


def lfs_depot_path(ctx, sha256):
    """Return path to large file within depot."""
    return LFS_DEPOT_PATH.format(P4GF_DEPOT=p4gf_const.P4GF_DEPOT,
                                 repo=ctx.config.repo_name,
                                 sha256=split_sha256(sha256))


class LFSFileSpec(object):
    """How to find/identify large file content within Git LFS."""
    # Keep this address class small and focused.
    # Avoid:
    # - GWT location or how that maps to //depot/...
    # - large file I/O
    # - sha1 of small text pointer

    def __init__(self, *
                , oid
                , large_size=None
                ):
                        # A Git LFS object identifier sha256, as a str.
                        # No "sha256" prefix. Just assume it's a sha256 within
                        # code until there's a reason not to.
        self.oid = oid

                        # Integer byte count of large file content.
                        #
                        # Filled in only when known. None if not.
        self.large_size = large_size

    def __hash__(self):
        return self.oid.__hash__() + self.large_size.__hash__()

    def __eq__(self, b):
        return self.oid == b.oid and self.large_size == b.large_size

    def __str__(self):
        return 'size: {} oid: {}'.format(self.oid, self.large_size)

    @staticmethod
    def from_text_pointer(text_pointer_content):
        """Parse text pointer content.

        version https://git-lfs.github.com/spec/v1
        oid sha256:731519fd44dfc217610c9ae47fa32d43bfe8ccf41677b40208f7e3ecafe05a3f
        size 85

        """
        oid = None
        large_size = None
        for line in text_pointer_content.splitlines():
            m = re.search(r'oid sha256:([0-9a-f]{64})', line)
            if m:
                oid = m.group(1)
                continue
            m = re.search(r'size ([0-9]+)', line)
            if m:
                large_size = int(m.group(1))
                continue
        if oid is None:
            raise RuntimeError(_("Not a Git LFS text pointer. No oid found."))
        return LFSFileSpec(oid = oid, large_size = large_size)

    def to_text_pointer(self):
        """Format text pointer content."""
        if self.large_size is None:
            # Call sequence error. Set large_size before formatting.
            raise RuntimeError(_("Missing large_size."))
        return \
"""version https://git-lfs.github.com/spec/v1
oid sha256:{sha256}
size {large_size}
""".format(sha256 = self.oid, large_size = self.large_size)

    @staticmethod
    def from_blob(ctx, blob_sha1):
        """If the blob content is formatted like a text pointer,
        parse the content and return a new LFSFileSpec.
        If not, return None.
        """
                        # Check blob size before extracting from Git.
                        # Waste no time extracting anything too large to
                        # be a "text pointer".
        blob = ctx.repo.get(blob_sha1)
        if not blob:
            return None
        if _TEXT_POINTER_MAX_BYTE_COUNT < blob.size:
            return None

        blob_bytes = p4gf_git.get_blob(blob_sha1, ctx.repo)
        try:
            as_text = blob_bytes.decode("utf-8")
            return LFSFileSpec.from_text_pointer(as_text)
        except (UnicodeDecodeError, RuntimeError):
            pass
        return None

    @staticmethod
    def for_blob(ctx, blob_sha1):
        """Create a text pointer for a large file blob."""
        blob_bytes = p4gf_git.get_blob(blob_sha1, ctx.repo)
        m = hashlib.sha256()
        m.update(blob_bytes)
        sha256 = m.hexdigest()
        return LFSFileSpec(oid=sha256, large_size=len(blob_bytes))

    def cache_path(self, ctx):
        """Path to large file within local file cache."""
        return LFS_CACHE_PATH \
               .format( repo_lfs = ctx.repo_dirs.lfs
                      , sha256   = split_sha256(self.oid) )

    def depot_path(self, ctx):
        """Path to large file within depot."""
        return LFS_DEPOT_PATH \
               .format( P4GF_DEPOT = p4gf_const.P4GF_DEPOT
                      , repo       = ctx.config.repo_name
                      , sha256     = split_sha256(self.oid) )

    def exists_in_cache(self, ctx):
        """Is this file already sitting in our upload cache?"""
        return os.path.exists(self.cache_path(ctx))

    def exists_in_depot(self, ctx):
        """Is this file already submitted to Perforce?"""
        r = ctx.p4run('files', '-e', self.depot_path(ctx))
        hit = p4gf_util.first_value_for_key(r, "depotFile")
        return hit

    def to_dict(self):
        """For JSON output."""
        return { "oid"  : self.oid
               , "size" : self.large_size
               }

    @staticmethod
    def from_dict(d):
        """For JSON input."""
        return LFSFileSpec( oid        = d["oid"]
                          , large_size = d["size"]
                          )

# Could promote to p4gf_util.

def split_sha256(sha256):
    """Insert "/" chars to return a sha256 string with a few
    directory levels.
    """
    return ( sha256[0:2] + "/"
           + sha256[2:4] + "/"
           + sha256[4:6] + "/"
           + sha256[6:8] + "/"
           + sha256[8:] )


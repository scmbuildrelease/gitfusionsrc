#! /usr/bin/env python3.3
"""A cheesy database table of LFS files."""
from p4gf_lfs_file_spec import LFSFileSpec
import p4gf_util
                        # Some day we'll support Python 3.4 and get its shiny
                        # new Enum support. Until then, here have a fake
                        # replacement.
try:
    from enum import Enum
except ImportError:
    class Enum:
        """Gee golly I wish we had Python 3.4 Enum."""
        def __init__(self):
            pass


class LFSLargeFileSource(Enum):
    """From where did we get this large file content?
    Intended as an optimization so that we waste no time
    trying to 'p4 add' files that already came from Perforce.
    """
    UPLOAD_CACHE = "upload"
    DEDUPE       = "dedupe"

# YAGNI until pull time
#
# class LFSTextPointerSource(Enum):
#     """From where did we get this text pointer content?
#     Intended as an optimization so that we waste no time
#     trying to 'p4 add' files that already came from Perforce
#     or git-hash-object-ing tiny blobs already in Git.
#     """
#     GENERATED = "gen"       # From a template string.
#     GITMIRROR = "gitmirror" # Re-cloned from //.git-fusion/objects/blobs/...
#     GITREPO   = "repo"      # Already in repo, no need to re-hash.


class LFSRow(object):
    """Everything about a single Git LFS file that we might need to know.

    Not all fields are filled in for each direction.
    Some files start None, get filled in later.
    """
    def __init__(self):
                                        # `git-fast-export`ed during push.
        self.commit_sha1            = None
        self.gwt_path               = None
        self.text_pointer_sha1      = None  # string sha1

        self.large_file_oid         = None  # string sha256
        self.large_file_byte_count  = 0
        self.large_file_source      = None  # LFSLargeFileSource

        # YAGNI until pull time
        #                                 # `p4 print`ed during pull
        # self.depot_path             = None
        # self.rev_num                = 0
        # self.large_file_sha1        = None  # string sha1
        # self.text_pointer_source    = None  # LFSTextPointerSource

    def to_dict(self):
        """For JSON formatting."""
        return { "commit_sha1"           : self.commit_sha1
               , "gwt_path"              : self.gwt_path
               , "text_pointer_sha1"     : self.text_pointer_sha1
               , "large_file_oid"        : self.large_file_oid
               , "large_file_byte_count" : self.large_file_byte_count
               , "large_file_source"     : self.large_file_source
               }

    @staticmethod
    def from_dict(d):
        """For JSON formatting."""
        row = LFSRow()
        row.commit_sha1           = d["commit_sha1"]
        row.gwt_path              = d["gwt_path"]
        row.text_pointer_sha1     = d["text_pointer_sha1"]
        row.large_file_oid        = d["large_file_oid"]
        row.large_file_byte_count = d["large_file_byte_count"]
        row.large_file_source     = d["large_file_source"]
        return row

    @staticmethod
    def from_gfe(ctx, gfe_commit, gfe_file):
        """Start a LFSRow with everything we can glean from its
        git-fast-export entry, including a parse of its text pointer blob.

        Checks local filesystem and Perforce to query for large file
        content existence/source.
        """
        lfsfs = LFSFileSpec.from_blob(ctx, blob_sha1 = gfe_file["sha1"])
        if not lfsfs:
            return None

        row = LFSRow()
        row.commit_sha1           = gfe_commit["sha1"]
        row.gwt_path              = gfe_file["path"]
        row.text_pointer_sha1     = gfe_file["sha1"]
        row.large_file_oid        = lfsfs.oid
        row.large_file_byte_count = lfsfs.large_size

        if lfsfs.exists_in_cache(ctx):
            row.large_file_source = LFSLargeFileSource.UPLOAD_CACHE
        elif lfsfs.exists_in_depot(ctx):
            row.large_file_source = LFSLargeFileSource.DEDUPE
        else:
            row.large_file_source = None  # Does not exist, your push will fail.

        return row

    def to_lfsfs(self):
        """Convert."""
        return LFSFileSpec( oid        = self.large_file_oid
                          , large_size = self.large_file_byte_count )

    def __repr__(self):
        _a = p4gf_util.abbrev   # for less typing
        return ("commit:{commit_sha1}"
                " src:{large_file_source}"
                " ptr_sha1:{text_pointer_sha1}"
                " oid:{large_file_oid}"
                " cb:{large_file_byte_count:<4d}"
                " {gwt_path}"
                ).format( commit_sha1           = _a(self.commit_sha1)
                        , gwt_path              =    self.gwt_path
                        , text_pointer_sha1     = _a(self.text_pointer_sha1)
                        , large_file_oid        = _a(self.large_file_oid)
                        , large_file_byte_count =    self.large_file_byte_count
                        , large_file_source     =    self.large_file_source
                        )


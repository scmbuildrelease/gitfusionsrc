#! /usr/bin/env python3.3
"""Code to provide the Perforce Server's Librarian with all
it needs to store file revisions.
"""
import logging
import os

import p4gf_config
from   p4gf_hex_str         import md5_int
from   p4gf_l10n            import _
import p4gf_squee_value

LOG = logging.getLogger("p4gf_fast_push.librarian")

class LibrarianStore:
    """BigStore of sha1->LibrarianRecord.
    """
    def __init__(self, file_path, store_how):
                        # pylint:disable=invalid-name
        if store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_DICT:
            self._d = {}
        elif store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MEMORY:
            self._d = p4gf_squee_value.SqueeValueDict(file_path)
        elif store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_SINGLE_TABLE:
            self._d = p4gf_squee_value.SqueeValue(file_path)
        else:
            raise RuntimeError(_("Unsupported store_how {store_how}")
                               .format(store_how=store_how))

    def get(self, sha1):
        """Return any record for the given sha1, or None."""
        return self._d.get(sha1)

    def store( self, *
             , sha1
             , md5
             , byte_ct
             , lbr_file_type
             ):
        """Store one librarian record for the given sha1.
        Return the record for the newly stored entry.

        Does not check for previous existence.
        It is the caller's responsibility to do so.

        Zig slightly prefers an explicit setter function like this
        to a lower-level setter where callers are responsible for
        choosing the element type. Gives us a chance to enforce
        an invariant: all elements are LibrarianRecords.

        Keep records tiny: intentionally ignore lbr_path here. Outer code has
        the type (blob/tree) and sha1. Let it programmatically generate
        lbr_path from that.
        """
                        # I know the comment header says we don't check,
                        # and once I switch to BigStore I won't. But for
                        # now, let's catch any bugs.
        assert sha1 not in self._d
        lbr_record =  LibrarianRecord(
                              md5           = md5
                            , byte_ct       = byte_ct
                            , lbr_file_type = lbr_file_type)
        self._d[sha1] = lbr_record
        return lbr_record

# ----------------------------------------------------------------------------

class LibrarianRecord:
    """What the Perforce Server's Librarian requires for any single
    file revision.

    Keep this struct tiny. We retain millions of these in a BigStore.

    Intentionally omits the librarian file path. Path strings
    are long, expensive to store, and rarely necessary. Usually
    the path is generated from the same sha1 used to look up this
    record, or used only once then discarded.

    Intentionally do NOT store lbr_path here. lbr_path is programmatically
    generated from tree/blob type and sha1.
    """
    def __init__( self, *
                , md5
                , byte_ct
                , lbr_file_type
                ):
        self.md5           = md5_int(md5)
        self.byte_ct       = int(byte_ct)
        self.lbr_file_type = int(lbr_file_type)

# -- module-wide -------------------------------------------------------------

def lbr_rev_str(change_num = 1):
    """1234 ==> "1.1234"

    lbrRev fields all seem to be "1.nnn" where "nnn" was the change_num that
    submitted them.
    """
    return "1.{}".format(change_num)


def depot_to_archive_path(depot_path, change_num = 1):
    """
    Return the path within the zip archive where a librarian file goes.

    Just appends "/1.1234" to the lbr_path.

    blobs and other files where we force the librarian rev with
    `p4 unzip --retain-lbr-revisions` should always use change_num = 1.
    """
    return os.path.join(depot_path[2:], lbr_rev_str(change_num))



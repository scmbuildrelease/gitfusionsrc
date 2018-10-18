#! /usr/bin/env python3.3
"""Code to recall every revision of every file we've created."""
import bisect
import logging

import p4gf_config
from   p4gf_l10n                    import _
import p4gf_squee_value

LOG = logging.getLogger("p4gf_fast_push.rev_history")

class RevHistoryStore:
    """BigStore of depot_path -> RevHistory.
    """
    def __init__(self, ctx, file_path, store_how):
                        # use a dict until we have a BigStore.
                        # pylint:disable=invalid-name
        self.file_path = file_path
        if store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_DICT:
            self._d = {}
        elif store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MEMORY:
            self._d = p4gf_squee_value.SqueeValueDict(file_path)
        elif store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_SINGLE_TABLE:
            self._d = p4gf_squee_value.SqueeValue(file_path)
        elif store_how == p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MULTIPLE_TABLES:
            self._d = p4gf_squee_value.SqueeValueMultiTable(file_path)
        else:
            raise RuntimeError(_("Unsupported store_how {store_how}")
                               .format(store_how=store_how))

        self._is_contextdata_written = set()
        self.ctx = ctx

    def head_db_rev(self, depot_path):
        """Return the DbRev record most recently record_head()ed for this
        depot_path.
        """
        depot_path_key = self.ctx.path_to_key(depot_path)
        rh = self._d.get(depot_path_key)
        if rh:
            return rh.head_db_rev()
        return None

    def record_head(self, db_rev, is_delete):
        """Record a new head revision."""
        depot_path_key = self.ctx.path_to_key(db_rev.depot_path)
        rh = self._d.get(depot_path_key)
        if not rh:
            rh = RevHistory()
        rh.record_head(db_rev, is_delete)
        self._d[depot_path_key] = rh
        self._is_contextdata_written.add(depot_path_key)

    def next_rev_num(self, depot_path):
        """Return the integer rev number to use for the next
        file action on depot_path.
        """
        depot_path_key = self.ctx.path_to_key(depot_path)
        rh = self._d.get(depot_path_key)
        if rh:
            return rh.next_rev_num()
        return 1

    def is_contextdata_written(self, depot_path):
        """Have we already written this depot path to the current
        commit_gfunzip's contextdata.jnl?
        """
        depot_path_key = self.ctx.path_to_key(depot_path)
        return depot_path_key in self._is_contextdata_written

    def clear_all_contextdata_written(self):
        """Clear is_contextdata_written() for all depot paths.

        Call this when you open a new commit_gfunzip.
        """
        self._is_contextdata_written = set()

    def exists_at_head(self, depot_path):
        """Does this depot file current exist, undeleted, at head?

        Used when deciding whether to treat git-fast-export 'M' as either
        'p4 add' or 'p4 delete'
        """
        depot_path_key = self.ctx.path_to_key(depot_path)
        rh = self._d.get(depot_path_key)
        if not rh:
            return False
        return not rh.is_deleted_at_head()

    def src_range(self, depot_path, change_num):
        """Return a (#startRev,endRev) integer pair to use as an integ
        source for 'p4 integ -f src@change_num.

        Return (None, None) if no revision at that change_num.
        """
        if depot_path not in self._d:
            return (None, None)
        rh = self._d.get(depot_path)
        if not rh:
            rh = RevHistory()
        return rh.src_range(change_num)

# ----------------------------------------------------------------------------

class RevHistory:
    """Every revision of a single depot file.

    Keep this struct tiny. We retain millions of these in a BigStore.

    Do NOT use gfmarks here. Integer changelist numbers only.

    Internal compact storage: a single list of integers,
    with element [0] special
    [1:] = list of integer changelist numbers, one per revision.
           index into self._l is rev#1, value at that location is change_num.
    [0]  = None for most files
           list of integer revision numbers that "add" this file after a delete
           for those few files deleted then re-added.

    Note for future second-and-later push:
        Assumes changelists monotonically increase. For the possible future
    where we use this code for pushes into existing Perforce history, choose
    your starting gfmark to be greater than the current 'p4 counter change'
    value so that when you fill us with existing file history, those
    changelists sort numerically before anything you append later.
    """
    def __init__(self):
        self._l = [None]     # pylint:disable=invalid-name
        self._deleted_at_head = False
        self._head_db_rev = None

    def head_db_rev(self):
        """Return the DbRev record most recently record_head()ed."""
        return self._head_db_rev

    def record_head(self, db_rev, is_delete):
        """Record a new head revision."""
        self.append_rev(
              change_num = db_rev.change_num
            , is_delete  = is_delete
            , depot_path = db_rev.depot_path )
        self._head_db_rev = db_rev

    def next_rev_num(self):
        """Return the integer number to use for the next rev of this file."""
        if not self._head_db_rev:
            return 1
        return 1 + self._head_db_rev.depot_rev

    def append_rev(self, change_num, is_delete, depot_path):
        """Record a new revision.

        Returns the integer revision number for this new action.

        depot_path used only for reporting bugs.
        """
        if is_delete:
            if (not 1 < len(self._l)) or self._deleted_at_head:
                LOG.warning("RevHistory.append_rev()"
                         " attempt to delete a depot file that"
                         " does not exist, undeleted at head: {}"
                         .format(depot_path))

        is_add = self._deleted_at_head and not is_delete

                        # Record "add" revisions so that they can
                        # act as #startRev integ sources later.
        rev_num = len(self._l)
        if is_add and 1 < rev_num:
            if self._l[0] is None:
                self._l[0] = [rev_num]
            else:
                self._l[0].append(rev_num)
        self._l.append(int(change_num))
        self._deleted_at_head = is_delete
        return rev_num

    def src_range(self, change_num):
        """Return a (#startRev,endRev) integer pair to use as an integ
        source for 'p4 integ -f src@change_num.

        Return (None, None) if no revision at that change_num.
        """
                        # Find highest #rev number at or before @change_num
                        #
                        # +1 here because index_le() is called with a slice
                        # that starts at index 1.
                        #
        i = _index_le(self._l[1:], int(change_num)) + 1
        if i < 1:       # change_num is before first rev#1 change_num
            return (None, None)
        end_rev = i

                        # Find highest #rev number at or before #end_rev
                        # that 'p4 add'ed this file.
        start_rev = 1
        if self._l[0]:
            i = _index_le(self._l[0], end_rev)
            if 0 <= i:
                start_rev = self._l[0][i]
        return (start_rev, end_rev)

    def is_deleted_at_head(self):
        """Was the most recent action one that deleted this file?"""
        return self._deleted_at_head and 1 < len(self._l)

# -- module-wide -------------------------------------------------------------

def _index_le(lisst, val):
    """Return index of element in lisst that is <= val.
    Return -1 if no such element.

    Do this using O(lg n) binary search rather than O(n) scan.
    """
    i = bisect.bisect_right(lisst, val)
    if i:
        return i - 1
    return -1

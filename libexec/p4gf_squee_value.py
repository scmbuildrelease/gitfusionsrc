#! /usr/bin/env python3.3
"""A key/value store that uses SQLite instead of memory."""
from   collections import namedtuple
import logging
import pickle
import re
import sqlite3

import p4gf_const


_RE_BRANCH_ROOT = re.compile("(//{P4GF_DEPOT}/branches/[^/]+/../../[^/]+)/(.*)"
                             .format(P4GF_DEPOT=p4gf_const.P4GF_DEPOT))

LOG = logging.getLogger("p4gf_fast_push").getChild("squee_value")

class SqueeValueDict(object):
    """A key/value store that uses a dict to hold its elements.

    Intended as a drop-in replacement for SqueeValue(), so that we can  measure
    just the Python function call overhead vs. the SQLite cost.
    """

    def __init__(self, file_path):
        self._file_path = file_path
        self._d = {}        # pylint:disable=invalid-name

    def __getitem__(self, key):
        return self._d[key]

    def __setitem__(self, key, value):
        self._d[key] = value

    def __contains__(self, key):
        return key in self._d

    def get(self, key, default = None):
        """dict.get() replacement"""
        return self._d.get(key, default)

    def add(self, key):
        """set.add() replacement"""
        self.__setitem__(key, True)

# end SqueeValueDict
# ----------------------------------------------------------------------------

class SqueeValue(object):
    """A key/value store that uses a single SQLite table instead of memory."""

    def __init__(self, file_path):
        self._file_path = file_path

                        # How many calls to __setitem__ since our last COMMIT?
                        # When this reaches _uncommitted_set_max, we commit.
        self._uncommitted_set_ct  = 0
        self._uncommitted_set_max = 1000

        self._db = sqlite3.connect( database = file_path
                                  , isolation_level = "EXCLUSIVE" )

                        # Accelerate inserts.
                        #
                        # Don't wait for the filesystem to flush to disk.
        self._db.execute("PRAGMA synchronous = OFF")
                        #
                        # Accumulate current transaction in memory, not
                        # a disk-based journal file.
        self._db.execute("PRAGMA journal_mode = MEMORY")

                        # Some times you can accelerate inserts by skipping the
                        # index/primary key, or batch-building the index once
                        # all the inserts are over. This is not one of those
                        # times.
                        #
                        # Each of our uses of SqueeValue interweave reads and
                        # writes for each commit.
        self._db.execute("CREATE TABLE kv(key TEXT PRIMARY KEY, val TEXT)")
                        ### RevHistory may require a richer schema, so that we
                        ### can bulk-clear "written to contextdata.jnl".

    def __getitem__(self, key):
        cursor = self._db.execute("SELECT val FROM kv WHERE key=?", (key,) )
        row = cursor.fetchone()
        if row is None:
            raise KeyError(key)
        pickled_value = row[0]
        value = pickle.loads(pickled_value)
        return value

    def __setitem__(self, key, value):
        pickled_value = pickle.dumps(value)
        try:
            self._db.execute("INSERT INTO kv VALUES(?, ?)", (key, pickled_value))
        except sqlite3.IntegrityError:
            self._db.execute("UPDATE kv SET val=? WHERE key=?", (pickled_value, key))

        self._uncommitted_set_ct += 1
        if self._uncommitted_set_max <= self._uncommitted_set_ct:
            self._db.commit()
            self._uncommitted_set_ct = 0

    def __contains__(self, key):
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

    def get(self, key, default = None):
        """dict.get() replacement."""
        try:
            val = self.__getitem__(key)
            return val
        except KeyError:
            return default

    def add(self, key):
        """set.add() replacement"""
        self.__setitem__(key, True)

# end SqueeValue
# ----------------------------------------------------------------------------

class SqueeValueMultiTable(object):
    """A key/value store that uses multiple SQLite tables instead of memory."""

    def __init__(self, file_path):
        self._file_path = file_path

                        # How many calls to __setitem__ since our last COMMIT?
                        # When this reaches _uncommitted_set_max, we commit.
        self._uncommitted_set_ct  = 0
        self._uncommitted_set_max = 100000

        self._db = sqlite3.connect( database = file_path
                                  , isolation_level = "EXCLUSIVE" )

                        # Accelerate inserts.
                        #
                        # Don't wait for the filesystem to flush to disk.
        self._db.execute("PRAGMA synchronous = OFF")
                        #
                        # Accumulate current transaction in memory, not
                        # a disk-based journal file.
        self._db.execute("PRAGMA journal_mode = MEMORY")


        self._table_names = {}

    def __getitem__(self, key):
        tk = self._to_table_key(key)
        created = False
        if not tk.table_exists:
            self._create_table(tk, "get")
            created = True
        cursor = self._db.execute(
                    "SELECT val FROM {} WHERE key=?".format(tk.table_name)
                    , (tk.sub_key,) )
        row = cursor.fetchone()
        if created:
            LOG.debug("SELECT after CREATE TABLE complete")
        if row is None:
            raise KeyError(key)
        pickled_value = row[0]
        value = pickle.loads(pickled_value)
        return value

    def __setitem__(self, key, value):
        pickled_value = pickle.dumps(value)
        tk = self._to_table_key(key)
        created = False
        if not tk.table_exists:
            self._create_table(tk, "set")
            created = True
        try:
            self._db.execute("INSERT INTO {} VALUES(?, ?)".format(tk.table_name)
                , (tk.sub_key, pickled_value))
        except sqlite3.IntegrityError:
            self._db.execute("UPDATE {} SET val=? WHERE key=?".format(tk.table_name)
                , (pickled_value, tk.sub_key))
        if created:
            LOG.debug("INSERT INTO after CREATE TABLE complete")

        self._uncommitted_set_ct += 1
        if self._uncommitted_set_max <= self._uncommitted_set_ct:
            LOG.debug("COMMIT after {} writes".format(self._uncommitted_set_ct))
            self._db.commit()
            LOG.debug("COMMIT done")
            self._uncommitted_set_ct = 0

    def __contains__(self, key):
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

    def get(self, key, default = None):
        """dict.get() replacement."""
        try:
            val = self.__getitem__(key)
            return val
        except KeyError:
            return default

    def add(self, key):
        """set.add() replacement"""
        self.__setitem__(key, True)

    def _create_table(self, table_key, why):
        """New table time."""
        LOG.debug("CREATE TABLE {table} for {why} {tk}".format(
            table=table_key.table_name, tk=table_key, why=why))

                        # Some times you can accelerate inserts by skipping the
                        # index/primary key, or batch-building the index once
                        # all the inserts are over. This is not one of those
                        # times.
                        #
                        # Each of our uses of SqueeValue interweave reads and
                        # writes for each commit.
        self._db.execute( "CREATE TABLE {}(key TEXT PRIMARY KEY, val TEXT)"
                          .format(table_key.table_name))
        self._table_names[table_key.table_fodder] = table_key.table_name
        LOG.debug("CREATE TABLE {} complete".format(table_key.table_name))

    def _to_table_key(self, key):
        """Split a key into a table name and key within that table.
        """
        (table_fodder, sub_key) = _to_key_fodder_branchity(key)
        try:
            table_name = self._table_names[table_fodder]
        except KeyError:
            table_name = "T_{}".format(len(self._table_names))

        return TableKey(
              table_name   = table_name
            , sub_key      = sub_key
            , table_fodder = table_fodder
            , table_exists = table_fodder in self._table_names )


def _to_key_fodder_75(key):
    """ Return key, split into a 2-tuple (table_fodder, sub_key).

    Tends to produce far too many tables with very few rows each.
    """
    if len(key) < 75:
                        # Not enough prefix to use as a table name.
                        # Stuff all short paths into one table.
        table_fodder = "_p4gf_short"
        sub_key      = key
    else:
        table_fodder = key[:75]
        sub_key      = key[75:]
    return (table_fodder, sub_key)


def _to_key_fodder_branchity(key):
    """ Return key, split into a 2-tuple (table_fodder, sub_key)."""
                        # //.git-fusion/branches/...
    m = _RE_BRANCH_ROOT.search(key)
    if m:
        table_fodder = m.group(1)   # DepotBranchInfo root
        sub_key      = m.group(2)   # path below dbi root
        return (table_fodder, sub_key)

                        # //depot/... space
    w = key.split('/', 5)
    table_fodder = "/".join(w[:-1])
    sub_key      = "/".join(w[-1:])
    return (table_fodder, sub_key)

# Used internally by SqueeValueMultiTable
TableKey = namedtuple("TableKey"
            , [ "table_name"
              , "sub_key"
              , "table_exists"
              , "table_fodder"
              ])


# end SqueeValue
# ----------------------------------------------------------------------------

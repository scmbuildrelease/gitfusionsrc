#! /usr/bin/env python3.3
"""Scan Git Fusion's metadata storage and identify any unreachable objects
that could be garbage-collected via 'p4 obliterate' to reclaim P4D server
space.

Because blobs/ and trees/ are shared across all repos, any blob/tree garbage
collection must scan the ENTIRE Git Fusion store, all repos, to check for
reachability. This is not something that can be done per-repo as part of
p4gf_delete_repo.py.

This script is only useful after deleting a repo: Git Fusion does not create
unreachable objects as part of normal operation.

This script is only useful if you deleted 1 or some, but not ALL, of your
repos. If you delete ALL Git Fusion repos, you can skip this script and
obliterate all of //.git-fusion/objects/... .

This script is VERY expensive to run. It must download every single Git commit
and tree object from the Perforce server (often 100K-10M files, and multiple GB
of disk space), then scan through all of that to calculate Git object
reachability (no, neither `git prune` nor `git gc` will not work here, they
were designed to work with a handful of refs and a well-formed repo, not the
thousands of refs and the fractured repo that we create here.)
* takes hours to run
* takes multiple GB of disk space within current working directory
* <1GB of memory

Does NOT run `p4 obliterate`. Just reports files that can be obliterated.
The `p4 obliterate` step will likely be too large to run in a single pass
without locking up your Perforce server for hours (days!). We leave it up to
the admin to work with Perforce support, chunk up the obliterate into
acceptable sizes, and run them during off hours when Perforce users can
accept server downtime.

Implementation Details:

1. Create directory .p4gf_gc
2. Initialize a Git repo under .p4gf_gc
3. Fill that Git repo with commit and tree objects from //.git-fusion/objects/...
4. Fill a sqlite database with tree and blob sha1s from //.git-fusion/objects/...

    ... Hours and GB for Steps 3 + 4 ...

5. Scan the Git repo for all objects "reachable from" any commit in any repo,
   update sqlite database with reachable successes.

    ... Hours for Step 5 ...

6. Scan the sqlite database for all objects not updated as reachable:
   these are objects that can be garbage collected with `p4 obliterate`.
7. Also report all deleted commits and blobs in //.git-fusion/objects/...
8. Delete the directory .p4gf_gc

"""

from   collections import namedtuple
import logging
import os
import sys
import re
import pygit2
import shutil
import sqlite3
import time
import datetime
import pickle

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_branch
import p4gf_const
import p4gf_create_p4
import p4gf_eta
from   p4gf_l10n             import _, NTR
import p4gf_log
import p4gf_proc
import p4gf_util
import p4gf_p4spec
import p4gf_ensure_dir
from P4 import OutputHandler
import p4gf_progress_reporter as ProgressReporter


# Progress Codes .. set into the database
INIT             = 1
BLOBS_ADDED      = 2
TREES_DELETED    = 3
BLOBS_DELETED    = 4
SYNC             = 5
OBJECTS_MOVED    = 6
REACHABLES_FOUND = 7
REPORTED         = 99

# some table names
TREES                      = 'trees'
BLOBS                      = 'blobs'
COMMITS                    = 'commits'
DELETED_TREES              = 'deleted_trees'
DELETED_BLOBS              = 'deleted_blobs'
DELETED_COMMITS            = 'deleted_commits'
TREES_FROM_COMMITS         = "trees_from_commits"
MISSING_TREES_FROM_COMMITS = "missing_trees_from_commits"

SHA1_PREFIX_LEN            = 3  # used to scatter the inserts across a family of table names.


                        # Log level reminder from p4gf_util.apply_log_args():
                        # verbose   = INFO
                        # <nothing> = WARNING
                        # quiet     = ERROR
                        #
LOG = logging.getLogger("p4gf_gc_stdout")

class SyncHandler(OutputHandler):
    """OutputHandler for p4 sync; prevents memory overrun of stat data."""
    # pylint: disable=invalid-name
    def __init__(self, gc, object_type):
        OutputHandler.__init__(self)
        self.gc = gc
        self.object_type = object_type
        self.message = _('Syncing {otype}... {et} {ed}')


    def outputStat(self, h):
        """Supress output to prevent memory exhaustion."""
        try:
            self.gc.eta.increment()
            if not self.gc.quiet:
                ProgressReporter.increment(self.message.format(
                    otype = self.object_type, et = self.gc.eta.eta_str(),
                    ed = self.gc.eta.eta_delta_str()))
            self.gc.sync_counts[self.object_type] += 1
        except Exception:  # pylint: disable=broad-except
            pass
        return OutputHandler.HANDLED


class FilesHandler(OutputHandler):
    """OutputHandler for p4 files; prevents memory overrun of stat data."""
    # pylint: disable=invalid-name
    def __init__(self, gc, blobs_root):
        OutputHandler.__init__(self)
        self.gc = gc
        self.blobs_root = blobs_root
        self.message = _("Finding blobs ... {et} {ed}")

    def outputStat(self, h):
        """Used to store all blob depot_paths into the database."""
        try:
            if not self.gc.quiet:
                self.gc.eta.increment()
                ProgressReporter.increment(self.message.format(
                    et = self.gc.eta.eta_str() , ed = self.gc.eta.eta_delta_str()))
            depot_path = h['depotFile']
            deleted = 1 if h['action'] in ['delete','move/delete'] else 0
            sha1 = depot_path.replace(self.blobs_root,'')
            sha1 = sha1.replace('/','')
            if deleted:
                self.gc.sql_insert_object(DELETED_BLOBS, depot_path)
            else:
                self.gc.sql_insert_object(BLOBS, sha1, depot_path)
        except Exception as e:  # pylint: disable=broad-except
            LOG.exception("FilesHandler:outputStat {}".format(str(e)))
        return OutputHandler.HANDLED

class FstatHandler(OutputHandler):
    """OutputHandler for p4 fstat; prevents memory overrun of stat data."""
    # pylint: disable=invalid-name
    def __init__(self, gc, object_type):
        OutputHandler.__init__(self)
        self.gc = gc
        self.object_type = object_type
        self.message = _('Finding deleted %ss ' % (object_type,))
        if object_type == 'tree':
            self.depot_path_prefix = NTR("{}/trees/".format(
                         p4gf_const.objects_root()))


    def outputStat(self, h):
        """Used to insert DELETED trees and commits into the database."""
        try:
            if not self.gc.quiet:
                ProgressReporter.increment(self.message)
            depot_path = h['depotFile']
            if self.object_type == 'tree':
                #sha1 = depot_path.replace(self.depot_path_prefix,'')
                #sha1 = sha1.replace('/','')
                self.gc.sql_insert_object(DELETED_TREES, depot_path)
            elif self.object_type == 'commit':
                self.gc.sql_insert_object(DELETED_COMMITS, depot_path)
        except Exception as e:  # pylint: disable=broad-except
            LOG.exception("FstatHandler:outputStat {}".format(str(e)))
        return OutputHandler.HANDLED


class GC(object):  # pylint: disable=too-many-public-methods,too-many-instance-attributes
    """A class that knows how to scan Git Fusion metadata under
    //.git-fusion/objects/... and identify any objects that are no longer
    referenced by any existing Git Fusion repo.
    """
    def __init__(self, port, user, *, cont=False,
                 keep=False, force=False, stdout=False, doreport=False, quiet=False):
        self.starttime   = time.time()
        self.cont        = cont
        self.keep        = keep
        self.force       = force
        self.stdout      = stdout
        self.doreport    = doreport
        self.quiet       = quiet
        self.root_path   = os.getcwd()
        self.report_file = None

        if not self.stdout:
            self.report_file = os.path.abspath('p4gf_gc_unreachables')
        self.dir_abspath     = os.path.abspath(".p4gf_gc")
        self.git_dir_abspath = os.path.join(self.dir_abspath, "git")
        self.p4_dir_abspath  = os.path.join(self.dir_abspath, "p4")
        self.git_dir_objects = os.path.join(self.git_dir_abspath, "objects")
        self.sql_dir_abspath = os.path.join(self.dir_abspath, "sql")
        self.sql_db_abspath  = os.path.join(self.dir_abspath, "sql", "db.sqlite")

        if self.cont:
            self.keep = True   # force keep for cont
        else:
            if self.force:
                self.rmdir()
            self.create_dir()

        self.db              = None  # pylint:disable=invalid-name
        self.seen_trees      = set()
        p4gf_ensure_dir.ensure_dir(self.p4_dir_abspath)
        os.chdir(self.p4_dir_abspath)
        os.environ['PWD']=self.p4_dir_abspath
        self.p4 = p4gf_create_p4.create_p4_temp_client(port = port,
                                              user=user)
        p4gf_branch.init_case_handling(self.p4)
        self.report_progress_msg      = ''
        self._uncommitted_set_ct      = 0
        self._uncommitted_set_max     = 1000
        self.git_repo                 = None
        self.max_rows_per_table       = 500000
        self.sync_counts              = { TREES                : 0,
                                          COMMITS              : 0}

        self.table_type_counts        = { TREES                : 0,
                                          BLOBS                : 0,
                                          DELETED_TREES        : 0,
                                          DELETED_BLOBS        : 0,
                                          DELETED_COMMITS    : 0,
                                          TREES_FROM_COMMITS : 0,
                                          MISSING_TREES_FROM_COMMITS : 0 }
        self.table_names              = { TREES                : [],
                                          BLOBS                : [],
                                          DELETED_TREES        : [],
                                          DELETED_BLOBS        : [],
                                          DELETED_COMMITS      : [],
                                          TREES_FROM_COMMITS   : [],
                                          MISSING_TREES_FROM_COMMITS   : [] }

        self.fd = None         # pylint:disable=invalid-name
        self.debug  = LOG.isEnabledFor(logging.DEBUG)
        self.debug2 = LOG.isEnabledFor(logging.DEBUG2)
        self.debug3 = LOG.isEnabledFor(logging.DEBUG3)
        self.status = None
        self.eta    = None
        self.tree_recursion_depth = 0
        self.git_dir = None


    def _to_table_key(self, table_type, key):
        """Split tables into sets of tables.
        """
        # Tables which are to be updated need to distribute the sha1 keys across
        # multiple tables for which the names based on the table_type and
        # the first three characters of the key which is the git object sha1
        if table_type in [TREES, BLOBS]:
            suffix = key[:SHA1_PREFIX_LEN]  # usually set at 3
            table_name = "%s_%s" % (table_type,suffix)

        # tables which will not be updated need no lookup key
        # thus the table key is merely the index into the current table of max_rows_per_table
        # Use integer divide '//' the index into max_rows_per_table tables.
        # These table names ignore the key for table name determination
        else:
            table_index = self.table_type_counts[table_type] // self.max_rows_per_table
            try:
                table_name = self.table_names[table_type][table_index]
            except IndexError:
                table_name = "{}_{}".format(table_type,table_index)

        return TableKey(
              table_name   = table_name
            , table_type   = table_type
            , table_exists = table_name in self.table_names[table_type] )


    def gc(self):   # pylint:disable=invalid-name
        """Do the thing."""
        if not self.cont:
            self.git_init()
            self.sql_init()
            self.sql_set_status(INIT)
            self.add_blobs_to_db()
            self.sql_set_status(BLOBS_ADDED)

            # add deleted trees to database
            depot_path = NTR("{}/trees/...".format(
                         p4gf_const.objects_root()))
            self.add_deleted_objects_to_db(depot_path, 'tree')
            self.sql_set_status(TREES_DELETED)

            # add commits to database
            depot_path = NTR("{}/repos/.../commits/...".format(
                         p4gf_const.objects_root()))
            self.add_deleted_objects_to_db(depot_path, 'commit')
            self.sql_set_status(BLOBS_DELETED)

            self.sync()
            self.sql_set_status(SYNC)
            # 'rename' the sync'ed files into .git/objects/xx/zzz...
            self.move_gf_objects_locations()
            pickled_value = pickle.dumps(self.table_names)
            # Save the table names and counts in the db
            # These will be read for the --cont option
            self.sql_set_admin(pickled_value, 'table_names')
            pickled_value = pickle.dumps(self.table_type_counts)
            self.sql_set_admin(pickled_value, 'table_type_counts')
            self.sql_set_status(OBJECTS_MOVED)
            self.db.commit()
        else:
            # will check the data base exists and get the status
            self.cont_init()

        if self.status == OBJECTS_MOVED:
            self.find_reachable()
            self.sql_set_status(REACHABLES_FOUND)
            self.db.commit()
        if self.status == REACHABLES_FOUND or (self.doreport and self.status == REPORTED):
            self.report()
        self.sql_set_status(REPORTED)
        self.db.commit()
        self.rmdir(keep=self.keep)
        laspsed_time = time.time() - self.starttime
        self.print_quiet(
                _("\nLapsed time: {}").format(str(datetime.timedelta(seconds=laspsed_time))))


    def create_dir(self):
        """Create .p4gf_gc directory, or fail if already exists."""
        if os.path.exists(self.dir_abspath):
            raise RuntimeError("Directory already exists: {}"
                               "\nEither:"
                               "\n* p4gf_gc.py --force to delete it and start clean, or"
                               "\n* p4gf_gc.py --cont to use whatever data is in .p4gf_gc"
                               .format(self.dir_abspath))
        LOG.info("Creating work directory: {}".format(self.dir_abspath))
        p4gf_util.ensure_dir(self.dir_abspath)
        p4gf_util.ensure_dir(self.git_dir_abspath)
        p4gf_util.ensure_dir(self.sql_dir_abspath)

    def git_init(self):
        """Create an empty Git repository into which we can sync objects from
        Perforce and then examine them with pygit2.
        """
        os.chdir(self.git_dir_abspath)
        LOG.info("Initializing Git repository: {}".format(self.git_dir_abspath))
        pygit2.init_repository(self.git_dir_abspath, bare=True)
        self.git_repo = pygit2.Repository(self.git_dir_abspath)

    def sql_init(self):
        """Create a sqlite database that will house the millions of tree and
        blob sha1s and whether each sha1 is reachable by a commit.
        """
        LOG.info("Creating sqlite database: {}".format(self.sql_db_abspath))
        self.db = sqlite3.connect( database = self.sql_db_abspath
                                 , isolation_level = "EXCLUSIVE" )

                        # Accelerate inserts.
                        #
                        # Don't wait for the filesystem to flush to disk.
        self.db.execute("PRAGMA synchronous = OFF")
                        #
                        # Accumulate current transaction in memory, not
                        # a disk-based journal file.
        self.db.execute("PRAGMA journal_mode = MEMORY")
        # Create the status table for tracking state for --cont
        statement = "CREATE TABLE admin (key TEXT PRIMARY KEY," " data TEXT)"
        self.db.execute(statement)
        self.db.commit()

    def sql_create_table(self, table_key):
        """Create the table based on type and name."""
        if table_key.table_type in [TREES, BLOBS]:
            statement = "CREATE TABLE {} (key TEXT PRIMARY KEY," \
                " reachable INTEGER, depot_path TEXT)".format(
                        table_key.table_name)
            if self.debug3:
                LOG.debug3("sql_create_table: {}".format(statement))
            self.db.execute(statement)
            statement = "CREATE INDEX {}_reachable_idx ON {} (reachable)".format(
                    table_key.table_name, table_key.table_name)
            self.db.execute(statement)
        elif table_key.table_type in [DELETED_TREES,  DELETED_BLOBS, TREES_FROM_COMMITS,
                                      DELETED_COMMITS,
                                      MISSING_TREES_FROM_COMMITS]:
            self.db.execute("CREATE TABLE {}(key TEXT PRIMARY KEY)".format(table_key.table_name))
        self.table_names[table_key.table_type].append(table_key.table_name)
        self.sql_commit(force=True)

    def sql_set_admin(self, data, key):
        """Set or update an admin record."""
        try:
            self.db.execute("INSERT INTO admin VALUES(?, ?)", (key, data))
        except sqlite3.IntegrityError:
            self.db.execute("UPDATE admin SET data=? WHERE key=?",(data, key))

    def sql_get_admin(self, key):
        """Get an admin record."""
        try:
            cursor = self.db.execute("SELECT data FROM admin WHERE key=?", (key, ))
            row = cursor.fetchone()
            if row == None:
                return None
            return row[0]
        except Exception:  # pylint: disable=broad-except
            return None

    def sql_set_status(self, data):
        """Set or update a status record."""
        # Convert the int status to a string.
        self.sql_set_admin(str(data),'status')
        self.status = data

    def sql_get_status(self):
        """Get a status record."""
        # Convert the db string status to int.
        return int(self.sql_get_admin('status'))



    def sql_commit(self, force=False):
        """Commit after max updates."""
        self._uncommitted_set_ct += 1
        if force or self._uncommitted_set_max <= self._uncommitted_set_ct:
            self.db.commit()
            self._uncommitted_set_ct = 0


    def sql_insert_object(self, table_type, key, depot_path=None):
        """Add record to database."""
        tk = self._to_table_key(table_type, key)
        if self.debug3:
            LOG.debug3("inserting object into {} : {} {}".format(tk.table_name, key,
            depot_path))
        if not tk.table_exists:
            self.sql_create_table(tk)
        try:
            # The default value for tree/blob inserts in not-reachable
            if table_type in [TREES, BLOBS]:
                self.db.execute("INSERT INTO {} VALUES(?, ?, ?)".format(tk.table_name),
                        (key, 0, depot_path))
            else:
                self.db.execute("INSERT INTO {} VALUES(?)".format(tk.table_name), (key,))
            self.table_type_counts[tk.table_type] +=  1
        except sqlite3.IntegrityError:
            # multiple branch with the same commits are possible
            # thus ignore this error for trees in TREES_FROM_COMMITS.
            # No other tables should produce this error.
            if table_type == TREES_FROM_COMMITS:
                pass
            else:
                raise
        self.sql_commit()
        return True

    def sql_mark_object_reachable(self, table_type, sha1):
        """Mark a tree/blob reachable."""
        table = self._to_table_key(table_type, sha1)
        if self.debug3:
            LOG.debug3("sql_mark_object_reachable {}  {}".format(table, sha1))
        try:
            self.db.execute("UPDATE {} SET reachable=? WHERE key=?".format(table.table_name),
                (1, sha1))
        except Exception as e :  # pylint: disable=broad-except
            if 'no such table' in str(e):
                pass
            else:
                raise
        self.sql_commit()

    def sql_report_table(self, statement, value=None):
        """Select and print the rows of a table."""
        rows_printed = 0
        if not value == None:
            cursor = self.db.execute(statement, (0,))
        else:
            cursor = self.db.execute(statement)
        while True:
            row = cursor.fetchone()
            if row == None:
                break
            print("%s" %  (row[0],), file=self.fd)
            rows_printed += 1
            if not self.report_file == 'stdout' and not self.quiet:
                ProgressReporter.increment(self.report_progress_msg)
        return rows_printed

    def report(self):        # pylint: disable=too-many-branches,too-many-statements
        """Print the table."""
        if self.stdout:
            self.fd = sys.stdout
            self.report_file = 'stdout'  # For reporting message
        else:
            self.fd = open(self.report_file, 'w')
        msg = _("#\n"
                "# DELETED: "
                "These Git Fusion object cache files are already deleted.\n"
                "# They are safe to 'p4 obliterate'.\n#")
        print(msg, file=self.fd)

        unreachable_rows = 0
        deleted_rows = 0
        self.report_progress_msg = _("Reporting deleted objects")
        with ProgressReporter.Indeterminate():
            for table_type in [DELETED_COMMITS, DELETED_TREES, DELETED_BLOBS]:
                for t in self.table_names[table_type]:
                    statement = "SELECT * FROM {}".format(t)
                    deleted_rows += self.sql_report_table(statement)

        print(_("#\n# END DELETED: {} deleted objects exist.".format(deleted_rows)), file=self.fd)

        msg = _("#\n#\n#\n"
               "# UNREACHABLE: These existing Git Fusion "
               "object cache files are not reachable by any existing commit.\n"
               "# They are safe to 'p4 obliterate'.\n#")
        print(msg, file=self.fd)

        self.report_progress_msg = _("Reporting unreachable objects")
        with ProgressReporter.Indeterminate():
            for table_type in ['trees', 'blobs']:
                for t in self.table_names[table_type]:
                    statement = "SELECT depot_path FROM {} where reachable=?".format(t)
                    unreachable_rows += self.sql_report_table(statement, 0)

            print(_("#\n# END UNREACHABLE: {} unreachable objects exist.".
                format(unreachable_rows)), file=self.fd)

        # Display the report path to stdout.
        print("Report of Git Fusion unreachable objects written to: '{}'.".
                format(self.report_file))
        # Display the 'counts' to stdout.
        if not self.report_file == 'stdout' and not self.quiet:
            print(_("{} deleted objects exist.".format(deleted_rows)))
            print(_("{} unreachable objects exist.".format(unreachable_rows)))

    def sync(self):
        """Sync.
        The naming convention of /git-fusion/objects/repos/commits/... prevents
        using 'sync' to 'git/objects/... ( requires two lhs '...' against one rhs '...').
        Additionally blobs use a tree depth two deeper than used by commits and trees.
        To make easier git-comformed renaming of these objects into .git/objects/...
        we initially sync each into separate directories: trees/..., repos/...

        Use P4 OutputHandler to avoid memory overruns and perform db inserts.

        """
        # Set client for syncing trees into git/objects/trees/...
        view =  ['//{depot}/objects/trees/... //{client}/trees/...'.format(
                depot=p4gf_const.P4GF_DEPOT,
                client=self.p4.client)]
        trees_path =  '//{depot}/objects/trees/...'.format(
                depot=p4gf_const.P4GF_DEPOT)
        # Use p4 sizes to get number of trees
        num_trees = self.get_num_files(trees_path)
        self.eta = p4gf_eta.ETA(total_ct = num_trees)
        p4gf_p4spec.ensure_spec_values(self.p4, 'client', self.p4.client,
                {'Root': self.p4_dir_abspath, 'View': view})
        with ProgressReporter.Determinate(num_trees):
            handler = SyncHandler(self, TREES)
            with self.p4.using_handler(handler):
                self.p4.run('sync', '-p', '...')

        # Set client for syncing clients into git/objects/repos/...
        view =  ['//{depot}/objects/repos/.../commits/... //{client}/repos/.../commits/...'.format(
                depot=p4gf_const.P4GF_DEPOT,
                client=self.p4.client)]
        p4gf_p4spec.ensure_spec_values(self.p4, 'client', self.p4.client,
                {'Root': self.p4_dir_abspath, 'View': view})
        commits_path =  '//{depot}/objects/repos/.../commits/...'.format(
                depot=p4gf_const.P4GF_DEPOT)
        # Use p4 sizes to get number of commits
        num_commits = self.get_num_files(commits_path)
        self.eta = p4gf_eta.ETA(total_ct = num_commits)
        with ProgressReporter.Determinate(num_commits):
            handler = SyncHandler(self, COMMITS)
            with self.p4.using_handler(handler):
                self.p4.run('sync', '-p', '...')


    def move_objects(self, walk_root, top_level_regex, subdir_regex, trim_hyphenated_suffix=False):
        """Move the object cache objects to '.git/objects'."""
        # pylint: disable=too-many-branches,too-many-statements
        # Because git paths are distributed over directory names taken
        # from the first 2 chars of the sha1, and Git Fusion cache
        # paths use xx/xx (trees and commits)
        # there is a boatload of data munging going on here.
        doing_trees = TREES in walk_root
        doing_commits = 'repos' in walk_root
        if doing_trees:
            progress_msg = _("Moving cached trees to local git ... {et} {ed}")
            object_count = self.sync_counts[TREES]
        else:
            progress_msg = _("Moving cached commits to local git ... {et} {ed}")
            object_count = self.sync_counts[COMMITS]
        self.eta = p4gf_eta.ETA(total_ct = object_count)
        with ProgressReporter.Determinate(object_count):
            for walk_root, _dirs, files in os.walk(walk_root):
                # For top level dirs, create the same dir under '.git/objects'
                m = top_level_regex.match(walk_root)
                if m:
                    for d in _dirs:
                        obj_dir = os.path.join(self.git_dir_objects, d)
                        p4gf_ensure_dir.ensure_dir(obj_dir)

                # If we have files we need to move them to 'git/objects'
                if files:
                    if not self.quiet:
                        self.eta.increment()
                        ProgressReporter.increment(progress_msg.
                            format( et = self.eta.eta_str()
                                  , ed = self.eta.eta_delta_str()))
                    sub1 = sub2 = None
                    m = subdir_regex.match(walk_root)
                    if m:
                        sub1 = m.group('sub1')
                        sub2 = m.group('sub2')
                    else:
                        LOG.error("regex failed to match as expected on {}.\nStopping.".format
                                (walk_root))
                        print("regex failed to match as expected on {}.\nStopping.".
                                format(walk_root))
                        sys.exit(1)
                    if doing_trees:
                        depot_path_prefix = NTR("{}/trees/{}/{}/".format(
                             p4gf_const.objects_root(), sub1, sub2))
                    for name in files:
                        git_file = sub2 + name
                        if trim_hyphenated_suffix:
                            git_file = re.sub(r'-.*$','',git_file)
                        git_sha1 = sub1 + git_file
                        if doing_trees:
                            depot_path = depot_path_prefix + name
                            self.sql_insert_object(TREES, git_sha1, depot_path)

                        git_path = os.path.join(self.git_dir_objects, sub1)
                        git_path = os.path.join(git_path,git_file)
                        p4_path = os.path.join(walk_root, name)
                        # Finally , move the p4 path to the git path
                        try:
                            os.rename(p4_path,git_path)
                        except OSError as e:
                            LOG.error("exception {}".format(str(e)))
                            sys.exit(1)
                        if doing_commits:
                            self.add_tree_from_commit_to_table(git_sha1)
                            # now that the commit's tree sha1 is in the db,
                            # the commit object is no longer needed
                            try:
                                os.unlink(git_path)
                            except OSError as e:
                                LOG.error("exception {}".format(str(e)))
                                sys.exit(1)


    def add_tree_from_commit_to_table(self, git_sha1):
        """Call git to get the tree of this commit.
        Insert the tree and mark it reachable."""
        pygit2_commit = self.git_repo[git_sha1]
        tree_sha1 = pygit2_commit.tree_id
        self.sql_insert_object(TREES_FROM_COMMITS,(str(tree_sha1)))
        self.sql_mark_object_reachable(TREES, str(tree_sha1))
        if self.debug3:
            LOG.debug3("add_tree_from_commit_to_table commit={}  tree={}".format(
                git_sha1, str(tree_sha1)))

    def move_gf_objects_locations(self):
        """P4 sync has placed our GF object cache objects into
        directories: trees/... , repos/...
        Now move them into properly named and located 'git/object/...' paths.
        """
        two_sub_regex = re.compile(r'^.+(?P<sub1>[0-9a-fA-F]{2})/(?P<sub2>[0-9a-fA-F]{2})$')

        # Move trees.
        tree_root = os.path.join(self.p4_dir_abspath, TREES)
        tree_top_level_re = re.compile('^' + tree_root + '$')
        self.move_objects(tree_root, tree_top_level_re, two_sub_regex)

        # Move commits.
        commit_root = os.path.join(self.p4_dir_abspath, 'repos')
        repo_commits_regex = re.compile(r'^.+p4/repos/(?P<repo>.*)/commits$')
        self.move_objects(commit_root, repo_commits_regex,
                          two_sub_regex, trim_hyphenated_suffix=True)

    def get_num_files(self, depot_path):
        """Get number of files at #head."""
        r = self.p4.run('sizes', '-s', depot_path)
        return int(r[0]['fileCount'])

    def add_blobs_to_db(self):
        """Use p4 files to populate the database with blob sha1.
        The FilesHandler does the inserts and avoids memory overruns."""

        blobs_root = NTR('{objects_root}/blobs/').format(
                objects_root=p4gf_const.objects_root())
        num_blobs = self.get_num_files(blobs_root + '...')
        self.eta = p4gf_eta.ETA(total_ct = num_blobs)
        with ProgressReporter.Determinate(num_blobs):
            handler = FilesHandler(self, blobs_root)
            with self.p4.using_handler(handler):
                self.p4.run('files', blobs_root + '...')

    def add_deleted_objects_to_db(self, depot_path, object_type, ):
        """Add deleted objects (trees or commits) to the database.
        The FstatHandler does the inserts and avoids memory overruns."""
        with ProgressReporter.Indeterminate():
            handler = FstatHandler(self, object_type)
            with self.p4.using_handler(handler):
                self.p4.run('fstat', '-T', 'depotFile',
                            '-F', 'headAction=delete | headAction=move/delete', depot_path)

    def cont_init(self):
        """This continue init is called when the P4 syncs, and database inserts
        etc are done but the reachable computation or the final report is not.
        Thus we reuse the existing database, and initialize from data stored therein."""

        if not os.path.exists(self.sql_db_abspath):
            print(_("-the --cont option is set but the sql database '{}' does not exist.").
                format(self.sql_db_abspath))
            sys.exit(1)
        if not os.path.exists(self.git_dir_abspath):
            print(_("-the --cont option is set but the git repo  '{}' does not exist.").
                format(self.git_dir_abspath))
            sys.exit(1)
        self.print_quiet(_("Continuing with already retrieved objects from Helix.\n"
                "Using existing sql database: '{}'.\n").
                format(self.sql_db_abspath))
        self.git_repo = pygit2.Repository(self.git_dir_abspath)
        self.db = sqlite3.connect( database = self.sql_db_abspath
                                 , isolation_level = "EXCLUSIVE" )
        self.status = self.sql_get_status()
        if self.status >= OBJECTS_MOVED:
            table_names = self.sql_get_admin('table_names')
            if table_names:
                self.table_names = pickle.loads(table_names)
            table_counts = self.sql_get_admin('table_type_counts')
            if table_counts:
                self.table_type_counts = pickle.loads(table_counts)
        if self.status == REPORTED and not self.doreport:
            print(_("Nothing to do. database 'status' table reports status is REPORTED."))
            print(_("Use the '--doreport' option to re-create the report."))

    def find_reachable(self):
        """Mark every tree and blob reachable in the database.
        The trees in these tables are already marked reachable.
        Use recursion into the trees and mark trees/blobs reachable."""
        tree_count = self.table_type_counts[TREES_FROM_COMMITS]
        self.eta = p4gf_eta.ETA(total_ct = tree_count)
        with ProgressReporter.Determinate(tree_count):
            # This first table set contains the trees extracted from known commits.
            for table in self.table_names[TREES_FROM_COMMITS]:
                cursor = self.db.execute("SELECT * from {}".format(table))
                self.git_dir = "--git-dir={}".format(self.git_dir_abspath)
                while True:
                    row = cursor.fetchone()
                    if row == None:
                        break
                    if not self.quiet:
                        self.eta.increment()
                        ProgressReporter.increment(
                                _("Traversing commit trees to find "
                                  " cached reachable trees and blobs ... {et} {ed}").
                            format( et = self.eta.eta_str() , ed = self.eta.eta_delta_str()))
                    tree = row[0]
                    # this method recurses for any tree entry within this top-level tree
                    self.mark_tree_contents_reachable(str(tree))

    def mark_tree_contents_reachable(self, tree):
        """Mark each tree and blob in this tree as reachable.
        Assume this tree is already marked.
        Use recursion to mark tree entries. Mark blobs directly.
        Use a set of seen_trees to avoid gargantuan duplication of effort.
        """
        self.tree_recursion_depth += 1
        assert self.git_repo
        tree_object = self.git_repo.get(tree)
        if tree_object:
            for entry in tree_object:
                sha1 = entry.hex
                if entry.filemode == 0o040000:
                    if sha1 in self.seen_trees:
                        continue
                    else:
                        self.sql_mark_object_reachable(TREES, sha1)
                        self.seen_trees.add(sha1)
                        self.mark_tree_contents_reachable(sha1)   # recursion for trees
                elif entry.filemode in [0o100644, 0o100755, 0o100664]:  # blob
                    self.sql_mark_object_reachable(BLOBS, sha1)
        else:
            # Some tree is not in our artificial git repo. Skip it.
            LOG.debug("no tree object in git for {}".format(tree))
        self.tree_recursion_depth -= 1


    def rmdir(self, keep=False):
        """Remove the work directory. Maybe."""
        if keep:
            self.print_quiet(_("Retaining work directory: {}".format(self.dir_abspath)))
        else:
            if os.path.exists(self.dir_abspath):
                self.print_quiet(_("Deleting work directory: {}".format(self.dir_abspath)))
                shutil.rmtree(self.dir_abspath)

    def print_quiet(self, msg):
        """Print the message unless self.quiet."""
        if not self.quiet:
            print(msg)

TableKey = namedtuple("TableKey"
            , [ "table_name"
              , "table_type"
              , "table_exists"
              ])


def parse_argv():
    """Convert command line into a usable dict."""
    usage = _("""p4gf_gc.py [options]

Scan Git Fusion's metadata storage and identify any unreachable objects that
could be garbage-collected via 'p4 obliterate' to reclaim P4D server space.
Also report currently deleted commits, trees, and blobs in the object cache,
Deleted and unreachable paths are reported in sections in the report file.

Because blobs/ and trees/ are shared across all repos, blob/tree garbage
collection must scan the ENTIRE Git Fusion object store to determine reachability.

This script is only useful after deleting a repo: Git Fusion does not create
unreachable objects as part of normal operation.

This script is only useful if you deleted 1 or some, but not ALL, of your
repos. If you delete ALL Git Fusion repos, you can skip this script and
obliterate all of //.git-fusion/objects/... .

This script is VERY expensive to run.
* Takes hours to run.
* Takes multiple GB of disk space within current working directory.
* <1GB of memory
*
* Does NOT run `p4 obliterate`. Rather it reports files that can be obliterated.
* The report file is '$PWD/p4gf_gc_unreachables'.
* Performs no updates Git Fusion or Helix.

            """)
#    usage = _("""p4gf_gc.py [options]
#options:
#    --p4port/-p     Perforce server
#    --p4user/-u     Perforce user
#    --verbose/-v    write more to console
#    --quiet/-q      write nothing but errors to console
#    --cont          reuse existing .p4gf_gc data rather than fetch from Perforce.
#    --keep          retain .p4gf_gc data for future --continue.
#    --force/-f      delete current .p4gf_gc data, start clean
#    --log-stdout    copy logging to stdout
#""")
    parser = p4gf_util.create_arg_parser(
          usage        = usage
        , add_p4_args  = True
        , add_log_args = True
        , add_debug_arg= True
        )
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--force', '-f', action=NTR("store_true"),
            help=_("delete current .p4gf_gc data, start clean"))
    group.add_argument('--cont', action=NTR("store_true"),
            help = _("reuse existing .p4gf_gc data rather than fetch from Perforce."))
    parser.add_argument('--keep', action=NTR("store_true"),
            help=_("retain .p4gf_gc data for future --cont."))
    parser.add_argument('--doreport', action=NTR("store_true"),
            help = _("force report to be produced"))
    group2 = parser.add_mutually_exclusive_group()
    group2.add_argument('--stdout', action=NTR("store_true"),
            help = _("direct report to stdout"))
    group2.add_argument('--log-stdout', action=NTR("store_true"),
            help=_("copy logging to stdout"))
    args = parser.parse_args()
    p4gf_util.apply_log_args(args, LOG, args.log_stdout)
    LOG.debug("args={}".format(args))
    return args

def main():
    """Do the thing."""
    args = parse_argv()
    p4gf_proc.init()

    # Use current or supplied p4user, NOT git-fusion-user, for our work.
    # Use p4 not os.environ(), so that we can honor .p4config.
    p4_query_enviro = p4gf_create_p4.create_p4(port = args.p4port,
                                               user=args.p4user,
                                               warn_no_client = False)
    LOG.debug("P4PORT   : {}".format(p4_query_enviro.port))
    LOG.debug("P4USER   : {}".format(p4_query_enviro.user))
    p4gf_const.P4GF_USER = p4_query_enviro.user

    gc = GC(p4_query_enviro.port,
            p4_query_enviro.user,
            cont=args.cont,
            keep=args.keep,
            force=args.force,
            stdout=args.stdout,
            doreport=args.doreport,
            quiet=args.quiet)
    gc.gc()

    LOG.info("done")

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

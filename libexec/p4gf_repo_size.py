#! /usr/bin/env python3.3
"""Report the estimated size for a newly created P4 based Git Fusion repo."""

import locale
import logging
import os
import re
import shutil
import sqlite3
import sys

import p4gf_env_config  # pylint: disable=unused-import
import p4gf_context
import p4gf_branch
import p4gf_create_p4
from   p4gf_l10n             import _, NTR
import p4gf_log
import p4gf_const
import p4gf_config
import p4gf_p4spec
import p4gf_tempfile
import p4gf_util

DESCRIPTION= _(
"""Report the estimated size for a newly created P4 based Git Fusion repo.

Requires a p4gf_config input data file.
This may be run against any Git Fusion p4d , as long as the Git Fusion code base
in properly installed locally. No Git Fusion instance need be created.
The serverid is spoofed only for the purpuse of creating temp client names.
If run in the Git Fusion user environment, it will use the P4PORT set therein,
honoring the P4GF_ENV configuration.

""")

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_repo_size")
TREE_SIZE   = 200
COMMIT_SIZE = 200
SHA1_SIZE   =  40
SPOOFED_SERVER_ID = 'temp-server-id'


def user_has_admin(p4):
    """Does this user have admin permissions somewhere?"""
    r = p4.run(['protects'])
    for rr in r:
        if rr['perm'] == 'admin' or rr['perm'] == 'super':
            return True
    return False


def _get_all_parent_paths(depotpath):
    """Return an array of each descending parent to this path.
    Assume the path end with a file - not a directory"""

    # Expect paths to start with '//' and so remove it.
    parents = []
    if depotpath.startswith('//'):
        # remove the leading '//depot/'
        parts = re.sub(r'//[^/]+/','',depotpath).split('/')
    else:
        parts = depotpath.split('/')
    for i in range(1,len(parts)):
        parents.append('/'.join(parts[:i]))
    return parents


class RepoSize():

    """"Estimate the git and P4 disk size of cloning a new repo."""

    def __init__(self, config_path, p4port, p4user, _locale, need_server_id):
        p4gf_const.READ_ONLY = True    # READ_ONLY will permit ctx to cleanup temporary clients
        p4gf_const.P4GF_USER = p4user
        self.config_path = config_path
        self.p4port = p4port
        self.p4user = p4user
        self.locale = _locale
        self.repo_name          = None
        self.repo_config        = None
        self.changes            = []
        self.git_data           = 0
        self.p4_data            = 0
        self.git_file_data      = 0    # use P4 digest to prevent increase from duplicated file data
        self.p4_file_data       = 0
        self.file_rev_count         = 0
        self.commit_count       = 0
        self.commit_data        = 0
        self.tree_count         = 0
        self.tree_data          = 0
        self.key_count          = 0
        self.key_data           = 0
        self.sql_db             = None
        self.tempdir            = None
        self.db_file_name       = None
        self.is_empty           = False
        if need_server_id:
            self.tempdir = p4gf_tempfile.new_temp_dir()
            p4gf_const.P4GF_HOME = self.tempdir
            server_id_path = p4gf_util.server_id_file_path()
            with open(server_id_path, 'w') as sip:
                sip.write(SPOOFED_SERVER_ID)
                sip.write('\n')

        self._create_database()
        self.ctx           = self.create_ctx()
        locale.setlocale(locale.LC_ALL, self.locale)

    def create_ctx(self):
        """
        Connect to Perforce using environment.
        """
        p4 = p4gf_create_p4.create_p4_temp_client(port=self.p4port, user=self.p4user)
        p4gf_branch.init_case_handling(p4)
        self.repo_name = 'estimate_repo_size_' + p4gf_util.uuid()
        self.repo_config = p4gf_config.RepoConfig.from_local_files(
                  self.repo_name, p4, self.config_path, None)
        ctx = p4gf_context.create_context(self.repo_name)
        ctx.p4gf = p4
        ctx.repo_config = self.repo_config
        ctx.config.p4user = self.p4user
        ctx.config.p4port = self.p4port
        ctx.connect()
        return ctx

    def _create_database(self):
        """Use sqlite3 maintain separate lists of unique MD5 and tree paths"""
        db_file = p4gf_tempfile.new_temp_file(prefix='repo_size_', suffix='.db', delete=False)
        self.sql_db = sqlite3.connect( database = db_file.name
                                  , isolation_level = "EXCLUSIVE" )
        self.db_file_name = db_file.name
        self.sql_db.execute("PRAGMA synchronous = OFF")
        self.sql_db.execute("CREATE TABLE md5  (key TEXT PRIMARY KEY)")
        self.sql_db.execute("CREATE TABLE tree (key TEXT PRIMARY KEY)")

    def _add_md5_if_not_present(self, md5):
        """Add the md5 key if not already present.
        Return True if added - else False."""
        cursor =  self.sql_db.execute("SELECT key FROM md5 WHERE key=?",(md5,))
        row = cursor.fetchone()
        if row is None:
            self.sql_db.execute("INSERT INTO md5 VALUES(?)",(md5,))
            return True
        else:
            return False

    def _add_tree_if_not_present(self, dirpath):
        """Add the dirpath key if not already present.
        Return True if added - else False."""
        cursor =  self.sql_db.execute("SELECT key FROM tree WHERE key=?",(dirpath,))
        row = cursor.fetchone()
        if row is None:
            self.sql_db.execute("INSERT INTO tree VALUES(?)",(dirpath,))
            return True
        else:
            return False

    def unlock_delete_temp_clients(self):
        """Unlock the temp clients so the user may consequently delete them."""
        pattern = p4gf_const.P4GF_REPO_TEMP_CLIENT.format(server_id=p4gf_util.get_server_id(),
                                                          repo_name=self.ctx.config.repo_name,
                                                          uuid='*')
        clients = [client['client'] for client
                   in self.ctx.p4gfrun('clients', '-e', pattern)]
        options = re.sub('locked', 'unlocked', p4gf_const.CLIENT_OPTIONS)
        for client in clients:
            p4gf_p4spec.ensure_spec_values(self.ctx.p4gf,
                                     spec_type='client',
                                     spec_id=client,
                                     values={ 'Options': options})
            self.ctx.p4gfrun('client', '-d', client)

    def cleanup(self):
        """Clean up the context and clientpool."""
        try:
            self.unlock_delete_temp_clients()
            self.ctx.disconnect()
            if self.tempdir:
                shutil.rmtree(self.tempdir)
            if self.db_file_name:
                os.unlink(self.db_file_name)
        except Exception as e:  # pylint: disable=W0703
            print(_('error on disconnect {exception}').format(exception=e))

    def estimate_size(self):
        """Perforce the repo size estimate."""
        if self.ctx.union_view_empty():
            self.is_empty = True
        else:
            self.get_changes()
            self.estimate_files()
        self.cleanup()

    def count_trees_in_path(self, depotpath):
        """Count the trees - list of descending dirs - to a path.
        Use a sqlite3 single table to de-dupe trees."""
        trees = _get_all_parent_paths(depotpath)
        for t in trees:
            if self._add_tree_if_not_present(t):
                self.tree_count += 1
                self.tree_data  += TREE_SIZE

    def estimate_files(self):
        """For each change, add to the commit, tree, key, file and size counts."""
        num_changes = len(self.changes)
        current     = 0
        for change_num in self.changes:
            sys.stdout.write(
                _("\r ...  {num_revs:12} file revisions in "
                  "{current_change:12} of {num_changes:12} changelists")
                .format(num_revs=self.file_rev_count,
                        current_change=current,
                        num_changes=num_changes))
            current += 1
            r = self.ctx.p4run('describe', '-s', change_num)
            self.commit_count += 1
            self.commit_data  += COMMIT_SIZE
            # Keys: one each of these per commit
            # git-fusion-index-branch-{repo_name},{change_num},{branch_id}'
            # git-fusion-{repo_name}-rev-sha1-{change_num}
            self.key_count += 2
            self.key_data +=  SHA1_SIZE  # size of git-fusion-index-branch... value
            for rr in r:
                # size and digest may be 'None'.
                # A changelist of only deletes will have no digest/fileSize keys
                paths = rr['depotFile']
                if 'fileSize' in rr:
                    sizes       = rr['fileSize']
                else:
                    sizes       = [None] * len(paths)
                if 'digest' in rr:
                    digests     = rr['digest']
                else:
                    digests     = [None] * len(paths)
                for p, s, d in zip(paths, sizes, digests):
                    if s:
                        self.p4_file_data += int(s)
                    if d and s and  self._add_md5_if_not_present(d):
                        self.git_file_data += int(s)
                    self.file_rev_count += 1
                    self.count_trees_in_path(p)
                    # git-fusion-{repo_name}-rev-sha1-{change_num}
                    # each file path and its sha1 is added to this key value
                    # accumulate size of this key value
                    self.key_data += len(p) + SHA1_SIZE

        self.git_data  = self.commit_data + self.tree_data + self.git_file_data
        self.p4_data   = self.commit_data + self.tree_data + self.key_data
        sys.stdout.write('\r {0:80}'.format(' '))  # clean the progress line

    def get_changes(self):
        """Get the a list of all changes from all branches."""
        for b in self.ctx.branch_dict().values():
            with self.ctx.switched_to_branch(b):
                path_range = self.ctx.client_view_path() + '@1,#head'
                r = self.ctx.p4run('changes', path_range)
                for rr in r:
                    self.changes.append(rr['change'])

    def report(self):
        """Print the results."""
        if self.is_empty:
            print(_("Repo views in {path} are empty.".format(path=self.config_path)))
        else:
            print('\n'.join([
                _('\nRepo config: {path}').format(
                    path=self.config_path),
                _('File revision count:               {rev_count:>20}').format(
                    rev_count=locale.format("%d", self.file_rev_count, grouping=True)),
                _('\ngit clone size:'),
                _('  Commit count:                      {commit_count:>20}').format(
                    commit_count=locale.format("%d", self.commit_count, grouping=True)),
                _('  Commit objects size*:              {commit_data:>20}').format(
                    commit_data=locale.format("%d", self.commit_data, grouping=True)),
                _('  Tree count:                        {tree_count:>20}').format(
                    tree_count=locale.format("%d", self.tree_count, grouping=True)),
                _('  Tree objects size*:                {tree_data:>20}').format(
                    tree_data=locale.format("%d", self.tree_data, grouping=True)),
                _('  Git total uncompressed file data:  {git_file_data:>20}').format(
                    git_file_data=locale.format("%d", self.git_file_data, grouping=True)),
                _('  Git total data:                    {git_data:>20}').format(
                    git_data=locale.format("%d", self.git_data, grouping=True)),
                _('\nP4 data added:'),
                _('  Commit count:                      {commit_count:>20}').format(
                    commit_count=locale.format("%d", self.commit_count, grouping=True)),
                _('  Commit objects size*:              {commit_data:>20}').format(
                    commit_data=locale.format("%d", self.commit_data, grouping=True)),
                _('  Tree count:                        {tree_count:>20}').format(
                    tree_count=locale.format("%d", self.tree_count, grouping=True)),
                _('  Tree objects size*:                {tree_data:>20}').format(
                    tree_data=locale.format("%d", self.tree_data, grouping=True)),
                _('  P4 key count:                      {key_count:>20}').format(
                    key_count=locale.format("%d", self.key_count, grouping=True)),
                _('  P4 key data:                       {key_data:>20}').format(
                    key_data=locale.format("%d", self.key_data, grouping=True)),
                _('  P4 total data:                     {p4_data:>20}').format(
                    p4_data=locale.format("%d", self.p4_data, grouping=True)),
                _('                                           '
                  '* using average commit size = {commit_size}').format(commit_size=COMMIT_SIZE),
                _('                                           '
                  '* using average tree  size = {tree_size}').format(tree_size=TREE_SIZE)
                ]))


def main():
    """Parse the command-line arguments and report on locks."""
    desc = _(DESCRIPTION)
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument(NTR('--config'),  metavar=NTR('config'),
        help=_('Path to Git Fusion p4gf_config file (required)'), required=True)
    parser.add_argument('-u', '--p4user', metavar='p4user',
        help=_('Perforce user'))
    parser.add_argument('-p', '--p4port', metavar='p4port',
        help=_('Perforce server'))
    parser.add_argument('--locale', metavar='locale', default='en_US.UTF-8',
        help=_('system locale setting'))
    args = parser.parse_args()

    need_serverid = False
    try:
        p4gf_util.get_server_id()
    except:  # pylint: disable=W0702
        need_serverid = True

    # If connect args not passed, check that the environment is set.
    if not args.p4port:
        if 'P4PORT' not in os.environ and 'P4GF_ENV' not in os.environ:
            print(
              _("Neither --p4port is an argument nor are P4GF_ENV and P4PORT in the environment."))
            sys.exit(0)
        if 'P4PORT' in os.environ:
            args.p4port = os.environ['P4PORT']
    else:
        # Set the requested port for Git Fusion's environment
        os.environ['P4PORT'] = args.p4port

    if not args.p4user:
        if 'P4USER' not in os.environ:
            print(_("Neither --p4user is an argument nor is P4USER in the environment."))
            sys.exit(0)
        else:
            args.p4user = os.environ['P4USER']
    else:
        # Set the requested user for Git Fusion's environment
        os.environ['P4USER'] = args.p4user

    repo_size = RepoSize(args.config, args.p4port, args.p4user, args.locale, need_serverid)
    repo_size.estimate_size()
    repo_size.report()


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

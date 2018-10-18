#! /usr/bin/env python3.3
"""FastImport class."""

import datetime
import gzip
import logging
import os
import re
from   subprocess import CalledProcessError

import pytz

from   p4gf_branch    import Branch
import p4gf_const
from   p4gf_desc_info import DescInfo
import p4gf_p4key     as     P4Key
from   p4gf_l10n      import _, NTR
from   p4gf_lfs_file_spec import LFSFileSpec
import p4gf_path_convert
import p4gf_proc
from   p4gf_profiler  import Timer
import p4gf_tempfile
import p4gf_usermap
import p4gf_util

LOG = logging.getLogger(__name__)
LOG_SCRIPT = LOG.getChild("script")

OVERALL      = NTR('FastImport Overall')
BUILD        = NTR('Build')
RUN          = NTR('Run')
SCRIPT_LINES = NTR('Script length')
SCRIPT_BYTES = NTR('Script size')


def _log_crash_report(errmsg):
    """Capture the fast-import crash report to a separate log file.

    :type errmsg: str
    :param errmsg: error message to write to Git Fusion log.

    """
    log = logging.getLogger('failures')
    if not log.isEnabledFor(logging.ERROR):
        return

    log.error(errmsg)

    # For each crash report we find, dump its contents.
    # In theory we clean up after a crash so there should be only one.
    cwd = os.getcwd()
    for entry in os.listdir('.git'):
        if entry.startswith('fast_import_crash_'):
            report_path = os.path.join(cwd, '.git', entry)
            date = datetime.datetime.now()
            date_str = date.isoformat().replace(':', '').split('.')[0]
            log_path = p4gf_const.P4GF_FAILURE_LOG.format(
                P4GF_DIR=p4gf_const.P4GF_HOME, prefix='git-fast-import-', date=date_str)
            p4gf_util.ensure_parent_dir(log_path)
            # Compress the file to help in preserving its integrity.
            gz_path = log_path + '.gz'
            log.error('Compressing fast-import crash report to {}'.format(gz_path))
            with open(report_path, 'rb') as fin, gzip.open(gz_path, 'wb') as fout:
                while True:
                    b = fin.read(100 * 1024)
                    if not len(b):
                        break
                    fout.write(b)
                fout.flush()


class FastImport:

    """Create a git-fast-import script and use it to import from Perforce.

    Steps to use FastImport:

    1) For each Perforce changelist to import:
            Call add_commit() with changelist and files.
            This adds everything necessary for one commit to the fast-import
            script.  The mark for the commit will be the change number.
    2) Call run_fast_import() to run git-fast-import with the script produced
       by steps 1-4.
    3) 'git checkout' or 'git branch -f' to put HEAD and branch refs where
       you want them.
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.script = p4gf_tempfile.new_temp_file(prefix='fastimport-')
        self.timezone = ctx.timezone
        self.__tzname = None
        self.project_root_path_length = len(ctx.contentlocalroot)
        self._line_count = 0
        self._byte_count = 0
        self.username_map = dict()
        self.usermap = p4gf_usermap.UserMap(ctx.p4gf, ctx.email_case_sensitivity)
        self.lfs_files = {}
        self.text_pointers = []

    def __get_timezone_offset(self, timestamp):
        """
        Determine the time zone offset for the given timestamp, using the.

        time zone name set in the P4GF time zone p4key. For example, with
        time zone 'US/Pacific' and timestamp 1371663968, the offset returned
        will be the string '-0700'.
        """
        try:
            ts = int(timestamp)
        except ValueError:
            LOG.error("__get_timezone_offset() given non-numeric input {}".format(timestamp))
        if self.__tzname is None:
            value = P4Key.get(self.ctx.p4, p4gf_const.P4GF_P4KEY_TIME_ZONE_NAME)
            if value == '0' or value is None:
                # Upgrade from an EA system, perhaps, in which the upgrade p4keys
                # have been set but the later changes where not applied during init.
                LOG.warning("p4key '{}' not set, using UTC as default."
                            " Change this to your Perforce server's time zone."
                            .format(p4gf_const.P4GF_P4KEY_TIME_ZONE_NAME))
                value = NTR('UTC')
            self.__tzname = value
        try:
            mytz = pytz.timezone(self.__tzname)
        except pytz.exceptions.UnknownTimeZoneError:
            LOG.warning("Time zone '{}' not found, using UTC as default".format(self.__tzname))
            mytz = pytz.utc
        LOG.debug("__get_timezone_offset({}) with {}".format(ts, mytz))
        dt = datetime.datetime.fromtimestamp(ts, tz=pytz.utc)
        ct = dt.astimezone(mytz)
        return ct.strftime('%z')

    def __append(self, data):
        """Append data to script."""
        if type(data) == str:
            data = data.encode()
        self.script.write(data)
        self._byte_count += len(data)
        self._line_count += data.count(b'\n')
        if LOG_SCRIPT.isEnabledFor(logging.DEBUG):
            l = str(data.decode().strip()).splitlines()
            for ll in l:
                LOG_SCRIPT.debug(ll)

    def __add_data(self, string):
        """Append a string to fast-import script, git style."""
        encoded = string.encode()
        header = NTR('data {}\n').format(len(encoded))
        self.__append(header.encode() + encoded)

    @staticmethod
    def escape_quotes(gwt_path):
        """git-fast-import dies on paths with quotes in surprising places:
            "dir/path".png
        Escape 'em, then wrap the whole thing in quotes:
            \"dir/path\".png
        """
        if gwt_path and '"' in gwt_path:
            return '"' + gwt_path.replace('"', '\\"') + '"'
        else:
            return gwt_path

    def _gwt_path(self, p4file):
        """Return local path of p4file, relative to view root."""
        if p4file.gwt_path:
            return p4file.gwt_path
        return self.ctx.depot_path(p4file.depot_path).to_gwt()

    def __add_files(self, branch, snapshot, cl):
        """Write files in snapshot to fast-import script.

        snapshot = list of P4File, usually from P4Changelist.files.
        """
                # Uniquen: it's rare, but snapshot can contain duplicate
                # p4file elements if we printed a changelist multiple times
                # due to it overlapping multiple branches. git-fast-import
                # silently ignores the duplicates, but humans reading the
                # debug log do not, and they're... distracting.
        p4file_list = []
        seen = set()
        for p4file in snapshot:
            if p4file.depot_path not in seen:
                seen.add(p4file.depot_path)
                p4file_list.append(p4file)

                # Why partition()?
                # Don't delete a parent after adding its child:
                # M 100644 deba01f cookbooks/apt/README
                # D cookbooks/apt   <== BUG, would also delete/omit README

        partitioned = p4gf_util.partition(lambda x: x.is_delete(), p4file_list)
        for p4file in partitioned:
            gwt_path = self._gwt_path(p4file)
            escaped_gwt_path = self.escape_quotes(gwt_path)
            if not gwt_path:
                continue
            if gwt_path == p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER:
                # Perforce-only artifact. Never copy this into Git.
                continue
            if p4file.is_delete():
                self.__append("D {0}\n".format(escaped_gwt_path))
            else:
                if p4file.sha1 == "":
                    LOG.debug("skipping missing revision {}#{}".format(gwt_path, p4file.revision))
                    continue
                # For LFS tracked files, compute the sha256 for the large file
                # and use it to create a text pointer file.  Hash that into the
                # repo and use its sha1 to replace the sha1 in the p4file.
                if (self.ctx.is_lfs_enabled and
                        self.ctx.lfs_tracker.is_tracked_p4(branch=branch,
                                                           p4change=cl,
                                                           gwt_path=gwt_path)):
                    pointer = LFSFileSpec.for_blob(self.ctx, p4file.sha1)
                    p4file.sha1 = str(self.ctx.repo.create_blob(pointer.to_text_pointer()))
                    self.text_pointers.append(p4file.sha1)
                    self.lfs_files[pointer.oid] = p4file.depot_path
                if p4file.is_x_type():
                    mode = "100755"
                elif p4file.is_symlink():
                    mode = "120000"
                else:
                    mode = "100644"
                self.__append("M {0} {1} {2}\n".
                              format(mode, p4file.sha1, escaped_gwt_path))

    def __add_gitlinks_d(self, gitlinks):
        """Write the submodule/gitlink "D"elete entries from the given list.

        You must Delete submodules BEFORE adding any normal file paths
        "under" them. Otherwise the "D" of a submodule will incorrectly
        prevent conversion from a submodule to a directory of files.
        """
        for sha1, path in gitlinks:
            if sha1 == p4gf_const.NULL_COMMIT_SHA1:
                self.__append("D {}\n".format(path))

    def __add_gitlinks_m(self, gitlinks):
        """Write the submodule/gitlink add-or-"M"odify entries from the
        given list.
        """
        for sha1, path in gitlinks:
            if sha1 != p4gf_const.NULL_COMMIT_SHA1:
                self.__append("M 160000 {0} {1}\n".format(sha1, path))

    def __branch_from(self
                     , destination_branch
                     , destination_p4cl
                     , first_branch_from_branch_id
                     , first_branch_from_change_number):
        """This is the first commit on a new branch, rearrange files from
        parent branches to match the new branch's view mapping.

        branch_source_dict: dict branch_id ==> int(changelist number)

        destination_p4cl must be the first changelist on this branch.

        Side effect: switches client view to parent branch view,
                     then back to destination branch view.
        """
        # Need a complete list of every depot file in the child/dest branch
        # after destination_p4cl change brings this branch into existence.
        # Does not include inherited files due to lightweight branching.
        child_map = destination_branch.view_p4map
        child_w2d = {child_map.translate(f.depot_path): f.depot_path
                     for f in destination_p4cl.files}

        # Need a complete list of every file in the parent/source branch view at
        # source changelist, and the depot file that landed there. Includes
        # inherited files due to lightweight branching.
        #
        # From this list we 'D'elete files that are not mapped in the
        # destination branch's view.
                # +++ Branch.p4_files() inserts 'clientFile' path so we don't
                # +++ have to deal with "inherited from some ancestor branch"
                # +++ mapping.
        source_branch   = self.ctx.branch_dict().get(first_branch_from_branch_id)
        source_p4_files = source_branch.p4_files( self.ctx
                                                , first_branch_from_change_number)
        source_p4_files = Branch.strip_conflicting_p4_files(source_p4_files)

        if destination_branch.is_lightweight:
            # First commit on a lightweight branch? Then delete only files
            # unmapped by the view switch (which should be none, since
            # lightweight branches copy their parent view.)
            child_c2d_map = child_map.reverse()
            for source_p4_file in source_p4_files:
                        # Parent Workspace File, in client syntax.
                pwf_client_path = source_p4_file['clientFile']
                if not child_c2d_map.translate(pwf_client_path):
                    gwt_path = self.ctx.client_path(pwf_client_path).to_gwt()
                    self.__append("D {0}\n".format(gwt_path))
                    LOG.debug("__branch_from lt deleted path={}".format(gwt_path))
                    continue
        else:
            # First commit on a fully populated branch? Then 'D'elete all files
            # not explicitly listed in the branch changelist.
            for source_p4_file in source_p4_files:
                pwf = source_p4_file['clientFile']  # Parent Workspace File
                pwf = p4gf_path_convert.set_client_path_client(pwf, self.ctx.p4.client)
                pdf = source_p4_file['depotFile']   # Parent Depot     File
                cdf = child_w2d.get(pwf)            # Child  Depot     File

                LOG.debug("__branch_from fp pwf={:<30} pdf={:<40} cdf={:<40}"
                          .format(str(pwf), str(pdf), str(cdf)))

                # work_tree holds the same depot_file in both parent and child
                # branch views? Let it carry forward from parent. NOP.
                if pdf == cdf:
                    continue

                # work_tree holds no file in child where parent had a file?
                # Delete it.
                if not cdf:
                    path = self.ctx.client_path(pwf).to_gwt()
                    self.__append("D {0}\n".format(path))
                    LOG.debug("__branch_from fp deleted path={}".format(path))
                    continue

                # Other cases where parent holds different or no file, but child
                # holds a file? Nothing to do here. __add_files() will catch
                # those.

    def __email_for_user(self, username):
        """Get email address for a user."""
        user_3tuple = self.usermap.lookup_by_p4user(username)
        if not user_3tuple:
            return _('Unknown Perforce User <{}>').format(username)
        return "<{0}>".format(user_3tuple[p4gf_usermap.TUPLE_INDEX_EMAIL])

    def __full_name_for_user(self, username):
        """Get human's first/last name for a user."""
        # First check our cache of previous hits.
        if username in self.username_map:
            return self.username_map[username]

        # Fall back to p4gf_usermap, p4 users.
        user_3tuple = self.usermap.lookup_by_p4user(username)
        if user_3tuple:
            user = p4gf_usermap.tuple_to_P4User(user_3tuple)
        else:
            user = None
        fullname = ''
        if user:
            # remove extraneous whitespace for consistency with Git
            fullname = ' '.join(user.full_name.split())
        self.username_map[username] = fullname
        return fullname

    def _add_parent(self, parent_commit, keyword=NTR('from')):
        """Add one parent to the commit we're currently building."""
        # Parent is either SHA1 of an existing commit or mark of a commit
        # created earlier in this import operation. Assume a length of
        # 40 indicates the former and mark ids will always be shorter.
        if isinstance(parent_commit, str) and len(parent_commit) == 40:
            self.__append(NTR('{keyword} {sha1}\n').format(keyword=keyword, sha1=parent_commit))
        else:
            self.__append(NTR('{keyword} :{mark}\n').format(keyword=keyword, mark=parent_commit))

    def _add_commit_parent_list(self, parent_commit_list):
        """Add the one 'parent' or multiple 'merge' commands for a commit currently
        being written to the fast-import script.
        """
        # If this is not the initial commit, say what it's based on
        # otherwise start with a clean slate
        LOG.debug("add_commit(): parent_commit={}".format(parent_commit_list))
        has_parent = False
        if parent_commit_list:
            if isinstance(parent_commit_list, list):
                for p in parent_commit_list:
                    if has_parent:
                        self._add_parent(p, NTR('merge'))
                    else:
                        self._add_parent(p)
                        has_parent = True
            else:
                self._add_parent(parent_commit_list)
                has_parent = True
        if not has_parent:
            self.__append(NTR('deleteall\n'))

    def add_commit( self
                  , cl
                  , p4file_list
                  , mark_number
                  , parent_commit_list
                  , first_branch_from_branch_id
                  , first_branch_from_change_number
                  , dest_branch
                  , branch_name
                  , deleteall
                  , is_git_orphan):
        """Add a commit to the fast-import script.

        Arguments:
        cl            -- P4Changelist to turn into a commit
        p4file_list   -- [] of P4File containing files in changelist
                         Often is cl.files, but not when git-first-parent
                         isn't the previous changelist on current branch.
        mark_number   -- Mark number assigned to this commit
        parent_commit_list
                      -- Mark or SHA1 of commit this commit will be based on.
                         Can be a singular str mark/SHA1 or a list of
                         [mark/SHA1 str] if cl should be a merge commit .
        first_branch_from_branch_id
        first_branch_from_change_number
                      -- branch_id and integer changelist number from which we're branching.
                         None unless this is the first commit on a new branch.
        dest_branch   -- Branch that receives this commit.
        """
        # pylint: disable=too-many-arguments, too-many-branches
        # Yeah I know add_commit() is a tad complex. Breaking it into single-use
        # pieces just scatters the complexity across multiple functions, making
        # things less readable. Shut up, pylint.

        with Timer(OVERALL):
            with Timer(BUILD):
                if is_git_orphan:
                    self.__append(NTR('reset refs/heads/{0}\n').format(branch_name))
                self.__append(NTR('commit refs/heads/{0}\n').format(branch_name))
                self.__append(NTR('mark :{0}\n').format(mark_number))
                desc_info = DescInfo.from_text(cl.description)
                committer_added = False
                if desc_info:
                    for key in ('author', 'committer'):
                        v = desc_info[key]
                        if v:
                            self.__append(NTR('{key} {fullname} {email} {time} {timezone}\n').
                                          format( key      = key
                                                , fullname = v['fullname']
                                                , email    = v['email'   ]
                                                , time     = v['time'    ]
                                                , timezone = _clean_timezone(v['timezone'])))
                            committer_added = True
                    desc = desc_info.clean_desc
                else:
                    desc = cl.description
                # If configured (default is 'yes')
                #     Add 'Copied from Perforce' to commit messages
                if self.ctx.add_copied_from_perforce:
                    desc = _append_copied_from_perforce(desc, cl.change)
                if self.ctx.git_p4_emulation:
                    desc = _append_git_p4_emulation(
                              description = desc
                            , change_num  = cl.change
                            , branch      = dest_branch )

                # Convoluted logic gates but avoids duplicating code. The point
                # is that we add the best possible committer data _before_
                # adding the description.
                if not committer_added:
                    if desc_info:
                        # old change description that lacked detailed author info,
                        # deserves a warning, but otherwise push onward even if the
                        # commit checksums will likely differ from the originals
                        LOG.warning('commit description did not match committer regex: @{} => {}'.
                                 format(cl.change, desc_info.suffix))
                    timezone = self.__get_timezone_offset(cl.time)
                    self.__append(NTR('committer {fullname} {email} {time} {timezone}\n').
                                  format(fullname=self.__full_name_for_user(cl.user),
                                         email=self.__email_for_user(cl.user),
                                         time=cl.time,
                                         timezone=timezone))

                self.__add_data(desc)

                self._add_commit_parent_list(parent_commit_list)
                if deleteall:
                    self.__append(NTR('deleteall\n'))

                if      first_branch_from_branch_id \
                    and first_branch_from_change_number:
                    self.__branch_from( dest_branch
                                      , cl
                                      , first_branch_from_branch_id
                                      , first_branch_from_change_number)
                if desc_info and desc_info.gitlinks:
                    self.__add_gitlinks_d(desc_info.gitlinks)
                if self.ctx.is_lfs_enabled:
                    self.ctx.lfs_tracker.add_cl(branch=dest_branch, p4change=cl)
                self.__add_files(dest_branch, p4file_list, cl)
                if desc_info and desc_info.gitlinks:
                    self.__add_gitlinks_m(desc_info.gitlinks)

    def run_fast_import(self):
        """Run git-fast-import to create the git commits.

        Returns: a list of commits.  Each line is formatted as
            a change number followed by the SHA1 of the commit.

        The returned list is also written to a file called marks.
        """
        with Timer(OVERALL):
            with Timer(RUN):
                LOG.debug("running git fast-import")
                # tell git-fast-import to export marks to a temp file
                self.script.flush()
                marks_file = p4gf_tempfile.new_temp_file(prefix='marks-')
                try:
                    cmd = ['git', 'fast-import', '--quiet', '--export-marks=' + marks_file.name]
                    ec = p4gf_proc.wait(cmd, stdin=self.script.name)
                    if ec:
                        _log_crash_report('git-fast-import failed for {}'.format(
                            self.ctx.config.repo_name))
                        raise CalledProcessError(ec, NTR('git fast-import'))

                    # read the exported marks from file and return result
                    with open(marks_file.name, "r") as marksfile:
                        marks = [l.strip() for l in marksfile.readlines()]
                    if LOG.getChild('marks').isEnabledFor(logging.DEBUG3):
                        LOG.getChild('marks').debug3('git-fast-import returned marks ct={}\n'
                                                     .format(len(marks))
                                                     + '\n'.join(marks))
                    return marks
                finally:
                    self.script.close()
                    marks_file.close()

    def cleanup(self):
        """Ensure temporary files are closed so they may be deleted."""
        self.script.close()

    def __repr__(self):
        return "\n".join([repr(self.ctx),
                          "timezone                : " + self.timezone,
                          "project_root_path_length: " + str(self.project_root_path_length),
                          "script length           : {} lines".format(self._line_count),
                          "script size             : {} bytes".format(self._byte_count)
                          ])

    def __str__(self):
        return "\n".join([str(self.ctx),
                          "timezone                : " + self.timezone,
                          "project_root_path_length: " + str(self.project_root_path_length),
                          "script length           : {} lines".format(self._line_count),
                          "script size             : {} bytes".format(self._byte_count)
                          ])


def _clean_timezone(tz):
    """Ensure timezone offset is legal for git-fast-import.

    There have been cases seen where a commit contains an invalid timezone offset.
    If this is allowed to pass through to git-fast-import it will cause a crash.

    To completely deal with this would require patching up the offending commit(s)
    immediately after copying them into git.  This would require a large coding
    effort and/or a significant performance hit.

    So we take the easier way out and just replace the offending timezone offsets
    with something git-fast-import will accept.  The cost of this approach is
    that we will not be able to re-repo any more.  The altered timezone offsets
    will lead to altered commit sha1s.
    """
    # do nothing with valid timezone offset
    regex = re.compile(r'^([-+])(\d){4}$')
    if regex.search(tz):
        LOG.debug('tz ok {}'.format(tz))
        return tz

    # The only observed instance so far of an invalid timezone offset is '+051800'
    # which is probably caused by a failed decoding of 'IST'.
    # 'IST' can mean any of:
    #  'Irish Summer Time'      +0100
    #  'Israeli Standard Time'  +0200
    #  'Iran Standard Time'     +0330
    #  'Indian Standard Time'   +0530
    # So faced with this ambiguity, apparently some have decided to use +051800
    # to avoid making an incorrect selection from those options.
    #
    # We could replace an invalid offset with +0000 and treat the time as UTC.
    # But it's nice to be able to identify this as the source of a rerepo failure
    # using repo_compare.py.  Since repo_compare.py uses pygit2, which silently
    # transforms invalid timezone offsets to 0, it's more convenient to use some
    # other non-zero offset.
    #
    # By using +0001 git-fast-import is happy and yet it is still a more or less
    # invalid timezone offset, rather than the wrong valid timezone offset.
    cleaned = '+0001'
    LOG.warning("replacing commit's invalid timezone offset {} with {}".format(tz, cleaned))
    return cleaned


def _choose_eol(description):
    """Return either \n or \r\n depending on what's already in the
    changelist description.
    """
    if '\r\n' in description:
        return '\r\n'
    return '\n'


def _append_lines(description, lines):
    """Append one or more lines to a changelist description, reusing
    whatever eol character(s) already used in the description.

    :param: lines a list of lines
    """
    assert isinstance(lines, list)

    eol = _choose_eol(description)
    desc = description

                        # Force a double-eol to separate this extra from the
                        # human-authored description text.
    if not description.endswith(eol):
        desc = desc + eol
    if not description.endswith(eol * 2):
        desc = desc + eol

    desc = desc + eol.join(lines) + eol
    return desc


def _append_copied_from_perforce(description, change_num):
    """Tell Git users which Perforce changelist originally contained
    this commit.
    """
    lines = [ p4gf_const.P4GF_EXPORT_HEADER
            , " {}: {}".format(p4gf_const.P4GF_DESC_KEY_CHANGE, change_num)
            # Ravenbrook local patch: Add ServerID so that people can tell *which* Perforce the
            # change came from.  A general patch will need to have the read ServerID to hand,
            # fetched from the Perforce server.  See <https://swarm.workshop.perforce.com/jobs/job000442>.
            , " {}: {}".format(p4gf_const.P4GF_DESC_KEY_SERVERID, "perforce.ravenbrook.com")
            ]
    return _append_lines(description, lines)


def _append_git_p4_emulation(description, change_num, branch):
    """Tell Git users which Perforce changelist and depot paths
    originally contained this commit.
    """

                        # git-p4 inserts a comma-delimited list of all "depot
                        # paths" in this branch. Where "depot path" is usually
                        # a single root directory in the branch definition, but
                        # could be as complex as a full Git Fusion branch view
                        # definition including overlay/+ and exclusion/-
                        # mapping lines, some ending in "/..." and some not.
                        #
                        # Strip off the trailing "..." like git-p4 does.
                        # Skip any exclusionary lines.
                        #
                        # Do NOT quote-wrap any space-carrying paths. The
                        # entire comma-delimited list of paths is quote-wrapped
                        # later. Yes, this implies that git-p4 could fail if
                        # there are any commas in the actual depot paths. Oh
                        # well.
    depot_paths = []
    for lhs in branch.view_p4map.lhs():
        if lhs.startswith("-"):
            continue
        elif lhs.startswith("+"):
            lhs = lhs[1:]
        if lhs.endswith("/..."):
            lhs = lhs[:-3]
        depot_paths.append(lhs)
                        # git-p4 sorts paths. Further evidence that it cannot
                        # handle overlay or exclusionary paths.
    depot_paths = sorted(depot_paths)

                        # git-p4 also writes "options: xxx", but we do not.
                        # Because we are not git-p4 and do not have git-p4
                        # options to write.
    fmt = "[git-p4: depot-paths = \"{depot_paths}\": change = {change_num}]"
    line = fmt.format( depot_paths = ",".join(depot_paths)
                     , change_num  = change_num )
    return _append_lines(description, [line])


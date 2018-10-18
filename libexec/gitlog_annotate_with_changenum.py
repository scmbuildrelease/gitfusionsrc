#! /usr/bin/env python3.3
"""Annotate a 'git log' output with Perforce changelist numbers and branch assignments.

<p4files.txt> must be the output from 'p4 files" that lists all the commit
ObjectType gitmirror objects for this repo:
$ p4 files //.git-fusion/objects/repos/{repo}/commits/... > p4files.txt

<gitlog> must be the output of some 'git log' command.
$ git log --graph --decorate --oneline --all > gitlog-all.txt

<p4changes.txt> is
'p4 changes -l //.git-fusion/branches/{repo}/... //depot/xxx/... //depot/yyy/...'
to capture all of the "push-state: complete" markers.
optional.

"""

import p4gf_env_config  # pylint:disable=unused-import
from   collections import defaultdict
import logging
import os
import re
import sys
import subprocess
from argparse import RawTextHelpFormatter

# If P4CONFIG is set in P4GF_ENV P4USER/P4PORT will be rejected.
# Remove this to avoid confusion if this is executed somewhere
# other than HOME of the Git Fusion system user.
if 'P4CONFIG' in os.environ:
    del os.environ['P4CONFIG']
from   p4gf_l10n             import _, NTR
import p4gf_log
import p4gf_proc
import p4gf_util
import p4gf_context
import p4gf_branch
import p4gf_tempfile
LOG = logging.getLogger("gitlog_annotate_stdout")
REPORT_FILE = "annotate_gitlog_{repo}.txt"
DEBUG = False


def annotate(args):
    """Use the files to produce the report."""
    logging.basicConfig(format="%(message)s")

    gitlogs_created = False
    p4 = ctx = None
    if args.repo:
        (p4, ctx) = create_context(args)
        sha1_to_tuple_list = read_p4files_from_depot_path(p4, args.repo)
        complete_change_nums = read_p4changes_from_depot_path(ctx)
        args.gitlogs = get_gitlogs_from_git(ctx.repo_dirs.GIT_DIR)
        ctx.disconnect()
        gitlogs_created = True
    else:
        sha1_to_tuple_list = read_p4files_from_file_path(args.p4files)
        complete_change_nums = set()
        if args.p4changes:
            complete_change_nums = read_p4changes_from_text_file(args.p4changes)

    translate_gitlog(args.gitlogs, sha1_to_tuple_list, complete_change_nums, args.stdout)
    if gitlogs_created and not DEBUG:
        os.remove(args.gitlogs)


def read_p4changes_from_text_file(p4changes_path):
    """Read 'p4 changes -l' text and scan for changlists with "push state: complete"."""
    complete_change_nums = set()

    re_change = re.compile(r'^Change (\d+) on')
    re_complete = re.compile(r'^\s+push-state: complete')

    with open(p4changes_path, 'r') as f:
        change_num = None
        for line in f.readlines():
            m = re_change.search(line)
            if m:
                change_num = m.group(1)
                continue
            if re_complete.search(line):
                complete_change_nums.add(change_num)
    return complete_change_nums


def read_p4changes_from_depot_path(ctx):
    """Read 'p4 changes -l' text and scan for changlists with "push state: complete"."""
    tmp_file = p4gf_tempfile.new_temp_file(mode='w', prefix='annotate-changes', delete=False)
    regex = re.compile(r'^', re.MULTILINE)

    for branch_chunk in ctx.iter_branch_chunks():

        with ctx.switched_to_union(branch_chunk):
            p4_result = ctx.p4run('changes', '-l', '//{}/...'.format(ctx.p4.client),
                                  log_warnings=logging.WARN)
            for rr in p4_result:
                tmp_file.write("Change {} on \n".format(rr['change']))
                desc = re.sub(regex, '    ', rr['desc'])
                tmp_file.write("{}\n".format(desc))
    tmp_file.close()
    complete_change_nums = read_p4changes_from_text_file(tmp_file.name)
    if DEBUG:
        print(_("p4 changes data for this repo written to: '{}'.").format(tmp_file.name))
    else:
        os.remove(tmp_file.name)
    return complete_change_nums


def get_gitlogs_from_git(git_dir):
    """Create file with git log output.

    git log --graph --decorate --oneline --all

    """
    tmp_file = p4gf_tempfile.new_temp_file(mode='w', prefix='annotate-gitlog', delete=False)
    if DEBUG:
        print(_("get_gitlogs_from_git: gitdir='{}'.").format(git_dir))
    if not os.path.exists(git_dir):
        LOG.warning("Git repository {} missing stopping.".format(git_dir))
        print(_("Git repository {} missing stopping.").format(git_dir))
        sys.exit(1)
        # it's not the end of the world if the git repo disappears, just recreate it
    else:
        # Ensure the pack config settings are set to desired values.
        git_dir = '--git-dir={}'.format(git_dir)
        cmd = ['git', git_dir, 'log', '--graph',  '--decorate', '--oneline', '--all']
        subprocess.call(cmd, stdout=tmp_file)
    tmp_file.close()
    if DEBUG:
        print(_("git logs data for this repo written to: '{}'.").format(tmp_file.name))
    return tmp_file.name


def _change_num_complete(change_num, complete_change_nums):
    """Append asterisk to "push-state: complete" changelist numbers."""
    if change_num in complete_change_nums:
        return "@{}*".format(change_num)
    else:
        return "@{}".format(change_num)


def translate_gitlog(gitlog_path, sha1_to_tuple_list, complete_change_nums, stdout=False):
    """Read each line of gitlog and print it with " @{change_num} {branch}  "
    inserted after the first sha1."""
    global REPORT_FILE
    if stdout:
        fd = sys.stdout
        REPORT_FILE = 'stdout'  # For reporting message
    else:
        if os.path.exists(REPORT_FILE):
            try:
                prev_report = REPORT_FILE + '.prev'
                os.rename(REPORT_FILE, prev_report)
                print(_("Renamed previous report '{}' to '{}'.").format(
                    os.path.abspath(REPORT_FILE), os.path.abspath(prev_report)))
            except IOError:
                print(_("Could not rename previous report '{}' to '{}'.").format(
                    os.path.abspath(REPORT_FILE), os.path.abspath(prev_report)))
        fd = open(REPORT_FILE, 'w')
        print(_("Report written to '{}'.").format(os.path.abspath(REPORT_FILE)))

    r1 = re.compile(r'([0-9a-f]{7,40}) ')   # Why doesn't [ $] work here?
    r2 = re.compile(r'([0-9a-f]{7,40})$')
    insert_format = "{change_num:<7} {branch_id:<20}"
    with open(gitlog_path, 'r') as f:
        for line in f.readlines():
            line = line.rstrip()
                        # Our internal case portal tends to append <br />
                        # on each line.
            if line.endswith("<br />"):
                line = line[:-(len("<br />"))]
            m = r1.search(line)
            if not m:
                m = r2.search(line)
            if m:
                key = m.group(1)
                if key in sha1_to_tuple_list:
                    l = sha1_to_tuple_list[key]
                    insert = "/".join(
                        [ insert_format.format(
                                  branch_id=tup[1]
                                , change_num=_change_num_complete(
                                        tup[2], complete_change_nums))
                          for tup in l])
                else:
                    insert = insert_format.format(branch_id=""
                                                , change_num="")

                print("{left} {insert} {right}"
                      .format(  left       = line[:m.end()]
                              , insert     = insert
                              , right      = line[m.end():].rstrip()
                              ), file=fd)
                continue
            print(line, file=fd)


def read_p4files_from_depot_path(p4, repo):
    """Write the depot path for the repo's commits to a temporary file.
    Then call read_p4files_from_file_path."""

    tmp_file = p4gf_tempfile.new_temp_file(mode='w', prefix='annotate-files', delete=False)

    commits_path = '//.git-fusion/objects/repos/{}/commits/...'.format(repo)
    p4_result = p4.run('files', '-e', commits_path)

    for rr in p4_result:
        depot_path = rr.get('depotFile')
        if not depot_path:
            continue
        tmp_file.write("{}\n".format(depot_path))

    tmp_file.close()
    d = read_p4files_from_file_path(tmp_file.name)
    if DEBUG:
        print(_("p4 files data for this repo written to: '{}'.").format(tmp_file.name))
    else:
        os.remove(tmp_file.name)
    return d


def read_p4files_from_file_path(p4file_path):
    """Return a dict of sha1 to list-of-3-tuple."""
    r = re.compile(r'.*commits/(..)/(..)/(.{36})-([^,]+),(\d+).*')
    d = defaultdict(list)
    with open(p4file_path, 'r') as f:
        for line in f.readlines():
            # print(line)
            m = r.search(line)
            if not m:
                break
            sha1       = m.group(1) + m.group(2) + m.group(3)
            branch_id  = m.group(4)
            change_num = m.group(5)
                        # Truncate GUID branch_ids
            if branch_id.endswith("=="):
                branch_id = branch_id[:7]

                        # Improve chances we'll match a 7- or 8-char
                        # abbreviated sha1. Associate abbreviated sha1
                        # keys with this tuple value. Trust 'git log'
                        # to not abbreviate down to ambiguously short sha1s.
                        #
            t = (sha1, branch_id, change_num)
            d[sha1    ].append(t)
            d[sha1[:7]].append(t)
            d[sha1[:8]].append(t)
            d[sha1[:9]].append(t)

            LOG.debug("'{}' {} {}".format(sha1, change_num, branch_id))
    return d


def parse_argv():
    """Convert command line into a usable dict."""
    usage = _("""
    Annotate a Git Fusion repo's 'git log' against its Helix changes.

    gitlog_annotate_with_changenum.py --repo REPO
    gitlog_annotate_with_changenum.py gitlogs.txt p4files.txt [p4changes.txt]
    """)
    desc = _("""


Annotate each git log line with the Helix changelist number (the asterisk marks a "push-state: complete" change):
    * ace5678  master               git commit description
  ==>
    * ace5678  @2323232* master               git commit description

The report file is written to PWD/{}.

The first syntax requires execution from the Git Fusion system user's home directory.
This is the recommended and simplest method, for in this case
all 'git log', 'p4 files' and 'p4 changes' data will be acquired.

The second syntax requires no access to the Git Fusion instance by providing all data in files as arguments.
The contents of the file data is described below.
The optional 'p4changes.txt' is used to identify "push-state: complete" markers.
            """).format(REPORT_FILE)
    # pylint: disable=line-too-long
    parser = p4gf_util.create_arg_parser(
          desc         = desc
        , usage        = usage
        , formatter_class = RawTextHelpFormatter
        )
    parser.add_argument('--repo', '-r', nargs=1,metavar=NTR('REPO'),
        help=_("repo name if fetching data from Helix."))
    parser.add_argument(NTR('gitlogs'),      metavar=NTR('gitlogs'), nargs='?',
        help=_("output from: 'git log --graph --decorate --oneline --all > gitlogs.txt'"))
    parser.add_argument(NTR('p4files'),      metavar=NTR('p4files'), nargs='?',
        help=_("output from: 'p4 files //.git-fusion/objects/repos/{repo}/commits/... > p4files.txt'"))
    parser.add_argument(NTR('p4changes'),      metavar=NTR('p4changes'), nargs='?',
        help=_("output from: 'p4 changes -l //.git-fusion/branches/{repo}/... //depot/xx/... //depot/yy/...'" + \
             "\n              (list of all branch views)"))
    parser.add_argument('--stdout', action=NTR("store_true"),
            help = _("direct report to stdout"))
    parser.add_argument('--debug', action=NTR("store_true"),
            help = _("display some debug information"))
    args = parser.parse_args()
    LOG.debug("args={}".format(args))
    return args


def verify_file_args_exist(args):
    """Check files from arguments exist."""
    for f in (args.p4files, args.gitlogs, args.p4changes):
        if f:
            if not os.path.exists(f):
                print(_("File does not exist: '{}'.").format(f))
                sys.exit(2)


def create_context(args):
    """Create a p4gf_context for accessing Git Fusion data."""
    ctx = p4gf_context.create_context(args.repo)
    ctx.connect_cli(LOG)
    p4gf_proc.init()
    p4gf_branch.init_case_handling(ctx.p4gf)
    if DEBUG:
        print(_("create_context: ctx.p4.port='{}'  ctx.p4.user='{}' .").format(
            ctx.p4.port, ctx.p4.user))
    return (ctx.p4gf, ctx)


def main():
    """Do the thing."""
    global DEBUG, REPORT_FILE
    args = parse_argv()
    if args.debug:
        print(_("args: {} .").format(args))
        DEBUG = args.debug

    # reduce the arg.repo array to a string
    if args.repo:
        args.repo = args.repo[0]

    # Require the first two files or --repo.
    if args.repo:
        REPORT_FILE = REPORT_FILE.format(repo=args.repo)
        if args.gitlogs:  # no positional args permitted
            print(_("You must provide either --repo <repo> OR the required positional arguments."))
            sys.exit(2)
    else:
        REPORT_FILE = REPORT_FILE.format(repo='from_files')
        if not (args.gitlogs and args.p4files):
            print(_("You must provide both gitlogs and p4files."))
            sys.exit(2)

    verify_file_args_exist(args)

    annotate(args)

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

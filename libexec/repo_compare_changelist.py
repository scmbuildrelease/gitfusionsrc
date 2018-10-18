#! /usr/bin/env python3.3

"""A subset of repo_compare_to_depot.py that compares a single
Perforce changelist against its corresponding Git commit.

Intended for huge histories that would spew gigabytes of useless noise
if run through repo_compare.py or repo_compare_to_depot.py

"""
import argparse
from   collections              import defaultdict
import logging
import os
import pygit2
import sys
import tempfile
import P4

import p4gf_branch
import p4gf_const
import p4gf_context
from   p4gf_filemode            import FileModeStr
from   p4gf_l10n                import NTR, _
import p4gf_log
from   p4gf_object_type         import ObjectType
from   p4gf_p2g_print_handler   import PrintHandler
import p4gf_p4filetype
from   p4gf_progress_reporter   import Determinate
import p4gf_util
import p4gf_version_3

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


LOG = logging.getLogger("p4gf_first_push_stdout")

class RepoCompareSingle:
    """Class that knows how to compare Perforce changelists
    against their Git commit counterparts."""
    def __init__(self, ctx, args):
        self.ctx             = ctx
        self.max_gwt_char_ct = 10
        self.line_format     = None
        self.how_ct          = defaultdict(int)

                        # Our Git repo that contains the Git
                        # half of what we compare.
        try:
            repo_dir = pygit2.discover_repository(args.git_dir)
        except KeyError:
            raise RuntimeError("Not a Git directory: {}".format(args.git_dir))
        #pylint: disable=protected-access
        repo_dir_abspath = os.path.abspath(repo_dir)
        ctx._repo = pygit2.Repository(repo_dir_abspath)

                        # A temporary repo for calculating blob sha1
                        # from Perforce file revisions.
        self.temp_repo_dir   = tempfile.TemporaryDirectory(
                                    prefix="p4gf_repo_cmp_")
        self.temp_repo       = pygit2.init_repository(self.temp_repo_dir.name)
        os.chdir(self.temp_repo_dir.name)

    def compare_change_num(self, change_num):
        """Compare one Perforce changelist with its corresponding Git
        commit.
        """
        ot = ObjectType.change_num_to_commit(self.ctx, change_num)
        if not ot:
            key_pattern = (p4gf_const.P4GF_P4KEY_INDEX_OT
                .format( repo_name  = self.ctx.config.repo_name
                       , change_num = change_num
                       , branch_id  = "*"))
            raise RuntimeError("No Git commit for @{change_num}."
                               "\nNo 'p4 keys -e {key_pattern}"
                               .format( change_num=change_num
                                      , key_pattern = key_pattern
                                      ))

        branch = self.ctx.branch_dict().get(ot.branch_id)
        if not branch:
            raise RuntimeError("No branch view defined for {}"
                               .format(ot.branch_id))
        self._compare_ot(ot)

    def _compare_ot(self, ot):
        """Compare a single changelist against a Git commit, using
        a specific branch_id.
        """
        gwt_to_p4file = self._p4_files(ot)
        gwt_to_git    = self._git_ls_tree(ot.sha1)

        gwt_superset = set(gwt_to_p4file.keys()) | set(gwt_to_git.keys())
        self.max_gwt_char_ct = max(len(gwt) for gwt in gwt_superset)

                        # Make the progress reporter more useful.
        with Determinate(len(gwt_to_p4file)):

            for gwt in sorted(gwt_superset):
                diff_how_set = self._compare_file(
                                       gwt    = gwt
                                     , p4file = gwt_to_p4file.get(gwt)
                                     , git    = gwt_to_git.get(gwt)
                                     )
                self._report(gwt, diff_how_set)

                for dh in diff_how_set:
                    self.how_ct[dh] += 1

    def _compare_file(self, gwt, p4file, git):
        """Compare a file's 'p4 files' and 'git-ls-tree' results.
        Return a set of mismatches.
        """
        if not p4file:
            return set([DiffHow.FILE_MISSING_P4])
        if not git:
            return set([DiffHow.FILE_MISSING_GIT])

        result = set()
        if not _mode_match( git_mode_str = git.mode
                          , p4filetype   = p4file['type']
                          , gwt_path     = gwt
                          ):
            result.add(DiffHow.FILE_MODE)

        if not self._sha1_match(p4file, git):
            result.add(DiffHow.FILE_SHA1)

        return result

    def _sha1_match(self, p4file, git):
        """Calculate a blob sha1 for a Perforce file revision and
        compare to what Git has for the same blob.
        """
        printhandler            = PrintHandler(self.ctx)
        printhandler.repo       = self.temp_repo
        printhandler.change_set = set()
        depot_file_rev = p4gf_util.to_path_rev( p4file['depotFile']
                                              , p4file['rev'])
        with p4gf_util.raw_encoding(self.ctx.p4)                \
                , self.ctx.p4.using_handler(printhandler) \
                , self.ctx.p4.at_exception_level(P4.P4.RAISE_ALL):
            self.ctx.p4run('print', '-k', depot_file_rev)
        printhandler.flush()
                        # PrintHandler.revs is a RevList, which
                        # lacks easy access to "just give me the one
                        # p4file I know you have." So iterate and break.
        p4_sha1 = None
        for p4file in printhandler.revs:
            p4_sha1 = p4file.sha1
            break
        if p4_sha1 is None:
            LOG.error("No revision 'p4 print'ed for {}"
                        .format(depot_file_rev))
            return False
        return p4_sha1 == git.sha1

    def _git_ls_tree(self, commit_sha1):
        """Run 'git ls-tree' to get a list of files in Git at a this commit.

        Returns a list of pygit2.TreeEntry objects.
        """
        gwt_to_git = {}
        for ls_tree in p4gf_util.git_ls_tree_r( self.ctx.repo
                                              , commit_sha1 ):
            LOG.debug3("Git: {gwt}    {ls}"
                       .format(gwt=ls_tree.gwt_path, ls=ls_tree))
                        # Directories never exist in Perforce.
            if ls_tree.mode == FileModeStr.DIRECTORY:
                continue
            gwt_to_git[ls_tree.gwt_path] = ls_tree
        return gwt_to_git

    def _p4_files(self, ot):
        """Run 'p4 files' to get a list of files in Perforce at this change
        on this branch view.
        """
        gwt_to_p4file = {}
        with self.ctx.switched_to_branch(self.ctx.branch_dict()[ot.branch_id]):
            p4files = self.ctx.p4run(
                   'files'
                  , '-e'
                  , self.ctx.client_view_path(ot.change_num)
                  )
            for p4file in p4files:
                if not isinstance(p4file, dict):
                    LOG.debug("Skipping non-dict response to 'p4 files':\n{}"
                              .format(p4file))
                    continue
                if "depotFile" not in p4file:
                    LOG.debug("Skipping non-depotFile dict response to 'p4 files':\n{}"
                              .format(p4file))
                    continue

                gwt_path   = self.ctx.depot_to_gwt_path(p4file['depotFile'])
                gwt_to_p4file[gwt_path] = p4file
                LOG.debug3("P4: {gwt}   {p4}".format(gwt=gwt_path, p4=p4file))
        return gwt_to_p4file

    def _line_format(self):
        """Return the one-line format string we use."""
        if not self.line_format:
            self.line_format = "{gwt:<" + str(self.max_gwt_char_ct) + "} {how}"
        return self.line_format

    def _report(self, gwt, diff_how_set):
        """Explain how Git and Perforce differ."""
        level = logging.ERROR

                        # No difference? Report only if logging at
                        # --verbose/INFO level or noisier.
        if not diff_how_set:
            level = logging.INFO
            diff_how_str = "="
        else:
            diff_how_str = " ".join(diff_how_set)#DiffHow.to_string(diff_how_set)
        LOG.log(level, self._line_format().format( gwt = gwt
                                                 , how = diff_how_str
                                                 ))

    def report_summary(self):
        """How many of each?"""
        labels = [dh for dh in DiffHow.ALL
                  if dh in self.how_ct]
        values = [self.how_ct[dh] for dh in labels]
        if not labels:
            LOG.info("No mismatches.")
            return
        lines = p4gf_util.tabular( labels     = labels
                                 , int_values = values )
        LOG.warning("\n".join(lines))


# end class RepoCompareSingle
# -- enum DiffHow ------------------------------------------------------------

class DiffHow(Enum):
    """How Perforce and Git can differ."""
    FILE_MISSING_GIT                    = 'file.missing.git'
    FILE_MISSING_P4                     = 'file.missing.p4'
    FILE_SHA1                           = 'file.sha1'
    FILE_MODE                           = 'file.mode'

    # Canonical order so things stay lined up
    ALL = [ FILE_MISSING_GIT
          , FILE_MISSING_P4
          , FILE_SHA1
          , FILE_MODE
          ]

    @staticmethod
    def to_string(diff_how_set):
        """Return string suitable for report."""
        l = [dh for dh in DiffHow.ALL
            if dh in diff_how_set]
        return " ".join(l)

# end enum DiffHow
# -- module-wide -------------------------------------------------------------

def _mode_match(git_mode_str, p4filetype, gwt_path):
    """Match up +x and symlink."""
    p4_base_mods     = p4gf_p4filetype.to_base_mods(p4filetype)
    p4_is_symlink    = p4_base_mods[0] == "symlink"
    p4_is_executable = "x" in p4_base_mods

    if git_mode_str == FileModeStr.PLAIN      :  # "100644"
        return not p4_is_executable and not p4_is_symlink

    if git_mode_str == FileModeStr.EXECUTABLE :  # "100755"
        return     p4_is_executable and not p4_is_symlink

    if git_mode_str == FileModeStr.SYMLINK    :  # "120000"
        return not p4_is_executable and     p4_is_symlink

    LOG.error("Unexpected Git file mode {} {}".format(git_mode_str, gwt_path))
    return False

# -- Begin 15.1 copypasta ----------------------------------------------------
#
# Copied and pasted from 15.1 code so that this script can stand alone in
# older installations.


def p4gf_util_create_arg_parser(
                       desc             = None
                     , *
                     , epilog           = None
                     , usage            = None
                     , help_custom      = None
                     , help_file        = None
                     , add_p4_args      = False
                     , add_log_args     = False
                     , add_debug_arg    = False
                     , add_profiler_arg = False
                     ):
    """Creates and returns an instance of ArgumentParser configured
    with the options common to all Git Fusion commands. The caller
    may further customize the parser prior to calling parse_args().

    Supply your own user-visible text:

    :param desc:    Description of the command being invoked,
                    appears at top of argparser-generated help string.
                    Not used if help_custom supplied.
    :param epilog:  More helpful text that appears at bottom of
                    argparser-generated help string.
                    Not used if help_custom supplied.
    :param usage:   Custom "usage" string that appears when incorrect options
                    specified. Also appears as part of argparser-generated
                    help text, unless help_custom specified.
    :param help_custom:
                    Custom help text. Displayed upon --help/-h.
                    If you're reading this from a text file, use help_file.
    :param help_file:
                    Name of text file with custom help.

    Add common parameters:
    Include common command-line arguments with common descriptions.

    :param add_p4_args:
        --p4port, -p    P4PORT of server.
        --p4user, -u    P4USER of user.
        See also apply_p4_args(args, p4).

    :param add_log_args:
        --verbose, -v   Write additional diagnostics to standard output.
        --quiet, -q     Write nothing but errors to standard output.
        See also apply_log_args(args, [logger]).
    :param add_debug_arg:
        --debug         Write debug diagnostics to standard output.
    :param add_profiler_arg:
        --profiler      Run with Python performance profiler.
        You must add your own calls to p4gf_profiler.start_cprofiler()
        and stop_cprofiler() if set.
    Not optional:
    Always includes, and handles,
        -h, --help      show this help message and exit
        -V              displays version information and exits
    """
    class VersionAction(argparse.Action):

        """Custom argparse action to display version to stdout.

        Stdout instead of stderr, which seems to be the default in argparse.
        """

        def __call__(self, parser, namespace, values, option_string=None):
            print(p4gf_version_3.as_string(include_checksum=True))
            sys.exit(0)

    class HelpAction(argparse.Action):

        """Dump help and exit."""

        def __call__(self, parser, namespace, values, option_string=None):
            if help_file:
                help_txt = p4gf_util.read_bin_file(help_file)
                print(help_txt)
            else:
                print(help_custom)
            sys.exit(0)

    # argparse wraps the description and epilog text by default, but
    # could customize using formatter_class
    parser = argparse.ArgumentParser(
          description = desc
        , epilog      = epilog
        , usage       = usage
        , add_help    = not (help_custom or help_file)
        )
    parser.add_argument("-V", action=VersionAction, nargs=0,
                        help=_('displays version information and exits'))
    # We normally get -h and --help for free: prints programmatically
    # generated help then exit. Bypass and supply our own help dumper if
    # custom help provided.
    if help_custom or help_file:
        parser.add_argument('-h', '--help'
                          , nargs   = 0
                          , action  = HelpAction)

    if add_p4_args:
        parser.add_argument('--p4port', '-p', metavar='P4PORT'
                            , help=_('P4PORT of server'))
        parser.add_argument('--p4user', '-u', metavar='P4USER'
                            , help=_('P4USER of user'))

    if add_log_args:
        parser.add_argument('--verbose', '-v', action=NTR("store_true")
                , help=_('Write additional diagnostics to standard output.'))
        parser.add_argument('--quiet', '-q', action=NTR("store_true")
                , help=_('Write nothing but errors to standard output.'))

    if add_debug_arg:
        parser.add_argument('--debug', nargs='?', default=argparse.SUPPRESS
                , help=_('Write debug diagnostics to standard output.'))
    if add_profiler_arg:
        parser.add_argument('--profiler', action=NTR('store_true')
                , help=_('Run with Python performance profiler.'))

    return parser


def p4gf_util_apply_log_args(
                    args
                  , logger     = None
                  , for_stdout = True
                  ):
    """Honor any optional --verbose/--quiet command line arguments
    by setting the logger's level to
        verbose   = INFO
        <nothing> = WARNING
        quiet     = ERROR

    If both set, --quiet beats --verbose.

    :param logger:
        Which logger to configure. If none specified, use the root logger. But
        this is rarely what you want. You want your own specific logger for
        your command line script, with messages designed for standard out, not
        just generic debugging dumps.

    :param for_stdout:
        Configure logger for stdout? This is common (why pass --verbose if you
        don't expect that verbosity to dump to console?).
    """
    l = logger if logger else logging.getLogger()

    if 'quiet' in args and args.quiet:
        l.setLevel(logging.ERROR)
    elif 'debug' in args:
                          # None = value set by passing --debug when
                          #        defualt=argparse.SUPPRESS
                          # True = value set by action=NTR('store_true'),
                          #        not used in create_arg_parser() but some
                          #        older/custom parsers still might.
        if args.debug in [None, True, 1, '1', 'debug', 'DEBUG']:
            l.setLevel(logging.DEBUG)
        elif args.debug in [2, '2', 'debug2', 'DEBUG2']:
            l.setLevel(logging.DEBUG2)
        elif args.debug in [3, '3', 'debug3', 'DEBUG3']:
            l.setLevel(logging.DEBUG3)
    elif 'verbose' in args and args.verbose:
        l.setLevel(logging.INFO)
    else:
        l.setLevel(logging.WARNING)

    if for_stdout:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        l.addHandler(handler)

# -- End 15.1 copypasta ------------------------------------------------------

def parse_argv():
    """Process command line args."""
    help_custom = \
"""
usage: repo_compare_changelist.py [options] --repo-name <repo-name>
                                  [--git-dir GIT_DIR]
                                  change-num [change-num ...]

Compare one or more individual Perforce changelists against their
corresponding Git commits.

Detects only these mismatches:
  {}

  --repo-name <repo-name>   name of existing Git Fusion repo

  change-num                changelist number(s) to compare

  --git-dir GIT_DIR     Git repo to compare against Perforce

other options:

  -h, --help            show this help message and exit
  -V                    displays version information and exits

  --p4port,-p P4PORT    P4PORT of server
  --p4user/-u P4USER    P4USER of user

  --verbose, -v         Write additional diagnostics to standard output.
  --quiet, -q           Write nothing but errors to standard output.
  --debug [DEBUG]       Write debug diagnostics to standard output.

""".format("\n  ".join(DiffHow.ALL))

    parser = p4gf_util_create_arg_parser(
          help_custom      = help_custom
        , add_p4_args      = True
        , add_log_args     = True
        , add_debug_arg    = True

        )

    parser.add_argument(
          '--repo-name'
        , required=True
        , help=_('name of existing Git Fusion repo'))
    parser.add_argument(
        '--git-dir'
        , default='.'
        , help=_('Git repo to compare against Perforce'))
    parser.add_argument(
         'change_num', metavar="change-num"
        , nargs='+'
        , help=_('Changelist number(s) to compare.'))

    args = parser.parse_args()
    p4gf_util_apply_log_args(args, logger=LOG)
    LOG.debug("args={}".format(args))
    return args


def main():
    """Do the thing."""
    args = parse_argv()

    ctx = p4gf_context.create_context(args.repo_name)
    ctx.config.p4user = args.p4user
    ctx.config.p4port = args.p4port
    ctx.connect_cli(log=LOG)

    p4gf_branch.init_case_handling(ctx.p4)

    rc = RepoCompareSingle(ctx, args)
    for change_num in args.change_num:
        rc.compare_change_num(change_num)
    rc.report_summary()

    if rc.how_ct:
        sys.exit(1)

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

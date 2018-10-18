#! /usr/bin/env python3.3
"""Collection of sundry utility functions."""

import argparse
import base64
from   collections import namedtuple, deque, OrderedDict
from contextlib import contextmanager
import copy
import json
import logging
import os
import pprint
import re
import shutil
import stat
import sys
import tempfile
import time
import traceback
from   uuid import uuid4
import zlib

import pygit2

import P4

import p4gf_bootstrap  # pylint: disable=unused-import
import p4gf_p4cache
import p4gf_const
from   p4gf_ensure_dir import parent_dir, ensure_dir, ensure_parent_dir
import p4gf_p4key      as     P4Key
from   p4gf_l10n       import _, NTR, mo_dir
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_path
import p4gf_pygit2
import p4gf_tempfile
import p4gf_util_p4run_logged  # pylint: disable=unused-import
import p4gf_version_3
import p4gf_repo_dirs

# Import the 2.6 compatible pieces, which are shared with OVA scripts.
#
from p4gf_util_26 import *  # pylint: disable=unused-wildcard-import, wildcard-import

LOG = logging.getLogger(__name__)

# Dictionary of p4 info server configurables
# Lazy loaded
P4ServerInfoAndConfigurables = {}


def get_p4_info_and_configurables(p4):
    """Lazy load once from p4 info and p4 keys proxy information."""
    if not P4ServerInfoAndConfigurables:
        r = p4gf_p4cache.fetch_info(p4)
        P4ServerInfoAndConfigurables['clientAddress'] = r.get('clientAddress')
        P4ServerInfoAndConfigurables['proxyAddress'] = r.get('proxyAddress')
        P4ServerInfoAndConfigurables['brokerAddress'] = r.get('brokerAddress')
        P4ServerInfoAndConfigurables['serverServices'] = r.get('serverServices')
        P4ServerInfoAndConfigurables['serverDate'] = r.get('serverDate')
        P4ServerInfoAndConfigurables['serverVersion'] = r.get('serverVersion')
        P4ServerInfoAndConfigurables['caseHandling'] = r.get('caseHandling')
        P4ServerInfoAndConfigurables['changeServer'] = r.get('changeServer')
        P4ServerInfoAndConfigurables['uses-proxy-prefix'] = False

        # Check various settings to determine if dm.proxy.protects will be applicable.
        # Gig Fusion supports proxy, broker, forwarding-replica, edge-server multi-servers
        # which utilize the dm.proxy.protects configurable.
        # standard and commit-server do not use dm.proxy.protects.
        if (P4ServerInfoAndConfigurables['proxyAddress'] or
                P4ServerInfoAndConfigurables['brokerAddress']):
            P4ServerInfoAndConfigurables['uses-proxy-prefix'] = True
        elif (P4ServerInfoAndConfigurables['serverServices'] and
              P4ServerInfoAndConfigurables['serverServices']
                in ['edge-server', 'forwarding-replica']):
            P4ServerInfoAndConfigurables['uses-proxy-prefix'] = True

        # P4GF_P4KEY_PROXY_PROTECTS was set by p4gf_super_init from dm.proxy.protects
        # storing 'true' or 'false'.
        # The 'p4 configure show' command requires super privileges
        P4ServerInfoAndConfigurables['dm.proxy.protects'] = \
            P4Key.get(p4, p4gf_const.P4GF_P4KEY_PROXY_PROTECTS)
        LOG.debug("P4ServerInfoAndConfigureables['dm.proxy.protects']={}".format(
            P4ServerInfoAndConfigurables['dm.proxy.protects']))
        # if not 'true' or 'false' raise error - it was not set by super_init
        if P4ServerInfoAndConfigurables['dm.proxy.protects'] == '0':
            raise RuntimeError(_('Git Fusion key {key} is not set. Contact your administrator. '
                                 'Run configure-git-fusion.sh and try again.').format(
                                     key=p4gf_const.P4GF_P4KEY_PROXY_PROTECTS))
        # set as boolean
        P4ServerInfoAndConfigurables['dm.proxy.protects'] = \
            P4ServerInfoAndConfigurables['dm.proxy.protects'].lower() == 'true'

    LOG.debug("p4 server info/configurable settings {}".format(P4ServerInfoAndConfigurables))
    return P4ServerInfoAndConfigurables


def create_arg_parser( desc             = None
                     , *
                     , epilog           = None
                     , usage            = None
                     , help_custom      = None
                     , help_file        = None
                     , formatter_class  = argparse.HelpFormatter
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
                help_txt = read_bin_file(help_file)
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
        , formatter_class = formatter_class
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


def apply_log_args( args
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


def print_dictionary_list(dictlist):
    """Dump a dictlist of dictionaries, for debugging purposes."""
    c = 0
    for adict in dictlist:
        c += 1
        print("\n--%d--" % c)
        for key in adict.keys():
            print("%s: %s" % (key, adict[key]))


def service_user_exists(p4, user):
    """"Check for service user."""
    # Scanning for users when there are NO users? p4d returns ERROR "No such
    # user(s)." instead of an empty result. That's not an error to us, so
    # don't raise it.
    # need '-a' option to list service users
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run(['users', '-a', user])
        for user_ in r:
            if user_['User'] == user:
                return True
    return False


def is_legal_repo_name(name):
    """Ensure that the repo name contains only legal characters.

    Legal means characters which are accepted by Perforce for client names. This
    means excluding the following character sequences: @ # * , / " %%x ...
    """
    # According to usage of 'p4 client' we get the following:
    # * Revision chars (@, #) are not allowed
    # * Wildcards (*, %%x, ...) are not allowed
    # * Commas (,) not allowed
    # * Slashes (/) ARE now allowed - supporting slashed git urls
    # * Double-quote (") => Wrong number of words for field 'Client'.
    # Additionally, it seems that just % causes problems on some systems,
    # with no explanation as to why, so for now, prohibit them as well.
    if re.search('[@#*,"]', name) or '%' in name or '...' in name:
        return False
    return True


def escape_path(path):
    """Filesystem/Git-to-Perforce  '@#%*'.

    Convert special characters fromthat Perforce prohibits from file paths
    to their %-escaped format that Perforce permits.
    """
    return path.replace('%', '%25').replace('#', '%23').replace('@', '%40').replace('*', '%2A')


def unescape_path(path):
    """Perforce-to-filesystem/Git  '@#%*'.

    Unescape special characters before sending to filesystem or Git.
    """
    return path.replace('%23', '#').replace('%40', '@').replace('%2A', '*').replace('%25', '%')


def argv_to_repo_name(argv1):
    """Convert a string passed in from argv to a usable repo name.

    Provides a central place where we can switch to unicode if we ever want
    to permit non-ASCII chars in repo names.

    Also defends against bogus user input like shell injection attacks:
    "p4gf_init.py 'myrepo;rm -rf *'"

    Raises an exception if input is not a legal repo name.
    """
    # To switch to unicode, do this:
    # argv1 = argv1.decode(sys.getfilesystemencoding())
    if not is_legal_repo_name(argv1):
        raise RuntimeError(_("Git Fusion: Not a repo client name: '{repo}'").format(repo=argv1))
    return argv1


def to_path_rev(path, rev):
    """Return file#rev."""
    return '{}#{}'.format(path, rev)


def strip_rev(path_rev):
    """Convert "file#rev" to file."""
    r = path_rev.split('#')
    return r[0]


def to_path_rev_list(path_list, rev_list):
    """Given a list of paths, and a corresponding list of file revisions, return
    a single list of path#rev.

    path_list : list of N paths. Could be depot, client, or local syntax
                (although local syntax with unescaped # signs will probably
                cause problems for downstream code that uses our result.)
    rev_list  : list of N revision numbers, one for each path in path_list.
    """
    return ['{}#{}'.format(path, rev)
            for (path, rev) in zip(path_list, rev_list)]


def reset_git_enviro():
    """Clear Git related environment variables, then chdir to GIT_WORK_TREE.

    This undoes any strangeness that might come in from T4 calling 'git
    --git-dir=xxx --work-tree=yyy' which might cause us to erroneously
    operate on the "client-side" git repo when invoked from T4.

    Also handles git-receive-pack chdir-ing into the .git dir.

    """
    git_env_key = [k for k in os.environ if k.startswith("GIT_")]
    for key in git_env_key:
        del os.environ[key]
    # Find our repo name, use that to calculate and chdir into our
    # GIT_WORK_TREE. This really on matters for the git hooks, which are
    # always started in the .git directory.
    repo_name = p4gf_path.cwd_to_repo_name()
    LOG.debug("reset_git_enviro() cwd_to_repo_name() returned {}".format(repo_name))
    if repo_name:
        repo_dirs = p4gf_repo_dirs.from_p4gf_dir(p4gf_const.P4GF_HOME, repo_name)
        # Path may not yet exist! And too early in the process to create anything on disk.
        if os.path.exists(repo_dirs.GIT_WORK_TREE):
            os.chdir(repo_dirs.GIT_WORK_TREE)


def sha1_exists(sha1):
    """Check if there's an object in the repo for the given sha1."""
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
        return sha1 in repo
    except KeyError:
        return False
    except ValueError:
        return False


def git_rev_list_1(commit):
    """Return the sha1 of a single commit, usually specified by ref.

    Return None if no such commit.
    """
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
        obj = repo.revparse_single(commit)
        return p4gf_pygit2.object_to_sha1(obj)
    except KeyError:
        return None
    except ValueError:
        return None

def _repo(repo):
    """If supplied a repo, use it. If not, find one."""
    if repo:
        return repo
    path = pygit2.discover_repository('.')
    return pygit2.Repository(path)

def git_sha1_to_parents(child_sha1, repo=None):
    """Retrieve the list of all parents of a single commit."""
    try:
        r = _repo(repo)
        obj = r.get(child_sha1)
        if obj.type == pygit2.GIT_OBJ_COMMIT:
            return [p4gf_pygit2.object_to_sha1(parent) for parent in obj.parents]
        return None
    except KeyError:
        return None
    except ValueError:
        return None


def sha1_for_branch(branch, repo=None):
    """Return the sha1 of a Git branch reference.

    Return None if no such branch.
    """
    try:
        r = _repo(repo)
        ref = r.lookup_reference(fully_qualify(branch))
        return p4gf_pygit2.ref_to_sha1(ref)
    except KeyError:
        return None
    except ValueError:
        return None


def git_empty():
    """Is our git repo completely empty, not a single commit?"""
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
        return len(repo.listall_references()) == 0
    except KeyError:
        return True
    except ValueError:
        return True


def fully_qualify(branch_ref_name):
    """What we usually call 'master' is actually 'refs/heads/master'.

    Does not work for remote branches!
    It's stupdily expensive. I'm not digging through 'git remotes' to
    see if your partial name matches 'refs/{remote}/{partial-name} for
    each possible value of {remote}.
    """
    if branch_ref_name.startswith('refs/'):
        return branch_ref_name
    return 'refs/heads/' + branch_ref_name


def git_ref_list_to_sha1(ref_list):
    """Dereference multiple refs to their corresponding sha1 values.

    Output a dict of ref to sha1.
    """
    try:
        path = pygit2.discover_repository('.')
        repo = pygit2.Repository(path)
    except KeyError:
        return None
    except ValueError:
        return None
    result = {}
    for n in ref_list:
        try:
            ref = repo.lookup_reference(fully_qualify(n))
            result[n] = p4gf_pygit2.ref_to_sha1(ref)
        except KeyError:
            result[n] = None
        except ValueError:
            result[n] = None
    return result


def dict_not(d, key_list):
    """Return a dict that omits any values for keys in key_list."""
    r = copy.copy(d)
    for key in key_list:
        if key in r:
            del r[key]
    return r


def quiet_none(x):
    """Convert None to empty string for quieter debug dumps."""
    if x is None:
        return ''
    return x


def test_vars_apply():
    """Apply changes as directed by environment variables."""
    if p4gf_const.P4GF_TEST_UUID_SEQUENTIAL in os.environ:
        global _uuid
        _uuid = uuid_sequential
        LOG.debug("UUID generator switched to sequential")


def repo_to_client_name(repo_name):
    """Construct client name using server id and repo name."""
    return p4gf_const.P4GF_REPO_CLIENT.format( server_id = get_server_id()
                                             , repo_name = repo_name )


def client_to_repo_name(client_name):
    """Parse repo name from client name."""
    prefix = repo_to_client_name('')
    return client_name[len(prefix):]


def serverid_dict(p4):
    """Return a dictionary of all the registered Git Fusion server_id -> hosts."""
    return {key.replace(p4gf_const.P4GF_P4KEY_SERVER_ID, ''): value
            for key, value in P4Key.get_all(
                p4, p4gf_const.P4GF_P4KEY_SERVER_ID + '*').items()}


def serverid_dict_for_repo(p4, repo_name):
    """Return a dict of all known Git Fusion serverids and their
    hostnames which have a P4GF_REPO_CLIENT defined for this client's repo
    Reads them from 'p4 clients'.

    Return empty dict if none found.
    """
    # get list of all clients, any serverid but this repo
    pattern = p4gf_const.P4GF_P4KEY_REPO_SERVER_CONFIG_REV.format(
            repo_name = repo_name, server_id = '*')
    server_id_host = {}
    sid_start = len('git-fusion-{0}-'.format(repo_name))

    for key in P4Key.get_all(p4, pattern).keys():
        sid_end = key.rfind('-p4gf-config-rev')
        sid = key[sid_start:sid_end]
        host = P4Key.get(p4, p4gf_const.P4GF_P4KEY_SERVER_ID + sid)
        server_id_host[sid] = host

    return server_id_host


def repo_config_list(p4):
    """Return a list of all known Git Fusion repos.

    Gets list of files matching //.git-fusion/repos/*/p4gf_config and extracts
    repo name part of path for each.

    Return empty list if none found.
    """
    config_path = p4gf_const.P4GF_CONFIG_REPO.format(P4GF_DEPOT = p4gf_const.P4GF_DEPOT
                                             , repo_name = '*')
    r = p4.run('files', '-e',  config_path)
    return [os.path.split(os.path.split(f['depotFile'])[0])[1] for f in r]


def first_dict(result_list):
    """Return the first dict result in a p4 result list.

    Skips over any message/text elements such as those inserted by p4broker.
    """
    for e in result_list:
        if isinstance(e, dict):
            return e
    return None


def first_dict_with_key(result_list, key):
    """Return the first dict result that sets the required key."""
    for e in result_list:
        if isinstance(e, dict) and key in e:
            return e
    return None


def first_value_for_key(result_list, key):
    """Return the first value for dict with key."""
    for e in result_list:
        if isinstance(e, dict) and key in e:
            return e[key]
    return None


def read_bin_file(filename):
    """Return the contents of bin/xxx.txt.

    Used to fetch help.txt and other such text templates.

    Returns False if not found.
    Return empty string if found and empty.
    """
                        # Check for localized/translated version
                        # in bin/mo/xx_YY/LC_MESSAGES/
    _mo_dir = mo_dir()

    file_path = None
    if _mo_dir:
        file_path = os.path.join(_mo_dir, filename)

                        # Fall back to bin/ directory.
    if not file_path or not os.path.exists(file_path):
        file_path = os.path.join(os.path.dirname(__file__), filename)

    if not os.path.exists(file_path):
        return False

    with open(file_path, "r") as file:
        text = file.read()

    return text


def depot_to_local_path(depot_path, p4=None, client_spec=None):
    """Where does this depot path land on the local filesystem?

    If we have a client spec, use its Root and View to calculate a depot
    path's location on the local filesystem. If we lack a client spec,
    but have a P4 connection, use that connection (and its implicit
    Perforce client spec) to calculate, using 'p4 where'.
    """
    if client_spec:
        p4map = P4.Map(client_spec['View'])
        client_path = p4map.translate(depot_path)
        if not client_path:
            raise RuntimeError(_('Depot path {dp} not in client view client={cn}')
                               .format(dp=depot_path, cn=client_spec['Client']))
        client_name = client_spec['Client']
        client_root = client_spec['Root']
        rel_path = client_path.replace('//{}/'.format(client_name), '')
        rel_path_unesc = unescape_path(rel_path)
        local_path = os.path.join(client_root, rel_path_unesc)
        return local_path

    if p4:
        return first_dict(p4.run('where', depot_path))['path']

    raise RuntimeError(_('Bug: depot_to_local_path() called with neither a'
                         ' client spec nor a p4 connection. depot_file={path}')
                       .format(path=depot_path))


def make_writable(local_path):
    """chmod existing file to user-writable.

    NOP if no such file or already writable.
    """
    if not os.path.exists(local_path):
        return
    s = os.stat(local_path)
    if not s.st_mode & stat.S_IWUSR:
        sw = s.st_mode | stat.S_IWUSR
        LOG.debug('chmod {:o} {}'.format(sw, local_path))
        os.chmod(local_path, sw)


class NumberedChangelist:

    """RAII class to create a numbered change, open files into it and submit it
    On exit, if change has not been successfully submited, all files are reverted
    and the change is deleted.
    """

    # pylint:disable=too-many-arguments
    def __init__(self,
                 p4=None, ctx=None, gfctx=None,
                 description=_('Created by Git Fusion'),

                        # If None, creates a new numbered pending changelist
                        # (what you usually want). Pass in a change_num of an
                        # existing numbered pending changelist if you want to
                        # use that instead.
                 change_num=None
                 ):
        """Call with exactly one of p4, ctx or gfctx set.
        In the case of ctx or gfctx, this numbered changelist will be attached
        to the context such that p4run() or p4gfrun() respectively will add
        the -c changelist option to applicable commands.
        """
        assert bool(p4) ^ bool(ctx) ^ bool(gfctx)
        self.ctx = ctx
        self.gfctx = gfctx
        if ctx:
            self.p4 = ctx.p4
            assert not ctx.numbered_change
            ctx.numbered_change = self
        elif gfctx:
            self.p4 = gfctx.p4gf
            assert not gfctx.numbered_change_gf
            gfctx.numbered_change_gf = self
        else:
            self.p4 = p4

        self.change_num = 0
        self.submitted  = False
        self.shelved    = False

        if change_num:
            change = self.p4.fetch_change(change_num)
            self.change_num = change_num
            LOG.debug('reusing numbered change {} with description {}'
                      .format(self.change_num, change["Description"]))
            self.submitted  = False
            self.shelved    = True

                        # Reclaim ownership of this change so that we can use it.
            change['User'] = p4gf_const.P4GF_USER
            self.p4.input = change
            self.p4.run('change', '-f', '-i')

        else:
            self._open_new(description)

    def _open_new(self, description):
        """Create a new numbered pending changelist."""
        change = self.p4.fetch_change()
        change["Description"] = description
        self.p4.input = change
        result = self.p4.run("change", "-i")
        self.change_num = int(result[0].split()[1])
        LOG.debug('created numbered change {} with description {}'
                  .format(self.change_num, description))
        self.submitted = False
        self.shelved   = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, _traceback):
        # attempt to revert change if anything goes wrong

        # unhook from context first, so that we can use -c on revert
        # whether there's a context or not
        if self.ctx:
            self.ctx.numbered_change = None
        elif self.gfctx:
            self.gfctx.numbered_change_gf = None

        if not (self.submitted or self.shelved) and self.change_num:
            if exc_type:
                LOG.debug("numbered_change_exception: {}".format(exc_value))
                LOG.debug2("tb: {}".format(traceback.format_tb(_traceback)))
            LOG.debug('numbered change not submitted, reverting and deleting')
            with self.p4.at_exception_level(P4.P4.RAISE_ERROR):
                self.p4.run("revert", "-c", self.change_num, "//...")
            self.p4.run("change", "-d", self.change_num)
        return False

    def p4run(self, *cmd):
        """add -c option to cmd and run it."""
        with self.p4.at_exception_level(P4.P4.RAISE_ALL):
            self.p4.run(self.add_change_option(*cmd))

    def submit(self):
        """Submit the numbered change and remember its new change number.

        Return result of submit command"""
        with self.p4.at_exception_level(P4.P4.RAISE_ALL):
            r = self.p4.run(self.add_change_option(["submit", "-f", "submitunchanged"]))
        self.change_num = self._changelist_from_submit_result(r)
        self.submitted = self.change_num is not None
        return r

    def submit_with_retry(self):
        """Attempt to submit a change, reverting any conflicting files.

        :return: results of submit, or None of nothing left.

        """
        while True:
            try:
                return self.submit()
            except P4.P4Exception:
                # get flat list of codes from all messages
                if p4gf_p4msg.find_msgid(self.p4, p4gf_p4msgid.MsgServer_NoSubmit):
                    LOG.debug('nothing left to submit after retrying')
                    return None
                elif p4gf_p4msg.find_msgid(self.p4, p4gf_p4msgid.MsgServer_CouldntLock,
                                           allcodes=True):
                    # just retry and wait for the re-add errors before reverting anything
                    LOG.debug('submit failed for locked files; retrying')
                elif p4gf_p4msg.find_msgid(self.p4, p4gf_p4msgid.MsgServer_ResolveOrRevert,
                                           allcodes=True):
                    # look for 'add of added file; must revert' messages
                    readds = [m.dict['depotFile'] for m in self.p4.messages
                              if m.msgid == p4gf_p4msgid.MsgDm_SubmitWasAdd]
                    if not readds:
                        LOG.debug('out of date files but no readds')
                        raise
                    LOG.debug('submit failed for readd of files; reverting and retrying')
                    self.p4.run(self.add_change_option(['revert', readds]))
                elif p4gf_p4msg.find_msgid(self.p4, p4gf_p4msgid.MsgServer_MergesPending,
                                           allcodes=True):
                    # look for merge conflicts and revert them
                    resolves = [m.dict['toFile'] for m in self.p4.messages
                                if m.msgid == p4gf_p4msgid.MsgDm_MustResolve]
                    if not resolves:
                        LOG.debug('out of date files but no resolves')
                        raise
                    LOG.debug('submit failed for integ of files; reverting and retrying')
                    self.p4.run(self.add_change_option(['revert', resolves]))
                else:
                    LOG.debug('submit failed for Unexpected reason')
                    raise

    def shelve(self, replace = False):
        """Shelve all pending file actions.

        Caller probably wants to revert all pending file actions after this,
        but that's not part of this function.
        """
        if replace:
            cmd = [ NTR('shelve')
                  , '-r'    # replace-all shelved files
                            # with currently open files
                  , '-c', self.change_num
                  ]
        else:
            cmd = [ NTR('shelve')
                  , '-c', self.change_num
                  , '//...']
        self.p4.run(cmd)
        self.shelved = True

    def add_change_option(self, *args):
        """Add p4 option to command to operate on a numbered pending changelist.

        command may be passed as a list or separate args.
        result will be returned as a list.

        command should not already contain a -c option
        """
        cmd = list(args)
        if len(cmd) == 1 and isinstance(cmd[0], list):
            cmd = cmd[0]
        if not self._cmd_needs_change(cmd[0]):
            return cmd
        if self.submitted:
            raise RuntimeError(_("Change already submitted"))
        assert "-c" not in cmd
        return cmd[:1] + ["-c", self.change_num] + cmd[1:]

    @staticmethod
    def _cmd_needs_change(cmd):
        """check if a command needs the -c changelist option."""
        return cmd in NTR(['add', 'edit', 'delete', 'copy', 'integ',
                           'opened', 'revert', 'reopen', 'unlock',
                           'resolve', 'submit'])

    @staticmethod
    def _changelist_from_submit_result(r):
        """Search for 'submittedChange'."""
        for d in r:
            if 'submittedChange' in d:
                return d['submittedChange']
        return None

    def second_open(self, description):
        """Open a new numbered pending changelist to replace our current
        changelist that we just submitted.
        """
        self._open_new(description)

    def __str__(self):
        """Return string representation of this numbered changelist."""
        return 'change_num={0}, submitted={1}, shelved={2}'.format(
            self.change_num, self.submitted, self.shelved)


def add_depot_file(p4, depot_path, file_content, client_spec=None, filetype=None):
    """Create a new local file with file_content, add and submit to
    Perforce, then sync#0 it away from our local filesystem: don't leave
    the local file around as a side effect of adding.
    If added, return True.

    If already exists in Peforce, return False.

    If unable to add to Perforce (probably because already exists) raise
    Perforce exception why not.

    Uses and submits a numbered pending changelist.
    """
    # Where does the file go?
    local_path = depot_to_local_path(depot_path, p4, client_spec)

    # File already exists in perforce and not deleted at head revision?
    with p4.at_exception_level(p4.RAISE_NONE):
        stat_ = p4.run('fstat', '-T', 'headAction', depot_path)
        if stat_ and 'headAction' in stat_[0]:
            action = stat_[0]['headAction']
            if action != 'delete' and action != 'move/delete':
                return False

    LOG.debug("add_depot_file() writing to {}".format(local_path))
    ensure_dir(parent_dir(local_path))
    with open(local_path, mode='w', encoding='utf-8') as f:
        f.write(file_content)

    filename = depot_path.split('/')[-1]
    desc = _("Creating initial '{filename}' file.").format(filename=filename)

    with NumberedChangelist(p4=p4, description=desc) as nc:
        args = ["add"]
        if filetype:
            args += ['-t', filetype]
        args.append(depot_path)
        nc.p4run(args)
        nc.submit()

    p4.run('sync', '-q', depot_path + "#0")
    return True


def edit_depot_file(p4, depot_path, file_content, client_spec=None):
    """p4 sync + edit + submit a single file in Perforce.

    Remove file from workspace when done: sync#0

    File must already exist in Perforce.

    Use and submit a numbered pending changelist.
    """

    p4.run('sync', '-q', depot_path)

    filename = depot_path.split('/')[-1]
    desc = _("Update '{filename}'.").format(filename=filename)

    with NumberedChangelist(p4=p4, description=desc) as nc:
        nc.p4run('edit', depot_path)
        # Where does the file go?
        local_path = depot_to_local_path(depot_path, p4, client_spec)
        with open(local_path, 'w') as f:
            f.write(file_content)
        nc.submit()

    p4.run('sync', '-q', depot_path + "#0")


def print_depot_path_raw(p4, depot_path, change_num=None, fallback=b''):
    """p4 print a file, using raw encoding, and return the raw bytes of that file."""
    (b, _d) = print_depot_path_raw_tagged(p4, depot_path,
                                          change_num=change_num,
                                          fallback=fallback)
    return b


def print_depot_path_raw_tagged(p4, depot_path, change_num=None,
                                fallback=b''):
    """Print a file, using raw encoding, and return the raw bytes of that file.

    :param p4: P4API object.
    :param str depot_path: depot path of file to print.
    :param change_num: change number at which to print file (defaults to None).
    :param fallback: value to return if file does not exist.

    Returns a tuple of
    * raw bytes
    * 'p4 print' tagged dict
    """
    tempdir = p4gf_tempfile.new_temp_dir()
    with tempdir:
        tf = tempfile.NamedTemporaryFile( dir=tempdir.name
                                        , prefix='print-'
                                        , delete=False )
        tf.close()                      # Let p4, not Python, write to this file.
        with raw_encoding(p4):
            dpath = depot_path if change_num is None else '{}@{}'.format(depot_path, change_num)
            r = p4.run('print', '-o', tf.name, dpath)
            d = first_dict_with_key(r, "depotFile")
        if p4gf_p4msg.find_msgid(p4, p4gf_p4msgid.MsgDm_ExFILE):
            return (fallback, d)
        with open(tf.name, mode='rb') as f2:
            b = f2.read()
        return (b, d)


def depot_file_exists(p4, depot_path):
    """Does this file exist in the depot, and is its head revision not deleted?"""
    with p4.at_exception_level(p4.RAISE_NONE):
        head_action = first_value_for_key(p4.run( 'fstat'
                                                , '-TheadAction'
                                                , depot_path)
                                         , 'headAction')
    if head_action and 'delete' not in head_action:
        return True
    else:
        return False


def depot_file_head_rev_action(p4, depot_path):
    """Return tuple of head rev, action, and change.

    If file does not exist, returns (None, None, None)

    :type p4: :class:`P4API`
    :param p4: P4 API instance

    :type depot_path: str
    :param depot_path: path of file in depot

    :rtype: 3-tuple

    """
    head_rev = None
    head_action = None
    head_change = None
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('fstat', '-TheadRev,headAction,headChange', depot_path)
    if r:
        rr = first_dict(r)
        head_rev = rr.get('headRev')
        head_action = rr.get('headAction')
        head_change = rr.get('headChange')
    return (head_rev, head_action, head_change)


def depot_file_is_re_added(p4, depot_path, start_rev, end_rev):
    """Return True if a revision with action == 'delete' occurs between the start_rev and end_rev.

    start_rev and end_rev must numeric strings
    """
    start = start_rev.lstrip('#')
    end = end_rev.lstrip('#')

    # Must be difference of 2 to find an intervening 'delete'
    try:
        if int(start) + 1 >= int(end):
            return False
    except ValueError:
        return False

    depot_path_with_revs = depot_path + '#' + start_rev + ',#' + end_rev
    # Surpisingly, the return is an array of length 1 which is
    # a dictionary with positionally ordered array values for keys 'action', 'rev', etc
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('filelog', depot_path_with_revs)
    if r:
        for rr in r:
            if not isinstance(rr, dict):
                continue
            if 'action' in rr:
                middle_revs = rr['action'][1:-1]
                if 'delete' in middle_revs:
                    return True

    return False


def get_p4key(p4, p4key_name):
    """Return p4key value.

    ### Move to P4Key.
    """
    value = 0
    with p4.at_exception_level(p4.RAISE_NONE):
        value = P4Key.get(p4, p4key_name)
    return value


def set_p4key(p4, p4key_name, value):
    """Set a p4key.

    ### Move to P4Key.
    """
    with p4.at_exception_level(p4.RAISE_NONE):
        P4Key.set(p4, p4key_name, value)

_UUID_SEQUENTIAL_PREV_VALUE = 0
_P4KEY_UUID_SEQUENTIAL = "uuid-sequential"


def uuid_sequential(p4, namespace=None):
    """Replacement for uuid() that returns a deterministic sequence of
    values so that test scripts can more easily test for exepected results.

    Keep the differentiating part of the UUID in the first 7 chars, since
    many debug log statements only print that much of the UUID.
    """
    if p4 and namespace:
        value = int(P4Key.increment(p4, '{}-{}'.format(_P4KEY_UUID_SEQUENTIAL, namespace)))
    elif p4:
        value = int(P4Key.increment(p4, _P4KEY_UUID_SEQUENTIAL))
    else:
        global _UUID_SEQUENTIAL_PREV_VALUE
        _UUID_SEQUENTIAL_PREV_VALUE += 1
        value = _UUID_SEQUENTIAL_PREV_VALUE
    return NTR('{:05}-uuid').format(value)


def uuid_real(_p4_unused, _namespace_unused):
    """Return a globally unique identifier.

    Returns a 128-bit GUID, encoded as a 26-character base32 string.
    Trailing '=' pad characters are stripped off and the result is lowercased.
    """
    # p4 argument ignored - used only by uuid_sequential
    return base64.b32encode(uuid4().bytes)[:-6].lower().decode()


_uuid = uuid_real    # How to generate UUIDs. See test_vars_apply()


def uuid(p4=None, namespace=None):
    """Call our UUID generator.

    Usually real UUIDs, sometimes sequential if running under a test that needs
    deterministic results.
    """
                        # It's fine to call uuid() without a p4 connection,
                        # but our test suite works a lot better and more
                        # reproducible if you always pass in a p4 connection.
                        # Fink on the callers that don't.
    if not p4:
        LOG.warning("uuid() without a p4")
        LOG.warning("".join(traceback.format_stack()))

    return _uuid(p4, namespace)


def dict_to_log(d):
    """Format a dict for logging, with keys in sorted order."""
    return pprint.saferepr(d)


def log_collection(log, coll):
    """If DEBUG3, return entire collection (ouch).

    If just DEBUG, return just collection length.
    If not DEBUG at all, return None.
    """
    if log.isEnabledFor(logging.DEBUG):
        if log.isEnabledFor(logging.DEBUG3):
            return coll
        else:
            return len(coll)
    else:
        return None


def log_environ(logger, environ, caption):
    """Dump our environment to the log at DEBUG3 level.

    :param logger: the logger instance to which environ is recorded.
    :param environ: dict containing environment variables.
    :param caption: descriptive title for logging output.

    """
    if logger.isEnabledFor(logging.DEBUG3):
        logger.debug3("{} environment:".format(caption))
        for name in sorted(environ.keys()):
            value = environ[name] if not name == 'P4PASSWD' else '********'
            logger.debug3("    {}: {}".format(name, value))


def first_of(c):
    """Return first non-None element of c."""
    for x in c:
        if x:
            return x
    return None


def gf_reviews_user_name():
    """Return a service user name for a per GF instance service user.

    We use this to record client views as Reviews.
    """
    return p4gf_const.P4GF_REVIEWS_SERVICEUSER.format(get_server_id())


# Handler go bye-bye; use P4.using_handler() instead
# class Handler: ...


@contextmanager
def raw_encoding(p4):
    """Set p4.encoding to "raw" on entry, restore on exit."""
    save_encoding = p4.encoding
    p4.encoding = NTR('raw')
    try:
        yield
    finally:
        p4.encoding = save_encoding


@contextmanager
def restore_client(p4, client_name):
    """Restore the original client upon exit or exception."""
    saved_client = p4.client
    p4.client = client_name
    try:
        yield
    finally:
        p4.client = saved_client


@contextmanager
def ignored(*exceptions):
    """Quietly ignore one or more exceptions.

    # usage
    with ignored(OSError):
        os.remove('somefile.tmp')

    """
    try:
        yield
    except exceptions:
        pass


class UserClientHost:

    """RAII class to set p4.user, p4.client, and p4.host on entry, restore on exit."""

    def __init__(self, p4, user, client, host):
        self.p4 = p4
        self.new_user = user
        self.new_client = client
        self.new_host = host
        self.save_user = p4.user
        self.save_client = p4.client
        self.save_host = p4.host

    def __enter__(self):
        self.p4.user = self.new_user
        self.p4.client = self.new_client
        self.p4.host = self.new_host

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.p4.user = self.save_user
        self.p4.client = self.save_client
        self.p4.host = self.save_host


class ClientUnlocker:

    """RAII class to unlock a client, restore original setting on exit."""

    def __init__(self, p4, client):
        """Initialize ClientUnlocker with the given P4API and client map."""
        self.p4 = p4
        self.client = client
        self.options = client['Options'].split()
        self.was_locked = 'locked' in self.options

    def __enter__(self):
        if self.was_locked:
            self.options[self.options.index(NTR('locked'))] = NTR('unlocked')
            self.client['Options'] = " ".join(self.options)
            self.p4.save_client(self.client, '-f')

    def __exit__(self, _exc_type, _exc_value, _traceback):
        if self.was_locked:
            self.options[self.options.index(NTR('unlocked'))] = NTR('locked')
            self.client['Options'] = " ".join(self.options)
            self.p4.save_client(self.client, '-f')


class FileDeleter:

    """RAII object to delete all registered files."""

    def __init__(self):
        """Initialize ClientUnlocker with the given P4API and client map."""
        self.file_list = []

    def __enter__(self):
        pass

    def __exit__(self, _exc_type, _exc_value, _traceback):
        LOG.debug('FileDeleter deleting {}'.format(self.file_list))
        for f in self.file_list:
            os.unlink(f)


def enslash(path_element):
    """Break a single path element into multiple nested parts.

    This restricts the number of files in any single depot or filesystem
    directory to something that most GUIs can handle without bogging down.

    Input string must not contain any slashes.
    Input string must be at least 5 characters long.
    """
    return (       path_element[0:2]
           + '/' + path_element[2:4]
           + '/' + path_element[4: ] )


def octal(x):
    """
    Convert a string such as a git file mode "120000" to an integer 0o120000.

    Integers pass unchanged.
    None converted to 0.

    See mode_str() for counterpart.
    """
    if not x:
        return 0
    if isinstance(x, int):
        return x
    return int(x, 8)


def mode_str(x):
    """Convert octal integer to string, return all others unchanged.

    See octal() for counterpart.
    """
    if isinstance(x, int):
        return NTR('{:06o}').format(x)
    return x


def chmod_644_minimum(local_path):
    """Grant read access to all unix accounts."""
    old_mode = os.stat(local_path).st_mode
    new_mode = old_mode | 0o000644
    os.chmod(local_path, new_mode)


def _force_clear(dest_local):
    """Clear the way before a copy/link_file_forced().

    rm -rf is very dangerous. You would be wise to pass
    absolute paths for dest_local.
    """
    if os.path.exists(dest_local):
        if os.path.isdir(dest_local):
            shutil.rmtree(dest_local)
        else:
            os.unlink(dest_local)


def write_server_id_to_file(server_id):
    """Write server_id to P4GF_HOME/server-id.

    NOP if that file already holds server_id.
    """
    # NOP if already set to server_id.
    if server_id == read_server_id_from_file():
        return

    path = server_id_file_path()
    ensure_parent_dir(path)
    with open(path, 'w') as f:
        f.write('{}\n'.format(server_id))


def write_dict_to_file(write_dict, filename):
    """JSON-ify a dict and write it."""
    try:
        with open(filename, 'w') as fobj:
            # indentation is for readability; if the file gets too large, gzip it
            json.dump(write_dict, fobj, indent=4)
    except TypeError as exc:
        LOG.error("failed to serialize dict: {}"
                  .format(pprint.pformat(write_dict)))
        raise RuntimeError("failed to serialize dict") from exc

def read_dict_from_file(filename):
    """JSON-ify a dict and write it."""
    with open(filename) as fobj:
        package = json.load(fobj, object_pairs_hook=OrderedDict)
    return package


def p4map_lhs_canonical(lhs):
    """Strip quotes and leading +/-.

    Return 2-tuple of (leading +/-, line).
    """
    mod = ''
    if not lhs:
        return (mod, lhs)
    r   = lhs
    dequoted = False          # You only get one level of dequote.
    if r[0] == r[-1] == '"':
        r = r[1:-1]
        if not r:
            return (mod, r)
    if r[0] in ['-', '+']:
        mod = r[0]
        r   = r[1:]
        if not r:
            return (mod, r)
    if not dequoted and r[0] == r[-1] == '"':
        r = r[1:-1]
    return (mod, r)


def p4map_lhs_line_replace_root(lhs, old_root, new_root):
    """Convert a branch view mapping line's lhs from one depot root to another.

    root strings assumed to end in slash. Not doing that here because
    it's usually more efficient to do that outside of this function
    which is usually called in a loop.

    Strips any double-quotes from lhs. Those shouldn't have survived
    config-to-view anyway.
    """
    (mod, orig_lhs_path) = p4map_lhs_canonical(lhs)
    if orig_lhs_path.startswith(old_root):
        new_lhs_path = new_root + orig_lhs_path[len(old_root):]
    else:
        new_lhs_path = orig_lhs_path
    return mod + new_lhs_path


def create_p4map_replace_lhs_root(orig_p4map, old_root, new_root):
    """Create a new P4.Map instance, similar to orig_p4map but with the
    lhs of each set to a new depot root.

    old_root and new_root should both end in / delimiter.
    """
    result = P4.Map()
    for (orig_lhs, rhs) in zip(orig_p4map.lhs(), orig_p4map.rhs()):
        new_lhs = p4map_lhs_line_replace_root(orig_lhs, old_root, new_root)
        result.insert(new_lhs, rhs)
    return result


def p4map_split(line):
    """Split a single mapping line into (lhs, rhs), stripping " quotes.

    This is a transliteration of P4Python's C++ P4MapMaker::SplitMapping(),
    with the same behavior for weird cases such as leading spaces or odd
    numbers of double-quotes.
    """
    quoted    = False
    split     = False  # aka "in rhs"
    lhs_rhs   = [ [], [] ]
    i_lhs      = 0
    i_rhs      = 1
    dst_index = i_lhs

    for c in line:
        if c == '"':
            quoted = not quoted
        elif c == ' ':
            if not quoted and not split:
                # Whitespace between lhs and rhs. Skip and start the rhs.
                split     = True
                dst_index = i_rhs
            elif not quoted:
                # Trailing space on rhs. Ignore
                pass
            else:
                # Embedded space. Retain.
                lhs_rhs[dst_index].append(c)
        else:
            lhs_rhs[dst_index].append(c)
    if not len(lhs_rhs[i_rhs]):
        lhs_rhs[i_rhs] = lhs_rhs[i_lhs]
    return ("".join(lhs_rhs[i_lhs]), "".join(lhs_rhs[i_rhs]))


def gmtime_str_iso_8601(seconds_since_epoch=None):
    """Return an ISO 8601-formatted timestamp in UTC time zone (gmtime).

    YYYY-MM-DDThh:mm:ssZ

    seconds_since_epoch is usually something from time.gmtime() or a P4 date
    number. If omitted, current time is used. If supplied, caller is responsible
    for converting from time zone to UTC before passing to us.
    """
    t = seconds_since_epoch if seconds_since_epoch else time.gmtime()
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)


def bytes_to_git_object(byte_array):
    """Decompress a Git object's content, strip header, return content."""
    data = zlib.decompress(byte_array)
    # skip over the type and length
    return data[data.index(b'\x00') + 1:]


def local_path_to_git_object(local_path):
    """Retrieve a decompressed Git object from the Git Fusion object cache.

    The object header is not included in the result.
    """
    with open(local_path, "rb") as f:
        blob = f.read()
    return bytes_to_git_object(blob)


def depot_path_to_git_object(p4, depot_path):
    """Fetch a Git object from its location in Perforce."""
    byte_array = print_depot_path_raw(p4, depot_path)
    return bytes_to_git_object(byte_array)


def remove_duplicates(collection):
    """There's probably a library function for this somewhere."""
    s = set(collection)
    if len(s) == len(collection):
        return collection
    r = []
    for c in collection:
        if c in s:
            r.append(c)
            s.remove(c)
    return r


def iter_chunks(collection, chunk_size):
    """Return subsets of collection,
    each subset no more than chunk_size elements.
    """
    for start in range(0, len(collection), chunk_size):
        yield collection[start:start + chunk_size]


def pairwise(iterable):
    """Convert a single-element list into a 2-tuple list.

    If odd number of elements, omits final odd element.

    Mostly used to convert lists to dicts:

    l = ['a', 1, 'b', 2]
    d = dict(pairwise(l))
    print(d)  # ==> {'b': 2, 'a': 1}

    Adapted from http://code.activestate.com/recipes/252176-dicts-from-lists/
    """
    i = iter(iterable)
    while 1:
        yield next(i), next(i)


def sha1_to_git_objects_path(sha1):
    """Return a path to a loose object: "objects/??/?{38}".

    This is the path that git-hash-objects and other Git tools create when
    writing an object to Git's own object store.

    See git/sha1_file.c/sha1_file_name().

    WARNING: Nathan has seen Git nest objects more deeply after unpacking
             into loose objects. This code knows nothing of that.
    """
    return 'objects/' + sha1[0:2] + '/' + sha1[2:]


def abbrev(x):
    """To 7 chars.

    Accepts str.
    Accepts None.
    Accepts list.
    """
    if None == x:
        return 'None'
    elif isinstance(x, list):
        return [abbrev(e) for e in x]
    elif isinstance(x, set):
        return {abbrev(e) for e in x}
    elif isinstance(x, str):
        return x[:7]
    return x


def ralign_commafied(val, max_val):
    """Right-alight a number to fit a space large enough to hold max_val.
    """
    max_str = "{:,}".format(max_val)
    max_len = len(max_str)
    val_fmt = "{:>" + str(max_len) + ",}"
    return val_fmt.format(val)


def tabular(labels, int_values):
    """Produce a tabular list of strings, "label: value"

    Integer values are right-aligned and commafied.
    """
    lwidth = max([len(s) for s in labels])
    rwidth = len("{:,}".format(max(int_values)))
    line_fmt = "{:<" + str(lwidth) + "} : {:>" + str(rwidth) + ",}"
    return [ line_fmt.format(l, r) for l, r in zip(labels, int_values) ]


def debug_list(log, lizt, details_at_level=logging.DEBUG2):
    """Conditionally summarize a list for log.

    If debug level at or finer than details_at_level, return list.
    If not, return "ct=N"
    """
    if log.isEnabledFor(details_at_level):
        return lizt
    return NTR('ct={}').format(len(lizt))


def partition(pred, iterable):
    """Use a predicate to partition true entries before false entries."""
    false = []
    for x in iterable:
        if pred(x):
            yield x
        else:
            false.append(x)
    for f in false:
        yield f


def alpha_numeric_sort(list_):
    """Sort the given iterable in the way that humans expect."""
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(list_, key=alphanum_key)


def dirtree_has_no_files(dirpath):
    """Test directory tree has no files."""
    for _root, _dirs, files in os.walk(dirpath, followlinks=True):
        if len(files):
            return False
    return True


GitLsTreeResult = namedtuple( 'GitLsTreeResult'
                            , ['mode', 'type', 'sha1', 'gwt_path'])


def _tuple_from_tree_entry(entry, path=None):
    """Make a tuple from a TreeEntry and path."""
    if not path:
        path = entry.name
    if entry.filemode == 0o040000:
        otype = 'tree'
    elif entry.filemode == 0o160000:
        otype = 'gitlink'
    else:
        otype = 'blob'

    ### Zig would like to leave mode as int rather than convert to and from str.
    ### Who still relies on it being a str?
    return GitLsTreeResult( mode     = mode_str(entry.filemode)
                          , type     = otype
                          , sha1     = p4gf_pygit2.object_to_sha1(entry)
                          , gwt_path = path
                          )


def git_ls_tree_one(repo, commit_sha1, gwt_path):
    """Return a single file's ls-tree 4-tuple.

    ('100644', 'blob', '47cb616851147876ee381cd8aad569530fe25a91', 'a/a3')
    """
    try:
        tree_entry = repo.get(commit_sha1).tree[gwt_path]
    except:  # pylint: disable=bare-except
        return None

    return _tuple_from_tree_entry(tree_entry, gwt_path)


def git_ls_tree(repo, treeish_sha1):
    """Return a list of ls-tree 4-tuples direct children of a treeish.

    ('100644', 'blob', '47cb616851147876ee381cd8aad569530fe25a91', 'a/a3')

    DOES NOT RECURSE. See git_ls_tree_r() for that.

    Squelches errors, because this is frequently used to query for existence
    and not-exists is a valid result, not an exception to raise.
    """
    try:
        obj = repo.get(treeish_sha1)
        if obj.type == pygit2.GIT_OBJ_TREE:
            tree = obj
        elif obj.type == pygit2.GIT_OBJ_COMMIT:
            tree = obj.tree
        else:
            # BLOB or TAG
            raise RuntimeError(_('object is not a commit or tree'))
        return [_tuple_from_tree_entry(entry) for entry in tree]
    except:  # pylint: disable=bare-except
        return []


def treeish_to_tree(repo, treeish_sha1):
    """Convert a commit or tree sha1 to its pygit2.Tree object."""
    obj = repo.get(treeish_sha1)
    if obj:
        if obj.type == pygit2.GIT_OBJ_TREE:
            return obj
        elif obj.type == pygit2.GIT_OBJ_COMMIT:
            return obj.tree
    return None

TreeWalk = namedtuple('TreeWalk', ['gwt_path', 'tree'])


def git_iter_tree(repo, treeish_sha1):
    """Iterate through a tree, yielding
        parent directory path
        child name (single name not full path)
        child mode (integer)
        child sha1

    +++ Do not construct and return GitLsTreeResult instances.
        That increases the cost of a tree walk, and many walks (such as 'find
        all symlinks') discard most nodes. Don't pay construction costs for
        objects you don't need.
    """

    # Initialize the walk with top-level TreeEntry.
    start_tree = treeish_to_tree(repo, treeish_sha1)
    if not start_tree:
        return
    work_queue = deque([TreeWalk(gwt_path='', tree=start_tree)])

    log = LOG.getChild('ls_tree')
    log.debug2('git_iter_tree / {}'.format(abbrev(treeish_sha1)))
    is_debug3 = log.isEnabledFor(logging.DEBUG3)

    # Walk the tree, yielding rows as we encounter them.
    # Yield directories when encountered, no different than blobs,
    # but also queue them up for later "recursion".

    while work_queue:
        curr_tree_walk = work_queue.pop()

        parent_gwt_path = curr_tree_walk.gwt_path

        for child_te in curr_tree_walk.tree:
            if is_debug3:
                log.debug3('git_iter_tree Y {:06o} {:<40} {:<20} {}'
                           .format( child_te.filemode
                                  , p4gf_pygit2.object_to_sha1(child_te)
                                  , child_te.name
                                  , parent_gwt_path))

            yield parent_gwt_path, child_te.name, child_te.filemode, \
                p4gf_pygit2.object_to_sha1(child_te)

            # "Recurse" into subdirectory TreeEntry later.
            if child_te.filemode == 0o040000:  # dir
                child_gwt_path = p4gf_path.join(parent_gwt_path, child_te.name)
                child_tree     = repo[child_te.oid]
                work_queue.append(TreeWalk( gwt_path = child_gwt_path
                                          , tree     = child_tree     ))
                if is_debug3:
                    log.debug3('git_iter_tree Q {}'.format(child_gwt_path))


def _filemode_to_type(filemode):
    """It is faster to infer blob/tree type from an integer filemode
    than to instantiate a pygit2 object just to ask it this question.
    """
    if filemode == 0o040000:
        return 'tree'
    elif filemode == 0o160000:
        return 'gitlink'
    else:
        return 'blob'


def git_ls_tree_r(repo, treeish_sha1):
    """Return a generator that produces a list of ls-tree 4-tuples, one for
    each directory or blob in the entire tree.

    ('100644', 'blob', '47cb616851147876ee381cd8aad569530fe25a91', 'a/a3')

    Walks entire tree.
    """
    return ( GitLsTreeResult(
                  mode     = mode_str(mode)  ### Zig dislikes str not int here.
                , type     = _filemode_to_type(mode)
                , sha1     = sha1
                , gwt_path = p4gf_path.join(parent_gwt_path, name) )
            for parent_gwt_path
              , name
              , mode
              , sha1 in git_iter_tree(repo, treeish_sha1) )


def unlink_file_or_dir(path, delete_non_empty=False):
    """Handle links, files, and empty/full directory hierarchies."""
    if os.path.lexists(path):
        if os.path.isdir(path) and not os.path.islink(path):
            if delete_non_empty or dirtree_has_no_files(path):
                shutil.rmtree(path)
            else:
                LOG.debug("Cannot remove dir {0} - not empty".format(path))
        else:
            os.unlink(path)  # would fail on real dir


def rm_dir_contents(dir_path):
    """Remove the contents of a dir path."""
    if os.path.isdir(dir_path):
        for file_object in os.listdir(dir_path):
            file_object_path = os.path.join(dir_path, file_object)
            if os.path.isdir(file_object_path) and not os.path.islink(file_object_path):
                shutil.rmtree(file_object_path)
            else:
                os.unlink(file_object_path)  # would fail on real dir


def remove_tree(tree, contents_only=True):
    """Delete a directory tree."""
    if not os.path.exists(tree):
        return
    try:
        rm_dir_contents(tree)
        if not contents_only:
            if os.path.isdir(tree) and not os.path.islink(tree):
                os.rmdir(tree)
            else:
                os.remove(tree)
    except FileNotFoundError as e:
        sys.stderr.write(_('File not found error while removing tree: {exception}\n')
                         .format(exception=e))
    except PermissionError as e:
        sys.stderr.write(_('Permission error while removing tree: {exception}\n')
                         .format(exception=e))


def p4_client_df(p4, client_name):
    """Run 'p4 client -df {}' but don't bail if server error. Keep going."""
    with p4.at_exception_level(P4.P4.RAISE_NONE):
        p4.run('client', '-df', client_name)


def enquote_list(l):
    """Return a new list with every space-infected item wrapped in quotes."""
    return [p4gf_path.enquote(e) for e in l]


def files_in_change_num(ctx, change_num):
    """Run 'p4 describe -s change_num' and yield all of the depotFile
    results.

    'p4 describe' returns a SINGLE dict with list elements:

    { 'depotFile' : [ '//d/f1', '//d/f2' ] }
    """
    r = ctx.p4run('describe', '-s', change_num)
    depot_path_list = first_value_for_key(r, 'depotFile')
    if depot_path_list:
        return depot_path_list
    else:
        return []


def any_opened(ctx):
    """p4 opened -m1 -c NNN'

    Are _any_ files opened in the current numbered pending changelist?
    """
    r = ctx.p4run('opened', '-m1')
    d = first_dict(r)
    if d:
        return True
    return False


def head_change_as_string(ctx, submitted=False, view_path=None):
    """Return most recent change for path, or None if no changes for path.

    Run 'p4 changes -m1 //client/...' and return the change string from first
    change dict, or None if result is empty.
       param: view_path : Use the ctx.client_view_path() if not set
       param: submitted : If True, get only most recent submitted changelist
    """
    if not view_path:
        view_path = ctx.client_view_path()

    cmd = ['changes', '-m1', view_path]
    if submitted:
        cmd = ['changes', '-m1', '-s', 'submitted', view_path]

    r = ctx.p4run(cmd)
    head_change = first_dict_with_key(r, 'change')
    if head_change:
        head_change = head_change['change']
    return head_change


def map_lhs_to_relative_rhs_list(lhs_list):
    """Turn a list of LHS strings to RHS strings."""
    return [map_lhs_to_relative_rhs(l) for l in lhs_list]


def map_lhs_to_relative_rhs(lhs):
    """Turn "//depot/..." into "depot/...".

    Honors quotes '"' and + (as long as the + prece

    Does not honor exlude/ovelay -/+ prefixes because YAGNI: you're building
    the RHS for something and likely keeping the LHS as-is.
    """
    rhs = lhs

    quoted = False
    if rhs.startswith('"'):     # Temporarily remove leading quotes.
        quoted = True
        rhs = rhs[1:]
    if rhs.startswith('+'):     # Omit overlay/plus-mapping marker.
        rhs = rhs[1:]
        if rhs.startswith('"'):     # Temporarily remove quotes after +
            quoted = True
            rhs = rhs[1:]
    if rhs.startswith('//'):    # Absolute // becomes relative ''
        rhs = rhs[2:]

    if quoted:                  # Restore leading quotes.
        rhs = '"' + rhs
    return rhs


class CommandError(RuntimeError):

    """An Error with reduced logging requirements."""

    def __init__(self, val, usage=None):
        """Save usage message, if any."""
        self.usage = usage  # Printed to stdout if set
        RuntimeError.__init__(self, val)


def raise_gfuser_insufficient_perm(p4port):
    """Some random Helix error implies that git-fusion-user lacks
    permission to see or do something. Translate from Helix to US English.
    """
    LOG.error(_("git-fusion-user not granted sufficient privileges.\n"
                "          Check the P4 protects entry for git-fusion-user.\n"
                "          The IP field must match the IP set in P4PORT={p4port}").
              format(p4port=p4port))

    raise CommandError(_("Perforce: git-fusion-user not granted sufficient privileges."
                         " Please contact your admin.\n"))


def is_temp_object_client_name(name):
    """Check if name is a valid temp object client name."""
    prefix = p4gf_const.P4GF_OBJECT_CLIENT.format(server_id=get_server_id())
    return re.match('^' + prefix + '-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$',
                    name)

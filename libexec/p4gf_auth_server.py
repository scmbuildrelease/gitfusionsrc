#! /usr/bin/env python3.3
"""p4gf_auth_server.py.

A shell replacement that ssh invokes to run push or pull commands on the
Git Fusion server.

Arguments:
--user=p4user  required  which Perforce user account is the pusher/puller
--keyfp=<key>  required  SSH key fingerprint of key used to authenticate
<command>      required  one of git-upload-pack or git-receive-pack
                         no other commands permitted

Record the request, along with p4user and key fingerprint and requested
git command, to an audit log.

Run the appropriate protocol interceptor for git-upload-pack or
git-receive-pack.

Reject attempt if p4user lacks read privileges for the entire repo.

Reject unknown git command

"""
import argparse
import functools
import logging
import os
import re
import sys

import p4gf_env_config  # pylint: disable=unused-import
import p4gf_const
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_log
import p4gf_proc
from p4gf_profiler import with_timer
import p4gf_server_common
import p4gf_util
import p4gf_version_3
import p4gf_atomic_lock
import p4gf_git

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_auth_server")


def illegal_option(option):
    """Trying to sneak a shell command into my world? Please do not do that."""
    if ';' in option:
        return True

    # git-upload-pack only understands --strict and --timeout=<n>.
    # git-receive-pack understands no options at all.
    re_list = [re.compile("^--strict$"),
               re.compile(r"^--timeout=\d+$")]
    for reg in re_list:
        if reg.match(option):
            return False
    return True


def is_special_command(repo):
    """See if repo is actually a special command masquerading as a repo."""
    special_specials = [
        p4gf_const.P4GF_UNREPO_FEATURES + "@",
        p4gf_const.P4GF_UNREPO_WAIT + "@",
        p4gf_const.P4GF_UNREPO_STATUS + "@",
        p4gf_const.P4GF_UNREPO_PROGRESS + "@"
    ]
    for special_cmd in special_specials:
        if repo.startswith(special_cmd):
            return True
    return repo in p4gf_const.P4GF_UNREPO


def parse_args(argv=None):
    """Parse the command line arguments into a struct and return it.

    On error, print error to stdout and return None.

    If unable to parse, argparse.ArgumentParser.parse_args() will exit,
    so we add our own exit() calls, too. Otherwise some poor unsuspecting
    programmer would see all our "return None" calls and think that
    "None" is the only outcome of a bad argv.
    """
    argv = argv or sys.argv[1:]
    parser = p4gf_util.create_arg_parser(
        _("Records requests to audit log, performs only permitted requests."),
        usage=_("usage: p4gf_auth_server.py [-h] [-V] [--user] [--keyfp] "
                "git-upload-pack | git-receive-pack [options] <repo>"))
    parser.add_argument(NTR('--user'),
                        metavar="",
                        help=_('Perforce user account requesting this action'))
    parser.add_argument(NTR('--keyfp'),
                        metavar="",
                        help=_('ssh key used to authenticate this connection'))
    parser.add_argument(NTR('command'),
                        metavar=NTR("command"),
                        nargs=1,
                        help=_('git-upload-pack or git-receive-pack, plus options'))
    parser.add_argument(NTR('options'),
                        metavar="",
                        nargs=argparse.REMAINDER,
                        help=_('options for git-upload-pack or git-receive-pack'))

    # reverse git's argument modifications
    # pylint:disable=anomalous-backslash-in-string
    # raw strings don't play well with this lambda function.
    fix_arg = lambda s: s.replace("'\!'", "!").replace("'\\''", "'")
    argv = [fix_arg(arg) for arg in argv]
    args = parser.parse_args(argv)
    if args.command[0] not in p4gf_server_common.COMMAND_TO_PERM:
        raise p4gf_server_common.CommandError(
            _("Unknown command '{bad}', must be one of '{good}'.")
            .format(bad=args.command[0],
                    good="', '".join(p4gf_server_common.COMMAND_TO_PERM.keys())),
            usage=parser.usage)

    if not args.options:
        raise p4gf_server_common.CommandError(
            _("Missing directory in '{cmd}' <repo>")
            .format(cmd=args.command[0]))

    # Carefully remove quotes from any repo name, allowing for imbalanced quotes.
    repo_name = args.options[-1]
    if repo_name[0] == '"' and repo_name[-1] == '"' or\
            repo_name[0] == "'" and repo_name[-1] == "'":
        repo_name = repo_name[1:-1]
    # Allow for git+ssh URLs where / separates host and repository.
    if repo_name[0] == '/':
        repo_name = repo_name[1:]
    args.options[-1] = repo_name

    # strip @foruser={user} option if present
    repo_name = re.sub(r'@foruser=[^ @]+', '', repo_name)

    # Reject impossible repo names/client spec names
    if (not is_special_command(repo_name) and
            not repo_name.startswith('@') and
            not p4gf_util.is_legal_repo_name(repo_name)):
        raise p4gf_server_common.CommandError(_("Illegal repo name '{repo_name}'")
                                              .format(repo_name=repo_name))

    for o in args.options[:-1]:
        if illegal_option(o):
            raise p4gf_server_common.CommandError(_("Illegal option: '{option}'")
                                                  .format(option=o))

    # Require --user if -V did not early return.
    if not args.user:
        raise p4gf_server_common.CommandError(_("--user required."),
                                              usage=parser.usage)

    # LOG.warning("running command: {}\n".format(argv)) ### DEBUG REMOVE FOR GA

    return args


@with_timer('git backend')
def _call_git(args, ctx):
    """
    Invoke the git command, returning its exit code.

    Arguments:
        args -- parsed command line arguments object.
        ctx -- context object.
    """
    # Pass to git-upload-pack/git-receive-pack. But with the repo
    # converted to an absolute path to the Git Fusion repo.
    converted_argv = args.options[:-1]
    converted_argv.append(ctx.repo_dirs.GIT_DIR)
    cmd_list = args.command + converted_argv
    fork_it = 'git-receive-pack' in args.command
    env = dict(os.environ)
    if ctx.foruser:
        env[p4gf_const.P4GF_FORUSER] = ctx.foruser

    if fork_it:
        LOG.debug2('_call_git() will fork push processing for {}'.format(ctx.config.repo_name))
        env[p4gf_const.P4GF_FORK_PUSH] = ctx.repo_lock.group_id

    ec = p4gf_proc.call(cmd_list, env=env)
    LOG.debug("_call_git() {} returned {}".format(cmd_list, ec))
    return ec


class AuthServer(p4gf_server_common.Server):

    """Server subclass for SSH."""

    def __init__(self, poll_only):
        super(AuthServer, self).__init__()
        if poll_only:
            self.skip_perms_check = True
            self.poll_only = True

    def before(self):
        """get args from command line."""
        args = parse_args(sys.argv[1:])
        if not args:
            raise p4gf_server_common.BadRequestException

        # Record the p4 user in environment. We use environment to pass to
        # git-invoked hook. We don't have to set ctx.authenticated_p4user because
        # Context.__init__() reads it from environment, which we set here.
        os.environ[p4gf_const.P4GF_AUTH_P4USER] = args.user

        # repo_name_git    is the untranslated repo name
        # repo_name        is the translated repo name

        # print "args={}".format(args)
        self.user           = args.user
        self.repo_name_git  = args.options[-1]
        self.command        = args.command[0]
        self.git_caller     = functools.partial(_call_git, args)

    def after(self):
        """override to do cleanup after process()."""
        # if poll_only , call 'git gc --auto'
        if self.poll_only:
            p4gf_git.git_gc_auto(self.git_dir)

    def push_received(self):
        """Push received if 'receive' in command."""
        return 'receive' in self.command

    def record_access(self):
        """Record the access of the repository in the audit log."""
        log_args = {
            'userName': self.user,
            'command': self.command,
            'repo': self.repo_name
        }
        p4gf_log.record_argv(log_args)

    def record_error(self, msg):
        """Record the given error message to the audit log."""
        args = {
            'userName': self.user,
            'command': self.command + NTR('-failed'),
            'repo': self.repo_name
        }
        p4gf_log.record_error(msg, args)


def _report_error(msg):
    """Report error via stderr and log file."""
    sys.stderr.write("{}\n".format(msg))
    LOG.error(msg)


@with_timer('SSH main')
def main(poll_only=False):
    """set up repo.

    repo_name_git    is the untranslated repo name
    repo_name        is the translated repo name
    """
    p4gf_proc.install_stack_dumper()
    p4gf_util.log_environ(LOG, os.environ, "SSH")

    encoding = sys.getfilesystemencoding()
    if encoding == 'ascii':
        # This encoding is wrong and will eventually lead to problems.
        _report_error(_("Using 'ascii' file encoding will ultimately result in errors, "
                        "please set LANG/LC_ALL to 'utf-8' in environment configuration."))
        return os.EX_CONFIG

    server = AuthServer(poll_only)

    try:
        return server.process()
    except p4gf_server_common.BadRequestException as e:
        _report_error(str(e))
        return os.EX_USAGE
    except p4gf_server_common.PerforceConnectionFailed:
        _report_error(_("Perforce connection failed"))
        return 2
    except p4gf_server_common.SpecialCommandException:
        return os.EX_OK
    except p4gf_server_common.RepoNotFoundException as e:
        _report_error(str(e))
        return 1
    except p4gf_server_common.RepoInitFailedException as e:
        _report_error(str(e))
        return 1
    except p4gf_server_common.ReadOnlyInstanceException as e:
        _report_error(str(e))
        return 1
    except p4gf_server_common.MissingSubmoduleImportUrlException:
        _report_error(_('Stream imports require an ssh-url'
                        ' be configured. Contact your administrator.'))
        return 0
    except p4gf_atomic_lock.LockConflict as lc:
        _report_error(str(lc))
    return os.EX_SOFTWARE


def main_ignores():
    """Call main() while ignoring certain exceptions.

    Ignore exceptions that we cannot do anything with and are best served by
    concisely logging their occurrence.
    """
    try:
        return main()
    except BrokenPipeError:
        LOG.warning("client connection terminated?")


def run_main():
    """Some initial logging, then run main()."""
    # Ensure any errors occurring in the setup are sent to stderr, while the
    # code below directs them to stderr once rather than twice.
    try:
        with p4gf_log.ExceptionLogger(squelch=False, write_to_stderr_=True):
            p4gf_version_3.log_version_extended()
            log_l10n()
            p4gf_version_3.version_check()
    except:  # pylint: disable=bare-except
        # Cannot continue if above code failed.
        sys.exit(1)
    # main() already writes errors to stderr, so don't let logger do it again
    p4gf_log.run_with_exception_logger(main_ignores, write_to_stderr=False)

if __name__ == "__main__":
    p4gf_version_3.print_and_exit_if_argv()
    run_main()

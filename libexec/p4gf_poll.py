#! /usr/bin/env python3.3
"""Command-line script to copy from Perforce to Git Fusion's internal repo.

Invokes code from the same script (p4gf_auth_server.py) that normal Git clients
invoke when they connect to Git Fusion over sshd, but passes "poll_only=True"
to suppress 'git pull' permission check or call to original git-upload-pack.
"""

import logging
import os
import sys

import p4gf_env_config    # pylint: disable=unused-import
import p4gf_auth_server
import p4gf_const
import p4gf_create_p4
from p4gf_l10n import _, NTR, log_l10n
import p4gf_log
import p4gf_util
import p4gf_version_3
import p4gf_repo_dirs
import p4gf_translate
import p4gf_lock

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_poll")


def repo_is_locked(repo_name):
    """Determine if the repo is locked by GF."""
    #
    # We need to open a new connection every time, because the server
    # common code closes all connections after each poll.
    #
    p4 = p4gf_create_p4.create_p4_temp_client()
    is_locked = False
    repo_name_tx = p4gf_translate.TranslateReponame.url_to_repo(repo_name, p4)
    # Do not block on repo lock - this will cause issues when called from cron
    try:
        with p4gf_lock.RepoLock(p4, repo_name_tx, blocking=False):
            pass
    except p4gf_lock.LockBusy:
        sys.stdout.write(_("View '{repo_name}' is locked. Skipping poll update.\n"
                         .format(repo_name=repo_name)))
        is_locked = True
    p4gf_create_p4.destroy(p4)
    return is_locked


def _list_for_server(p4):
    """Return list of repos that have been copied to the given Git Fusion server.

    "have been copied" here means "has a .git-fusion/views/<repo_name>/
    directory on this server."
    """
    result = []

    for repo_name in p4gf_util.repo_config_list(p4):
        repo_dirs = p4gf_repo_dirs.from_p4gf_dir(p4gf_const.P4GF_HOME, repo_name)
        if os.path.exists(repo_dirs.GIT_DIR):
            result.append(repo_name)
    return result


def _get_args():
    """Parse command-line args."""
    parser = p4gf_util.create_arg_parser(
        _("Update Git Fusion's internal repo(s) with recent changes from Perforce."))
    parser.add_argument('-a', '--all', action=NTR('store_true'),
                        help=_('Update all repos'))
    parser.add_argument('-v', '--verbose', action=NTR('store_true'),
                        help=_('List each repo updated.'))
    parser.add_argument(NTR('views'), metavar=NTR('view'), nargs='*',
                        help=_('name of view to update'))
    return parser.parse_args()


def main(args):
    """Invoke p4gf_auth_server as if we're responding to a 'git pull'."""
    # Check that either --all, --gc, or 'views' was specified.
    if not args.all and len(args.views) == 0:
        sys.stderr.write(_('Missing view names; try adding --all option.\n'))
        sys.exit(2)

    p4 = p4gf_create_p4.create_p4_temp_client()
    repo_list = _list_for_server(p4)
    if not args.all:
        bad_repos = [x for x in args.views
                     if p4gf_translate.TranslateReponame.url_to_repo(x, p4) not in repo_list]
        if bad_repos:
            p4gf_create_p4.destroy(p4)
            sys.stderr.write(_('One or more views are not defined on this server:\n\t'))
            sys.stderr.write('\n\t'.join(bad_repos))
            sys.stderr.write('\n')
            sys.stderr.write(_('Defined views:\n\t'))
            sys.stderr.write('\n\t'.join(repo_list))
            sys.stderr.write('\n')
            sys.exit(2)
        repo_list = args.views
    #
    # Because the server common code closes all connections, may as well
    # close this one now. We can't reuse it anyway.
    #
    p4gf_create_p4.destroy(p4)

    for repo_name in repo_list:
        if repo_is_locked(repo_name):
            if args.verbose:
                sys.stdout.write(_('Skipping locked repo: {}\n').format(repo_name))
            continue
        if args.verbose:
            sys.stdout.write(_('Updating: {}\n').format(repo_name))
        sys.argv = [
            'p4gf_auth_server.py',
            '--user={}'.format(p4gf_const.P4GF_USER),
            'git-upload-pack',
            repo_name
        ]
        p4gf_auth_server.main(poll_only=True)


def run_main():
    """Some initial logging, then run main()."""
    # Ensure any errors occurring in the setup are sent to stderr, while the
    # code below directs them to stderr once rather than twice.
    try:
        with p4gf_log.ExceptionLogger(squelch=False, write_to_stderr_=True):
            p4gf_version_3.log_version()
            log_l10n()
            p4gf_version_3.version_check()
    except SystemExit:
        # -V or --help options...
        sys.exit(0)
    except:  # pylint: disable=bare-except
        # Cannot continue if above code failed.
        sys.exit(1)

    # main() already writes errors to stderr, so don't let logger do it again
    args = _get_args()
    p4gf_log.run_with_exception_logger(main, args, write_to_stderr=False)

if __name__ == "__main__":
    run_main()

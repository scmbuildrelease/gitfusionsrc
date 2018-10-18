#! /usr/bin/env python3.3
"""Script to update the hook scripts in Git Fusion repositories.

Serves to easily update the hook scripts in the event the Git Fusion
administrator changes the location of the Git Fusion scripts.

"""

from contextlib import ExitStack

import sys

import P4

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_config
import p4gf_context
import p4gf_init_host
import p4gf_create_p4
from p4gf_l10n import _, NTR
import p4gf_lock
import p4gf_translate
import p4gf_util


def main():
    """Update one or more repository hook scripts."""
    parser = p4gf_util.create_arg_parser(
        _('Updates the hook scripts in one or more Git Fusion repositories.'))
    parser.add_argument('-a', '--all', action='store_true',
                        help=_('process all known Git Fusion repositories'))
    parser.add_argument(NTR('repos'), metavar=NTR('repo'), nargs='*',
                        help=_('name of repository to be updated'))
    args = parser.parse_args()

    # Check that either --all, or a repo was named.
    if not args.all and len(args.repos) == 0:
        sys.stderr.write(_('Missing repo names; try adding --all option.\n'))
        sys.exit(2)
    if args.all and len(args.repos) > 0:
        sys.stderr.write(_('Ambiguous arguments. Choose --all or a repo name.\n'))
        sys.exit(2)

    p4 = p4gf_create_p4.create_p4_temp_client()
    if not p4:
        sys.exit(2)

    # Sanity check the connection (e.g. user logged in?) before proceeding.
    try:
        p4.fetch_client()
    except P4.P4Exception as e:
        sys.stderr.write(_('P4 exception occurred: {exception}').format(exception=e))
        sys.exit(1)

    if args.all:
        repos = p4gf_util.repo_config_list(p4)
        if not repos:
            print(_("No repos exist yet."))
    else:
        repos = args.repos
    p4gf_create_p4.p4_disconnect(p4)

    have_error = False
    for git_view in repos:
        repo_name = p4gf_translate.TranslateReponame.git_to_repo(git_view)
        print(_("Processing repository {repo_name}...").format(repo_name=repo_name), end='')
        try:
            ctx = p4gf_context.create_context(repo_name)
            ctx.create_config_if_missing(False)
            with ExitStack() as stack:
                stack.enter_context(ctx)
                ctx.repo_lock = p4gf_lock.RepoLock(ctx.p4gf, repo_name, blocking=False)
                stack.enter_context(ctx.repo_lock)
                # If __file__ contains a symlink, decoding at this top level
                # will cause Python to retain it, for use in the hook paths.
                p4gf_init_host.install_hook(ctx.repo_dirs.GIT_DIR,
                                            overwrite=True, hook_abs_path=__file__)
            print(_(" successful."))
        except p4gf_config.ConfigLoadError as e:
            import logging
            # cannot use __name__ since it will be "__main__"
            logging.getLogger("p4gf_update_hooks").exception("failed to update hooks")
            print(_(" failed."))
            sys.stderr.write(
                _("\n{exception}\nHook scripts not updated for repo '{repo_name}'.")
                .format(exception=e, repo_name=repo_name))
            have_error = True
    if have_error:
        sys.exit(1)


if __name__ == "__main__":
    main()

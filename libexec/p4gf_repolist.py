#! /usr/bin/env python3.3
"""Get list of repos."""

import p4gf_config
import p4gf_group
from p4gf_l10n import NTR
import p4gf_util
import p4gf_read_permission


class RepoList:

    """build list of repos available to user."""

    def __init__(self):
        """empty list."""
        self.repos = []

    @staticmethod
    def list_for_user(p4, user):
        """build list of repos visible to user."""
        result = RepoList()

        for repo in p4gf_util.repo_config_list(p4):
            # check user permissions for repo
            # PERM_PUSH will avoid checking the repo config file for read-permission-check = user
            repo_perm = p4gf_group.RepoPerm.for_user_and_repo(
                p4, user, repo, p4gf_group.PERM_PUSH)
            # sys.stderr.write("repo: {}, user: {}, perm: {}".format(repo, user, repo_perm))
            if repo_perm.can_push():
                perm = NTR('push')
                # If use fails check-read-permissions don't add (as PUSH)
                if not p4gf_read_permission.user_has_read_permissions(
                        p4, repo_perm, p4gf_group.PERM_PULL):
                    continue

            elif repo_perm.can_pull():
                perm = NTR('pull')
                # If use fails check-read-permissions don't add (as PULL)
                if not p4gf_read_permission.user_has_read_permissions(
                        p4, repo_perm, p4gf_group.PERM_PULL):
                    continue
            else:
                continue

            if not p4gf_util.is_legal_repo_name(repo):
                continue

            repo_config = p4gf_config.RepoConfig.from_depot_file(repo, p4)
            charset = repo_config.get(p4gf_config.SECTION_REPO_CREATION,
                                      p4gf_config.KEY_CHARSET)

            desc = ''
            if repo_config.has_option(p4gf_config.SECTION_REPO, p4gf_config.KEY_DESCRIPTION):
                desc = repo_config.get(p4gf_config.SECTION_REPO,
                                       p4gf_config.KEY_DESCRIPTION)

            result.repos.append((repo, perm, charset, desc))

        result.repos.sort(key=lambda tup: tup[0])
        return result

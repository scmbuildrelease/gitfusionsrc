#! /usr/bin/env python3.3
"""Code to look up a Git commit's corresponding Perforce user."""

import logging

import p4gf_config

LOG = logging.getLogger(__name__)


class G2PUser:
    """A class that knows how to find Perforce users
    that correspond to a Git commit's author/pusher/owner.
    """
    def __init__( self, *
                , ctx
                , usermap
                ):
        self.ctx            = ctx
        self.usermap        = usermap
                        # dict of pair ("email_addr", "git_user")
                        # to Usermap.lookup() result tuple.
                        # Saves us repeated trips to the usermap
                        # and its O(n) scan.
        self.usermap_cache  = {}

    def get_acp(self, fecommit):
        """Return a dict with values set to the Perforce user id for
        - author
        - committer
        - pusher

        Values set to None if no corresponding Perforce user id.

        Separate from and superset of _get_author_pusher_owner(). Called only
        for Git Swarm reviews because only Git Swarm reviews care about
        Git committer.
        """
        return { 'author'    : self._git_to_p4_user(fecommit, 'author')
               , 'committer' : self._git_to_p4_user(fecommit, 'committer')
               , 'pusher'    : self.ctx.authenticated_p4user
               }

    def get_author_pusher_owner(self, commit):
        """Add to commit: p4 user id for: Git author, Git pusher and p4 change owner

        Retrieve the Perforce user performing the push, and the original
        author of the Git commit, if a known Perforce user, or unknown_git
        if that user is available.

        If the ignore-author-permissions config setting is false, or the
        change-owner is set to 'author', then the commit author must be a
        valid Perforce user.
        """
        pusher = self.ctx.authenticated_p4user
        if self.ctx.owner_is_author:
            author = self._git_to_p4_user(commit, 'author')
            change_owner = author
        elif self.ctx.foruser:
            author = self.ctx.foruser
            change_owner = self.ctx.foruser
        else:
            author = pusher
            change_owner = pusher
        commit['author_p4user'] = author
        commit['pusher_p4user'] = pusher
        commit['owner'] = change_owner

    def _git_to_p4_user(self, fecommit, fecommit_key):
        """Return the Perforce user that corresponds to a given Git commit
        key 'author' or 'committer'.
        """
        auth_src_cfg = self.ctx.repo_config.get(p4gf_config.SECTION_AUTHENTICATION,
                                                p4gf_config.KEY_AUTHOR_SOURCE)
        if auth_src_cfg is None:
            # The default is to use the email address in its entirety.
            author_source = [p4gf_config.VALUE_GIT_EMAIL]
        else:
            # Otherwise the sources are comma-separated and in order of
            # precedence -- first match wins.
            author_source = [s.strip() for s in auth_src_cfg.split(',')]
        user = None
        email_addr = fecommit[fecommit_key]['email'].strip('<>')
        git_user = fecommit[fecommit_key]['user']
        (cached, result) = self._get_usermap_cache(email_addr, git_user)
        if cached:
            return result

        for source in author_source:
            if source == p4gf_config.VALUE_GIT_EMAIL:
                LOG.debug2("_git_to_p4_user(GIT_EMAIL) for email {} user {}"
                           .format(email_addr, user))
                user = self.usermap.lookup_by_email_with_subdomains(email_addr)
            elif source == p4gf_config.VALUE_GIT_EMAIL_ACCT:
                email_acct = email_addr.split('@', 1)[0]
                LOG.debug2("_git_to_p4_user(GIT_EMAIL_ACCT) for emailacct {}"
                           " user {}".format(email_acct, user))
                user = self.usermap.lookup_by_p4user(email_acct)
            elif source == p4gf_config.VALUE_GIT_USER:
                LOG.debug2("_git_to_p4_user(GIT_USER) for email {} gituser {}"
                           .format(email_addr, git_user))
                user = self.usermap.lookup_by_p4user(git_user)
            else:
                LOG.warning('ignoring unknown author-source value {}'.format(source))
            if user is not None:
                LOG.debug2("_git_to_p4_user() for email {} found user {}".format(email_addr, user))
                break
        if user is None:
            user = self.usermap.lookup_unknown_git()
        if (user is None) or (not self.usermap.p4user_exists(user[0])):
            result = None
        else:
            result = user[0]
        self._set_usermap_cache(email_addr, git_user, result)
        return result

    def _get_usermap_cache(self, email_addr, git_user):
        """If we've already looked up and found a Perforce user for this
        Git user, return that. Don't keep looking things up over and over.

        None is a valid cache hit, so return a 2-tuple with
        ("got a hit?", "result")
        """
        key = (email_addr, git_user)
        if key in self.usermap_cache:
            return (True, self.usermap_cache.get(key))
        else:
            return (False, None)

    def _set_usermap_cache(self, email_addr, git_user, result):
        """Cache the result for this usermap lookup so we never have
        to look for it again.
        """
        key = (email_addr, git_user)
        self.usermap_cache[key] = result

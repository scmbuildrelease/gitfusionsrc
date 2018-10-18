#! /usr/bin/env python3.3
"""Script that manages the user map file in Git Fusion.

The user map consists of Perforce user names mapped to the email addresses
that appear in the Git commit logs. This is used to associated Git authors
with Perforce users, for purposes of attribution. The Perforce user
accounts are typically mapped automatically by searching for an account
with the same email address as the Git author. In cases where the email
addresses are not the same, the Perforce administrator may add a mapping to
the p4gf_usermap file.

"""

import logging
import os
import re
import sys

import p4gf_env_config    # pylint: disable=unused-import
import p4gf_p4cache
import p4gf_const
import p4gf_create_p4
import p4gf_init
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_log
import p4gf_p4spec
import p4gf_p4user
import p4gf_path
import p4gf_util

LOG = logging.getLogger('p4gf_usermap')

# Only sync the user map once per run.
_user_map_synced = False
USERMAP_REGEX = re.compile('([^ \t]+)[ \t]+([^ \t]+)[ \t]+"?([^"]+)"?')
_VALIDATE_EMAIL_ILLEGAL = '<>,'


def _split_email_with_domain_lower_cased(email):
    """Split email on the last @.

    Return the tuple (email_user, email_domain.lower()) """
    last_at = email.rfind('@')
    return (email[:last_at], email[last_at+1:].lower())


def _find_by_tuple_index(index, find_value, users, email_case_sensitivity=False):
    """Return the first matching element of tuple_list that matches find_value.

    Return None if not found.
    """
    email_lookup = index == TUPLE_INDEX_EMAIL
    if email_lookup:
        if email_case_sensitivity:
            find_name, find_domain = _split_email_with_domain_lower_cased(find_value)
        else:
            find_value_lower = find_value.lower()

    for usr in users:
        if email_lookup:
            if email_case_sensitivity:
                usr_email, usr_domain = _split_email_with_domain_lower_cased(usr[index])
                if usr_email == find_name and usr_domain == find_domain:
                    return usr
            else:
                if usr[index].lower() == find_value_lower:
                    return usr
        else:
            if usr[index] == find_value:
                return usr
    return None

# Because tuple indexing is less work for Zig than converting to NamedTuple
TUPLE_INDEX_P4USER   = 0
TUPLE_INDEX_EMAIL    = 1
TUPLE_INDEX_FULLNAME = 2


def tuple_to_P4User(um_3tuple):  # pylint: disable=invalid-name
                                 # The correct type is P4User, not p4user.
    """Convert one of our 3-tuples to a P4User."""
    p4user = p4gf_p4user.P4User()
    p4user.name      = um_3tuple[TUPLE_INDEX_P4USER  ]
    p4user.email     = um_3tuple[TUPLE_INDEX_EMAIL   ]
    p4user.full_name = um_3tuple[TUPLE_INDEX_FULLNAME]
    return p4user


class UserMap:

    """Mapping of Git authors to Perforce users.

    Caches the lists of users to improve performance when performing repeated
    searches (e.g. when processing a Git push consisting of many commits).
    """

    def __init__(self, p4, email_case_sensitivity=False):
        # List of 3-tuples: first whatever's loaded from p4gf_usermap,
        # then followed by single tuples fetched from 'p4 users' to
        # satisfy later lookup_by_xxx() requests.
        self.users = None

        # List of 3-tuples, filled in only if needed.
        # Complete list of all Perforce user specs, as 3-tuples.
        self._p4_users = None

        self.p4 = p4
        self._case_sensitive = None
        self._email_case_sensitivity = email_case_sensitivity

    def _is_case_sensitive(self):
        """Return True if the server indicates case-handling is 'sensitive'.

        Return False otherwise.
        """
        if self._case_sensitive is None:
            info = p4gf_p4cache.fetch_info(self.p4)
            self._case_sensitive = info.get('caseHandling') == 'sensitive'
        return self._case_sensitive

    def _read_user_map(self):
        """Read the user map file from Perforce into a list of tuples.

        Tuples consist of username, email address, and full name. If no
        such file exists, return an empty list.

        Return a list of 3-tuples: (p4user, email, fullname)
        """
        usermap = []
        all_existing_mapped_users = {}   # value True indicates standard type
        mappath = p4gf_const.P4GF_HOME + '/users/p4gf_usermap'

        global _user_map_synced
        if not _user_map_synced:
            # don't let a writable usermap file get in our way
            self.p4.run('sync', '-fq', mappath)
            _user_map_synced = True

        if not os.path.exists(mappath):
            return usermap

        with open(mappath) as mf:
            no_folding = self._is_case_sensitive()
            for line in mf:
                if not line:
                    continue
                line = line.strip()
                if not line or line[0] == '#':
                    continue
                m = USERMAP_REGEX.search(line)
                if not m:
                    LOG.debug('No match: {}'.format(line))
                    continue

                p4user = m.group(1) if no_folding else m.group(1).casefold()
                email = m.group(2)
                _validate_email(email)
                fullname = p4gf_path.dequote(m.group(3))
                # Do not load a usermap for P4GF_USER - git-fusion-user
                if p4user == p4gf_const.P4GF_USER:
                    LOG.warning("{0} user disallowed in usermap. Skipping: {1}".format(
                        p4gf_const.P4GF_USER, line))
                    continue
                # Disallow any mapped existing users which are not of standard type
                if p4user in all_existing_mapped_users:
                    if not all_existing_mapped_users[p4user]:
                        LOG.warning("non standard user {0} disallowed in usermap. "
                                    "Skipping: {1}".format(p4user, line))
                        continue
                else:
                    if p4gf_p4spec.spec_exists(self.p4, 'user', p4user):
                        all_existing_mapped_users[p4user] = p4gf_p4spec.spec_values_match(
                            self.p4, 'user', p4user, {'Type': 'standard'})
                        if not all_existing_mapped_users[p4user]:
                            LOG.warning("non standard user {0} disallowed in usermap. "
                                        "Skipping: {1}".format(p4user, line))
                            continue

                usermap.append((p4user, email, fullname))
        return usermap

    @property
    def p4_users(self):
        """Retrieve the set of users registered in the Perforce server, in a
        list of tuples consisting of username, email address, and full name.
        If no users exist, an empty list is returned.

        Returns a list of 3-tuples: (p4user, email, fullname)
        """
        # lazy init
        if not self._p4_users:
            self._p4_users = []
            results = self.p4.run('users')
            if results:
                no_folding = self._is_case_sensitive()
                for r in results:
                    name = r['User'] if no_folding else r['User'].casefold()
                    self._p4_users.append((name, r['Email'], r['FullName']))
        return self._p4_users

    def _lookup_by_tuple_index(self, index, value):
        """Return 3-tuple for user whose tuple matches requested value.

        Searches in order:
        * p4gf_usermap (stored in first portion of self.users)
        * previous lookup results (stored in last portion of self.users)
        * 'p4 users' (stored in self.p4_users)

        Lazy-fetches p4gf_usermap and 'p4 users' as needed.

        O(n) list scan.

        """
                        # Empty list is a valid and common result of
                        # _read_user_map(). Don't keep re-parsing the same
                        # empty file over and over.

        if self.users is None:
            self.users = self._read_user_map()
        # Look for user in existing map. If found return. We're done.
        user = _find_by_tuple_index(index, value, self.users, self._email_case_sensitivity)
        if user:
            return user

        # Look for user in Perforce.
        user = _find_by_tuple_index(index, value, self.p4_users, self._email_case_sensitivity)

        if user:
            # Found. Append to our hit list so that
            # _find_by_tuple_index() will see it next time,
            # without a trip to 'p4 users'.
            self.users.append(user)

        return user

    def lookup_unknown_git(self):
        """Scan for "unknown git" in our results and return its 3-tuple."""
        return _find_by_tuple_index(TUPLE_INDEX_P4USER,
                                    p4gf_const.P4GF_UNKNOWN_USER,
                                    self.p4_users)

    def lookup_by_email_with_subdomains(self, addr):
        """Match "bob@host.company.com" or "bob@company.com", looping through
        possible subdomains until we get a hit, or fall off the end of the
        loop.
        """
        for sub in email_subdomain_iter(addr):
            result = self._lookup_by_tuple_index(TUPLE_INDEX_EMAIL, sub)
            if result:
                return result
        return None

    def lookup_by_email(self, addr):
        """Retrieve details for user by their email address.

        Return a tuple consisting of the user name, email address, and full
        name. First search the p4gf_usermap file in the .git-fusion workspace,
        then search the Perforce users. Return None if not found.

        See also lookup_by_email_with_subdomains().
        """
        return self._lookup_by_tuple_index(TUPLE_INDEX_EMAIL, addr)

    def lookup_by_p4user(self, p4user):
        """Return 3-tuple for given Perforce user."""
        if not self._is_case_sensitive():
            p4user = p4user.casefold()
        return self._lookup_by_tuple_index(TUPLE_INDEX_P4USER, p4user)

    def p4user_exists(self, p4user):
        """Return True if we saw this p4user in 'p4 users' list."""
        # Look for user in Perforce.
        if not self._is_case_sensitive():
            p4user = p4user.casefold()
        user = _find_by_tuple_index(TUPLE_INDEX_P4USER, p4user, self.p4_users)
        if user:
            return True
        return False


def _validate_email(email):
    """Raise error upon unwanted <>."""
    LOG.debug('checking email: {}'.format(email))
    for c in _VALIDATE_EMAIL_ILLEGAL:

        if c in email:
            LOG.error('Nope {} in {}'.format(c, email))
            raise RuntimeError(
                _("Unable to read '{usermap}'."
                  " Illegal character '{c}' in email address '{email}'")
                .format( usermap = 'p4gf_usermap'
                       , c       = c
                       , email   = email))


def email_subdomain_iter(email):
    """Iterator/generator produces ['x@a.b.c', 'x@b.c', 'x@c'] for 'x@a.b.c'.

    So that "bob@dhcp-host.company.com" can match "bob@company.com".
    """
    last_at     = email.rfind('@')
    if last_at <= 0:    # No @ sign (or an @ sign at front of string?)
        return email    # Then I don't know what to skip.

    account_at  = email[:last_at + 1]
    full_domain = email[last_at + 1:]
    dlist       = full_domain.split('.')
    yield from (account_at + '.'.join(dlist[i:]) for i in range(len(dlist)))


def main():
    """Parse the command line arguments and perform a search for the given
    email address in the user map.
    """
    p4gf_util.has_server_id_or_exit()
    log_l10n()

    # Set up argument parsing.
    parser = p4gf_util.create_arg_parser(
        _("Searches for an email address in the user map."))
    parser.add_argument(NTR('email'), metavar='E',
                        help=_('email address to find'))
    args = parser.parse_args()

    # make sure the world is sane
    ec = p4gf_init.main()
    if ec:
        print(_("p4gf_usermap initialization failed"))
        sys.exit(ec)

    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4_temp_client()
        if not p4:
            sys.exit(1)

        usermap = UserMap(p4)
        user = usermap.lookup_by_email(args.email)
        if user:
            print(_("Found user '{user}' <{fullname}>")
                  .format(user=user[0], fullname=user[2]))
            sys.exit(0)

        unknown_git = usermap.lookup_unknown_git()
        if unknown_git:
            print(_("No such user found: '{email}'\n").format(email=args.email))
            print(_("Found user '{user}' <{fullname}>")
                  .format(user=unknown_git[0], fullname=unknown_git[2]))
            sys.exit(0)

        sys.stderr.write(_("No such user found: '{email}'\n").format(email=args.email))
        sys.exit(1)


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

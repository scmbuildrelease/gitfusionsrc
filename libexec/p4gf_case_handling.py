#! /usr/bin/env python3.3
"""A Git to Perforce branch association.

Mostly just a wrapper for one section of a repo config file.

"""
import logging

import pygit2

import p4gf_env_config  # pylint:disable=unused-import

LOG = logging.getLogger(__name__)


CaseHandlingString = None


def init_case_handling(p4=None):
    """Set up CaseHandlingString class based on case sensitivity of server.

    If the server is case-sensitive, avoid extra overhead and just make use
    regular strings.
    """
    if p4 is not None:
        insensitive = p4.server_case_insensitive
    else:
        try:
            # May not be in a git directory during unit tests
            path = pygit2.discover_repository('.')
            repo = pygit2.Repository(path)
            insensitive = repo.config.get_bool('core.ignorecase')
        except KeyError:
            insensitive = False

    global CaseHandlingString
    if insensitive:
        LOG.debug("CaseHandlingString will be CaseInsensitiveString")
        CaseHandlingString = CaseInsensitiveString
    else:
        LOG.debug("CaseHandlingString will be str")
        CaseHandlingString = str


class CaseInsensitiveString(str):

    """String subclass that compares according to server's case sensitivity.

    A branch id or branch ref is just a string, but it needs to be compared
    in a case-insensitive fashion if the server is case insensitive.  An easy
    way to do that is to wrap it with a class that knows how to do the right
    type of compares.

    In order to be used as a dict key, the class needs to be hashable, meaning
    it needs __hash__() and __eq__().  For good measure, __ne__() is overridden
    too.
    """

    # str does indeed have many public methods
    # pylint: disable=too-many-public-methods

    def __eq__(self, other):
        """Compare lowercased strings."""
        return other is not None and self.lower() == other.lower()

    def __ne__(self, other):
        """Compare lowercased strings."""
        return other is None or self.lower() != other.lower()

    def __hash__(self):
        """Return hash of lowercased string."""
        return hash(self.lower())

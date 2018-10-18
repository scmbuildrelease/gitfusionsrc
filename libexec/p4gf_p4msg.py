#! /usr/bin/env python3.3
"""Utilities for dealing with P4.Message and p4.messages list."""

# Do not import p4gf_version_3 here. This file must be accessible from
# Python 2.6 for OVA web UI.

import p4gf_p4msgid


def msg_repr(msg):
    """P4.Message.__repr__() strips out msgid. This does not.

    Return a string like this:
        gen=EV_ADMIN/35 sev=E_FAILED/3 msgid=6600 Protections table is empty.
    """
    return ("gen={gen_t}/{gen} sev={sev_t}/{sev} msgid={msgid} {str}"
            .format( gen_t = p4gf_p4msgid.generic_to_text (msg.generic)
                   , gen   =                             msg.generic
                   , sev_t = p4gf_p4msgid.severity_to_text(msg.severity)
                   , sev   =                             msg.severity
                   , msgid =                             msg.msgid
                   , str   =                         str(msg)
                   ))


def find_msgid(p4, msgids, allcodes=False):
    """Return all p4.messages that match any of the requested ids.

    msgids: may be a single value or a list of values to find.
    allcodes: if True, searches all message codes for msgids
              if False, only looks at each message's first code
    """
    if not isinstance(msgids, list):
        msgids = [msgids]
    msgids = set(msgids)

    def codes(msg):
        """Return a list of unique codes in the message, respecting 'allcodes'."""
        if allcodes:
            return set([int(code) & 0xffff for code in msg.dict['code']])
        else:
            return set([msg.msgid])

    return [m for m in p4.messages if codes(m) & msgids]


def contains_protect_error(p4):
    """Check if this P4 object contains a "You don't have permission..." error.

    P4Exception does not include the error severity/generic/msgid, have to
    dig through P4.messages not P4.errors for numeric codes instead of US
    English message strings.
    """
    for m in p4.messages:
        if (    p4gf_p4msgid.E_FAILED   <= m.severity
            and p4gf_p4msgid.EV_PROTECT == m.generic):
            return True
    return False


def first_fatal_error(p4):
    """Scan the p4.messages list for possible fatal errors.

    :param p4: the P4 API object to examine.

    Returns the first fatal message found, or None if none.

    """
    for m in p4.messages:
        if m.severity == p4gf_p4msgid.E_FATAL:
            return m
    return None

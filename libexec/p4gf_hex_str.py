#! /usr/bin/env python3.3
"""Utilities for hex int/str conversion.

Promoted to their own .py file so that we can import these without importing
p4gf_util.py and its humorously large transitive closure.
"""

from   p4gf_l10n    import _

# Please avoid importing any p4gf_xxx modules here.

def md5_int(md5):
    """Convert an MD5 or sha1 hex string to an integer.
    NOP if fed an integer.

    Both 32-digit MD5 and 40-digit sha1 consume MUCH less space as
    int than as str:

    md5 sha1
    81  89   str bytes
    44  49   int bytes
    """
    if isinstance(md5, str):
        return int(md5, 16)
    elif isinstance(md5, int):
        return md5
    raise RuntimeError(_("Unexpected type {type} for md5")
                       .format(type=md5.__class__))


def md5_str(md5):
    """Convert an MD5 to an uppercase hex string.
    NOP if fed a string.

    Counterpart to md5_int().
    """
    if isinstance(md5, str):
        return md5
    elif isinstance(md5, int):
        return "{:0>32x}".format(md5).upper()
    raise RuntimeError(_("Unexpected type {type} for md5")
                       .format(type=md5.__class__))

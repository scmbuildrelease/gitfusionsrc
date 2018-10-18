#! /usr/bin/env python3.3
""" P4File class."""

import binascii
import locale
import sys

import p4gf_char
from   p4gf_l10n      import _, NTR


def update_type_string(old_type):
    """Convert old style perforce type name to new style."""
    old_filetypes = NTR({
    'ctempobj'  : 'binary+Sw',
    'ctext'     : 'text+C',
    'cxtext'    : 'text+Cx',
    'ktext'     : 'text+k',
    'kxtext'    : 'text+kx',
    'ltext'     : 'text+F',
    'tempobj'   : 'binary+FSw',
    'ubinary'   : 'binary+F',
    'uresource' : 'resource+F',
    'uxbinary'  : 'binary+Fx',
    'xbinary'   : 'binary+x',
    'xltext'    : 'text+Fx',
    'xtempobj'  : 'binary+Swx',
    'xtext'     : 'text+x',
    'xunicode'  : 'unicode+x',
    'xutf16'    : 'utf16+x',
    })
    if old_type in old_filetypes:
        return old_filetypes[old_type]
    return old_type


def has_type_modifier(typestring, modifier):
    """Check a perforce filetype for a +modifier, e.g. +x."""
    parts = update_type_string(typestring).split('+')
    if len(parts) < 2:
        return False
    return parts[1].find(modifier) != -1


def string_from_print(d):
    """Create a string from p4 print dict.

    This is a noop for unicode servers, because p4python returns strings.

    But for non-unicode servers, when running 'p4 print' we use "raw" encoding
    with p4python to avoid mangling file content, so we get back bytes from
    p4python, which need to be decoded. Usually those bytes are utf8,
    but that's not guaranteed. Let p4gf_char.decode() find a match.
    """
    if type(d) == str:
        return sys.intern(d)
    try:
        s = p4gf_char.decode(d)
        #return sys.intern(d.decode(locale.nl_langinfo(locale.CODESET)))
        return sys.intern(s)
    except UnicodeDecodeError:
        replaced = d.decode(locale.nl_langinfo(locale.CODESET), 'replace').replace('\ufffd', '?')
        raise RuntimeError(_('Error decoding file path: {path}').format(path=replaced))


class P4File:

    """A file, as reported by p4 describe or p4 sync.

    Also contains SHA1 of file content, if that has been set.
    """
    _fstat_cols = None

    def __init__(self):
        self.depot_path = None
        self.action = None
        self._revision = None
        # The SHA1 is stored as a sequence of bytes, which are converted to
        # a string as needed. The purpose is to keep the memory footprint
        # small since there will be many instances of P4File in memory
        # during the p4-to-git processing.
        self._sha1 = b''
        self.type = ""
        self._change = None
        self.gwt_path = None

    @property
    def sha1(self):
        """Get sha1."""
        return binascii.hexlify(self._sha1).decode()

    @sha1.setter
    def sha1(self, value):
        """Set sha1."""
        self._sha1 = binascii.unhexlify(value)

    @property
    def revision(self):
        """Get revision."""
        return self._revision

    @revision.setter
    def revision(self, value):
        """Set revision."""
        if not isinstance(value, int):
            value = int(value)
        self._revision = value

    @property
    def change(self):
        """Get change."""
        return self._change

    @change.setter
    def change(self, value):
        """Set change."""
        if not isinstance(value, int):
            value = int(value)
        self._change = value

    @staticmethod
    def create_from_describe(vardict, index):
        """Create P4File from p4 describe.

        Describe does not report the client path, but that will be
        reported later by p4 sync and set on the P4File at that time.
        """
        # pylint: disable=protected-access
        f = P4File()
        f.depot_path = sys.intern(vardict["depotFile"][index])
        f.type = sys.intern(vardict["type"][index])
        f.action = sys.intern(vardict["action"][index])
        f._revision = int(vardict["rev"][index])
        return f

    @staticmethod
    def create_from_print(vardict):
        """Create P4File from p4 print."""
        # pylint: disable=protected-access
        f = P4File()
        f.depot_path = string_from_print(vardict["depotFile"])
        f.action     = string_from_print(vardict["action"])
        f._revision  = int(vardict["rev"])
        f.type       = string_from_print(vardict["type"])
        f._change    = int(vardict["change"])
        return f

    @staticmethod
    def create_from_files(vardict):
        """Create P4File from p4 files."""
        # pylint: disable=protected-access
        f = P4File()
        f.depot_path =     vardict["depotFile"]
        f._revision  =     vardict["rev"      ]
        f._change    = int(vardict["change"   ])
        f.action     =     vardict["action"   ]
        f.type       =     vardict["type"     ]
        return f

    @staticmethod
    def create_from_filelog(vardict):
        """Create P4File from p4 filelog.

        Note that 'p4 filelog' dict values are mostly single-element lists, not
        values. This is to accomodate the rarely seen opportunity to have
        multiple integ actions in a single changelist. For P4File's
        depotFile/rev/action needs, the first element from these lists is
        usually enough. When not, those brains probably belong outside of
        P4File, in calling code, not in this simple struct-like class.

        {
        , 'depotFile'   : '//depot/main/git-fusion/bin/p4gf_super_init.py'
        , 'action'      : ['edit']
        , 'rev'         : ['31']
        , 'type'        : ['xtext']
        , 'change'      : ['698466']
        ...
        }
        """
        # pylint: disable=protected-access
        f = P4File()
        f.depot_path = string_from_print(vardict["depotFile"]   )
        f.action     = string_from_print(vardict["action"   ][0])
        f._revision  = int(              vardict["rev"      ][0])
        f.type       = string_from_print(vardict["type"     ][0])
        f._change    = int(              vardict["change"   ][0])
        return f

    def is_delete(self):
        """Return True if fie is deleted at this revision."""
        return self.action == "delete" or self.action == "move/delete"

    def rev_path(self):
        """Return depotPath#rev."""
        return self.depot_path + "#" + str(self._revision)

    def is_k_type(self):
        """Return True if file type uses keyword expansion."""
        return has_type_modifier(self.type, "k")

    def is_x_type(self):
        """Return True if file is executable type."""
        return has_type_modifier(self.type, "x")

    def is_symlink(self):
        """Return True if file is a symlink type."""
        return self.type.startswith("symlink")

    def equal(self, path, rev):
        """Compare this object to the given path and revision."""
        if not isinstance(rev, int):
            rev = int(rev)
        return self.depot_path == path and self._revision == rev

    def less_than(self, path, rev):
        """Compare this object to the given path and revision for order."""
        if not isinstance(rev, int):
            rev = int(rev)
        if self.depot_path == path:
            return self._revision < rev
        return self.depot_path < path

    # pylint: disable=protected-access

    def __eq__(self, other):
        return self.depot_path == other.depot_path and self._revision == other._revision

    def __ne__(self, other):
        return self.depot_path != other.depot_path or self._revision != other._revision

    def __gt__(self, other):
        if self.depot_path == other.depot_path:
            return self._revision > other._revision
        return self.depot_path > other.depot_path

    def __ge__(self, other):
        return self.depot_path >= other.depot_path and self._revision >= other._revision

    def __lt__(self, other):
        if self.depot_path == other.depot_path:
            return self._revision < other._revision
        return self.depot_path < other.depot_path

    def __le__(self, other):
        return self.depot_path <= other.depot_path and self._revision <= other._revision

    def __str__(self):
        return self.depot_path

    def __repr__(self):
        return ("depot_path: {0}, revision: {1}, type: {2}, action: {3},"
                " sha1: {4} gwt: {5}"
                .format( self.depot_path, self._revision, self.type
                       , self.action, self.sha1, self.gwt_path ))

    def __sizeof__(self):
        size = sys.getsizeof(self.depot_path)
        size += sys.getsizeof(self.action)
        size += sys.getsizeof(self._revision)
        size += sys.getsizeof(self._sha1)
        size += sys.getsizeof(self.type)
        size += sys.getsizeof(self._change)
        return size

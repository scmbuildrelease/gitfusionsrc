#! /usr/bin/env python3.3
"""Perforce filetypes handling."""
from   p4gf_l10n      import NTR

# See 'p4 help filetypes'
#	       Type        Is Base Type  Plus Modifiers
#	      --------    ------------  --------------
ALIASES = {
	      'ctempobj' : ['binary',    'S', 'w'       ]
	    , 'ctext'    : ['text',      'C'            ]
	    , 'cxtext'   : ['text',      'C', 'x'       ]
	    , 'ktext'    : ['text',      'k'            ]
	    , 'kxtext'   : ['text',      'k', 'x'       ]
	    , 'ltext'    : ['text',      'F'            ]
	    , 'tempobj'  : ['binary',    'F', 'S', 'w'  ]
	    , 'ubinary'  : ['binary',    'F'            ]
	    , 'uresource': ['resource',  'F'            ]
	    , 'uxbinary' : ['binary',    'F', 'x'       ]
	    , 'xbinary'  : ['binary',    'x'            ]
	    , 'xltext'   : ['text',      'F', 'x'       ]
	    , 'xtempobj' : ['binary',    'S', 'w', 'x'  ]
	    , 'xtext'    : ['text',      'x'            ]
	    , 'xunicode' : ['unicode',   'x'            ]
	    , 'xutf16'   : ['utf16',     'x'            ]
	    , 'xutf8'    : ['utf8',      'x'            ]
        }

BASES = {
      "text"
    , "binary"
    , "symlink"
    , "apple"
    , "resource"
    , "unicode"
    , "utf16"
    , "utf8"
}

def to_base_mods(filetype):
    """Split a string p4filetype like "xtext" into an array of 2+ strings.

    'text'      => ['text', '' ]
    "xtext"     => ['text', 'x']
    "+x"        => ['',     'x']
    "ktext+S10" => ['text', 'k', 'S', '1', '0']

    Invalid filetypes produce undefined results.

    Multi-char filetypes like +S1 become multiple elements in the returned list.
    """

    # +S<n> works only because we tear down and rebuild our + mod chars in
    # the same sequence. We actually treat +S10 as +S +1 +0, then rebuild
    # that to +S10 and it just works. Phew.

    # Just in case we got 'xtext+k', split off any previous mods.
    base_mod = filetype.split('+')
    # convert the string values in a (possible empty) subarray to array of chars
    mods = list(''.join(base_mod[1:]))
    base = base_mod[0]
    if mods:
        # Try again with just the base.
        base_mod = to_base_mods(base)
        if base_mod[1]:
            mods += base_mod[1:]
            base = base_mod[0]

    if base in ALIASES:
        x = ALIASES[base]
        base = x[0]
        if mods:
            mods += x[1:]
        else:
            mods = x[1:]

                        # Re-combine 'S' '1' '0' into 'S10"
    if 'S' in mods:
        i = mods.index('S')
        while i + 1 < len(mods) and mods[i + 1] in "0123456789":
            mods[i] += mods.pop(i + 1)

                        # Re-combine 'k' 'o' into 'ko"
    if 'k' in mods:
        i = mods.index('k')
        if i + 1 < len(mods) and mods[i + 1] == 'o':
            mods[i] += mods.pop(i + 1)

    if mods:
        return [base] + mods
    else:
        return [base, '']


def from_base_mods(base, mods):
    """Return 'text+x', or just '+x' or 'text' or even ''.

    base : string like "text"
    mods : list of modifiers ['x'].
           Ok if empty or if contains empty string ''.
           Order preserved, so OK to split multi-char mods
           like "+S10" into multiple chars ['S', '1', '0']
    """
    if not mods:
        return base
    if not base:
        return '+' + ''.join(mods)
    return base + '+' + ''.join(mods)


def remove_mod(filetype, mod):
    """Remove a single modifier such as 'x' or 'S10'.

    Cannot remove multiple modifiers at a time.
    """
    if 1 < len(mod) and (mod[0] != 'S' or mod == "ko"):
        raise RuntimeError('BUG: Cannot remove multiple modifier chars: {}'.format(mod))

    base_mods = to_base_mods(filetype)
    base_str  = base_mods[0]
    mods      = base_mods[1:]
    if '' in mods:
        mods.remove('')
    if mod in mods:
        mods.remove(mod)

    if not mods:
        return base_str
    return NTR('{base}+{mods}').format(base=base_str,
                                       mods=''.join(mods))


def replace_base(filetype, oldbase, newbase):
    """Used to convert unicode or utf16 to binary.

    oldbase = array of possible types to convert from
    newbase = new base type
    """
    if not filetype:
        return filetype
    newtype = filetype
    for obase in oldbase:
        if obase == newbase:
            continue
        elif obase in filetype:
            newtype = filetype.replace(obase, newbase)
            break
    return newtype


def restore_plus(p4filetype):
    """ P4.Map("+x //depot/...") returns "x" not "+x" . Put the + prefix
    back.
    """
    if not p4filetype or "+" in p4filetype:
        return p4filetype

    for x in BASES:
        if x in p4filetype:
            return p4filetype

    for x in ALIASES.keys():
        if x in p4filetype:
            return p4filetype

    return "+{}".format(p4filetype)

def detect(byte_array):
    """Return a p4filetype string to use for the given file content.

    Return one of
        "text"
        "binary"
    """
    fst = _detect(byte_array)

    return { FileSysType.FST_EMPTY   : "text"
           , FileSysType.FST_BINARY  : "binary"
           , FileSysType.FST_UTF16   : "binary"
           , FileSysType.FST_TEXT    : "text"
           }.get(fst, "binary")


                        # Some day we'll support Python 3.4 and get its shiny
                        # new Enum support. Until then, here have a fake
                        # replacement.
try:
    from enum import Enum
except ImportError:
    class Enum:
        """Gee golly I wish we had Python 3.4 Enum."""
        def __init__(self):
            pass


class FileSysType(Enum):
    """Internal FileSys type constants."""

    FST_TEXT        = 0x0001    # file is text
    FST_BINARY      = 0x0002    # file is binary
    FST_GZIP        = 0x0003    # file is gzip
    FST_DIRECTORY   = 0x0005    # it's a directory
    FST_SYMLINK     = 0x0006    # it's a symlink
    FST_RESOURCE    = 0x0007    # Macintosh resource file
    FST_SPECIAL     = 0x0008    # not a regular file
    FST_MISSING     = 0x0009    # no file at all
    FST_CANTTELL    = 0x000A    # can read file to find out
    FST_EMPTY       = 0x000B    # file is empty
    FST_UNICODE     = 0x000C    # file is unicode
    FST_GUNZIP      = 0x000D    # stream is gzip
    FST_UTF16       = 0x000E    # stream is utf8 convert to utf16


def _detect(byte_array):
    """Return a FST_XXX constant that matches what Perforce would
    use if you tried to 'p4 add' this content as a file with no
    additional type information such as 'p4 add -t' or 'p4 typemap'.

    Based on a subset of p4/sys/filecheck.cc FileSys::CheckType()

    Returns one of
        FST_EMPTY
        FST_BINARY
        FST_UTF16
        FST_TEXT

    """
                        # 'p4 configure filesys.binaryscan' defaults to 64K.
                        # Could fetch this limit from the Perforce server if we
                        # felt ambitious. Maybe some future Git Fusion release.
    p4tune_filesys_binaryscan = 64*1024
    buf = byte_array[:p4tune_filesys_binaryscan]

    if 0 == len(buf):
        return FileSysType.FST_EMPTY

                        # But text with just %PDF- is still binary
    pdf_magic = b'%PDF-'
    if buf[:len(pdf_magic)] == pdf_magic:
        return FileSysType.FST_BINARY

                        # is there an UTF16 BOM at the start
    utf16_be_bom = b'\xfe\xff'
    utf16_le_bom = b'\xff\xfe'
    if (   buf[:2] == utf16_be_bom
        or buf[:2] == utf16_le_bom):
                        # second word of zero means UTF-32
        if buf[2:4] != b'\0\0':
            return FileSysType.FST_UTF16

    # SURPRISE! Doesn't matter if there's a UTF8 BOM present, because Git
    # Fusion stores UTF8 content as 'text' not 'utf8'. A future version of Git
    # Fusion could store utf8-bom content as utf8, (Perforce won't strip the
    # BOM) but not utf8-nobom (Perforce will insert BOM).
    #
    #                   # Is there a UTF8 BOM?
    # utf8_bom_present  = buf.startswith(b'\xef\xbb\xbf')

    controlchar = False
    for b in buf:
        if b in _CNTRL and b not in _SPACE:
            controlchar = True
            break

                        # Git Fusion p4charset is always "none" (non-unicode
                        # P4D) or "utf8" (unicode P4D), so this is the only
                        # case to copy from switch(content_charSet): case
                        # CharSetCvt::UTF_8
    if controlchar:
        return FileSysType.FST_BINARY

    return FileSysType.FST_TEXT


# -- 8-bit char tests from p4/i18n/charman.h ---------------------------------
#    Wrappers for <ctype.h> isxxx() functions that return false
#    if high bit 0x80 set.
#
# Normally we'd use Python class "str" which has various functions such as
# isalpha() and isspace(). But we don't want to perform byte-to-char
# conversion within _detect(): we want to exactly mimic FileSys::CheckType()'s
# byte-oriented code.
#
# Do NOT turn these into Python functions. Python function calls are EXPENSIVE
# and calling these as functions once for every single file revision byte in
# the repo can DOUBLE the clock time it takes to convert a repo from Git to
# Perforce.
#

_CNTRL = set(
    [ 0o000, 0o001, 0o002, 0o003, 0o004
    , 0o005, 0o006, 0o007, 0o010, 0o011
    , 0o012, 0o013, 0o014, 0o015, 0o016
    , 0o017, 0o020, 0o021, 0o022, 0o023
    , 0o024, 0o025, 0o026, 0o027, 0o030
    , 0o031, 0o032, 0o033, 0o034, 0o035
    , 0o036, 0o037, 0o177
    ])


_SPACE = set(
    [ 0o011, 0o012, 0o013, 0o014, 0o015
    , 0o040
    ])

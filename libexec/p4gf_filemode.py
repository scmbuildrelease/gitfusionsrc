#! /usr/bin/env python3.3
"""Enough with the magic numbers already!

You may also be interested in p4gf_util.octal() and mode_str()
"""
# import stat           # Intentionally NOT importing stat here. Which of these
                        # two assignments more clearly communicates intent?
                        #
                        #  PLAIN = 0o100644
                        #
                        #  PLAIN = ( stat.S_IFREG
                        #          | stat.S_IWRITE
                        #          | stat.S_IRUSR
                        #          | stat.S_IRGRP
                        #          | stat.S_IROTH
                        #          )
                        #

from p4gf_l10n import NTR

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

class FileModeInt(Enum):
    """Integer file modes."""
    PLAIN       = 0o100644
    EXECUTABLE  = 0o100755
    SYMLINK     = 0o120000
    DIRECTORY   = 0o040000
    COMMIT      = 0o160000

    @staticmethod
    def from_str(s):
        """Convert string to integer."""
        return int(s, 8)

class FileModeStr(Enum):
    """String file modes. Octal digits only, no leading 0o prefix."""
    PLAIN       = "100644"
    EXECUTABLE  = "100755"
    SYMLINK     = "120000"
    DIRECTORY   = "040000"
    COMMIT      = "160000"

    @staticmethod
    def from_int(i):
        """Convert an integer to a string."""
        return NTR("{:06o}").format(i)


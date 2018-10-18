#! /usr/bin/env python3.3
"""Functions to implement Perforce's -V version string."""

import sys

import p4gf_env_config    # pylint: disable=unused-import
import p4gf_version_3


if __name__ == "__main__":
    for h in ['-?', '-h', '--help']:
        if h in sys.argv:
                                                        # '_' is defined/imported by p4gf_version_26
            print(_('Git Fusion version information.'))  # pylint: disable=undefined-variable
    print(p4gf_version_3.as_string_extended(include_checksum=True))
    sys.exit(0)

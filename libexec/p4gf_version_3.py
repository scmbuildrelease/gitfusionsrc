#! /usr/bin/env python3.3
"""Functions to implement Perforce's -V version string."""

import logging
import os
import shutil
import subprocess
import sys

try:
    from P4 import P4Exception, P4
except ImportError:
                                        # Not importing l10n for just this one error message.
    print("Missing P4 Python module")   # pylint: disable=W9903
    sys.exit(1)

try:
    from p4gf_l10n import _, NTR
except ImportError:
    print("Missing p4gf_l10n module")   # pylint: disable=W9903
    sys.exit(1)

# Yeah we're importing *. Because we're the internal face for
# p4gf_version_26.py and I don't want ANYONE importing p4gf_version26.
#
from p4gf_version_26 import *   # pylint: disable=wildcard-import, unused-wildcard-import

import p4gf_p4cache


def _create_p4(*, p4, args):
    """Return a connected P4 instance.

    Low-level copypasta from p4gf_create_p4 since we can't import
    that from down here.
    """
    _p4 = p4
    if not _p4:
        # import P4   #ZZ not sure why outer import not visible here.
        _p4 = P4()
        _p4.prog = as_single_line()
        if args:
            if 'p4user' in args and args.p4user:
                _p4.user = args.p4user
            if 'p4port' in args and args.p4port:
                _p4.port = args.p4port
    if not _p4.connected():
        _p4.connect()
    return _p4


def as_string_extended(*, p4=None, args=None, include_checksum=False):
    """Return a page-long dump of Git Fusion, P4D, and uname info."""
                        # Git Fusion version info, including Git and P4Python.
    a = as_string(include_checksum)
    l = []
                        # Git Fusion server OS version: uname -a
    l.append(NTR('uname: {}').format(uname()))
    l.append(NTR('Git Fusion path: {}').format(os.path.dirname(os.path.realpath(__file__))))
    l.append(_get_lsb_release())

                        # P4PORT, if supplied
    if p4:
        l.append(_('Perforce server address: {p4port}').format(p4port=p4.port))

                        # 'p4 info', if we can get it.
    try:
        _p4 = _create_p4(p4=p4, args=args)

                    # Run 'p4 info' un-tagged to get human-friendly
                    # server info labels.
        l.append(NTR("p4 info:"))
        l.extend(p4gf_p4cache.fetch_info(_p4, tagged=False))

                    # Run 'p4 info' a SECOND time, tagged, to get
                    # the "unicode" setting that untagged omits.
        u = p4gf_p4cache.fetch_info(_p4, tagged=True).get(("unicode"), _("disabled"))
        l.append(_("Unicode: {value}").format(value=u))
    except P4Exception:
        pass
    return a + "\n".join(l) + "\n"


def log_version_extended(include_checksum=False):
    """Record 'p4gf_version.py -V' output to debug log.

    p4gf_auth_server is the main entry point, calls this. You can add more calls
    to log_version() from other top-level scripts, but maybe keep it down to
    p4gf_init, p4gf_init_repo, and p4gf_auth_server.
    """
    logging.getLogger("version").info(as_string_extended(include_checksum=include_checksum))


def _get_lsb_release():
    """Retreive the Linux Standard Base release information, if available."""
    lsb_path = shutil.which('lsb_release')
    if lsb_path:
        try:
            lsb_output = subprocess.check_output(
                ['lsb_release', '-a'], stderr=subprocess.DEVNULL, universal_newlines=True)
            if lsb_output:
                return lsb_output.strip()
        except subprocess.CalledProcessError:
            pass
    return _('No LSB release information available.')

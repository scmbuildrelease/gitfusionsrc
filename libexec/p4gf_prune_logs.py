#! /usr/bin/env python3.3
"""Prune old log files."""

import os
import sys
import time

import p4gf_const
from p4gf_l10n import _
import p4gf_util


def prune_files_older_than(limit):
    """Remove files older than limit days."""
    # P4GF_LOGS_DIR: admins may set up a different log dir.
    # configure-git-fusion.sh will set /fs/... path if configuring the OVA
    # Typically this will be set in p4gf_environment.cfg
    if p4gf_const.P4GF_LOGS_DIR in os.environ:
        logs_dir = os.environ[p4gf_const.P4GF_LOGS_DIR]
    else:
        logs_dir = os.path.join(p4gf_const.P4GF_HOME, "logs")
    if not os.path.exists(logs_dir):
        return
    now = time.time()
    limit_secs = limit * 24 * 3600
    for fname in os.listdir(logs_dir):
        fpath = os.path.join(logs_dir, fname)
        mtime = os.stat(fpath).st_mtime
        if now - mtime > limit_secs:
            os.unlink(fpath)


def main():
    """Parse the command-line arguments and perform the requested operation."""
    desc = _("""Remove older log files.""")
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument('--limit', type=int, default=7,
                        help="log files older than LIMIT days are removed (default 7)")
    parser.add_argument('--yes', action='store_true',
                        help="required if LIMIT is 0 to remove all files")
    args = parser.parse_args()
    if args.limit < 0:
        sys.stderr.write('Limit cannot be negative\n')
        sys.exit(2)
    if args.limit == 0 and not args.yes:
        print('Limit of 0 will remove all files, invoke with --yes if you are sure.')
        sys.exit(2)
    prune_files_older_than(args.limit)


if __name__ == "__main__":
    main()

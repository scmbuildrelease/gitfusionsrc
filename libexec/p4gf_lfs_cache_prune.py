#! /usr/bin/env python3.3
"""Script to remove old files from LFS repo file cache."""
import logging
import os
import sys
import time
import glob
import re

import p4gf_env_config    # pylint: disable=unused-import
from   p4gf_l10n import _,  log_l10n
import p4gf_log
import p4gf_util
import p4gf_lfs_file_spec
import p4gf_const

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_lfs_cache_prune")

LFS_FILE_MAX_SECONDS_TO_KEEP = 7*24*3600   # seven days
LFS_TIME_BETWEEN_PRUNES      = 24*3600     # one day
TOP_SHA56_DIR_RE = re.compile(r'sha256/[0-9a-fA-F]{2}$')
LFS_LAST_PRUNE   = p4gf_const.P4GF_HOME + '/lfs_last_prune_time'


def test_vars_apply():
    """Apply test environment variable."""
    global LFS_FILE_MAX_SECONDS_TO_KEEP, LFS_TIME_BETWEEN_PRUNES
    if p4gf_const.P4GF_TEST_LFS_FILE_MAX_SECONDS_TO_KEEP in os.environ:
        LFS_FILE_MAX_SECONDS_TO_KEEP = \
                int(os.environ[p4gf_const.P4GF_TEST_LFS_FILE_MAX_SECONDS_TO_KEEP])
        LFS_TIME_BETWEEN_PRUNES = \
                int(os.environ[p4gf_const.P4GF_TEST_LFS_FILE_MAX_SECONDS_TO_KEEP])
        LOG.debug("setting LFS_FILE_MAX_SECONDS_TO_KEEP={} from environment.".
                format(LFS_FILE_MAX_SECONDS_TO_KEEP))


def remove_dir_if_empty(d):
    """Remove directory if empty, returning True if removed."""
    if len(os.listdir(d)) == 0:
        try:
            os.rmdir(d)
            return True
        except OSError:
            pass
    return False


def remove_empty_lfs_dirs(deepest_dir):
    """Removed the chain of emnpty dirs above the deepest lfs dir."""
    if not os.path.isdir(deepest_dir):
        return False
    ret = False
    dir_list = os.listdir(deepest_dir)
    if len(dir_list) == 0:
        try:
            parent1 = os.path.abspath(os.path.join(deepest_dir, '..'))
            parent2 = os.path.abspath(os.path.join(deepest_dir, '../..'))
            parent3 = os.path.abspath(os.path.join(deepest_dir, '../../..'))
            # ensure we are removing the correct paths
            if TOP_SHA56_DIR_RE.search(parent3):
                ret = True
                for d in (deepest_dir, parent1, parent2, parent3):
                    if not remove_dir_if_empty(d):
                        ret = False
                        break
        except OSError as e:
            ret = False
            LOG.debug("error removing empty lfs dirs {0}".format(str(e)))
        if ret:
            LOG.debug("removed empty LFS dirs ending with {0}".format(deepest_dir))
    return ret


def needs_prune():
    """Use a file's mtime to determine the last prune.
    If 24 hours have elapsed
        reset mtime and return True."""

    ret = False
    if os.path.exists(LFS_LAST_PRUNE):
        mtime = int(os.stat(LFS_LAST_PRUNE).st_mtime)
        lapsed = time.time() - mtime
        if lapsed > LFS_TIME_BETWEEN_PRUNES:   # one day
            os.utime(LFS_LAST_PRUNE, None)     # reset atime,mtime
            LOG.debug("needs_prune: resetting mtime which was {0}={1}".
                    format(mtime, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))))
            ret = True
    else:
        with open(LFS_LAST_PRUNE,'w') as fd:
            fd.write("mtime of this file inicates the most recent lfs prune time\n")
            ret = True

    return ret


def prune_lfs_file_cache():
    """For each repo , remove LFS cached files
    which have not been accessed over the configured time period."""
    test_vars_apply()   # override SECONDS_TO_KEEP from environment
    if not needs_prune():  # 24 hours since last prune?
        return
    views_dir = os.path.join(p4gf_const.P4GF_HOME,'views')

    for view in [ os.path.join(views_dir,v) for v in os.listdir(views_dir)
               if os.path.isdir(os.path.join(views_dir,v))]:

        lfs    = os.path.join(view, "lfs")
        lfs_cache = p4gf_lfs_file_spec.LFS_CACHE_PATH \
                            .format( repo_lfs   = lfs
                                   , sha256     = "" )
        lfs_glob_path = lfs_cache + '??/??/??/??/*'
        oldest =  time.time() - LFS_FILE_MAX_SECONDS_TO_KEEP
        for f in glob.iglob(lfs_glob_path):
            try:
                atime = int(os.stat(f).st_atime)
                if  atime < oldest:
                    LOG.debug("prune_lfs_file_cache: removing {}".format(f))
                    os.remove(f)
            except (OSError,ValueError) as e:
                LOG.debug("prune_lfs_file_cache error removing {}:{}".format(f, str(e)))


def main():
    """Copy the SSH keys from Perforce to the authorized keys file."""
    global LFS_FILE_MAX_SECONDS_TO_KEEP
    p4gf_util.has_server_id_or_exit()

    log_l10n()

    # Set up argument parsing.
    parser = p4gf_util.create_arg_parser(_("""Removes files from LFS file cache
    which have not been accessed in some configurable time."""))
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--hours', nargs=1,
                       help=_('number of hours since last access to keep lfs file'))
    group.add_argument('--days', nargs=1,
                       help=_('number of days since last access to keep lfs file'))
    group.add_argument('--seconds', nargs=1,
                       help=_('number of seconds since last access to keep lfs file'))
    parser.add_argument('--gfhome', nargs=1,
                        help=_('set GFHOME'))
    args = parser.parse_args()

    if args.gfhome:
        p4gf_const.P4GF_HOME = args.gfhome[0]
    try:
        if args.hours:
            LFS_FILE_MAX_SECONDS_TO_KEEP = int(args.hours[0])*3600
        if args.days:
            LFS_FILE_MAX_SECONDS_TO_KEEP = int(args.days[0])*24*3600
        if args.seconds:
            LFS_FILE_MAX_SECONDS_TO_KEEP = int(args.seconds[0])
    except ValueError:
        LOG.debug("--hours, --days, --seconds must be integers.")
        if sys.stdout.isatty():
            print("--hours, --days, --seconds must be integers.")
        sys.exit(1)

    prune_lfs_file_cache()


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

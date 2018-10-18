#! /usr/bin/env python3.3
"""Functions for managing temporary files."""

import logging
import os
import stat
import tempfile
import time

import p4gf_const

LOG = logging.getLogger(__name__)

#
# Problems:
#
# 1. In general, writing to /tmp fills up small root file systems.
# 2. Existing documentation regarding temporary files is misleading.
#    a. States that either TMP or TMPDIR may be used.
#    b. Does not specify which one takes precedence.
#    c. Git Fusion does not specifically do anything with these.
#    d. Python selects from TMPDIR, TEMP, and TMP, in that order.
#    e. Hence, if TEMP is defined, but not TMPDIR, then TMP is ignored.
# 3. Abnormally terminated processes leave temporary files behind (GF-2766).
#    We cannot automatically prune the system-wide "TMP" path as there
#    are likely many files there that we cannot safely remove.
#
# Solutions:
#
# 1. By default, write to a location under P4GF_HOME, instead of /tmp.
# 2. Use P4GF_TMPDIR as temporary directory, ignoring TMPDIR, TEMP, and TMP.
# 3. Prune old temporary files/directories in the background push process.
#    Since we alone know about P4GF_TMPDIR, we can safely remove old files.
#    This assumes the administrator does not point P4GF_TMPDIR to /tmp.
#


def new_temp_file(mode='w+b', encoding=None, suffix='', prefix='tmp', delete=True):
    """Create a new temporary file.

    The file is created using tempfile.NamedTemporaryFile().

    """
    td = gettempdir()
    return tempfile.NamedTemporaryFile(
        mode=mode, encoding=encoding, suffix=suffix, prefix=prefix, delete=delete, dir=td)


def new_temp_dir(suffix='', prefix='tmp'):
    """Create a new temporary directory.

    The resulting object can be used as a context manager. On completion of
    the context or destruction of the temporary directory object the newly
    created temporary directory and all its contents are removed from the
    filesystem.

    """
    td = gettempdir()
    return tempfile.TemporaryDirectory(suffix=suffix, prefix=prefix, dir=td)


def gettempdir():
    """Return the path to which temporary files are created."""
    # We are counting on some other ("main") module importing env_config
    # for us, in order for us to get the effective P4GF_TMPDIR setting.
    if 'P4GF_TMPDIR' in os.environ:
        td = os.environ['P4GF_TMPDIR']
    else:
        td = os.path.join(p4gf_const.P4GF_HOME, "tmp")
    # Need to ensure the directory exists as most callers will assume it
    # does already.
    if not os.path.exists(td):
        os.makedirs(td)
    return td


def prune_old_files():
    """Remove all temporary files older than 7 days."""
    prune_files_older_than(7)


def prune_files_older_than(limit):
    """Remove files older than the given number of days.

    :param int limit: number of days old a file must be in order to be removed.

    """
    temp_dir = gettempdir()
    if not os.path.exists(temp_dir):
        return
    now = time.time()
    limit_secs = limit * 86400
    LOG.debug2('prune_files_older_than() threshold %s (%s)', limit_secs, now - limit_secs)

    def _prune_directory(path):
        """Prune everything within the given path, if it is old enough."""
        for fname in os.listdir(path):
            fpath = os.path.join(path, fname)
            try:
                fstat = os.stat(fpath)
            except OSError as err:
                # Most likely the entry has been removed.
                continue
            mtime = fstat.st_mtime
            LOG.debug2('prune_files_older_than() %s mtime %s', fname, mtime)
            if now - mtime > limit_secs:
                if stat.S_ISDIR(fstat.st_mode):
                    _prune_directory(fpath)
                    # Remove the directory itself if it is now empty.
                    try:
                        if not os.listdir(fpath):
                            os.rmdir(fpath)
                            LOG.debug('prune_files_older_than() removed dir %s', fpath)
                    except OSError as err:
                        LOG.warning('could not remove temporary directory: %s', err)
                else:
                    try:
                        os.unlink(fpath)
                        LOG.debug('prune_files_older_than() removed file %s', fpath)
                    except OSError as err:
                        LOG.warning('could not remove temporary file: %s', err)

    _prune_directory(temp_dir)

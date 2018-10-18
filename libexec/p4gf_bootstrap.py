#! /usr/bin/env python3.3
"""Code to bootstrap Git Fusion."""

import logging

#
# Do _NOT_ import other Git Fusion code here. This is the basis of all of
# Git Fusion, nothing can come before this module. As such, anything that
# other modules rely upon that cannot be added elsewhere without creating
# circular imports, may be added here.
#

# -- Installing two new log levels, DEBUG2 and DEBUG3 -------------------------

logging.DEBUG2 = 8
logging.DEBUG3 = 7
logging.PROFILER = 6

if hasattr(logging, 'addLevelName'):
    logging.addLevelName(logging.DEBUG2, 'DEBUG2')
    logging.addLevelName(logging.DEBUG3, 'DEBUG3')
    logging.addLevelName(logging.PROFILER, 'PROFILER')
else:
    # We're intentionally poking new levels into module logging.
    logging._levelNames['DEBUG2'] = logging.DEBUG2  # pylint:disable=protected-access,no-member
    logging._levelNames['DEBUG3'] = logging.DEBUG3  # pylint:disable=protected-access,no-member
    logging._levelNames['PROFILER'] = logging.PROFILER  # pylint:disable=protected-access,no-member
    logging._levelNames[logging.DEBUG2] = 'DEBUG2'  # pylint:disable=protected-access,no-member
    logging._levelNames[logging.DEBUG3] = 'DEBUG3'  # pylint:disable=protected-access,no-member
    logging._levelNames[logging.PROFILER] = 'PROFILER'  # pylint:disable=protected-access,no-member


def debug2(self, msg, *args, **kwargs):
    """For logging details deeper than logger.debug()."""
    if self.isEnabledFor(logging.DEBUG2):
        self._log(logging.DEBUG2, msg, args, **kwargs)  # pylint:disable=protected-access


def debug3(self, msg, *args, **kwargs):
    """For log-crushing details deeper than logger.debug()."""
    if self.isEnabledFor(logging.DEBUG3):
        self._log(logging.DEBUG3, msg, args, **kwargs)  # pylint:disable=protected-access

def profiler(self, msg, *args, **kwargs):
    """For the profiler only - caution set for any other file will set debug3"""
    if self.isEnabledFor(logging.PROFILER):
        self._log(logging.PROFILER, msg, args, **kwargs)  # pylint:disable=protected-access

logging.Logger.debug2 = debug2
logging.Logger.debug3 = debug3
logging.Logger.profiler  = profiler

# -----------------------------------------------------------------------------

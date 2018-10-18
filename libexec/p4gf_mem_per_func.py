#! /usr/bin/env python3.3
"""Tools for logging space consumption.
"""
from   collections import defaultdict
import logging
import operator
import os
import resource

import p4gf_util

                        # Linux seems to report in KB while Mac uses bytes.
MAXRSS_KB = 2**10 if os.uname()[0] == "Darwin" else 2**1
MAXRSS_MB = 2**20 if os.uname()[0] == "Darwin" else 2**10

class MemPerFunc:
    """Instrumentation to report how much memory certain functions
    allocate.
    """
    def __init__(self
        , log                       = None
        , log_level                 = logging.DEBUG3
        , report_period_record_ct   = 1000
        ):

        self.log                      = log
        self.log_level                = log_level
        self._report_period_record_ct = report_period_record_ct
        self._report_ct               = 0
        self._current_mem_kb          = 0
        self.data                     = defaultdict(int)

        if not self.log:
            self.log = logging.getLogger(__name__)

    def record(self, func_name):
        """Assign memory growth to funct_name.

        Assigns all memory growth since the previous call to record().
        """
        curr_mem_kb = current_memory_kb()

                        # First call? No base from which to diff.
        if not self._current_mem_kb:
            self._current_mem_kb = curr_mem_kb
            return

        diff = curr_mem_kb - self._current_mem_kb
        self.data[func_name] += diff
        self._current_mem_kb = curr_mem_kb

        self._report_ct += 1
        if not self._report_ct % self._report_period_record_ct:
            self.report()

    def report(self):
        """Dump our data to log."""
        if not self.log.isEnabledFor(self.log_level):
            return

        kv = sorted(self.data.items(), key=operator.itemgetter(1), reverse=True)
        (kl, vl) = zip(*kv)
        lines = p4gf_util.tabular(kl, vl)
        self.log.log(self.log_level, "Memory by function (KB):\n" + "\n".join(lines))


# -- module-wide -------------------------------------------------------------

def current_memory_kb():
    """How much memory does Python consume right now?"""
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / MAXRSS_KB)

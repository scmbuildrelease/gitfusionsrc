#! /usr/bin/env python3.3
"""Tools for logging time and space consumption.

See also
* p4gf_log.memory_usage() for simple logging of current memory level
* p4gf_profiler.Timer for measuring time
* p4gf_profiler.start_cprofiler()/stop_cprofiler() for
  detailed time measurements.
"""
import logging
import os
import resource
import time

                        # Linux seems to report in KB while Mac uses bytes.
MAXRSS_KB = 2**10 if os.uname()[0] == "Darwin" else 2**1
MAXRSS_MB = 2**20 if os.uname()[0] == "Darwin" else 2**10


class TimeSpaceRecorder:
    """Instrumentation to track some incrementing event, report time and
    space consumption every N events.
    """
    def __init__(self, *
            , report_period_event_ct = 100
            , fmt         = None
            , log         = None
            , log_level   = logging.DEBUG
            ):
        self._report_period_event_ct = report_period_event_ct
        self._event_ct    = 0

                        # Optional second count, such as accumulated file
                        # content bytes in addition to _event_ct's file count.
        self._aux_ct      = 0
        self._prev_record = TimeSpaceRecord.from_snapshot(event_ct = 0)
        self.fmt          = fmt
        self.log          = log
        self.log_level    = log_level

        if not fmt:
            self.fmt = ("   {curr_event_ct:>15,} ct total"
                        "   {diff_time:5.2f} seconds"
                        "   {events_per_second:>7,} ct/second"
                       #"   {events_per_mb:>10,} ct/mb"
                        "   {bytes_per_event:>10,} bytes/ct"
                        "   {memory_mb:7.2f} MB total")
        if not log:
            self.log = logging.getLogger(__name__)

    def increment(self, aux_ct = 0):
        """Increment our event counter, report if we've incremented
        enough events.
        """
        self._event_ct += 1
        if self._event_ct % self._report_period_event_ct:
            return

        curr_record = TimeSpaceRecord.from_snapshot( self._event_ct
                                                   , aux_ct = aux_ct)
        self.report(curr_record)
        self._prev_record = curr_record

        # if self._event_ct >= 3000:
        #     raise RuntimeError("That's quite enough of that.");

    def report(self, curr_record):
        """Unconditionally report right now."""
        diff = curr_record - self._prev_record

        if diff.time_secs:
            events_per_sec = diff.event_ct / diff.time_secs
            aux_per_sec    = diff.aux_ct   / diff.time_secs
        else:
            events_per_sec = 0.0
            aux_per_sec    = 0.0

        per_kb  = diff.event_ct / diff.memory_kb if diff.memory_kb else 0.0
        per_mb  = 1024.0 * per_kb
        bytes_per = diff.memory_kb * 1024 / diff.event_ct \
                                                 if diff.event_ct else 0.0

        self.log.log(self.log_level, self.fmt.format(
                  curr_event_ct     = curr_record.event_ct
                , curr_aux_ct       = curr_record.aux_ct
                , diff_time         = diff.time_secs
                , diff_aux_ct       = diff.aux_ct
                , diff_kb           = diff.memory_kb
                , events_per_second = int(events_per_sec)
                , aux_per_second    = int(aux_per_sec)
                , bytes_per_event   = int(bytes_per)
                , events_per_kb     = int(per_kb)
                , events_per_mb     = int(per_mb)
                , memory_mb         = curr_record.memory_kb / 1024
                ))

    @property
    def event_ct(self):
        """How many calls to increment()?"""
        return self._event_ct

# end class TimeSpaceRecorder
# ----------------------------------------------------------------------------
class TimeSpaceRecord:
    """One row in our time/space record."""
    def __init__(self, *
        , event_ct  = 0
        , aux_ct    = 0
        , time_secs = 0
        , memory_kb = 0
        ):
        self.event_ct  = event_ct
        self.aux_ct    = aux_ct
        self.time_secs = time_secs
        self.memory_kb = memory_kb

    @staticmethod
    def from_snapshot(event_ct, aux_ct = 0):
        """Create a record for right now."""
        return TimeSpaceRecord(
              event_ct  = event_ct
            , aux_ct    = aux_ct
            , time_secs = current_time_secs()
            , memory_kb = current_memory_kb()
            )

    def __sub__(self, other):
        """Return a new TimeSpaceRecord with the delta."""
        return TimeSpaceRecord(
                 event_ct  = self.event_ct  - other.event_ct
               , aux_ct    = self.aux_ct    - other.aux_ct
               , time_secs = self.time_secs - other.time_secs
               , memory_kb = self.memory_kb - other.memory_kb
               )

# end class TimeSpaceRecord
# ----------------------------------------------------------------------------


# -- module-wide -------------------------------------------------------------

def current_memory_kb():
    """How much memory does Python consume right now?"""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / MAXRSS_KB


def current_time_secs():
    """What time is it?"""
                    # Time to unpimp ze auto!
    return time.time()

#! /usr/bin/env python3.3
"""Reporting ETA."""
import datetime
import logging
LOG = logging.getLogger('p4gf_eta')

class ETA(object):
    """A class that reports an estimated time of completion,
    probably with about the same accuracy as the Windows 95
    file explorer.
    """

    def __init__(self, *, total_ct=100):
        self.start_dt   = datetime.datetime.now()
        self.prev_dt    = self.start_dt
        self.current_ct = 0
        self.total_ct   = total_ct

                        # Weighted moving average
        self.ct_per_second = None

                        # Weight:
                        #   1.0 = no smoothing at all
                        #   0.1 = takes about 10 seconds to settle down
        self.weight     = 0.01

                        # Accumulate delta ct until you have
                        # at least 1 second difference
        self.delta_ct   = 0

    def start(self, total_ct):
        """(Re)start our timer, for N increments."""
        self.start_dt       = datetime.datetime.now()
        self.prev_dt        = self.start_dt
        self.current_ct     = 0
        self.total_ct       = total_ct
        self.ct_per_second  = None
        self.delta_ct       = 0

    def increment(self, inc_ct=1):
        """Increment our count, recalculate average ct/sec."""
        self.delta_ct += inc_ct
        now  = datetime.datetime.now()
        delta_seconds = (now - self.prev_dt).total_seconds()
        if delta_seconds < 1.0:
            return

        dx_dy = self.delta_ct / delta_seconds

        if self.ct_per_second == None:
            self.ct_per_second = dx_dy

            LOG.debug3("∆ct {dx:>4} / ∆sec {dy:>6.3f} = {dx_dy:>7.3f}"
                      .format( dx    = self.delta_ct
                             , dy    = delta_seconds
                             , dx_dy = dx_dy ))

        else:
            bump = dx_dy - self.ct_per_second
            pcs = self.ct_per_second
            self.ct_per_second += bump * self.weight

            LOG.debug3("∆ct {dx:>4} / ∆sec {dy:>6.3f} = {dx_dy:>7.3f}"
                      " bump {b:>7.3f}  * wt {w:>6.3f} = bw {bw:>6.3f}"
                      "   + prev ct/sec {pcs:>6.3f} = new ct/sec {ccs:>6.3f}"
                      "  {eta}"
                      .format( dx    = self.delta_ct
                             , dy    = delta_seconds
                             , dx_dy = dx_dy
                             , b     = bump
                             , w     = self.weight
                             , bw    = bump * self.weight
                             , pcs   = pcs
                             , ccs   = self.ct_per_second
                             , eta   = self.eta_str()
                             ))

        self.prev_dt = now
        self.current_ct += self.delta_ct
        self.delta_ct = 0

    def now_dt(self):
        """Return most recent update to time/delta."""
        return self.prev_dt

    def eta_delta(self):
        """Estimated time remaining, as a timedelta object."""
        remaining_ct = self.total_ct - self.current_ct
        if not self.ct_per_second:
            return None
        remaining_seconds = remaining_ct / self.ct_per_second
        return datetime.timedelta(seconds = remaining_seconds)

    def eta_dt(self):
        """Calculate an ETA."""
        d = self.eta_delta()
        if not d:
            return None
        return self.prev_dt + d

    def eta_str(self):
        """Return a formatted time string."""
        d = self.eta_dt()
        if not d:
            return "--:--:--"
        return d.strftime("%H:%M:%S")

    def eta_delta_str(self):
        """Return "1h23m13s" """
        d = self.eta_delta()
        if not d:
            return ""

        s = int(self.eta_delta().total_seconds())

        d = 0           # Why 2*? Because I'd rather see "40h" than "1d16h"
        if 2*SECONDS_PER_DAY < s:
            d = int(s / SECONDS_PER_DAY)
            s %= SECONDS_PER_DAY

        h = 0
        if SECONDS_PER_HOUR < s:
            h = int(s / SECONDS_PER_HOUR)
            s %= SECONDS_PER_HOUR

        m = 0
        if SECONDS_PER_MINUTE < s:
            m = int(s / SECONDS_PER_MINUTE)
            s %= SECONDS_PER_MINUTE

        if d:
            return "{d:}d{h:}h{m:02d}m{s:02d}s".format(d=d, h=h, m=m, s=s)
        elif h:
            return "{h:,}h{m:02d}m{s:02d}s".format(h=h, m=m, s=s)
        elif m:
            return "{m:2d}m{s:02d}s".format(m=m, s=s)
        else:
            return "{s:2d}s".format(s=s)

SECONDS_PER_DAY    = 60 * 60 * 24
SECONDS_PER_HOUR   = 60 * 60
SECONDS_PER_MINUTE = 60

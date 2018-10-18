#! /usr/bin/env python3.3
"""A simple profiling timer class for timing sections of code.

To use:

with Timer('A'):
    do work
    with Timer('B'):
        do work

with Timer('C'):
    do work
    with Timer('B'):
        do work

At exit, a debug log entry will be produced:

A                    : 0.2000 seconds
  self time          : 0.1000 seconds
  B                  : 0.1000 seconds
C                    : 0.3000 seconds
  self time          : 0.1000 seconds
  B                  : 0.2000 seconds

Restrictions:
  Timer names must not contain '.'.
  Don't try to use class _Timer directly.

Use of timers as function decorators is also possible, like so:

@with_timer('D')
def func(...):
    pass

"""

import atexit
from functools import wraps
import logging
import time
import sys

# pylint:disable=W9903
# non-gettext-ed string
# debugging module, no translation required.
LOG = logging.getLogger(__name__)

_ACTIVE_TIMERS = []
_TIMERS = {}
_INDENT = 2
_SEP = '.'


def with_timer(timer):
    """Wrap the decorated function with the named timer.

    :type timer: str
    :param timer: name of a timer.

    :rtype: callable
    :return: decorator

    """
    def interior_decorator(func):
        """The actual function wrapper."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            """The function wrapper."""
            with Timer(timer):
                return func(*args, **kwargs)
        return wrapper
    return interior_decorator


class _Timer:

    """Simple class for timing code."""

    def __init__(self, name, top_level):
        self.name = name
        self.top_level = top_level
        self.time = 0
        self.start = 0
        self.active = False

    def __float__(self):
        return self.time

    def __enter__(self):
        assert not self.active
        assert not _ACTIVE_TIMERS or self.name.startswith(_ACTIVE_TIMERS[-1].name)
        self.active = True
        _ACTIVE_TIMERS.append(self)
        self.start = time.time()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        assert self.active
        assert _ACTIVE_TIMERS[-1] == self
        delta = time.time() - self.start
        _ACTIVE_TIMERS.pop()
        self.active = False
        self.time += delta

    def is_child(self, t):
        """Return True if t is a direct child of this timer."""
        if t == self:
            return False
        if not t.name.startswith(self.name + _SEP):
            return False
        if _SEP in t.name[len(self.name)+len(_SEP):]:
            return False
        return True

    def children(self):
        """Return list of timers nested within this timer."""
        return [t for t in _TIMERS.values() if self.is_child(t)]

    def child_time(self):
        """Return sum of times of all nested timers."""
        return sum([t.time for t in self.children()])

    def do_str(self, indent):
        """Helper function for str(), recursively format timer values."""
        items = [" " * indent + "{:32}".format(self.name.split(_SEP)[-1]) + " " * (10 - indent) +
                 ": {:8.4f} seconds".format(self.time)]
        ctimers = sorted(self.children(), key=lambda t: t.name)
        if ctimers:
            indent += _INDENT
            self_time = self.time - self.child_time()
            items.append(" " * indent + "{:32}".format("self time") + " " * (10 - indent) +
                         ": {:8.4f} seconds".format(self_time))
            for t in ctimers:
                items.append(t.do_str(indent))
        return "\n".join(items)

    def __str__(self):
        return self.do_str(0)


class _DummyTimer:

    """Missing docstring."""

    def __init__(self):
        self.time = None

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        pass


def Timer(name):  # pylint: disable=invalid-name
    """Create and return a timer."""
    assert _SEP not in name

    if _ACTIVE_TIMERS:
        assert name not in _ACTIVE_TIMERS[-1].name.split(_SEP)
        full_name = _ACTIVE_TIMERS[-1].name + _SEP + name
    else:
        full_name = name
    if full_name not in _TIMERS:
        _TIMERS[full_name] = _Timer(full_name, full_name == name)
    return _TIMERS[full_name]


@atexit.register
def Report():  # pylint: disable=invalid-name
    """Log all recorded timer activity."""
    top_timers = sorted([t for t in _TIMERS.values() if t.top_level], key=lambda t: t.name)
    if top_timers:
        LOG.profiler("\n".join(["Profiler report for {}".format(sys.argv)]
                            + [str(t) for t in top_timers]))


def start_cprofiler():
    """Turn on the profiler, if we can.

    Return cprofiler if cprofiler started, None if not.
    Pass this to stop_cprofiler.
    """
    try:
        import cProfile
        prof = cProfile.Profile()
        prof.enable()
        return prof
    except ImportError:
        LOG.warning('cProfile not available on this system, profiling disabled')
        return None


def stop_cprofiler(cprofiler, outfile="profiler.txt"):
    """Dump cprofiler statistics to profiler.txt.

    :param cprofiler: result from start_cprofiler()
    """
    if not cprofiler:
        return
    cprofiler.disable()
    with open(outfile, "w") as f:
        import pstats
        ps = pstats.Stats(cprofiler, stream=f)
        ps.sort_stats('cumulative')
        ps.print_stats(100)
        ps.print_callees(100)

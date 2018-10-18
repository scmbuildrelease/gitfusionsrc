#! /usr/bin/env python3.3
"""Merge two cProfiler runs into a tab-separated textfile you can read in
a spreadsheet.
"""
from   collections import defaultdict
import copy
import logging
import re

import p4gf_util

# pylint: disable = line-too-long
                        # Some day we'll support Python 3.4 and get its shiny
                        # new Enum support. Until then, here have a fake
                        # replacement.
try:
    from enum import Enum
except ImportError:
    class Enum:
        """Gee golly I wish we had Python 3.4 Enum."""
        def __init__(self):
            pass

                        # Log level reminder from p4gf_util.apply_log_args():
                        # verbose   = INFO
                        # <nothing> = WARNING
                        # quiet     = ERROR
                        #
LOG = logging.getLogger("profiler_merge_stdout")

# Some of the sample line comments are riduiculously long.
# pylint: disable = line-too-long

# cProfiler output
#
# tottime = total time spent in function itself, not anything it calls
# cumtime = time spent in function or anything it called
#
# High cumtime is usually where to start drilling down to find suboptimal
# algorithms.
# High tottime + very high ncalls is likely a function called in some
# O(n^2) or worse loop. Walk up the call chain to find the suboptimality.

# -- main/mopdule-wide functions ---------------------------------------------

def main():
    """Do the thing."""
    args = _parse_argv()
    da = parse_file(args.file_a)
    db = parse_file(args.file_b)
    fa = da.sanitize_functions()
    fb = db.sanitize_functions()
    merged = MergedStats.from_stats(fa, fb)
    LOG.warning(str(merged))


def _parse_argv():
    """Convert command line into a usable dict."""
    parser = p4gf_util.create_arg_parser(
          add_log_args  = True
        , add_debug_arg = True
        , desc          = "Merge two cProfiler runs into a single spreadsheet."
        )
    parser.add_argument('file_a', metavar='file_a')
    parser.add_argument('file_b', metavar='file_b')
    args = parser.parse_args()
    p4gf_util.apply_log_args(args, LOG)
    LOG.debug("args={}".format(args))
    return args


def parse_file(filename):
    """Read a line into a parsed XXX."""

    class State(Enum):
        """Where are we within the file?"""
        BEFORE_SUMMARY   = "pre summary"
        IN_SUMMARY       = "in  summary"
        IN_CALLERS       = "in  callers"

    current_caller = None

    with open(filename, "r", encoding="utf-8") as fin:
        state = State.BEFORE_SUMMARY
        stats = Stats()
        for line in fin:
            if state is State.BEFORE_SUMMARY:
                if SummaryLine.START_PICKET_RE.match(line):
                    state = State.IN_SUMMARY
                    LOG.debug("sl: " + SummaryLine.HEADER)
                    continue
            elif state is State.IN_SUMMARY:
                sl = SummaryLine.from_line(line)
                if sl:
                    LOG.debug("sl: " + str(sl))
                    stats.add_summary_line(sl)
                    continue
                elif CallerLine.START_PICKET_RE.match(line):
                    state = State.IN_CALLERS
                    continue
            elif state is State.IN_CALLERS:
                caller_line = CallerLine.from_line(line)
                if caller_line:
                    current_caller = caller_line.caller
                    LOG.debug("cl: " + str(caller_line))
                    LOG.debug("cl: " + CalleeLine.HEADER)
                    line = caller_line.remainder
                        # intentional fall through to process
                        # remainder as callee
                callee_line = CalleeLine.from_line(line)
                if callee_line:
                    stats.add_callee_line(current_caller, callee_line)
                    LOG.debug("cl: " + str(callee_line))

    return stats

def _ncalls(s):
    """Un-split any "recursive calls/primitive calls" slashed ncalls values."""
    if "/" in s:
        (recursive_ncalls, primitive_ncalls) = s.split("/")
        return int(recursive_ncalls) + int(primitive_ncalls)
    return int(s)

def _sanitize_package(orig):
    """Shorten ridiculously long package paths."""
    line = orig
    # python3.3 packages (pygit2, P4Python)
    # Do these BEFORE general python3.3, since python3.3 prefix would match site-packages prefix
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/site-packages/p4python-2015.2.1205721-py3.3-macosx-10.6-intel.egg/P4.py:569(run)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/site-packages/p4python-2015.2.1205721-py3.3-macosx-10.6-intel.egg/P4.py:749(__flatten)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/site-packages/p4python-2015.2.1205721-py3.3-macosx-10.6-intel.egg/P4.py:877(insert)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/site-packages/pygit2/repository.py:58(__init__)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/site-packages/pygit2/repository.py:71(_common_init)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/site-packages/pytz/__init__.py:245(__str__)
    python_packages_re = re.compile(r'.*/site-packages/(.*)')
    m = python_packages_re.match(orig)
    if m:
        line = m.group(1)
        package_module_re = re.compile(r'([^/]+)/(.*)')
        m = package_module_re.match(line)
        if m:
            package = m.group(1)
            module  = m.group(2)
            for p in ['p4python', 'pygit2']:
                if p in package:
                    package = p
            line = package + "/" + module
            return line

    # python3.3 library:
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/multiprocessing/synchronize.py:296(is_set)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/os.py:671(__getitem__)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/os.py:694(__iter__)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/re.py:158(search)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/re.py:212(compile)
    # /Library/Frameworks/Python.framework/Versions/3.3/lib/python3.3/tempfile.py:386(__del__)
    python33_re = re.compile(r'.*/python3.3/(.*)')
    m = python33_re.match(line)
    if m:
        line = m.group(1)
        return line

    # Git Fusion
    # /Users/zig/Dropbox/git-fusion-main/bin/p4gf_atomic_lock.py:177(update_all_gf_reviews)
    # /Users/zig/Dropbox/git-fusion-main/bin/p4gf_atomic_lock.py:202(update_repo_reviews)
    # /Users/zig/Dropbox/git-fusion-main/bin/p4gf_util_p4run_logged.py:49(_log_p4_request)
    # /Users/zig/Dropbox/git-fusion-main/bin/p4gf_util_p4run_logged.py:55(_log_p4_results)
    git_fusion_re = re.compile(r'.*/(p4gf_[^/]+)')
    m = git_fusion_re.match(line)
    if m:
        line = m.group(1)
        return line

    # Built-in  (leave unchanged)
    # {built-in method chdir}
    # {built-in method discover_repository}
    # {built-in method getcwd}
    # {built-in method getfilesystemencoding}
    # {built-in method hasattr}
    # {built-in method isinstance}
    # {built-in method len}
    # {built-in method max}
    # {built-in method poll}
    # {built-in method proxy}
    # {built-in method sorted}
    # {built-in method time}
    # {method 'acquire' of '_multiprocessing.SemLock' objects}
    # {method 'add' of 'set' objects}
    # {method 'append' of 'collections.deque' objects}
    # {method 'append' of 'list' objects}
    # {method 'as_array' of 'P4API.P4Map' objects}
    # {method 'decode' of 'bytes' objects}

    return line

def _sanitize_line_num(orig):
    """Strip any line numbers."""

                        # Don't strip list comprehension line numbers.
                        # They can sometimes be significant.
    for c in ["<listcomp>", "<dictcomp>"]:
        if c in orig:
            return orig

    line_num_re = re.compile(r':\d+')
    m = line_num_re.search(orig)
    if not m:
        return orig
    return orig[:m.start()] + orig[m.end():]

def _sanitize_function(orig):
    """cProfiler function names include long file paths and line numbers.
    Shorten file paths. Remove line numbers: they change between test runs.
    """
    line = _sanitize_package(orig)
    line = _sanitize_line_num(line)
    return line

def _cell(ab, side, attr):
    """Return a formatted cell."""
    if not getattr(ab, side):
        return " "
    v = getattr(getattr(ab, side), attr)
    if isinstance(v, int):
        return "{:>6d}".format(v)
    elif isinstance(v, float):
        return "{:>7.3f}".format(v)
    else:
        return str(v)

# -- end module-wide ---------------------------------------------------------

class Stats(object):
    """One file's stats, parsed."""
    def __init__(self):
                        # list of SummaryLine
        self.summary_lines = []
                        # dict[str(caller)] ==> CalleeLine
        self.callee = defaultdict(list)

    def add_summary_line(self, summary_line):
        """Record one SummaryLine."""
        self.summary_lines.append(summary_line)

    def add_callee_line(self, caller, callee_line):
        """Record one function's call to another."""
        self.callee[caller].append(callee_line)

    def sanitize_functions(self):
        """Reduce long function names into something small.
        Strip line numbers: they won't match across different test runs.

        Return a NEW Stats instance built out of the sanitized names.

        """
        r = Stats()
        for sl in self.summary_lines:
            sl2 = copy.copy(sl)
            sl2.function = _sanitize_function(sl.function)
            r.summary_lines.append(sl2)
        for k,v in self.callee.items():
            k2 = _sanitize_function(k)
            v2 = []
            for cl in v:
                cl2 = copy.copy(cl)
                cl2.function = _sanitize_function(cl.function)
                v2.append(cl2)
            r.callee[k2] = v2
        return r

    def __str__(self):
        l = []
        l.append(SummaryLine.HEADER)
        l.extend([str(sl) for sl in self.summary_lines])
        for k in sorted(self.callee.keys()):
            l.append(k)
            v = self.callee[k]
            l.append(CalleeLine.HEADER)
            l.extend([str(cl) for cl in v])
        return "\n".join(l)

# -- end Stats ---------------------------------------------------------------

class Side(Enum):
    """File A or File B?"""
    # pylint:disable=invalid-name
    A = "a"
    B = "b"

class MergedCalleeDict(defaultdict):
    """One of these for each caller in MergedStats."""
    def __init__(self):
        defaultdict.__init__(self, AB)

class MergedStats(object):
    """Two file's stats, merged together."""
    def __init__(self):

                        # Both data members are dicts:
                        # * key = (sanitized) function name
                        # * val = AB with value or None for the A and B sides.

        self.summary_lines = defaultdict(AB)
        self.callee = defaultdict(MergedCalleeDict)

    def add_summary_line(self, side, summary_line):
        """Record either a's or b's summary line."""
        v = self.summary_lines[summary_line.function]
        setattr(v, side, summary_line)

    def add_callee_line(self, side, caller, callee_line):
        """Record either a's or b's caller line."""
        mcd = self.callee[caller]
        v = mcd[callee_line.function]
        LOG.debug("add_callee_line side={} caller='{:<30}' callee_line={}".format(side, caller, callee_line))
        setattr(v, side, callee_line)

    @staticmethod
    def from_stats(a, b):
        """Merge two Stats into one MergedStats."""
        r = MergedStats()
        r.add_side(Side.A, a)
        r.add_side(Side.B, b)
        return r

    def add_side(self, side, stats):
        """Add either A or B stats."""
        for sl in stats.summary_lines:
            self.add_summary_line(side, sl)
        for caller, callee_lines in stats.callee.items():
            for cl in callee_lines:
                self.add_callee_line(side, caller, cl)

    def _all_functions_sort_key(self):
        """Assist output sorting by building a dict of function name to
        file_a's summary cumtime value.

        Anything not in file_a's summary gets a value of 0.
        """
        r = {}
        for function, ab in self.summary_lines.items():
            if not ab.a:
                r[function] = 0.0
            else:
                r[function] = ab.a.cumtime
            LOG.debug("afskey: {:>7.3f} {}".format(r[function],function))
        return r

    def all_functions(self):
        """Return a list of all functions seen anywhere, in canonical order"""
        summary = set(self.summary_lines.keys())
        afskey = self._all_functions_sort_key()
        sum_sorted = sorted(list(summary), key=afskey.get, reverse=True)
        callers = set(self.callee.keys())
        callees = set()
        for mcd in self.callee.values():
            callees = set.union(callees, set(mcd.keys()))
        all_set = set.union(summary, callers, callees)
        not_sum_set = all_set - summary
        not_sum_sorted = sorted(list(not_sum_set))
        return sum_sorted + not_sum_sorted

    def __str__(self):
                        # Find the widest function name and format accordingly.
        sl = self._report_summary_lines()
        cl = self._report_callee_lines()
        return "\n".join(sl + ["\n"] + cl)

    @staticmethod
    def _format_summary(func_width):
        """Return a format string for the summary lines at the top.
        Format specifiers for numeric items are left as STRINGS so that they
        can handle blank cells.
        """
        return ( "{function:<" + str(func_width) + "}"
                 "\t{ncalls_a:>8}"
                 "\t{ncalls_b:>8}"
                 "\t{cumtime_a:>9}"
                 "\t{cumtime_b:>9}"
                )

    def _report_summary_lines(self):
        """Return list of lines for "summary" section. Includes header."""
        func_order  = self.all_functions()
        func_width  = max(len(f) for f in func_order if f in self.summary_lines)
        lines = []
        fmt = self._format_summary(func_width)
        header = fmt.format(
                      function  = "function"
                    , ncalls_a  = "ncalls_a"
                    , ncalls_b  = "ncalls_b"
                    , cumtime_a = "cumtime_a"
                    , cumtime_b = "cumtime_b"
                    )
        lines.append(header)

        for func in func_order:
            ab = self.summary_lines.get(func)
            if not ab:
                continue
            line = fmt.format(
                      function  = func
                    , ncalls_a  = _cell(ab, Side.A, "ncalls")
                    , ncalls_b  = _cell(ab, Side.B, "ncalls")
                    , cumtime_a = _cell(ab, Side.A, "cumtime")
                    , cumtime_b = _cell(ab, Side.B, "cumtime")
                    )
            lines.append(line)
        return lines

    def _report_callee_lines(self):
        """Return list lines for "caller/callee" section. Includes headers."""
        func_order  = self.all_functions()
        func_width  = max(len(f) for f in func_order)
        lines = []
        fmt = self._format_callee(func_width + 3)


        for caller_func in func_order:
            mcd = self.callee.get(caller_func)
            if not mcd:
                LOG.debug("report_callee: SKIP {}".format(caller_func))
                continue
            LOG.debug("report_callee:     {}".format(caller_func))

            header = fmt.format(
                          function  = "-- " + caller_func
                        , ncalls_a  = "ncalls_a"
                        , ncalls_b  = "ncalls_b"
                        , cumtime_a = "cumtime_a"
                        , cumtime_b = "cumtime_b"
                        )
            lines.append(header)


            for callee_func in func_order:      # Ayep, O(n^2)
                ab = mcd.get(callee_func)
                if not ab:
                    continue
                line = fmt.format(
                          function  = "   " + callee_func
                        , ncalls_a  = _cell(ab, Side.A, "ncalls")
                        , ncalls_b  = _cell(ab, Side.B, "ncalls")
                        , cumtime_a = _cell(ab, Side.A, "cumtime")
                        , cumtime_b = _cell(ab, Side.B, "cumtime")
                        )
                lines.append(line)
        return lines

    @staticmethod
    def _format_callee(func_width):
        """Return a format string for the callee lines.
        Format specifiers for numeric items are left as STRINGS so that they
        can handle blank cells.
        """
                        # Unintentional, but both the summary and callee lines
                        # contain the same layout of cells. Keeping the function
                        # calls separate in case I ever need to change 'em.
        return MergedStats._format_summary(func_width)

# -- end MergedStats ---------------------------------------------------------

class AB(object):
    """Mutable tuple to carry two file's record of some row."""
    def __init__(self):
        # pylint:disable=invalid-name
        self.a = None
        self.b = None

# -- end AB ------------------------------------------------------------------

class SummaryLine(object):
    """One line in the summary dump of the top 100 function calls:

       ncalls  tottime  percall  cumtime  percall filename:lineno(function)
            1    0.000    0.000  431.769  431.769 /Users/zig/Dropbox/git-fusion-main/bin/p4gf_auth_server.py:291(main_ignores)
          5/1    0.000    0.000  431.769  431.769 /Users/zig/Dropbox/git-fusion-main/bin/p4gf_profiler.py:66(wrapper)
        16001    0.317    0.000  163.234    0.010 /Users/zig/Dropbox/git-fusion-main/bin/p4gf_branch.py:303(add_to_config)
    """
    #                     1:nc       2:tot       3:per       4:cume      5:per       6:func
    REGEX = re.compile(r'\s*([\d/]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+(.*)')

    START_PICKET_RE = re.compile(r'\s+ncalls\s+tottime\s+percall\s+cumtime\s+percall\s+filename:lineno\(function\)')

    FORMAT = ( "{ncalls:>6d}"
               "  {tottime:>7.3f}"
               "  {tottime_percall:>7.3f}"
               "  {cumtime:>7.3f}"
               "  {cumtime_percall:>7.3f}"
               "  {function}" )
    HEADER = ( "{ncalls:>6}"
               "  {tottime:>7}"
               "  {tottime_percall:>7}"
               "  {cumtime:>7}"
               "  {cumtime_percall:>7}"
               "  {function}").format(
                 ncalls          = "ncalls"
               , tottime         = "tottime"
               , tottime_percall = "percall"
               , cumtime         = "cumtime"
               , cumtime_percall = "percall"
               , function        = "function"
               )

    def __init__(self, *
                , ncalls
                , tottime
                , tottime_percall
                , cumtime
                , cumtime_percall
                , function
                ):
        self.ncalls          = int(ncalls)
        self.tottime         = float(tottime)
        self.tottime_percall = float(tottime_percall)
        self.cumtime         = float(cumtime)
        self.cumtime_percall = float(cumtime_percall)
        self.function        = str(function).strip()

    @staticmethod
    def from_line(line):
        """Return a new SummaryLine that containas the parsed data from line.
        Return None if line not a summary line.
        """
        m = SummaryLine.REGEX.match(line)
        if not m:
            return None
        return SummaryLine(
                  ncalls          = _ncalls(m.group(1))
                , tottime         = m.group(2)
                , tottime_percall = m.group(3)
                , cumtime         = m.group(4)
                , cumtime_percall = m.group(5)
                , function        = m.group(6)
                )

    def __str__(self):
        return SummaryLine.FORMAT.format(
              ncalls          = self. ncalls
            , tottime         = self. tottime
            , tottime_percall = self. tottime_percall
            , cumtime         = self. cumtime
            , cumtime_percall = self. cumtime_percall
            , function        = self. function
            )

# -- end SummaryLine ---------------------------------------------------------

class CallerLine(object):
    """The first line in a "what functions were called by this function" dump.
    Includes one called function's data, too.
    """
    START_PICKET_RE = re.compile(r'\s+ncalls\s+tottime\s+cumtime')

    REGEX = re.compile(r'(.*)\s+\-\> (.*)')

    def __init__( self, *
                , caller
                , remainder ):
        self.caller = caller.strip()
        self.remainder = remainder

    @staticmethod
    def from_line(line):
        """parse"""
        m = CallerLine.REGEX.match(line)
        if not m:
            return None
        return CallerLine(caller = m.group(1), remainder = m.group(2))

    def __str__(self):
        return self.caller

# -- end CallerLine ----------------------------------------------------------

class CalleeLine(object):
    """A line in the "what functions were called by a function?" dump."""

    # ncalls  tottime  cumtime
    #      1    0.000    0.044  /Users/zig/Dropbox/git-fusion-main/bin/p4gf_copy_to_git.py:125(all_branches)

                        # 1:ncalls   2:tot       3:cume      4:callee
    REGEX = re.compile(r'\s+([\d/]+)\s+([\d\.]+)\s+([\d\.]+)\s+(.*)')

    FORMAT = ( "{ncalls:>6d}"
               "  {tottime:>7.3f}"
               "  {cumtime:>7.3f}"
               "  {function}" )
    HEADER = ( "{ncalls:>6}"
               "  {tottime:>7}"
               "  {cumtime:>7}"
               "  {function}" ).format(
                 ncalls   = "ncalls"
               , tottime  = "tottime"
               , cumtime  = "cumtime"
               , function = "function"
               )


    def __init__( self, *
                , ncalls
                , tottime
                , cumtime
                , function
                ):
        self.ncalls    = int(ncalls)
        self.tottime   = float(tottime)
        self.cumtime   = float(cumtime)
        self.function  = str(function).strip()

    @staticmethod
    def from_line(line):
        """parse"""
        m = CalleeLine.REGEX.match(line)
        if not m:
            return None
        return CalleeLine(
                  ncalls    = _ncalls(m.group(1))
                , tottime   = m.group(2)
                , cumtime   = m.group(3)
                , function  = m.group(4)
                )

    def __str__(self):
        return CalleeLine.FORMAT.format(
                  ncalls   = self.ncalls
                , tottime  = self.tottime
                , cumtime  = self.cumtime
                , function = self.function
                )

# -- end CalleeLine ----------------------------------------------------------

if __name__ == "__main__":
    main()

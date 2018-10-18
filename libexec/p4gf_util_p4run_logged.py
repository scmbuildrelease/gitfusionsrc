#! /usr/bin/env python3.3
"""Logging p4 command requests and responses.

Broken out from p4gf_util so that p4gf_p4key can use them without
importing p4gf_util. Avoids circular import.
"""
import logging
import pprint

import P4

import p4gf_p4msg
import p4gf_p4msgid

# Common Perforce warnings that occur normally as part of how we interact
# with Perforce. Don't clutter the log with WARNING entries unworthy of
# human attention.
_SQUELCHED_P4_RESULTS = [
  p4gf_p4msgid.MsgDm_ExHAVE         # "[%argc% - file(s)|File(s)] not on client." } ;
, p4gf_p4msgid.MsgDm_ExFILE         # "[%argc% - no|No] such file(s)." } ;
, p4gf_p4msgid.MsgDm_ExINTEGPERM    # "[%argc% - all|All] revision(s) already integrated." } ;

]


def run_wrapper(run_func):
    """Wrap P4.P4.run() to add logging.

    Then just use P4.run() normally and command and result will be logged.
    Args may be passed separately or as a list or as some combination:
        p4.run('user', '-o', 'myron')
        p4.run(['user', '-o', 'myron'])
        p4.run('user', ['-o', 'myron'])

    In some cases, errors and warnings will be logged; see _log_p4_results().
    """
    def new_run(*args, **kwargs):
        """Wrapper function for P4.P4.run().


        Optional keyword args log_warnings and log_errors specify what logging
        level to use when recording Perforce warnings and errors. We run some
        Perforce commands where we expect warnings (especially "no such file(s)"
        and we really don't need to pollute the log with those expected
        warnings.)
        """
        nargs = list(args)
        p4 = nargs[0]
        cmd = nargs[1:]
        log_warnings = kwargs.get('log_warnings', logging.WARNING)
        log_errors = kwargs.get('log_errors', logging.ERROR)
        _log_p4_request(*cmd)
        results = run_func(*args)
        fatal_msg = p4gf_p4msg.first_fatal_error(p4)
        if fatal_msg:
            raise RuntimeError("Fatal error encountered: {}".format(
                p4gf_p4msg.msg_repr(fatal_msg)))
        _log_p4_results(p4, results, log_warnings=log_warnings,
                        log_errors=log_errors)
        return results
    return new_run

# install the wrapper on P4.P4:
P4.P4.run = run_wrapper(P4.P4.run)


def _log_p4_request(*args):
    """Write p4 cmd request to log, depending on log level."""
    # flatten args
    cmd = []
    for a in args:
        if isinstance(a, list):
            cmd.extend(a)
        else:
            cmd.append(a)
    logging.getLogger('p4.cmd').debug(' '.join([str(c) for c in cmd]))


def _log_p4_results( p4, results
                  , log_warnings = logging.WARNING
                  , log_errors   = logging.ERROR ):
    """Write p4 results to log, depending on log level."""
    log_out = logging.getLogger('p4.out')
    log_out.debug('result ct={}'.format(len(results)))
    if log_out.isEnabledFor(logging.DEBUG3):
        pp = pprint.PrettyPrinter()
        log_out.debug3(pp.pformat(results))

    log_err   = logging.getLogger('p4.err')
    log_warn  = logging.getLogger('p4.warn')
    log_msgid = logging.getLogger('p4.msgid')

    # If we're inside a "with p4.at_exception_level(p4.RAISE_NONE), then
    # don't pollute the log with expected errors. Log expected errors
    # at level "debug".
    _log_errors   = log_errors
    _log_warnings = log_warnings
    if p4.exception_level == p4.RAISE_NONE:
        _log_errors   = logging.DEBUG
        _log_warnings = logging.DEBUG

    # Dump in two groups: first the textual stuff, then all the numeric message
    # ID stuff. Too hard to read text + numeric when commingled.

    if (       (p4.errors   and log_err  .isEnabledFor(_log_errors  ))
        or not (p4.warnings and log_warn .isEnabledFor(_log_warnings))):

        for m in p4.messages:
            if m.msgid in _SQUELCHED_P4_RESULTS:
                continue
            if p4gf_p4msgid.E_FAILED <= m.severity:
                log_err.log(_log_errors, str(m))
            elif p4gf_p4msgid.E_WARN == m.severity:
                log_warn.log(_log_warnings, str(m))

    if log_msgid.isEnabledFor(logging.DEBUG2):
        for m in p4.messages:
            log_msgid.debug2(p4gf_p4msg.msg_repr(m))

#! /usr/bin/env python3.3
"""Search an XML formatted Git Fusion log file for matching records."""

import argparse
from datetime import datetime
import functools
import io
import os
import re
import shutil
import signal
import sys
import textwrap
import xml.sax
import xml.sax.handler
import xml.sax.saxutils

from p4gf_l10n import _
import p4gf_log
import p4gf_util

DIVIDER = '=' * 78
HEADER_PRINTED = False


class LogSource(io.RawIOBase):

    """Acts as an XML input source.

    Converts the poorly formed input XML "document" into a conformant file.
    In particular, adds the XML declaration and the enclosing document tag.

    """

    def __init__(self, source):
        """Initialize the LogSource instance."""
        self._preamble = io.BytesIO(b'<?xml version="1.0" encoding="iso-8859-1"?>\n<log>\n')
        self._source = source
        self._appendix = io.BytesIO(b'\n</log>\n')

    def readinto(self, b):
        """Read up to len(b) bytes into bytearray b."""
        for attr in ['_preamble', '_source', '_appendix']:
            src = getattr(self, attr)
            if src is not None:
                result = src.readinto(b)
                # The sax parser reads zero bytes initially to get the type
                # of the input stream, so only clear our fields when we are
                # really trying to read something.
                if result == 0 and len(b) != 0:
                    setattr(self, attr, None)
                else:
                    return result
        return 0

    def readable(self):
        """Return a bool indicating whether object was opened for reading."""
        # pylint: disable=no-self-use
        return True


class LogContentHandler(xml.sax.handler.ContentHandler):

    """Process the content events from the SAX parser."""

    def __init__(self, callback):
        """Initialize the LogContentHandler instance."""
        xml.sax.handler.ContentHandler.__init__(self)
        # Attribute names must match the expected tag names.
        self.pid = None
        self.dt = None  # pylint:disable=invalid-name
        self.lvl = None
        self.nm = None  # pylint:disable=invalid-name
        self.msg = None
        self.req = None
        self._current = None
        self._callback = callback

    def startElement(self, name, attrs):
        """Signal the start of an element in non-namespace mode."""
        if name == 'rec':
            self.pid = None
            self.dt = None
            self.lvl = None
            self.nm = None
            self.msg = None
            self.req = None
        elif name != 'log':
            self._current = name

    def endElement(self, name):
        """Signal the end of an element in non-namespace mode."""
        self._current = None
        if name == 'rec':
            if self.msg is None:
                self.msg = ''
            self._callback(self)

    def characters(self, content):
        """Receive notification of character data."""
        if self._current is None:
            return
        existing = getattr(self, self._current)
        if existing is None:
            existing = ''
        setattr(self, self._current, existing + content)

    def __str__(self):
        """Return the string form of this."""
        return "[{pid}] |{req}| <{dt}> {{{lvl}}} ({nm}) {msg}".format(
            pid=self.pid, dt=self.dt, lvl=self.lvl, nm=self.nm, msg=self.msg, req=self.req)


def search(regex, args, log_file, record):
    """Compare the record with the given search parameters.

    :param regex: compiled regular expression for matching (may be None).
    :param args: parsed command-line arguments.
    :param log_file: name of the log file being scanned.
    :param record: parsed log record.

    """
    if args.level and record.lvl.lower() != args.level:
        return
    if args.name and not args.name.search(record.nm.lower()):
        return
    if args.before or args.after:
        # Lazily parse the record date/time string.
        dt = datetime.strptime(record.dt, p4gf_log.XML_DATEFMT)
        if args.before and args.before < dt:
            return
        if args.after and args.after > dt:
            return
    matches = True
    if regex:
        # Lazily escape the record message.
        record.msg = xml.sax.saxutils.unescape(record.msg)
        matches = regex.search(record.msg)
    if matches:
        if args.pretty:
            # Wrap long lines in the message text and print on a separate
            # line, with indentation to set it apart from the other record
            # attributes.
            width = shutil.get_terminal_size().columns
            lines = []
            # Replace the escaped control characters with the real thing to
            # improve the readability of the message text.
            unescaped_msg = record.msg.replace('\\t', '\t').replace('\\n', '\n')
            for line in unescaped_msg.splitlines():
                lines.append(textwrap.fill(line, width=width, replace_whitespace=False,
                             initial_indent='    ', subsequent_indent='        '))
            msg = "\n".join(lines)
            out = "[{0}] <{1}> {{{2}}} ({3})\n{4}".format(
                record.pid, record.dt, record.lvl, record.nm, msg)
        else:
            out = str(record)
        global HEADER_PRINTED
        if not args.no_header and not HEADER_PRINTED:
            print(log_file)
            print(DIVIDER)
            HEADER_PRINTED = True
        print(out)


def levelup(level):
    """Return the canonical form of the level name."""
    lvl = level.lower()
    return 'warning' if lvl == 'warn' else lvl


def category_regex(string):
    """Convert the given category name to a compiled regular expression."""
    return re.compile(string.lower())


def strptime(string):
    """Parse the string as a date/time in the format found in the XML log."""
    try:
        return datetime.strptime(string, p4gf_log.XML_DATEFMT)
    except ValueError as ve:
        raise argparse.ArgumentTypeError(str(ve))


def main():
    """Parse the command-line arguments and perform the requested operation."""
    desc = _("""Search an XML formatted Git Fusion log file for matching records.""")
    epilog = _("""The --before and --after options take a date/time value with
    the following format: YYYY-mm-dd HH:MM:SS (e.g. --before '2015-11-17 10:12:49').
    The --name argument can take a regular expression, as supported by the `re`
    module in the Python standard library. 'e.g. (p4gf_git_repo_lock|(?<!p4\\.)cmd.*)'""")
    parser = p4gf_util.create_arg_parser(desc=desc, epilog=epilog)
    parser.add_argument('log', metavar="LOG", nargs='+',
                        help=_("name of log file(s) to be processed"))
    parser.add_argument('-q', '--query',
                        help=_("regular expression to select log records"))
    parser.add_argument('-l', '--level', type=levelup,
                        help=_("logging level by which to filter (e.g. debug, info, warning)"))
    parser.add_argument('-n', '--name', type=category_regex,
                        help=_("logging category name (e.g. p4gf_auth_server, p4gf_copy_to_p4)"))
    parser.add_argument('-B', '--before', type=strptime,
                        help=_("select log entries before the given date/time"))
    parser.add_argument('-A', '--after', type=strptime,
                        help=_("select log entries after the given date/time"))
    parser.add_argument('-i', '--ignore-case', action='store_true',
                        help=_("compare in a case-insensitive manner"))
    parser.add_argument('-p', '--pretty', action='store_true',
                        help=_("present the log records in a pleasing fashion"))
    parser.add_argument('--no-header', action='store_true',
                        help=_("do not print file name and divider between log files"))
    args = parser.parse_args()
    flags = re.IGNORECASE if args.ignore_case else 0
    regex = re.compile(args.query, flags) if args.query else None
    # Suppress the super annoying BrokenPipeError exception when piping our
    # output to a utility such as `head`.
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    for log_file in args.log:
        if not os.path.exists(log_file):
            sys.stderr.write(_('File does not exist: {logfile}\n').format(logfile=log_file))
            sys.exit(2)
        global HEADER_PRINTED
        HEADER_PRINTED = False
        callback = functools.partial(search, regex, args, log_file)
        with open(log_file, 'rb') as fobj:
            xml.sax.parse(LogSource(fobj), LogContentHandler(callback))


if __name__ == "__main__":
    main()

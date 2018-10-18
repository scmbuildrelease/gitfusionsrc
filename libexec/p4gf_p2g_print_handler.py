#! /usr/bin/env python3.3
"""PrintHandler."""

import codecs
import os
import logging
import tempfile

from P4 import OutputHandler, P4Exception

import p4gf_git
from   p4gf_l10n                  import _
from   p4gf_p2g_rev_list          import RevList
from   p4gf_p4file                import P4File
import p4gf_progress_reporter     as     ProgressReporter
import p4gf_pygit2
import p4gf_util

LOG = logging.getLogger('p4gf_copy_to_git').getChild('print_handler')


class PrintHandler(OutputHandler):

    """OutputHandler for p4 print, hashes files into git repo."""

    def __init__(self, ctx):
        """Initialize the PrintHandler instance."""
        OutputHandler.__init__(self)
        self.rev = None             # a P4File
        self.revs = RevList()
        self.temp_file = None
        self.p4 = ctx.p4
        self.p4gf = ctx.p4gf
        self.change_set = set()
        self.repo = ctx.repo
        self.ctx = ctx

    def outputBinary(self, h):
        """Assemble file content, then pass it to hasher via temp file."""
        try:
            LOG.debug3('outputBinary() called with {} bytes'.format(len(h)))
            self.appendContent(h)
        except Exception:  # pylint: disable=broad-except
            LOG.exception("outputBinary")
        return OutputHandler.HANDLED

    def outputText(self, h):
        """Assemble file content, then pass it to hasher via temp file.

        Either str or bytearray can be passed to outputText.  Since we
        need to write this to a file and calculate a SHA1, we need bytes.

        For unicode servers, we have a charset specified which is used to
        convert a str to bytes.

        For a nonunicode server, we will have specified "raw" encoding to
        P4Python, so we should never see a str.
        """
        try:
            if self.p4.charset and self.p4.charset != 'none':
                try:
                    # self.p4.__convert() doesn't work correctly here
                    if type(h) == str:
                        b = getattr(self.p4, '__convert')(self.p4.charset, h)
                    else:
                        b = getattr(self.p4, '__convert')(self.p4.charset, h.decode())
                except:
                    raise P4Exception(
                        _("error: failed '{charset}' conversion for '{path}#{rev}'")
                        .format(charset=self.p4.charset,
                                path=self.rev.depot_path,
                                rev=self.rev.revision))
            else:
                if type(h) == str:
                    raise RuntimeError(_('unexpected outputText'))
                b = h
            LOG.debug3('outputText() called with {} bytes'.format(len(b)))
            self.appendContent(b)
        except Exception:  # pylint: disable=broad-except
            LOG.exception("outputText")
        return OutputHandler.HANDLED

    def appendContent(self, h):  # pylint: disable=invalid-name
        """Append a chunk of content to the temp file.

        It would be nice to incrementally compress and hash the file
        but that requires knowing the size up front, which p4 print does
        not currently supply.  If/when it does, this can be reworked to
        be more efficient with large files.  As it is, as long as the
        TemporaryFile doesn't rollover, it won't make much of a difference.

        So with that limitation, the incoming content is stuffed into
        a TemporaryFile.
        """
        if not len(h):
            return
        if self.temp_file is None:
            LOG.warning('outputBinary/outputText called before outputStat, nothing can be done')
            # In fact, we don't even have the file meta information, so we
            # cannot provide any details for debugging this issue.
            return
        self.temp_file.write(h)

    def flush(self):
        """Hash the last printed file into the repo."""
        if not self.rev:
            LOG.debug3('flush() nothing to flush')
            return
        size = self.temp_file.tell()
        if size > 0 and self.rev.is_symlink():
            # p4 print adds a trailing newline, which is no good for symlinks.
            self.temp_file.seek(-1, 2)
            b = self.temp_file.read(1)
            if b[0] == 10:
                size = self.temp_file.truncate(size - 1)
        self.temp_file.close()
        # p4.charset could be None, empty string, or 'none'
        if (not self.p4.charset or self.p4.charset == 'none') and 'utf16' in self.rev.type:
            # Convert UTF-8 encoded files to UTF-16, but only for non-
            # Unicode servers, in which case we would have already handled
            # the conversion in the outputText() function.
            LOG.debug('flush() converting {} to UTF-16'.format(self.rev.depot_path))
            with open(self.temp_file.name, 'r', encoding="utf8") as fobj:
                temp_str = fobj.read()
            # Include a Byte Order Mark (why doesn't P4API do this for
            # us?). The choice of Little Endian is arbitrary, since we are
            # including the BOM anyway. Regardless, it is not possible to
            # know the endianness of the client, but LE is most likely.
            temp_data = getattr(self.p4, '__convert')("utf16le", temp_str)
            with open(self.temp_file.name, 'wb') as fobj:
                fobj.write(codecs.BOM_UTF16_LE)
                fobj.write(temp_data)
        LOG.debug3('flush() writing {} to Git repository'.format(self.rev.depot_path))
        try:
            tmpname = os.path.basename(self.temp_file.name)
            self.rev.sha1 = p4gf_pygit2.create_blob_fromdisk(self.repo, tmpname)
            self.revs.append(self.rev)
            self._chmod_644_minimum(self.rev.sha1)
        except Exception:  # pylint: disable=broad-except
            LOG.exception('failed to write blob to repository')
        finally:
            LOG.debug3('flush() removing temporary file {}'.format(self.temp_file.name))
            try:
                os.unlink(self.temp_file.name)
            finally:
                self.temp_file = None
                self.rev = None

    def outputStat(self, h):
        """Save path of current file."""
        try:
            self.flush()
            self.rev = P4File.create_from_print(h)
            self.change_set.add(self.rev.change)
            ProgressReporter.increment(_('Copying files'))
            LOG.debug2("PrintHandler.outputStat() ch={} {}#{}".format(
                self.rev.change, self.rev.depot_path, self.rev.revision))
            # use the git working tree so we can use create_blob_fromfile()
            tmpdir = os.getcwd()
            self.temp_file = tempfile.NamedTemporaryFile(
                buffering=10000000, prefix='p2g-print-', dir=tmpdir, delete=False)
            LOG.debug3('outputStat() temporary file created: {}'.format(self.temp_file.name))
        except Exception:  # pylint: disable=broad-except
            LOG.exception("outputStat")
        return OutputHandler.HANDLED

    def outputInfo(self, h):
        """outputInfo call not expected."""
        try:
            LOG.debug3('outputInfo() called, ignoring {}'.format(h))
        except Exception:  # pylint: disable=broad-except
            LOG.exception("outputInfo")
        return OutputHandler.REPORT

    def outputMessage(self, h):
        """outputMessage call not expected, indicates an error."""
        try:
            LOG.debug3('outputMessage() called, ignoring {}'.format(h))
        except Exception:  # pylint: disable=broad-except
            LOG.exception("outputMessage")
        return OutputHandler.REPORT

    def _chmod_644_minimum(self, sha1):
        """Ensure blob file mode is correct.

        pygit2 created a blob. If that blob is a loose object,
        make sure that loose object's file has at least file mode 644
        so that we (owner) has read+write, and the world has read access.

        Don't raise/abort if this fails due to file not found. Assume that
        is due to the blob landing in a packfile.
        """
        obj_path = p4gf_git.object_path(sha1)
        if obj_path is None:
            # Object is already in the repo in a pack file? Nothing
            # more do be done here.
            if LOG.isEnabledFor(logging.DEBUG2):
                LOG.debug2("_chmod_644_minimum() no such loose blob %s, nothing to do", sha1)
            return
        blob_path = os.path.join(self.ctx.repo_dirs.GIT_WORK_TREE, obj_path)
        try:
            p4gf_util.chmod_644_minimum(blob_path)
        except OSError as e:
            LOG.warning("chmod 644 failed path={path} err={e}".format(path=blob_path, e=e))

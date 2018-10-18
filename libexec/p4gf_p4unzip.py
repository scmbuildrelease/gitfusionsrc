#! /usr/bin/env python3.3
"""Code for creating a generic `p4 unzip` archive."""
import logging
import os
import zipfile
import p4gf_util

LOG = logging.getLogger("p4gf_fast_push.zip")

class P4Unzip:
    """Holder of our open file pointers while we build a generic `p4 unzip`
    archive.

    Teach P4Unzip only `p4 unzip`. Teach it nothing about Git Fusion
    specifics. That knowledge belongs in XXX (which we're about to refactor
    to create).

    Holds open a zip archive to which you can write files directly, skipping
    any intermediate tempfile.

    Also holds open the 3 journal files that `p4 unzip` accepts:
      changedata.jnl
      contextdata.jnl
      integdata.jnl

    When you're ready, closes the journal records, copies them and a MANIFEST
    to the zip archive, then closes the zip archive so you can send it
    to the Perforce server.

    Expected call sequence:
        Open
            p4unzip = P4Unzip(tmpdirpath)
            p4unzip.open_all()

        Write journal records
            p4unzip.change.jnl( record_str )
            p4unzip.context.jnl( record_str )
            p4unzip.integ.jnl( record_str )

        Write zip archive files
            p4unzip.zip.fp.writestr(archive_relpath, raw_bytes)

        Close journal files, write MANIFEST, close zip archive
            close_all()

        Send to Perforce
            p4run(['unzip', p4unzip.zip.abspath])

        Clean up:
            rm -rf tmpdirpath
    """

    def __init__(self, dirname):
        d = os.path.abspath(dirname)
        self._dir_abspath = d

        self.change  = FileStruct(d, "changedata.jnl")
        self.context = FileStruct(d, "contextdata.jnl")
        self.integ   = FileStruct(d, "integrationdata.jnl")
        self.zip     = FileStruct(d, "archive.zip")

                        # For easier iterating through journal files,
                        # since we often operate on them all at the same time.
        self.jnl     = [ self.change
                       , self.context
                       , self.integ ]

    def open_all(self):
        """Open zipfile and journal files for write.

        Generates temp files for all of 'em.
        """
        p4gf_util.ensure_dir(self._dir_abspath)
        for j in self.jnl:
            assert j.fp is None
            j.fp = open(j.abspath, "w", encoding="utf-8")
        self.zip.fp = zipfile.ZipFile(self.zip.abspath, "w"
            #, compression = zipfile.ZIP_STORED
                        # ZIP_DEFLATED saves 50% temporary/working disk space,
                        # but costs 5% additional time for the compression.
            , compression = zipfile.ZIP_DEFLATED
            , allowZip64  = True )

    def copy_journal_files_to_zip_file(self):
        """Copy our change/context/whatever journal files to zipfile.

        Requires that they already be closed, or you'll miss the last
        few records.
        """
                        # Close 'em to flush content to disk and avoid
                        # accidentally writing anything more.
        self.close_journal_files()
        for j in self.jnl:
            self.zip.fp.write( filename = j.abspath    # source
                             , arcname  = j.filename ) # dest within archive

    def write_manifest_to_zip_file(self, ctx):
        """Write our MANIFEST file to zipfile.

        Writes to disk, then zipfile.

        ctx required so we can record case sensitivity.
        """
        abspath    = os.path.join(self._dir_abspath, "MANIFEST")
        _write_manifest(abspath, self.change.db_change_ct, ctx)
        self.zip.fp.write( filename = abspath         # source
                         , arcname  = "MANIFEST" )    # dest within archive

    def close_all(self, ctx):
        """Close journal files, copy to zip, add manifest, close the zip."""
        self.copy_journal_files_to_zip_file()
        self.write_manifest_to_zip_file(ctx)
        self.close_zip_file()

    def close_journal_files(self):
        """Close all journal files.

        Required before you can write them to the zip archive, otherwise the
        archive won't see any unflushed writes to these files.
        """
        for j in self.jnl:
            j.close()

    def close_zip_file(self):
        """Close the zip file. Do this last.

        Don't really need a function for this, but writing one anyway for
        symmetry with close_journal_files().
        """
        self.zip.close()

# ----------------------------------------------------------------------------

class FileStruct:
    """One file that we work with."""
    def __init__(self, dirpath, filename):
        self.filename     = filename
        self.abspath      = os.path.join(os.path.abspath(dirpath), filename)
        self.fp           = None    # pylint:disable=invalid-name
        self.jnl_ct       = 0
        self.db_change_ct = 0       # To help us automatically fill
                                    # in MANIFEST change count

    def close(self):
        """Close file and drop the file pointer so we don't accidentally
        try to use it after close.
        """
        if self.fp:
            self.fp.close()
            self.fp = None

    def jnl(self, record_str, is_db_change = False):
        """Write a journal record."""
        self.fp.write(record_str)
        self.fp.write("\n")
        self.jnl_ct += 1
        if is_db_change:
            self.db_change_ct += 1

# -- module-wide -------------------------------------------------------------

def _write_manifest(abspath, change_ct, ctx):
    """Write a tiny text file with data about this `p4 unzip` payload."""
    cs  = "sensitive" if ctx.server_is_case_sensitive else "insensitive"
    uni = "enabled" if ctx.p4.server_unicode else "disabled"

    lines = [ "export-version = 0.6"
            , "server-version = 40"
            , "total-changes = {}".format(change_ct)
            , "unicode-handling = {}".format(uni)
            , "case-handling = {}".format(cs)
            , "schema-version = 40"
            ]
    content = "\n".join(lines) + "\n"
    with open (abspath, "w", encoding="utf-8") as f:
        f.write(content)

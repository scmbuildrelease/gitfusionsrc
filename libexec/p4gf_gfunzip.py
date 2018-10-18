#! /usr/bin/env python3.3
"""Git Fusion-specific knowledge about building various `p4 unzip`
payloads.
"""
import hashlib
import logging

import p4gf_config
from   p4gf_fast_push_librarian         import depot_to_archive_path \
                                             , lbr_rev_str
from   p4gf_p4unzip                     import P4Unzip
from   p4gf_fast_push_gitmirror_change  import GitMirrorChange
import p4gf_git
from   p4gf_hex_str                     import md5_str
from   p4gf_object_type                 import commit_depot_path
import p4gf_p4dbschema
import p4gf_path
from   p4gf_time_zone                   import utc_dt_to_p4d_secs

LOG = logging.getLogger("p4gf_fast_push.gfunzip")


class GFUnzip:
    """Git Fusion knowledge about what goes into a `p4 unzip` payload.

    For the first Git Fusion unzip payload, knows how to store:
        //.git-fusion/objects/blobs/...
        //.git-fusion/objects/trees/...

    For the second Git Fusion unzip payload, knows how to store
    translated commits, and lazy-copy revisions from the first payload.
        //depot/... (or wherever)
        //.git-fusion/branches/{repo}/...

    For the third Git Fusion unzip payload, knows how to fill the
    GitMirror's ObjectType data, repo config files, and depot
    branch-info files:
        //.git-fusion/objects/repos/{repo}/commits/...
        //.git-fusion/repos/p4gf_config(2)
        //.git-fusion/branch-info/...

    First and third payloads use GitMirrorChange, which knows to switch to
    a new changelist once the current one fills up with 1 million revisions.

    Originally started as three separate classes, but their AT-field
    collapsed and the three merged into one big vat of butterscotch
    pudding.

    write_xxx() functions expect you to have done most of the work.
                They're little more than journal record formatters/writers.
                This lower-level API exists because its called by code that
                already has the data handy, usually from a LibrarianStore
                or other source.

    copy_xxx() functions do the work for you.
                This higher-level API exists because there are no stores
                or sources of this data.

    Expected call sequence:
        open:
            gfunzip = GFUnzip(ctx, dirname)

        fill loop:
            gfunzip.write_xxx()
            gfunzip.copy_xxx()

        close:
            gfunzip.close()
    """

    def __init__(self, ctx, dirname):
        """
        :param dirname: directory name (not a path, no slashes!)
                        where P4Unzip can build its archive.
                        Must be empty, must not be shared with other
                        P4Unzip accumulators.
        """
        self.ctx      = ctx
        self.p4unzip  = P4Unzip(dirname)

                        # gmchange not used for second payload "Commits".
                        # NOPs, writes no no db.change, when not used.
        self.gmchange = GitMirrorChange(self.ctx, self.p4unzip)

    def open(self):
        """Open zipfile and all journal files for future write."""
        self.p4unzip.open_all()

    def close(self):
        """Close journal files, copy to zip, close the zip."""
                        # Flush any open change before writing
                        # everything to zip.
        self.gmchange.close()
        self.p4unzip.close_all(self.ctx)

    # -- Git Fusion paths ----------------------------------------------------
    #    static functions to generate paths.
    #    See also:
    #       ObjectType.to_depot_path()
    #       DepotBranchInfo.to_config_depot_path()

    # -- Writing content and db.rev records ----------------------------------

    def write_blob_rev_1( self, *
                        , sha1
                        , md5
                        , raw_bytes
                        , lbr_file_type
                        ):
        """Write a 'p4 add rev#1' record to zipfile.

        Use this only for files that have a single revision such as
        //.git-fusion/objects/blobs/... and trees/...

        I'll either write a separate function for adding
        """
        self._add_gmchange_rev(
              depot_path         = p4gf_path.blob_p4_path(sha1)
            , md5                = md5
            , raw_bytes          = raw_bytes
            , lbr_file_type      = lbr_file_type
            , lbr_rev_change_num = 1
            )

    def write_rev(self, db_rev, *
                 , context_db_rev = None
                 , context_rev_0  = False
                 ):
        """Write a single db.rev record to changedata.jnl. If this is the
        first revision, also write a zero-th revision to contextdata.jnl.

        Use this for general revs in translated commits: adds, edits, deletes.
        Do not use this for the //.git-fusion/objects/blobs/... bat_gfunzip
        blobs.

        context_rev_0: If true, then use db_rev for contextdata.jnl, too.
                       Just with depot_rev temporarily set to 0 for the jnl
                       write. This optimization saves an unnecessary
                       construction of a copy of db_rev, which saves about 5%
                       time in one of Zig's tests.
        """
        self.p4unzip.change.jnl(str(db_rev))
        if context_db_rev:
            self.p4unzip.context.jnl(str(context_db_rev))
        elif context_rev_0:
            save = db_rev.depot_rev
            db_rev.depot_rev = 0
            self.p4unzip.context.jnl(str(db_rev))
            db_rev.depot_rev = save

    def write_changelist( self, *
                        , ctx
                        , change_num
                        , client
                        , user
                        , date_utc_dt
                        , description
                        ):
        """Write a single changelist to zip.
        - db.change
        - db.desc

        Does not include db.rev or db.integ records. Do those in separate
        calls to write_rev() and write_integ().
        """
        jnl_change = p4gf_p4dbschema.db_change(
                  change_num  = change_num
                , client      = client
                , user        = user
                , date        = utc_dt_to_p4d_secs(date_utc_dt, ctx)
                , description = description
                )
        self.p4unzip.change.jnl(jnl_change, is_db_change = True)

        jnl_desc = p4gf_p4dbschema.db_desc(
                  change_num  = change_num
                , description = description
                )
        self.p4unzip.change.jnl(jnl_desc)

    def write_dbi(self, dbi):
        """Write a depot branch-info file to our payload."""
        dbi_config = dbi.to_config()
        no_comment_header = ""
        dbi_str    = p4gf_config.to_text(no_comment_header, dbi_config)
        dbi_bytes  = dbi_str.encode("utf-8")

        self._add_gmchange_rev(
              depot_path         = dbi.to_config_depot_path()
            , md5                = hashlib.md5(dbi_bytes).hexdigest().upper()
            , raw_bytes          = dbi_bytes
            , lbr_file_type      = p4gf_p4dbschema.FileType.CTEXT
            , lbr_rev_change_num = 1
            )

    def write_integ_pair(self, *
        , src_depot_path
        , src_start_rev_int
        , src_end_rev_int
        , dest_depot_path
        , dest_rev_int
        , how
        , how_r
        , dest_change_num_int
        ):
        """Write a PAIR of db.integed rows for a single file integ action."""
        integed_pair = p4gf_p4dbschema.db_integed_pair(
              src_depot_path      = src_depot_path
            , src_start_rev_int   = src_start_rev_int
            , src_end_rev_int     = src_end_rev_int
            , dest_depot_path     = dest_depot_path
            , dest_rev_int        = dest_rev_int
            , how                 = how
            , how_r               = how_r
            , dest_change_num_int = dest_change_num_int
            )
        self.p4unzip.integ.jnl(integed_pair[0])
        self.p4unzip.integ.jnl(integed_pair[1])


    # -- Extracting from Git, then writing content and db.rev records---------

    def copy_tree(self, tree_sha1):
        """Copy a Git ls-tree to zipfile.

        Return the number of bytes extracted/written.
        """
        return self._copy_compressed_git_object(
              sha1               = tree_sha1
            , depot_path         = p4gf_path.tree_p4_path(tree_sha1)
            , lbr_rev_change_num = 1
            )

    def copy_commit(self, ot, submitted_change_num):
        """Copy a Git commit to our GitMirror objecttype store,
        with the commit sha1, branch_id, and Perforce changelist number
        all included in the depot path.

        Return the number of bytes extracted/written.
        """
        depot_path = commit_depot_path(
              commit_sha1   = ot.sha1
            , change_num    = submitted_change_num
            , repo          = ot.repo_name
            , branch_id     = ot.branch_id
            )
        return self._copy_compressed_git_object(
              sha1               = ot.sha1
            , depot_path         = depot_path
            , lbr_rev_change_num = self.gmchange.change_num
            )

    def _copy_compressed_git_object(self, *
            , sha1
            , depot_path
            , lbr_rev_change_num
            ):
        """Extract a Git commit or tree object from Git, compress it,
        then write it to the `p4 unzip` archive as a file revision.

        Return the number of bytes extracted/written.
        """
        zlib_bytes   = p4gf_git.cat_file_compressed(
                                self.ctx.repo, sha1)
        self._add_gmchange_rev(
              depot_path         = depot_path
            , md5                = hashlib.md5(zlib_bytes).hexdigest().upper()
            , raw_bytes          = zlib_bytes
            , lbr_file_type      = p4gf_p4dbschema.FileType.UBINARY
            , lbr_rev_change_num = lbr_rev_change_num
            )
        return len(zlib_bytes)

    # ------------------------------------------------------------------------

    def _add_gmchange_rev( self, *
               , depot_path
               , md5
               , raw_bytes
               , lbr_file_type
               , lbr_rev_change_num = 0
               ):
        """Write a db.rev record to 'p4 add' both change and context
        journal files.

        Designed for our blobs and other gitmirror files that have
        exactly one revision, addressed by sha1. Always adds, always
        creates rev 1, always requires no previous rev.

        :param lbr_rev_change_num:
            If zero, uses current self.change_num. You usually want this
                unless you're writing the bat_gfunzip, where we lock all lbrRev
                strings to "1.1"
            If non-zero, use that. bat_gfunzip passes int(1).
        """
        lbr_path     = depot_path
        archive_path = depot_to_archive_path(depot_path)
        self.p4unzip.zip.fp.writestr(archive_path, raw_bytes)

                        # Open for 'p4 add'.
        self.gmchange.ensure_writable()
        jnl_rev = p4gf_p4dbschema.db_rev(
                  depot_path            = depot_path
                , depot_rev             = 1
                , depot_file_type_bits  = lbr_file_type
                , file_action_bits      = p4gf_p4dbschema.FileAction.ADD
                , change_num            = self.gmchange.change_num
                , date                  = self.gmchange.date_seconds()
                , md5                   = md5_str(md5)
                , uncompressed_byte_ct  = len(raw_bytes)
                , lbr_is_lazy           = p4gf_p4dbschema.RevStatus.NOT_LAZY
                , lbr_path              = lbr_path
                , lbr_rev               = lbr_rev_str(lbr_rev_change_num)
                , lbr_file_type_bits    = lbr_file_type
                )
        self.p4unzip.change.jnl(jnl_rev)

                    # Before this rev#1, there was no rev.
                    # Same journal record, but with depot_rev dropped to 0.
        jnl_rev = p4gf_p4dbschema.db_rev(
                  depot_path            = depot_path
                , depot_rev             = 0
                , depot_file_type_bits  = lbr_file_type
                , file_action_bits      = p4gf_p4dbschema.FileAction.ADD
                , change_num            = self.gmchange.change_num
                , date                  = self.gmchange.date_seconds()
                , md5                   = md5_str(md5)
                , uncompressed_byte_ct  = len(raw_bytes)
                , lbr_is_lazy           = p4gf_p4dbschema.RevStatus.NOT_LAZY
                , lbr_path              = lbr_path
                , lbr_rev               = lbr_rev_str(lbr_rev_change_num)
                , lbr_file_type_bits    = lbr_file_type
                )
        self.p4unzip.context.jnl(jnl_rev)


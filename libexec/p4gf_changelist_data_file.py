#! /usr/bin/env python3.3
"""ChangelistDataFile."""
import p4gf_const
from   p4gf_l10n import NTR
import p4gf_util


class ChangelistDataFile:
    """Per-changelist data files that provide more information about a
    Git-originated changelist than what Perforce itself can store.

    DO NOT CREATE WITHOUT TALKING TO ZIG
    Use these _sparingly_. As in "you're going to have to convice both
    Zig and Alan that you can't get this data any other way" sparingly.
    Currently only Git Swarm review changelists are permitted to create
    these files.

    Each repo can have ONLY ONE changelist data file per changelist.
    No ObjectTypeList-like expansion of branches or Git commits here.

    Yes, one file per repo. Makes it easy for p4gf_delete_repo.py to delete
    these files. Prepares for an unlikely-to-occur future where two different
    repos might have data for the same changelist.

    ### Current file format is a placeholder. Easiest for Swarm would be JSON or
    ### some other trivially machine-parsable format. Hand-formatting just for
    ### now to get the file I/O plumbing working without distracting myself with
    ### parser APIs.
    """

    def __init__(self, ctx, change_num):
        self.ctx                    = ctx
        self.change_num             = change_num

                        # Git commits that are part of this review:
                        # Commits that contribute to ("are reachable by") the
                        # review commit, but not the destination branch.
        self.ancestor_commit_otl    = None

                        # Indicates whether matrix2 code was in effect or not.
        self.matrix2                = False

    def depot_path(self):
        """Return //.git-fusion/changelists/{repo}/{change_num}"""
        return p4gf_const.P4GF_CHANGELIST_DATA_FILE.format(
                      P4GF_DEPOT = p4gf_const.P4GF_DEPOT
                    , repo_name  = self.ctx.config.repo_name
                    , change_num = self.change_num )

    def local_path(self):
        """Return P4GF_HOME/changelists/{repo}/{change_num}"""
        return p4gf_util.depot_to_local_path(
                                       depot_path  = self.depot_path()
                                     , p4          = self.ctx.p4gf
                                     , client_spec = self.ctx.client_spec_gf )

    def write(self):
        """Create a local file P4GF_HOME/changelists/{repo}/{change_num}
        and tells GitMirror about it.

        Does not yet open file for 'p4 add' or 'p4 edit': let
        GitMirror.add_objects_to_p4() do that much later.
        """
        lpath = self.local_path()
        p4gf_util.ensure_dir(p4gf_util.parent_dir(lpath))
        p4gf_util.make_writable(lpath)
        with open(lpath, 'w') as f:
            self._write(f)

    def _write(self, f):
        """Dump to a data file while it's open and ready for output."""
        self._write_ancestor_commit_list(f)
        self._write_matrix2(f)

    def _write_ancestor_commit_list(self, f):
        """If we have any ancestor commits, write a section listing all of them."""
        if not self.ancestor_commit_otl:
            return

        f.write(NTR('[ancestor-list]\n'))
        for ot in self.ancestor_commit_otl:
            f.write(NTR('{sha1} {change_num}\n')
                    .format( sha1       = ot.sha1
                           , change_num = ot.change_num ))
        f.write('\n')

    def _write_matrix2(self, f):
        """If matrix2 is True, write out the [matrix2] tag."""
        if not self.matrix2:
            return
        f.write(NTR('[matrix2]\n'))

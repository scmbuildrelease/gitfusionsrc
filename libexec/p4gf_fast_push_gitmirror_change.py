#! /usr/bin/env python3.3
"""A db.change recorder for Git Fusion data such as
our blobs, trees, and commits copied from Git.
"""
import logging

import p4gf_gitmirror
from   p4gf_l10n                        import NTR
import p4gf_p4dbschema
import p4gf_time_zone

LOG = logging.getLogger("p4gf_fast_push.gitmirror_change")

class GitMirrorChange:
    """A db.change recorder for anything that isn't versioned
    files from Git:
    * blobs
    * trees
    * commits (the ObjectType gitmirror backup of them)
    * depot branch-info files
    * Git Fusion repo config files

    Knows to "close" the current change, write its db.change and db.desc
    records to the zip archive, then "open" a new one after 1 million revs.

    Assumes it is the ONLY source of db.change records for its
    zipfile. Maintains its own change counter.


    Expected call sequence:

    Open:
        gmchange = GitMirrorChange(ctx, p4unzip)

    Write revisions:
        gmchange.ensure_writable(...)
        change_num  = gmchange.change_num
        change_date = gmchange.date_seconds()

    Close:
        gmchange.close()
    """
                        # Perforce can handle millions of files added
                        # in a single changelist. But browsing tools
                        # tend to crash when asked to display such
                        # changelists. Keep it under one megarev.
    MAX_REVS_PER_CHANGELIST = 10**6

    def __init__(self, ctx, p4unzip):
        self.ctx         = ctx
        self.p4unzip     = p4unzip
        self.change_num  = 1
        self.date_p4_dt  = _now_p4_dt(ctx)
        self.rev_ct      = 0

    def db_change(self):
        """Return a db.change journal record as a single string."""
        return p4gf_p4dbschema.db_change(
               change_num  = self.change_num
             , client      = self.ctx.p4gf.client
             , user        = self.ctx.config.p4user
             , date        = self.date_seconds()
             , status      = p4gf_p4dbschema.ChangeStatus.COMMITTED
             , description = self.description()
             , root        = NTR("//...")
             )

    def db_desc(self):
        """Return a db.desc journal record as a single string."""
        return p4gf_p4dbschema.db_desc( change_num  = self.change_num
                                      , description = self.description() )

    def description(self):
        """Return our "Repo 'xxx' copied from Git." description."""
        return (p4gf_gitmirror.DESCRIPTION
                .format(repo = self.ctx.config.repo_name))

    def date_seconds(self):
        """Return seconds since the epoch, in Perforce server timezone."""
        return int(self.date_p4_dt.timestamp())

    def ensure_writable(self):
        """If the current "open" db.change has 1 million revisions,
        close it and start a new one.
        """
                        # Currently open change is full.
                        # Close it.
        if self.MAX_REVS_PER_CHANGELIST <= self.rev_ct:
            self.close()
            self._open()
        self.rev_ct += 1

    def _open(self):
        """Create a new db.change changelist record which we'll use as a
        container for future db.rev file revisions.
        """
        self.change_num += 1
        self.date_p4_dt = _now_p4_dt(self.ctx)
        self.rev_ct     = 0

    def close(self):
        """Write the currently "open" db.change changelist record to
        the zip archive, then "close" it so no more revisions can use
        it as a container.

        Do not call if you haven't written any db.rev records to this change.
        Perforce prohibits empty changelists.
        """
        if not self.rev_ct:
            return
        self.p4unzip.change.jnl(self.db_change(), is_db_change = True)
        self.p4unzip.change.jnl(self.db_desc())

# end class GitMirrorChange
# ----------------------------------------------------------------------------


# -- module-wide -------------------------------------------------------------

def _now_p4_dt(ctx):
    """Return the current time, as integer seconds since the epoch,
    in the Perforce server's time zone.
    """
    return (p4gf_time_zone.now_utc_dt()
             .astimezone(p4gf_time_zone.server_tzinfo(ctx)))


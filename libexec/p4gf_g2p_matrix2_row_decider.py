#! /usr/bin/env python3.3
"""class RowDecider."""

from   collections              import deque, namedtuple
import logging

                        # Avoid import cycles:
                        # Do not import p4gf_g2p_matrix2.

import p4gf_const
from   p4gf_l10n                import _
from   p4gf_g2p_matrix_column   import G2PMatrixColumn as Column
import p4gf_g2p_matrix2_common                         as common
from   p4gf_g2p_matrix2_decided import Decided
import p4gf_g2p_matrix2_ghost
import p4gf_g2p_matrix2_integ                          as p4gf_g2p_matrix_integ

LOG = common.LOG.getChild('row_decider')    # p4gf_g2p_matrix2.row_decider

                        # pylint:disable=protected-access
                        # Yeah, we're reaching into G2PMatrix2

# -- RowDecider ---------------------------------------------------------------


class RowDecider:

    """Move the decision code to its own class where partial per-row decision
    state can live.

    Reusable. Call decide() over and over.
    """

    def __init__(self, matrix):

        self.m                          = matrix    # pylint:disable=invalid-name
                                                    # Invalid name "m" for type attribute
                                                    # (should match [a-z_][a-z0-9_]{2,40}$)
                                                    # Tough. It's a backpointer undeserving of more
                                                    # than one char.

        self.row                        = None
        self.gdest_cell                 = None
        self.is_integ_branch_or_delete  = False
        self.has_integ                  = False
        self.populate_from_cell         = None
        self.p4jitfp_cell               = None
        self.git_delta_cell             = None
        self.ghost_cell                 = None
        self.integ_dest_cell            = None

                # deque of all GPARN and some GPARFPN Columns
        self.integ_work_queue           = None

    def _reset(self):
        """Clear out anything we set as part of decide().

        Keep only our backpointer to owning Matrix.
        """
        self.row                        = None
        self.gdest_cell                 = None
        self.is_integ_branch_or_delete  = False
        self.has_integ                  = False
        self.populate_from_cell         = None
        self.p4jitfp_cell               = None
        self.git_delta_cell             = None
        self.ghost_cell                 = None
        self.integ_dest_cell            = None
        self.integ_work_queue           = None

    @staticmethod
    def _col_index(column):
        """Column index, for logging columns."""
        if column:
            return column.index
        else:
            return '-'

    def decide(self, row):
        """Main entry point for RowDecider."""
        self._reset()
        self.row = row

        self._raise_if_symlink_in_gdest_path()

        self.gdest_cell      = row.cell_if_col(self.m._gdest_column)
        self.p4jitfp_cell    = row.cell_if_col(self.m._p4jitfp_column)
        self.git_delta_cell  = row.cell_if_col(self.m.git_delta_column)
        self.integ_dest_cell = row.cell_if_col(self.m._integ_dest_column)

        if LOG.isEnabledFor(logging.DEBUG2):
            LOG.debug2('RowDecider.decide() GDEST={gdest}'
                       ' P4JITFP={p4jitfp} git_delta={git_delta}'
                       ' pop_fm={pop_fm} row={row}'
                       .format(
                          row       = row.gwt_path
                        , gdest     = self._col_index(self.m._gdest_column)
                        , pop_fm    = self.m._populate_from_column.index
                                      if self.m._populate_from_column else None
                        , p4jitfp   = self._col_index(self.m._p4jitfp_column)
                        , git_delta = self._col_index(self.m.git_delta_column)
                        ))

        self._decide_populate_from(exists_in_git = self.row.exists_in_git())

                        # Git LFS file? Then it's too complex to try to integ
                        # it from any GPAR(fp)N at the same time we integ from
                        # LFS de-dupe storage. Ignore/bypass GPAR(fp)N. The
                        # only integ we might decide() for an LFS file is a JIT
                        # integ from P4JITFP.
        lfs_oid = None
        if self.m.ctx.is_lfs_enabled:
            lfs_oid = _lfs_oid(self.gdest_cell)
        if not lfs_oid:
                        # Integrate from each Git parent commit, and possibly
                        # from any fully populated bases for those parent
                        # commits. Using a queue so that we can add GPARFPN
                        # columns later if we need to.
            self.integ_work_queue = \
                deque(col for col in Column.of_col_type( self.m.columns, Column.GPARN))
            common.debug3('Row.decide() initial integ_work_queue={col}'
                          , col=[col.index for col in self.integ_work_queue])
            while self.integ_work_queue:
                column = self.integ_work_queue.pop()    # GPARN or GPARFPN
                self._decide_integ_from_column(column)

        self._decide_gdest_have_delta()

                        # Git says Add/Modify/Delete? Do so.
        if not self.has_integ:
                        # Apply Git's requested add/edit/delete.
            self._apply_git_delta()

                        # If add/edit/deleting, do we also need to JIT-branch?
            self._decide_jit_if()

        self._remove_duplicate_integ()

                        # decide() does not fill in row.p4_request or
                        # row.p4filetype. Those can depend on whether do_it()
                        # successfully performs decided integ actions, or if an
                        # integ fallback command has to happen.

        # LOG.error('RowDecider.decide() result={}'
        #          .format(row.to_log_level(logging.DEBUG3)))

    def ghost_decide(self, row):
        """Main entry point from Matrix.ghost_decide().

        What Perforce file action, if any, should we record in a
        ghost changelist?

        Store decision in ghost_cell.decided
        """
                        # pylint:disable=too-many-branches
        self._reset()

                        # Never include .p4gf_empty_changelist_placeholder in a
                        # GHOST changelist. Doing so will erroneously trigger
                        # "need to delete this file that isn't in Git" actions,
                        # which in turn leads to ghost changelists that do
                        # nothing but delete a placeholder. Pointless waste.
        if row.gwt_path == p4gf_const.P4GF_EMPTY_CHANGELIST_PLACEHOLDER:
            return

        self.row = row

        self.ghost_cell     = row.cell_if_col(self.m.ghost_column)
        self.gdest_cell     = row.cell_if_col(self.m._gdest_column)
        self.git_delta_cell = row.cell_if_col(self.m.git_delta_column)
        self.p4jitfp_cell   = row.cell_if_col(self.m._p4jitfp_column)

        orig_gparn_col = self.m.ghost_orig_gparn_column
        gparn_cell = row.cell_if_col(orig_gparn_col)
        if orig_gparn_col:
            gparfpn_cell = row.cell_if_col(orig_gparn_col.fp_counterpart)
        else:
            gparfpn_cell = None

                        # Special case for git-lfs-initial-track
                        # top-level .gitattributes files that do not (yet)
                        # exist in Perforce but do in Git.
                        #
                        # This is rare.
        decided = self._ghost_decide_from_git_lfs_initial_track(gparn_cell)

                        # Normal decision matrix code.
        if not decided:
            decided = self._ghost_decide_from_matrix(
                      gparn_cell     = gparn_cell
                    , gparfpn_cell   = gparfpn_cell
                    , orig_gparn_col = orig_gparn_col
                    )
                        # If we need to ghost LFS content into existence, do so
                        # from LFS de-dupe storage, NOT the usual ghost source.
        if (    decided
            and decided.p4_request != p4gf_g2p_matrix2_ghost.DELETE
            and self.ghost_cell.discovered
            and common.KEY_LFS in self.ghost_cell.discovered ):
            was_branch_delete = decided.branch_delete
            d2 = Decided( p4_request    = common.P4_REQUEST_LFS_COPY
                        , branch_delete = was_branch_delete
                        , integ_input   = decided.integ_input
                        )
            decided = d2

                        # Lazy-create a cell to hold the decision.
                        # +++ Unless that decision is "None".
                        # +++ Don't need a whole cell just to say "None".
        if decided:
            self._lazy_create_ghost_cell_discovered()
        if self.ghost_cell:
            self.ghost_cell.decided = decided
        return

    def _ghost_decide_from_matrix( self, *
                                 , gparn_cell
                                 , gparfpn_cell
                                 , orig_gparn_col
                                 ):
        """Run a row through the ghost decision matrix.

        Returns a Decided value if something to do in a ghost changelist,
        None if nothing to do, or raises exception if surprised.

        Basically a big Python switch statement, without the clever
        Python dict wrapper.
        """
        decision_input = p4gf_g2p_matrix2_ghost.to_input(
                      p4jitfp_cell   = self.p4jitfp_cell
                    , gdest_cell     = self.gdest_cell
                    , gparn_cell     = gparn_cell
                    , gparfpn_cell   = gparfpn_cell
                    , ghost_cell     = self.ghost_cell
                    , gdest_column   = self.m._gdest_column
                    , gparn_column   = orig_gparn_col )

        decision_row = p4gf_g2p_matrix2_ghost.find_row(decision_input)
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3("ghost_decide() {gwt:<15} {input} {output:<6} {comment}"
                      .format( input   = p4gf_g2p_matrix2_ghost.deb(
                                              decision_input
                                            , p4gf_g2p_matrix2_ghost.LONG)
                             , output  = decision_row.output
                             , gwt     = self.row.gwt_path
                             , comment = decision_row.comment
                             ))

        decided = None
        if not decision_row:
            raise RuntimeError(_('ghost decision input {input} matched no known'
                                 ' decision table entry {gwt_path}')
                               .format(  input    = p4gf_g2p_matrix2_ghost.deb(decision_input)
                                       , gwt_path = self.row.gwt_path))

                        # Branch from GPARN.
        if    decision_row.output ==   p4gf_g2p_matrix2_ghost.BRANCH:
            decided = Decided( integ_flags      = '-Rbd'
                             , resolve_flags    = '-at'
                             , on_integ_failure = Decided.FALLBACK
                             , integ_fallback   = 'add'
                             , p4_request       = None
                             , integ_input      = decision_input
                             )
                        # To simplify later code, copy integration source
                        # from GPARN/GPARNFP into GHOST column so that
                        # later code can use GHOST as integ source.
            self._lazy_create_ghost_cell_discovered()
            _copy_discovered(self.ghost_cell, gparn_cell, gparfpn_cell)

                        # Branch from P4JITFP, submit,
                        # then DELETE in a second GHOST changelist.
        elif  decision_row.output ==   p4gf_g2p_matrix2_ghost.BRANCH_DELETE:
            decided = Decided( integ_flags      = '-Rbd'
                             , resolve_flags    = '-at'
                             , on_integ_failure = Decided.FALLBACK
                             , integ_fallback   = 'add'
                             , p4_request       = None
                             , integ_input      = decision_input
                             , branch_delete    = True
                             )
                        # To simplify later code, copy integration source
                        # from P4JITFP into GHOST column so that
                        # later code can use GHOST as integ source.
            self._lazy_create_ghost_cell_discovered()
            _copy_discovered(self.ghost_cell, self.p4jitfp_cell)

        elif  decision_row.output in [ p4gf_g2p_matrix2_ghost.EDIT
                                     , p4gf_g2p_matrix2_ghost.DELETE ]:
            decided = Decided( p4_request  = decision_row.output
                             , integ_input = decision_input )

        elif  decision_row.output ==   p4gf_g2p_matrix2_ghost.NOP:
            decided = None

        elif  decision_row.output ==   p4gf_g2p_matrix2_ghost.IMPOSSIBLE:
            LOG.error('IMPOSSIBLE ghost. Check your inputs. {}'.format(self.row.gwt_path))
            p4gf_g2p_matrix2_ghost.to_input(
                      p4jitfp_cell   = self.p4jitfp_cell
                    , gdest_cell     = self.gdest_cell
                    , gparn_cell     = gparn_cell
                    , gparfpn_cell   = gparfpn_cell
                    , ghost_cell     = self.ghost_cell
                    , gdest_column   = self.m._gdest_column
                    , gparn_column   = orig_gparn_col )

            raise RuntimeError(
                    _('ghost decision input {input} is impossible. gwt_path={gwt_path}')
                    .format( input    = p4gf_g2p_matrix2_ghost.deb(
                                              decision_input
                                            , fmt=p4gf_g2p_matrix2_ghost.LONG)
                           , gwt_path = self.row.gwt_path))
        else:
            raise RuntimeError(_('ghost decision input {input} produced unknown'
                                 ' output {output}. gwt_path={gwt_path}')
                               .format(
                                      input    = p4gf_g2p_matrix2_ghost.deb(decision_input)
                                    , output   = decision_row.output
                                    , gwt_path = self.row.gwt_path))

        return decided

    def _ghost_decide_from_git_lfs_initial_track(self, gparn_cell):
        """If this is a top-level .gitattributes row that exists in Git
        but not in Perforce, it's probably one that git-lfs-initial-track
        inserted into Git history but not Perforce. Time to ghost it into
        Perforce history, too.

        Returns a Decided to 'p4 add' it into Perforce history if so, or
        None if not.
        """
        if (    self.m.ctx.is_lfs_enabled
            and self.row.gwt_path == p4gf_const.GITATTRIBUTES
            and gparn_cell
            and "sha1" in gparn_cell.discovered
            and not "depotFile" in gparn_cell.discovered ):
            return Decided( p4_request  = 'add'
                          , integ_input = 'lfs_init'
                          )
        return None

    def _lazy_create_ghost_cell_discovered(self):
        """Need to record a decision on a file that didn't fill in the GHOST
        cell or cell.discovered during discovery?
        Can't get by with None any longer.

        Happens with deleted files sometimes (deleted in lw parent
        of this lw child that we're populating).
        """
        if not self.ghost_cell:
            self.row.discovered(self.m.ghost_column.index)
            self.ghost_cell = self.row.cell(self.m.ghost_column.index)

    def _iter_integ_src_tuples(self):
        """Iterate over cells that have integration sources.

        Produce a 2-tuple of (depot_path, column).

        Helper for _remove_duplicate_integ().
        """
        IntegSrcTuple = namedtuple('IntegSrcTuple', ['depot_path', 'column']) # pylint:disable=invalid-name
                                            # Invalid name "RequestFiletype" for type variable
                                            # It's a type name, not a variable.
                                            # Pylint does not understand namedtuple.
        for cell, column in zip(self.row.cells, self.m.columns):
            if column.col_type == Column.GHOST:
                continue
            if not (    cell
                    and cell.decided
                    and cell.decided.has_integ() ):
                continue
            yield IntegSrcTuple( depot_path = cell.discovered['depotFile']
                               , column     = column)

    def _remove_duplicate_integ(self):
        """Integrating (including JIT-branching) from multiple revisions of the
        same source depot path? Could happen if the same fully populated
        branch appears as the basis for multiple GPARN or this branch, but at
        different changelists. In such case, defer to the highest revision
        number. Keep our revision graph simple.
        """
        if not self.has_integ:
            return

        winning_column = {}     # depot_path to Column
        doomed = []

        for ist in self._iter_integ_src_tuples():
            old_winning_column = winning_column.get(ist.depot_path)
            if old_winning_column:
                if old_winning_column.change_num < ist.column.change_num:
                    # This column is newer. Beats the old winner.
                    doomed.append(old_winning_column)
                    winning_column[ist.depot_path] = ist.column
                else:
                    # This column is no newer than the current winner.
                    doomed.append(ist.column)
            else:
                # First winner for this source path.
                winning_column[ist.depot_path] = ist.column

        # Revoke the integ action for all the unwinners.
        for column in doomed:
            self.row.cells[column.index].decided = None

    def _populate_from_cell(self):
        """If populating a new branch, and our population source (or its fully
        populated basis, if destination is also fully populated) has a file for
        us, return the GPARN (or GPARFP) cell that holds our source.
        """
        col = self.m._populate_from_column
        if not col:
            return None
        assert col.col_type == Column.GPARN

        # Population source has a file for us.
        cell = self.row.cells[col.index]
        if cell:
            return cell

        # If we're lightweight, then it doesn't matter if population source has
        # a basis or not. Lightweight branches populate solely from immediate
        # parents, or nothing at all.
        if self.m.current_branch.is_lightweight:
            return None

        # If population source lacks a fully populated basis, then we've
        # no more to do.
        if not col.fp_counterpart:
            return None

        # We're populating a fully populated branch, so yeah, use any basis
        # for population source.
        cell = self.row.cells[col.fp_counterpart.index]
        return cell

    def _decide_populate_from(self, exists_in_git):
        """If discovery marked a column for "populate this branch from this
        column", do so.

        Infrequent. Occurs only on first changelist on a new Perforce branch.
        """
        if not self._want_populate_from(exists_in_git):
            return

        # Yep, we can indeed populate this row's GWT from the
        # populate_from column.
        common.debug3( '_decide_populate_from() col={col} integ -Rbd resolve -at'
               , col=self.m._populate_from_column.index )
        self.populate_from_cell.decided \
                                    = Decided( integ_flags      = '-Rbd'
                                             , resolve_flags    = '-at'
                                             , on_integ_failure = Decided.RAISE
                                             , integ_input      = 'populate_from')
        self.has_integ = True

    def _want_populate_from(self, exists_in_git):
        """If we should copy this row's file as part of a
        'populate first changelist on a new branch' changelist,
        return a Decided
        """
        # Choose not to populate files that do not exist in Git
        # destination: we don't want them.
        if not exists_in_git:
            return False

        self.populate_from_cell = self._populate_from_cell()

        # Cannot populate from a file that doesn't exist at all in P4.
        if not self.populate_from_cell:
            return False

        # Cannot populate from a file that is deleted at
        # head revision in Perforce.
        if not self._p4cell_exists(self.populate_from_cell):
            return False

        # Must never create a file "below" a symlink. Do not allow symlinks
        # to masquerade as directories.
        if self.m.symlink_in_path(self.row.gwt_path):
            return False

        # Don't populate lightweight branches from fully populated parents.
        # Only from lightweight parents.
        if (        self.m.current_branch.is_lightweight
            and     self.m._populate_from_column.branch
            and not self.m._populate_from_column.branch.is_lightweight ):
            return False

        # Yep, we can indeed populate this row's GWT from the
        # populate_from column.
        return True

    def _decide_integ_from_column(self, column):
        """If this column has an integration source that we _discover_branches()
        decide should be used, decide how.

        If column is GPARN with nothing to integrate, but backed by a GPARFPN,
        schedule GPARFP as next work_queue item so that we can check to see
        if it holds something to integrate.

        This code runs before _decide_jit_if(). Prefer p4 'branch' file
        actions from actual Git parent branches over JIT branch file actions.
        """
        common.debug3('Row._decide_integ_from_column() {col}', col=column.index)
        # Skip any already-integrated population source
        if column is self.m._populate_from_column:
            common.debug3( 'Row._decide_integ_from_column() {col} == pop_fm. Skipping.'
                   , col=column.index )
            return
        # Or from our destination branch.
        if column.branch == self.m.current_branch:
            common.debug3( 'Row._decide_integ_from_column() {col} == curr. Skipping.'
                   , col=column.index )
            return

        # Nothing to integ?
        src_cell = self.row.cells[column.index]
        if not src_cell or not src_cell.discovered:
            # Even if nothing to integ from GPARN, check its GPARFPN
            # basis for something to integ. If so, prepend to work queue
            # to check for something to integ.
            if (    column.col_type == Column.GPARN
                and column.fp_counterpart
                and self.row.cells[column.fp_counterpart.index]):
                self.integ_work_queue.appendleft(column.fp_counterpart)
            common.debug3( 'Row._decide_integ_from_column() {col} no src disc.'
                     ' Skipping.'
                    , col=column.index )
            return

        # Skip integ sources that do not exist at all.
        if 'depotFile' not in src_cell.discovered:
            common.debug3('Row._decide_integ_from_column() {col} no src depotFile.'
                          ' Skipping.'
                         , col=column.index )
            return

        # Don't integ duplicate branch or delete actions.
        ### Zig warns: now that we 'p4 fstat' instead of 'p4 integ', a 'branch'
        ### action here means "head action on source depot file is 'branch'"
        ### not "want to branch file from source to dest. This use of
        ### _p4_branch_or_delete() no longer defends against multiple branch
        ### actions in a single dest file, single changelist. This appears to
        ### be okay, because we've never seen multiple branch actions cause a
        ### problem. It's only multiple deletes that fail.
        cur_is = _p4_branch_or_delete(src_cell.discovered)
        if self.is_integ_branch_or_delete and cur_is:
            common.debug3( 'Row._decide_integ_from_column() {col}'
                     ' double-delete/branch. Skipping.'
                   , col=column.index )
            return
        # Or integs that treat a symlink file as a directory.
        if self.m.symlink_in_path(self.row.gwt_path):
            common.debug3( 'Row._decide_integ_from_column() {col} symlink ancestor.'
                     ' Skipping.'
                    , col=column.index)
            return
        # Or source file revisions at or before the destination's
        # fully populated basis
        # common.debug3('### Row._decide_integ_from_column() {col}'
        #         ' checking FP basis...', col=column.index)
        if not self._after_dest_fp(src_cell):
            common.debug3( 'Row._decide_integ_from_column() {col} Not after'
                     ' JIT basis. Skipping.'
                    , col=column.index )
            return

        # Look up the correct integ, resolve, and fallback action
        # to take for this file. Sometimes is "do nothing" and that's okay.
        # common.debug3('### Row._decide_integ_from_column() {col}'
        #         ' checking integ matrix...', col=column.index)
        ri = p4gf_g2p_matrix_integ.to_input( row             = self.row
                                           , integ_src_cell  = src_cell
                                           , integ_dest_cell = self.integ_dest_cell
                                           , git_delta_cell  = self.git_delta_cell
                                           , gdest_cell      = self.gdest_cell )
        r = p4gf_g2p_matrix_integ.find_row(ri)
        if (   not r
            or not ((r.integ_flags is not None) or r.fallback) ):
            common.debug3('Row._decide_integ_from_column() {col} {ri} matrix returned'
                    ' no action. Skipping.'
                   , col=column.index
                   , ri = p4gf_g2p_matrix_integ.deb(ri)
                   )
            return

        # I don't _think_ there's a way to get here on a column we already
        # decided (such as self._poulate_from_column). assert() to be sure.
        assert not (src_cell.decided and src_cell.decided.has_p4_action())

        src_cell.decided = Decided.from_integ_matrix_row(r, ri)

        common.debug3( 'Row._decide_integ_from_column() col={col} decided={decided}'
               , col     = column.index
               , decided = src_cell.decided )

        if r.integ_flags is not None:
            self.has_integ = True
            self.is_integ_branch_or_delete |= cur_is

    def _after_dest_fp(self, src_cell):
        """Does src_cell integrate from a Perforce file revision at or after our
        destination's fully populated basis?

        Always True if destination has no basis.
        """
        assert src_cell
        # Even if lightweight, if we have no basis, then all revisions
        # are permitted as integration sources.
        if not self.p4jitfp_cell:
            common.debug3('_after_dest_fp() True no FP basis. ')
            return True

        # Integrating from some Perforce path other than our basis?
        # Go right ahead.
        fp  = self.p4jitfp_cell.discovered    # for less typing
        src = src_cell       .discovered
        fp_from_file  = _from_depot_file(fp)
        src_from_file = _from_depot_file(src)
        if (   (not fp_from_file)
            or (not src_from_file)
            or fp_from_file != src_from_file):
            common.debug3('_after_dest_fp() True src={src} not from'
                    ' JIT FP basis={fp}. '
                    .format(src=src_from_file
                           ,fp =fp_from_file))
            return True

        # Integrating a later revision of our basis?
        fp_rev  = self.p4jitfp_cell.rev()
        src_rev = src_cell.rev()
        if ( not fp_rev
             or not src_rev
             or int(fp_rev) < int(src_rev)):
            common.debug3('_after_dest_fp() True src={src} > '
                    ' JIT FP basis={fp}. '
                    .format( src=src_rev
                           , fp =fp_rev))
            return True

        # Sorry, you're trying to integrate from a source revision that is at
        # or before lightweight GDEST's fully populated basis P4JITFP's revision
        # for this file. Can't do that: that would make our lightweight branch
        # unnecessarily heavyweight.
        common.debug3('_after_dest_fp() False src={src_d}#{src_r} <= '
                ' JIT FP basis={fp_d}#{fp_r}. '
                .format( src_d=src.get('depotFile')
                       , src_r=src_rev
                       , fp_d =fp .get('depotFile')
                       , fp_r =fp_rev
                       ))
        return False

    def _decide_jit_if(self):
        """Branch file from fully populated basis if necessary and possible.

        This code runs after any _decide_integ_from_column() and
        _apply_git_delta(). Prefer p4 'branch' file actions from actual Git
        parent branches over JIT branch file actions.

        Store the JIT action we WOULD run if we later decide JIT is required.
        """
        # Not worth attempting to JIT-branch a file that we've already decided
        # to integrate from at least one other branch. We've got files and
        # content coming from other integ source branches, and that's good
        # enough to hold the incoming commit's results.
        if self.has_integ:
            common.debug3('_decide_jit_if() no: has_integ')
            return

        # If we've got no Git action to apply, there's no reason to
        # JIT-branch this file.
        if not (    self.git_delta_cell
                and self.git_delta_cell.decided
                and self.git_delta_cell.decided.has_p4_action() ):
            common.debug3('_decide_jit_if() no: no git delta from prev commit')
            return

        # No need to JIT-branch a file that already exists in destination.
        if common.gdest_cell_exists_in_p4(self.gdest_cell):
            common.debug3('_decide_jit_if() no: already exists in p4 destination')
            return

        # Cannot JIT-branch a file that has no source.
        if not self._p4jitfp_exists():
            common.debug3('_decide_jit_if() no: does not exist in JIT FP basis')
            return

        if self.m.symlink_in_path(self.row.gwt_path):
            common.debug3('_decide_jit_if() no: symlink in path')
            return

        # Look up the correct integ, resolve, and fallback action
        # to take for this file. Sometimes is "do nothing" and that's okay.
        ri = p4gf_g2p_matrix_integ.to_input( row             = self.row
                                           , integ_src_cell  = self.p4jitfp_cell
                                           , integ_dest_cell = self.integ_dest_cell
                                           , git_delta_cell  = self.git_delta_cell
                                           , gdest_cell      = self.gdest_cell )
        r = p4gf_g2p_matrix_integ.find_row(ri)
        if (   not r
            or not ((r.integ_flags is not None) or r.fallback) ):
            common.debug3('_decide_jit_if() no: integ matrix returned'
                          ' no integ or fallback')
            return

        self.p4jitfp_cell.decided = Decided.from_integ_matrix_row(r, ri)
        common.debug3('_decide_jit_if() JIT: {}'.format(self.p4jitfp_cell.decided))
        if r.integ_flags is not None:
            self.has_integ = True

    def _gdest_exists_in_git(self):
        """Does GWT appear in git-ls-tree for GDEST's commit?"""
        return (    self.gdest_cell
                and self.gdest_cell.discovered
                and self.row.mode)

    @staticmethod
    def _p4cell_exists(cell):
        """Is there a Perforce file revision for the given cell?

        False if not, or if file deleted at cell's changelist number.
        """
        return (    cell
                and cell.discovered
                and 'depotFile'  in cell.discovered
                and cell.action()
                and 'delete' not in cell.action())

    def _p4jitfp_exists(self):
        """Is there a Perforce file revision for our fully populated basis
        that we can now integrate into our current branch?

        False if not, or if basis is deleted at our basis changelist number.
        """
        return self._p4cell_exists(self.p4jitfp_cell)

    def _apply_git_delta(self):
        """If git-fast-export or git-diff-tree says to Add/Modify/Delete
        this GWT path, then do so.

        Internally only applies to row if we've no integ action. The big
        integ decision table already factors in Git actions when integrating.
        """
        common.apply_git_delta_cell( gdest_cell     = self.gdest_cell
                             , git_delta_cell = self.git_delta_cell )

    def _raise_if_symlink_in_gdest_path(self):
        """Git normally prohibits any files that have a symlink as an ancestor.
        But clever users or old histories might produce such a thing.

        Git Fusion NEVER permits a file with a symlink in the path. That leaves
        the Git Fusion server vulnerable to writing a file thorugh a
        dereferenced symlink, and that symlink could point anywhere, such as
        "/home/git/.ssh" or "/home/git/.bashrc".
        """
        if not self._gdest_exists_in_git():
            return
        symlink = self.m.symlink_in_path(self.row.gwt_path)
        if not symlink:
            return

        msg = (_("Git commit {sha1} prohibited."
               " File '{gdest_path}' cannot co-exist with symlink '{symlink}'.")
               .format( gdest_path = self.row.gwt_path
                      , symlink    = symlink
                      , sha1       = self.row.sha1 ))
        raise RuntimeError(msg)

    def _decide_gdest_have_delta(self):
        """Last chance before JIT-branch to force GDEST into correctness.
        If GDEST's current depot branch head differs from what GDEST's
        incoming Git commit wants, force a p4_request to counteract the delta.

        Overrides and overwrites any previously calculated Decided.p4_request.

        Will also REMOVE an action if we already match what GDEST wants.
        Avoid a few unnecessary Perforce file actions.

        Must fire after integ and git-fast-export decision,
        before JIT decision.
        """
                        # Don't delta against the empty void of
                        # unpopulated branches. That induces JIT-branch of
                        # every file in our branch.
        if not self.m._current_branch_head_change_num():
            return

        action = self._calc_gdest_have_delta()
        if action:
            if action == 'D' and not _gdest_cell_exists_in_p4(self.gdest_cell):
                return
            decided = self.row.decided(self.m._gdest_column.index)
            if decided.p4_request == 'move/delete' and action == 'D':
                return
            decided.add_git_action(git_action=action)

    def _calc_gdest_have_delta(self):
        """How does what we HAVE in GDEST's current head changelist differ
        from what we WANT in the incoming Git commit?

        Return a single-letter counteraction command 'A', 'M', 'T', 'D', 'N'.

        Return None if no cell.discovered.
        """
        if not (self.gdest_cell and self.gdest_cell.discovered):
            return None

        d = self.gdest_cell.discovered  # for less typing
        have_sha1 = d.get('have-sha1')
        have_mode = d.get('have-git-mode')
        want_sha1 = d.get('sha1')
        want_mode = d.get('git-mode')

        if (not (have_sha1 and have_mode)) and (    (want_sha1 and want_mode)):
            return 'A'
        if (    (have_sha1 and have_mode)) and (not (want_sha1 and want_mode)):
            return 'D'
        if have_sha1 != want_sha1:
            return 'M'
        if have_mode != want_mode:
            return 'T'
        return 'N'

# -- module functions ---------------------------------------------------------

def _lfs_oid(cell):
    """If discovery recorded a Git LFS oid for this cell, return it."""
    if cell and cell.discovered:
        return cell.discovered.get(common.KEY_LFS)
    return None


def _p4_branch_or_delete(integ_dict):
    """Does the given 'p4 integ' (or 'p4 copy') result dict show
    that the file would be opened for branch or delete?

    Perforce prohibits multiple branch or delete actions on the same
    file in the same changelists.
    """
    return (    isinstance(integ_dict, dict)
            and 'depotFile' in integ_dict
            and 'action'    in integ_dict
            and integ_dict['action'] in ['branch', 'delete'])


def _copy_discovered(ghost_cell, src_cell1, src_cell2=None):
    """To simplify other code, allow GHOST column to act as integ source.

    Copy integ source data from first of src_cell1, src_cell2 to have a
    discovered 'depotFile' we can use as an integ source.
    """
    if not (ghost_cell and ghost_cell.discovered is not None):
        return

    if (    src_cell1
        and src_cell1.discovered
        and src_cell1.discovered.get('depotFile') ):
        src = src_cell1.discovered
    elif (   src_cell2
             and src_cell2.discovered
             and src_cell2.discovered.get('depotFile') ):
        src = src_cell2.discovered
    else:
        return

    dst = ghost_cell.discovered     # for less typing
    for key, val in src.items():
        if key not in dst:
            dst[key] = val


def _from_depot_file(discovered):
    """Return the first of fromFile, depotFile."""
    for k in ['fromFile', 'depotFile']:
        v = discovered.get(k)
        if v:
            return v
    return None


def _ghost_to_orig_gparn_col(ghost_col, column_list):
    """What is the ghost trying to reproduce? Return the GPARN of which
    we're creating a GHOST.
    """
    if not ghost_col:
        return None
    for col in column_list:
        if (    col.col_type   == Column.GPARN
            and col.branch     == ghost_col.branch
            and col.change_num == ghost_col.change_num):
            return col
    return None


def _gdest_cell_exists_in_p4(gdest_cell):
    """Does this cell.discovered contain a depotFile?"""
    return (    gdest_cell
            and gdest_cell.discovered
            and 'depotFile' in gdest_cell.discovered)

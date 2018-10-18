#! /usr/bin/env python3.3
# -*- coding: utf-8 -*-
"""Ghost changelist decision matrix.

Rearrange a Perforce depot branch so that it looks just like what Git expects
before we copy a Git commit. Put all branch management work in ghost
changelists so that we do not commingle Git actions with branch management
actions in the same Perforce changelist.

* populate a new depot branch (lightweight or fully populated)
  branch files from parent

* just-in-time branch for edit or delete
  branch files from fully populated basis

* rearrange to reuse existing depot branch (lightweight or fully populated)
  branch/add/edit/delete existing files in depot branch

= Rearrange =
To rearrange an existing Perforce depot branch to look exactly like the
impending Git commit's first parent:

* GDEST.p4: Existing depot branch currently holds some old Git commit.
  From a Perforce viewpoint, the previous changelist on a depot branch is the
  "implied" parent of the next changelist on this depot branch.
  We're rearranging the world to sever this implied link.

* GDEST.git: The impending Git commit that we want to copy to Perforce, but
  cannot until we're done rearranging Perforce.

* GPARN: A Git parent of GDEST.git. This is the commit which we
  want to rearrange Perforce so that it looks like this GPARN. So we
  create a GHOST of GPARN in the destination Perforce branch.
  First of [current branch head if that's a GPARN, Git-first-parent if not]

* P4JITFP: The fully populated basis of the destination Perforce depot branch.
  Git Fusion assigns this column only if the destination is a lightweight
  branch.

* GPARFPN: The fully populated basis of GPARN's Perforce depot branch,
  Git Fusion assigned this column only if GPARN is on a lightweight branch.

  There is no guarantee that P4JITFP and GPARFPN are the same. They are likely
  to be on two unrelated depot branches. Even when on the same depot branch,
  they might be from different points in time: different Perforce changelist
  numbers. They are guaranteed to match only when creating a new lightweight
  child branch from a lightweight parent branch.

There's no requirement for any Git relation between GPARN and GDEST.p4: Git
Fusion's reuse of a Perforce depot branch to house multiple Git history
branches is none of Git's concern. So the Git history can look like this:

       GPARFPN ------...--- GPARN --- GDEST.git

P4JITFP --...-- GDEST.p4


This turns into (up to) 6 Perforce changelists spread across (up to) 4 Perforce
depot branches:

             .------ ... -- GPARN               //x/branch_1: GPARN's
            /
       GPARFPN                                  //x/branch_2: GPARN's fully
                                                populated basis if GPARN's
                                                branch is lightweight.

     .-- ... -- GDEST.p4  . GHOST --- GDEST.git //x/branch_3: GDEST's branch,
    /                       of                  where the impending Git commit
   /                        GPARN               will be copied.
  /
P4JITFP                                         //x/branch_4: GDEST's fully
                                                populated basis, if GDEST's
                                                branch is lightweight

To figure out which Perforce file actions rearrange GDEST.p4 to look like
GHOST, ask 'p4 fstat' for file existence/size/MD5 and then compare values.

//-----------------------------------------------------------------------------
Rearrange

Let GHOST := GPARFPN + GPARN to simplify the decision matrix.


    (P4JITFP + GDEST.p4) ----∆- GHOST               How does what we HAVE differ from what
                                                    we WANT?
                                                    We'll return counteracting actions.
                                                    compare p4 fstat values

P4JITFP ---------------------∆- GHOST               How does what we WANT differ from our
                                                    FP basis?
                                                    Avoid unnecessary branch actions in
                                                    LW GDEST.
                                                    compare p4 fstat values

                                GHOST -∆- GDEST.git git-fast-export "git-action"
                                                    What is the impending Git commit about
                                                    to do?
                                                    Detect impending delete actions that
                                                    require a preceding branch action.

//-----------------------------------------------------------------------------
Branch for delete

GPAR(fp)N--∆--GDEST.git git action (git-fast-export) D

P4JITFP  E
GDEST.p4 E (NE)

//-----------------------------------------------------------------------------
Populate first change on new branch

P4JITFP column might exist, or might not exist at all (GDEST could be FP!)
It's all about GPAR[FP]N.

FP POP : GPAR[FP] E ==> branch it

LW POP from FP GPARN: NOP.

LP POP from LW GPARN + GPARFPN:
    P4JITFP := GPARFPN
    So anything in GPARN that differs from GPARFPN also differs from P4JITFP,

    so E in GPARN ==> branch it.

         GPARN
LW POP : GPARN[FP] != P4JITFP ==> branch it         AHA! Do NOT use "GPARN E" here!
                                                    That's an implicit mis-use of
                                                    GPARN's FP basis as GDEST's P4JITFP.
                                                    There's one of your big bugs.

                                                    But if POP LW, won't
                                                    P4JITFP always == GPARFPN
                                                    if GPARN is LW?
                                                    And == GPARN if GPARN is FP?

//-----------------------------------------------------------------------------
Honor LW branch rules: branch no file if:

P4JITFP  E
GDEST.p4 NE (never existed)
P4JITFP --∆-- GPAR(fp)N shows no difference


//-----------------------------------------------------------------------------
NEW columns


-- ∆1 Rearrange delta: P4JITFP+GDEST.p4 ∆ GHOST
-- ∆2 LW branch rules: P4JITFP          ∆ GHOST
-- ∆3 git-fast-export:                    GHOST ∆ GDEST.git

File existence
-- P4JITFP
-- GDEST.p4
-- GPARFPN
-- GPARN

Mode flags
-- GDEST is LW?
-- POP first changelist on a new depot branch?

= Warning: git-fast-export never 'A'dds =
A warning about git-fast-export "git-action": git-fast-export never reports
'A' for add. It reports 'M' for either "add" or "modify". We have to check for
file existence in Git first-parent to differentiate. I'm not adding Yet Another
Column here to handle that case. Easier to sanitize that input before we get to
this table.
"""
from   collections  import namedtuple

from   p4gf_l10n               import NTR

                        # pylint:disable=invalid-name
                        # Invalid function name _G, _UN_G.
                        # Yeah, they're short to keep the table width down.

# -- Inputs ---------------------------------------------------------------

                        # Set in all these values so debug code can
                        # differentiate between Ghost and non-Ghost input
                        # integers.
                        #
GHOST_BIT      = 0b010000000000000000000000000000000000
GHOST_MASK     = 0b001111111111111111111111111111111111

                        # Does this file exist in the given Perforce
                        # depot branch?
_E             = 0b010000000000000000000000000000000001  # Exists at rev.
NE             = 0b010000000000000000000000000000000010  # Never existed.
DL             = 0b010000000000000000000000000000000100  # Existed, but deleted at rev.
__             = 0b010000000000000000000000000000000110  # NE | DL : doesn't exist
XX             = 0b010000000000000000000000000000000111  # don't care
                        # Above bits shifted into 4 columns.
GDEST_P4_XX    = 0b010000000000000000000000000000000111
P4JITFP_P4_XX  = 0b010000000000000000000000000000111000

                        # Differences between columns.
                        # For GPARN-->GDEST, this is the result of
                        # 'git-fast-export'. Stored in GDEST.git.
                        #
                        # For Perforce differences, this is calculated
                        # in to_input().
                        #
A              = 0b010000000000000000000001000000000000
M              = 0b010000000000000000000010000000000000
T              = 0b010000000000000000000100000000000000
D              = 0b010000000000000000001000000000000000
N              = 0b010000000000000000010000000000000000
X              = 0b010000000000000000011111000000000000
                        # Above bits shifted into 3 diffs
                        #
                        # have --> want  GDEST(+fp).p4 --> GPARN(+fp).p4 diffs
H_W_X          = 0b010000000000000000011111000000000000
                        # fp --> want P4JITFP.p4 --> GPARN(+fp).p4 diffs
FP_W_X         = 0b010000000000001111100000000000000000
                        # git-fast-export GPARN.git --> GDEST.git actions
GFE_X          = 0b010000000111110000000000000000000000

                        # Is the destination Perforce depot branch lightweight?
                        # Fully populated?
                        #
LW             = 0b010000001000000000000000000000000000
FP             = 0b010000010000000000000000000000000000
LW_X           = 0b010000011000000000000000000000000000

                        # Must we populate a new lightweight depot branch?
POP            = 0b010000100000000000000000000000000000
P__            = 0b010001000000000000000000000000000000
P_X            = 0b010001100000000000000000000000000000

                        # Is this row a copy/rename source
CRS_S          = 0b010010000000000000000000000000000000
CRS__          = 0b010100000000000000000000000000000000
CRS_X          = 0b010110000000000000000000000000000000

                        # Shifting things into columns
_SHIFT_E_GDEST   =  0
_SHIFT_E_P4JITFP =  3

_SHIFT_DIFF_H_W  =  0
_SHIFT_DIFF_FP_W =  5
_SHIFT_DIFF_GFE  = 10


# -- bit shifters ------------------------------------------------------------
def _shift(bits, shift_by):
    """Shift bits from right-aligned into their bit-column position."""
    return ((bits & GHOST_MASK) << shift_by) | GHOST_BIT


def _unshift(bits, shift_by):
    """Shift bits from bit-column position to right-aligned."""
    return ((bits & GHOST_MASK) >> shift_by) | GHOST_BIT


def _GDEST_P4(exists):
    """Shift "exists in Perforce depot branch" bits into GDEST_P4 bit-column."""
    return _shift(exists, _SHIFT_E_GDEST)


def _P4JITFP_P4(exists):
    """Shift "exists in Perforce depot branch" bits into P4JITFP_P4 bit-column."""
    return _shift(exists, _SHIFT_E_P4JITFP)


def _GFE(diff):
    """Shift diff bits into git-fast-export "GPARN --> GDEST" bit-column."""
    return _shift(diff, _SHIFT_DIFF_GFE)


def _H_W(diff):
    """Shift diff bits into "have (GDEST.p4+fp) --> want (GPARN.p4+fp)" bit-column."""
    return _shift(diff, _SHIFT_DIFF_H_W)


def _FP_W(diff):
    """Shift diff bits into "fp basis (P4JITFP.p4) --> want (GPARN.p4 + fp) bit-column."""
    return _shift(diff, _SHIFT_DIFF_FP_W)


# -- Outputs --------------------------------------------------------------

                        # What to do?
                        #
                        # SURPRISE: BRANCH here is "branch from GPAR(fp)N",
                        # never "branch from P4JITFP". As of Matrix2, we no
                        # longer "branch from P4JITFP". Ever. We branch from
                        # the GPAR(fp)N that we're GHOSTing.
                        # Sometimes GPARFPN == P4JITFP, so in those cases,
                        # yeah, we "branch from P4JITFP" but by coincidence,
                        # not intent.
                        #
                        # BRANCH_DELETE produces TWO ghost changelists: one to ADD
                        # the file, then a second one to DELETE it. This forces
                        # a lightweight branch to have a record of the file
                        # being deleted.
                        #
BRANCH     = NTR('branch')
EDIT       = NTR('edit')
DELETE     = NTR('delete')
IMPOSSIBLE = NTR('impossible')
                        # Branch from P4JITFP, then delete it.
                        # Requires _P4JITFP_P4(_E).
BRANCH_DELETE = NTR('bra+del')
NOP        = None


R = namedtuple('R', ['input', 'output', 'comment']) # pylint:disable=invalid-name
                                                    # Invalid class name "R"
                                                    # Intentionally short name here.
                                                    # Type does not matter. Only content matters.


TABLE = [
# pylint:disable=line-too-long

# Testing for file existence in     | Differences between            | Git delta from  | Mode flags
# (up to) 2 different Perforce      | Perforce branches.             | git-fast-export |
#  branches. 'p4 fstat' action      | 'p4 fstat' headType,           |                 | Lightweight
#                                   | fileSize, digest               |                 | GDEST.p4
#                                   |                                |                 | .
#   destination   | destination     | HAVE in       | FP basis in    | GPARN + GPARFPN | .    | Populating first
#   branch        | branch's        | GDEST.p4 +    | P4JITFP        | vs. GDEST.git   | .    | changelist in a new
#   GDEST.p4      | fully populated | P4JITFP       | vs.            |                 | .    | GDEST.p4 branch?
#                 | basis P4JITFP   | vs.           | WANT in        |                 | .    | .
#                 |                 | WANT in GPARN | GPARN +        |                 | .    | .  ==> Resulting P4
#                 |                 | + GPARFPN     | GPARFPN        |                 | .    | .  ==> Action    "comment"
#

# Branch for impending git-fast-export action.
# Don't branch for NOP if lightweight.
#
# Re-branch for NOP if was deleted in LW branch, otherwise we'll erroneously
# look like that file really _should_ be deleted
#
  R(_GDEST_P4(__) | _P4JITFP_P4(XX) | _H_W(A|M  |N) | _FP_W(A|M|D|N) | _GFE(  M|T|D  ) | CRS_X | LW_X | P_X, BRANCH, "branch-for-MTD replace inherited with GPARN")
, R(_GDEST_P4(DL) | _P4JITFP_P4(XX) | _H_W(A      ) | _FP_W(A|M|D|N) | _GFE(        N) | CRS_X | LW_X | P_X, BRANCH, "branch-for-NOP re-add to undo previous D")
, R(_GDEST_P4(__) | _P4JITFP_P4(XX) | _H_W(A|M    ) | _FP_W(A|M|D|N) | _GFE(  M|T|D|N) | CRS_X | LW_X | P_X, BRANCH, "branch to replace inherited with GPARN")
, R(_GDEST_P4(NE) | _P4JITFP_P4(NE) | _H_W(A      ) | _FP_W(A      ) | _GFE(        N) | CRS_X | LW   | P__, BRANCH, "branch-for-NOP from GPARN in lw because differs (A) from P4JITFP")

# Branch to populate or rearrange fully populated GDEST.
#
, R(_GDEST_P4(NE) | _P4JITFP_P4(XX) | _H_W(A      ) | _FP_W(A|M|D|N) | _GFE(A|M|T|D|N) | CRS_X | FP   | POP, BRANCH, "branch to populate new FP GDEST")
, R(_GDEST_P4(NE) | _P4JITFP_P4(XX) | _H_W(A      ) | _FP_W(A|M|D|N) | _GFE(A|M|T|D|N) | CRS_X | FP   | P__, BRANCH, "branch to rearrange FP GDEST")

# Rearrange: edit existing file content/type to match GPAR(fp)N. No need to branch:
# already exists in destination depot branch. Edit for delete is kind of
# pointless, but  might as well, just for completeness.
#
, R(_GDEST_P4(_E) | _P4JITFP_P4(XX) | _H_W(  M    ) | _FP_W(A|M|D|N) | _GFE(A|M|T|D|N) | CRS_X | LW_X | P_X, EDIT  , "edit to match GPARN")

# Rearrange: delete existing P4IMPLY/A2 file to match GPARN/M3.
#
, R(_GDEST_P4(_E) | _P4JITFP_P4(XX) | _H_W(    D  ) | _FP_W(A|M|D|N) | _GFE(A|M|T|D|N) | CRS_X | LW_X | P_X, DELETE, "D to match GPARN's no file")
, R(_GDEST_P4(NE) | _P4JITFP_P4(_E) | _H_W(    D  ) | _FP_W(A|M|D|N) | _GFE(A|M|T|D|N) | CRS_X | LW_X | P_X, BRANCH_DELETE, "BRANCH+DELETE from P4JITFP to update LW with deleted since FP basis")
, R(_GDEST_P4(NE) | _P4JITFP_P4(_E) | _H_W(A|M|D|N) | _FP_W(A|M|D|N) | _GFE(A|M|T|D|N) | CRS_S | LW_X | P_X, BRANCH, "branch for git copy/rename source")

, R(_GDEST_P4(__) | _P4JITFP_P4(_E) | _H_W(A|M|D|N) | _FP_W(A|M|D|N) | _GFE(  M|T|D  ) | CRS_X | LW   | P_X, BRANCH, "branch-for-MTD E in GPARFPN")

# NOP catchall.
#
# Not going to allow scan to fall off bottom of list and return NOP: that
# splits my attention between this table and find_row().
# Not going to add rows for every possible case: that clutters the table and
# slows each table scan.
#
, R(_GDEST_P4(XX) | _P4JITFP_P4(XX) | _H_W(A|M|D|N) | _FP_W(A|M|D|N) | _GFE(A|M|T|D|N) | CRS_X | LW_X | P_X, NOP, "NOP bottom row")
]


def to_input( p4jitfp_cell
            , gdest_cell
            , gparn_cell
            , gparfpn_cell
            , ghost_cell
            , gdest_column
            , gparn_column ):
    """Return an integer that encodes a single file's input to the
    above decision matrix.
    """
    # pylint:disable=too-many-arguments

                        # Simplify lightweight branches: which cell.discovered
                        # holds the currently applicable 'p4 fstat' info?
    have = _lw_or_fp(lw=gdest_cell, fp=p4jitfp_cell)
    want = _lw_or_fp(lw=gparn_cell, fp=gparfpn_cell)

    row_input = ( 0
                        # p4 file existence
                | _GDEST_P4  (_exists_how(gdest_cell  ))
                | _P4JITFP_P4(_exists_how(p4jitfp_cell))
                        # differences
                | _H_W (_p4diff(have,         want))
                | _FP_W(_p4diff(p4jitfp_cell, want))
                | _GFE (_gfediff(gdest_cell, ghost_cell))
                        # mode flags
                | _crs(gdest_cell, ghost_cell)
                | _lw(gdest_column.branch.is_lightweight)
                | _pop(gdest_column, gparn_column)
                )

    return row_input


def find_row(row_input):
    """Search the above table to match the above input."""
    for row in TABLE:
        if (row.input & row_input) == row_input:
            return row
    return None


def _lw_or_fp(lw, fp):
    """Return either lw or fp, whichever is appropriate.

    Surprise:
      Return fp if we're not lw, AND file doesn't exist (not even deleted) in
      lw. This is currently benign: if we're not lightweight, returning fp
      (==None) isn't much different than returning lw (.discovered.p4==None).
    """
                        # If ever existed in lightweight branch even if
                        # deleted at rev, that's the cell.discovered.p4 to use.
    if NE != _exists_how(lw):
        return lw
                            # If never existed in lightweight branch, then
                            # use fully populated basis.
    else:
        return fp


def _exists_how(cell):
    """Does this file exist in this column?
    Existed but deleted at column's revision?
    Never existed?

    Returns one of input bit values [_E, DL, NE]
    """
    if not (     cell
            and  cell.discovered
            and ('depotFile' in cell.discovered)):
        return NE

                        # Problem:
                        #     'p4 copy -n' from a lightweight branch will store
                        # 'delete' actions for any file not yet branched into
                        # that lightweight branch. 'p4 files' won't clobber
                        # this 'delete' action because that file doesn't EXIST
                        # in the lightweight branch: it's "inherited" from its
                        # GPARFPN fully populated basis.
                        #
                        # Solution:
                        #     Examine the dict more deeply than just
                        # "('delete' in action)". If the file really _was_
                        # deleted, there'd be a 'change' value telling which
                        # changelist holds the 'p4 delete' file action.
                        #
                        # 2014-04-16 zig: I just ran into an integ source dict
                        # with no change. Fall back to 'startFromRev' which
                        # is the string 'none' in this case.
                        #
                        # action      : delete
                        # clientFile  : AbstractCapabilityInitializer.java
                        # depotFile   : …00009-u/…pabilityInitializer.java
                        # endFromRev  : none
                        # fromFile    : AbstractCapabilityInitializer.java
                        # otherAction : sync
                        # startFromRev: none
                        # workRev     : 1
                        #
    if 'delete' in cell.action():
        for key in ['headChange', 'change', 'startFromRev']:
            val = cell.discovered.get(key)
            if val and val != 'none':
                return DL
        return NE
    return _E


def _p4diff(before, after):
    """Return one of [A, M, D, N] that tells how a file differs between two
    Perforce depot branches. Never returns T (pointless precision that
    decision matrix ignores).

    Requires 'p4 fstat -Ol' to get MD5 digest and fileSize.

    Warning: +k RCS keyword expansions are included in MD5 digest:
    * +k causes erroneous MD5 mismatch even if the file revisions
      otherwise match.
        This means that most +k files will ALWAYS DIFFER between branches,
      triggering unnecessary 'p4 edit' actions that do nothing but
      propagate diffs in expanded RCS keywords.

    * +k causes erroneous MD5 match if one file is +k and one is not +k
      but the +k's RCS-expanded content matches the non-k's content.
        This means you cannot trust (a.digest == b.digest) when
      only one of the two files has +k. Avoid by treating
      a.fileType != b.fileType as M.
    """
                        # Avoid dereferencing nothingness. Swat away any
                        # exists/deleted/not-exists cases.
    e_before = _exists_how(before) == _E
    e_after  = _exists_how(after)  == _E
    if (not e_before) and (not e_after): # !e + N = !e
        return N
    if (not e_before) and (    e_after): # !e + A =  e
        return A
    if (    e_before) and (not e_after): #  e + D = !e
        return D
    # else do rest of function           #  e + {M, N} = e

                        # For less typing
    b = before.discovered
    a = after.discovered
    match =  (    b['digest']   == a['digest']
              and b['fileSize'] == a['fileSize']
              and b['headType'] == a['headType'] )
    if match:
        return N
    else:
        return M


_GIT_ACTION_TO_ENUM = {
      'A'  : A
    , 'M'  : M
    , 'T'  : T
    , 'D'  : D
    , None : N
}


def _gfediff(gdest_cell, ghost_cell):
    """Calculate our own replacement for git-fast-export's desired action.

    git-fast-export is relative to Git first-parent GPARN0, but we might be
    creating a GHOST of GPARN n != 0. We've got the Git mode and sha1 of the
    correct GPARN already stored in GHOST column (have) and GDEST (want).
    """
    if gdest_cell and gdest_cell.discovered:
        want_sha1 = gdest_cell.discovered.get('sha1')
        want_mode = gdest_cell.discovered.get('git-mode')
    else:
        want_sha1 = None
        want_mode = None

    if ghost_cell and ghost_cell.discovered:
        have_sha1 = ghost_cell.discovered.get('sha1')
        have_mode = ghost_cell.discovered.get('git-mode')
    else:
        have_sha1 = None
        have_mode = None

    if have_sha1 == want_sha1 and have_mode == want_mode:
        return N
    if (not have_sha1) and want_sha1:
        return A
    if have_sha1 and not want_sha1:
        return D
    if have_sha1 == want_sha1 and have_mode != want_mode:
        return T
    return M


def _crs(gdest_cell, ghost_cell):
    """Is this row a copy/rename source?
    Git expects all copy/rename sources to exist, so we need to make
    sure they're present in Perforce.
    """
    for cell in [gdest_cell, ghost_cell]:
        if not (cell and cell.discovered):
            continue
                        # It is a source for rename. Make sure we have
                        # something to 'move/delete'.
        if cell.discovered.get('git-action') == 'Rs':
            return CRS_S

                        # It is a source for copy. Make sure we have
                        # something from which to 'integ'.
                        #
                        # Why consider both GHOST and GDEST? Because the matrix
                        # has not yet decided whether or not to submit this
                        # ghost changelist or not. If yes, then the next
                        # changelist will be a delta from this GHOST, if no,
                        # then it will be a delta from GDEST's previous commit.
        for key in [ 'is-git-delta-a-copy-source'
                   , 'is-gdest-a-copy-source'
                   , 'is-ghost-a-copy-source'
                   ]:
            if key in cell.discovered:
                return CRS_S

    return CRS__


def _lw(is_lightweight):
    """Lightweight depot branches prohibit some branch-for-no-reason actions that
    fully populated depot branches permit.
    """
    if is_lightweight:
        return LW
    else:
        return FP


def _pop(gdest_column, gparn_column):
    """Are we populating a new depot branch?"""
    if _is_pop_new_branch(gdest_column, gparn_column):
        return POP
    else:
        return P__


def _is_pop_new_branch(gdest_column, gparn_column):
    """Are we populating a new depot branch?"""
                        # Not if there are already changelists in the depot
                        # branch.
    if gdest_column.change_num:
        return False

                        # No Git parent at all? Then we don't really have
                        # anything from which to populate, even if this is a
                        # new branch.
    if not gparn_column:
        return False

                        # Not if creating a lightweight GDEST child of a fully
                        # populated GPARN parent. Never fully populate a
                        # lightweight branch.
    if (        gdest_column.branch.is_lightweight
        and not gparn_column.branch.is_lightweight):
        return False

                        # Yes. We're either populating a new fully populated
                        # GDEST, or we're copying the files from a lightweight
                        # GPARN that contains only files that differ from
                        # lightweight GPARN's fully populated GPARFPN basis.
    return True


# -- debugging formatters ----------------------------------------------------

# Format strings for deb().
# LONG  Looks like a row in the decision table.
# SHORT Keeps Ghost Matrix Dump cells tolerable.
# RAW   A string of "010010110010101".
LONG  = '_GDEST_P4({gdest_p4}) | _P4JITFP_P4({p4ijtfp_p4})'         \
        ' | _H_W({h_w}) | _FP_W({fp_w}) | _GFE({gfe}) | {crs:<4} | {lw:<4}'       \
        ' | {pop}'
SHORT = 'gd{gdest_p4} jit{p4ijtfp_p4}' \
        ' hw{h_w} fpw{fp_w} gfe{gfe} {crs} {lw} {pop}'
RAW   = '{raw:035b}'


def deb(row_input, fmt=SHORT):
    """Debugging converter for input int."""
    if not isinstance(row_input, int):
        return str(row_input)

    return fmt.format(
              gdest_p4   = _deb_e   ( _unshift(row_input, _SHIFT_E_GDEST  ))
            , p4ijtfp_p4 = _deb_e   ( _unshift(row_input, _SHIFT_E_P4JITFP))
            , h_w        = _deb_diff( _unshift(row_input, _SHIFT_DIFF_H_W ))
            , fp_w       = _deb_diff( _unshift(row_input, _SHIFT_DIFF_FP_W))
            , gfe        = _deb_diff( _unshift(row_input, _SHIFT_DIFF_GFE ))
            , crs         = _deb_crs(row_input)
            , lw         = _deb_lw(row_input)
            , pop        = _deb_pop(row_input)
            , raw        = row_input
            )


def _masked_bin(x, mask):
    """0b000111000 ==> "111"."""
    mask_str = NTR('{:b}').format(mask)
    bit_str  = NTR('{:b}').format(mask & x)
    first_1_index = mask_str.find('1')
    last_1_index  = mask_str.find('1')
    return bit_str[first_1_index:last_1_index]


def _deb_e(x):
    """Debugging string for "exists in Perforce"."""
    mask = XX
    return { _E : '_E'
           , NE : 'NE'
           , DL : 'DL'
           , __ : '__'
           , XX : 'XX' }.get(x & mask, _masked_bin(x, mask))


def _deb_diff(x):
    """Debugging string for "Git or Perforce difference"."""
    bits = []
    for (const, literal) in zip([A, M, T, D, N], NTR('AMTDN')):
        if const & x & ~GHOST_BIT:
            bits.append(literal)
        else:
            bits.append('.')
    return ''.join(bits)


def _deb_lw(x):
    """debug dump."""
    mask = LW_X
    return { LW   : 'LW'
           , FP   : 'FP'
           , LW_X : 'LW_X' }.get(x & mask, _masked_bin(x, mask))


def _deb_crs(x):
    """debug dump."""
    mask = CRS_X
    return { CRS_S : 'CRS_S'
           , CRS__ : 'CRS__'
           , CRS_X : 'CRS_X' }.get(x & mask, _masked_bin(x, mask))


def _deb_pop(x):
    """debug dump."""
    mask = P_X
    return { POP : 'POP'
           , P__ : 'P__'
           , P_X : 'P_X' }.get(x & mask, _masked_bin(x, mask))

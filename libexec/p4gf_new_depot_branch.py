#! /usr/bin/env python3.3
"""Create new fully populated Perforce depot branches during a Git push.

Two different modes:

    * mode = explicit:
      Only refs that match a specific pattern become new fully populated
      branches:
        git push origin foo:depot-branch/mybranch
    * mode = all:
      Each pushed branch that does not already exist creates a new
      fully populated branch:
        git push [--set-upstream] origin foo:mybranch

Implementation shares some common API and concepts with Git Swarm reviews. Both
modules collect information about pushed PreReceiveTuples, both do some ref
shuffling after the copy to Perforce is complete. But they're also different
enough that I couldn't find a point to an abstract base class or a collection
of common functions. If you would like to hear a duck quack, press 7.

Main class is NDBCollection

    NDBCollection.from_prl() to instantiate

    NDBCollection.pre_copy_to_p4() to add new Branch definitions to
        ctx.branch_dict() and modify PreReceiveTuples in-place to point
        to their new fully-populated branch views.

    NDBCollection.post_push() to rename Git refs 'depot-branch/mybranch'
        to just 'mybranch'. Git user must 'git fetch --prune' to see
        the new ref and lose the old.
"""
import copy
import logging
import os
import pprint
import re

import P4
import p4gf_branch
from   p4gf_branch_id   import PreReceiveTuple
from   p4gf_gitmirror import write_config1
import p4gf_config
import p4gf_const
from   p4gf_l10n        import _, NTR
import p4gf_p4spec
import p4gf_path
import p4gf_proc
import p4gf_util

LOG = logging.getLogger(__name__)

                        # Any Git reference that matches this regex
                        # becomes a new depot branch.
RE_EXPLICIT = re.compile(r'refs/heads/depot-branch/(.*)')
RE_ALL      = re.compile(r'refs/heads/(.*)')
REF_GBN     = NTR(        'refs/heads/{git_branch_name}')

# ----------------------------------------------------------------------------

# Need an "enum" of modes. Might as well reuse the config strings for "enable".
MODE_NO       = p4gf_config.VALUE_NDB_ENABLE_NO
MODE_EXPLICIT = p4gf_config.VALUE_NDB_ENABLE_EXPLICIT
MODE_ALL      = p4gf_config.VALUE_NDB_ENABLE_ALL
_MODE_SET     = { MODE_NO, MODE_EXPLICIT, MODE_ALL }


class NDBCollection:

    """New depot branch collection.

    If this 'git push' contains Git references to create new fully populated
    depot branches, collect those new branch ids in a dict for fast
    lookup later.
    """

    def __init__(self, ctx, ndb_list=None):
        """."""
        self.ctx       = ctx
        self._ndb_list = ndb_list
        self._mode     = _get_mode(ctx)

    @staticmethod
    def from_prl(ctx, prl, gsreview_coll):
        """Create and populate a new NDBCollection from a pushed PreReceiveTuple list.

        Returns None if no new fully populated branches requested
        by this push.
        """
        ndb_list = None
        mode = _get_mode(ctx)
        LOG.debug('from_prl() mode={}'.format(mode))
                        # Explicitly request ref depot-branch/xxx
        r = _from_prl_explicit(ctx, prl)
        if MODE_EXPLICIT == mode:
            ndb_list = r
        elif r:
            raise RuntimeError(_("depot-branch creation disabled:\n{refs}")
                               .format(refs="\n".join([ndb.orig_prt.ref
                                                   for ndb in r])))

        if MODE_ALL == mode:
            ndb_list = _from_prl_all(ctx, prl, gsreview_coll)
        if not ndb_list:
            LOG.debug3('from_prl() returning None')
            return None

                        # We have 1 or more branches to create.
                        # Is pusher permitted to create branches?
        if _p4group_prohibits(ctx):
            raise RuntimeError(_('Perforce user {p4user}'
                                 ' not permitted to create depot branches.')
                               .format(p4user=ctx.authenticated_p4user))

                        # Batch up all errors and report them all at once. I'm
                        # getting sick of the one-by-one errors where you fix
                        # one error only to discover something else is wrong.
        err_msg_list = []
        gbn_to_branch = _gbn_to_branch(ctx)
        for ndb in ndb_list:
            err_msg = _err_msg( ctx           = ctx
                              , gbn_to_branch = gbn_to_branch
                              , ndb           = ndb)
            if err_msg:
                err_msg_list.append(err_msg)
        if err_msg_list:
            raise RuntimeError("\n".join(err_msg_list))

        return NDBCollection(ctx, ndb_list)

    def pre_copy_to_p4(self):
        """Insert new branch definitions into the Context.branch_dict().

        Modify PreReceiveTuple ref names to point to the desired
        git-branch-name.

        Must be called before Branch Assigner, since we want incoming commits
        to be assigned to these new fully populated branches.
        """
        LOG.debug('pre_copy_to_p4()')
        self._add_to_branch_dict()
        self._modify_prt_refs()

    def post_push(self, ctx):
        """Rename git references.

        Main entry point called after any 'git push' returns from both our
        pre-receive hook and Git itself.

        Now it's safe to rename each Git references to the fully populated
        branch refs, change 'depot-branch/mybranch' to just 'mybranch'.
        """
        LOG.debug('post_push()')
        self.rename_git_refs(ctx)

    def to_dict(self):
        """Convert the collection to a dict, for easy JSON serialization."""
        dict_list = [ndb.to_dict() for ndb in self._ndb_list]
        return {'branches': dict_list}

    @staticmethod
    def from_dict(ctx, dikt):
        """Create a new collection from the given dict, as from to_dict()."""
        ndb_list = [NDB.from_dict(d) for d in dikt['branches']]
        return NDBCollection(ctx, ndb_list)

    def rename_git_refs(self, ctx):
        """Replace any Git ref 'depot-branch/foo' with just 'foo'."""
        LOG.debug('rename_git_refs()')
        for ndb in self._ndb_list:
            ndb.rename_git_ref(ctx)

    def _add_to_branch_dict(self):
        """For each newly defined branch, create and insert a new Branch
        into the branch dict.

        Mark branches as new/needs write.
        """
        LOG.debug('_add_to_branch_dict()')
        d = {}
        for ndb in self._ndb_list:
            branch = ndb.create_branch(self.ctx)
            d[branch.branch_id] = branch
        if d:
            LOG.debug2('_add_to_branch_dict() adding:\n{}'
                       .format(pprint.pformat(d)))
            self.ctx.branch_dict().update(d)
            LOG.debug2('_add_to_branch_dict() updated branch_dict:{}'
                       .format(pprint.pformat(self.ctx.branch_dict())))

    def _modify_prt_refs(self):
        """Change ref 'refs/heads/depot-branch/mybranch' to 'refs/heads/mybranch'.

        To make the Branch Assigner use assign fully populated branches
        to incoming commits, perform this rename before calling the assigner.

        Original ref retained in NDB.orig_prt.ref

        Returns number of refs renamed.
        """
                        # Only MODE_EXPLICIT needs to rename refs.
                        # MODE_ALL keeps the pushed refs without modification.
        if MODE_EXPLICIT != self._mode:
            return

        LOG.debug('_modify_prt_refs()')
        changed_ct = sum((ndb.modify_prt_ref() for ndb in self._ndb_list))
        return changed_ct


# ----------------------------------------------------------------------------

class NDB:

    """A single New Depot Branch request."""

    def __init__( self, *
                , ctx
                , prt
                , git_branch_name
                , depot_root = None
                , view_p4map = None
                , orig_prt   = None
                ):
        assert prt and git_branch_name
        self.prt             = prt
        self.orig_prt        = orig_prt if orig_prt else copy.copy(prt)
        self.git_branch_name = git_branch_name
        self.depot_root      = depot_root
        self.view_p4map      = view_p4map
        if ctx:
            if not self.depot_root:
                self.depot_root  = _calc_depot_root(ctx, git_branch_name)
            if not self.view_p4map:
                self.view_p4map  = _calc_branch_p4map(ctx, self.depot_root)
        LOG.debug3(self.__str__())

    def __str__(self):
        return (  'NDB: gbn    : {git_branch_name}'
                '\n prt        : {prt}'
                '\n depot_root : {depot_root}'
                '\n view_p4map : {view_p4map}'
                .format( prt             = self.prt
                       , git_branch_name = self.git_branch_name
                       , depot_root      = self.depot_root
                       , view_p4map      = self.view_p4map ))

    def to_dict(self):
        """For easier JSON formatting."""
        return { 'prt'             : self.prt.to_dict()
               , 'orig_prt'        : self.orig_prt.to_dict()
               , 'git_branch_name' : self.git_branch_name
               , 'depot_root'      : self.depot_root
               , 'view_lines'      : self.view_p4map.as_array()
               }

    @staticmethod
    def from_dict(d):
        """For easier JSON parsing."""

        ndb = NDB( ctx             = None
                 , prt             = PreReceiveTuple.from_dict(d['prt'])
                 , orig_prt        = PreReceiveTuple.from_dict(d['orig_prt'])
                 , git_branch_name = d['git_branch_name']
                 , depot_root      = d['depot_root']
                 , view_p4map      = P4.Map(d['view_lines'])
                 )
        return ndb

    def create_branch(self, ctx):
        """Create and return a new Branch defintion, using our content."""
        branch                  = p4gf_branch.Branch(p4=ctx.p4gf)
        branch.git_branch_name  = self.git_branch_name
        branch.view_lines       = self.view_p4map.as_array()
        branch.view_p4map       = self.view_p4map
        branch.depot_branch     = None
        branch.is_lightweight   = False
        branch.is_new_fp_from_push  = True
        LOG.debug('create_branch() {}'.format(branch))
        return branch

    def modify_prt_ref(self):
        """Change prt.ref from 'refs/heads/depot-branch/mybranch' to 'refs/heads/mybranch'."""
        want = REF_GBN.format(git_branch_name=self.git_branch_name)
        LOG.debug2('modify_prt_ref() want={} got={}'.format(want, self.prt.ref))
        if self.prt.ref != want:
            self.prt.ref = want
            return True
        else:
            return False

    def rename_git_ref(self, ctx):
        """Change actual Git ref from 'refs/heads/depot-branch/mybranch'to 'refs/heads/mybranch'."""
        if self.prt.ref == self.orig_prt.ref:
            return
        old_name = _strip_refs_heads(self.orig_prt.ref)
        new_name = _strip_refs_heads(self.prt.ref)

        # Ensure the new name does not exist (may be left over from a failed push)
        try:
            ctx.repo.lookup_reference(self.prt.ref)
            cmd = ['git', 'branch', '-D', new_name]
            p4gf_proc.popen(cmd)
        except KeyError:
            pass
        except ValueError:
            pass

        cmd = ['git', 'branch', '-m', old_name, new_name]
        p4gf_proc.popen(cmd)
        LOG.debug('rename_git_ref() {} ==> {}'
                  .format(old_name, new_name))


# -- Module-wide defintions --------------------------------------------------

def _calc_depot_root(ctx, git_branch_name):
    """Return a depot path to use as a root.

    Returned string never ends in /. We'll sanitize that much.

    WARNING:
        Returned string may contain evil @#%* // ... prohibited strings.
        Returned root might be "under" a file within Perforce.
        Returned root might not be empty.
    """
    template = ctx.repo_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                   p4gf_config.KEY_NDB_DEPOT_PATH)
    if not template:
        template = p4gf_config.VALUE_NDB_DEPOT_PATH_DEFAULT

    r = template.format( repo            = ctx.config.repo_name
                       , git_branch_name = git_branch_name
                       , user            = ctx.authenticated_p4user )
                        # Strip trailing '/'. strip_trailing
    r = p4gf_path.strip_trailing_delimiter(r)
    return r


def _calc_branch_p4map(ctx, depot_root):
    """Return a branch view, rerooted at depot_root."""
    view_lines = ctx.repo_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                     p4gf_config.KEY_NDB_VIEW)
    if not view_lines:
        view_lines = p4gf_config.VALUE_NDB_VIEW_DEFAULT
    view_lines = p4gf_config.to_view_lines(view_lines)

    relative_p4map = P4.Map(view_lines)
    relative_p4map = p4gf_branch.convert_view_from_no_client_name(
        relative_p4map, ctx.config.p4client)
    remapper = P4.Map(os.path.join(depot_root, '...'), '...')
    remapped = P4.Map.join(remapper, relative_p4map)
    return remapped


def _gbn_to_branch(ctx):
    """Return a dict of known, not deleted, git_branch_name to Branch."""
    gbn_to_branch = {}
    for branch in ctx.branch_dict().values():
        if branch.deleted:
            continue
        if not branch.git_branch_name:
            continue
        gbn_to_branch[branch.git_branch_name] = branch
    return gbn_to_branch


def _from_prl_explicit(ctx, prl):
    """Return a list of NDB, one for each PreReceiveTuple that matches
    the "explicit push" pattern.
    """
    ndb_list = []
    for prt in prl:
        m = RE_EXPLICIT.match(prt.ref)
        if not m:
            continue
        ndb_list.append(NDB(ctx=ctx, prt=prt, git_branch_name=m.group(1)))
    return ndb_list


def _from_prl_all(ctx, prl, gsreview_coll):
    """Return a list of NDB, one for each PreReceiveTuple whose git-branch-name
    does not match any known, undeleted, Branch view.
    """
    gbn_to_branch = _gbn_to_branch(ctx)
    ndb_list = []
    for prt in prl:
        m = RE_ALL.match(prt.ref)
        if not m:
            continue
        git_branch_name = m.group(1)
        if not git_branch_name:
            continue
        if git_branch_name in gbn_to_branch:
            continue
        if gsreview_coll and gsreview_coll.ref_in_review_list(prt.ref):
            continue
        ndb_list.append(NDB(ctx=ctx, prt=prt, git_branch_name=m.group(1)))
    return ndb_list


def _get_mode(ctx):
    """Is depot branch creation enabled?

    Returns one of MODE_NO, MODE_EXPLICIT, or MODE_ALL.
    """
    value = ctx.repo_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                p4gf_config.KEY_NDB_ENABLE,
                                fallback=MODE_NO)
    value = str(value).lower()
    if value in _MODE_SET:
        return value
    else:
        return MODE_NO

def _to_key(view):
    """Return hash of stream name or view lines."""
    if type(view) is str:
        return hash(view)
    return sum(hash(line) for line in view)

def _deleted_repo_exists_for_reuse(ndb, ctx):
    """The branch views for this ndb are not empty.
    Determine if there is a deleted FP branch with the same git-branch-name
    and views. If so permit branch reuse only if there are no new changes
    on the P4 view path since the branch was deleted."""
    # pylint: disable=too-many-branches
    max_deleted_branch = None
    # find the branch with the highest 'deleted_at_change'
    for b in ctx.branch_dict().values():
        if b.deleted and not b.is_lightweight:
            # Verify deleted branch views identical to ndb branch views
            if _to_key(b.view_lines) == _to_key(ndb.view_p4map.as_array()):
                if not max_deleted_branch:
                    max_deleted_branch = b
                elif b.deleted_at_change:
                    if max_deleted_branch.deleted_at_change:
                        if  b.deleted_at_change > max_deleted_branch.deleted_at_change:
                            max_deleted_branch = b
                    else:
                        max_deleted_branch = b
                LOG.debug("_deleted_repo_exists get : branch_id={} deleted_at_change={}".
                        format(max_deleted_branch.branch_id, max_deleted_branch.deleted_at_change))

    if max_deleted_branch:
        # Need to check branch.deleted_at_change for non-FF push
        if max_deleted_branch.deleted_at_change:
            with ctx.switched_to_branch(max_deleted_branch):
                head_change = int(p4gf_util.head_change_as_string(ctx, submitted=True))
            if head_change > max_deleted_branch.deleted_at_change:
                # First create this branch definition so user can pull new changes.
                LOG.debug("_deleted_repo_exists non-FF : branch_id={} deleted_at_change={} head={}".
                    format(max_deleted_branch.branch_id,
                           max_deleted_branch.deleted_at_change,
                           head_change))
                new_branch = ndb.create_branch(ctx)
                d = {}
                # set the  the starting CL for the object cache for this re-used branch
                if max_deleted_branch.deleted_at_change:
                    new_branch.start_at_change = max_deleted_branch.deleted_at_change + 1
                d[new_branch.branch_id] = new_branch
                ctx.branch_dict().update(d)
                write_config1(ctx)    # from p4gf_gitmirror
                errmsg =  _("Perforce: Cannot create new depot branch for ref '{ref}':\n"
                         " Non Fast Forward push.\n"
                         " Git branch {git_branch_name} was deleted previously.\n"
                         " Git depot root '{depot_root}' already contains\n"
                         " Perforce changelists and there are now new Perforce changes since"
                         " the delete.\n"
                         " Execute 'git fetch <remote> {git_branch_name}:<alt-branch>'.\n"
                         " Rebase/merge as needed.\n"
                         " Then push again with 'git push <remote> {git_branch_name}.\n"
                         .format( ref        = ndb.prt.ref
                                , git_branch_name = ndb.git_branch_name
                                , depot_root = ndb.depot_root))
                return (False, errmsg)  # reject for non-FF of ndb over deleted branch

        LOG.debug("depot-branch_creation: found matching deleted branch:{}".
                format(max_deleted_branch))
        return (True, None)   # allow ndb : matching views and no new changes for deleted branch

    else:
        return (False, None)   # no deleted branch so the non-empty views criterion applies

def _err_msg(*, ctx, gbn_to_branch, ndb):
    """Validation check: is the requested new branch legal?

    Git branch name must not already exist. (Exist but deleted okay.)
    Must not contain evil @#%* chars or ... // path elements. (/ okay).
    Must not already have any changelists under its root.
    """
    # pylint: disable=too-many-branches
    if ndb.git_branch_name in gbn_to_branch:
        return _("Perforce: Cannot create new depot branch for ref '{ref}':"
                 " Git branch '{git_branch_name}' already exists."
                 .format( ref             = ndb.prt.ref
                        , git_branch_name = ndb.git_branch_name))

    evil_list = ['@', '#', '$', '*', '//', '..', '{', '}']
    for evil in evil_list:
        if evil in ndb.git_branch_name:
            return _("Perforce: Cannot create new depot branch for ref '{ref}':"
                     " Git branch '{git_branch_name}' contains "
                     "prohibited '{evil}'."
                     .format( ref             = ndb.prt.ref
                            , git_branch_name = ndb.git_branch_name
                            , evil            = evil))

                        # Handle // in depot root later.
        if evil == '//' and evil:
            continue
        if evil in ndb.depot_root:
            return _("Perforce: Cannot create new depot branch for ref '{ref}':"
                     " Git depot root '{depot_root}' contains "
                     "prohibited '{evil}'."
                     .format( ref        = ndb.prt.ref
                            , depot_root = ndb.depot_root
                            , evil       = evil))

                        # Permit (require!) // at front, reject elsewhere.
    if not ndb.depot_root.startswith('//'):
        return _("Perforce: Cannot create new depot branch for ref '{ref}':"
                 " Git depot root '{depot_root}' must start with '//' ."
                 .format( ref        = ndb.prt.ref
                        , depot_root = ndb.depot_root))
    elif '//' in ndb.depot_root[1:]:
        return _("Perforce: Cannot create new depot branch for ref '{ref}':"
                 " Git depot root '{depot_root}' must not contain"
                 " '//' except at start."
                 .format( ref        = ndb.prt.ref
                        , depot_root = ndb.depot_root))

    depot_re = re.compile(r'^//([^/]+)/.+$')
    m        = depot_re.match(ndb.depot_root)
    if m:
        depot = m.group(1)
        if not p4gf_p4spec.spec_exists(ctx.p4gf, 'depot', depot):
            return _("Perforce: Cannot create new depot branch for ref '{ref}':"
                     " Perforce depot '{depot}' in Git depot root '{depot_root}' "
                     " does not exist."
                     .format( ref        = ndb.prt.ref
                            , depot = depot
                            , depot_root = ndb.depot_root))

    # disallow a depot_root like '//somedepot'
    else:
        return _("Perforce: Cannot create new depot branch for ref '{ref}':"
                 " in Git depot root '{depot_root}': "
                 " a depot without any subdirectory is not allowed."
                 .format( ref        = ndb.prt.ref
                        , depot_root = ndb.depot_root))
    with ctx.switched_to_view_lines(ndb.view_p4map.as_array()):
        r = ctx.p4run('changes', '-m1', ctx.client_view_path())
    if r:
        # We have non-empty views
        # Exists a deleted FP branch def with same git_branch_name which qualifies for re-creation?
        (can_reuse_deleted, errmsg) = _deleted_repo_exists_for_reuse(ndb, ctx)
        if not can_reuse_deleted:
            # No - either no deleted branch definition
            # Or some disqualifying condition in 'errmsg'
            if errmsg:
                return errmsg
            else:
                return _("Perforce: Cannot create new depot branch for ref '{ref}':"
                     " Git depot root '{depot_root}' already contains"
                     " Perforce changelists."
                     .format( ref        = ndb.prt.ref
                            , depot_root = ndb.depot_root))

                        # Require matched RHS.
    enable_mismatched_rhs = ctx.repo_config.getboolean(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                                       p4gf_config.KEY_ENABLE_MISMATCHED_RHS,
                                                       fallback=False)
    if not enable_mismatched_rhs:
        master_ish = ctx.most_equal()
        if (    master_ish
            and master_ish.view_p4map.rhs() != ndb.view_p4map.rhs()):
            return ( _("Perforce: Cannot create new depot branch for ref '{ref}':")
                     .format(ref=ndb.prt.ref)
                   + "\n" + _("branch views do not have same right hand sides")
                   + "\n" + _("view for branch '{}':\n{}")
                              .format(master_ish.branch_id, master_ish.view_lines)
                   + "\n" + _("view for branch '{}':\n{}")
                              .format(ndb.git_branch_name,  ndb.view_p4map.as_array)
                   )

                        # Congratulations. You pass.
    return None


def _filename(ctx):
    """Return path to temp file where we carry our NDB list from
    pre-receive hook process to ancestor p4gf_auth_server process.
    """
    return os.path.join( ctx.repo_dirs.repo_container
                       , p4gf_const.P4GF_NEW_DEPOT_BRANCH_LIST)


def _strip_refs_heads(ref_name):
    """Strip ref prefix: "refs/heads/mybranch" ==> "mybranch"."""
    prefix = "refs/heads/"
    if ref_name.startswith(prefix):
        return ref_name[len(prefix):]
    else:
        return ref_name


def _p4group_prohibits(ctx):
    """Deny branch creation permission if
    * depot-branch-creation-p4group set, and
    * current pusher not in that group
    """
                    # Name of group pusher must be a member of.
    p4group_name = ctx.repo_config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                       p4gf_config.KEY_NDB_P4GROUP)
    if not p4group_name:
        return False

                    # Names of groups pusher is a member of.
    p4group_list = ctx.p4run('groups', '-i', '-u', ctx.authenticated_p4user)
    pusher_ok = any(p4group.get('group') == p4group_name for p4group in p4group_list)
    if not pusher_ok:
        return True

    if not ctx.foruser:
        return False

                    # Names of groups foruser is a member of.
    p4group_list = ctx.p4run('groups', '-i', '-u', ctx.foruser)
    foruser_ok = any(p4group.get('group') == p4group_name for p4group in p4group_list)
    return not foruser_ok

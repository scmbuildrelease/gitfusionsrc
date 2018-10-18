#! /usr/bin/env python3.3
"""A Git to Perforce branch association.

Mostly just a wrapper for one section of a repo config file.

"""
from   collections import defaultdict, namedtuple
import copy
import logging
import operator
import re
import sys

import P4

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_case_handling
import p4gf_config
import p4gf_const
import p4gf_create_p4
from   p4gf_depot_branch import DepotBranchInfo
from   p4gf_l10n import _, NTR
import p4gf_path
import p4gf_path_convert
import p4gf_translate
import p4gf_util
import p4gf_streams

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_branch")


BranchId = None
BranchRef = None

# A mapping line as left and right sides. Element of tuple_list.
MapTuple = namedtuple("MapTuple", ["lhs", "rhs"])


def init_case_handling(p4=None):
    """Set BranchId, BranchRef to the correct class."""
    p4gf_case_handling.init_case_handling(p4)
    global BranchId, BranchRef
    BranchId = p4gf_case_handling.CaseHandlingString
    BranchRef = p4gf_case_handling.CaseHandlingString


class Branch:

    """A Git<->Perforce branch association.

    Mostly just a wrapper for one section of a repo config file.

    There are two types of branches: classic and stream

    For a classic branch, stream_name and writable_stream_name will be None
    and view_lines and view_p4map will always be set.

    For a stream branch, stream_name and writable_stream_name will be set but
    view_lines and view_p4map will initially be unset, as that requires
    running 'p4 stream -ov' to get the stream's view.

    view_lines and view_p4map may list their RHS as either relative to some
    root ('//depot/... ...') or when used in p4gf_context, already associated
    with a client ('//depot/... //p4gf_myrepo/...').
    """

    # pylint: disable=too-many-public-methods
    def __init__(self, branch_id=None, p4=None):
        # if no branch_id supplied but a p4 is supplied, create a branch_id
        if not branch_id:
            branch_id = _create_anon_branch_id(p4)
        self._branch_id    = None
        self.branch_id     = branch_id  # Section name.
                                        # Must be unique for this repo.
                                        #
                                        # Can be None: from_branch() creates
                                        # temporary branch view instances that
                                        # never go into ctx.branch_dict() or any
                                        # config file.

        self.git_branch_name    = None  # Git ref name, minus "refs/heads/".
                                        # None for anonymous branches.

                                        # This branch's view into the
                                        # P4 depot hierarchy.
                                        #
                                        # Both can be None for new lightweight
                                        # branches for which we've not yet
                                        # created a view.
                                        #
        self.view_lines         = None  # as list of strings
        self.view_p4map         = None  # view_lines as P4.Map
        self.stream_name        = None  # for stream branches, the stream this
                                        # branch is connected to
        self.original_view_lines = None # snapshot of stream's view at the time
                                        # the branch was created
        self.writable_stream_name = None# for stream branches, the name of the
                                        # stream that is writable on this branch
                                        # either the stream itself or its
                                        # baseParent for virtual streams
        self.depot_branch       = None  # DepotBranchInfo where we store files
                                        # if we're lightweight.
                                        #
                                        # None if fully populated OR if not
                                        # yet set.
                                        #
                                        # Reading from config? Starts out as
                                        # just a str depot_branch_id. Someone
                                        # else (Context) must convert to full
                                        # DepotBranchInfo pointer once that list
                                        # has been loaded.

        self.deleted            = False # Latches true when git deletes a lightweight task branch
        self.deleted_at_change  = None  # Head change of branch views at time of delete
        self.start_at_change    = None  # First change to add to object-cache for this branch-id
                                        # If there are deleted FP branches with the same git-name
                                        # and views then set to max( 'deleted_at_change') + 1
                                        # Only used if set



        self.is_read_only       = False # Prohibit push to this branch view?

        self.fork_of_branch_id  = None  # If this branch is a copy from some
                                        # other repo, what's its branch id
                                        # WITHIN THAT OTHER REPO. Not a valid
                                        # branch_id within this repo.

        self._fp_depot_root     = None  # This branch's topmost container within Perforce,
                                        # except for any import/shared paths. "//depot/main"
                                        #
                                        # Only set for fully populated branches.  Concept is
                                        # invalid for lightweight branches: lightweight
                                        # branches can contain anything, their root is always
                                        # dbi.root_depot_path "//.gf/branches/{uuid}", which
                                        # corresponds to "//", not "//depot/main".
                                        #
                                        # Optionally specified by human in p4gf_config file.
                                        # Used in p4gf_fork_config.py to reroot a branch
                                        # without changing import/shared paths.
                                        #
                                        # See @property fp_depot_root() accessors.

        # -- begin data not stored in config files ----------------------------
        #
        # Adding a new property? Make sure it's covered in the from/to methods
        #

        self.is_lightweight     = False # Is this a branch that stores only
                                        # changed files in Perforce?

        self.is_dbi_partial     = False # Set True when we set when defining a
                                        # new depot branch, if we suspect our
                                        # parent or fully populated basis is
                                        # empty because it is not yet submitted
                                        # to Perforce.
                                        #
                                        # Cleared back to False when we set
                                        # dbi's parent, or know for sure that it
                                        # doesn't have one.

        self.populated          = None  # Latches True or False during is_populated()
                                        ### (was latched in p4gf_jit
                                        ### .populate_first_commit_on_current_branch() )

        self.more_equal         = False # Is this the first branch view listed
                                        # in p4gf_config? This is the view that
                                        # we use for HEAD. Usually this is
                                        # 'master', but not always.

        self.is_new_fp_from_push = False# Is this a git branch being pushed
                                        # causing a new fully populated branch to be created?

    @property
    def branch_id(self):
        """Get branch ID."""
        return self._branch_id

    @branch_id.setter
    def branch_id(self, value):
        """Set branch ID."""
        self._branch_id = BranchId(value)

    @staticmethod
    def from_config(config, branch_id, p4, strict=False):
        """Factory to seed from a config file.

        Returns None if config file lacks a complete and correct branch
        definition.

        """
        # pylint: disable=too-many-branches, too-many-statements
        valid_branch_id = re.compile("^[-_=.a-zA-Z0-9]+$")
        if not valid_branch_id.match(branch_id):
            raise RuntimeError(_("repository configuration section [{section}] "
                                 "has invalid section name '{section}'")
                               .format(section=branch_id))
        is_deleted = False
        if config.has_option(branch_id, p4gf_config.KEY_GIT_BRANCH_DELETED):
            is_deleted = config.getboolean(branch_id, p4gf_config.KEY_GIT_BRANCH_DELETED)
        deleted_at_change = None
        if config.has_option(branch_id, p4gf_config.KEY_GIT_BRANCH_DELETED_CHANGE):
            deleted_at_change = int(config.get(branch_id,p4gf_config.KEY_GIT_BRANCH_DELETED_CHANGE))
        start_at_change = None
        if config.has_option(branch_id, p4gf_config.KEY_GIT_BRANCH_START_CHANGE):
            start_at_change = int(config.get(branch_id,p4gf_config.KEY_GIT_BRANCH_START_CHANGE))
        is_read_only = _is_read_only_from_config(branch_id, config)

        result = Branch(branch_id=branch_id)
        branch_config = config[branch_id]
        result.git_branch_name = branch_config.get(p4gf_config.KEY_GIT_BRANCH_NAME)
        result.depot_branch = branch_config.get(p4gf_config.KEY_DEPOT_BRANCH_ID)
        result.deleted = is_deleted
        result.deleted_at_change = deleted_at_change
        result.start_at_change = start_at_change
        result.is_read_only = is_read_only
        result.fork_of_branch_id = branch_config.get(p4gf_config.KEY_FORK_OF_BRANCH_ID, None)
        result._fp_depot_root = branch_config.get(p4gf_config.KEY_DEPOT_ROOT, None)  # pylint: disable=protected-access
        if p4gf_config.KEY_STREAM in branch_config and p4gf_config.KEY_VIEW in branch_config:
            raise RuntimeError(_("repository configuration section [{section}] "
                                 "may not contain both 'view' and 'stream'")
                               .format(section=branch_id))
        if strict and not (p4gf_config.KEY_STREAM in branch_config
                           or p4gf_config.KEY_VIEW in branch_config):
            raise RuntimeError(_("repository configuration section [{section}] "
                                 "must contain either 'view' or 'stream'")
                               .format(section=branch_id))
        if p4gf_config.KEY_STREAM in branch_config:
            result.stream_name = branch_config.get(p4gf_config.KEY_STREAM)
            stream = p4gf_util.first_dict(p4.run('stream', '-ov', result.stream_name))
            if p4gf_streams.stream_contains_isolate_path(p4, stream):
                f = _("repository configuration section [{}] '{}' refers to stream"
                      " with 'isolate' Path")
                raise RuntimeError(f.format(branch_id, result.stream_name))

            LOG.debug("stream for branch:\n{}\n".format(stream))
            if 'View' not in stream:
                raise RuntimeError(_("repository configuration section [{section}] "
                                     "'{stream}' does not refer to a valid stream")
                                   .format(section=branch_id, stream=result.stream_name))
            if stream['Type'] == 'task':
                raise RuntimeError(_("repository configuration section [{section}] "
                                     "'{stream}' refers to a task stream")
                                   .format(section=branch_id, stream=result.stream_name))
            if stream['Type'] == 'virtual':
                result.writable_stream_name = stream['baseParent']
            else:
                result.writable_stream_name = result.stream_name
            vl = stream['View']
            view_lines = canonical_view_lines(vl)
            LOG.debug3("View lines:\n%s\n", view_lines)
            # if this is a config2, stream branches will have stored
            # a snapshot of the stream's view at branch create time
            if p4gf_config.KEY_ORIGINAL_VIEW in branch_config:
                result.original_view_lines = p4gf_config.get_view_lines(
                                              branch_config
                                            , p4gf_config.KEY_ORIGINAL_VIEW )
        else:
            vl = p4gf_config.get_view_lines( branch_config
                                           , p4gf_config.KEY_VIEW )
            view_lines = canonical_view_lines(vl)

        LOG.debug2("view_lines=%s", view_lines)
        if not view_lines:
            return None

        if isinstance(view_lines, str):
            view_lines = view_lines.replace('\t', ' ') # pylint: disable=maybe-no-member
                                                       # 'list' has no 'replace' member
                                                       # Well yeah because it's a str.
        elif isinstance(view_lines, list):
            view_lines = [ln.replace('\t', ' ') for ln in view_lines]
        result.view_p4map = P4.Map(view_lines)
        result.view_lines = view_lines

        return result

    @staticmethod
    def from_branch(branch, new_branch_id):
        """Return a new Branch instance, with values copied from another."""
        r = copy.copy(branch)
        r.branch_id = new_branch_id
        return r

    @staticmethod
    def from_dict(dikt, client_name):
        """Create a Branch instance from a dict, as from to_dict().

        :param str client_name: used to set rhs of view mapping.

        """
        result = Branch()
        for k, v in dikt.items():
            setattr(result, k, v)
        # depot_branch needs special treatment
        if result.depot_branch:
            result.depot_branch = DepotBranchInfo.from_dict(result.depot_branch)
        # view_p4map needs special treatment
        if result.view_lines:
            result.view_p4map = convert_view_from_no_client_name(result.view_lines, client_name)
            result.view_lines = result.view_p4map.as_array()
        return result

    def to_dict(self):
        """Produce a dict of this object, suitable for JSON serialization."""
        # pylint:disable=maybe-no-member
        result = dict()
        result['branch_id'] = self.branch_id
        result['git_branch_name'] = self.git_branch_name
        client_less = p4gf_path_convert.convert_view_to_no_client_name(self.view_lines)
        result['view_lines'] = P4.Map(client_less.splitlines()).as_array()
        # P4.Map is not JSON serializable
        # result['view_p4map'] = self.view_p4map
        result['stream_name'] = self.stream_name
        result['original_view_lines'] = self.original_view_lines
        result['writable_stream_name'] = self.writable_stream_name
        if self.depot_branch:
            result['depot_branch'] = self.depot_branch.to_dict()
        else:
            result['depot_branch'] = None
        result['deleted'] = self.deleted
        result['is_read_only'] = self.is_read_only
        result['fork_of_branch_id'] = self.fork_of_branch_id
        result['is_lightweight'] = self.is_lightweight
        result['is_dbi_partial'] = self.is_dbi_partial
        result['populated'] = self.populated
        result['more_equal'] = self.more_equal
        result['is_new_fp_from_push'] = self.is_new_fp_from_push
        return result

    def add_to_config(self, config):
        """Create a section with this Branch object's data.

        This is used to add lightweight and stream-based branches to
        the p4gf_config2 file.
        """
        # pylint: disable=too-many-branches
        section = p4gf_config.branch_section(config, self.branch_id)
        if self.git_branch_name:
            config[section][p4gf_config.KEY_GIT_BRANCH_NAME] = self.git_branch_name
        if self.stream_name:
            config[section][p4gf_config.KEY_STREAM] = self.stream_name
        if self.view_lines:
            stripped_lines = p4gf_path_convert.convert_view_to_no_client_name(self.view_lines)
            # for stream-based branches, we're saving a snapshot of the view
            # at the time of branch creation to enable mutation detection
            # later on.
            if self.stream_name:
                config[section][p4gf_config.KEY_ORIGINAL_VIEW] = stripped_lines
            else:
                config[section][p4gf_config.KEY_VIEW] = stripped_lines

        # Store only if set
        if self.start_at_change:
            config[section][p4gf_config.KEY_GIT_BRANCH_START_CHANGE] =  \
                    str(self.start_at_change)
        if self.deleted:
            config[section][p4gf_config.KEY_GIT_BRANCH_DELETED] = NTR('True')
            if self.deleted_at_change:
                config[section][p4gf_config.KEY_GIT_BRANCH_DELETED_CHANGE] =  \
                        str(self.deleted_at_change)
        if self.is_read_only:
            config[section][p4gf_config.KEY_READ_ONLY] = NTR('True')
        if self.fork_of_branch_id:
            config[section][p4gf_config.KEY_FORK_OF_BRANCH_ID] = self.fork_of_branch_id
        if self._fp_depot_root:
            config[section][p4gf_config.KEY_DEPOT_ROOT] = self._fp_depot_root
        if self.depot_branch:
            if isinstance(self.depot_branch, str):
                config[section][p4gf_config.KEY_DEPOT_BRANCH_ID] = self.depot_branch
            else:
                config[section][p4gf_config.KEY_DEPOT_BRANCH_ID] = self.depot_branch.depot_branch_id

    def intersects_p4changelist(self, p4changelist):
        """Does any file in the given P4Changelist object intersect our branch's
        view into the Perforce depot hierarchy?
        """
        if LOG.isEnabledFor(logging.DEBUG3):
            def _loggit(intersects, path):
                """Noisy logging dumpage."""
                LOG.debug3('branch_id={br} intersect={i} change={cl} path={pa} view={vw}'
                           .format( br = self.branch_id[:7]
                                  , i  = intersects
                                  , cl = p4changelist.change
                                  , pa = path
                                  , vw = '\n'.join(self.view_lines)))
            loggit = _loggit
        else:
            loggit = None

        # Do NOT optimize by checking p4changelist.path against view and
        # early-returning False if path is not in our view. Path might be
        # something really high up like //... or //depot/... for changelists
        # that straddle multiple branches, and False here would miss that
        # changelist's intersection with our view. Thank you
        # push_multi_branch.t for catching this.

        LOG.debug2('intersects_p4changelist() view map=%s', self.view_p4map)
        for p4file in p4changelist.files:
            LOG.debug3('intersects_p4changelist() considering {}'.format(p4file.depot_path))
            if self.view_p4map.includes(p4file.depot_path):
                if loggit:
                    loggit(True, p4file.depot_path)
                return True

        if loggit:
            loggit(False, NTR('any depot_path'))
        return False

    def intersects_depot_file_list(self, depot_file_list):
        """Does any depotFile in the given list intersect our branch view?"""
        for depot_file in depot_file_list:
            if self.view_p4map.includes(depot_file):
                return True
        return False

    def intersects_depot_path(self, depot_path):
        """Does a depot path intersect our branch view?"""
        if self.view_p4map.includes(depot_path):
            return True
        return False

    def __repr__(self):
        lines = [ '[{}]'.format(self.branch_id)
                , 'git-branch-name = {}'.format(self.git_branch_name)
                , 'is_lightweight = {}'.format(self.is_lightweight)
                , 'deleted = {}'.format(self.deleted)
                , 'deleted-at-change = {}'.format(self.deleted_at_change)
                , 'start-at-change = {}'.format(self.start_at_change)
                , 'view =\n\t{}'.format(self.view_lines) ]
        if self.stream_name:
            lines.append('stream = {}'.format(self.stream_name))
            lines.append('writable_stream = {}'.format(self.writable_stream_name))
        if self.view_p4map:
            lines.append('p4map = {}'.format(self.view_p4map))
        return '\n'.join(lines)

    def to_log(self, logger=LOG):
        """Return a representation suitable for the logger's level.

        If DEBUG  or less, return only our branch_id, abbreviated to 7 chars.
        If DEBUG2 or more, dump a full representation.
        """
        if logger.isEnabledFor(logging.DEBUG2):
            return self.__repr__()
        return self.branch_id[:7]

    def set_rhs_client(self, client_name):
        """Convert a view mapping's right-hand-side from its original client
        name to a new client name:

            //depot/dir/...  dir/...
            //depot/durr/... durr/...

        becomes

            //depot/dir/...  //client/dir/...
            //depot/durr/... //client/durr/...
        """
        self.view_p4map = convert_view_from_no_client_name( self.view_p4map
                                                          , client_name )
        self.view_lines = self.view_p4map.as_array()

    def strip_rhs_client(self):
        """Set all RHS lines to client-less, as written to the config file."""
        # pylint:disable=maybe-no-member
        less = p4gf_path_convert.convert_view_to_no_client_name(self.view_lines)
        self.view_p4map = P4.Map(less.splitlines())
        self.view_lines = self.view_p4map.as_array()

    def set_depot_branch(self, new_depot_branch):
        """Replace any previous depot_branch with new_depot_branch (None okay).

        Calculate a new view mapping with our old branch root replaced by
        the new branch root.
        """
        old_root = _depot_root(self.depot_branch)
        new_root = _depot_root(new_depot_branch)

        if self.view_p4map:
            new_p4map = p4gf_util.create_p4map_replace_lhs_root( self.view_p4map
                                                               , old_root
                                                               , new_root )
            new_lines = new_p4map.as_array()

        else:   # Nothing to change.
            new_p4map = self.view_p4map
            new_lines = self.view_lines

        self.depot_branch = new_depot_branch
        self.view_p4map   = new_p4map
        self.view_lines   = new_lines

    def _fully_populated_view_p4map(self, fp_basis_map_line_list):
        """Return a P4.Map instance that lists our view onto the fully populated depot.

        Returns a new P4.Map that looks like our lightweight view,
        re-rooted to // via fp_basis_map_line_list.
        """
        assert self.is_lightweight
        assert fp_basis_map_line_list
        fp_to_lw_p4map     = P4.Map(fp_basis_map_line_list)
        lw_to_client_p4map = self.view_p4map
        fp_to_client_p4map = P4.Map.join(fp_to_lw_p4map, lw_to_client_p4map)
        return fp_to_client_p4map

    def find_fully_populated_change_num(self, ctx):
        """Return the changelist number from which this branch first diverged
        from fully populated Perforce.

        If this branch IS fully populated Perforce, return None.
        """
                        # Do not call self.find_fully_populated_basis()
                        # from inside this function! This function
                        # find_fully_populated_change_num() must remain
                        # safe for use during preflight, when it might
                        # return a ":mark" instead of a change_num.

        if not self.is_lightweight:
            return None

        return ctx.depot_branch_info_index()\
            .find_fully_populated_change_num(self.get_or_find_depot_branch(ctx))

    def find_fully_populated_basis(self, ctx):
        """Return the changelist and mapping that defines which files
        we can "inherit" from our fully populated basis.

        CANNOT BE CALLED DURING GIT-PUSH PREFLIGHT:
        requires that all parent commits be submitted, so they have changelist
        numbers not git-fast-import ":mark" strings, for their own changelists.
        """
        dbi = self.get_or_find_depot_branch(ctx)
        if not dbi:
            LOG.debug2("find_fully_populated_basis() {} no FPBasis: dbi=None"
                       " ==> fully populated branch view."
                       .format(abbrev(self)))
            return None
        return ctx.depot_branch_info_index()\
                   .get_fully_populated_basis(dbi, self.view_p4map)

    def is_populated(self, ctx):
        """Does this branch have at least one changelist?

        Latches True once we see a changelist.
        """
        if self.populated is None:
            with ctx.switched_to_branch(self):
                r = ctx.p4run('changes', '-m1', ctx.client_view_path())
            if r:
                self.populated = True
                return self.populated
            else:
                        # Do not latch False during 'git push'.
                        # Answer will change as soon as we submit anything
                        # to this branch (which is likely our very
                        # next submit).
                return False
        return self.populated

    def find_depot_branch(self, ctx):
        """Scan through known DepotBranchInfo until we find one whose root
        contains the first line of our view.
        """
        lhs0 = p4gf_path.dequote(self.view_p4map.lhs()[0])
        depot_branch = ctx.depot_branch_info_index().find_depot_path(lhs0)
        return depot_branch

    def get_or_find_depot_branch(self, ctx):
        """If we already know our depot branch, return it.
        If not, go find it, remember it, return it.
        """
        if not self.depot_branch:
            self.depot_branch = self.find_depot_branch(ctx)
        return self.depot_branch

    def is_ancestor_of_lt(self, ctx, child_branch):
        """Is our depot_branch an ancestor of lightweight
        child_branch's depot_branch?

        Strict: X is not its own ancestor.
        """
        if not child_branch.is_lightweight:
            return False

        our_depot_branch   = self.get_or_find_depot_branch(ctx)
        child_depot_branch = child_branch.get_or_find_depot_branch(ctx)
        # Strict: X is not its own ancestor.
        if our_depot_branch == child_depot_branch:
            return False
        # +++ None ==> Fully populated Perforce,
        # +++ always ancestor of any lightweight branch.
        if not our_depot_branch:
            return True

        # Must walk lightweight child's ancestry tree,
        # looking for our depot branch.
        cl_num = ctx.depot_branch_info_index().find_ancestor_change_num(
                                        child_depot_branch, our_depot_branch)
        return True if cl_num else False

    def p4_files(self, ctx, at_change=NTR('now'), depot_path=None):
        """Run 'p4 files //client/...@change' and return the result.

        If this is a lightweight branch run the command TWICE, once for our
        lightweight view, and then again for fully populated Perforce at
        whatever changelist we diverged from fully populated Perforce.
        Return the merged results.

        Inserts 'clientFile' values for each p4 file dict because we have it
        handy and we understand "inherited from fully populated Perforce" better
        than code that calls us. Inserts 'gwt_path' for the same reason.

        Switches the client view to ancestor and back.

        WARNING: You probably want to run strip_conflicting_p4_files()
                 on the result list.
        """
        our_files = self.lw_files(ctx, at_change, depot_path)

        if not self.is_lightweight:
            return our_files

                        # Keep only the fully populated paths not replaced by
                        # a file in our own branch.
                        # +++ Hash by client path.
        our_client_path_list = {x['clientFile'] for x in our_files}
        fp_files = [f for f in self.fp_basis_files(ctx, depot_path)
                    if f['clientFile'] not in our_client_path_list]

        return our_files + fp_files

    def lw_files(self, ctx, at_change=NTR('now'), depot_path=None):
        """Run 'p4 files //client/...@change' on our branch and return results.

        Inserts 'clientFile' values for each p4 file dict because we have it
        handy and we understand "inherited from fully populated Perforce" better
        than code that calls us. Inserts 'gwt_path' for the same reason.

        Ignores any fully populated basis.

        Returned list includes files deleted at change. Check those 'action'
        values!
        """
        our_files = []
        with ctx.switched_to_branch(self):
            if depot_path:
                path_at = '{}@{}'.format(depot_path, at_change)
            else:
                path_at = ctx.client_view_path(at_change)
            if at_change != 'now':
                r = ctx.branch_files_cache.files_at( ctx        = ctx
                                                   , branch     = self
                                                   , change_num = at_change )
            else:
                r = ctx.p4run('files', path_at)
            for rr in r:
                if isinstance(rr, dict) and 'depotFile' in rr:
                    c = self.view_p4map.translate(rr['depotFile'])
                    rr['clientFile'] = c
                    rr['gwt_path'] = ctx.depot_to_gwt_path(rr['depotFile'])
                    our_files.append(rr)
        return our_files

    def fp_basis_files(self, ctx, depot_path=None):
        """Run 'p4 files //client/...@change' on our fully populated basis and
        return results.

        Inserts 'clientFile' values for each p4 file dict because we have it
        handy and we understand "inherited from fully populated Perforce" better
        than code that calls us. Inserts 'gwt_path' for the same reason.

        Returned list includes files deleted at basis. Check those 'action'
        values!

        Return empty list if no FP basis, or FP basis is empty.
        """
        if not self.is_lightweight:
            return []

        fp_basis = self.find_fully_populated_basis(ctx)
        if not (    fp_basis
                and str(fp_basis.change_num) != '0'
                and fp_basis.map_line_list):
            return []

        fp_files = []
        fp_p4map = self._fully_populated_view_p4map(fp_basis.map_line_list)
        with ctx.switched_to_view_lines(fp_p4map.as_array()):
            if depot_path:
                path_at = '{}@{}'.format(depot_path, fp_basis.change_num)
            else:
                path_at = ctx.client_view_path(fp_basis.change_num)
            fp_files_result = ctx.p4run('files', path_at)

            for rr in fp_files_result:
                if not (isinstance(rr, dict) and 'depotFile' in rr):
                    continue
                client_path = fp_p4map.translate(rr['depotFile'])
                rr['clientFile'] = client_path
                        # We've got the FP map loaded, it's the perfect
                        # time to convert to GWT path.
                rr['gwt_path'] = ctx.depot_to_gwt_path(rr['depotFile'])
                fp_files.append(rr)
        return fp_files

    @staticmethod
    def strip_conflicting_p4_files(p4files):
        """Remove 'p4 files' result dicts whose clientFile path conflicts
        with other clientFile paths in the same list.

        Branch.p4_files() can return a p4files list with a single file path
        used as both a directory and a file. This is legal (but dangerous!) in
        Perforce, but illegal (good!) in Git.

        The primary cause of this is when a pushed Git repo deletes a symlink
        and replaces it with a directory, or vice-versa. Pre-Matrix-2  Git
        Fusion fails to record the deletion in Perforce (no branch-for-delete
        file action), so the lightweight branch resurrects the  deleted
        symlink, (or files, if vice-versa).

        Use changelist numbers to help guess at whether to delete the ancestor
        or descendant paths.
        """
                        # How was each path most recently used?
                        # path to int(change num)
        as_dir  = defaultdict(int)
        as_file = defaultdict(int)
        for f in p4files:
            change_num = int(f['change'])
            file_path  = f['clientFile']
            as_file[file_path] = max(as_file[file_path], change_num)
            for dir_path in p4gf_path.dir_path_iter(file_path):
                as_dir[dir_path] = max(as_dir[dir_path], change_num)

        result = []
        for f in p4files:
                        # Remove files more recently used as directories.
            file_path = f['clientFile']
            if as_file[file_path] <= as_dir[file_path]:
                continue
                        # Remove files whose ancestor directory
                        # was more recently used as a file.
            keep = True
            for dir_path in p4gf_path.dir_path_iter(file_path):
                if as_dir[dir_path] <= as_file[dir_path]:
                    keep = False
                    break
            if not keep:
                continue
            result.append(f)
        return result

    def head_change(self, ctx):
        """Run 'p4 changes -m1 //client/...' and return the first change dict."""
        with ctx.switched_to_branch(self):
            r = ctx.p4run('changes', '-m1', ctx.client_view_path())
            return p4gf_util.first_dict_with_key(r, 'change')

    def copy_rerooted(self, new_depot_info):
        """Return a new Branch object with a view like source_branch's, but with
        source_branch's LHS depot path roots changed from old_depot_info
        to new_depot_info.

        Assigns no branch_id. Do this yourself if you deem this branch worthy.
        """
        r = Branch.from_branch(self, None)
        r.set_depot_branch(new_depot_info)
        r.more_equal = False
        LOG.debug2('copy_rerooted() demoted {} ({})'.format(r.branch_id, r.git_branch_name))
        return r

    def sha1_for_branch(self):
        """Convenience wrapper for branch_name -> sha1."""
        if not self.git_branch_name:
            return None
        return p4gf_util.sha1_for_branch(self.git_branch_name)

    def depot_branch_id(self):
        """Deal with partially created depot_branch fields."""
        try:
            return self.depot_branch.depot_branch_id
        except AttributeError:
                        # field isn't a DepotBranchInfo. Probably a string
                        # depot branch ID, or a None if we're fully populated.
            return self.depot_branch

    @property
    def fp_depot_root(self):
        """If specified in config file, return that.
        If not, return the greatest common depot path.

        For lightweight branches, return None.

        Returned string omits trailing delimiter.
        """
        if self.is_lightweight:
            return None

                        # Fully populated branch? Return whatever we read from
                        # p4gf_config or received from our setter.
        if self._fp_depot_root is not None:
            return self._fp_depot_root

                        # Fully populated branch with no assigned depot_root?
                        # Attempt to guess at a greatest- common-path root, but
                        # this guess will return a uselessly imprecise root
                        # such as "//" or "//depot" if our branch view imports
                        # from other directories or depots.
        gcd = p4gf_path.greatest_common_dir(
                self._lhs_gcd_iter())
        return p4gf_path.strip_trailing_delimiter(gcd)

    @fp_depot_root.setter
    def fp_depot_root(self, value):
        """Tell this branch where all of its (non-shared/imported) files go.

        NOP for lightweight branches: we use a full DepotBranchInfo for that.
        """
        if self.is_lightweight:
            return

        self._fp_depot_root = p4gf_path.strip_trailing_delimiter(value)

    def _lhs_gcd_iter(self):
        """Iterator/generator for mapping lines that contribute to our
        branch view mapping's greatest-common-directory path.

        Dequote (golly those quotes are annoying), and skip over
        any exclusionary/minus-mapping paths, since they don't contribute
        to GCD and are often root-level things like "-//....txt".

        Strip off any leading overlay/plus-mapping "+" prefix.
        Still include overlay lines: they contribute to GCD.
        """
        for l in self.view_p4map.lhs():
            dq = p4gf_path.dequote(l)
                        # Exclusionary/minus-mappped lines don't
                        # contribute to GCD. Skip.
            if len(dq) and dq[0] == '-':
                continue
                        # Overlay/plus-mapped lines do contribute.
                        # Include, but get that plus sign out of the way.
            elif len(dq) and dq[0] == '+':
                yield dq[1:]
                        # Normal mapping lines contribute to GCD.
            else:
                yield dq

    def is_swarm_review(self):
        '''
        Does our git-branch-name match what we use for Git-Swarm reviews?
        '''
        return (    self.git_branch_name
                and self.git_branch_name.startswith(
                        p4gf_const.P4GF_GIT_SWARM_REF_PREFIX_SHORT))

    def is_worthy_of_config2(self):
        """lightweight and stream branches both get written to config2."""
        return self.is_lightweight or self.stream_name

# -- end class Branch ---------------------------------------------------------


class BranchDict(dict):

    """Mapping of Git to Perforce branch associations."""

    def __init__(self):
        """Initialize the BranchDict instance."""
        super(BranchDict, self).__init__()

    def __getitem__(self, key):
        """Retrieve the named branch from the dictionary."""
        result = super(BranchDict, self).__getitem__(key)
        if result is None:
            LOG.warning(_("branch not found in dictionary: {name}").format(name=key))
        return result


def _depot_root(depot_branch_info):
    """Return the root portion of the depot paths in a Branch view's RHS.

    Include trailing delimiter for easier str.startswith()/replace() work.
    """
    if depot_branch_info:
        return depot_branch_info.root_depot_path + '/'
    else:
        return '//'


def depot_branch_to_branch_view_list(branch_dict, depot_branch):
    """Return all known Branch view instances that use depot_branch to store
    their data.
    """
    return [branch for branch in branch_dict.values()
            if branch.depot_branch == depot_branch]


def from_dict(dikt, client_name):
    """Create a branch dictionary from the given dict, as from to_dict().

    :param str client_name: used to set rhs of view mapping.

    """
    results = BranchDict()
    for props in dikt.values():
        b = Branch.from_dict(props, client_name)
        results[b.branch_id] = b
    return results


def to_dict(branch_dict):
    """Convert the given branch dictionary into a plain dict, for JSON serialization."""
    return {brid: branch.to_dict() for brid, branch in branch_dict.items()}


def dict_from_config(config, p4=None):
    """Factory to return a new dict of branch_id ==> Branch instance,
    one for each branch defined in config.

    Have a Context handy? Then use Context.branch_dict():
    you already have this branch dict and do not need a new one.

    This is the simple "just load 'em into a dict, don't think about 'em"
    version. See configs_to_branch_dict() for the full-blown loader.
    """
    results = BranchDict()
    for section in p4gf_config.branch_section_list(config):
        b = Branch.from_config(config, section, p4)
        if b:
            results[b.branch_id] = b
    return results


def _find_first_nondeleted_branch(branch_dict, bsl):
    """Find the first non-deleted branch in the list.

    :type branch_dict: dict
    :param branch_dict: branch dictionary as from dict_from_config()

    :type bsl: list
    :param bsl: list of branch identifiers (from config section names)

    :return: most-equal branch or None of all branches deleted

    """
    for name in bsl:
        branch = branch_dict[BranchId(name)]
        if not branch.deleted:
            return branch
    return None


def config_to_branch_dict(*, config, rhs_client_name, p4=None, dbi_index=None):
    """Given a RepoConfig containing two ConfigParser instances, loaded from
    p4gf_config and p4gf_config2, load them into a single branch dict, setting
    more_equal and is_lightweight and merging any config2 stream views into
    their initial config1 instance.

    This is the full-blown loader. See also dict_from_config() for the
    simpler "just load 'em into a dict" function.

    Refactored from Context.branch_dict().

    :param config:      RepoConfig loaded from p4gf_config and p4gf_config2
    :param rhs_client_name:
                        Perforce client name inserted into each branch
                          view's rhs, changing "..." to "//myclient/..."
    :param p4:          Connection to Perforce. Used to read a stream-based
                          branch's stream view.
                          Required iff any branch is based on a stream.
    :param dbi_index:   DepotBranchInfoIndex instance to use when encountering
                          a Branch instance with a string dbid instead
                          of a full DepotBranchInfo instance.
                          Optional if you don't need to inflate partial
                          DBI fields.
    """
    # pylint:disable=too-many-branches
    assert p4gf_config
    assert rhs_client_name

    LOG.debug('config_to_branch_dict() client %s, dbi %s', rhs_client_name, dbi_index)
    if LOG.isEnabledFor(logging.DEBUG3):
        # These files can be very large, only print them at debug3.
        LOG.debug3('config1 %s', p4gf_config.to_text("", config.repo_config))
        if config.repo_config2.sections():
            LOG.debug3('config2 %s', p4gf_config.to_text("", config.repo_config2))
        else:
            LOG.debug3('config2 None')
    branch_dict = dict_from_config(config.repo_config, p4)
    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug('branch_dict %s', p4gf_util.dict_to_log(branch_dict))
    branch_names_non_lw = {b.git_branch_name: b.branch_id
                           for b in branch_dict.values()
                           if b.git_branch_name and not b.deleted}

                        # First branch listed in p4gf_config becomes our
                        # default HEAD. This is usually 'master', but not
                        # required.
    bsl = p4gf_config.branch_section_list(config.repo_config)
    LOG.debug('bsl {}'.format(bsl))
    if bsl:
        branch = _find_first_nondeleted_branch(branch_dict, bsl)
        branch.more_equal = True
        LOG.debug2('config_to_branch_dict() promoted {} ({})'.format(
            branch.branch_id, branch.git_branch_name))

                        # Load the lightweight and stream-based branch config
                        # data into the branch_dict. This is stored in
                        # p4gf_config2. For lightweight branches, the full
                        # branch def is there. For stream-based branches all
                        # we care about is the original-view, which gets merged
                        # with any config stored in p4gf_config.
    if config.repo_config2.sections():
        branch_dict2 = dict_from_config(config.repo_config2, p4)
        lwb_dict = {}
        for branch in branch_dict2.values():
            if branch.stream_name:
                if branch.branch_id in branch_dict:
                    branch_dict[branch.branch_id].original_view_lines = \
                        branch.original_view_lines
            else:
                # Disallow non-deleted fully populated and non-deleted LW branch of the same name
                if branch.git_branch_name in branch_names_non_lw and not branch.deleted:
                    raise RuntimeError(_("Perforce: repository configuration section [{section}] "
                                         "has git_branch_name '{git_branch_name}' which already "
                                         "exists as a lightweight git-branch-name.\n"
                                         "Contact your adminstrator.")
                                       .format(section=branch_names_non_lw[branch.git_branch_name],
                                               git_branch_name=branch.git_branch_name))
                branch.is_lightweight = True
                if (    branch.depot_branch
                    and isinstance(branch.depot_branch, str)
                    and dbi_index):
                    branch.depot_branch = dbi_index \
                        .find_depot_branch_id(branch.depot_branch)
                lwb_dict[branch.branch_id] = branch
        branch_dict.update(lwb_dict)

    for b in branch_dict.values():
        b.set_rhs_client(rhs_client_name)

    return branch_dict


def attach_depot_branch_info(branch_dict, dbi_index):
    """Attach the depot branch info to the known branches.

    :param branch_dict: existing branch dictionary
    :param dbi_index: depot branch info data

    If any of the branches has a `depot_branch` that is a string, find the
    corresponding depot branch info and replace `depot_branch` with the
    full DepotBranchInfo instance.

    """
    for branch in branch_dict.values():
        if not branch.stream_name and branch.depot_branch and isinstance(branch.depot_branch, str):
            branch.depot_branch = dbi_index.find_depot_branch_id(branch.depot_branch)


def _branch_view_union_tuples_one(branch, client_name):
    """Return one branch's view as a list of MapTuple left/right tuples."""
    if not branch.view_lines:
        LOG.error("Empty branch view {}".format(repr(branch)))
        raise RuntimeError(_("Branch view must not be empty"))
    branch_p4map = P4.Map()
    for line in branch.view_lines:
        # Skip exclusion lines.
        if line.startswith('-') or line.startswith('"-'):
            continue
        # Flatten overlay lines, remove leading +
        if line.startswith('+'):
            line = line[1:]
        elif line.startswith('"+'):
            line = '"' + line[2:]

        branch_p4map.insert(line)

    # Replace branch view's RHS (client side) with a copy of its LHS
    # (depot side) so that each depot path "//depot/foo" maps to a client
    # path "depot/foo". This new RHS allows us un-exclude
    # P4.Map-generated minus/exclusion lines that P4.Map had to insert
    # into branch_p4map when multiple LHS collided on the same RHS.
    lhs = branch_p4map.lhs()
    rhs = p4gf_util.map_lhs_to_relative_rhs_list(lhs)

    rhs_prefix = '//{}/'.format(client_name)
    tuple_list = []
    for (ll, rr) in zip(lhs, rhs):
        if ll.startswith('-') or ll.startswith('"-'):
            continue
        if rr[0] == '"':
            tuple_list.append(MapTuple(ll, '"' + rhs_prefix + rr[1:]))
        else:
            tuple_list.append(MapTuple(ll, rhs_prefix + rr))
    if not tuple_list:
        LOG.error("Empty branch view {}".format(repr(branch)))
        raise RuntimeError(_("Branch view must contain at least one non-exclusionary line."))
    return tuple_list


def _branch_view_union_tuple_list(client_name, branches):
    """Return a P4.Map object that contains the union of the branch views
    defined in a list of branches.

    Exclusion lines from the config file are NOT included in this P4.Map: you
    cannot easily add those to a multi-view P4.Map without unintentionally
    excluding valid files from previous views.

    RHS of view map is programmatically generated nonsense.

    Returned P4.Map _will_ include exclusion lines. These are inserted by
    P4.Map itself as overlapping views are layered on top of each other.
    That's okay.
    """
    tuple_list = []
    for br in branches:
        tuple_list.extend(_branch_view_union_tuples_one(br, client_name))
    return tuple_list


def _is_read_only_from_config(branch_id, repo_config):
    """Ask the config file if this branch is read-only.

    Bypass Branch.is_read_only data member.

    Does not check repo-wide read-only. Do that yourself if you want.
    """
    return repo_config.getboolean( branch_id
                                 , p4gf_config.KEY_READ_ONLY
                                 , fallback=False
                                 )


def writable_branch_list(branch_dict, repo_config):
    """Return a list of Branch elements that are writable.

    :param repo_config: we ask the config whether the branch is read-only or not.
    """
    return [br for br in branch_dict.values()
            if not _is_read_only_from_config(br.branch_id, repo_config)]


def _writable_branch_view_union_tuple_list(client_name, branch_dict, repo_config):
    """Return a P4.Map object that contains the union of ALL non-read-only
    branch views defined in branch_dict.

    Exclusion lines from the config file are NOT included in this P4.Map: you
    cannot easily add those to a multi-view P4.Map without unintentionally
    excluding valid files from previous views.

    RHS of view map is programmatically generated nonsense.
    """
    tuple_list = []
    for br in branch_dict.values():
        if _is_read_only_from_config(br.branch_id, repo_config):
            continue
        tuple_list.extend(_branch_view_union_tuples_one(br, client_name))
    return tuple_list


def calc_branch_union_tuple_list(client_name, branches):
    """Do most of the prep work for loading a "union of branches" into a
    client view map, without actually changing the client spec or anything
    else.

    Calculate a view that maps in all of the listed branches,
    and set that view's RHS to use the given client spec name.

    Return a list of MapTuple.
    """
    return _branch_view_union_tuple_list(client_name, branches)


def calc_writable_branch_union_tuple_list(client_name, branch_dict, repo_config):
    """Do most of the prep work for loading a "union of all branches" into a
    client view map, without actually changing the client spec or anything
    else.

    Calculate a view that maps in all of the NON_READ_ONLY branches in branch_dict,
    and set that view's RHS to use the given client spec name.

    Return result as a list of MapTuple
    """
    return _writable_branch_view_union_tuple_list(client_name, branch_dict, repo_config)


def convert_view_from_no_client_name(view, new_client_name):
    """Convert a view mapping's right-hand-side from its original client
    name to a new client name:

        //depot/dir/...  dir/...
        //depot/durr/... durr/...

    becomes

        //depot/dir/...  //client/dir/...
        //depot/durr/... //client/durr/...

    Accepts view as P4.Map, str. or list.
    Returns view as P4.Map().
    """
    if isinstance(view, P4.Map):
        old_map = view
    elif isinstance(view, str):
        view_lines = view.splitlines()
        old_map = P4.Map(view_lines)
    else:
        view_lines = view
        old_map = P4.Map(view_lines)

    lhs = old_map.lhs()
    new_prefix = '//{}/'.format(new_client_name)
    rhs = [new_prefix + p4gf_path.dequote(r) for r in old_map.rhs()]
    new_map = P4.Map()
    for (l, r) in zip(lhs, rhs):
        new_map.insert(l, r)

    return new_map


def replace_client_name(view, old_client_name, new_client_name):
    """Convert "//depot/... //old_client/..." to "//depot/... //new_client".

    Accepts view as P4.Map, str. or list.
    Returns view as P4.Map().
    """
    if isinstance(view, P4.Map):
        old_map = view
    elif isinstance(view, str):
        view_lines = view.splitlines()
        old_map = P4.Map(view_lines)
    else:
        view_lines = view
        old_map = P4.Map(view_lines)

    lhs = old_map.lhs()
    new_prefix = '//{}/'.format(new_client_name)
    old_prefix = '//{}/'.format(old_client_name)
    old_len    = len(old_prefix)
    rhs = [new_prefix + p4gf_path.dequote(r)[old_len:] for r in old_map.rhs()]
    new_map = P4.Map()
    for (l, r) in zip(lhs, rhs):
        new_map.insert(l, r)

    return new_map


def iter_fp_non_deleted(branch_dict):
    """Iterate through all the non-deleted fully populated branch definitions."""
    for branch in branch_dict.values():
        if not branch.is_lightweight and not branch.deleted:
            yield branch


def define_branch_views_for(ctx, depot_branch):
    """Given a depot branch that is not yet mapped into any known Branch view,
    create zero or more Branch views that map this depot_branch into the repo.

    Returns up to one Branch view per fully populated branch. Typically returns
    only one Branch view total unless you have overlapping fully populated
    branch views, or the Depot branch's first changelist holds files that
    straddle multiple locations in the depot.

    Can return empty list if unable to map this Depot branch into the repo,  in
    which case you should shun this Depot branch. Shun this depot branch. Shun
    the mapping of this depot branch. Shun everything, and then shun shunning.

    Returns with any new Branches already assigned branch_ids and inserted into
    ctx.branch_dict().
    """

    # What files does this branch hold? We'll use them
    # to find intersecting fully populated branches.
    depot_root = depot_branch.root_depot_path
    r = ctx.p4run('files', '{}/...'.format(depot_root))
    depot_file_list = [x['depotFile'] for x in r
                       if isinstance(x, dict) and 'depotFile' in x]

    fully_populated_branch_list = [br for br in ctx.branch_dict().values()
                                   if not br.is_lightweight]

    result_list = []
    for br in fully_populated_branch_list:
        br_rerooted = br.copy_rerooted(depot_branch)
        if br_rerooted.intersects_depot_file_list(depot_file_list):
            br_rerooted.branch_id        = p4gf_util.uuid(ctx.p4gf)
            br_rerooted.git_branch_name  = None
            br_rerooted.is_lightweight   = True
            br_rerooted.populated        = True
            br_rerooted.depot_branch     = depot_branch
            ctx.branch_dict()[br_rerooted.branch_id] = br_rerooted
            result_list.append(br_rerooted)

    return result_list


def abbrev(branch):
    """Return first 7 char of branch ID, or "None" if None."""
    if isinstance(branch, Branch):
        return p4gf_util.abbrev(branch.branch_id)
    return p4gf_util.abbrev(branch)


def most_equal(branch_dict):
    """Return the Branch definition that was listed first in p4gf_config."""
    for b in branch_dict.values():
        if b.more_equal:
            return b
    return None


def ordered(branch_list):
    """Return a list of branches, in some reproducible order.

    Don't rely on this order, it's mostly to make debugging
    and displays easier to follow across multiple runs or dumps.
    """
                        # Be permissive in what we accept as input.
                        # Accept a dict, operate on its values.
    if isinstance(branch_list, dict):
        return ordered(branch_list.values())

    masterish = []      # list of 1 element, or 0 if no masterish
    fp_list   = []
    lw_list   = []
    for b in branch_list:
        if b.more_equal:
                        # Should only be 1 masterish, but  don't let that
                        # invariant violation break a debugging dump.
            masterish.append(b)
        elif b.is_lightweight:
            lw_list.append(b)
        else:
            fp_list.append(b)

    fp_list = sorted(fp_list, key=operator.attrgetter("branch_id"))
    lw_list = sorted(lw_list, key=operator.attrgetter("branch_id"))
    return masterish + fp_list + lw_list


# Branch ID generator functions. Pick one for your testing or release needs.
# Release should use GUIDs so there's never a collision.
# Humans debugging should use sha1.
# Test scripts should use counter.
def _guid(p4):
    """Branch ID generator that returns GUIDs.

    GUIDs never recur, even across multiple Git Fusion servers, so there's
    never a chance for a collision (which would be bad).
    """
    return p4gf_util.uuid(p4) if p4 else None

_counter_curr = 0


def _counter(_p4_unused):
    """Branch ID generator that returns anon-0000001.

    Incrementing counter should produce the same branch IDs for consecutive
    runs, which makes test scripts easier to write (but still brittle).
    """
    global _counter_curr
    _counter_curr += 1
    return _('anon-{}').format(_counter_curr)

# Create anonymous branch names like "anon-123abc7" instead of the usual GUID?
# WARNING: sha1-based branch names are not unique and don't actually work.
# But they sure make debugging easier. ###
_anon_branch_func = _guid


def use_guid_branch_ids():
    """Switch to using GUID branch ID generator.

    For testing only.
    """
    global _anon_branch_func
    _anon_branch_func = _guid


def use_consecutive_branch_ids():
    """Switch to using consecutive branch ID generator.

    For testing only.
    """
    global _anon_branch_func
    _anon_branch_func = _counter


def _create_anon_branch_id(p4):
    """Generate a new branch_id for an anonymous branch."""
    return _anon_branch_func(p4)


def _canonical_key(line):
    """Ignore quotes and leading minus sign for sorting."""
    lhs = p4gf_util.p4map_split(line)[0]
    if lhs.startswith('-'):
        return lhs[1:]
    return lhs


def _canonical_sort(orig_lines):
    """Sort a sequence of view mapping lines."""
    return sorted(orig_lines, key=_canonical_key)


def canonical_view_lines(orig_lines):
    """Sort adjacent exclusion (minus) lines.

    Order of multiple adjacent exclusion (minus) lines is not significant. 'p4
    stream -ov' returns such blocks in different order (usually [1,2,3] or
    [3,2,1]). Sort each block of multiple adjacent exclusion lines.
    """
    result = []
    exclude_block = []
    for line in orig_lines:
        if line.startswith("-") or line.startswith('"-'):
            exclude_block.append(line)
        else:
            if exclude_block:
                result.extend(_canonical_sort(exclude_block))
                exclude_block = []
            result.append(line)
    if exclude_block:
        result.extend(_canonical_sort(exclude_block))
    return result


def main():
    """Print the list of branches to stdout for testing purposes."""
    p4gf_util.has_server_id_or_exit()
    p4 = p4gf_create_p4.create_p4_temp_client()
    if not p4:
        sys.exit(1)
    desc = _("Display the list of branches for a given repo.")
    epilog = _("Primarily used by the test scripts and unsupported.")
    parser = p4gf_util.create_arg_parser(desc=desc, epilog=epilog)
    parser.add_argument(NTR('repo'), metavar=NTR('R'),
                        help=_('name of the repository'))
    args = parser.parse_args()
    if args.repo:
        init_case_handling(p4)
        repo_name = p4gf_translate.TranslateReponame.git_to_repo(args.repo)
        repo_config = p4gf_config.RepoConfig.from_depot_file(repo_name, p4)
        branch_dict = config_to_branch_dict(
            config=repo_config,
            p4=p4, rhs_client_name='test')
        blaster = most_equal(branch_dict)
        branch_dict.pop(blaster.branch_id)
        # print the master branch to make testing easier
        deleted = '-' if blaster.deleted else '+'
        print("{} {} {}".format(deleted, blaster.branch_id, blaster.git_branch_name))
        # now print the rest of the branches
        for key in sorted(branch_dict.keys()):
            branch = branch_dict[key]
            deleted = '-' if branch.deleted else '+'
            print("{} {} {}".format(deleted, branch.branch_id, branch.git_branch_name))


if __name__ == "__main__":
    main()

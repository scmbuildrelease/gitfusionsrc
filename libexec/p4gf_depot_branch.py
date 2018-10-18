#! /usr/bin/env python3.3
"""A branch of depot hierarchy in which a lightweight branch stores its files."""
import logging
from   collections import namedtuple
import configparser
import copy
import re

import P4

import p4gf_config
import p4gf_const
from   p4gf_l10n    import NTR
import p4gf_util

# Section [{depot branch id}]
KEY_ROOT_DEPOT_PATH     = NTR('root-depot-path')
KEY_PARENT_BRANCH_ID    = NTR('parent-branch-id')
KEY_PARENT_CHANGELIST   = NTR('parent-changelist')
KEY_PARENT_BRANCH_ID_N  = NTR('parent-{}-branch-id')
KEY_PARENT_CHANGELIST_N = NTR('parent-{}-changelist')
KEY_BASIS_CHANGE_NUM    = NTR('basis-change-num')
KEY_BASIS_MAP_LINES     = NTR('basis-map')

LOG = logging.getLogger(__name__)


class DepotBranchInfo:

    """Describe a branch of depot hierarchy.

    This is the same data that we store in a depot branch's branch-info file.
    """

    def __init__(self, dbid, p4=None):
        """Init with id and optional p4.

        If p4 is passed, DepotBranchInfo will lazy load when other attributes
        are referenced; otherwise, other attributes will be initialized with
        default values.

        Lazy loading works by overriding __getattr__(), which will be called
        when any undefined attribute is referenced.
        """
                # Depot branch ID is separate from any lightweight branch
                # mapping that uses this depot branch. Because a single depot
                # branch might be mapped to multiple Git branches, a single
                # Git branch might map to multiple depot branches. Highly
                # unlikely at first, but after a few repo refactors, it could
                # happen.
        self.depot_branch_id        = dbid

        if p4:
            self.p4 = p4
        else:
                # A single path without the trailing "/...".
            self.root_depot_path        = None

                # Matching lists of depot branch/changelist values
                # that tell us from where to JIT-branch files later.
                #
                # Usually only a single value, but it is possible to create
                # a Git branch whose first commit has mulitiple parents.
                #
                # Can be empty lists for orphan branches.
            self.parent_depot_branch_id_list = []
            self.parent_changelist_list      = []

                # Our JIT basis
                #
                # New depot branch created during 'git push'? These two fields
                # are left blank (None) during preflight time when our parents
                # are likely to be ":mark" strings instead of submitted
                # changelist numbers. Lazy-filled in later during copy phase.
                #
            self.fp_basis_known              = False
            self.fp_basis_change_num         = None
                #
                # P4.Map.as_array() list of lines mapping from fully
                # populated Perforce to this depot branch.
                #
            self.fp_basis_map_line_list      = None

                # Is this structure new, not yet written to a branch-info file?
                # Needs to be 'p4 add'ed.
                # Was needs_write
            self.needs_p4add                 = False

                # Is this structure old, but has modifications that need
                # to be 'p4 edit'ed into to its existing branch-info file?
            self.needs_p4edit                = False

    def __getattr__(self, name):
        """Do lazy load when any new attribute is requested.

        If DepotBranchInfo was created with a p4, and it has not yet been lazy
        loaded, do so now.  Then, remove the p4 attribute to avoid repeated
        lazy loading.
        """
        if 'p4' in self.__dict__:
            self._lazy_load()
        if name not in self.__dict__:
            raise AttributeError("type object '{}' has no attribute '{}'".format(
                self.__class__, name))
        return getattr(self, name)

    def _lazy_load(self):
        """Do lazy load: p4 print the dbi file and initialize with its contents.

        This will be called once by __getattr__() the first time an
        uninitialized attribute is referenced.
        """
        depot_path = self.to_config_depot_path()
        dbi_list = DepotBranchInfo._print_to_dbi_list(self.p4, depot_path)
        if 1 != len(dbi_list):
            raise RuntimeError("BUG: expect exactly 1 dbi file for {dbid},"
                               " path {depot_path}. Got {ct}"
                               .format( dbid       = self.depot_branch_id
                                      , depot_path = depot_path
                                      , ct         = len(dbi_list) ))
        LOG.debug2("lazy_load {}".format(dbi_list[0]))
        self.__dict__.update(dbi_list[0].__dict__)
        del self.p4

    @staticmethod
    def from_dbid_incomplete(dbid, p4):
        """Factory for a partial/incomplete DepotBranchInfo instance.

        Incomplete instance knows only its depot_branch_id and has a p4
        connection to be able to do lazy load at a later time.

        The vast majority of depot branches are irrelevant to any single push
        or pull, so there's no reason to 'p4 print' and parse their DBI file.
        This saves a LOT of time for small pushes into Perforce servers with
        thousands of lightweight branches.
        """
        return DepotBranchInfo(dbid, p4)

    def __repr__(self):
        s = "depot branch id={} root={}".format(self.depot_branch_id, self.root_depot_path)
        if self.parent_depot_branch_id_list:
            l = ' '.join(["{}@{}".format(br, cl)
                          for br, cl in zip( self.parent_depot_branch_id_list
                                           , self.parent_changelist_list     )])
            s += "; " + l
        return s

    def dump(self):
        """Multi-line string with all our fields."""
        l = []
        l.append("  depot_branch_id             : {}".format(self.depot_branch_id) )
        if 'p4' in self.__dict__:
            l.append("  (needs lazy load)")
        else:
            l.append("  root_depot_path             : {}".format(self.root_depot_path) )
            l.append("  parent_depot_branch_id_list : {}".format(self.parent_depot_branch_id_list) )
            l.append("  parent_changelist_list      : {}".format(self.parent_changelist_list) )
            l.append("  fp_basis_known              : {}".format(self.fp_basis_known) )
            l.append("  fp_basis_change_num         : {}".format(self.fp_basis_change_num) )
            l.append("  fp_basis_map_line_list      : {}".format(self.fp_basis_map_line_list) )
            l.append("  needs_p4add                 : {}".format(self.needs_p4add) )
            l.append("  needs_p4edit                : {}".format(self.needs_p4edit) )
        return "\n".join(l)

    def to_config(self):
        """Return a new ConfigParser object with our data."""
        config = configparser.ConfigParser( interpolation  = None
                                          , allow_no_value = True)
        section = self.depot_branch_id
        config.add_section(section)
        config[section][KEY_ROOT_DEPOT_PATH] = self.root_depot_path
        if not self.parent_depot_branch_id_list:
            return config

        # First parent doesn't need an index. Most depot branches have only
        # one parent depot branch and it's silly to pollute the world with
        # unnecessary ordinals.
        config[section][KEY_PARENT_BRANCH_ID ] = str(self.parent_depot_branch_id_list[0])
        config[section][KEY_PARENT_CHANGELIST] = str(self.parent_changelist_list[0])

        # Second-and-later parents. Rare, but write 'em out with
        # numbers in their keys.
        for i in range(1, len(self.parent_depot_branch_id_list)):
            key_id = KEY_PARENT_BRANCH_ID_N .format(1+i)
            key_cl = KEY_PARENT_CHANGELIST_N.format(1+i)
            val_id = str(self.parent_depot_branch_id_list[i])
            val_cl = str(self.parent_changelist_list     [i])
            config[section][key_id] = val_id
            config[section][key_cl] = val_cl

        if self.fp_basis_known:
            config[section][KEY_BASIS_CHANGE_NUM] = str(self.fp_basis_change_num)
            if self.fp_basis_map_line_list:
                config[section][KEY_BASIS_MAP_LINES] \
                    = "\n".join(self.fp_basis_map_line_list)
            else:
                config[section][KEY_BASIS_MAP_LINES] = ""

        return config

    def to_config_depot_path(self):
        """Return the depot path to our branch-info file where we store our data."""
        root = p4gf_const.P4GF_DEPOT_BRANCH_INFO_ROOT.format(
            P4GF_DEPOT=p4gf_const.P4GF_DEPOT)
        return root + '/' + p4gf_util.enslash(self.depot_branch_id)

    def contains_depot_file(self, depot_file):
        """Does this Depot branch root hold depot_file?"""
        return depot_file.startswith(self.root_depot_path + '/')

    @staticmethod
    def _print_to_dbi_list(p4, depot_path):
        """Print the given path, and return a list of all the dbi files
        it printed, as a list of dbi instances.
        """
        dbi_list = []
        with p4gf_util.raw_encoding(p4):
            file_data = p4.run('print', depot_path)
        delete = False
        started = False
        file_contents = ''
        for item in file_data:
            if isinstance(item, dict):
                LOG.debug2('depot_branch_info_index() print item details: {}'.format(item))
                if started and file_contents:  # finish with the current branch info
                    dbi = depot_branch_info_from_string(file_contents)
                    if dbi:
                        dbi_list.append(dbi)
                if item['action'] == 'delete':
                    started = False
                    delete = True
                else:
                    file_contents = ''
                    delete = False
                    started = True
            else:
                if delete:
                    continue
                new_item = item.decode().strip()
                if len(new_item):
                    file_contents = file_contents + new_item

        if started and file_contents:
            dbi = depot_branch_info_from_string(file_contents)
            if dbi:
                dbi_list.append(dbi)
        return dbi_list

    @staticmethod
    def from_dict(dikt):
        """Create a DepotBranchInfo instance from a dict, as from to_dict()."""
        result = DepotBranchInfo(dikt['depot_branch_id'])
        for k, v in dikt.items():
            setattr(result, k, v)
        return result

    def to_dict(self):
        """Produce a dict of this object, suitable for JSON serialization."""
        result = dict()
        result['depot_branch_id'] = self.depot_branch_id
        result['root_depot_path'] = self.root_depot_path
        result['parent_depot_branch_id_list'] = self.parent_depot_branch_id_list
        result['parent_changelist_list'] = self.parent_changelist_list
        result['fp_basis_known'] = self.fp_basis_known
        result['fp_basis_change_num'] = self.fp_basis_change_num
        result['fp_basis_map_line_list'] = self.fp_basis_map_line_list
        result['needs_p4add'] = self.needs_p4add
        result['needs_p4edit'] = self.needs_p4edit
        return result


# -----------------------------------------------------------------------------

def abbrev(dbi):
    """Return first 7 char of branch ID, or "None" if None."""
    if isinstance(dbi, DepotBranchInfo):
        return p4gf_util.abbrev(dbi.depot_branch_id)
    return p4gf_util.abbrev(dbi)


def new_definition(repo_name, *, p4=None, dbid=None):
    """Factory method to generate an return a new depot branch definition.

    :param p4:   Used only for generating counter-driven sequential UUIDs.
                 Optional.

    :param dbid: If you already have a Depot Branch ID that you want to use,
                 supply it here. or leave None and we'll generate one for you.
                 Optional.
    """
    dbi = DepotBranchInfo(dbid or p4gf_util.uuid(p4))
    dbi.root_depot_path = new_depot_branch_root(
                              depot_branch_id = dbi.depot_branch_id
                            , repo_name       = repo_name )
    dbi.needs_p4add = True
    return dbi


def new_depot_branch_root(depot_branch_id, repo_name):
    """Return a path to a new root of depot hierarchy where a lightweight
    branch can store future files.
    """
    template    = p4gf_const.P4GF_DEPOT_BRANCH_ROOT
    slashed_id  = p4gf_util.enslash(depot_branch_id)
    return template.format( P4GF_DEPOT = p4gf_const.P4GF_DEPOT
                          , repo_name  = repo_name
                          , branch_id  = slashed_id )


def depot_branch_info_from_string(dbi_string):
    """Return DepotBranchInfo from string (from file contents).

    :param dbi_string: the content to be parsed as a config file.

    Returns the depot branch info object, or None if the content produced
    no configuration values (i.e. ConfigParser.sections() yielded nothing).

    """
    config = configparser.ConfigParser( interpolation  = None
                                      , allow_no_value = True )
    config.read_string(dbi_string)
    # detect if the file contained actual values
    if len(config.sections()) > 0:
        dbi = depot_branch_info_from_config(config)
    else:
        dbi = None
    p4gf_config.clean_up_parser(config)
    if dbi and not dbi.root_depot_path:
        LOG.debug('error parsing DepotBranchInfo\ncontent={}\nresult={}'.format(
            dbi_string, dbi.dump()))
    return dbi


def _dbid_section(config):
    """Return the ConfigParser section that is this Depot Branch's ID."""
    for sec in config.sections():
        if sec:
            return sec
    return None


def depot_branch_info_from_config(config):
    """ Return DepotBranchInfo from configparser object."""
    dbi = DepotBranchInfo(_dbid_section(config))
    dbi.root_depot_path = config.get(dbi.depot_branch_id, KEY_ROOT_DEPOT_PATH)
    firstbranch         = None
    firstcl             = None
    branch              = []
    cl                  = []
    fp_basis_change_num     = None
    fp_basis_map_line_list  = None
    for option in config.options(dbi.depot_branch_id):
        value = config.get(dbi.depot_branch_id, option)
        if option == KEY_PARENT_BRANCH_ID:
            firstbranch = value
        elif option == KEY_PARENT_CHANGELIST:
            firstcl = value
        elif option == KEY_BASIS_CHANGE_NUM:
            fp_basis_change_num = value
        elif option == KEY_BASIS_MAP_LINES:
            fp_basis_map_line_list = p4gf_config.get_view_lines(
                                           config[dbi.depot_branch_id]
                                         , KEY_BASIS_MAP_LINES )
                        # We're being clever with parent lists here. variables
                        # branch and cl are lists of strings that include a
                        # numbered "parent -{}-branch-id/changelist:" prefix,
                        # so that the two lists will alphanumeric sort
                        # identically, keeping branch and cl in step with each
                        # other.
        elif option.endswith(NTR('branch-id')):
            branch.append(option + ':' + value)
        elif option.endswith(NTR('changelist')):
            cl.append(option + ':' + value)

    branch = p4gf_util.alpha_numeric_sort(branch)
    cl     = p4gf_util.alpha_numeric_sort(cl)

    if firstbranch and firstcl:
        dbi.parent_depot_branch_id_list.append(firstbranch)
        dbi.parent_changelist_list.append(firstcl)

    for i in range(len(branch)):
        dbi.parent_depot_branch_id_list.append(branch[i].split(':')[1])
        dbi.parent_changelist_list.append(cl[i].split(':')[1])

    if fp_basis_change_num is not None:
        dbi.fp_basis_known          = True
        dbi.fp_basis_change_num     = int(fp_basis_change_num)
        dbi.fp_basis_map_line_list  = fp_basis_map_line_list

    return dbi


# ----------------------------------------------------------------------------

class DepotBranchInfoIndex:

    """Hash id->info and root->info."""

    def __init__(self, ctx):
        self.by_id   = {}
        self.by_root = {}
        self.ctx     = ctx

    def add(self, depot_branch_info):
        """Add to our indices."""
        self.by_id[depot_branch_info.depot_branch_id] = depot_branch_info
        # don't access root_depot_path if this is an incompletely loaded
        # DepotBranchInfo since that would force it to fully load now
        if not hasattr(depot_branch_info, 'p4') and depot_branch_info.root_depot_path:
            self.by_root[depot_branch_info.root_depot_path] = depot_branch_info

    def find_depot_branch_id(self, depot_branch_id):
        """Seek."""
        d = self.by_id.get(depot_branch_id)
        return d

    def find_root_depot_path(self, depot_root):
        """Return DepotBranchInfo whose root exactly matches depot_root."""
        return self.by_root.get(depot_root)

    def find_depot_path(self, depot_path):
        """Return DepotBranchInfo whose root prefixes depot_path.

        Extracts dbid out of depot_path and uses that to find
        the matching DBI instance, if any.

        Lazy-loads the matching DBI instance if not yet loaded.
        """
        # Passed us just a root with no trailing delimiter? The loop below
        # won't find a match, but our dict lookup will (and quickly).
        r = self.find_root_depot_path(depot_path)
        if r:
            return r

        # Path not under our lw branch territory? Then it's not ours.
        dbi_root = "//{P4GF_DEPOT}/branches/" \
                    .format(P4GF_DEPOT = p4gf_const.P4GF_DEPOT)
        if not depot_path.startswith(dbi_root):
            return None

        # Strip off depot branch root and repo name.
        # Requires that repo name contain no slashes.
        # Requires that dbid be slashed using p4gf_util.enslash()
        # format: [0:2]/[2:4]/[4:]/user-path-elements
        s = depot_path[len(dbi_root):]
        m = re.search(r'[^/]+/([^/]{2})/([^/]{2})/([^/]+)/', s)
        if not m:
            return None

        dbid = m.group(1) + m.group(2) + m.group(3)
        dbi = self.by_id.get(dbid)
        if not dbi:
            return None

        return dbi

    def find_ancestor_change_num(self, child_dbi, ancestor_dbid):
        """If either child_dbi or one of its ancestors lists ancestor_dbi
        as a parent, return the changelist associated with ancestor_dbi.

        If no depot branch lists ancestor_dbi as a parent, return None.
        """
        seen         = {child_dbi.depot_branch_id}    # set
        parent_id_q  = copy.copy(child_dbi.parent_depot_branch_id_list)
        change_num_q = copy.copy(child_dbi.parent_changelist_list     )

        # Special case: None or 'None' acceptable spellings of
        # "fully populated Perforce"
        if ancestor_dbid in [None, 'None']:
            match_list = [None, 'None']
        else:
            match_list = [ancestor_dbid]

        while parent_id_q:
            parent_id  = parent_id_q. pop(0)
            change_num = change_num_q.pop(0)

            # Found a winner.
            if parent_id in match_list:
                return change_num

            # Add this parent's own parents to our list of ancestors to check.
            if parent_id not in seen:
                seen.add(parent_id)
                parent_dbi = self.find_depot_branch_id(parent_id)
                if parent_dbi:
                    parent_id_q .extend(parent_dbi.parent_depot_branch_id_list)
                    change_num_q.extend(parent_dbi.parent_changelist_list     )

        # Ran out of all ancestors without ever finding ancestor_dbid.
        return None

    def find_fully_populated_change_num(self, dbi):
        """Either this depot branch, or one of its first-parent ancestors, has
        "None" listed as a parent: that depot branch is based on a fully
        populated Perforce hierarchy, at some changelist. Return that
        changelist number.

        Either succeeds or returns None
        """
        change_num_dbi = self._find_fp_change_num_change_dbi(dbi)
        if change_num_dbi:
            return change_num_dbi.change_num
        return None

    def _find_fp_change_num_change_dbi(self, dbi):
        """Return a <change_num, child depot branch info> tuple that tells how
        we got to our fully populated basis.
        Return None if no FP basis.

        Return values:
         change_num : The changelist on fully populated Perforce.
         child_dbi  : The the CHILD depot branch that points to fully
                      populated Perforce. Returned so that callers can
                      use this child to discover which subset of
                      fully populated perforce was initially inherited
                      into lightweight branches that divered here.

        There's no point in returning the depot branch info for our
        fully populated basis: that's always "None".
        """
        # Special case: None or 'None' acceptable spellings of
        # "fully populated Perforce"
        match_list = [None, 'None']

        child_dbi = dbi
        while child_dbi:
                        # Ran out of first-parents.
            if not child_dbi.parent_depot_branch_id_list:
                break
                        # Found fully populated Perforce.
            first_parent_dbid = child_dbi.parent_depot_branch_id_list[0]

            if first_parent_dbid == child_dbi.depot_branch_id:
                LOG.error("_find_fp_change_num_change_dbi()"
                          " child lists itself as a parent!")
                break

            if first_parent_dbid in match_list:
                return ChangeNumDBI( change_num = child_dbi.parent_changelist_list[0]
                                   , child_dbi  = child_dbi )

                        # Iterate up to first-parent and try again.
            parent_dbi = self.find_depot_branch_id(first_parent_dbid)
            child_dbi  = parent_dbi

                        # Ran out of all ancestors without ever finding None?
                        # This is a rare but possible lightweight depot branch
                        # with no fully populated ancestor.
        return None

    def get_fully_populated_basis(self, dbi, branch_view_p4map):
        """Return a lightweight depot branch's fully populated basis:
        a changelist and a branch view that maps fully populated Perforce
        into this depot branch.

        The fully populated basis is the intersection
        of the branch mapping source @ changelist.

        Never returns None:  if there's no basis, returns FPBASIS_NONE
        This is expected and normal for root/orphan branches.

        CANNOT BE CALLED DURING GIT-PUSH PREFLIGHT:
        requires that all parent commits be submitted, so they have changelist
        numbers not git-fast-import ":mark" strings, for their own changelists.
        """
        assert dbi
        assert branch_view_p4map

                        # If we don't know it yet, now would be
                        # a good time to learn.
        if not dbi.fp_basis_known:
            fp_basis = self._calc_fully_populated_basis(dbi, branch_view_p4map)

                        # Cache this FPBasis for this dbi, and
                        # all of its ancestors that have not yet
                        # cached it.
            d = dbi
            while d and not d.fp_basis_known:
                map_line_list = reroot_rhs(
                                      map_line_list = fp_basis.map_line_list
                                    , old_rhs_root  = fp_basis.map_rhs_root
                                    , new_rhs_root  = d.root_depot_path )
                d.fp_basis_change_num    = fp_basis.change_num
                d.fp_basis_map_line_list = map_line_list
                d.fp_basis_known         = True

                        # Iterate up the first-parent chain.
                if not d.parent_depot_branch_id_list:
                    break
                d = self.find_depot_branch_id(d.parent_depot_branch_id_list[0])

        return FPBasis( change_num    = dbi.fp_basis_change_num
                      , map_line_list = dbi.fp_basis_map_line_list
                      , map_rhs_root  = dbi.root_depot_path )

    def _calc_fully_populated_basis(self, dbi, branch_view_p4map):
        """Return an FPBasis tuple that is this lightweight branch's
        divergence from fully populated Perforce.

        Searches up the first-parent line until it either finds an
        ancestor with an already-cached basis, or hits fully populated
        Perforce.

        Never returns None. Returns FPBASIS_NONE if no basis.
        """
        change_num_dbi = self._find_fp_change_num_change_dbi(dbi)
        if not (    change_num_dbi
                and change_num_dbi.change_num
                and change_num_dbi.child_dbi ):
            return FPBASIS_NONE

                        # What subset of fully populated Perforce is
                        # inherited into that first child depot branch?
                        #
                        # If first child has already cached a mapping, use that
                        # mapping, rerooted to our new depot root.
                        #
        child_dbi = change_num_dbi.child_dbi
        if child_dbi.fp_basis_known:
            map_line_list = reroot_rhs(
                      map_line_list = child_dbi.fp_basis_map_line_list
                    , old_rhs_root  = child_dbi.root_depot_path
                    , new_rhs_root  = dbi.root_depot_path )
            return FPBasis( change_num    = change_num_dbi.change_num
                          , map_line_list = map_line_list
                          , map_rhs_root  = dbi.root_depot_path )

                        # Use the current branch VIEW as the lens through which
                        # to view fully populated perforce. What subset of
                        # fully populated Perforce can be seen in the current
                        # branch view? That's the portion that is "inherited"
                        # into lightweight branches. That's the portion that we
                        # record in the fp_basis_map_line_list. That's
                        # represented in the LHS side, which we'll reroot to FP
                        # Perforce, and then  repeat on the RHS, rooted to our
                        # LW depot branch root.

                        # LHS is view, rerooted to FP Perforce.
        rerooter = P4.Map("//...", dbi.root_depot_path + "/...")
        rerooted = P4.Map.join(rerooter, branch_view_p4map)
                        # RHS is LHS, rerooted to our LW depot branch root.
        lhs = rerooted.lhs()
        rhs = p4gf_util.map_lhs_to_relative_rhs_list(lhs)
        m = P4.Map()
        for l, r in zip(lhs, rhs):
            m.insert(l, r)
        map_line_list = reroot_rhs( map_line_list = m.as_array()
                                  , old_rhs_root  = ""
                                  , new_rhs_root  = dbi.root_depot_path )
        return FPBasis( change_num    = change_num_dbi.change_num
                      , map_line_list = map_line_list
                      , map_rhs_root  = dbi.root_depot_path )

    def depot_file_list_to_depot_branch_list(self, depot_file_list):
        """Return a list of the few DepotBranchInfo objects whose roots contain
        the many given depot_file paths.
        """
        dfl = sorted(depot_file_list)  # Sorting increases chance that we'll
        last_dbi = None                # re-hit the same depot branch over and over

        dbi_set = set()

        for depot_file in dfl:
            # +++ No need to search if this depot_file is
            # +++ in the same branch as previous depot_file.
            if last_dbi and last_dbi.contains_depot_file(depot_file):
                continue
            dbi = self.find_depot_path(depot_file)
            if dbi:
                dbi_set.add(dbi)
                last_dbi = dbi
        return list(dbi_set)

    @staticmethod
    def from_p4_incomplete(ctx):
        """Factory to 'p4 files' all depot branch-info depot paths,
        use those to identify know DBI IDs, but leave the DBI instances
        pretty much empty until we later lazy_load() the one or two
        that we actually need. Saves a lot of 'p4 print' time.
        """
        dbi_index = DepotBranchInfoIndex(ctx)
        root = p4gf_const.P4GF_DEPOT_BRANCH_INFO_ROOT.format(
            P4GF_DEPOT=p4gf_const.P4GF_DEPOT)
        root_ddd = root + '/...'
        r = ctx.p4run('files', '-e', root_ddd)
        depot_paths = [rr["depotFile"]
                       for rr in r
                       if isinstance(rr, dict)]
        LOG.debug("from_p4_incomplete found {} dbi file paths."
                  .format(len(depot_paths)))
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3("\n".join(depot_paths))

        root_char_ct = len(root) + 1  # +1 for trailing slash
        for depot_path in depot_paths:
            # Strip root and slashes
            dbid = depot_path[root_char_ct:].replace("/","")
            dbi  = DepotBranchInfo.from_dbid_incomplete(dbid, ctx.p4gf)
            dbi_index.add(dbi)
        return dbi_index


# -- module-level ------------------------------------------------------------

def reroot_rhs(map_line_list, old_rhs_root, new_rhs_root):
    """Return a new list of map lines, replacing RHS prefix
    with a new prefix.
    """
                        # Permit missing views.
    if not map_line_list:
        return map_line_list
                        # +++ NOP if nothing to change.
    if old_rhs_root == new_rhs_root:
        return map_line_list

    orig = P4.Map(map_line_list)
    if old_rhs_root:
        rerooter = P4.Map("{old}/... {new}/..."
                          .format(old = old_rhs_root, new=new_rhs_root))
    else:
        rerooter = P4.Map("... {new}/..."
                          .format(new=new_rhs_root))
    rerooted = P4.Map.join(orig, rerooter)
    return rerooted.as_array()


# _find_fp_change_num_dbi() result.
ChangeNumDBI = namedtuple('ChangeNumDBI', ['change_num', 'child_dbi'])


# get_fully_populated_basis() result.
FPBasis = namedtuple('FPBasis', ['change_num', 'map_line_list', 'map_rhs_root'])


# We did the work and know that there is NO fully populated basis for
# this depot branch.
FPBASIS_NONE = FPBasis( change_num    = 0
                      , map_line_list = []
                      , map_rhs_root  = "")


# "static initializer" time
p4gf_util.test_vars_apply()  # Honor sequential UUID option

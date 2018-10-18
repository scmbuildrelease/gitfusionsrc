#! /usr/bin/env python3.3
"""Determine whether a user has repo read permissions.

This is an all or nothing test. Either the user may read all
views in all branches or is denied repo read access.
"""

import logging
import os
import re
import uuid

import P4
import p4gf_const
from   p4gf_l10n           import _, NTR
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_util
import p4gf_protect
import p4gf_config
import p4gf_context
import p4gf_branch
import p4gf_group
from p4gf_config_validator import view_lines_define_empty_view
import p4gf_p4spec
import p4gf_translate


LOG = logging.getLogger(__name__)

# simulate enum with new type


def enum(*sequential, **named):
    """Implement enum as dictionary."""
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type(NTR('Enum'), (), enums)

# enum used by _compare_paths()
PATHS = enum('EQUAL', 'NO_OVERLAP', 'SUBSET', 'SUPERSET', 'OVERLAP')
# pylint doesn't understand the class 'PATHS' and thinks it has no members
# pylint: disable=no-member

# indexes into (view_path, is_marked, is_inclusion) tuple
VIEW  = 0
MARK  = 1
INCLU = 2

READ_DENIED_MSG = _("User '{p4user}' denied read access to repo '{repo_name}' by Perforce.")

# 'depot-path-repo-creation-enable' regex to match "depot/repo/branch"
RE_NDPR = re.compile(r'^([^/]+)/([^/]+)/([^/]+)')


def can_create_depot_repo(p4, repo_name):
    """Return whether depot-path-repo creation is possible."""
    def valid_repo_name():
        """Return False if repo_name does not correctly reference a valid depot."""
        matches = RE_NDPR.search(repo_name_git)
        depot_name = matches and matches.group(1)
        if not depot_name:
            LOG.debug3("can_create_depot_repo depot not of correct format")
            return False

        depot_types = [NTR('local'), NTR('stream')]
        depot_list = [depot['name'] for depot in p4.run('depots')
                      if depot['type'] in depot_types and
                      depot['name'] != p4gf_const.P4GF_DEPOT]
        if depot_name not in depot_list:       # invalid depot in repo_name
            LOG.debug('%s is not a depot (%s)', depot_name, depot_list)
            return False
        return True

    def any_user_permitted():
        """Return whether any user is permitted to create a repo."""
        return p4gf_config.GlobalConfig.getboolean(
            p4gf_config.SECTION_REPO_CREATION,
            p4gf_config.KEY_NDPR_ENABLE)

    def this_user_permitted():
        """Return whether this user is permitted to create a repo."""
        # when p4gf_init_repo.py is called from the command line
        # os.environ.get(p4gf_const.P4GF_AUTH_P4USER) is not set.
        # In this case, we need not check group permissions.  We assume someone
        # who can run this as a program has permissions to create the repo
        user = os.environ.get(p4gf_const.P4GF_AUTH_P4USER)
        if not user:
            return True
        # Otherwise, check if there's a group user must be a member of.
        p4group_name = p4gf_config.GlobalConfig.get(
            p4gf_config.SECTION_REPO_CREATION,
            p4gf_config.KEY_NDPR_P4GROUP)
        LOG.debug('this_user_permitted() p4group_name = {}'.format(p4group_name))
        # if no group set, all users are permitted
        if not p4group_name:
            return True
        # need to check that this user is a member of the named group
        p4group_list = p4.run(['groups', '-i', '-u', user])
        return p4group_name in [g.get('group') for g in p4group_list]

    repo_name_git = p4gf_translate.TranslateReponame.repo_to_git(repo_name)
    return valid_repo_name() and\
        any_user_permitted() and\
        this_user_permitted()


def user_has_read_permissions(p4, repo_perm,  required_perm):
    """If this is a pull - check read permissions if enabled."""
    if required_perm != p4gf_group.PERM_PULL:
        return True
    # query the global config for read_permission check
    read_perm_check = p4gf_config.GlobalConfig(p4).get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                                       p4gf_config.KEY_READ_PERMISSION_CHECK)
    if read_perm_check is None or \
            read_perm_check.lower() != 'user':  # pylint:disable=maybe-no-member
        # no user perms enabled? then return True - no check
        return True

    # perform the user read permissions check
    try:
        read_permission = ReadPermission(p4, repo_perm)
        read_permission.read_permission_check_for_repo()
        return repo_perm.user_perm_repo_pull
    except RuntimeError:
        # Treat errors in the (config) validation as if the repo were not
        # accessible to the reader. Otherwise commands like @list explode
        # and fail the entire request, returning nothing.
        LOG.exception('permission checking failed, returning False')
        return False


def _views_have_no_exclusions(views):
    """Return True if these views contain no exclusions."""
    for v in views:
        if v.startswith('-') or v.startswith('"-'):
            return False
    return True


def _remove_view_modifier(line):
    """Remove the leading - or + from a view line."""
    l = line
    if line.startswith('-') or line.startswith('+'):
        l = line[1:]
    elif line.startswith('"-') or line.startswith('"+'):
        l = '"' + line[2:]
    return l


def _compare_paths(path_a, path_b):
    """Return the relation between two paths as named in the enums below.

    Ignore leading - or +
    """
    a = _remove_view_modifier(path_a)
    b = _remove_view_modifier(path_b)
    if a == b:
        return PATHS.EQUAL
    amap = P4.Map(a, a)
    bmap = P4.Map(b, b)
    jmap = P4.Map.join(amap, bmap)
    if P4.Map.is_empty(jmap):
        return PATHS.NO_OVERLAP
    else:
        c = P4.Map.lhs(jmap)[0]
        if a == c:
            return PATHS.SUBSET
        elif b == c:
            return PATHS.SUPERSET
        else:
            return PATHS.OVERLAP


class ReadPermission:

    """Determine whether user's read permissions permit repo access."""

    def __init__(self, p4, repo_perm):
        self.p4               = p4
        self.p4user           = repo_perm.p4user_name
        self.repo_name        = repo_perm.repo_name
        self.repo_perm        = repo_perm
        self.gf_user          = os.environ.get(p4gf_const.P4GF_AUTH_P4USER)
        self.p4client         = None
        self.p4client_created = False
        self.config           = None
        self.user_to_protect  = p4gf_protect.UserToProtect(self.p4)
        self.current_branch   = None
        self.user_branch_protections = None
        self.branches_without_exclusions = None

    def get_branch_dict(self):
        """Get a branch dictionary for this repo.

        If the p4gf_config exists, use that.
        Else if the p4 client exists
        create a branch dict containing a branch from the client views.
        Else if the stream exists
        create a branch dict containing a branch from the stream views.
        Else return None
        """
        LOG.debug("get_branch_dict for {0}".format(self.repo_name))
        # Repo config file already checked into Perforce?
        # Use that.
        try:
            repo_config = p4gf_config.RepoConfig.from_depot_file(self.repo_name, self.p4)
            branch_dict = p4gf_branch.dict_from_config(repo_config.repo_config, self.p4)
            for b in branch_dict.values():
                b.set_rhs_client(self.p4client)
            return branch_dict
        except p4gf_config.ConfigLoadError:
            pass

        LOG.debug("checking if client %s exists", self.repo_name)
        if p4gf_p4spec.spec_exists(self.p4, 'client', self.repo_name):
            template_spec = p4gf_p4spec.fetch_client(self.p4, self.repo_name, routeless=True)
            if 'Stream' in template_spec:
                return self.get_branch_dict_from_stream(template_spec['Stream'])
            return self.get_branch_dict_from_client()

        repo_name_raw = p4gf_translate.TranslateReponame.repo_to_git(self.repo_name)
        stream_name = '//' + repo_name_raw
        LOG.debug("checking if stream %s exists", stream_name)
        if p4gf_p4spec.spec_exists(self.p4, 'stream', stream_name):
            return self.get_branch_dict_from_stream(stream_name)

        if can_create_depot_repo(self.p4, self.repo_name):
            viewline = NTR('//{DEPOT_REPO}/... //{CLIENT_NAME}/...')\
                .format(DEPOT_REPO=repo_name_raw, CLIENT_NAME=self.p4client)
            return self.get_branch_dict_from_view(viewline)

        # No config, client, or stream, nothing here.
        return None

    @staticmethod
    def get_branch_dict_from_view(view_lines):
        """Generate a branch dict from a view."""
        # create a Branch object to manage this client view
        if isinstance(view_lines, str):
            view_lines = view_lines.splitlines()
        LOG.debug("create branch from client views: %s", view_lines)
        branch = p4gf_branch.Branch(branch_id='master')
        branch.git_branch_name = 'master'
        branch.view_p4map = P4.Map(view_lines)
        branch.view_lines = view_lines
        LOG.debug("create branch from client branch view_p4map: %s", branch.view_p4map)
        LOG.debug("create branch from client branch view_lines: %s", branch.view_lines)
        branch_dict = {}
        branch_dict[branch.branch_id] = branch
        return branch_dict

    def get_branch_dict_from_client(self):
        """Generate a branch dict from a client.

        :return: branch dict, or None if client view is unacceptable.

        """
        view_lines = p4gf_p4spec.get_client_template_view(self.p4, self.p4client, self.repo_name)
        if not view_lines:
            LOG.debug('no View in client %s', self.repo_name)
            return None
        LOG.debug('branch_dict from client %s', self.repo_name)
        return self.get_branch_dict_from_view(view_lines)

    def get_branch_dict_from_stream(self, stream_name):
        """Generate a branch dict from a stream.

        :param str stream_name: name of the stream.
        :return: branch dict, or None if stream view is unacceptable.

        """
        stream = p4gf_util.first_dict(self.p4.run('stream', '-ov', stream_name))
        if 'View' not in stream:
            LOG.debug('no View in stream %s', stream_name)
            return None
        view_lines = stream['View']
        LOG.debug('branch_dict from stream %s', stream_name)
        view_p4map = p4gf_branch.convert_view_from_no_client_name(
            P4.Map(view_lines), self.p4client)
        view_lines = view_p4map.as_array()
        return self.get_branch_dict_from_view(view_lines)

    def switch_client_to_stream(self, branch):
        """Change this repo's Perforce client view to the branch's stream."""
        # Lazy create our read-perm client for streams
        if not self.p4client_created:
            p4gf_p4spec.set_spec(
                                self.p4, 'client'
                              , spec_id = self.p4client
                              , cached_vardict = None)
            self.p4client_created = True
        #pylint: disable=too-many-function-args
        self.p4.run('client', '-f', '-s', '-S', branch.stream_name, self.p4client)

    def switch_client_view_to_branch(self, branch):
        """Set the repo's Perforce client to view of the given Branch object.

        The client is used only by this class.
        """
        if branch.stream_name:
            self.switch_client_to_stream(branch)
        else:
            self.switch_client_view_lines(branch.view_lines)

    def switch_client_view_lines(self, lines):
        """Change this repo's Perforce client view to the given line list."""
        LOG.debug("switch_client_view_lines {0}".format(lines))
        _lines = p4gf_context.to_lines(lines)
        p4gf_p4spec.set_spec(
                            self.p4, 'client'
                          , spec_id = self.p4client
                          , values  = {'View': _lines, 'Stream': None}
                          , cached_vardict = None)
        self.p4client_created = True

    def gf_user_has_list_permissions(self):
        """Determine whether git-fusion-user has 'list' permissions as its last protects line.

        Only required when appyling 'user' read-permission-check.
        """
        protects_dict = self.user_to_protect.user_to_protect(
            p4gf_const.P4GF_USER).get_protects_dict()
        last_perm = protects_dict[-1]
        return last_perm['perm'] == 'list' and last_perm['depotFile'] == '//...'

    @staticmethod
    def _protect_dict_to_str(pdict):
        """Format one protection line as dictionary to string."""
        excl = '-' if 'unmap' in pdict else ''
        if NTR('user') in pdict:
            user = NTR('user ') + pdict['user']
        else:
            user = NTR('group ') + pdict['group']
        return "{0} {1} {2} {3}{4}".format(
            pdict['perm'], user, pdict['host'], excl, pdict['depotFile'])

    def log_rejected_not_included(self, view_mark_inclusion):
        """Some view paths are not included in the protections.

        Report only repo is protected to git user.
        LOG the unpermitted views.
        """
        msg = READ_DENIED_MSG.format(p4user=self.p4user, repo_name=self.repo_name)
        self.repo_perm.error_msg = '\n' + msg
        for view_path in [vmi[VIEW] for vmi in view_mark_inclusion if not vmi[MARK]]:
            msg += '\n     denied view by missing inclusion: {0}'.format(view_path)
        LOG.warning(msg)

    def log_rejected_excluded(self, view_path):
        """Report only repo is protected to git user.

        LOG the offending excluded view.
        """
        msg = READ_DENIED_MSG.format(p4user=self.p4user, repo_name=self.repo_name)
        self.repo_perm.error_msg = '\n' + msg
        msg += '\n     denied view by exclusion: {0}'.format(view_path)
        LOG.warning(msg)

    def check_views_read_permission(self):
        """Check a set of view_lines against a user's read permissions.

        Compare each view line (bottom up) against each protect line (bottom up).
        By this strategy, later views once passed by later protections need not
        be rejected by earlier protections.
        Marking a view = True marks it as being granted read permission.

        If all inclusionary views are marked readable return True.
        If a yet unmarked inclusionary view line compares as not NO_OVERLAP
        to an exlusionary protect line return False.
        If any inclusionary view lines remain unmarked after testing
        against all protect lines return False.

        See: doc/p4gf_read_protect.py  and  doc/p4gf_compare_paths.py
        """
        # pylint: disable=too-many-branches
        # always get the views from the P4.Map to apply the disambiguator
        view_lines = self.current_branch.view_p4map.lhs()
        # Get the full permissions granted this user by requesting READ
        read_protections = p4gf_protect.create_read_permissions_list(
            self.user_branch_protections.get_protects_dict(),
            p4gf_protect.READ)

        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3("check_views_read_permission: protections {0} ".format(
                self.user_branch_protections.get_protects_dict()))
            LOG.debug3("check_views_read_permission: user {0} : view {1}".format(
                self.p4user, self.repo_name) + "\nview_lines: {0} \nprotects: {1}".format(
                view_lines, read_protections))

        # A client may be defined such the views resolve to empty
        # In this case PASS the read check - as nothing can be denied
        if view_lines_define_empty_view(view_lines):
            return True

        # initially the number of lines in view which are NOT exclusions
        unmarked_inclusions_count = 0

        # create a list of view tuples (view_line, is_marked, is_inclusion)
        # if is_marked == True, view line is readable
        # count non-exclusion lines
        view_mark_inclusion = []
        for v in view_lines:
            if not v.startswith('-'):
                unmarked_inclusions_count += 1
                view_mark_inclusion.append((v, False, True))
            else:
                view_mark_inclusion.append((v, False, False))
        lastidx = len(view_mark_inclusion) - 1
        for p in read_protections[::-1]:               # reverser order slice
            for vix in range(lastidx, -1, -1):
                vmi = view_mark_inclusion[vix]
                result = _compare_paths(vmi[VIEW], p)
                if result == PATHS.NO_OVERLAP:
                    continue                           # vmi inner loop
                if p.startswith('-'):                  # p is exclusion
                    if vmi[INCLU] and not vmi[MARK]:   # +view and not marked
                        # in this case reject for all test results and deny read permission
                        self.log_rejected_excluded(vmi[VIEW])
                        LOG.warning("rejected by permission {0}".format(p))
                        return False
                    # case with -view OR +view and marked
                    if result == PATHS.SUBSET or result == PATHS.OVERLAP:
                        continue                        # vmi inner loop
                    else:  # PATHS.EQUAL || PATHS.SUPERSET
                        break    # out of vmi loop into protects loop
                else:  # p not exclusion
                    if result == PATHS.SUBSET or result == PATHS.EQUAL:
                        if vmi[INCLU]:                  # +view
                            # mark this view as granted read permission
                            view_mark_inclusion[vix] = (vmi[VIEW], True, vmi[INCLU])
                            unmarked_inclusions_count -= 1        # decrease unmarked count
                        if unmarked_inclusions_count <= 0:
                            return True
                    else:
                        continue   # next vmi

        self.log_rejected_not_included(view_mark_inclusion)
        return False   # something must have been left unmarked

    def check_branch_read_permissions(self, branch):
        """Check a repo  branch against a user's read permissions."""
        LOG.debug("read_permission_check_for_view : switch to branch dict {0}".
                  format(branch.to_log(LOG)))
        try:
            self.switch_client_view_to_branch(branch)
        except P4.P4Exception:
            if p4gf_p4msg.find_msgid(self.p4, p4gf_p4msgid.MsgDm_MapNotUnder):
                return False
            raise
        self.user_branch_protections = self.user_to_protect.user_view_to_protect(self.p4user,
                                                                                 self.p4client)
        self.current_branch = branch
        return self.check_views_read_permission()

    def read_permission_check_for_repo(self):
        """Determine whether the user's p4 protects permit read access to the repo."""
        # Indicates this test was invoked by GF global configuration setting
        self.repo_perm.user_read_permission_checked = True

        self.p4client =  p4gf_const.P4GF_REPO_READ_PERM_CLIENT.format(
            server_id = p4gf_util.get_server_id()
            , repo_name = self.repo_name, uuid = str(uuid.uuid1()))
        LOG.debug("read_permission_check_for_repo : p4client {0}".format(self.p4client))
        with p4gf_util.restore_client(self.p4, self.p4client):
            branch_dict = self.get_branch_dict()
            if not branch_dict:
                LOG.debug("no branch_dict for {0}".format(self.repo_name))
                # No p4gf_config and no client - so return the same message as does p4gf_init_repo
                nop4client = _("p4 client '{p4client}' does not exist\n").format(
                    p4client=self.repo_name)
                self.repo_perm.error_msg = '\n' + p4gf_const.NO_REPO_MSG_TEMPLATE.format(
                    repo_name=self.repo_name,
                    repo_name_p4client=self.repo_name,
                    nop4client=nop4client)
                LOG.warning(self.repo_perm.error_msg)
                self.repo_perm.user_perm_repo_pull = False
                return self.repo_perm.user_perm_repo_pull

            num_branches = len(branch_dict)
            LOG.debug("read_permission_check_for_repo repo: {0}  num branches {1}".
                      format(self.repo_name, num_branches))
            self.repo_perm.user_perm_repo_pull = True

            if branch_dict and self.repo_perm.user_perm_repo_pull:
                # check each branch for read permissions
                for branch in branch_dict.values():
                    self.repo_perm.user_perm_repo_pull = self.check_branch_read_permissions(branch)
                    LOG.debug("read_permission_check_for_repo branch {0} = {1}".
                              format(branch.branch_id, self.repo_perm.user_perm_repo_pull))
                    if not self.repo_perm.user_perm_repo_pull:
                        # Log as error to aid debugging when root level is 'warning'
                        LOG.error("read_permission_check_for_repo repo={0} branch={1} FAILED".
                              format(self.repo_name, branch.branch_id ))
                        break

        # delete this temporary read perm only client
        if self.p4client_created:
            self.p4.run('client', '-df', self.p4client)
        return self.repo_perm.user_perm_repo_pull

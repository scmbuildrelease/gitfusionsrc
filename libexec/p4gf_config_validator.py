#! /usr/bin/env python3.3
"""Validation of Git Fusion configuration files."""

import logging
import os
import re
import sys
import uuid

import P4

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_branch
import p4gf_config
import p4gf_const
import p4gf_create_p4
from   p4gf_l10n import _, NTR
import p4gf_git
import p4gf_translate
import p4gf_util
import p4gf_p4spec

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_config_validator")

REMOVE_HYPEN_REGEX = re.compile(r'^("?)-(.*)')
SPACE_AFTER_PLUS_OR_MINUS = re.compile(r'^("\s*)?[\+\-]\s')
FIRST_UNQUOTED_SPACE = re.compile(r'([^"]|"[^"]*")*?([ \t])')
DEPOT_FROM_LHS = re.compile(r'^\"?[+-]?//([^/]+)/.*')
DOTS_SUFFIX = re.compile(r'.+\.\.\.$')
WILDCARD = re.compile(r'(\*|%%|\.\.\.)')
OVERLAY_PREFIX = re.compile(r'^"?[-+]')
P4CLIENTERROR = re.compile('Error in client specification')

INVALID_GIT_BRANCH_NAME_PREFIXES = ['remotes/']

# Legal @xxx section names. Any other result in
# p4gf_config_validator.is_valid() rejection.
AT_SECTIONS = [
    p4gf_config.SECTION_REPO,
    p4gf_config.SECTION_FEATURES
]

# Options that are permitted in branch sections.
BRANCH_OPTIONS = [
    p4gf_config.KEY_DEPOT_BRANCH_ID,
    p4gf_config.KEY_DEPOT_ROOT,
    p4gf_config.KEY_FORK_OF_BRANCH_ID,
    p4gf_config.KEY_GIT_BRANCH_DELETED,
    p4gf_config.KEY_GIT_BRANCH_DELETED_CHANGE,
    p4gf_config.KEY_GIT_BRANCH_START_CHANGE,
    p4gf_config.KEY_GIT_BRANCH_NAME,
    p4gf_config.KEY_ORIGINAL_VIEW,
    p4gf_config.KEY_READ_ONLY,
    p4gf_config.KEY_STREAM,
    p4gf_config.KEY_VIEW
]

BOOLEAN_OPTIONS = [
    (p4gf_config.SECTION_REPO_CREATION,   p4gf_config.KEY_GIT_AUTOPACK),
    (p4gf_config.SECTION_REPO_CREATION,   p4gf_config.KEY_NDPR_ENABLE),
    (p4gf_config.SECTION_GIT_TO_PERFORCE, p4gf_config.KEY_ENABLE_BRANCH_CREATION),
    (p4gf_config.SECTION_GIT_TO_PERFORCE, p4gf_config.KEY_ENABLE_MERGE_COMMITS),
    (p4gf_config.SECTION_GIT_TO_PERFORCE, p4gf_config.KEY_ENABLE_SWARM_REVIEWS),
    (p4gf_config.SECTION_GIT_TO_PERFORCE, p4gf_config.KEY_ENABLE_SUBMODULES),
    (p4gf_config.SECTION_GIT_TO_PERFORCE, p4gf_config.KEY_IGNORE_AUTHOR_PERMS),
    (p4gf_config.SECTION_GIT_TO_PERFORCE, p4gf_config.KEY_ENABLE_FAST_PUSH),
    (p4gf_config.SECTION_GIT_TO_PERFORCE, p4gf_config.KEY_USE_SHA1_TO_SKIP_EDIT),
    (p4gf_config.SECTION_PERFORCE_TO_GIT, p4gf_config.KEY_SUBMODULE_IMPORTS),
    (p4gf_config.SECTION_PERFORCE_TO_GIT, p4gf_config.KEY_CLONE_TO_CREATE_REPO),
    (p4gf_config.SECTION_PERFORCE_TO_GIT, p4gf_config.KEY_UPDATE_ONLY_ON_POLL),
    (p4gf_config.SECTION_PERFORCE_TO_GIT, p4gf_config.KEY_ENABLE_ADD_COPIED_FROM_PERFORCE),
    (p4gf_config.SECTION_PERFORCE_TO_GIT, p4gf_config.KEY_ENABLE_GIT_P4_EMULATION),
    (p4gf_config.SECTION_AUTHENTICATION,  p4gf_config.KEY_EMAIL_CASE_SENSITIVITY),
]


def view_lines_have_space_after_plus_minus(viewlines):
    """Return True if view lines have space after '+'' or '-'."""
    return any(SPACE_AFTER_PLUS_OR_MINUS.match(vl) for vl in viewlines)


def find_first_whitespace_not_in_quotes(vline):
    """Locate the first white_space not in quotes."""
    m = FIRST_UNQUOTED_SPACE.search(vline)
    return m.lastindex if m else -1


def view_lines_define_empty_view(view_lines):
    """Determine whether the views lines define an empty view.

    Return True if empty.

    It is assumed that the view_lines have passed
    through P4.Map and so have been disambiuated.

    [//depot/a/..., -//depot/...] disambiguates to
    [//depot/a/..., -//depot/a/...]
    Remove matching pairs across inclusions and exclusions.
    Return True if no inclusions remain.
    """
    inclusions = set([])
    exclusions = set([])
    # collect the sets of inclusions and exclusions
    # while stripping the '-' from the exclusions
    for v in view_lines:
        if v.startswith('-') or v.startswith('"-'):
            exclusions.add(re.sub(REMOVE_HYPEN_REGEX, r'\1\2', v))
        else:
            inclusions.add(v)
    # subtract the matching exclusions from inclusions
    # and check if inclusions count > 0
    return not len(inclusions - exclusions) > 0


def depot_from_view_lhs(lhs):
    """Extract depot name from lhs of view line."""
    s = DEPOT_FROM_LHS.search(lhs)
    if s:
        return s.group(1)
    else:
        return None


def validate_copy_rename_value(val):
    """Validate and return copy/rename values.

    :type val: str
    :param val: the incoming configuration value.

    :rtype: str or False
    :return: False for invalid values, otherwise a number between
             0 and 100 (inclusive) with a trailing percent sign (%).
             If the value is 'no' or 'off', returns '0%'.

    """
    if val is None:
        return False
    val = val.lower()
    if val == 'no' or val == 'off':
        return '0%'
    if val[-1] != '%':
        return False
    try:
        num = int(val[:-1])
        if num < 0 or num > 100:
            return False
    except ValueError:
        return False
    return val


def validate_preflight_commit_value(val):
    """Ensure the preflight commit hook value is well formed."""
    if val is None:
        return True
    val = val.lower()
    if val == 'none' or val == 'pass' or val == 'fail':
        return True
    if val[0] == '[':
        # find the next unescaped ] in the line
        offset = 0
        while True:
            path_end = val.find(']', offset)
            if path_end == -1:
                return False
            if val[path_end-1] == '\\':
                offset = path_end + 1
            else:
                break
    return True


class Validator:

    """A validator for Git Fusion configuration files.

    Construct with a RepoConfig and use is_valid() or is_valid2() to test
    validity of p4gf_config and p4gf_config2 files respectively.
    """

    def __init__(self, config, p4):
        """Initialize a Config Validator."""
        self.config = config
        self.p4 = p4
        self.report_count = 0
        self.require_git_branch_name = True
        self.branches_union_view_lines = []
        self.tmp_client_name = p4gf_const.P4GF_CONFIG_VALIDATE_CLIENT.format(
                uuid=str(uuid.uuid1()))

    def set_require_git_branch_name(self, val):
        """Set whether git_branch_name value is required for branch sections."""
        self.require_git_branch_name = val
        return self

    def is_valid(self):
        """Check if p4gf_config file is valid."""
        # pylint:disable=too-many-branches
        # Validate the global config while we're in here.
        if not self._is_global_valid():
            return False

        # reject sections starting with @ except for @repo
        # like if they put @repos or @Repo instead of @repo
        at_sections = [section for section in self.config.sections()
                       if section.startswith('@') and section not in AT_SECTIONS]
        if at_sections:
            self._report_error(_("unexpected section(s): '{sections}'\n")
                               .format(sections="', '".join(at_sections)))
            return False

        # Ensure there are no spurious sections. Note that those sections
        # that do not mirror default section names will be checked by
        # _valid_branches() and need not be checked here. Likewise, any
        # section beginning with @ has already been checked.
        if self._ignored_sections():
            # condition already reported
            return False

        # Ensure the [@repo] section does not harbor any unknown options.
        if self._ignored_options():
            return False

        # Make sure if a charset specified that it's valid
        if self.config.has_option(p4gf_config.SECTION_REPO, p4gf_config.KEY_CHARSET):
            charset = self.config.get(p4gf_config.SECTION_REPO, p4gf_config.KEY_CHARSET)
            LOG.debug('checking charset %s', charset)
            if charset and not self.valid_charset(charset):
                self._report_error(_("invalid charset: '{charset}'\n")
                                   .format(charset=charset))
                return False
        # Ensure the change-owner setting is correctly defined
        if self.config.has_option(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                  p4gf_config.KEY_CHANGE_OWNER):
            value = self.config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                    p4gf_config.KEY_CHANGE_OWNER)
            if value != p4gf_config.VALUE_AUTHOR and value != p4gf_config.VALUE_PUSHER:
                self._report_error(_("Perforce: Improperly configured {key} value\n")
                                   .format(key=p4gf_config.KEY_CHANGE_OWNER))
                return False

        # Ensure correct new_depo_branch settings
        if not self._validate_new_depot_branch(self.config):
            return False
        # Ensure the git copy/rename settings are valid
        if self.config.has_option(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                  p4gf_config.KEY_ENABLE_GIT_FIND_COPIES):
            value = self.config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                    p4gf_config.KEY_ENABLE_GIT_FIND_COPIES)
            if not validate_copy_rename_value(value):
                self._report_error(_("invalid git copy/rename value: '{value}'\n")
                                   .format(value=value))
                return False
        if self.config.has_option(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                  p4gf_config.KEY_ENABLE_GIT_FIND_RENAMES):
            value = self.config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                    p4gf_config.KEY_ENABLE_GIT_FIND_RENAMES)
            if not validate_copy_rename_value(value):
                self._report_error(_("invalid git copy/rename value: '{value}'\n")
                                   .format(value=value))
                return False

        # Validate the preflight-commit option
        if self.config.has_option(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                  p4gf_config.KEY_PREFLIGHT_COMMIT):
            value = self.config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                                    p4gf_config.KEY_PREFLIGHT_COMMIT)
            if not validate_preflight_commit_value(value):
                self._report_error(_("invalid preflight commit value: '{value}'\n")
                                   .format(value=value))
                return False

        # Make sure branches are present and properly configured
        if not self._valid_branches():
            return False
        if not self._valid_depots():
            return False
        return True

    def is_valid2(self):
        """Check if p4gf_config2 file is valid."""
        # reject sections starting with @ including @repo
        # none of that stuff in config2 files
        at_sections = [section for section in self.config.repo_config2.sections()
                       if section.startswith('@')]
        if at_sections:
            self._report_error(_("unexpected section(s): '{sections}'\n")
                               .format(sections="', '".join(at_sections)))
            return False
        # Make sure branches are present and properly configured
        if not self._valid_branches2():
            return False
        if not self._valid_depots(allow_p4gf_depot=True):
            return False
        return True

    @staticmethod
    def valid_charset(charset):
        """Return True for a valid charset, False for an invalid charset."""
        p4 = p4gf_create_p4.create_p4(connect=False)
        try:
            # setting invalid charset will raise an exception from p4python
            p4.charset = charset
        except P4.P4Exception:
            return False
        return True

    def _report_error(self, msg):
        """Report error message, including path to offending file."""
        if not self.report_count:
            sys.stderr.write(_("error: invalid configuration file: '{config_source}'\n")
                             .format(config_source=self.config.repo_config_source))
            if LOG.isEnabledFor(logging.DEBUG):
                contents = p4gf_config.to_text('', self.config.repo_config)
                LOG.debug('config contents: %s', contents)
        self.report_count += 1
        LOG.error("Config {} has error: {}".format(self.config.repo_config_source, msg))
        sys.stderr.write(_('error: {error}').format(error=msg))

    def _check_duplicate_branches(self, sections):
        """Check that branch ids are unique.

        For case-sensitive servers, no extra work is required; ConfigParser
        will raise configparser.DuplicateSectionError if there are duplicate
        sections.

        For case-insensitive servers, we need to ensure that there are no
        sections (branch ids) that differ only in case.
        """
        if not self.p4.server_case_insensitive:
            return
        branches = {}
        for s in sections:
            key = s.lower()
            if key in branches:
                branches[key].append(s)
            else:
                branches[key] = [s]
        for v in branches.values():
            if len(v) > 1:
                self._report_error(_("branch ids differ only in case: {branch_ids}")
                                   .format(branch_ids=", ".join(v)))

    def _validate_accumulate_union_views(self, branch):
        """Create a temporary client with the branch views for P4 to validate."""

        def errmsg(msg):
            """Add branch_id, view_lines to error message."""
            return msg + msg + _("view for branch '{branch_id}':\n{view_lines}\n")

        view_p4map = p4gf_branch.convert_view_from_no_client_name(
                branch.view_p4map, self.tmp_client_name)

        # Reject views lines is one but not both end in '/...'.
        rh_sides = set()
        for (lh, rh) in zip(view_p4map.lhs(), view_p4map.rhs()):
            # Check for "smart" quotes in the paths.
            if '\u201c' in lh or '\u201d' in lh:
                return errmsg(_('the left side contains "smart" quotes: {left_side}\n')
                              .format(left_side=lh))
            if '\u201c' in rh or '\u201d' in rh:
                return errmsg(_('the right side contains "smart" quotes: {right_side}\n')
                              .format(right_side=rh))
            # Check for the ellipsis character in the paths.
            if '\u2026' in lh:
                return errmsg(_('the left side contains ellipsis character: {left_side}\n')
                              .format(left_side=lh))
            if '\u2026' in rh:
                return errmsg(_('the right side contains ellipsis character: {right_side}\n')
                              .format(right_side=rh))
            lh = lh.strip('"\'')
            rh = rh.strip('"\'')
            lh_slash_dots = lh.endswith('/...')# or lh.endswith('/..."')
            rh_slash_dots = rh.endswith('/...')# or lh.endswith('/..."')
            if (    lh_slash_dots and not rh_slash_dots) or \
               (not lh_slash_dots and     rh_slash_dots) :
                return errmsg(_("the left and right side suffixes do not match for view line"
                                " '{left_side} {right_side}'\n")
                              .format(left_side=lh, right_side=rh))
            if not OVERLAY_PREFIX.search(lh):
                if rh in rh_sides:
                    return errmsg(_("non overlay right hand sides of view map must be different\n"))
                rh_sides.add(rh)

        view_lines = view_p4map.as_array()
        self.branches_union_view_lines.extend(view_lines)
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3("_validate_accumulate_union_views: viewlines...\n %s",
                       '\n'.join(self.branches_union_view_lines))

    def _validate_view_lines_using_p4_client(self):
        '''Attempt to create a temp client with the union of all branch views.

        This will validate all views.
        Return None if valid, or Error message otherwise.
        '''
        # This client root path will not be used by P4
        client_root = '/tmp/git-fusion/validate_config'
        desc = (_("Created by Perforce Git Fusion for config_validation'."))
        # Attributes common to all sorts of clients.
        spec = {
            'Owner': p4gf_const.P4GF_USER,
            'LineEnd': NTR('unix'),
            'Root': client_root,
            'Options': p4gf_const.CLIENT_OPTIONS,
            'Host': None,
            'Description': desc
        }
        spec['View'] = self.branches_union_view_lines
        try:
            p4gf_p4spec.set_spec(self.p4, 'client', spec_id=self.tmp_client_name, values=spec)
            p4gf_util.p4_client_df(self.p4, self.tmp_client_name)
            return None
        except P4.P4Exception as e :
            # Extract the interesting portion of the P4 error message
            errmsg = []
            for line in str(e).splitlines():
                if P4CLIENTERROR.search(line):
                    errmsg.append(line)
            errmsg = "\n".join(errmsg)
            LOG.debug3("_validate_view_lines_using_p4_client: p4 exception: tmpclient:%s: %s",
                       self.tmp_client_name, errmsg)
            return _("There is an error in some branch Views.\n{error}").format(error=errmsg)

    def _valid_branches(self):
        """Check if branch definitions in config file are valid."""
        # validation requires use of some settings merged in from the global config
        # for example [@features]
        config = self.config
        # Does the config contain any branch sections?
        sections = self.config.branch_sections()
        if not sections:
            self._report_error(_('repository configuration missing branch ID\n'))
            return False

        self._check_duplicate_branches(sections)

        if LOG.isEnabledFor(logging.DEBUG3):
            # config contents are too lengthy for debug level
            cfg_text = p4gf_config.to_text("", p4gf_config.GlobalConfig.instance())
            LOG.debug3('global config: %s', cfg_text)
        # check branch creation option
        try:
            config.getboolean(p4gf_config.SECTION_GIT_TO_PERFORCE,
                              p4gf_config.KEY_ENABLE_BRANCH_CREATION)
        except ValueError:
            self._report_error(_("repository configuration option '{key}' has illegal value\n")
                               .format(key=p4gf_config.KEY_ENABLE_BRANCH_CREATION))

        # check merge commits option
        try:
            config.getboolean(p4gf_config.SECTION_GIT_TO_PERFORCE,
                              p4gf_config.KEY_ENABLE_MERGE_COMMITS)
        except ValueError:
            self._report_error(_("repository configuration option '{key}' has illegal value\n")
                               .format(key=p4gf_config.KEY_ENABLE_MERGE_COMMITS))

        # check read-only option
        try:
            config.getboolean(p4gf_config.SECTION_REPO, p4gf_config.KEY_READ_ONLY, fallback=False)
        except ValueError:
            self._report_error(_("repository configuration option '{key}' has illegal value\n")
                               .format(key=p4gf_config.KEY_READ_ONLY))

        # Examine them and confirm they have branch views and all RHS match
        enable_mismatched_rhs = \
            config.getboolean(p4gf_config.SECTION_REPO,
                              p4gf_config.KEY_ENABLE_MISMATCHED_RHS,
                              fallback=False)
        first_branch = None
        for section in sections:
            branch = self._valid_branch(config.repo_config, section, first_branch)
            if not branch:
                return False

            if not enable_mismatched_rhs and not first_branch:
                first_branch = branch

        error_msg = self._validate_view_lines_using_p4_client()
        if error_msg:
            self._report_error(error_msg)
            return False

        return True

    def _valid_branches2(self):
        """Check if branch definitions in config file are valid.

        Returns branch if valid, else None
        """
        # validation requires use of some settings merged in from the global config
        # for example [@features]
        config = self.config
        # Does the config contain any branch sections?
        sections = self.config.branch_sections2()
        if not sections:
            self._report_error(_('repository configuration missing branch ID\n'))
            return False

        if LOG.isEnabledFor(logging.DEBUG3):
            # config contents are too lengthy for debug level
            cfg_text = p4gf_config.to_text("", p4gf_config.GlobalConfig.instance())
            LOG.debug3('global config: %s', cfg_text)

        # Examine them and confirm they have branch views and all RHS match
        enable_mismatched_rhs = \
            config.getboolean(p4gf_config.SECTION_REPO,
                              p4gf_config.KEY_ENABLE_MISMATCHED_RHS,
                              fallback=False)
        first_branch = None
        for section in sections:
            branch = self._valid_branch(config.repo_config2, section, first_branch)
            if not branch:
                return False

            if not enable_mismatched_rhs and not first_branch:
                first_branch = branch

        error_msg = self._validate_view_lines_using_p4_client()
        if error_msg:
            self._report_error(error_msg)
            return False

        return True

    def _valid_branch(self, config, section, first_branch):
        """Check if a single branch definition is valid.

        Returns branch if valid, else None.
        """
        # pylint:disable=too-many-branches
        try:
            branch = p4gf_branch.Branch.from_config(config, section, self.p4, strict=True)
            if branch.git_branch_name:
                for prefix in INVALID_GIT_BRANCH_NAME_PREFIXES:
                    if branch.git_branch_name.startswith(prefix):
                        raise RuntimeError(_("repository configuration section [{section}]: "
                                              "'git-branch-name = {git_branch_name}'"
                                              " must not start with '{prefix}'")
                                            .format(section=section,
                                                    git_branch_name=branch.git_branch_name,
                                                    prefix=prefix))
                if not p4gf_git.is_valid_git_branch_name(branch.git_branch_name):
                    raise RuntimeError(_("repository configuration section [{section}]: "
                                          "'git-branch-name = {git_branch_name}'"
                                          " contains invalid characters")
                                        .format(section=section,
                                                git_branch_name=branch.git_branch_name))
            elif self.require_git_branch_name:
                raise RuntimeError(_("repository configuration section [{section}]"
                                      " must contain 'git-branch-name'")
                                    .format(section=section))

        except RuntimeError as e:
            self._report_error("{}\n".format(e))
            return None

        # check read-only option
        try:
            config.getboolean(section, p4gf_config.KEY_READ_ONLY, fallback=False)
        except ValueError:
            self._report_error(_("branch {section} configuration option '{key}' "
                                 "has illegal value\n")
                               .format(section=section, key=p4gf_config.KEY_READ_ONLY))

        for vline in branch.view_lines:
            if find_first_whitespace_not_in_quotes(vline) == -1:
                self._report_error(_("view lines are invalid\n"
                                     "view for branch '{branch_id}':\n{view_lines}\n")
                                   .format(branch_id=branch.branch_id,
                                           view_lines=branch.view_lines))
                return None
            if vline.startswith('&') or vline.startswith('"&'):
                msg = _("ampersand is not supported for view for branch '{}':\n{}\n").format(
                    branch.branch_id, branch.view_lines)
                self._report_error(msg)
                return None

        if view_lines_have_space_after_plus_minus(branch.view_lines):
            self._report_error(_("space follows + or - in view line\n"
                                 "view for branch '{branch_id}':\n{view_lines}\n")
                               .format(branch_id=branch.branch_id,
                                       view_lines=branch.view_lines))
            return None

        # check branch for set of view lines which describe an empty view
        # we get the views after passsing through P4.Map's disambiuator
        if view_lines_define_empty_view(branch.view_p4map.lhs()):
            msg = p4gf_const.EMPTY_VIEWS_MSG_TEMPLATE.format(
                repo_name=self.config.repo_name,
                repo_name_p4client=self.config.repo_name)
            self._report_error(msg)
            return None

        # check that the suffixes of the lhs and rhs of each viewline match
        error_msg = self._validate_accumulate_union_views(branch)
        if error_msg:
            self._report_error(_(error_msg)
                               .format(branch_id=branch.branch_id,
                                       view_lines=branch.view_lines))
            return None

        if first_branch and branch.view_p4map.rhs() != first_branch.view_p4map.rhs():
            self._report_error(_("branch views do not have same right hand sides\n"
                                 "view for branch '{branch_id1}':\n{view_lines1}\n"
                                 "view for branch '{branch_id2}':\n{view_lines2}\n")
                               .format(branch_id1=first_branch.branch_id,
                                       view_lines1=first_branch.view_lines,
                                       branch_id2=branch.branch_id,
                                       view_lines2=branch.view_lines))
            return None

        for option in config.options(section):
            if option not in BRANCH_OPTIONS:
                self._report_error(_("option '{option}' is not relevant in section '{section}'\n")
                                   .format(option=option, section=section))

        return branch

    def _valid_depots(self, allow_p4gf_depot=False):
        """Prohibit remote, spec, and other changelist-impaired depot types."""
        # Fetch all known Perforce depots.
        depot_list = {depot['name']: depot for depot in self.p4.run('depots')}

        # Scan all configured branches for prohibited depots.
        # use merged config for this to pick up [@features]
        branch_dict     = p4gf_branch.dict_from_config(self.config.repo_config, self.p4)
        valid           = True
        for branch in branch_dict.values():
            if not branch.view_p4map:
                continue
            v = self._view_valid_depots( depot_list
                                       , branch.branch_id
                                       , branch.view_p4map
                                       , allow_p4gf_depot)
            valid = valid and v
        return valid

    def _view_valid_depots(self, depot_list, branch_id, view_p4map, allow_p4gf_depot):
        """Prohibit remote, spec, and other changelist-impaired depot types."""
        valid = True

        # Extract unique list of referenced depots. Only want to warn about
        # each depot once per branch, even if referred to over and over.
        lhs = view_p4map.lhs()
        referenced_depot_name_list = []
        for line in lhs:
            if line.startswith('-'):
                continue
            depot_name = depot_from_view_lhs(line)
            if not depot_name:
                self._report_error(_("branch '{branch_id}': badly formed depot "
                                     "syntax in view: '{line}' not permitted.\n'")
                                   .format(branch_id=branch_id, line=line))
                valid = False
                continue
            if depot_name not in referenced_depot_name_list:
                referenced_depot_name_list.append(depot_name)

        # check each referenced depot for problems
        for depot_name in referenced_depot_name_list:
            if not allow_p4gf_depot and depot_name == p4gf_const.P4GF_DEPOT:
                self._report_error(_("branch '{branch_id}': Git Fusion internal"
                                     " depot '{depot_name}' not permitted.\n'")
                                   .format(branch_id=branch_id, depot_name=depot_name))
                valid = False
                continue

            if depot_name not in depot_list:
                self._report_error(_("branch '{branch_id}': undefined "
                                     "depot '{depot_name}' not permitted "
                                     "(possibly due to lack of permissions).\n'")
                                   .format(branch_id=branch_id, depot_name=depot_name))
                valid = False
                continue

            depot = depot_list[depot_name]
            if depot['type'] not in [NTR('local'), NTR('stream')]:
                self._report_error(_("branch '{branch_id}': depot '{depot_name}'"
                                     " type '{depot_type}' not permitted.\n'")
                                   .format(branch_id=branch_id,
                                           depot_name=depot_name,
                                           depot_type=depot['type']))
                valid = False
                continue

        return valid

    def _ignored_sections(self):
        """Confirm the config does not contain sections that would be ignored.

        Any error reporting will be done by this function.

        :rtyp: bool
        :return: True if ignored sections found, False otherwise.

        """
        has_ignored_sections = False
        # Use the fully populated defaults to detect sections that should
        # not be in the repo configuration file.
        default_cfg = p4gf_config.default_config_global()
        for section in self.config.sections():
            # the @ sections have already been verified, so ignore those
            if not section.startswith('@') and default_cfg.has_section(section):
                self._report_error(_("repository configuration section '{section}'"
                                     " would be ignored\n")
                                   .format(section=section))
                has_ignored_sections = True
        return has_ignored_sections

    def _ignored_options(self):
        """Confirm the @repo section does not contain any ignored options.

        Any error reporting will be done by this function.

        :rtyp: bool
        :return: True if ignored options found, False otherwise.

        """
        has_ignored_options = False
        # Basically all possible options can appear in [@repo]
        if self.config.repo_config.has_section(p4gf_config.SECTION_REPO):
            all_options = set()
            all_options.add(p4gf_config.KEY_DESCRIPTION)
            all_options.add(p4gf_config.KEY_FORK_OF_REPO)
            all_options.add(p4gf_config.KEY_READ_ONLY)
            all_options.add(p4gf_config.KEY_FORK_OF_BRANCH_ID)
            all_options.add(p4gf_config.KEY_ENABLE_MISMATCHED_RHS)
            all_options.add(p4gf_config.KEY_FAST_PUSH_WORKING_STORAGE)
            default_cfg = p4gf_config.default_config_global()
            for section in default_cfg.sections():
                for option in default_cfg.options(section):
                    all_options.add(option)
            frm = _("repository configuration section '{section}' "
                    "contains ignored option '{option}'\n")
            for option in self.config.repo_config.options(p4gf_config.SECTION_REPO):
                if option not in all_options:
                    self._report_error(frm.format(section=p4gf_config.SECTION_REPO,
                                                  option=option))
                    has_ignored_options = True
        return has_ignored_options

    def _validate_new_depot_branch(self, config):
        '''Perform new depot branch validations.'''
        valid = True
        value = config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                               p4gf_config.KEY_NDB_ENABLE)
        if value != p4gf_config.VALUE_NDB_ENABLE_NO and \
                value != p4gf_config.VALUE_NDB_ENABLE_EXPLICIT and \
                value != p4gf_config.VALUE_NDB_ENABLE_ALL:
            self._report_error(_("Perforce: Improperly configured {key} value\n")
                               .format(key=p4gf_config.KEY_NDB_ENABLE))
            valid = False
        value = config.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                               p4gf_config.KEY_NDB_DEPOT_PATH)
        if not str(value).startswith("//"):
            self._report_error(_("Perforce: Improperly configured {key} value\n")
                               .format(key=p4gf_config.KEY_NDB_DEPOT_PATH))
            valid = False
        return valid

    def _is_global_valid(self):
        """Validate the global configuration file.

        :rtype: bool
        :return: True if global configuration appears valid, False otherwise.

        """
        # pylint:disable=too-many-branches
        valid = True
        default_cfg = p4gf_config.default_config_global()
        global_cfg = p4gf_config.GlobalConfig.instance(self.p4)
        frm_sec = _("global configuration contains ignored section '{section}'\n")
        frm_opt = _("global configuration section '{section}' contains ignored option '{option}'\n")
        for section in global_cfg.sections():
            # Ignore the @features section for the sake of backward compatibility.
            if section == p4gf_config.SECTION_FEATURES:
                continue
            if not default_cfg.has_section(section):
                self._report_error(frm_sec.format(section=section))
                valid = False
            else:
                for option in global_cfg.options(section):
                    if not default_cfg.has_option(section, option):
                        self._report_error(frm_opt.format(section=section, option=option))
                        valid = False

        # Ensure that boolean config settings are really boolean.
        frm_bool = _("Perforce: Improperly configured {option} value: must be boolean: {value}\n")
        for section, option in BOOLEAN_OPTIONS:
            try:
                global_cfg.getboolean(section, option)
            except ValueError:
                value = global_cfg.get(section, option)
                self._report_error(frm_bool.format(option=option, value=value))
                valid = False

        # Special values
        value = global_cfg.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                               p4gf_config.KEY_CHANGE_OWNER)
        if value != p4gf_config.VALUE_AUTHOR and value != p4gf_config.VALUE_PUSHER:
            self._report_error(_("Perforce: Improperly configured {key} value\n")
                               .format(key=p4gf_config.KEY_CHANGE_OWNER))
            valid = False

        # Ensure correct new_depo_branch settings
        if not self._validate_new_depot_branch(global_cfg):
            valid = False

        value = global_cfg.get(p4gf_config.SECTION_GIT_TO_PERFORCE,
                               p4gf_config.KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM)
        if value:
            try:
                int(value)
            except ValueError:
                self._report_error(_("Perforce: Improperly configured {key} value\n")
                                   .format(key=p4gf_config
                                           .KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM))
                valid = False

        valid &= self._value_expected(
              cfg                 = global_cfg
            , section_name        = p4gf_config.SECTION_GIT_TO_PERFORCE
            , key_name            = p4gf_config.KEY_FAST_PUSH_WORKING_STORAGE
            , expected_value_list =
                [ p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_DICT
                , p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MEMORY
                , p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_SINGLE_TABLE
                , p4gf_config.VALUE_FAST_PUSH_WORKING_STORAGE_SQLITE_MULTIPLE_TABLES
                ]
            )
        return valid

    def _value_expected(self, *
                , cfg
                , section_name
                , key_name
                , expected_value_list
                ):
        """If a key is configured, require that it be one of a list of expected
        values.

        Returun True if either not configured or if configured, expected.
        Return False if configured but with an unexpected value.
        """
        if key_name not in cfg[section_name]:
            return True
        value = cfg.get(section_name, key_name)
        if value not in expected_value_list:
            self._report_error(
                _("Perforce: Improperly configured {key} value {value}."
                  " Must be one of {expected}\n")
                .format( key      = key_name
                       , value    = value
                       , expected = expected_value_list))
            return False


def main():
    """Validate the configuration for one or more repositories."""
    # pylint:disable=too-many-branches
    desc = _("Report on the validity of a repository configuration.")
    parser = p4gf_util.create_arg_parser(desc)
    parser.add_argument('-a', '--all', action='store_true',
                        help=_('process all known Git Fusion repositories'))
    parser.add_argument(NTR('repos'), metavar=NTR('repo'), nargs='*',
                        help=_('name of repository or file to be validated'))
    args = parser.parse_args()

    # Check that either --all, or 'repos' was specified, but not both.
    if not args.all and len(args.repos) == 0:
        sys.stderr.write(_('Missing repo names; try adding --all option.\n'))
        sys.exit(2)
    if args.all and len(args.repos) > 0:
        sys.stderr.write(_('Ambiguous arguments. Choose --all or a repo name.\n'))
        sys.exit(2)

    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4_temp_client()
        if not p4:
            sys.exit(2)
        # Sanity check the connection (e.g. user logged in?) before proceeding.
        try:
            p4.fetch_client()
        except P4.P4Exception as e:
            sys.stderr.write(_('P4 exception occurred: {exception}').format(exception=e))
            sys.exit(1)

        p4gf_branch.init_case_handling(p4)

        if args.all:
            repos = p4gf_util.repo_config_list(p4)
            if len(repos) == 0:
                print(_('No Git Fusion repositories found, nothing to do.'))
                sys.exit(0)
        else:
            repos = args.repos

        for repo in repos:
            if os.path.exists(repo):
                print(_("Processing file {repo_name}...").format(repo_name=repo))
                try:
                    config = p4gf_config.RepoConfig.from_local_file(repo, p4, repo)
                except p4gf_config.ConfigLoadError as e:
                    sys.stderr.write("{}\n", e)
                except p4gf_config.ConfigParseError as e:
                    sys.stderr.write("{}\n", e)
            else:
                repo_name = p4gf_translate.TranslateReponame.git_to_repo(repo)
                print(_("Processing repository {repo_name}...").format(repo_name=repo_name))
                try:
                    config = p4gf_config.RepoConfig.from_depot_file(repo_name, p4)
                except p4gf_config.ConfigLoadError as err:
                    sys.stderr.write("{}\n", err)
            if Validator(config, p4).is_valid():
                print(_("ok"))
            print("")


if __name__ == "__main__":
    main()

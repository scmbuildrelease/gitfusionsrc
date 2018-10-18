#! /usr/bin/env python3.3
"""Git Fusion package constants."""

import os

from   p4gf_l10n import _, NTR

# pylint:disable=line-too-long

# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
#
# Normal usage of Git Fusion should not require changing of the
# P4GF_DEPOT constant. If a site requires a different depot name
# then set this constant on ALL Git Fusion instances to the same
# depot name.
#
# This depot should be created by hand prior to running any Git
# Fusion instance. Wild card and revision characters are not
# allowed in depot names (*, ..., @, #) and non-alphanumeric
# should typically be avoided.

P4GF_DEPOT              = NTR('.git-fusion')
P4GF_DEPOT_NAME         = NTR('P4GF_DEPOT')
P4GF_P4KEY_P4GF_DEPOT   = NTR('git-fusion-p4gf-depot')
P4GF_REQ_PATCH = { 2015.1: 1171507
                  ,2015.2: 1171507
                 }
#
#                 End block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------

P4GF_CLIENT_PREFIX                  = NTR("git-fusion-")
P4GF_REPO_CLIENT                    = NTR("git-fusion-{server_id}-{repo_name}")
P4GF_REPO_CLIENT_UNIQUE             = NTR("git-fusion-{repo_name}-{uuid}")
P4GF_REPO_TEMP_CLIENT               = NTR("git-fusion--temp-{server_id}-{repo_name}-{uuid}")
P4GF_REPO_READ_PERM_CLIENT          = NTR("git-fusion--readperm-{server_id}-{repo_name}-{uuid}")
P4GF_CONFIG_VALIDATE_CLIENT         = NTR("git-fusion--config-validate-{uuid}")
P4GF_SWARM_CLIENT                   = NTR("git-fusion--{repo_name}-swarm")
P4GF_OBJECT_CLIENT_PREFIX           = NTR("git-fusion--")
P4GF_OBJECT_CLIENT_12_2             = NTR("git-fusion--{hostname}")
P4GF_OBJECT_CLIENT                  = NTR("git-fusion--{server_id}")
P4GF_OBJECT_CLIENT_UNIQUE           = NTR("git-fusion--{server_id}-{uuid}")
P4GF_GROUP                          = NTR("git-fusion-group")
P4GF_USER                           = NTR("git-fusion-user")
P4GF_UNKNOWN_USER                   = NTR("unknown_git")

P4GF_GROUP_REPO_PULL                = NTR("git-fusion-{repo}-pull")
P4GF_GROUP_REPO_PUSH                = NTR("git-fusion-{repo}-push")
P4GF_GROUP_PULL                     = NTR("git-fusion-pull")
P4GF_GROUP_PUSH                     = NTR("git-fusion-push")

                                        # Use hyphens, not underscores.
P4GF_P4KEY_INIT_STARTED             = NTR('git-fusion-init-started')
P4GF_P4KEY_INIT_COMPLETE            = NTR('git-fusion-init-complete')
P4GF_P4KEY_UPGRADE_STARTED          = NTR('git-fusion-upgrade-started')
P4GF_P4KEY_UPGRADE_COMPLETE         = NTR('git-fusion-upgrade-complete')
P4GF_P4KEY_PERMISSION_GROUP_DEFAULT \
                                    = NTR('git-fusion-permission-group-default')
P4GF_P4KEY_PUSH_STARTED             = NTR('git-fusion-{repo_name}-push-start')
P4GF_P4KEY_LAST_COPIED_CHANGE       = NTR('git-fusion-{repo_name}-{server_id}-last-copied-changelist-number')
P4GF_P4KEY_LAST_SEEN_CHANGE         = NTR('git-fusion-{repo_name}-{server_id}-last-seen-changelist-number')
P4GF_P4KEY_REPO_SERVER_CONFIG_REV   = NTR('git-fusion-{repo_name}-{server_id}-p4gf-config-rev')
P4GF_P4KEY_PERM_CHECK               = NTR('git-fusion-auth-server-perm-check')
# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
#
P4GF_P4KEY_LOCK_VIEW                = NTR('git-fusion-view-{repo_name}-lock')
P4GF_P4KEY_LOCK_VIEW_OWNERS         = NTR('git-fusion-view-{repo_name}-lock-owners')
#
#                 End block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------
P4GF_P4KEY_SERVER_ID                = NTR('git-fusion-server-id-')
P4GF_P4KEY_LOCK_SPACE               = NTR('git-fusion-space-lock')

P4GF_P4KEY_PENDING_MB               = NTR('git-fusion-view-{repo_name}-pending-mb')
P4GF_P4KEY_TOTAL_MB                 = NTR('git-fusion-view-{repo_name}-total-mb')

P4GF_P4KEY_ALL_PENDING_MB           = NTR('git-fusion-space-pending-mb')
P4GF_P4KEY_ALL_REMAINING_MB         = NTR('git-fusion-space-remaining-mb')

P4GF_P4KEY_UPDATE_AUTH_KEYS         = NTR('git-fusion-auth-keys-last-changenum-{}')

# Needed to check for an un-upgraded 2012.2 install
P4GF_P4KEY_OLD_UPDATE_AUTH_KEYS 	  = NTR('p4gf_auth_keys_last_changenum-{}')

P4GF_P4KEY_TIME_ZONE_NAME           = NTR('git-fusion-perforce-time-zone-name')

P4GF_P4KEY_PREVENT_NEW_SESSIONS     = NTR('git-fusion-prevent-new-sessions')
P4GF_P4KEY_LAST_COPIED_TAG          = NTR('git-fusion-{repo_name}-{server_id}-last-copied-tag')
P4GF_P4KEY_READ_PERMISSION_CHECK    = NTR('git-fusion-read-permission-check')
P4GF_P4KEY_DISABLE_ERROR_CLEANUP    = NTR('git-fusion-disable-error-cleanup')
P4GF_P4KEY_REV_SHA1                 = NTR('git-fusion-{repo_name}-rev-sha1-{change_num}')
P4GF_P4KEY_CHECKPOINT               = NTR('git-fusion-checkpoint-{repo_name}-{server_id}-{name}')
P4GF_P4KEY_REPO_STATUS              = NTR('git-fusion-status-{repo_name}')
P4GF_P4KEY_REPO_STATUS_PUSH_ID      = NTR('git-fusion-status-{repo_name}-{push_id}')
P4GF_P4KEY_REPO_PUSH_ID             = NTR('git-fusion-push-id-{repo_name}')
P4GF_P4KEY_PROXY_PROTECTS           = NTR('git-fusion-proxy-protects')


      # for shorter names: OT  = Object Type
      #                    LCN = Last Change Num
P4GF_P4KEY_INDEX_OT                 = NTR('git-fusion-index-branch-{repo_name},{change_num},{branch_id}')
P4GF_P4KEY_INDEX_OT_REPO_ALL        = NTR('git-fusion-index-branch-{repo_name},*')
P4GF_P4KEY_INDEX_LCN_ON_BRANCH      = NTR('git-fusion-index-last-{repo_name},{branch_id}')
P4GF_P4KEY_INDEX_LCN_ON_BRANCH_REPO_ALL = NTR('git-fusion-index-last-{repo_name},*')

P4GF_BRANCH_EMPTY_REPO              = NTR('p4gf_empty_repo')
P4GF_BRANCH_TEMP_N                  = NTR('git-fusion-temp-branch-{}')

# Environment vars
P4GF_AUTH_P4USER                    = NTR('P4GF_AUTH_P4USER')
P4GF_FORK_PUSH                      = NTR('P4GF_FORK_PUSH')
P4GF_FORUSER                        = NTR('P4GF_FORUSER')

# Internal debugging keys
# section in rc file for test vars
P4GF_TEST                           = NTR('test')

# Assign sequential UUID numbers so that test scripts
# get the same results every time.
P4GF_TEST_UUID_SEQUENTIAL           = NTR('P4GF_TEST_UUID_SEQUENTIAL')


# Internal testing environment variables.
# Read config from here, not /etc/git-fusion.log.conf
P4GF_TEST_LOG_CONFIG_PATH           = NTR('P4GF_LOG_CONFIG_FILE')

# Logging related environment variables.
P4GF_LOG_UUID                       = NTR('P4GF_LOG_UUID')
P4GF_LOG_REPO                       = NTR('P4GF_LOG_REPO')
P4GF_LOGS_DIR                       = NTR('P4GF_LOGS_DIR')

# Set the LFS_FILE_MAX_SECONDS_TO_KEEP from the environment
P4GF_TEST_LFS_FILE_MAX_SECONDS_TO_KEEP = NTR('LFS_FILE_MAX_SECONDS_TO_KEEP')

# Label/tag added to .gitmodules file to indicate a submodule that is
# managed by Git Fusion via the stream-imports-as-submodules feature.
P4GF_MODULE_TAG                     = NTR('p4gf')

# Filenames
P4GF_DIR                            = NTR('.git-fusion')
P4GF_ID_FILE                        = NTR('server-id')
P4GF_MOTD_FILE                      = NTR('{P4GF_DIR}/motd.txt')
P4GF_FAILURE_LOG                    = NTR('{P4GF_DIR}/logs/{prefix}{date}.log.txt')
P4GF_SWARM_PRT                      = NTR('swarm-pre-receive-list')
P4GF_NEW_DEPOT_BRANCH_LIST          = NTR('new-depot-branch-list')
P4GF_PRE_RECEIVE_FLAG               = NTR('pre-receive-flag')

# P4GF_HOME
P4GF_HOME = os.path.expanduser(os.path.join("~", P4GF_DIR))
P4GF_HOME_NAME                  = NTR('P4GF_HOME')

# In support of the P4GF_ENV configuration
P4GF_ENV                         = None               # set from the env var P4GF_ENV, if it exists
P4GF_ENV_NAME                    = NTR('P4GF_ENV')
READ_ONLY                        = False
READ_ONLY_NAME                   = NTR('READ_ONLY')
MAX_TEMP_CLIENTS                 = 10
MAX_TEMP_CLIENTS_NAME            = NTR('MAX_TEMP_CLIENTS')
GIT_BIN_DEFAULT                  = 'git'
GIT_BIN_NAME                     = 'GIT_BIN'
GIT_BIN                          = GIT_BIN_DEFAULT

# section definition here avoids circularity issues with p4gf_env_config and p4gf_config
SECTION_ENVIRONMENT       = NTR('environment')

# Perforce copies of Git commit and ls-tree objects live under this root.
P4GF_OBJECTS_ROOT                   = NTR('//{P4GF_DEPOT}/objects')

# Config files (stored in Perforce, not local filesystem)
P4GF_CONFIG_GLOBAL                  = NTR('//{P4GF_DEPOT}/p4gf_config')
P4GF_CONFIG_REPO                    = NTR('//{P4GF_DEPOT}/repos/{repo_name}/p4gf_config')
P4GF_CONFIG_REPO2                   = NTR('//{P4GF_DEPOT}/repos/{repo_name}/p4gf_config2')

P4GF_DEPOT_BRANCH_ROOT              = NTR("//{P4GF_DEPOT}/branches/{repo_name}/{branch_id}")

# branch-info files, separate from the versioned files that the branch stores.
# Nothing but branch-info files can be below this root.
P4GF_DEPOT_BRANCH_INFO_ROOT         = NTR("//{P4GF_DEPOT}/branch-info")

P4GF_CHANGELIST_DATA_FILE           = NTR('//{P4GF_DEPOT}/changelists/{repo_name}/{change_num}')


# Placed in change description when importing from Git to Perforce.
        ### We'll swap these two headers later. Will need to update test
        ### scripts to deal with new header.
P4GF_IMPORT_HEADER                  = NTR('Imported from Git')
P4GF_IMPORT_HEADER_OLD              = NTR('Git Fusion additional data:')
P4GF_DESC_KEY_AUTHOR                = NTR('Author')      # Do not change: required by git-fast-import
P4GF_DESC_KEY_COMMITTER             = NTR('Committer')   # Do not change: required by git-fast-import
P4GF_DESC_KEY_PUSHER                = NTR('Pusher')
P4GF_DESC_KEY_SHA1                  = NTR('sha1')
P4GF_DESC_KEY_PUSH_STATE            = NTR('push-state')
P4GF_DESC_KEY_DEPOT_BRANCH_ID       = NTR('depot-branch-id')
P4GF_DESC_KEY_CONTAINS_P4_EXTRA     = NTR('contains-p4-extra')
P4GF_DESC_KEY_GITLINK               = NTR('gitlink')
P4GF_DESC_KEY_PARENTS               = NTR('parents')
P4GF_DESC_KEY_PARENT_CHANGES        = NTR('parent-changes')
P4GF_DESC_KEY_PARENT_BRANCH         = NTR('parent-branch')
P4GF_DESC_KEY_GHOST_OF_SHA1         = NTR('ghost-of-sha1')
P4GF_DESC_KEY_GHOST_OF_CHANGE_NUM   = NTR('ghost-of-change-num')
P4GF_DESC_KEY_GHOST_PRECEDES_SHA1   = NTR('ghost-precedes-sha1')

# Placed in commit message when exporting from Perforce to Git
P4GF_EXPORT_HEADER                  = NTR('Copied from Perforce')
P4GF_DESC_KEY_CHANGE                = NTR('Change')
P4GF_DESC_KEY_SERVERID              = NTR('ServerID')

# 'git clone' of these views (or pulling or fetching or pushing) runs special commands
P4GF_UNREPO_INFO                    = NTR('@info')      # Returns our version text
P4GF_UNREPO_LIST                    = NTR('@list')      # Returns list of repos visible to user
P4GF_UNREPO_HELP                    = NTR('@help')      # Returns contents of help.txt, if present
P4GF_UNREPO_FEATURES                = NTR('@features')  # Reports enabled state of features
P4GF_UNREPO_WAIT                    = NTR('@wait')      # wait for lock to be released
P4GF_UNREPO_STATUS                  = NTR('@status')    # report status of push operation
P4GF_UNREPO_PROGRESS                = NTR('@progress')  # report status changes while waiting
P4GF_UNREPO_CONFIG                  = NTR('@config')    # dump configuration of repository
P4GF_UNREPO = [
    P4GF_UNREPO_INFO,
    P4GF_UNREPO_LIST,
    P4GF_UNREPO_HELP,
    P4GF_UNREPO_FEATURES,
    P4GF_UNREPO_WAIT,
    P4GF_UNREPO_STATUS,
    P4GF_UNREPO_PROGRESS,
    P4GF_UNREPO_CONFIG
]
# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
#
# Atomic Push
#
# Atomic view locking requires special p4keys and users to insert Reviews into
# the user spec Each Git Fusion server has its own lock.
#
P4GF_REVIEWS_GF                     = NTR('git-fusion-reviews-') # Append GF server_id.
P4GF_REVIEWS__NON_GF                = P4GF_REVIEWS_GF + NTR('-non-gf')
P4GF_REVIEWS__ALL_GF                = P4GF_REVIEWS_GF + NTR('-all-gf')
P4GF_REVIEWS_NON_GF_SUBMIT          = NTR('git-fusion-non-gf-submit-')
P4GF_REVIEWS_NON_GF_RESET           = NTR('git-fusion-non-gf-')
DEBUG_P4GF_REVIEWS__NON_GF          = NTR('DEBUG-') + P4GF_REVIEWS__NON_GF
DEBUG_SKIP_P4GF_REVIEWS__NON_GF     = NTR('DEBUG-SKIP-') + P4GF_REVIEWS__NON_GF
P4GF_REVIEWS_SERVICEUSER            = P4GF_REVIEWS_GF + '{0}'
NON_GF_REVIEWS_BEGIN_MARKER_PATTERN = '//GF-{0}/BEGIN'
NON_GF_REVIEWS_END_MARKER_PATTERN   = '//GF-{0}/END'
P4GF_REVIEWS_COMMON_LOCK            = NTR('git-fusion-reviews-common-lock')
P4GF_REVIEWS_COMMON_LOCK_OWNER      = NTR('git-fusion-reviews-common-lock-owner')

# Is the Atomic Push submit trigger installed and at the correct version?
#
P4GF_P4KEY_TRIGGER_VERSION      = NTR('git-fusion-submit-trigger-version')
P4GF_TRIGGER_VERSION                = NTR('2016.1')
P4GF_P4KEY_LOCK_USER                = NTR('git-fusion-user-{user_name}-lock')

#
#                 End block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------
P4GF_LOCKED_BY_MSG                  = _("Files in the push are locked by '{user}'")

NULL_COMMIT_SHA1                    = '0' * 40
NULL_SHA1                           = '0' * 40
EMPTY_TREE_SHA1                     = NTR('4b825dc642cb6eb9a060e54bf8d69288fbee4904')
EMPTY_BLOB_SHA1                     = NTR('e69de29bb2d1d6434b8b29ae775ad8c2e48c5391')
GITATTRIBUTES                       = NTR('.gitattributes')

# Git-Swarm reviews: Git branch name prefix for any Swarm review.
P4GF_GIT_SWARM_REF_PREFIX_SHORT     = "review/"
P4GF_GIT_SWARM_REF_PREFIX_FULL      = "refs/heads/review/"

# File added in rare case when Git commit's own ls-tree is empty
# and does not differ from first-parent (if any).
P4GF_EMPTY_CHANGELIST_PLACEHOLDER   = NTR('.p4gf_empty_changelist_placeholder')

# 'p4 client' options for any client spec we create.
CLIENT_OPTIONS = NTR('allwrite clobber nocompress locked nomodtime normdir')

NO_SUCH_CLIENT_DEFINED = _("No such client '{}' defined")
NO_SUCH_REPO_DEFINED = _("No such repo '{}' defined")
NO_REPO_FROM_CLIENT_MSG_TEMPLATE = _("""
{nop4client}To define a Git Fusion repo:
* create a Git Fusion repo configuration file
  then check it into //P4GF_DEPOT/repos/{repo_name}/p4gf_config, or
* ask your Git Fusion administrator to
  create a Git Fusion repo configuration file
  then specify it on the command line to p4gf_init_repo.py --config <file>,
  run on the Git Fusion server
""")
NO_REPO_MSG_TEMPLATE = _("""
Git Fusion repo '{repo_name}' does not exist.
{nop4client}To define a Git Fusion repo:
* create a Perforce client spec '{repo_name_p4client}', or
* create a Git Fusion repo configuration file
  then check it into //P4GF_DEPOT/repos/{repo_name}/p4gf_config, or
* ask your Git Fusion administrator to
  create a Git Fusion repo configuration file
  then specify it on the command line to p4gf_init_repo.py --config <file>,
  run on the Git Fusion server
""")
DELETED_P4GF_CONFIG_MSG_TEMPLATE = _("""
The Git Fusion config file for repo '{repo_name}' has been deleted:
  //P4GF_DEPOT/repos/{repo_name}/p4gf_config
To define this Git Fusion repo:
* re-create the Git Fusion repo configuration file
  then check it into //P4GF_DEPOT/repos/{repo_name}/p4gf_config, or
* ask your Git Fusion administrator to
  create a Git Fusion repo configuration file
  then specify it on the command line to p4gf_init_repo.py --config <file>,
  run on the Git Fusion server
""")
MISSING_P4GF_CONFIG_MSG_TEMPLATE = _("""
The Git Fusion config file for repo '{repo_name}' has been obliterated:
  //P4GF_DEPOT/repos/{repo_name}/p4gf_config
* Contact your Git Fusion administrator to
  address this situation and configure this repo.
""")
EMPTY_VIEWS_MSG_TEMPLATE = _("""
Git Fusion repo '{repo_name}' cannot be created.
The views/exclusions for a branch/client allow no paths.
To define a Git Fusion repo:
* create a Perforce client spec '{repo_name_p4client}', or
* create a Git Fusion repo configuration file
  then check it into //P4GF_DEPOT/repos/{repo_name}/p4gf_config, or
* ask your Git Fusion administrator to
  create a Git Fusion repo configuration file
  then specify it on the command line to p4gf_init_repo.py --config <file>,
  run on the Git Fusion server
""")
# -- substitution-savvy functions ---------------------------------------------


def objects_root():
    """Return //P4GF_DEPOT/objects."""
    return P4GF_OBJECTS_ROOT.format(P4GF_DEPOT=P4GF_DEPOT)

# Not officially tested or supported, but quite useful: import any environment
# variables starting P4GF_ as overrides to replace the above constants.
#
# h/t to Ravenbrook for the feature.
# https://github.com/Ravenbrook/perforce-git-fusion/commit/5cace4df621b91ba8b3b20059400af5a3e0837f2
#
# Commented out until we can find all the places in our test machinery that set
# P4GF_ environment variables that break the automated tests.
#
# import os
# locals().update({ key:value
#                   for key, value in os.environ.items()
#                   if key.startswith('P4GF_')})

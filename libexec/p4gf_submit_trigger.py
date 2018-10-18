#! /usr/bin/env python
"""Git Fusion submit triggers.

    These triggers coordinate with Git Fusion to support git atomic pushes.
    Service user accounts use p4 user Reviews to manage list of locked files.
    There is one service user per Git Fusion instance
    and one for non Git Fusion submits.
    This trigger is compatible with python versions 2.x >= 2.6 and >= 3.3
    The trigger is compatible with p4d versions >= 2015.1.
    For distributed p4d triggers are installed only on the commit server.
    Submits from edge servers are handled by the commit server.
"""
# pylint:disable=W9903
# Skip localization/translation warnings about config strings
# here at the top of the file.

# -- Configuration ------------------------------------------------------------
# Edit these constants to match your p4d server and environment.

# Set the external configuration file location (relative to the script location
# or an absolute path)
P4GF_TRIGGER_CONF      = "p4gf_submit_trigger.cfg"
P4GF_TRIGGER_CONF_SETTINGS      = ""
CONFIG_PATH_ARG_PREFIX = '--config-path='    # prefix to identify alternate config path argument
CONFIG_PATH_ARG        = ''  # actual arg set into trigger entries by --generate
CONFIG_PATH_INDEX      = None  # used to insert CONFIG_PATH_ARG back into sys.argv
                               # for sudo re-invocation with  --install option

SERVER_ID_ARG_PREFIX = '--git-fusion-server='  # prefix to identify server id

# If a trigger configuration file is found, the configuration will be read from
# that file: Anything set there will override the configuration below.

# For unicode servers uncomment the following line
# CHARSET = ['-C', 'utf8']
CHARSET = []

P4GF_P4_BIN_PATH_VAR_NAME = "P4GF_P4_BIN_PATH"
P4GF_P4_BIN_CONFIG_OPTION_NAME = "P4GF_P4_BIN"
P4GF_P4_BIN_PATH_CONFIG_OPTION_NAME = "P4GF_P4_BIN_PATH"

# Set to the location of the p4 binary.
# When in doubt, change this to an absolute path.
P4GF_P4_BIN_PATH = "p4"

# For Windows systems use no spaces in the p4.exe path
# P4GF_P4_BIN_PATH = "C:\PROGRA~1\Perforce\p4.exe"

# If running P4D with a P4PORT bound to a specific network (as opposed to
# all of them, as in P4PORT=1666), then set this to the name of the host to
# which P4D is bound (e.g. p4prod.example.com). See also P4PORT below.
DEFAULT_P4HOST = 'localhost'

# By default P4PORT is set from the p4d trigger %serverport% argument.
# Admins optionally may override the %serverport% by setting P4PORT here to
# a non-empty string.
P4PORT = None

# If P4TICKETS is set , export value via os.environ
P4TICKETS = None

# If P4TRUST is set , export value via os.environ
P4TRUST = None
# End P4 configurables
# -----------------------------------------------------------------------------

import json
import sys
import inspect
import uuid

# Determine python version
PYTHON3 = True
if sys.hexversion < 0x03000000:
    PYTHON3 = False

# Exit codes for triggers, sys.exit(CODE)
P4PASS = 0
P4FAIL = 1

KEY_VIEW = 'view'
KEY_STREAM = 'stream'

# user password status from 'login -s'
LOGIN_NO_PASSWD    = 0
LOGIN_NEEDS_TICKET = 1
LOGIN_HAS_TICKET   = 2

# Create a unique client name to use with 'p4' calls.
# The client is not required, but it prevents failures
# should the default client (hostname)
# be identical to a Perforce depot name.
P4CLIENT = "GF-TRIGGER-{0}".format(str(uuid.uuid1())[0:12])

# Import the configparser - either from python2 or python3
try:
    # python3.x import
    import configparser                 # pylint: disable=import-error
    PARSING_ERROR = configparser.Error  # pylint: disable=invalid-name
except ImportError:
    # python2.x import
    import cStringIO
    import ConfigParser
    PARSING_ERROR = ConfigParser.Error  # pylint: disable=invalid-name
    configparser = ConfigParser         # pylint: disable=invalid-name

P4GF_USER = "git-fusion-user"

import os
import re
import platform
import getpass

                        # Optional localization/translation support.
                        # If the rest of Git Fusion's bin folder
                        # was copied along with this file p4gf_submit_trigger.py,
                        # then this block loads LC_MESSAGES .mo files
                        # to support languages other than US English.
try:
    from p4gf_l10n import _, NTR
except ImportError:
                        # Invalid name NTR()
    def NTR(x):         # pylint: disable=invalid-name
        """No-TRanslate: Localization marker for string constants."""
        return x
    _ = NTR
                        # pylint:enable=W9903

# Import pwd for username -> gid
# Only available on *nix
if platform.system() != "Windows":
    import pwd
    import stat

# Get and store the full paths to this script and the interpreter
SCRIPT_PATH = os.path.realpath(__file__)
PYTHON_PATH = sys.executable
P4GF_TRIGGER_CONF   = os.path.join(os.path.dirname(SCRIPT_PATH),"p4gf_submit_trigger.cfg")

# Try and load external configuration
CFG_SECTION = "configuration"
CFG_EXTERNAL = False

def get_datetime(with_year=True):
    """Generate a date/time string for the lock acquisition time."""
    date = datetime.datetime.now()
    if with_year:
        return date.isoformat(sep=' ').split('.')[0]
    else:
        return date.strftime('%m-%d %H:%M:%S')
def debug_log(msg, lineno=None):
    """Log message to the debug log."""
    if not lineno:
        lineno = inspect.currentframe().f_back.f_lineno
    DEBUG_LOG_FILE.write("{0} {1} line:{2:5}  ::  {3}\n".
            format(PROCESS_PID, get_datetime(with_year=False), lineno, msg))
    DEBUG_LOG_FILE.flush()


def fix_text_encoding(text):
    """Ensure the string is encodable using the stdout encoding.

    :param str text: string to be re-coded, if necessary.
    :return: a safe version of the string which can be printed.

    """
    encoding = sys.stdout.encoding
    if encoding is None:
        encoding = sys.getdefaultencoding()
    if not PYTHON3:
        text = unicode(text, encoding, 'replace')  # pylint:disable=undefined-variable
    try:
        text.encode(encoding)
        return text
    except UnicodeEncodeError:
        bites = text.encode(encoding, 'backslashreplace')
        result = bites.decode(encoding, 'strict')
        return result


def print_log(msg):
    """Print and optionally log to debug log."""
    msg = fix_text_encoding(msg)
    print(msg)
    if DEBUG_LOG_FILE:
        debug_log(msg, lineno=inspect.currentframe().f_back.f_lineno)


def is_trigger_install():
    """Return true if this is a trigger installation invocation.

    Return true if arguments have:
    --install
    --install_trigger_entries
    --generate_trigger_entries
    """
    install_args = ['--install','--install_trigger_entries', '--generate_trigger_entries']
    for arg in sys.argv:
        if arg in install_args:
            return True
    return False

MSG_CONFIG_FILE_MISSING = _(
    "Git Fusion Trigger: config file does not exist:'{path}'")
MSG_CONFIG_PATH_NOT_ABSOLUTE = _(
    "Git Fusion Trigger: argument must provide an absolute path:'{path}'")


def validate_config_path(config_path_file_path):
    """Ensure the config_path is absolute and exists."""
    if not len(config_path_file_path) or not os.path.isabs(config_path_file_path):
        print(MSG_CONFIG_PATH_NOT_ABSOLUTE.format(path=CONFIG_PATH_ARG))
        sys.exit(P4FAIL)
    if not is_trigger_install() and not os.path.exists(config_path_file_path):
        print(MSG_CONFIG_FILE_MISSING.format(path=config_path_file_path))
        sys.exit(P4FAIL)
    return config_path_file_path


def check_and_extract_no_config_from_args():
    """Remove the --no-config argument from sys.arg.

    if --no-config was present
        remove --no-config from sys.arg if command not --install
        return True
    else
        return False

    """
    for i, arg in enumerate(sys.argv):
        if arg == '--no-config':
            # Only the --install command needs to retain the --no-config arg
            if sys.argv[1] != '--install':
                del sys.argv[i]
            return True
    return False

def check_and_extract_debug_from_args():
    """Remove the --debug argument from sys.arg.

    if --debug was present
        remove --debug from sys.arg
        return True
    else
        return False

    """
    for i, arg in enumerate(sys.argv):
        if arg == '--debug':
            del sys.argv[i]
            return True
    return False


def extract_config_file_path_from_args():
    """Remove and set the--config-path argument from sys.arg.

    If  --config-path=/absolute/path/to/config argument exists:
        remove it from sys.arg, validate and return it
        Failed validation will FAIL the trigger with error message.
    Else return None
    """
    global CONFIG_PATH_ARG, CONFIG_PATH_INDEX
    for i, arg in enumerate(sys.argv):
        if arg.startswith(CONFIG_PATH_ARG_PREFIX):
            CONFIG_PATH_ARG   = arg
            CONFIG_PATH_INDEX = i
            config_path_file_path = arg.replace(CONFIG_PATH_ARG_PREFIX, '')
            del sys.argv[i]
            return validate_config_path(config_path_file_path)
        # missing the '=' at the end of --config-path=?
        elif arg.startswith(CONFIG_PATH_ARG_PREFIX[:-1]):
            CONFIG_PATH_ARG = arg
            print_log(_("'{path_arg}' argument must provide an absolute path.")
                      .format(path_arg=CONFIG_PATH_ARG))
            sys.exit(P4FAIL)
    return None


def extract_server_id_from_args():
    """Remove and set the --git-fusion-server argument from sys.arg."""
    for i, arg in enumerate(sys.argv):
        if arg.startswith(SERVER_ID_ARG_PREFIX):
            del sys.argv[i]
            return arg.replace(SERVER_ID_ARG_PREFIX, '')
    return None


def set_globals_from_config_file():
    """Detect and parse the p4gf_submit_trigger.cfg setting configuration variables.

    A missing config file is passed. A missing default path is acceptable.
    A missing --config-path=<path> is reported and failed earlier
    in extract_config_file_path_from_args().
    """
    # pylint: disable=too-many-branches
    global P4GF_TRIGGER_CONF, DEFAULT_P4HOST, CHARSET, DEBUG_LOG_PATH
    global P4GF_P4_BIN_PATH, P4PORT, P4TICKETS, P4TRUST, CFG_EXTERNAL
    global P4GF_TRIGGER_CONF_SETTINGS
    # If the path is absolute, just try and read it
    if isinstance(P4GF_TRIGGER_CONF, str) and not os.path.isabs(P4GF_TRIGGER_CONF):
        # If the path is relative, make it relative to the script, not CWD
        P4GF_TRIGGER_CONF = os.path.abspath(os.path.join(
            os.path.dirname(SCRIPT_PATH), P4GF_TRIGGER_CONF))

    if isinstance(P4GF_TRIGGER_CONF, str) and os.path.isfile(P4GF_TRIGGER_CONF):
        try:
            TRIG_CONFIG = configparser.ConfigParser()   # pylint: disable=invalid-name
            TRIG_CONFIG.read(P4GF_TRIGGER_CONF)
            if TRIG_CONFIG.has_section(CFG_SECTION):
                CFG_EXTERNAL = True
                if TRIG_CONFIG.has_option(CFG_SECTION, "DEFAULT_P4HOST"):
                    DEFAULT_P4HOST = TRIG_CONFIG.get(CFG_SECTION, "DEFAULT_P4HOST")
                    P4GF_TRIGGER_CONF_SETTINGS += "\n    DEFAULT_P4HOST={0}".format(DEFAULT_P4HOST)
                if TRIG_CONFIG.has_option(CFG_SECTION, "P4CHARSET"):
                    CHARSET = ["-C", TRIG_CONFIG.get(CFG_SECTION, "P4CHARSET")]
                    P4GF_TRIGGER_CONF_SETTINGS += "\n    CHARSET={0}".format(CHARSET)
                if TRIG_CONFIG.has_option(CFG_SECTION, P4GF_P4_BIN_CONFIG_OPTION_NAME):
                    P4GF_P4_BIN_PATH = TRIG_CONFIG.get(CFG_SECTION,
                            P4GF_P4_BIN_CONFIG_OPTION_NAME)
                    P4GF_TRIGGER_CONF_SETTINGS += "\n    P4GF_P4_BIN_PATH={0}".format(
                            P4GF_P4_BIN_PATH)
                elif TRIG_CONFIG.has_option(CFG_SECTION, P4GF_P4_BIN_PATH_CONFIG_OPTION_NAME):
                    P4GF_P4_BIN_PATH = TRIG_CONFIG.get(CFG_SECTION,
                            P4GF_P4_BIN_PATH_CONFIG_OPTION_NAME)
                    P4GF_TRIGGER_CONF_SETTINGS += "\n    P4GF_P4_BIN_PATH={0}".format(
                            P4GF_P4_BIN_PATH)
                if TRIG_CONFIG.has_option(CFG_SECTION, "P4PORT"):
                    P4PORT = TRIG_CONFIG.get(CFG_SECTION, "P4PORT")
                    P4GF_TRIGGER_CONF_SETTINGS += "\n    P4PORT={0}".format(P4PORT)
                if TRIG_CONFIG.has_option(CFG_SECTION, "P4TICKETS"):
                    P4TICKETS = TRIG_CONFIG.get(CFG_SECTION, "P4TICKETS")
                    P4GF_TRIGGER_CONF_SETTINGS += "\n    P4TICKETS={0}".format(P4TICKETS)
                if TRIG_CONFIG.has_option(CFG_SECTION, "P4TRUST"):
                    P4TRUST = TRIG_CONFIG.get(CFG_SECTION, "P4TRUST")
                    P4GF_TRIGGER_CONF_SETTINGS += "\n    P4TRUST={0}".format(P4TRUST)
                if TRIG_CONFIG.has_option(CFG_SECTION, "DEBUG-LOG-PATH"):
                    DEBUG_LOG_PATH = TRIG_CONFIG.get(CFG_SECTION, "DEBUG-LOG-PATH")
                    P4GF_TRIGGER_CONF_SETTINGS += "\n    DEBUG_LOG_PATH={0}".format(DEBUG_LOG_PATH)
            else:
                raise Exception(_("Didn't find section {section} in configuration file {path}")
                                .format(section=CFG_SECTION, path=P4GF_TRIGGER_CONF))
        except Exception as config_e:   # pylint: disable=broad-except
            print(_("Failed to load configuration from external file {path}").
                  format(path=P4GF_TRIGGER_CONF))
            print(_("Error: {exception}").format(exception=config_e))
            sys.exit(P4FAIL)

def skip_trigger_if_gf_user():
    """Permit Git Fusion to operate without engaging its own triggers.
    Triggers are to be applied only to non P4GF_USER.

    This is required for Windows as the trigger bash filter does
    not exist

    P4GF_USER changes to the p4gf_config are reviewed within the core
    Git Fusion process as it holds the locks and knows what it is
    doing. XXX One day this should probably only be done within the
    trigger to minimize duplicated code.
    """

    # The option --config-path=<path> trigger argument has been
    # removed from sys.argv at the time this is called
    # Permit the '--<option>' commands to be run by P4GF_USER
    # Thus pass P4GF_USER only for trigger table invocations of this trigger
    if len(sys.argv) >= 6 and not sys.argv[1].startswith('--') and sys.argv[3] == P4GF_USER:
        sys.exit(P4PASS)   # continue the submit but skip the trigger for GF

LOAD_CONFIG = True

# DEBUG LOG support
DEBUG_LOG       = False
DEBUG_LOG_FILE  = None
DEBUG_LOG_PATH  = None


# check for the --debug argument and remove from sys.argv
DEBUG_LOG  = check_and_extract_debug_from_args()
if DEBUG_LOG:   # set the default debug log path
    TMPDIR = ''
    # Find a suitable directory
    # This VAR list spans linux and Windos
    for tdir in ('TMPDIR', 'TEMP', 'TMP'):
        if tdir in os.environ and os.path.isdir(os.environ[tdir]):
            TMPDIR = os.environ[tdir]
            break
    # If nothing set and OS is linux, maybe we can use '/tmp'
    if not TMPDIR and platform.system() != "Windows" and os.path.isdir('/tmp'):
        TMPDIR = '/tmp'
    # we may have a suitable directory
    if TMPDIR:
        DEBUG_LOG_PATH = TMPDIR + '/p4gf_submit_trigger.log'
    PROCESS_PID = os.getpid()

# non trigger invocations of this file will have an command option '--<command>'
IS_OPTION_COMMAND = len(sys.argv) >= 2 and sys.argv[1].startswith('--')
# The optional comands will always ignore any trigger config file.
# Connection arguments are provided on the command line.
# The previously supported '--no-config' option is now removed form argv
# except in the case of '--install'. In this '--install' case the presence of
# the '--no-config' argument prevents creation of trigger config file.
if IS_OPTION_COMMAND:
    LOAD_CONFIG = not check_and_extract_no_config_from_args()
    if '--show-config' in sys.argv:
        LOAD_CONFIG = False


# Fetch configuration before testing for presence of p4 executable
# --config-path argument
CONFIG_PATH_FILE_PATH = extract_config_file_path_from_args()
if CONFIG_PATH_FILE_PATH:
    P4GF_TRIGGER_CONF = CONFIG_PATH_FILE_PATH
    CFG_EXTERNAL = True

# Get the review user name, if provided via --git-fusion-server
SERVER_ID_NAME = extract_server_id_from_args()

# Ignore the trigger default config file for non-trigger invocations
# The these config files
if LOAD_CONFIG:
    # DEBUG_LOG_PATH may be configured from the trigger config file
    set_globals_from_config_file()

# Disable the --debug request
#   if we did not find a suitable default directory
#   neither was it configured in the trigger cfg file
#   or it was configured but it's dirname does not exist
#
if DEBUG_LOG:
    if not DEBUG_LOG_PATH or not os.path.isdir(os.path.dirname(DEBUG_LOG_PATH)):
        DEBUG_LOG = None
# Find the 'p4' command line tool.
# If this fails, edit P4GF_P4_BIN_PATH in the "Configuration"
# block at the top of this file or in the p4gf_submit_trigger.cfg file.

# Now open the DEBUG_LOG_FILE if needed.
import datetime
if DEBUG_LOG:
    try:
        DEBUG_LOG_FILE = open(DEBUG_LOG_PATH,'a')
    except IOError as debug_file_e:
        print ("Git Fusion submit trigger: cannot open debug log {0}".
                format(DEBUG_LOG_PATH))
        print (   "I/O error({0}): {1}".format(debug_file_e.errno, debug_file_e.strerror))
        print ("   Continue without debug logging")

    if DEBUG_LOG_FILE and P4GF_TRIGGER_CONF_SETTINGS:
        debug_log("Using settings from trigger config: " + P4GF_TRIGGER_CONF)
        debug_log(P4GF_TRIGGER_CONF_SETTINGS.rstrip(', '))

import distutils.spawn
P4GF_P4_BIN = distutils.spawn.find_executable(P4GF_P4_BIN_PATH.strip('"\''))
if not P4GF_P4_BIN:
    print_log(
        _("Git Fusion Submit Trigger cannot find p4 binary: '{bin_path}'"
          "\nPlease edit either {script_path} or {trigger_conf} and set P4GF_P4_BIN_PATH.")
        .format(bin_path=P4GF_P4_BIN_PATH,
                script_path=SCRIPT_PATH,
                trigger_conf=P4GF_TRIGGER_CONF))
    sys.exit(P4FAIL)  # Cannot find the binary

skip_trigger_if_gf_user()
# import there here to avoid unneeded processing before the potential early exit above
import marshal
from   subprocess import Popen, PIPE, call
import time
from contextlib import contextmanager
import tempfile
import getopt
import io

#   obsolete version key names - these keys removed if exist by --install
P4GF_P4KEY_PRE_TRIGGER_VERSION      = NTR('git-fusion-pre-submit-trigger-version')
P4GF_P4KEY_POST_TRIGGER_VERSION     = NTR('git-fusion-post-submit-trigger-version')
# pylint: disable=invalid-name
# Invalid class name
# Invalid constant name
# Invalid class attribute name


class P4gfConst:

    """A spare class-ified version of Git Fusions's p4gf_const module."""

    def __init__(self):
        pass
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
# second block
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
#  third block
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

p4gf_const = P4gfConst()

GF_BEGIN_MARKER = ''
GF_END_MARKER   = ''
CHANGE_FOUND_BEGIN = False
CHANGE_FOUND_END = False

# Value for p4key P4GF_REVIEWS_NON_GF_SUBMIT when submit trigger decided this
# changelist requires no further processing by this trigger.
#
# Value must not be a legal depot path. Lack of leading // works.
#
DVCS                    ='dvcs'
DVCS_TRIGGER_TYPES      = ['pre-rmt-Push',  'post-rmt-Push', 'pre-user-fetch', 'post-user-fetch']
DVCS_PRE_TRIGGER_TYPES  = ['pre-rmt-Push',  'pre-user-fetch']
DVCS_POST_TRIGGER_TYPES = ['post-rmt-Push', 'post-user-fetch']
TRIGGER_TYPES = ['change-commit', 'change-content',
                    'change-failed', 'change-commit-p4gf-config', 'change-content-p4gf-config',
                    'pre-rmt-Push', 'post-rmt-Push', 'pre-user-fetch', 'post-user-fetch']
GF_TRIGGER_NAMES = ['GF-change-commit',
                    'GF-change-content', 'GF-change-failed',
                    'GF-post-submit', 'GF-pre-submit', 'GF-chg-submit',
                    'GF-change-commit-config', 'GF-post-submit-config',
                    'GF-change-content-config', 'GF-pre-submit-config',
                    'GF-pre-rmt-push', 'GF-post-rmt-push',
                    'GF-pre-user-fetch', 'GF-post-user-fetch']
# Messages for human users.
# Complete sentences.
# Except for trigger spec, hardwrap to 78 columns max, 72 columns preferred.
MSG_LOCKED_BY_GF            = _("\nFiles in the changelist are locked by Git Fusion user '{user}'.")
MSG_DVCS_LOCKED_BY_GF       = _("\nFiles in the push/pull are locked by Git Fusion user '{user}'.")
MSG_PRE_SUBMIT_FAILED       = _("Git Fusion pre-submit trigger failed.")
MSG_POST_SUBMIT_FAILED      = _("Git Fusion post-submit trigger failed.")
MSG_TRIGGER_FILENAME        = NTR("p4gf_submit_trigger.py")
MSG_WRITING_TICKETS         = _("Generating tickets in P4TICKETS file: {ticket_file}")
MSG_GENERATING_TICKET       = _("Attempting to login user: {user}")
MSG_TRIGGER_REPLACED        = _("The following trigger entries have been replaced:")
MSG_TRIGGER_REPLACEMENTS    = _("With these trigger entries:")
MSG_CANNOT_SUBMIT_MULTIPLE_CONFIG = _("Only one p4gf_config maybe submitted for validation.")
MSG_CANNOT_SUBMIT_INVALID_PATH    = _("Seems to be an invalid config path - cannot validate.\n"
                                      "{path}")
MSG_ILLEGAL_REPO_NAME    = _("\nIllegal repo name:'{raw_name} = {translated_name}'. "
                             "@#*,\"% or '...' not allowed.")
MSG_CANNOT_ACQUIRE_VIEWLOCK = _("Git Fusion has locked this repo. Try submitting again later.")
MSG_CANNOT_GET_COMMON_LOCK  = _("Cannot get Git Fusion Reviews lock. Try submittting again later.")

                        # About %user% here: if its index/position ever changes from
                        # [4 or 5] within the argv for TRIGGER_PRE's wrapper script,
                        # the wrapper script p4gf_submit_trigger_wrapper.sh must be
                        # updated to match.
MSG_TRIGGER_SPEC = NTR(
# pylint: disable = line-too-long
"""
    GF-change-content change-content //... "{TRIGGER_PRE}{PYTHON_PATH}{SPACER}{TRIGGER_PATH} change-content{CONFIG_PATH_ARG} %changelist% %quote%%user%%quote% %quote%%client%%quote% %quote%%serverport%%quote% %command% %args%"
    GF-change-commit change-commit //... "{TRIGGER_PRE}{PYTHON_PATH}{SPACER}{TRIGGER_PATH} change-commit{CONFIG_PATH_ARG} %changelist% %quote%%user%%quote% %quote%%client%%quote% %quote%%serverport%%quote% %oldchangelist% %command% %args%"
    GF-change-failed change-failed //... "{TRIGGER_PRE}{PYTHON_PATH}{SPACER}{TRIGGER_PATH} change-failed{CONFIG_PATH_ARG} %changelist% %quote%%user%%quote% %quote%%client%%quote% %quote%%serverport%%quote% %command% %args%"
    GF-change-commit-config change-commit //{P4GF_DEPOT}/repos/*/p4gf_config "{TRIGGER_PRE}{PYTHON_PATH}{SPACER}{TRIGGER_PATH} change-commit-p4gf-config{CONFIG_PATH_ARG} %changelist% %quote%%user%%quote% %quote%%client%%quote% %quote%%serverport%%quote% %oldchangelist% %command% %args%"
    GF-change-content-config change-content //{P4GF_DEPOT}/repos/*/p4gf_config "{TRIGGER_PRE}{PYTHON_PATH}{SPACER}{TRIGGER_PATH} change-content-p4gf-config{CONFIG_PATH_ARG} %changelist% %quote%%user%%quote% %quote%%client%%quote% %quote%%serverport%%quote% %oldchangelist% %command% %args%"
    GF-pre-rmt-push command pre-rmt-Push "{TRIGGER_PRE}{PYTHON_PATH}{SPACER}{TRIGGER_PATH} pre-rmt-Push{CONFIG_PATH_ARG} 0 %quote%%user%%quote% %quote%%client%%quote% %quote%%serverport%%quote% %command% %args%"
    GF-post-rmt-push command post-rmt-Push "{TRIGGER_PRE}{PYTHON_PATH}{SPACER}{TRIGGER_PATH} post-rmt-Push{CONFIG_PATH_ARG} 0 %quote%%user%%quote% %quote%%client%%quote% %quote%%serverport%%quote% %command% %args%"
    GF-pre-user-fetch command pre-user-fetch "{TRIGGER_PRE}{PYTHON_PATH}{SPACER}{TRIGGER_PATH} pre-user-fetch{CONFIG_PATH_ARG} 0 %quote%%user%%quote% %quote%%client%%quote% %quote%%serverport%%quote% %command% %args%"
    GF-post-user-fetch command post-user-fetch "{TRIGGER_PRE}{PYTHON_PATH}{SPACER}{TRIGGER_PATH} post-user-fetch{CONFIG_PATH_ARG} 0 %quote%%user%%quote% %quote%%client%%quote% %quote%%serverport%%quote% %command% %args%"
""")
MSG_EXAMPLE_UNIX  = NTR(
    '[python] p4gf_submit_trigger.py --generate-trigger-entries '
    '[--config-path=/absolute/path/to/config] '
    '["/absolute/path/to/python[3]"] ["/absolute/path/to/p4gf_submit_trigger.py"]')
MSG_EXAMPLE_DOS   = NTR(
    '[python] p4gf_submit_trigger.py --generate-trigger-entries '
    '[--config-path="C:\\absolute\\path\\to\\config"] '
    '["C:\\absolute\\path\\to\\python[3]"] ["C:\\absolute\\path\\to\\p4gf_submit_trigger.py"]')
MSG_EXAMPLE2_UNIX = NTR(
    'python p4gf_submit_trigger.py --install-trigger-entries '
    '[--config-path=/absolute/path/to/config] '
    '["/absolute/path/to/python[3]"] ["/absolute/path/to/p4gf_submit_trigger.py"] '
    'P4PORT superuser')
MSG_EXAMPLE2_DOS  = NTR(
    'python p4gf_submit_trigger.py --install-trigger-entries '
    '[--config-path="C:\\absolute\\path\\to\\config"] '
    '["C:\\absolute\\path\\to\\python[3]"] ["C:\\absolute\\path\\to\\p4gf_submit_trigger.py"] '
    'P4PORT superuser')
MSG_USAGE = _("""

    Git Fusion requires a submit trigger to be installed on your Perforce server
    to properly support atomic commits from Git.

    Installing Triggers
    -------------------
    Install triggers for each Perforce server configured for Git Fusion:

    1) Copy 'p4gf_submit_trigger.py' and 'p4gf_submit_trigger_wrapper.sh'
       to your Perforce server machine.
    2) These triggers require Python 2.6+ or Python 3.2+ on the
       Perforce server machine.
    3) Update the trigger spec, login the Perforce users and enable Git Fusion
       by running the following command on the Perforce Server.

        python p4gf_submit_trigger.py --install [--config-path=/absolute/path/to/config] P4PORT superuser password


    Please be aware that the --install function will write a configuration
    file, overriding settings at the top of this script.
    The default path of the configuration file is the dirpath(this trigger path)/p4gf_submit_trigger.cfg
    Tickets and trust files may also be written to default paths:
        dirpath(this trigger path)/p4gf_submit_trigger.cfg.tickets
        dirpath(this trigger path)/p4gf_submit_trigger.cfg.trust

    You may specify the configuration file path with the optional argument --config-path=/absolute/path/to/config
    Tickets and trust files will be named by appending '.tickets' and '.trust' to <somepath>.
    This option permits running two different GF instances configured differently against a single p4d server.

    To see the active configuration of this script, run the following command.
        python p4gf_submit_trigger.py --show-config [--config-path=/absolute/path/to/config]

    The actions of --install function can be run incrementally or manually.
    This may be prefered, as it will give you more control over how the trigger
    entries are installed, and where the P4TICKETS and P4TRUST files are written.



    *) Update the p4d trigger spec manually.

       As a Perforce super user run 'p4 triggers' and add the
       Git Fusion trigger entries displayed for your server by the following command.

        {MSG_EXAMPLE_UNIX}

        (for Windows):
        {MSG_EXAMPLE_DOS}

    *) The update of the trigger spec be done automatically by running the
       following command:

        {MSG_EXAMPLE2_UNIX}

        (for Windows):
        {MSG_EXAMPLE2_DOS}

    Logging in Perforce users
    -------------------------
    Running configure-git-fusion.sh on the Git Fusion server creates the users below,
    prompting for and setting a shared password.

    After running configure-git-fusion.sh, you must log each user into the
    Perforce server using 'p4 login':
        - for the Git Fusion server, p4 login under the unix account running Git Fusion.
        - for the Git Fusion triggers, p4 login under the OS account running p4d
          Note: the --install function will perform the appropriate logins.


    Logins for Git Fusion users are required as listed below.
                                            Git Fusion Server      p4d server
        git-fusion-user                       login                 login
        git-fusion-reviews-<server-id>        login                 login (used by --reset)
        git-fusion-reviews--non-gf            login                 login
        git-fusion-reviews--all-gf                                  login


    For convenience, you can use this script to login each of the users by running
    the following command on the Perforce Server under the same OS account
    running p4d:

        p4gf_submit_trigger.py --generate-tickets P4PORT superuser


    Configure Git Fusion trigger version P4 key.
    -------------------------------------------
    Configure Git Fusion trigger version key with this triggers version and
    thus avoid 'triggers are not installed' or 'triggers need updating'
    error messages:

        python p4gf_submit_trigger.py --set-version-p4key P4PORT

    The --install function will have already performed this task.

    Verify this trigger version matches Git Fusion's trigger P4 key.
    -------------------------------------------

        python p4gf_submit_trigger.py --verify-version-p4key P4PORT

    The --install function will have already performed this task.

    Clearing Locks
    --------------
    To clear any locks created by previous executions of this trigger or of Git Fusion:

        python p4gf_submit_trigger.py --reset P4PORT [superuser]

    This removes all 'p4 reviews' and 'p4 keys' data stored
    by this trigger and Git Fusion used to provide atomic locking for 'git push'.

        python p4gf_submit_trigger.py --reset --git-fusion-server=SERVER_ID P4PORT [superuser]

    This removes the 'p4 reviews' and 'p4 keys' data for the named server,
    leaving the other reviews and keys in place, if any.

    Defining Depot Paths Managed by Git Fusion
    ------------------------------------------
    To rebuild the list of Perforce depot paths currently part of any
    Git Fusion repo:

        python p4gf_submit_trigger.py --rebuild-all-gf-reviews P4PORT [superuser]

    By default this command runs as Perforce user 'git-fusion-reviews--all-gf'.
    The optional superuser parameter must be a Perforce super user.


""").format(MSG_EXAMPLE_UNIX  = MSG_EXAMPLE_UNIX
          , MSG_EXAMPLE_DOS   = MSG_EXAMPLE_DOS
          , MSG_EXAMPLE2_UNIX = MSG_EXAMPLE2_UNIX
          , MSG_EXAMPLE2_DOS  = MSG_EXAMPLE2_DOS )


# time.sleep() accepts a float, which is how you get sub-second sleep durations.
MS = 1.0 / 1000.0

# How often we retry to acquire the lock.
_RETRY_PERIOD = 100 * MS
# How many retries for the Reviews lock
# Set to 0 to wait forever
MAX_TRIES_FOR_COMMON_LOCK = 600

P4D_VERSION = None
SEPARATOR = '...'


# regex
# Edit these as needed for non-English p4d error messages
NOLOGIN_REGEX         = re.compile(r'Perforce password \(P4PASSWD\) invalid or unset')
PERMISSION_REGEX         = re.compile(r'You don\'t have permission for this operation.')
CONNECT_REGEX         = re.compile(r'.*TCP connect to.*failed.*')
NOSUCHKEY_REGEX         = re.compile(r'No such key.*')
TRUST_REGEX  = re.compile(r"^.*authenticity of '(.*)' can't.*fingerprint.*p4 trust.*$",
                          flags=re.DOTALL)
NEEDS_LEFT_JUSTIFIED_RE = re.compile(r'^\s+\[|=|:')
TRUST_MSG  = _("""
\nThe Git Fusion trigger has not established trust with its ssl enabled server.
Contact your adminstrator and have them run {command}""").format(command=NTR("'p4 trust'."))
# values for "action" argument to update_reviews()
ACTION_REMOVE     = NTR('remove')
ACTION_RESET      = NTR('reset')
ACTION_UNSET      = NTR('unset')
ACTION_ADD        = NTR('add')
ACTION_ADD_UNIQUE = NTR('add-unique')


def mini_usage(invalid=False):
    """Argument help."""
    _usage = ''
    if invalid:
                        # Newline moved out to make l10n.t script easier.
        _usage += _("Unrecognized or invalid arguments.") + "\n"
    _usage += _("""
Usage:
    p4gf_submit_trigger.py --generate-trigger-entries [--config-path="/absolute/path/to/config"] ["/absolute/path/to/python[3]"] ["/absolute/path/to/p4gf_submit_trigger.py"]
    p4gf_submit_trigger.py --install-trigger-entries [--config-path="/absolute/path/to/config"] ["/absolute/path/to/python[3]"] ["/absolute/path/to/p4gf_submit_trigger.py"] "P4PORT" superuser
    p4gf_submit_trigger.py --generate-tickets P4PORT superuser
    p4gf_submit_trigger.py --install [--config-path=/absolute/path/to/config] P4PORT superuser password
    p4gf_submit_trigger.py --set-version-p4key P4PORT
    p4gf_submit_trigger.py --verify-version-p4key P4PORT
    p4gf_submit_trigger.py --reset [--git-fusion-server=SERVER_ID] P4PORT [superuser]
    p4gf_submit_trigger.py --rebuild-all-gf-reviews P4PORT [superuser]
    p4gf_submit_trigger.py --show-config [--config-path=/absolute/path/to/config]
    p4gf_submit_trigger.py --help
""")
                        # pylint:enable=W9904
    print(_usage)
    if invalid:
        print(_("    args: {argv}").format(argv=sys.argv))

def p4d_version_string():
    """Return the serverVersion string from 'p4 info'.
    P4D/LINUX26X86_64/2015.2.MAIN-TEST_ONLY/1060553 (2015/05/15)
    """
    r = p4_run(['info', '-s'])
    key = 'serverVersion'
    for e in r:
        if isinstance(e, dict) and key in e:
            p4d_version = e[key]
            return p4d_version
    return None


def parse_p4d_version_string(version_string):
    """Convert a long server version string to a dict of parts:

    P4D/LINUX26X86_64/2012.2.PREP-TEST_ONLY/506265 (2012/08/07)

    product_abbrev  : P4D
    platform        : LINUX26X86_64
    release_year    : 2012
    release_sub     : 2
    release_codeline: PREP-TEST_ONLY
    patchlevel      : 506265
    date_year       : 2012
    date_month      : 08
    date_day        : 07
    """

    a = version_string.split(' ')
    b = a[0].split('/')
    result = {}

    r = b[2]
    m = re.search(r'^(\d+)\.(\d+)', r)
    result['release_year'   ] = m.group(1)
    result['release_sub'    ] = m.group(2)

    result['patchlevel'     ] = b[3]

    return result

# What patch level required for each year.sub?
_P4D_REQ_PATCH = p4gf_const.P4GF_REQ_PATCH
_P4D_REQ_YEAR_SUB = min(_P4D_REQ_PATCH.keys())
_P4D_MAX_YEAR_SUB = max(_P4D_REQ_PATCH.keys())


def p4d_version_required(version_string):
    """Return a 2-tuple (2010.2, 503309) of the (year.sub, patch) REQUIRED
    for the given p4d year.sub.
    """
    d = parse_p4d_version_string(version_string)
    year_sub_string = d['release_year'] + '.' + d['release_sub']
    year_sub = float(year_sub_string)

    if year_sub < _P4D_REQ_YEAR_SUB:
        return (_P4D_REQ_YEAR_SUB, _P4D_REQ_PATCH[_P4D_REQ_YEAR_SUB])

    if year_sub in _P4D_REQ_PATCH:
        return (year_sub, _P4D_REQ_PATCH[year_sub])

    return (_P4D_REQ_YEAR_SUB, _P4D_REQ_PATCH[_P4D_REQ_YEAR_SUB])


def p4d_version_acceptable(version_string):
    """Is this a P4D with the 'p4 reviews -C' patch?"""
    d = parse_p4d_version_string(version_string)
    year_sub_string = d['release_year'] + '.' + d['release_sub']
    year_sub = float(year_sub_string)
    patch_level = int(d['patchlevel'])

    if year_sub < _P4D_REQ_YEAR_SUB:
        return False

    if _P4D_MAX_YEAR_SUB < year_sub:
        return True

    if year_sub not in _P4D_REQ_PATCH:
        return False

    return _P4D_REQ_PATCH[year_sub] <= patch_level

def p4d_version_check():
    """Check that p4d version is acceptable.

    if p4 is not connected, it will be connected/disconnected
    if already connected, it will be left connected
    """

    version_string = p4d_version_string()

    acceptable = p4d_version_acceptable(version_string)
    if not acceptable:
        vr = p4d_version_required(version_string)
        msg = (_('Unsupported p4d version: {actual}'
                 '\nGit Fusion requires version {req_v}/{req_ch} or later')
               .format(actual=version_string,
                       req_v=vr[0],
                       req_ch=vr[1]))
        print_log(msg)
        sys.exit(P4FAIL)

# [/path_to_python] [/path_to_trigger] [P4PORT .. unused - present for backward compatibility]
MAX_GENERATE_TRIG_ARGS = 3

# [/path_to_python] [/path_to_trigger] P4PORT superuser
MAX_INSTALL_TRIG_ARGS  = 4

def get_generate_install_args(args):
    """Return a tuple (python_path, trigger_path, p4port, superuser)
    extracted from the args.
    Return empty strings for missing parameters.
    """

    python_path = trigger_path = p4port = suser = ""
    idx = 0
    arglen = len(args)

    if not arglen:
        return (python_path, trigger_path, p4port, suser)

    if 'python' in args[idx]:
        python_path = args[idx]
        idx += 1

    # python path may not be second argument
    if idx+1 < arglen and not python_path and 'python' in args[idx+1]:
        print(_("The python and trigger path parameters are invalid or in the incorrect order."))
        print("{0}\n{1}".format(MSG_EXAMPLE_UNIX, MSG_EXAMPLE_DOS))
        sys.exit(P4FAIL)

    if idx < arglen:
        if  MSG_TRIGGER_FILENAME in args[idx]:
            trigger_path = args[idx]
            idx += 1
        elif '/' in args[idx] or '\\' in args[idx]:
            # seems to be path but does not to the trigger
            # show error
            display_usage_and_exit(True, True)

    if idx < arglen:
        p4port = args[idx]
        idx += 1

    if idx < arglen:
        suser = args[idx]

    return (python_path, trigger_path, p4port, suser)


def generate_trigger_entries(generate_args):
    """Display Git Fusion trigger entries for local paths."""
    #pylint: disable=unused-variable

    global CONFIG_PATH_ARG
    spacer = ''
    (path_to_python, path_to_trigger, p4port, suser) = get_generate_install_args(generate_args)
    if path_to_python:
        spacer = ' '
    if not path_to_trigger:
        path_to_trigger = os.path.abspath(__file__)


    ispython = re.compile(r'^/.*/?[Pp]ython[\d\.]*$')
    istrigger = re.compile(r'^/.*/?' + MSG_TRIGGER_FILENAME + '$')
    ispython_dos = re.compile(r'^[A-Za-z]:\\.*\\?python[\d\.]*(\.exe)?$')
    istrigger_dos = re.compile(r'^[A-Za-z]:\\.*\\?' + MSG_TRIGGER_FILENAME + '$')
    if path_to_python:
        parms_match = (ispython.match(path_to_python) and
                       istrigger.match(path_to_trigger))
        dos_parms_match = (ispython_dos.match(path_to_python) and
                           istrigger_dos.match(path_to_trigger))
    else:
        parms_match = istrigger.match(path_to_trigger)
        dos_parms_match = istrigger_dos.match(path_to_trigger)
    if not (parms_match or dos_parms_match):
        print(_("The python and trigger path parameters are invalid or in the incorrect order."))
        print("{0}\n{1}".format(MSG_EXAMPLE_UNIX, MSG_EXAMPLE_DOS))
        sys.exit(P4FAIL)

    trigger_entries = MSG_TRIGGER_SPEC

    # To include performance wrapper or not
    trigger_wrapper = path_to_trigger.replace( "/p4gf_submit_trigger.py"
                                             , "/p4gf_submit_trigger_wrapper.sh")
    if ( platform.system() != "Windows"
         and os.path.exists(trigger_wrapper)
         and os.access(trigger_wrapper, os.X_OK) ):
        TRIGGER_PRE  = "%quote%{0}%quote% ".format(trigger_wrapper)
    else:
        TRIGGER_PRE = ""

    # Wrap the paths in %quote% to protect against space and UTF8 characters
    path_to_trigger = '%quote%' + path_to_trigger + '%quote%'
    if path_to_python:
        path_to_python = '%quote%' + path_to_python + '%quote%'
    if CONFIG_PATH_ARG:
        CONFIG_PATH_ARG = CONFIG_PATH_ARG.replace("=","=%quote%")
        CONFIG_PATH_ARG = ' ' + CONFIG_PATH_ARG + '%quote%'

    trigger_entries = trigger_entries.format(PYTHON_PATH=path_to_python,
                                             TRIGGER_PRE=TRIGGER_PRE,
                                             SPACER=spacer,
                                             TRIGGER_PATH=path_to_trigger,
                                             CONFIG_PATH_ARG=CONFIG_PATH_ARG,
                                             P4GF_DEPOT=p4gf_const.P4GF_DEPOT)
    return trigger_entries


def usage():
    """Display full usage."""
    print(MSG_USAGE)


class p4gf_config:

    """A spare version of Git Fusions's p4gf_config module."""

    def __init__(self):
        pass

# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
# [repo-creation]
    SECTION_REPO_CREATION      = NTR('repo-creation')
    KEY_CHARSET                = NTR('charset')
    KEY_GIT_AUTOPACK           = NTR('git-autopack')
    KEY_GIT_GC_AUTO            = NTR('git-gc-auto')
    KEY_NDPR_ENABLE            = NTR('depot-path-repo-creation-enable')
    KEY_NDPR_P4GROUP           = NTR('depot-path-repo-creation-p4group')

# [git-to-perforce]
    SECTION_GIT_TO_PERFORCE    = NTR('git-to-perforce')
    KEY_CHANGE_OWNER           = NTR('change-owner')
    KEY_ENABLE_BRANCH_CREATION = NTR('enable-git-branch-creation')
    KEY_ENABLE_MERGE_COMMITS   = NTR('enable-git-merge-commits')
    KEY_ENABLE_SWARM_REVIEWS   = NTR('enable-swarm-reviews')
    KEY_ENABLE_SUBMODULES      = NTR('enable-git-submodules')
    KEY_PREFLIGHT_COMMIT       = NTR('preflight-commit')
    KEY_IGNORE_AUTHOR_PERMS    = NTR('ignore-author-permissions')
    KEY_READ_PERMISSION_CHECK  = NTR('read-permission-check')
    KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM \
                           = NTR('git-merge-avoidance-after-change-num')
    KEY_ENABLE_GIT_FIND_COPIES = NTR('enable-git-find-copies')
    KEY_ENABLE_GIT_FIND_RENAMES = NTR('enable-git-find-renames')
    KEY_ENABLE_FAST_PUSH       = NTR('enable-fast-push')
    KEY_JOB_LOOKUP             = NTR('job-lookup')
    KEY_NDB_ENABLE             = NTR('depot-branch-creation-enable')
    VALUE_NDB_ENABLE_NO        = NTR('no')
    VALUE_NDB_ENABLE_EXPLICIT  = NTR('explicit')
    VALUE_NDB_ENABLE_ALL       = NTR('all')
    KEY_NDB_P4GROUP            = NTR('depot-branch-creation-p4group')
    KEY_NDB_DEPOT_PATH         = NTR('depot-branch-creation-depot-path')
    VALUE_NDB_DEPOT_PATH_DEFAULT = NTR('//depot/{repo}/{git_branch_name}')
    KEY_NDB_VIEW               = NTR('depot-branch-creation-view')
    VALUE_NDB_VIEW_DEFAULT     = NTR('... ...')
    KEY_FAST_PUSH_WORKING_STORAGE = NTR('fast-push-working-storage')
    KEY_USE_SHA1_TO_SKIP_EDIT  = NTR('use-sha1-to-skip-edit')
#                 End block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
#                 Begin block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
    SECTION_PERFORCE_TO_GIT    = NTR('perforce-to-git')
    KEY_CHANGELIST_DATE_SOURCE = NTR('changelist-date-source')
    VALUE_DATE_SOURCE_P4_SUBMIT     = NTR('perforce-submit-time')
    KEY_SUBMODULE_IMPORTS      = NTR('enable-stream-imports')
    KEY_CLONE_TO_CREATE_REPO   = NTR('enable-clone-to-create-repo')
    KEY_UPDATE_ONLY_ON_POLL    = NTR('update-only-on-poll')
    KEY_HTTP_URL               = NTR('http-url')
    KEY_SSH_URL                = NTR('ssh-url')
    KEY_ENABLE_ADD_COPIED_FROM_PERFORCE = NTR('enable-add-copied-from-perforce')
    KEY_ENABLE_GIT_P4_EMULATION = NTR('enable-git-p4-emulation')
# [@features]
    SECTION_FEATURES           = NTR('@features')
    FEATURE_KEYS = {
    }
# [authentication]
    SECTION_AUTHENTICATION     = NTR('authentication')
    KEY_EMAIL_CASE_SENSITIVITY = NTR('email-case-sensitivity')
    KEY_AUTHOR_SOURCE          = NTR('author-source')
# [quota]
    SECTION_QUOTA              = NTR('quota')
    KEY_COMMIT_LIMIT           = NTR('limit_commits_received')
    KEY_FILE_LIMIT             = NTR('limit_files_received')
    KEY_SPACE_LIMIT            = NTR('limit_space_mb')
    KEY_RECEIVED_LIMIT         = NTR('limit_megabytes_received')

# [undoc]
    SECTION_UNDOC              = NTR('undoc')
    KEY_ENABLE_CHECKPOINTS     = NTR('enable_checkpoints')
#
# In [@repo] of the per-repo config files only
#
    SECTION_REPO               = NTR('@repo')
    KEY_DESCRIPTION            = NTR('description')
    KEY_FORK_OF_REPO           = NTR('fork-of-repo')
#
# The following may also appear in the branch section...
#
    KEY_READ_ONLY              = NTR('read-only')
    KEY_FORK_OF_BRANCH_ID      = NTR('fork-of-branch-id')

#
# In the [<branch_id>] section of the per-repo config files only
#
    KEY_GIT_BRANCH_NAME        = NTR('git-branch-name')
    KEY_VIEW                   = NTR('view')
    KEY_STREAM                 = NTR('stream')
    KEY_ORIGINAL_VIEW          = NTR('original-view')
    KEY_ENABLE_MISMATCHED_RHS  = NTR('enable-mismatched-rhs')
    KEY_GIT_BRANCH_DELETED     = NTR('deleted')
    KEY_GIT_BRANCH_DELETED_CHANGE  = NTR('deleted-at-change')
    KEY_GIT_BRANCH_START_CHANGE    = NTR('start-at-change')
    KEY_DEPOT_BRANCH_ID        = NTR('depot-branch-id')
    KEY_GIT_LFS_ENABLE         = NTR('git-lfs-enable')
    KEY_GIT_LFS_INITIAL_TRACK  = NTR('git-lfs-initial-track')
#                 End block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
    VALUE_AUTHOR                = NTR('author')
    VALUE_PUSHER                = NTR('pusher')
    VALUE_YES                   = NTR('yes')
    VALUE_NO                    = NTR('no')
    VALUE_NONE                  = NTR('none')
    VALUE_GIT_EMAIL             = NTR('git-email')
#                 End block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
#                 Begin block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
    SECTION_LIST = [ SECTION_REPO_CREATION
                   , SECTION_GIT_TO_PERFORCE
                   , SECTION_PERFORCE_TO_GIT
                   , SECTION_FEATURES
                   , SECTION_AUTHENTICATION
                   , SECTION_QUOTA
                   , SECTION_UNDOC
                   , SECTION_REPO
                   ]
#                 End block copied to both p4gf_config.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------

    @staticmethod
    def to_text(comment_header, config):
        """Produce a single string with a comment header and a ConfigParser.

        Suitable for writing to file.
        """
        out = io.StringIO()
        out.write(comment_header)
        config.write(out)
        file_content = out.getvalue()
        out.close()
        return file_content

    @staticmethod
    def is_branch_section_name(section_name):
        """Does this section name look like a branch?"""
        if section_name.startswith('@'):
            return False
        if section_name in p4gf_config.SECTION_LIST:
            return False
        return True

    @staticmethod
    def branch_section_list(config):
        """Return a list of section names, one for each branch mapping section.

        Not every returned section name is guaranteed to be a correct and complete
        branch definition. Use p4gf_branch.Branch.from_config() to figure that out.
        """
        return [s for s in config.sections()
                if p4gf_config.is_branch_section_name(s)]

    @staticmethod
    def get_view_lines(config, branch_id, key):
        """Get a view value and return it as a list of view lines.

        Common code to deal with blank first lines (happens a lot
        in human-authored configs) and to force a list.
        """
        view_lines = config.get(branch_id, key)
        return p4gf_config.to_view_lines(view_lines)

    @staticmethod
    def to_view_lines(view_lines):
        """Common code to deal with blank first lines (happens a lot
        in human-authored configs) and to force a list.
        """
        if isinstance(view_lines, str):
            view_lines = view_lines.splitlines()
        # Common: first line blank, view starts on second line.
        if view_lines and not len(view_lines[0].strip()):
            del view_lines[0]
        return view_lines

    @staticmethod
    def get_p4_charset():
        """Extract the P4CHART value."""
        if len(CHARSET) == 2:
            return CHARSET[1]
        else:
            return 'utf8'

    @staticmethod
    def default_config_global():
        """Return a ConfigParser instance loaded with default values.

        :param p4: instance of P4 used to retrieve system values.
        """
        config = configparser.ConfigParser()

        config.add_section(p4gf_config.SECTION_REPO_CREATION)
        config.set(p4gf_config.SECTION_REPO_CREATION,
            p4gf_config.KEY_CHARSET, p4gf_config.get_p4_charset())
        config.set(p4gf_config.SECTION_REPO_CREATION,
            p4gf_config.KEY_GIT_AUTOPACK, p4gf_config.VALUE_YES)
        config.set(p4gf_config.SECTION_REPO_CREATION,
            p4gf_config.KEY_GIT_GC_AUTO, p4gf_config.VALUE_NONE)
        config.set(p4gf_config.SECTION_REPO_CREATION,
            p4gf_config.KEY_NDPR_ENABLE, p4gf_config.VALUE_NO)
        config.set(p4gf_config.SECTION_REPO_CREATION,
            p4gf_config.KEY_NDPR_P4GROUP, p4gf_config.VALUE_NONE)

        config.add_section(p4gf_config.SECTION_GIT_TO_PERFORCE)
        # Zig thinks it's clearer to address the section, not the configparser.
        # Maybe do this for other sections with 3+ entries.
        # (Zig also things tabular code should stay tabular, but he'll let
        #  pep8 fans overrule him here.)
        #sec = config[p4gf_config.SECTION_GIT_TO_PERFORCE]
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_CHANGE_OWNER, p4gf_config.VALUE_AUTHOR)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_ENABLE_BRANCH_CREATION, p4gf_config.VALUE_YES)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_ENABLE_SWARM_REVIEWS, p4gf_config.VALUE_YES)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_ENABLE_MERGE_COMMITS, p4gf_config.VALUE_YES)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_ENABLE_SUBMODULES, p4gf_config.VALUE_YES)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_PREFLIGHT_COMMIT, '')
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_IGNORE_AUTHOR_PERMS, p4gf_config.VALUE_NO)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_READ_PERMISSION_CHECK, '')
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_GIT_MERGE_AVOIDANCE_AFTER_CHANGE_NUM,
                '0')
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_ENABLE_GIT_FIND_COPIES, p4gf_config.VALUE_NO)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_ENABLE_GIT_FIND_RENAMES, p4gf_config.VALUE_NO)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_ENABLE_FAST_PUSH, p4gf_config.VALUE_YES)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_JOB_LOOKUP, p4gf_config.VALUE_NONE)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_NDB_ENABLE, p4gf_config.VALUE_NDB_ENABLE_NO)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_NDB_P4GROUP, p4gf_config.VALUE_NONE)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_NDB_DEPOT_PATH, p4gf_config.VALUE_NDB_DEPOT_PATH_DEFAULT)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_NDB_VIEW, p4gf_config.VALUE_NDB_VIEW_DEFAULT)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_CHANGELIST_DATE_SOURCE, p4gf_config.VALUE_DATE_SOURCE_P4_SUBMIT)
        config.set(p4gf_config.SECTION_GIT_TO_PERFORCE,
                p4gf_config.KEY_USE_SHA1_TO_SKIP_EDIT, p4gf_config.VALUE_YES)

        config.add_section(p4gf_config.SECTION_PERFORCE_TO_GIT)
        config.set(p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_SUBMODULE_IMPORTS, p4gf_config.VALUE_NO)
        config.set(p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_CLONE_TO_CREATE_REPO, p4gf_config.VALUE_YES)
        config.set(p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_UPDATE_ONLY_ON_POLL, p4gf_config.VALUE_NO)
        config.set(p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_GIT_LFS_ENABLE, p4gf_config.VALUE_NO)
        config.set(p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_GIT_LFS_INITIAL_TRACK, p4gf_config.VALUE_NONE)
        config.set(p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_HTTP_URL, p4gf_config.VALUE_NONE)
        config.set(p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_SSH_URL, p4gf_config.VALUE_NONE)
        config.set(p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_ENABLE_ADD_COPIED_FROM_PERFORCE, p4gf_config.VALUE_YES)
        config.set(p4gf_config.SECTION_PERFORCE_TO_GIT,
            p4gf_config.KEY_ENABLE_GIT_P4_EMULATION, p4gf_config.VALUE_NO)

        config.add_section(p4gf_config.SECTION_FEATURES)
        for key in p4gf_config.FEATURE_KEYS.keys():
            config.set(p4gf_config.SECTION_FEATURES, key, "False")

        config.add_section(p4gf_config.SECTION_AUTHENTICATION)
        config.set(p4gf_config.SECTION_AUTHENTICATION,
            p4gf_config.KEY_EMAIL_CASE_SENSITIVITY, p4gf_config.VALUE_NO)
        config.set(p4gf_config.SECTION_AUTHENTICATION,
            p4gf_config.KEY_AUTHOR_SOURCE, p4gf_config.VALUE_GIT_EMAIL)

        config.add_section(p4gf_config.SECTION_QUOTA)
        config.set(p4gf_config.SECTION_QUOTA, p4gf_config.KEY_COMMIT_LIMIT, '0')
        config.set(p4gf_config.SECTION_QUOTA, p4gf_config.KEY_FILE_LIMIT, '0')
        config.set(p4gf_config.SECTION_QUOTA, p4gf_config.KEY_SPACE_LIMIT, '0')
        config.set(p4gf_config.SECTION_QUOTA, p4gf_config.KEY_RECEIVED_LIMIT, '0')

        config.add_section(p4gf_config.SECTION_UNDOC)
        config.set(p4gf_config.SECTION_UNDOC,
            p4gf_config.KEY_ENABLE_CHECKPOINTS, p4gf_config.VALUE_NO)

        return config


class Branch:

    """A spare version of Git Fusion's Branch class.

    A Git<->Perforce branch association.

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
    def __init__(self):
        self.branch_id          = None  # Section name.
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
        self.stream_name        = None  # for stream branches, the stream this
                                        # branch is connected to
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
        self.fork_of_branch_id  = None  # If this branch is a copy from some
                                        # other repo, what's its branch id
                                        # WITHIN THAT OTHER REPO. Not a valid
                                        # branch_id within this repo.
        self.lhs                 = []   # will contain the extracted lhs/rhs arrays
        self.rhs                 = []

    @staticmethod
    def from_config(config, branch_id, strict=False):
        """Factory to seed from a config file.

        Returns None if config file lacks a complete and correct branch
        definition.

        """
        # pylint: disable=too-many-branches, too-many-statements
        valid_branch_id = re.compile("^[-_=.a-zA-Z0-9]+$")
        if not valid_branch_id.match(branch_id):
            f = _("repository configuration section [{section}] has invalid section"
                  " name '{section}'")
            return (None, f.format(section=branch_id))
        result = Branch()
        result.branch_id = branch_id
        if config.has_option(branch_id, p4gf_config.KEY_GIT_BRANCH_NAME):
            result.git_branch_name = config.get(branch_id, p4gf_config.KEY_GIT_BRANCH_NAME)
        if config.has_option(branch_id, p4gf_config.KEY_FORK_OF_BRANCH_ID):
            result.fork_of_branch_id = config.get(branch_id, p4gf_config.KEY_FORK_OF_BRANCH_ID)
        if (config.has_option(branch_id, p4gf_config.KEY_STREAM) and
                config.has_option(branch_id, p4gf_config.KEY_VIEW)):
            f = _("repository configuration section [{section}] may not contain both"
                  " 'view' and 'stream'")
            return (None, f.format(section=branch_id))
        if strict and not (config.has_option(branch_id, p4gf_config.KEY_STREAM)
                           or config.has_option(branch_id, p4gf_config.KEY_VIEW)):
            f = _("repository configuration section [{section}] must contain either"
                  " 'view' or 'stream'")
            return (None, f.format(section=branch_id))
        view_lines = None

        if config.has_option(branch_id, p4gf_config.KEY_STREAM):
            result.stream_name = config.get(branch_id, p4gf_config.KEY_STREAM)
            stream = first_dict(
                p4_run(['stream', '-ov', result.stream_name]))
            if stream_contains_isolate_path(stream):
                f = _("repository configuration section [{section}] '{stream}' refers to stream"
                      " with 'isolate' Path")
                return (None, f.format(section=branch_id, stream=result.stream_name))
            if 'View' not in stream and 'View0' not in stream:
                f = _("repository configuration section [{section}] '{stream}' does not refer"
                      " to a valid stream")
                return (None, f.format(section=branch_id, stream=result.stream_name))
            if stream['Type'] == 'task':
                f = _("repository configuration section [{section}] '{stream}' "
                      "refers to a task stream")
                return (None, f.format(section=branch_id, stream=result.stream_name))
            # Get the View lines in the correct order
            view_lines = []
            while NTR("View") + str(len(view_lines)) in stream:
                view_lines.append(stream.get(NTR("View") + str(len(view_lines))))
        else:
            view_lines = p4gf_config.get_view_lines(config, branch_id
                                                   , p4gf_config.KEY_VIEW )
        if not view_lines:
            f = _("repository configuration section [{section}] view lines are empty")
            return (None, f.format(section=branch_id))
        if isinstance(view_lines, str):
            view_lines = view_lines.replace('\t', ' ')  # pylint: disable=maybe-no-member
        elif isinstance(view_lines, list):
            view_lines = [ln.replace('\t', ' ')     # pylint: disable=maybe-no-member
                          for ln in view_lines]
        result.view_lines = view_lines
        result.lhs, result.rhs, have_depot_path, errmsg = get_views_lhs_rhs(view_lines)
        if errmsg:
            errmsg = errmsg.format(branch_id, view_lines)
            return (None, errmsg)
        if not len(result.lhs):
            f = _("repository configuration section [{section}] view lines are invalid"). \
                format(section=branch_id)
            if not have_depot_path:
                f0 = _("badly formed depot syntax in view: '{view_lines}' not permitted.\n'"). \
                    format(view_lines=view_lines)
                f = f + '\n' + f0
            return (None, f)

        return (result, None)

# Legal @xxx section names. Any other result in
# p4gf_config_validator.is_valid() rejection.
AT_SECTIONS = [
    p4gf_config.SECTION_REPO,
    p4gf_config.SECTION_FEATURES
]
INVALID_GIT_BRANCH_NAME_PREFIXES = ['remotes/']
ILLEGAL_PREFIXES = [r'^\+\s', r'^"\s*\+\s', r'^\-\s', r'^"\s*\-\s']


def stream_contains_isolate_path(stream_spec):
    """Return boolean whether the stream contains any 'isolate' path."""

    # Do the Paths contain 'isolate'"
    idx   = 0
    while NTR("Paths") + str(idx) in stream_spec:
        p = stream_spec.get(NTR("Paths") + str(idx))
        idx += 1
        if p.startswith('isolate '):
            return True

    if stream_spec['Parent'] == 'none':
        return False

    # recurse up the chain of parents. If any parent contains an 'isolate'
    # report True
    parent_stream = first_dict(
        p4_run(['stream', '-o', stream_spec['Parent']]))
    if not parent_stream:
        return False
    return stream_contains_isolate_path(parent_stream)

    # Note: we cannot use: p4 branch -S stream_name -o bogusname
    # to detect 'isolate' in parents by testing for exclusions.
    # exclusions exist for 'ignored' as well.

def depot_from_view_lhs(lhs):
    """extract depot name from lhs of view line.
     '" + //depot' '"+ //depot' '"+//depot'
     We catch malformed [+- ] errors later.

    """
    # Allow regular quote and the left smart quote (\u201c)
    if PYTHON3:
        # Cannot be certain which version of Python 3 we are using, since
        # the u'' notation only came back in Python 3.3, so do not use it
        # if we are running on any version of Python 3.
        s = re.search('^("|\u201c|\xe2\x80\x9c)? ?[+-]? ?//([^/]+)/.*', lhs)
    else:
        s = re.search(u'^("|\u201c|\xe2\x80\x9c)? ?[+-]? ?//([^/]+)/.*', lhs)
    if s:
        return s.group(2)
    else:
        return None


def view_lines_have_space_after_plus_minus(viewlines):
    """Return True if view lines have space after + or -."""
    illegal_prefix = False
    for vl in viewlines:
        for space_in_prefix in ILLEGAL_PREFIXES:
            if re.match(space_in_prefix, vl):
                illegal_prefix = True
    return illegal_prefix


def view_lines_suffixes_mismatch(lhs, rhs):
    """Determine whether the view's lines' lhs and rhs suffixes match.


    Extract the string after the last '/' and compare.
    Different suffixes with no wildcards['...','*','%%'] are passed.
    Detect these mistmatches/errors:
         '//depot/a...  //depot/a/...'.
         rhs starts with '/'.
         unequal number of '*' + '%%"
    On mismatch detection/error return specific message template.
    Otherwise return None
    """
    # pylint: disable=too-many-branches
    # check that no space follows + or -

    rh_sides = []
    for index, lh in enumerate(lhs):
        right = rhs[index]
        original_line = lh + ' ' + right
        if not lh.startswith('-') and not lh.startswith('"-'):
            if right in rh_sides:
                if not lh.startswith('+') and not lh.startswith('"+'):
                    msg = _("non overlay right hand sides of view map must be different\n") \
                        + original_line \
                        + _("\nview for branch '{branch_id}':\n{view_lines}\n")
                    return msg
            rh_sides.append(right)
                                            # extract the string after the last '/'
                                            # P4.Map may wrap lhs/rhs in " so strip
        left = lh.strip('"\'').split('/')[-1]
        right = right.strip('"\'')

        if '/' in right:                    # otherwise nothing to split
            if right.startswith('/'):       # rhs may not start with '/'
                msg = _("right hand side of view map may not start with '/'\n") \
                    + original_line \
                    + _("\nview for branch '{branch_id}':\n{view_lines}\n")
                return msg
            right = right.split('/')[-1]

        lhs_dots = left.count('...')
        rhs_dots = right.count('...')

                                            # validate mismatched suffix
        if left != right:
            lhs_subst = left.count('*') + left.count('%%')
            rhs_subst = right.count('*') + right.count('%%')

                                            # both have no wildcards - must be filepaths - ok
            if (lhs_dots + lhs_subst) == 0 and (rhs_dots + rhs_subst) == 0:
                continue
                                            # both have same number of * + %% - ok
            if lhs_subst == rhs_subst and lhs_dots == 0:
                continue
                                            # repo mismatch
            msg = _("the left and right side suffixes do not match for view "
                    "line '{left} {right}'").format(left=left, right=right)
            return msg + _("\nfor branch '{branch_id}':\n{view_lines}\n")
    return None


def view_lines_free_of_evil(lhs, rhs):
    """Ensure the left and right sides of the view contain no evil characters."""
    suffix = _("\nfor branch '{branch_id}':\n{view_lines}\n")
    for (lh, rh) in zip(lhs, rhs):
        # Check for "smart" quotes in the paths.
        if '\u201c' in lh or '\u201d' in lh or '\xe2\x80\x9c' in lh or '\xe2\x80\x9d' in lh:
            return _('the left side contains "smart" quotes: {left}\n').format(left=lh) + suffix
        if '\u201c' in rh or '\u201d' in rh or '\xe2\x80\x9c' in rh or '\xe2\x80\x9d' in rh:
            return _('the right side contains "smart" quotes: {right}\n').format(right=rh) + suffix
        # Check for the ellipsis character in the paths.
        if '\u2026' in lh or '\xe2\x80\xa6' in lh:
            return _('the left side contains ellipsis character: {left}\n').format(left=lh) + suffix
        if '\u2026' in rh or '\xe2\x80\xa6' in rh:
            return _('the right side contains ellipsis character: {right}\n').format(
                right=rh) + suffix
    return None


class Validator:

    """A spare version of Git Fusion's p4gf_config_validator.Validator class.

    A validator for Git Fusion configuration files. It should be used
    as a context manager, using the Python 'with' statement. This avoids
    leaking ConfigParser instances without the need for explicitly invoking
    del or hoping that __del__ will actually work."""

    def __init__(self):
        self.repo_name        = None
        self.config_file_path = None
        self.config           = None              # Can be None if empty config file.
        self.parses           = None
        self.emsg             = None
        self.display_error    = True
        self.lhs              = set()
        self.branch_dict      = {}

    @staticmethod
    def from_contents(contents, depot_path):
        """Parse and validate contenst of p4gf_config file."""
        # pylint: disable=too-many-branches

        v = Validator()

        v.config_file_path = depot_path

        if PYTHON3:
            v.config = configparser.ConfigParser(interpolation=None)
        else:
            v.config = ConfigParser.RawConfigParser()
        v.parses, v.emsg = _read_config_string(v.config, contents=str(contents))
        if v.emsg and re.match('parsing errors', v.emsg):
            matches = re.search("([^:]+:)", v.emsg)
            if matches:
                v.emsg = matches.group(1)

        # python's RawConfigParser does not detect duplicated sections
        if v.parses and not PYTHON3:
            contents_sections = "\n".join([
                line for line in contents.split('\n')
                if not line.startswith('#') and re.match(r'^\s*\[[^\]]+\]', line)])
            for branch in [section for section in v.config.sections()
                           if not section.startswith('@')]:
                if contents_sections.count('[' + branch + ']') > 1:
                    v.parses = False
                    v.emsg = "section '{0}' already exists".format(branch)
        return v

    def is_valid(self):
        """check if config file is valid."""
        # pylint:disable=too-many-branches
        # Reject empty config.
        if not self.config:
            self.report_error(_('empty config\n'))
            return False

        # reject sections starting with @ except for @repo
        # like if they put @repos or @Repo instead of @repo
        at_sections = [section for section in self.config.sections()
                       if     section.startswith('@')
                          and section not in AT_SECTIONS]
        if at_sections:
            self.report_error(_("unexpected section(s): '{sections}'\n")
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
            if not self.valid_charset(charset):
                self.report_error(_("invalid charset: '{charset}'\n")
                                  .format(charset=charset))
                return False
        # Ensure the change-owner setting is correctly defined
        if self.config.has_option(p4gf_config.SECTION_REPO,
                                  p4gf_config.KEY_CHANGE_OWNER):
            value = self.config.get(p4gf_config.SECTION_REPO,
                                    p4gf_config.KEY_CHANGE_OWNER)
            if value != p4gf_config.VALUE_AUTHOR and value != p4gf_config.VALUE_PUSHER:
                self.report_error(
                    _("repository configuration option '{option}' has illegal value\n")
                    .format(option=p4gf_config.KEY_CHANGE_OWNER))
                return False

        # Validate the preflight-commit option
        section_name = p4gf_config.SECTION_GIT_TO_PERFORCE
        if self.config.has_section(p4gf_config.SECTION_REPO):
            section_name = p4gf_config.SECTION_REPO
        if self.config.has_option(section_name, p4gf_config.KEY_PREFLIGHT_COMMIT):
            value = self.config.get(section_name, p4gf_config.KEY_PREFLIGHT_COMMIT)
            if not self.validate_preflight_commit_value(value):
                self.report_error(_("invalid preflight commit value: '{value}'\n")
                                  .format(value=value))
                return False

        # Ensure correct new_depo_branch settings
        if not self._validate_new_depot_branch():
            return False
        # Make sure branches are present and properly configured
        if not self._valid_branches():
            return False
        if not self._valid_depots():
            return False
        return True

    @staticmethod
    def valid_charset(charset):
        """Return True for a valid charset, False for an invalid charset."""
        # call info with the charset - report exception as false
        global CHARSET
        save_charset = CHARSET
        CHARSET = ['-C', charset]
        info = p4_run_ztag(["info", "-s"], exit_on_error=False)
        CHARSET = save_charset
        if info:
            if not isinstance(info, str):
                info = ' '.join(info)
            if re.match('Character set must be one of:', info):
                return False
            return True
        else:
            return False

    @staticmethod
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

    def _validate_new_depot_branch(self):
        '''Perform new depot branch validations.'''
        config = self.config
        valid = True
        if config.has_option(p4gf_config.SECTION_REPO,
                                  p4gf_config.KEY_NDB_ENABLE):
            value = config.get(p4gf_config.SECTION_REPO,
                                   p4gf_config.KEY_NDB_ENABLE)
            if value != p4gf_config.VALUE_NDB_ENABLE_NO and \
                    value != p4gf_config.VALUE_NDB_ENABLE_EXPLICIT and \
                    value != p4gf_config.VALUE_NDB_ENABLE_ALL:
                self.report_error(_("Perforce: Improperly configured {option} value\n")
                                  .format(option=p4gf_config.KEY_NDB_ENABLE))
                valid = False
        if config.has_option(p4gf_config.SECTION_REPO,
                                  p4gf_config.KEY_NDB_DEPOT_PATH):
            value = config.get(p4gf_config.SECTION_REPO,
                                   p4gf_config.KEY_NDB_DEPOT_PATH)
            if not str(value).startswith("//"):
                self.report_error(_("Perforce: Improperly configured {option} value\n")
                                  .format(option=p4gf_config.KEY_NDB_DEPOT_PATH))
                valid = False
        return valid

    def report_error(self, msg):
        """Report error message, including path to offending file"""
        self.emsg = msg
        if self.display_error and msg:
            print_log(_("error: invalid configuration file: '{path}'\n")
                      .format(path=self.config_file_path))
            print_log(_('error: {error}\n').format(error=msg))

    def _valid_branches(self):
        """Check if branch definitions in config file are valid."""
        # pylint:disable=too-many-branches,too-many-statements
        config = self.config
        # Does the config contain any branch sections?
        sections = p4gf_config.branch_section_list(config)
        if not sections:
            self.report_error(_('repository configuration missing branch ID\n'))
            return False

        # check branch creation option
        try:
            if config.has_option(p4gf_config.SECTION_REPO,
                                 p4gf_config.KEY_ENABLE_BRANCH_CREATION):
                config.getboolean(p4gf_config.SECTION_REPO,
                                  p4gf_config.KEY_ENABLE_BRANCH_CREATION)
        except ValueError:
            self.report_error(_("repository configuration option '{option}' has illegal value\n")
                              .format(option=p4gf_config.KEY_ENABLE_BRANCH_CREATION))

        # check merge commits option
        try:
            if config.has_option(p4gf_config.SECTION_REPO,
                                 p4gf_config.KEY_ENABLE_MERGE_COMMITS):
                config.getboolean(p4gf_config.SECTION_REPO,
                                  p4gf_config.KEY_ENABLE_MERGE_COMMITS)
        except ValueError:
            self.report_error(_("repository configuration option '{option}' has illegal value\n")
                              .format(option=p4gf_config.KEY_ENABLE_MERGE_COMMITS))

        # Examine them and confirm they have branch views and all RHS match
        enable_mismatched_rhs = \
            config.has_option(p4gf_config.SECTION_REPO,
                              p4gf_config.KEY_ENABLE_MISMATCHED_RHS) and \
            config.getboolean(p4gf_config.SECTION_REPO,
                              p4gf_config.KEY_ENABLE_MISMATCHED_RHS)
        first_branch = None
        for section in sections:
            try:
                branch, self.emsg = Branch.from_config(config, section,  strict=True)
                if not branch:
                    self.report_error(self.emsg)
                    return False

                if not branch.git_branch_name:
                    self.report_error(
                        _("repository configuration section [{section}] must contain"
                          " 'git-branch-name'").format(section=section))
                    return False

                for prefix in INVALID_GIT_BRANCH_NAME_PREFIXES:
                    if branch.git_branch_name.startswith(prefix):
                        self.report_error(
                            _("repository configuration section [{section}]: "
                              "'git-branch-name = {git_branch_name}' must not "
                              "start with '{prefix}'").format(
                                section=section,
                                git_branch_name=branch.git_branch_name,
                                prefix=prefix))
                        return False
            except RuntimeError as e:
                self.report_error("{0}\n".format(e))
                return False

            if view_lines_have_space_after_plus_minus(branch.view_lines):
                self.report_error(
                    _("space follows + or - in view line\n") +
                    _("view for branch '{branch}':\n{view_lines}\n").format(
                        branch=branch.branch_id,
                        view_lines=branch.view_lines))
                return False

            # NOT supported without the MAP() api.
            # check branch for set of view lines which describe an empty view
            # we get the views after passsing through P4.Map's disambiuator
#            if view_lines_define_empty_view(branch.view_p4map.lhs()):
#                msg = p4gf_const.EMPTY_VIEWS_MSG_TEMPLATE.format(repo_name=self.repo_name
#                    ,repo_name_p4client=self.repo_name)
#                self.report_error(msg)
#                return False

            # check that the view lines do not contain forbidden characters
            # (must do this before the left/right matching checks below)
            error_msg = view_lines_free_of_evil(branch.lhs, branch.rhs)
            if error_msg:
                error_msg = error_msg.format(branch_id=branch.branch_id,
                                             view_lines=branch.view_lines)
                self.report_error(error_msg)
                return False

            # check that the suffixes of the lhs and rhs of each viewline match
            error_msg = view_lines_suffixes_mismatch(branch.lhs, branch.rhs)
            if error_msg:
                error_msg = error_msg.format(branch_id=branch.branch_id,
                                             view_lines=branch.view_lines)
                self.report_error(error_msg)
                return False

            if enable_mismatched_rhs:
                continue

            if not first_branch:
                first_branch = branch
            else:
                if branch.rhs != first_branch.rhs:
                    self.report_error(
                        _("branch views do not have same right hand sides\n") +
                        _("view for branch '{branch_id}':\n{view_lines}\n").format(
                            branch_id=first_branch.branch_id,
                            view_lines=first_branch.view_lines) +
                        _("view for branch '{branch_id}':\n{view_lines}\n").format(
                            branch_id=branch.branch_id,
                            view_lines=branch.view_lines))
                    return False

            # Preserve all the branch lhs as a set
            for lhs in branch.lhs:
                self.lhs.add(lhs)
            #S ave each valid branch_id in the branch_dict
            self.branch_dict[branch.branch_id] = branch

        return True

    def _valid_depots(self):
        """Prohibit remote, spec, and other changelist-impaired depot types."""
        # Fetch all known Perforce depots.
        # python 2.6.6 does not support dictionary comprehension .. so ...
        depot_list = dict((depot['name'], depot) for depot in p4_run(['-ztag', 'depots']))

        # Scan all configured branches for prohibited depots.
        #
        branch_dict     = self.branch_dict
        valid           = True
        for branch in branch_dict.values():
            if not branch.lhs:
                continue
            v = self._view_valid_depots( depot_list
                                       , branch.branch_id
                                       , branch.lhs)
            valid = valid and v
        return valid

    def _view_valid_depots(self, depot_list, branch_id, lhs):
        """Prohibit remote, spec, and other changelist-impaired depot types."""
        valid = True

        # Extract unique list of referenced depots. Only want to warn about
        # each depot once per branch, even if referred to over and over.
        # lhs = view_p4map.lhs()
        referenced_depot_name_list = []
        for line in lhs:
            if line.startswith('-') or line.startswith('"-'):
                continue
            depot_name = depot_from_view_lhs(line)
            if not depot_name:
                self.report_error(
                    _("branch '{branch_id}':"
                      " badly formed depot syntax in view: '{line}' not permitted.\n'")
                    .format(branch_id=branch_id, line=line))
                valid = False
                continue
            if depot_name not in referenced_depot_name_list:
                referenced_depot_name_list.append(depot_name)

        # check each referenced depot for problems
        for depot_name in referenced_depot_name_list:
            if depot_name == p4gf_const.P4GF_DEPOT:
                self.report_error(
                    _("branch '{branch_id}':"
                      " Git Fusion internal depot '{depot_name}' not permitted.\n'")
                    .format( branch_id  = branch_id
                           , depot_name = depot_name))
                valid = False
                continue

            if depot_name not in depot_list:
                self.report_error(
                    _("branch '{branch_id}':"
                      " undefined depot '{depot_name}'"
                      " not permitted (possibly due to lack of permissions).\n'")
                    .format( branch_id  = branch_id
                           , depot_name = depot_name))
                valid = False
                continue

            depot = depot_list[depot_name]
            if depot['type'] not in [NTR('local'), NTR('stream')]:
                self.report_error(
                    _("branch '{branch_id}':"
                      " depot '{depot_name}' type '{depot_type}' not permitted.\n'")
                    .format( branch_id  = branch_id
                           , depot_name = depot_name
                           , depot_type = depot['type']))
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
                self.report_error(
                    _("repository configuration section '{section}' would be ignored\n")
                    .format(section=section))
                has_ignored_sections = True
        return has_ignored_sections

    def _ignored_options(self):
        """Confirm the @repo section does not contain any ignored options.

        Any error reporting will be done by this function.

        :rtyp: bool
        :return: True if ignored options found, False otherwise.

        """
        if DEBUG_LOG_FILE:
            debug_log("Validator._ignores_options() : begin")
        has_ignored_options = False
        # Basically all possible options can appear in [@repo]
        if self.config.has_section(p4gf_config.SECTION_REPO):
            all_options = set()
            all_options.add(p4gf_config.KEY_DESCRIPTION)
            all_options.add(p4gf_config.KEY_FORK_OF_REPO)
            all_options.add(p4gf_config.KEY_READ_ONLY)
            all_options.add(p4gf_config.KEY_FORK_OF_BRANCH_ID)
            all_options.add(p4gf_config.KEY_ENABLE_MISMATCHED_RHS)
            all_options.add(p4gf_config.KEY_FAST_PUSH_WORKING_STORAGE)
            all_options.add(p4gf_config.KEY_USE_SHA1_TO_SKIP_EDIT)
            default_cfg = p4gf_config.default_config_global()
            for section in default_cfg.sections():
                for option in default_cfg.options(section):
                    all_options.add(option)
            frm = _("repository configuration section '{section}' "
                    "contains ignored option '{option}'\n")
            for option in self.config.options(p4gf_config.SECTION_REPO):
                if option not in all_options:
                    self.report_error(frm.format(section=p4gf_config.SECTION_REPO,
                                                 option=option))
                    has_ignored_options = True
        return has_ignored_options


class ContentTrigger:

    """ContentTrigger class for the change-content trigger.

    This class does most of the trigger work.
    Used for p4 submit, p4 submit -e, p4 populate.

    1) It adds the list of files from the current changelist to
         the Reviews field of the git-fusion-reviews--non-gf user
    2) It calls 'p4 reviews' and passes/fails the submit based
       on the results of a reported collision with Git Fusion Reviews.
    3) If the Reviews fail the submit, the changelist files are removed from
           git-fusion-reviews--non-gf Reviews by this change-content trigger
    4) If it passed the submit, the subsequent change-commit trigger
        removes the same set of files from the Reviews


    There are three key determinations made by the triggers.
    Q1. What is the submit command? - submit, submit -e, or populate.
       This determines (2)
    Q2. Which arguments are used with 'p4 reviews' to determine contention for file updates
    Q3. How are the Review entries, which effect the atomic lock protections
       removed in case a submit fails after the GF change-content trigger succeeds.

    Q1:
    The submit command is passed as a trigger argument.
    This feature obviates the need of the change_submit trigger described just above.

    Q2:
    This issue is not p4d server dependent, but command dependent.
    'submit' and 'submit -e' run with the context of a p4 client.
    As such they may use the optimized 'p4 reviews -c -C' and
    avoid passing a list of files as arguments.
    For 'submit -e' the client name is actually the changelist number.

    'populate' does not run in the context of a client as must pass
    the list of changelist files as arguments.

    Q3:
    The addition of the change-failed trigger
    permits the trigger to remove files of the current failed changelist from
    the Reviews of the git-fusion-reviews--non-gf user.

    """
    # pylint: disable=too-many-arguments
    def __init__(self, change, client, command, trigger_type, args):
        self.change = change
        self.client = client
        self.command = command
        self.args = args
        self.trigger_type = trigger_type
        self.cfiles = None          # depot files from current change
        self.reviews_file = None     # tmp file used to pass cfiles to p4 reviews for populate
        self.is_locked = False
        self.is_in_union = False
        self.p4key_name = get_trigger_p4key_name(change)
        self.reviews_with_client = True
        self.submit_type = None
        self.pathlist    = []
        if self.trigger_type in DVCS_PRE_TRIGGER_TYPES:
            self.reviews_with_client = False
            self.submit_type = DVCS
            self.pathlist    = ['//...']
            self.cfiles    = ['//...']

        elif command == 'user-populate':
            self.submit_type = 'populate'
            self.reviews_with_client = False
        else:
            self.submit_type = 'submit'
            if ((self.command == 'user-submit' and self.args and ('-e' in self.args)) or
                    self.command == 'rmt-SubmitShelf'):
                self.submit_type = 'submit -e'


        # client is the changelist number for 'submit -e' for calling 'reviews -C client -c change'.
        self.reviews_client = self.change if self.submit_type == 'submit -e' else self.client

    def __str__(self):
        return "\nContentTrigger:\n" + \
            "change:{0} client:{1} command:{2} args {3}". \
            format(self.change, self.client, self.command, self.args) + \
            " reviews_with_client {0} submit_type:{1}". \
            format(self.reviews_with_client, self.submit_type)

    def check_if_locked_by_review(self):
        """Call the proper p4 reviews methods to check if GF has a lock on these submitted files."""
        if self.submit_type  == DVCS:
            self.is_locked, self.is_in_union = \
                get_reviews_using_arguments(self.cfiles, self.submit_type)

        elif self.reviews_with_client:
            self.is_locked, self.is_in_union = \
                get_reviews_using_client(self.change, self.reviews_client)
        else:
            # For populate, 'reviews' requires the list of changelist files
            # which is saved in file 'reviews_file' and passed as a file argument.
            # The file is preserved self.reviews_file for a second reviews call after adding
            # the file list to the git-fusion-reviews--non-gf user.
            self.get_cfiles()
            self.is_locked, self.reviews_file, self.is_in_union = \
                get_reviews_using_filelist(self.cfiles, self.reviews_file)

    def get_cfiles(self):
        """Lazy load of files from changelist."""
        if not self.cfiles:
            self.cfiles = p4_files_at_change(self.change)
        return self.cfiles

    def cleanup_populate_reviews_file(self):
        """Remove the reviews_file which exist only in the populate case."""

        # remove the input file to 'p4 -x file reviews'
        if self.reviews_file:
            remove_file(self.reviews_file)


def gf_reviews_user_name_list():
    """Return a list of service user names that match our per-server reviews user."""
    expr = p4gf_const.P4GF_REVIEWS_SERVICEUSER.format('*')
    r = p4_run(['users', '-a', expr])
    result = []
    for rr in r:
        if isinstance(rr, dict) and 'User' in rr:
            result.append(rr['User'])
    return result


def user_exists(user):
    """Return True if users exists."""
    r = p4_run(['users', '-a', user])
    for rr in r:
        if isinstance(rr, dict) and 'User' in rr:
            return True
    return False


def _encoding_list():
    """Return a list of character encodings, in preferred order.

    Use when attempting to read bytes of unknown encoding.
    """
    return ['utf8', 'latin_1', 'shift_jis']


def encode(data):
    """Attempt to encode using one of several code encodings."""
    if not PYTHON3:
        return data

    for encoding in _encoding_list():
        try:
            s = data.encode(encoding)
            return s
        except UnicodeEncodeError:
            pass
        except Exception as e:  # pylint: disable=broad-except
            print_log(str(e))
    # Give up, re-create and raise the first error.
    data.encode(_encoding_list[0])


def decode(bites):
    """Attempt to decode using one of several code pages."""
    for encoding in _encoding_list():
        try:
            s = bites.decode(encoding)
            return s
        except UnicodeDecodeError:
            pass
        except Exception as e:  # pylint: disable=broad-except
            print_log(str(e))
    # Give up, re-create and raise the first error.
    bites.decode(_encoding_list[0])


def _convert_bytes(data):
    """For python3, convert the keys in maps from bytes to strings.

    Recurses through
    the data structure, processing all lists and maps. Returns a new
    object of the same type as the argument. Any value with a decode()
    method will be converted to a string.
    For python2 - return data
    """
    def _maybe_decode(key):
        """Convert the key to a string using its decode() method.

        If decode() not available return the key as-is.
        """
        return decode(key) if 'decode' in dir(key) else key

    if not PYTHON3:
        return data

    if isinstance(data, dict):
        newdata = dict()
        for k, v in data.items():
            newdata[_maybe_decode(k)] = _convert_bytes(v)
    elif isinstance(data, list):
        newdata = [_convert_bytes(d) for d in data]
    else:
        # convert the values, too
        newdata = _maybe_decode(data)
    return newdata


def p4_print(depot_path):
    """Accumulate multiple 'data' entries to assemble content from p4 print."""
    result = p4_run(['print', '-q', depot_path])
    contents = ''
    for item in result:
        if 'data' in item and item['data']:
            contents += item['data']
    return contents


_unicode_error = [{'generic': 36,
                   'code': NTR('error'),
                   'data': _('Unicode server permits only unicode enabled clients.\n'),
                   'severity': 3}]


def first_dict(result_list):
    """Return the first dict result in a p4 result list.

    Skips over any message/text elements such as those inserted by p4broker.
    """
    for e in result_list:
        if isinstance(e, dict):
            return e
    return None


def p4_run(cmd, stdin=None, user=P4GF_USER, exit_on_error=True, client=P4CLIENT):
    """Use the -G option to return a list of dictionaries."""
    # pylint: disable=too-many-statements, too-many-branches
    raw_cmd = cmd

    # Data passed to stdin needs to be marshalled if it is form data.
    # Other stdin is expected to be plain text
    stdin_data = None
    if stdin is not None and stdin is not PIPE:
        stdin_data = stdin
        if not isinstance(stdin_data, bytes):
            stdin_data = encode(stdin_data)
        stdin = PIPE

    global CHARSET
    while True:
        cmd = [P4GF_P4_BIN, "-p", P4PORT, "-u", user, "-G" , "-c", client] + CHARSET + raw_cmd
        if DEBUG_LOG_FILE:
            debug_log("p4 begin:   {0}".format(cmd), lineno=inspect.currentframe().f_back.f_lineno)
        try:
            process = Popen(cmd, shell=False, stdin=stdin, stdout=PIPE, stderr=PIPE)
            if stdin_data is not None:
                process.stdin.write(stdin_data)
                process.stdin.close()

        except (OSError, ValueError) as e:
            print_log(_("Error calling Popen with cmd: {command}").format(command=cmd))
            print_log(_("Error: {exception}").format(exception=e))
            sys.stdout.flush()
            sys.exit(P4FAIL)

        data = []

        try:
            while True:
                data.append(marshal.load(process.stdout))
        except EOFError:
            pass
        ret = process.wait()
        if data:
            data = _convert_bytes(data)
        if ret != 0:
            # check for unicode error:
            if (not CHARSET) and (not stdin) and data and data == _unicode_error:
                # set charset and retry
                CHARSET = ['-C', 'utf8']
                continue

            else:
                error = process.stderr.read().splitlines()
                if exit_on_error and error and len(error) > 1:
                    for err in error:
                        if CONNECT_REGEX.match(_convert_bytes(err)):
                            print_log(_("Cannot connect to P4PORT: {p4port}").format(p4port=P4PORT))
                            sys.stdout.flush()
                            os._exit(P4FAIL)    # pylint: disable=protected-access
            data.append({"Error": ret})
        break
    if exit_on_error and len(data) and 'code' in data[0] and data[0]['code'] == 'error':
        if NOLOGIN_REGEX.match(data[0]['data']):
            errdata = data[0]['data'].strip()
            print_log(_("\n        Error in Git Fusion Trigger: {error_data}")
                      .format(error_data=errdata))
            p4tickets = os.environ['P4TICKETS'] if 'P4TICKETS' in os.environ else 'unset'
            # pylint: disable = line-too-long
            print_log(_("        Git Fusion Submit Trigger user '{user}' is not logged in. "
                        "P4PORT={p4port}   P4TICKETS={p4tickets}")
                      .format(user=user, p4port=P4PORT,  p4tickets=p4tickets))
            if user == p4gf_const.P4GF_REVIEWS__ALL_GF:
                print_log(_("        Your change is submitted. However, request "
                            "your admin login P4 user '{user}' and run: "
                            "'p4gf_submit_trigger.py --rebuild-all-gf-reviews'.")
                          .format(user=user))
            sys.exit(P4FAIL)
        m = TRUST_REGEX.match(data[0]['data'])
        if m:
            print_log(TRUST_MSG)
            sys.exit(P4FAIL)
    if exit_on_error and ret:
        # decode the 'data' in the error message
        d = None
        if data and 'data' in data[0]:
            d = data[0]['data']
            d = d.decode() if isinstance(d, bytes) else str(d)
        # Perforce reports an error when deleting a non-existing key
        # Detect this case and return without error
        if d and NOSUCHKEY_REGEX.match(d):
            if DEBUG_LOG_FILE:
                debug_log("    p4 ERROR end: {0}".format(cmd))
            return None
        print_log(_("Error in Git Fusion Trigger: P4PORT={p4port}").format(p4port=P4PORT))
        if data:
            if d:   # decoded 'data' ?
                print_log(d.strip())
                if PERMISSION_REGEX.match(d):
                    print_log(_("Check the Perforce protects table."))
            if 'Error' in data[0]:
                d = data[0]['Error']
                d = d.decode() if isinstance(d, bytes) else str(d)
                print_log(_("error={error}").format(error=d.strip()))
        print_log(_("Contact your administrator."))
        sys.exit(P4FAIL)
    if DEBUG_LOG_FILE:
        debug_log("    p4 end: {0}".format(cmd), lineno=inspect.currentframe().f_back.f_lineno)
    return data


def p4_run_ztag(cmd, stdin=None, user=P4GF_USER, exit_on_error=True, client=P4CLIENT):
    """Call p4 using the -ztag option to stdout.

    This is required to avoid sorting dictionary data when
    calling p4 reviews.
    """
    # pylint: disable=too-many-branches
    raw_cmd = cmd

    # Data passed to stdin needs to be marshalled if it is form data.
    # Other stdin is expected to be plain text
    stdin_data = None
    if stdin is not None and stdin is not PIPE:
        stdin_data = stdin
        if not isinstance(stdin_data, bytes):
            stdin_data = encode(stdin_data)
        stdin = PIPE

    cmd = [P4GF_P4_BIN, "-p", P4PORT, "-u", user, "-ztag", "-c", client] + CHARSET + raw_cmd
    if DEBUG_LOG_FILE:
        debug_log("p4 begin:   {0}".format(cmd), lineno=inspect.currentframe().f_back.f_lineno)
    try:
        process = Popen(cmd, shell=False, stdin=stdin, stdout=PIPE, stderr=PIPE)
        if stdin_data is not None:
            process.stdin.write(stdin_data)
            process.stdin.close()

    except (OSError, ValueError) as e:
        print_log(_("Error calling Popen with cmd: {command}").format(command=cmd))
        print_log(_("Error: {exception}").format(exception=e))
        sys.stdout.flush()
        sys.exit(P4FAIL)

    data = []
    while True:
        line = process.stdout.readline().strip()
        line = _convert_bytes(line)
        if line != '':
            line = re.sub(r'^\.\.\. ', '', line)
            data.append(line)
        else:
            break
    stderr = process.stderr.readlines()
    errdata = ''
    for l in stderr:
        l = l.strip()
        errdata += ' ' + _convert_bytes(l)
    ret = process.wait()
    if exit_on_error and ret:
        print_log(_("\n        Error in Git Fusion Trigger: {error}")
                  .format(error=errdata))
        if 'P4PASSWD' in errdata:
            p4tickets = os.environ['P4TICKETS'] if 'P4TICKETS' in os.environ else 'unset'
            # pylint: disable = line-too-long
            print_log(_("        Git Fusion Submit Trigger user '{user}' is not "
                        "logged in. P4PORT={p4port}   P4TICKETS={p4tickets}")
                      .format(user=user, p4port=P4PORT,  p4tickets=p4tickets))
            if user == p4gf_const.P4GF_REVIEWS__ALL_GF:
                print_log(_("        Your change is submitted. However, request "
                            "your admin login P4 user '{user}' and run: "
                            "'p4gf_submit_trigger.py --rebuild-all-gf-reviews'.")
                          .format(user=user))
        if DEBUG_LOG_FILE:
            debug_log("p4 end:   {0} {1}".format(cmd, errdata))
        sys.exit(P4FAIL)
    if data:
        data = _convert_bytes(data)
    if DEBUG_LOG_FILE:
        debug_log("p4 end:   {0}".format(cmd), lineno=inspect.currentframe().f_back.f_lineno)
    return data


def is_super(user):
    """Determine if user is a super user."""
    results = p4_run(['protects', '-u',  user], user=user)
    for r in results:
        if 'code' in r and r['code'] == 'error':
            return False
        if 'perm' in r and r['perm'] == 'super':
            return True
    return False


def set_p4key(name, value):
    """Set p4key."""
    p4_run(['key', name, value])


def inc_p4key(name, user=P4GF_USER):
    """Increment p4key."""
    p4key = p4_run(['key', '-i', name], user=user)[0]
    return p4key['value']


def delete_p4key(name, user=P4GF_USER):
    """Delete p4key."""
    if name:
        p4_run(['key', '-d', name], user=user)


def get_p4key(name, user=P4GF_USER):
    """Get p4key."""
    if not name:
        return None
    p4key = p4_run(['key',  name], user=user)[0]
    return p4key['value']


def get_p4key_lock(name, user=P4GF_USER):
    """Increment and test p4key for value == 1."""
    return '1' == inc_p4key(name, user=user)


def get_local_depots():
    """Get list of local depots."""
    depot_pattern = re.compile(r"^" + re.escape(p4gf_const.P4GF_DEPOT))
    data = p4_run(['-ztag', 'depots'])
    depots = []
    for depot in data:
        if (    (depot['type'] == 'local' or depot['type'] == 'stream')
            and not depot_pattern.search(depot['name'])):
            depots.append(depot['name'])
    return depots

def get_configureable(name, user=P4GF_USER):
    """Get configurable."""
    if not name:
        return None
    name_value = p4_run(['configure', 'show',  name], user=user)[0]
    return name_value['Value']


def p4_files_at_change(change):
    """Get list of files in changelist.

    p4 files@=CNN provides a valid file list during the change_content trigger.

    """
    depot_files = []
    depots = get_local_depots()
    for depot in depots:
        cmd = ['files']
        cmd.append("//{0}/...@={1}".format(depot, change))
        data = p4_run(cmd)
        for item in data:
            if 'depotFile' in item:
                depot_files.append(enquote_if_space(item['depotFile']))
    return depot_files


def has_files_at_change(change):
    """Determine if any files exist in the given changelist.

    p4 files@=CNN provides a valid file list during the change_content trigger.

    """
    depots = get_local_depots()
    for depot in depots:
        cmd = ['files', '-m1']
        cmd.append("//{0}/...@={1}".format(depot, change))
        data = p4_run(cmd)
        for item in data:
            if 'depotFile' in item:
                return True
    return False


def is_int(candidate):
    """Is the candidate an int?"""
    try:
        int(candidate)
        return True
    except ValueError:
        return False




def unlock_changelist(changelist, client):
    """Unlock the files in the failed changelist so GF may continue.

    Called as git-fusion-user with admin priviledges.
    """
    p4_run(['unlock', '-f', '-c', changelist], client=client)


def delete_all_p4keys():
    """Delete all non-Git Fusion p4keys."""
    p4keys = p4_run(['keys', '-e', p4gf_const.P4GF_REVIEWS_NON_GF_RESET + '*'])
    for p4key in p4keys:
        if 'key' in p4key:
            delete_p4key(p4key['key'])


def remove_file(file_):
    """Remove file from file system."""
    try:
        os.remove(file_.name)
    except IOError:
        pass


def find_depot_prefixes(depot_paths):
    """For each depot, find the longest common prefix."""
    prefixes = {}
    if not depot_paths:
        return prefixes
    last_prefix = None
    depot_pattern = re.compile(r'^//([^/]+)/')
    for dp in depot_paths:
        dp = dequote(dp)
        # since depot_paths is probably sorted, it's very likely
        # the current depot_path starts with the last found prefix
        # so check that first and avoid hard work most of the time
        if last_prefix and dp.startswith(last_prefix):
            continue
        # extract depot from the path and see if we already have a prefix
        # for that depot
        m = depot_pattern.search(dp)
        depot = m.group(1)
        depot_prefix = prefixes.get(depot)
        if depot_prefix:
            prefixes[depot] = last_prefix = os.path.commonprefix([depot_prefix, dp])
        else:
            prefixes[depot] = last_prefix = dp
    return prefixes.values()


def get_depot_patterns(depot_path_list):
    """Generate the reviews patterns for file list."""
    return [enquote_if_space(p + "...") for p in find_depot_prefixes(depot_path_list)]


def get_reviews_using_filelist(files, ofile=None):
    """Check if locked files in changelist are locked by GF in Reviews."""
    is_locked = False
    common_path_files = get_depot_patterns(files)
    if not ofile:
        ofile = write_lines_to_tempfile(NTR("islocked"), common_path_files)
    # else use the ofile which is passed in

    cmd = NTR(['-x', ofile.name, 'reviews'])
    users = p4_run(cmd)
    change_is_in_union = False
    for user in users:
        if 'code' in user and user['code'] == 'error':
            raise Exception(user['data'])
        _user = user['user']
        if _user.startswith(p4gf_const.P4GF_REVIEWS_GF):
            if _user == p4gf_const.P4GF_REVIEWS__ALL_GF:
                change_is_in_union = True
            elif _user != p4gf_const.P4GF_REVIEWS__NON_GF:
                print_log(MSG_LOCKED_BY_GF.format(user=user['user']))
                # reject this submit which conflicts with GF
                change_is_in_union = True
                is_locked = True
                break
    return (is_locked, ofile, change_is_in_union)


def get_reviews_using_client(change, client):
    """Check if locked files in changelist are locked by GF in Reviews."""
    is_locked = False

    cmd = NTR(['reviews', '-C', client, '-c', change])
    users = p4_run(cmd)
    change_is_in_union = False
    for user in users:
        if 'code' in user and user['code'] == 'error':
            raise Exception(user['data'])
        _user = user['user']
        if _user.startswith(p4gf_const.P4GF_REVIEWS_GF):
            if _user == p4gf_const.P4GF_REVIEWS__ALL_GF:
                change_is_in_union = True
            elif _user != p4gf_const.P4GF_REVIEWS__NON_GF:
                print_log(MSG_LOCKED_BY_GF.format(user=user['user']))
                # reject this submit which conflicts with GF
                change_is_in_union = True
                is_locked = True
                break
    return (is_locked, change_is_in_union)

def get_reviews_using_arguments(pathlist, submit_type):
    """Check if locked files in changelist are locked by GF in Reviews."""

    is_locked = False
    change_is_in_union = False
    if not pathlist:
        return (is_locked, change_is_in_union)
    cmd = NTR(['reviews'] + pathlist)
    users = p4_run(cmd)
    for user in users:
        if 'code' in user and user['code'] == 'error':
            raise Exception(user['data'])
        _user = user['user']
        if _user.startswith(p4gf_const.P4GF_REVIEWS_GF):
            if _user == p4gf_const.P4GF_REVIEWS__ALL_GF:
                change_is_in_union = True
            elif _user != p4gf_const.P4GF_REVIEWS__NON_GF:
                if submit_type == DVCS:
                    print_log(MSG_DVCS_LOCKED_BY_GF.format(user=user['user']))
                else:
                    print_log(MSG_LOCKED_BY_GF.format(user=user['user']))
                # reject this submit which conflicts with GF
                change_is_in_union = True
                is_locked = True
                break
    return (is_locked, change_is_in_union)

def set_submit_p4key(p4key_name, file_count, submit_type, client):
    """Set submit p4key using -x file input."""
    value = "{0}{1}{2}{3}{4}".format(
        submit_type,
        SEPARATOR,
        client,
        SEPARATOR,
        file_count)
    set_p4key(p4key_name, value)


def write_lines_to_tempfile(prefix_, lines):
    """Write list of lines to tempfile."""
    file_ = tempfile.NamedTemporaryFile(prefix='p4gf-trigger-' + prefix_, delete=False)
    for line in lines:
        ll = "%s\n" % dequote(line)
        file_.write(encode(ll))
    file_.flush()
    file_.close()
    return file_


def enquote_if_space(path):
    """Wrap path is double-quotes if SPACE in path."""
    if ' ' in path and not path.startswith('"'):
        path = '"' + path + '"'
    return path


def dequote(path):
    """Remove wrapping double quotes."""
    if path.startswith('"'):
        path = path[1:-1]
    return path


def shelved_files(change):
    """Return list of shelved files."""
    cfiles = []
    shelved_data = p4_run(['describe', '-S', change])[0]
    for key, value in shelved_data.items():
        if key.startswith('depotFile'):
            cfiles.append(enquote_if_space(value))
    return cfiles


def update_userspec(userspec, user, p4user=P4GF_USER):
    """Reset P4 userspec from local userspec dictionary."""
    if DEBUG_LOG_FILE:
        debug_log("update_userspec: begin: user={0} userspec size={1}".format(user, len(userspec)))
    newspec = ""
    for key, val in userspec.items():
        if key == 'Reviews':
            reviews = '\n' + key + ":\n"
            for line in val.splitlines():
                reviews = reviews + "\t" + line + "\n"
        else:
            newspec = "{0}\n{1}:\t{2}".format(newspec, key, val)
    newspec = newspec + reviews
    file_ = tempfile.NamedTemporaryFile(prefix='p4gf-trigger-userspec', delete=False)
    line = "%s" % newspec
    file_.write(encode(line))
    file_.flush()
    file_.seek(0)   # reset to 0th byte before passing to Popen as stdin
    if p4user != P4GF_USER:# assume this is super user as called by p4gf_super_init
        user_cmd =  ['-u', p4user, 'user', '-f', '-i']
    else:
        user_cmd =  ['-u', user, 'user', '-i']
    command = [P4GF_P4_BIN, '-p', P4PORT, '-c', P4CLIENT] +  CHARSET + user_cmd
    if DEBUG_LOG_FILE:
        debug_log("p4 begin:   {0}".format(command))
    p = Popen(command, shell=False, stdout=PIPE, stderr=PIPE, stdin=file_)
    stderr_data = p.communicate()[1]
    if DEBUG_LOG_FILE:
        debug_log("p4 end:   {0}".format(command))
    if p.returncode:
        print_log(stderr_data.decode('utf-8'))  # pylint: disable=no-member
        print_log(_("Error in submitting user spec for user {user}").format(user=user))
    try:
        file_.close()
        os.remove(file_.name)
    except IOError:
        pass

    if DEBUG_LOG_FILE:
        debug_log("update_userspec: end: user={0} ".format(user))

def add_unique_reviews(user, depot_files, change, p4user=P4GF_USER):
    """Add the files to Reviews to the user's user spec."""
    update_reviews(user, depot_files, change, action=ACTION_ADD_UNIQUE, p4user=p4user)


def append_reviews(user, depot_files, change, p4user=P4GF_USER):
    """Add the files to Reviews to the user's user spec."""
    update_reviews(user, depot_files, change, action=ACTION_ADD, p4user=p4user)


def remove_reviews(user, change, p4user=P4GF_USER):
    """Remove the files to Reviews to the user's user spec."""
    update_reviews(user, None, change, action=ACTION_REMOVE, p4user=p4user)


def reset_reviews(user, depot_files, p4user=P4GF_USER):
    """Remove all current files then add these files
    to Reviews of the user's spec."""
    update_reviews(user, depot_files, None, action=ACTION_RESET, p4user=p4user)


def unset_reviews(user, p4user=P4GF_USER):
    """Remove all files Reviews from the user's user spec."""
    update_reviews(user, None, None, action=ACTION_UNSET, p4user=p4user)


def review_path_in_changelist(path):
    """ Return True if path lies between (inclusive) the GF change markers.
    The path argument is passed in the list sequence from Reviews.
    """

    global CHANGE_FOUND_BEGIN, CHANGE_FOUND_END
    if not CHANGE_FOUND_BEGIN:
        if path == GF_BEGIN_MARKER:
            CHANGE_FOUND_BEGIN = True
            return True
        else:
            return False
    else:
        if CHANGE_FOUND_END:
            return False
        else:
            if path == GF_END_MARKER:
                CHANGE_FOUND_END = True
            return True


def update_reviews(user, depot_files, change, action=ACTION_ADD, p4user=P4GF_USER):
    """Add or remove Reviews to the user spec.

    add == Add the set of files
    remove == Remove the set of files
    unset == Set Reviews to none
    reset   == Set Reviews to these files
    """
    # pylint: disable=too-many-branches,too-many-statements
    global GF_BEGIN_MARKER,  GF_END_MARKER, CHANGE_FOUND_BEGIN, CHANGE_FOUND_END
    if DEBUG_LOG_FILE:
        debug_log("update_reviews: begin: user={0} change={1} action={2}".
                format(user, change, action))
    thisuser = p4user if p4user != P4GF_USER else user
    userspec = p4_run_ztag(['-Zspecstring', 'user', '-o', user], user=thisuser)
    newspec = {}
    current_reviews = []
    user_fields = []

    # get the list of the specfields
    for item in userspec:
        space = item.find(' ')
        key = item[:space]
        value = item[space+1:]
        if key == 'specdef':
            user_fields = [x.split(';')[0] for x in value.split(';;') if len(x)]
            break

    # Fetch the current reviews from userspec which contains the 'ReviewsNNN' fields
    # And the other fields into a dictionary

    for item in userspec:
        space = item.find(' ')
        key = item[:space]
        value = item[space+1:]
        if key == 'specdef':
            continue

        if not key.startswith('Review'):
            # need these to disallow extra and form-out data
            if key in user_fields:
                newspec[key] = value
        else:
            current_reviews.append(value.strip())

    # Convert list to set for ACTION_ADD_UNIQUE
    # This action is used only for P4GF_REVIEWS__ALL_GF
    # Assume the current reviews are UNIQUE
    # So this should be isomorphic
    if action == ACTION_ADD_UNIQUE:
        current_reviews = set(current_reviews)

    if action == ACTION_UNSET:
        newspec['Reviews'] = '\n'     # Set to empty
    else:
        if action == ACTION_ADD or action == ACTION_ADD_UNIQUE:
            if user == p4gf_const.P4GF_REVIEWS__NON_GF:
                current_reviews.append(
                    p4gf_const.NON_GF_REVIEWS_BEGIN_MARKER_PATTERN.format(change))
            if action == ACTION_ADD_UNIQUE:
                # in this case current_reviews is now a set
                current_reviews.update(set(depot_files))
            else:   # ACTION_ADD
                current_reviews += depot_files

            if user == p4gf_const.P4GF_REVIEWS__NON_GF:
                current_reviews.append(
                    p4gf_const.NON_GF_REVIEWS_END_MARKER_PATTERN.format(change))
        elif action == ACTION_RESET:
            current_reviews = depot_files
        else:   # remove by change is only called for p4gf_const.P4GF_REVIEWS__NON_GF
            CHANGE_FOUND_BEGIN = False
            CHANGE_FOUND_END = False
            GF_BEGIN_MARKER = p4gf_const.NON_GF_REVIEWS_BEGIN_MARKER_PATTERN.format(change)
            GF_END_MARKER = p4gf_const.NON_GF_REVIEWS_END_MARKER_PATTERN.format(change)
            current_reviews = [x for x in current_reviews if not review_path_in_changelist(x)]
        if len(newspec) > 0:
            newspec['Reviews'] = '\n'.join(current_reviews)
    update_userspec(newspec, user, p4user=p4user)
    if DEBUG_LOG_FILE:
        debug_log("update_reviews: end: user={0} change={1} action={2}".
                format(user, change, action))


def add_non_gf_reviews(content_trigger):
    """Add files in changelist to Reviews of non-GF user.

    trigger type determines name of p4key
    and method of getting list of files from changelist
    """
    # Set a key only for non dvcs push/pull triggers
    if content_trigger.submit_type != DVCS:
        set_submit_p4key(content_trigger.p4key_name, len(content_trigger.cfiles),
                     content_trigger.submit_type, content_trigger.client)
    append_reviews(p4gf_const.P4GF_REVIEWS__NON_GF, content_trigger.cfiles, content_trigger.change)


def remove_p4key_and_reviews(change):
    """Remove p4key and its reviews from non-gf user Spec."""

    p4key = get_trigger_p4key_name(change)
    remove_reviews(p4gf_const.P4GF_REVIEWS__NON_GF, change)
    delete_p4key(p4key)


def get_trigger_p4key_name(change):
    """Get the p4key name."""
    # Use no key for change == '0' (for dvcs triggers)
    if not change or change == '0':
        return None
    p4key_name = p4gf_const.P4GF_REVIEWS_NON_GF_SUBMIT + change
    return p4key_name




def change_content_trigger(change, client, command, trigger_type, args):
    """Reject p4 submit if change overlaps Git Fusion push.


    For 'p4 reviews:
      'The -C flag limits the files to those opened in the specified clients
       workspace,  when used with the -c flag limits the workspace to files
       opened in the specified changelist.'

    Using this option eliminates need for the files argument to 'p4 reviews'.
    However calls to 'p4 populate' may not take advantage of this featurer,
    there being no workspace associated with the populate.
    Thus triggers for 'p4 submit' and 'p4 populate' must handle the 'p4 reviews'
    differently.


    """
    # pylint: disable=too-many-branches
    if DEBUG_LOG_FILE:
        debug_log("change_content_trigger: begin: change={0} client={1} command={2} type={3}".
                format(change, client, command, trigger_type))
    returncode = P4PASS
    p4key_lock_acquired = False
    content_trigger = None

    try:
        # set methods and data for this change_content trigger
        content_trigger = ContentTrigger(change, client, command, trigger_type, args)
        content_trigger.check_if_locked_by_review()
        if content_trigger.is_locked:
            # Already locked by GF
            # Now reject this submit before we add any Reviews data
            returncode = P4FAIL
        elif content_trigger.is_in_union:   # needs protection from GF
            # Get the change list files into content_trigger.cfiles
            content_trigger.get_cfiles()

            # Now get the user spec lock
            acquire_p4key_reviews_common_lock()
            p4key_lock_acquired = True
            # add our Reviews
            add_non_gf_reviews(content_trigger)
            # now check again
            content_trigger.check_if_locked_by_review()
            if content_trigger.is_locked:
                # Locked by GF .. so remove the just added locked files from reviews
                remove_p4key_and_reviews(content_trigger.change)
                returncode = P4FAIL
        # not locked and not is_in_union do nothing
    # Catch Exception
    except Exception as exce:   # pylint: disable=broad-except
        print_log(MSG_PRE_SUBMIT_FAILED)
        print_log(_("Exception: {exception}").format(exception=exce))
        returncode = P4FAIL
    finally:
        if content_trigger:
            content_trigger.cleanup_populate_reviews_file()
        if p4key_lock_acquired:
            release_p4key_reviews_common_lock()
        if returncode == P4FAIL:
            # p4 unlock the files so that GF may proceed
            if change != '0':    # dvcs change arguement == 0
                unlock_changelist(change, client)
        if DEBUG_LOG_FILE:
            debug_log("change_content_trigger: finally: change={0} client={1} command={2} type={3}".
                format(change, client, command, trigger_type))
    if DEBUG_LOG_FILE:
        debug_log("change_content_trigger: end: change={0} client={1} command={2} type={3}".
                format(change, client, command, trigger_type))
    return returncode


def _read_config_string(config_, contents):
    """If unable to parse, convert generic ParseError to one that
    also contains a path to the unparsable file.
    """
    if PYTHON3:
        try:
            config_.read_string(contents)
            return (True, None)
        except configparser.Error as e:
            return (False, str(e))

    else:
        # python's ConfigParser requires sections and keys be left justified
        # while take care to not left-justify multi-line values
        # Only apply the left-justification if the first parse attempt fails.
        try:
            infp = cStringIO.StringIO(str(contents))
            config_.readfp(infp)
            return (True, None)
        except configparser.Error as e:
            try:
                stripped_list = []
                for l in  contents.splitlines():
                    if NEEDS_LEFT_JUSTIFIED_RE.search(l):
                        stripped_list.append(l.lstrip())
                    else:
                        stripped_list.append(l)
                stripped_contents = '\n'.join(stripped_list)
                infp = cStringIO.StringIO(str(stripped_contents))
                config_.readfp(infp)
                return (True, None)
            except configparser.Error as e:
                return (False, str(e))


def find_first_whitespace_not_in_quotes(vline):
    """Locate the first white_space not in quotes."""
    quote = '"'
    in_quote = False
    first_space = -1
    for i, c in enumerate(vline):
        if not in_quote:
            if c == quote:
                in_quote = True
            elif c in " \t":
                first_space = i
                break
        else:    # in_quote
            if c == '"':
                in_quote = False

    return first_space


def get_views_lhs_rhs(view_lines):
    """Return tuple of arrays of lhs and rhs for views in view_lines."""
    vlines = view_lines
    lhsides = []
    rhsides = []
    if isinstance(vlines, str):
        vlines = vlines.split()

    for line in vlines:
        # check for unsupported '&' prefix first
        if line.startswith('&') or line.startswith('"&'):
            errmsg = _("ampersand is not supported for view for branch '{0}':\n{1}\n")
            return ([], [], False, errmsg)
        have_depot_path = True
        # check for '//'
        depot_name = depot_from_view_lhs(line)
        if not depot_name:
            have_depot_path = False
            return ([], [], have_depot_path, None)
        lhs, rhs = get_lhs_rhs(line.strip())
        if lhs and rhs:
            lhsides.append(lhs)
            rhsides.append(rhs)

    return (lhsides, rhsides, have_depot_path, None)


def get_lhs_rhs(view_):
    """Extract the left map from the a config view line.

    If the left map starts with " it may not contain embedded quotes
    If the left map does not start with " it may contain embedded quotes
    If the left map starts with " only then may it contain embedded space

    Return None if the viewline is not well-formed
    """
    no_view = (None, None)
    if not view_:
        return no_view
    view = view_.strip()
    num_quotes = view_.count('"')

    # Unmatched quotes are an error
    if num_quotes % 2:
        return no_view
    # if no quotes , there must be one whitespace separator
    if num_quotes == 0 and len(view.split()) != 2:
        return no_view
    double_slash = view.find('//')
    # No double_slash is an error
    if double_slash < 0:
        return no_view

    first_whitespace = find_first_whitespace_not_in_quotes(view)
    if first_whitespace > -1:
        lhs = view[:first_whitespace]
        rhs = view[first_whitespace+1:].strip()
        # there may be internal pairs of double quotes
        # remove them and re-wrap with quotes
        # this reformatting copies that of P4.MAP()
        if lhs.count('"'):
            lhs = re.sub('"', '', lhs)
            lhs = '"' + lhs + '"'
        if rhs.count('"'):
            rhs = re.sub('"', '', rhs)
            rhs = '"' + rhs + '"'
        return (lhs, rhs)
    else:
        return no_view


def get_all_config_files(user):
    """Get list of all repos/*/p4gf_config files."""
    config_files = []
    data = p4_run(
        ['files', '-e', '//{0}/repos/*/p4gf_config'.format(p4gf_const.P4GF_DEPOT)],
        user=user)
    for _file in data:
        if 'depotFile' in _file and 'action' in _file:
            if 'delete' not in _file['action']:
                config_files.append(_file['depotFile'])
    return config_files


def get_p4gf_config_filelist(change):
    """Return list of p4gf_config files in this changelist."""
    config_files = []

    # Get the changelist file set
    data = p4_run(['describe', '-s', change])[0]

    # check against this regex for the p4gf_config file
    config_pattern = re.compile(
        r'^//' + re.escape(p4gf_const.P4GF_DEPOT) + '/repos/[^/]+/p4gf_config$')
    for key, value in data.items():
        if key.startswith('depotFile'):
            action_key = key.replace('depotFile', 'action')
            if 'delete' not in data[action_key]:
                if config_pattern.match(value):
                    config_files.append(enquote_if_space(value))
    return config_files


def validate_p4gf_config(change, config_files, add_action=None, user=P4GF_USER):
    """ Validate the p4gf_config in the list.

    Optionally add the views to the git-fusion-reviews--all-gf user.
    """
    # pylint: disable=too-many-branches
    if DEBUG_LOG_FILE:
        debug_log("validate_p4gf_config: {0}".format(config_files[0]))
    returncode = P4PASS
    repo_views = set()     # a set to add unique views to all-gf
    repo_l = len('//{0}/repos/'.format(p4gf_const.P4GF_DEPOT))

    for depot_file in config_files:
        # append @=change to depot path if change not None
        if change:
            contents = p4_print(depot_file+'@='+change)
        else:
            contents = p4_print(depot_file)
        v = Validator.from_contents(contents, depot_file)
        # We report errors differently for the --rebuild-all-gf case
        if add_action == ACTION_RESET:
            v.display_error = False
        if v.parses:
            if not v.is_valid():
                if add_action == ACTION_RESET:
                    print_log(_("Rebuild '{all_gf}' Reviews: ERROR: not adding "
                                "repo '{depot_file}' : {error}")
                              .format(all_gf=p4gf_const.P4GF_REVIEWS__ALL_GF,
                                      depot_file=depot_file,
                                      error=v.emsg))
                returncode = P4FAIL
            elif add_action and v.lhs:
                for lhs in v.lhs:
                    if re.match(r'^\"?-', lhs):  # starts with - or "-
                        continue
                    lhs = re.sub(r'\+', '', lhs)  # remove the +
                    repo_views.add(lhs)
                if add_action == ACTION_RESET:
                    repo_r = depot_file.rfind('/')
                    repo_name = depot_file[repo_l:repo_r]
                    print_log(_("Rebuild '{all_gf}' Reviews: adding repo views for '{repo_name}'")
                              .format(all_gf=p4gf_const.P4GF_REVIEWS__ALL_GF, repo_name=repo_name))
        else:
            if add_action == ACTION_RESET:
                print_log(_("Rebuild '{all_gf}' Reviews: ERROR: not adding repo "
                            "'{depot_file}' : {error}")
                          .format(all_gf=p4gf_const.P4GF_REVIEWS__ALL_GF,
                                  depot_file=depot_file, error=v.emsg))
            v.report_error(v.emsg)
            returncode = P4FAIL

    # Add the views for any p4gf_config which passed validation.
    # This code is excuted only by the change_commit trigger
    # after the p4gf_config is already submitted.
    if add_action == ACTION_ADD_UNIQUE and len(repo_views):
        add_unique_reviews(p4gf_const.P4GF_REVIEWS__ALL_GF, repo_views, None
                      , p4user=user)
    if add_action == ACTION_RESET:
        if len(repo_views) == 0:
            add_action = ACTION_UNSET
        update_reviews(p4gf_const.P4GF_REVIEWS__ALL_GF, repo_views, None
                      , action=add_action, p4user=user)
    return returncode


def change_content_p4gf_config(change):
    """Pre submit trigger on changes //p4gf_const.P4GF_DEPOT/repos/*/p4gf_config.

    Validate the p4gf_config and reject if needed.
    """
    # pylint: disable=too-many-branches
    if DEBUG_LOG_FILE:
        debug_log("change_content_p4gf_config: begin: change={0}".format(change))
    returncode = P4PASS
    config_files = get_p4gf_config_filelist(change)
    if not config_files:
        # probably a delete action
        return P4PASS

    if len(config_files) > 1:
        print_log(MSG_CANNOT_SUBMIT_MULTIPLE_CONFIG)
        return P4FAIL

    repo_name = None
    config_pattern = re.compile(
        r'^//' + re.escape(p4gf_const.P4GF_DEPOT) + '/repos/([^/]+)/p4gf_config$')
    m = config_pattern.match(config_files[0])
    if m:
        repo_name = m.group(1)
    else:
        print_log(MSG_CANNOT_SUBMIT_INVALID_PATH.format(path=config_files[0]))
        return P4FAIL

    if re.search('[@#*,"]', repo_name) or '%' in repo_name or '...' in repo_name:
        translated = repo_name.replace('%23','#').replace('%25','%') \
                    .replace('%40','@').replace('%2A','*').replace('%2a','*')
        print_log(MSG_ILLEGAL_REPO_NAME.format(raw_name=repo_name,
                                               translated_name=translated))
        return P4FAIL

    try:
        p4key_view_lock_acquired = acquire_repo_lock(repo_name)
        if p4key_view_lock_acquired:
            returncode = validate_p4gf_config(change,
                                              config_files,
                                              add_action=None)
        else:
            print_log(MSG_CANNOT_ACQUIRE_VIEWLOCK)
            returncode = P4FAIL
    except Exception as exce:   # pylint: disable=broad-except
        print_log(MSG_PRE_SUBMIT_FAILED)
        print_log(_("Exception: {exception}").format(exception=exce))
        returncode = P4FAIL
    finally:
        if p4key_view_lock_acquired:
            release_repo_lock(repo_name)
        if DEBUG_LOG_FILE:
            debug_log("change_content_p4gf_config: finally: change={0}".
                    format(change))
    if DEBUG_LOG_FILE:
        debug_log("change_content_p4gf_config: end: change={0}".format(change))
    return returncode


def change_commit_p4gf_config(change, user=P4GF_USER):
    """Post submit trigger on changes //p4gf_const.P4GF_DEPOT/repos/*/p4gf_config.

    Validate then add p4gf_config views to git-fusion-reviews--all-gf Reviews:.
    """
    if DEBUG_LOG_FILE:
        debug_log("change_commit_p4gf_config: begin: change={0}".format(change))
    returncode = P4PASS
    p4key_lock_acquired = False
    try:
        acquire_p4key_reviews_common_lock()
        p4key_lock_acquired = True
        config_files = get_p4gf_config_filelist(change)
        if config_files:  # not an empty list -  not a delete
            returncode = validate_p4gf_config(change,
                                              config_files,
                                              add_action=ACTION_ADD_UNIQUE,
                                              user=user)
    except Exception as exce:   # pylint: disable=broad-except
        print_log(MSG_POST_SUBMIT_FAILED)
        print_log(_("Exception: {exception}").format(exception=exce))
        returncode = P4FAIL
    finally:
        if p4key_lock_acquired:
            release_p4key_reviews_common_lock()
        if DEBUG_LOG_FILE:
            debug_log("change_commit_p4gf_config: finally: change={0}".
                    format(change))
    if DEBUG_LOG_FILE:
        debug_log("change_commit_p4gf_config: end: change={0}".format(change))
    return returncode

def pre_push_fetch_trigger(args):
    """pre dvs trigger for both pre-rmt-push and pre-user-fetch."""
    args.change='0'
    return change_content_trigger(args.change,args.client,
                                  args.command, args.trigger_type, args.args)

def post_push_fetch_trigger(args):
    """post dvs trigger for both pre-rmt-push and pre-user-fetch."""
    args.change='0'
    return change_commit_trigger(args.change, args.trigger_type)

def rebuild_all_gf_reviews(user=P4GF_USER):
    """Rebuild git-fusion-reviews--all-gf Reviews from //p4gf_const.P4GF_DEPOT/repos/*/p4gf_config.
    """
    returncode = P4PASS
    p4key_lock_acquired = False
    try:
        acquire_p4key_reviews_common_lock()
        p4key_lock_acquired = True
        config_files = get_all_config_files(user)
        validate_p4gf_config(None,
                             config_files,
                             add_action=ACTION_RESET,
                             user=user)
    except Exception as exce:   # pylint: disable=broad-except
        print_log(_("Exception: {exception}").format(exception=exce))
        returncode = P4FAIL
    finally:
        if p4key_lock_acquired:
            release_p4key_reviews_common_lock()
    return returncode

def change_commit_trigger(change, trigger_type):
    """Post-submit trigger for Git Fusion.

    Cleanup files from reviews for non-GF user.
    Main calls this with the old changelist
    """
    if DEBUG_LOG_FILE:
        debug_log("change_commit_trigger: begin: change={0} type={1}".format(change, trigger_type))
    returncode = P4PASS
    lock_acquired = False
    try:
        p4key_name = get_trigger_p4key_name(change)
        value = get_p4key(p4key_name)
        # No key is used for DVCS/15.1 triggers
        if (value and str(value) != "0") or trigger_type in DVCS_TRIGGER_TYPES:
            acquire_p4key_reviews_common_lock()
            lock_acquired = True
            remove_p4key_and_reviews(change)
    except Exception as exce:   # pylint: disable=broad-except
        print_log(MSG_POST_SUBMIT_FAILED)
        print_log(exce.args)
        returncode = P4FAIL
    finally:
        if lock_acquired:
            release_p4key_reviews_common_lock()
        if DEBUG_LOG_FILE:
            debug_log("change_commit_trigger: finally: change={0} type={1}".
                    format(change, trigger_type))
    if DEBUG_LOG_FILE:
        debug_log("change_commit_trigger: end: change={0} type={1}".
                format(change, trigger_type))
    return returncode


def change_failed_trigger(change, trigger_type):
    """Post-submit trigger for Git Fusion.

    Cleanup files from reviews for non-GF user.
    """
    if DEBUG_LOG_FILE:
        debug_log("change_failed_trigger: calls change_commit_trigger: change={0} type={1}".
                format(change, trigger_type))
    return change_commit_trigger(change, trigger_type)




@contextmanager
def p4key_lock(key_name, max_tries=0, fail_msg=None):
    """Acquire, yield, and release the lock in a safe manner."""
    acquire_p4key_lock(key_name, max_tries, fail_msg)
    try:
        yield
    finally:
        release_p4key_lock(key_name)
        if DEBUG_LOG_FILE:
            debug_log("p4key_lock: finally: key_name={0}".format(key_name))


def acquire_p4key_lock(key_name, max_tries=0, fail_msg=None):
    """Acquire a p4key based lock."""
    count = 0
    while True:
        if DEBUG_LOG_FILE:
            debug_log("acquire_p4key_lock: key={0}".format(key_name))
        if get_p4key_lock(key_name):
            break
        count = count + 1
        if max_tries and count > max_tries:
            if DEBUG_LOG_FILE:
                debug_log("acquire_p4key_lock: key={0} TIMEOUT exception".format(key_name))
            raise Exception(fail_msg)
        time.sleep(_RETRY_PERIOD)


def release_p4key_lock(key_name):
    """Release a p4key based lock."""
    delete_p4key(key_name)


def acquire_repo_lock(repo_name):
    """Acquire the lock for the named repo."""
    repo_lock_key = p4gf_const.P4GF_P4KEY_LOCK_VIEW.format(repo_name=repo_name)
    repo_lock_owners_key = p4gf_const.P4GF_P4KEY_LOCK_VIEW_OWNERS.format(repo_name=repo_name)
    with p4key_lock(repo_lock_key, max_tries=1):
        content = get_p4key(repo_lock_owners_key)
        if content != "0":
            return False
        content = {
            'server_id': 'p4gf_submit_trigger',
            'group_id': 'p4gf_submit_trigger',
            'owners': [
                {
                    'process_id': os.getpid(),
                    'start_time': get_datetime()
                }
            ]
        }
        set_p4key(repo_lock_owners_key, json.dumps(content))
    return True


def release_repo_lock(repo_name):
    """Release the lock for the named repo."""
    repo_lock_key = p4gf_const.P4GF_P4KEY_LOCK_VIEW.format(repo_name=repo_name)
    repo_lock_owners_key = p4gf_const.P4GF_P4KEY_LOCK_VIEW_OWNERS.format(repo_name=repo_name)
    with p4key_lock(repo_lock_key):
        delete_p4key(repo_lock_owners_key)


def acquire_p4key_reviews_common_lock():
    """Get the Reviews Common Lock."""
    acquire_p4key_lock(p4gf_const.P4GF_REVIEWS_COMMON_LOCK,
                       MAX_TRIES_FOR_COMMON_LOCK,
                       MSG_CANNOT_GET_COMMON_LOCK)
    content = {
        'server_id': 'p4gf_submit_trigger',
        'process_id': os.getpid(),
        'start_time': get_datetime()
    }
    set_p4key(p4gf_const.P4GF_REVIEWS_COMMON_LOCK_OWNER, json.dumps(content))


def release_p4key_reviews_common_lock():
    """Release the Reviews Common Lock."""
    delete_p4key(p4gf_const.P4GF_REVIEWS_COMMON_LOCK_OWNER)
    release_p4key_lock(p4gf_const.P4GF_REVIEWS_COMMON_LOCK)


def release_p4key_user_lock(user_name, user=P4GF_USER):
    """Delete any obsolete p4key user lock."""
    lock_name = p4gf_const.P4GF_P4KEY_LOCK_USER.format(user_name=user_name)
    delete_p4key(lock_name, user)


def marshal_dict(indict):
    """ Python2 style marshalled data formatter for Python3.

    p4 -G expects a dictionary of strings, but strings are unicode in Python3.
    This method constructs a marshalled dictionary of strings where the marshal
    library would produce a marshalled dictionary of unicode objects.
    """
    if not isinstance(indict, dict):
        return None

    def str_encode(instr):
        """string encoder."""
        data = bytes()
        num = len(instr)
        data += encode("s")
        data += bytes([num & 0xff, (num >> 8) & 0xff, (num >> 16) & 0xff, (num >> 24) & 0xff])
        data += encode(instr)
        return data

    data = bytes()
    # Dict start
    data += encode("{")

    for key, value in indict.items():
        # Check types
        if not isinstance(key, str) or not isinstance(value, str):
            return None
        data += str_encode(key) + str_encode(value)

    # Dict end
    data += encode("0")
    return data


def install_trigger_entries(new_gf_triggers, user):
    """Tool to take the generate trigger entries and install them in the
    Perforce Server. If existing Git-Fusion triggers are discoved, they are
    replaced; in this case we print both sets of triggers to stderr."""

    # pylint: disable=too-many-branches
    # Get the existing trigger table
    trigger_table = p4_run([NTR("triggers"), NTR("-o")], None, user)

    # If we were not super, we would not be here

    ntt = {}
    add_gf_triggers = []
    removed = []

    # Compute the list of trigger names we're going to install
    for t in new_gf_triggers.splitlines():
        words = re.findall(r'([^\s]+)', t)
        if len(words) > 0:
            # Add to the list of added triggers
            add_gf_triggers.append(NTR(" ").join(words))

    # Process the existing triggers
    # Get them in the right order first
    ott = []
    while NTR("Triggers") + str(len(ott)) in trigger_table[0]:
        ott.append(trigger_table[0][NTR("Triggers") + str(len(ott))])

    for t in ott:
        words = re.findall(r'([^\s]+)', t)
        if len(words) > 0:
            if words[0] in GF_TRIGGER_NAMES:
                # Add the trigger to the list of removed triggers
                removed.append(t)
            else:
                # Preserve the in the new trigger table
                ntt[NTR("Triggers") + str(len(ntt))] = t

    # Add the new triggers to the end of the new trigger table
    for t in add_gf_triggers:
        ntt[NTR("Triggers") + str(len(ntt))] = t

    # Set the triggers
    if PYTHON3:
        data = marshal_dict(ntt)
    else:
        data = marshal.dumps(ntt)
    out = p4_run([NTR("triggers"), NTR("-i")], stdin=data, user=user)

    if len(out) <= 0 or "level" not in out[0] or out[0]["level"] != 0:
        print("\n\nFailed to install new triggers!")
        return P4FAIL

    if len(removed) > 0:
        print(MSG_TRIGGER_REPLACED)
        for t in removed:
            print("\t"+t)
        print("\n\n" + MSG_TRIGGER_REPLACEMENTS)
        for t in add_gf_triggers:
            print("\t"+t)


def generate_tickets(suser):
    """Tool to generate tickets for each of the Git-Fusion users this script
    uses to do its work. This simplifies the process of logging in each user."""
    if not user_exists(P4GF_USER):
        print(_("'{user}' does not exist. Have you run configure-git-fusion.sh? Exiting.").
              format(user=P4GF_USER))
        return P4FAIL

    ticket_file = "~/.p4tickets"
    if "P4TICKETS" in os.environ:
        ticket_file = os.environ['P4TICKETS']
    print(MSG_WRITING_TICKETS.format(ticket_file=ticket_file))
    # P4GF_USER first (needed to run the users -a to get the list of users)
    print(MSG_GENERATING_TICKET.format(user=P4GF_USER))
    p4_run_ztag([NTR("login"), P4GF_USER], None, suser)

    users = gf_reviews_user_name_list()
    for u in users:
        print(MSG_GENERATING_TICKET.format(user=u))
        p4_run_ztag([NTR("login"), u], None, suser)


def print_config():
    """Tool to output the loaded configuration."""
    global CFG_EXTERNAL
    fstr = NTR("{option:<25}: {value}")
    if isinstance(P4GF_TRIGGER_CONF, str) and os.path.isfile(P4GF_TRIGGER_CONF):
        CFG_EXTERNAL = True
    if CFG_EXTERNAL:
        print(_("Active Configuration - from {config}")
              .format(config=P4GF_TRIGGER_CONF))
        set_globals_from_config_file()
    if not CFG_EXTERNAL:
        print(_("Active Configuration - using script defaults - no cfg file."))
    print(fstr.format(option=NTR("DEFAULT_P4HOST"),
                      value=DEFAULT_P4HOST))

    if not CFG_EXTERNAL:
        print(_("{option:<25}: {value}  ...  from {bin_path_var_name}: {bin_path}")
              .format(option=P4GF_P4_BIN_CONFIG_OPTION_NAME,
                      value=P4GF_P4_BIN,
                      bin_path_var_name=P4GF_P4_BIN_PATH_VAR_NAME,
                      bin_path=P4GF_P4_BIN_PATH))
    else:
        print(fstr.format(option=P4GF_P4_BIN_CONFIG_OPTION_NAME,
                          value=P4GF_P4_BIN))
    print(fstr.format(option=NTR("P4CHARSET"),
                      value=len(CHARSET) == 2 and CHARSET[1] or
                      _("Not set or overriden. Environment setting will be used.")))
    print(fstr.format(option=NTR("P4PORT"),
                      value=P4PORT or
                      _("Not set or overriden. Trigger parameter will be used.")))
    print(fstr.format(option=NTR("P4TICKETS"),
                      value=P4TICKETS or
                      _("Not set or overriden. Environment setting will be used.")))
    print(fstr.format(option=NTR("P4TRUST"),
                      value=P4TRUST or
                      _("Not set or overriden. Environment setting will be used.")))

    if not CFG_EXTERNAL:
        print(_("   ** {config} is missing or invalid.").format(config=P4GF_TRIGGER_CONF))


def get_p4d_user():
    """Utility to discover the user running the local P4D."""
    # pylint: disable=too-many-statements, too-many-branches, maybe-no-member
    user = None
    DEVNULL = open(os.devnull, 'wb')
    if platform.system() == "Windows":
        print(_("Please ensure that this script is run using 'run as " +
                "administrator'.\nIf not we might not be able to identify " +
                "the system user running the Perforce Server."))
        # We'll call WMIC to discover the owners of any P4D or P4S processes
        p1 = Popen(["wmic", "process", "WHERE",
                    "(Name=\"p4d.exe\" OR Name=\"p4s.exe\")",
                    "CALL", "GetOwner"], stdout=PIPE)
        while True:
            line = decode(p1.stdout.readline())
            if line == '':
                break
            if "User" in line:
                matches = re.search("\"(.+)\"", line)
                if matches is not None:
                    user = matches.group(1)
                    break
    else:
        # We'll call ps to discover the owners of any P4D processes
        p1 = Popen(["ps", "-aeo", "pid,uid,command"], stdout=PIPE)
        p2 = Popen(["grep", "[p]4d"], stdin=p1.stdout, stdout=PIPE, stderr=DEVNULL)
        users = []
        for line in p2.stdout:
            bits = decode(line).split(' ')
            while "" in bits:
                bits.remove("")
            if len(bits) > 1:
                users.append(pwd.getpwuid(int(bits[1])).pw_name)
        p2.stdout.close()

        # Make a unique list of users
        seen = set()
        users = [x for x in users if not (x in seen or seen.add(x))]

        # If there is more than one, we'll need to do more work
        if len(users) == 1:
            user = users[0]
        elif len(users) > 1 and platform.system() == 'Linux':
            # We'll need to try and identify our server using netstat
            n1 = Popen(["netstat", "-anelt"], stdout=PIPE)
            n2 = Popen(["grep", "^tcp"], stdin=n1.stdout, stdout=PIPE, stderr=DEVNULL)
            for line in n2.stdout:
                bits = decode(line).split(' ')
                while "" in bits:
                    bits.remove("")
                if len(bits) > 6:
                    port = bits[3].split(':').pop()
                    owner = pwd.getpwuid(int(bits[6])).pw_name
                    if port == P4PORT.split(':').pop() and owner in users:
                        user = owner
                        break
            n2.stdout.close()
        elif len(users) > 1:
            # Mostly for Darwin, but maybe other BSDs too?
            port_used = False
            # First, lets make sure the port is in use
            n1 = Popen(["netstat", "-anl", "-p", "tcp"], stdout=PIPE)
            n2 = Popen(["grep", "^tcp"], stdin=n1.stdout, stdout=PIPE, stderr=DEVNULL)
            for line in n2.stdout:
                bits = decode(line).split(' ')
                while "" in bits:
                    bits.remove("")
                if len(bits) > 3:
                    if ':' in bits[3]:
                        port = bits[3].split(':').pop()
                    else:   # OS X may presents address as NNN.NNN.NNN.NNN.PPPP   ( PPPP is port)
                        port = bits[3].split('.').pop()
                    if port == P4PORT.split(':').pop():
                        port_used = True
                        break
            n2.stdout.close()

            if port_used:
                # The port is in use, but if we're not root or the p4d user
                # we wont be able to see who is. If we default to root, then
                # we'll have to elevate and then we'll be able to see who the
                # owner really is.
                user = "root"
                port = P4PORT.split(':').pop()
                l1 = Popen(["lsof", "-iTCP:{0}".format(port)], stdout=PIPE)
                l2 = Popen(["grep", "LISTEN"], stdin=l1.stdout, stdout=PIPE, stderr=DEVNULL)
                for line in l2.stdout:
                    bits = decode(line).split(' ')
                    while "" in bits:
                        bits.remove("")
                    if len(bits) > 2:
                        user = bits[2]
                        break
                l2.stdout.close()
    DEVNULL.close()
    return user


def install(install_args, no_config):
    """Tool to write the configuration file and then generate tickets and
    install the triggers."""
    # pylint: disable=too-many-statements, too-many-branches
    print(_("\nThis script must be run on the same system as the Perforce Server.\n"))

    global P4PORT
    P4PORT = install_args[0]
    global CHARSET
    CHARSET = []

    # Who is p4duser? P4D is running (we just ran p4 info against it!)
    # This is a good test that the Perforce Server is running locally
    p4duser = get_p4d_user()
    if p4duser is None:
        print(_(
            "Failed to identify the system user running the Perforce Server.\n"
            "Is the binary named p4d?"))
        sys.exit(P4FAIL)

    # Find somewhere suitable for our tickets and trust files
    env_root, basename = os.path.split(P4GF_TRIGGER_CONF)

    p4gf_tickets = os.path.join(env_root, NTR("{0}.tickets".format(basename)))
    p4gf_trust   = os.path.join(env_root, NTR("{0}.trust".format(basename)))

    # This script must be run as root (we're writing to files we might not
    # normally be allowed to)
    if platform.system() != "Windows":
        current_user = pwd.getpwuid(os.geteuid()).pw_name
        if current_user != "root" and not no_config and not os.access(env_root, os.W_OK):
            print(_("This script must run with root privileges. Attempting to sudo!"))
            try:
                # If the CONFIG_PATH_ARG existed, it was removed prior to calling install()
                # Add it back to sys.argv for the re-call via sudo
                if CONFIG_PATH_ARG and CONFIG_PATH_INDEX:
                    sys.argv.insert(CONFIG_PATH_INDEX, CONFIG_PATH_ARG)
                sys.exit(call(" ".join(["sudo", "-H", PYTHON_PATH] + sys.argv), shell=True))
            except KeyboardInterrupt:
                sys.exit(P4FAIL)
        else:
            # Double chheck the home directory is set correctly
            os.environ['HOME'] = pwd.getpwuid(os.geteuid()).pw_dir

        if current_user != "root" and current_user == p4duser \
                and not no_config:
            # We're the Perforce Server user, but can we write to the
            # configuration files?
            if ((not os.path.exists(p4gf_tickets) or not os.path.exists(p4gf_trust)
                    or not os.path.exists(P4GF_TRIGGER_CONF))
                    and not os.access(env_root, os.W_OK)) \
                    or (os.path.exists(p4gf_tickets)
                        and os.stat(p4gf_tickets).st_uid != os.geteuid()) \
                    or (os.path.exists(p4gf_trust)
                        and os.stat(p4gf_trust).st_uid != os.geteuid()) \
                    or (os.path.exists(P4GF_TRIGGER_CONF)
                        and os.stat(P4GF_TRIGGER_CONF).st_uid != os.geteuid()):
                print(_("Configuration files are not owned or writable by this user!"))
                print(_("This script must run with root privileges. Attempting to sudo!"))
                # If the CONFIG_PATH_ARG existed, it was removed prior to calling install()
                # Add it back to sys.argv for the re-call via sudo
                if CONFIG_PATH_ARG and CONFIG_PATH_INDEX:
                    sys.argv.insert(CONFIG_PATH_INDEX, CONFIG_PATH_ARG)
                sys.exit(call(" ".join(["sudo", "-H", PYTHON_PATH] + sys.argv), shell=True))

    # Gather the details ------------------------------------------------------

    # Trust the server if using SSLglobal P4PORT
    if re.match('^ssl', P4PORT):
        p4_run(["trust", "-y", "-f"])

    # Ask Perforce about itself
    info = p4_run(["info", "-s"])

    # Check to see if the Perforce Server is in unicode mode
    if len(info) > 0 and 'unicode' in info[0] and info[0]['unicode'] == 'enabled':
        # Switch P4CHARSET to utf8
        CHARSET = ['-C', 'utf8']

    suser = install_args[1]
    login = p4_has_login(suser)
    # If super has a password set, prompt for password if it was not passed as a positional argument
    if  login != LOGIN_NO_PASSWD and  len(install_args) != 3:
        try:
            pw = getpass.getpass(_('super user password: '))
            if '\x03' in pw:
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            print( _("\n Stopping. Password input canceled."))
            sys.exit(P4FAIL)
        install_args.append(pw)

    # Ensure we are a super user
    suser = get_user_from_args(install_args, super_user_index=1, super_passwd_index=2)

    if not user_exists(P4GF_USER):
        print(_("'{user}' does not exist. Have you run configure-git-fusion.sh? Exiting.")
              .format(user=P4GF_USER))
        return P4FAIL

    triggers_io = get_configureable('triggers.io', suser)
    if triggers_io != '0':
        print(_("\nGit Fusion only supports Perforce server configurable 'triggers.io=0'."
                "This Perforce server at P4PORT={p4port} is set with 'triggers.io={triggers_io}'")
              .format(p4port=P4PORT, triggers_io=triggers_io))
        sys.exit(P4FAIL)

    if not no_config:
        # Update the environment ----------------------------------------------
        os.environ['P4TICKETS'] = p4gf_tickets
        os.environ['P4TRUST'] = p4gf_trust

        # Populate the configuration file
        config = configparser.ConfigParser()
        config.add_section(CFG_SECTION)
        config.set(CFG_SECTION, NTR("DEFAULT_P4HOST"), NTR("localhost"))
        config.set(CFG_SECTION, NTR("P4CHARSET"), NTR("none")
                   if len(CHARSET) < 2 else CHARSET[1])
        config.set(CFG_SECTION, NTR(P4GF_P4_BIN_CONFIG_OPTION_NAME), P4GF_P4_BIN)
        config.set(CFG_SECTION, NTR("P4TICKETS"), p4gf_tickets)
        config.set(CFG_SECTION, NTR("P4TRUST"), p4gf_trust)
        config.set(CFG_SECTION, NTR("P4PORT"), P4PORT)
        print(_("Writing configuration to configuration file: {config}")
              .format(config=P4GF_TRIGGER_CONF))
        for item in config.items(CFG_SECTION, raw=True):
            print(_("Option '{option}' set to '{value}'")
                  .format(option=item[0], value=item[1]))
        with open(P4GF_TRIGGER_CONF, "w") as cfg_file:
            config.write(cfg_file)

    # Trust the server if using SSL (new P4TRUST file!)
    if re.match('^ssl', P4PORT):
        p4_run(["trust", "-y", "-f"])

    # Ensure we're logged in (new P4TICKETS file!)
    get_user_from_args(install_args, super_user_index=1, super_passwd_index=2)

    # Gerate tickets for the Git Fusion users
    generate_tickets(suser)

    # Ensure P4GF_P4KEY_p4gf_const.P4GF_DEPOT is set
    _set_p4gf_depot_from_p4key()

    # Generate and install the triggers ---------------------------------------
    triggers = generate_trigger_entries([PYTHON_PATH, SCRIPT_PATH, P4PORT])
    if install_trigger_entries(triggers, suser) == P4FAIL:
        sys.exit(P4FAIL)

    # Set the version counter
    set_version_p4key()

    # chown/chmod the P4TICKETS, P4TRUST and P4GF_TRIGGER_CONF files
    if platform.system() != "Windows":
        p4d_pw = pwd.getpwnam(p4duser)
        if os.path.isfile(p4gf_tickets):
            os.chown(p4gf_tickets, p4d_pw.pw_uid, p4d_pw.pw_gid)
            os.chmod(p4gf_tickets, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)
        if os.path.isfile(p4gf_trust):
            os.chown(p4gf_trust, p4d_pw.pw_uid, p4d_pw.pw_gid)
            os.chmod(p4gf_trust, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)
        if os.path.isfile(P4GF_TRIGGER_CONF):
            os.chown(P4GF_TRIGGER_CONF, p4d_pw.pw_uid, p4d_pw.pw_gid)
            os.chmod(P4GF_TRIGGER_CONF, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def reset_all(user):
    """Tool to remove all GF and trigger Reviews and p4keys."""
    print(_("Removing all non-Git Fusion initiated reviews and p4keys"))
    delete_all_p4keys()
    for p4key in [p4gf_const.P4GF_P4KEY_TRIGGER_VERSION]:
        set_p4key(p4key,
                  "{0} : {1}".format(p4gf_const.P4GF_TRIGGER_VERSION, datetime.datetime.now()))
    for user_name in gf_reviews_user_name_list():
        if user_name != p4gf_const.P4GF_REVIEWS__ALL_GF:   # preserve the all-gf reviews
            unset_reviews(user_name, p4user=user)
        release_p4key_user_lock(user_name)
    release_p4key_reviews_common_lock()


def reset_one(p4user):
    """Tool to remove a single GF and trigger Reviews and p4keys."""
    if re.search(r'[/@#*%]|\.\.\.', SERVER_ID_NAME):
        print(_('Special characters (/, @, #, *, %, ...) not allowed in '
                'Git Fusion server ID {server_id}.')
              .format(server_id=SERVER_ID_NAME))
        sys.exit(P4FAIL)
    username = p4gf_const.P4GF_REVIEWS_SERVICEUSER.format(SERVER_ID_NAME)
    if username == p4gf_const.P4GF_REVIEWS__NON_GF or username == p4gf_const.P4GF_REVIEWS__ALL_GF:
        print(_('Cannot reset reviews for {user} user').format(user=username))
        sys.exit(P4FAIL)
    if not user_exists(username):
        print(_('Cannot reset reviews for a non-existing Git Fusion server'))
        sys.exit(P4FAIL)
    print(_("Removing reviews and lock for {server_id}").format(server_id=SERVER_ID_NAME))
    unset_reviews(username, p4user=p4user)
    release_p4key_user_lock(username)


def remove_obsolete_version_p4keys():
    """Remove, if exist,  the two previous trigger version key names."""
    value = get_p4key(P4GF_P4KEY_PRE_TRIGGER_VERSION)
    if value and str(value) != "0":
        delete_p4key(P4GF_P4KEY_PRE_TRIGGER_VERSION)
    value = get_p4key(P4GF_P4KEY_POST_TRIGGER_VERSION)
    if value and str(value) != "0":
        delete_p4key(P4GF_P4KEY_POST_TRIGGER_VERSION)

def set_version_p4key():
    """ Reset the Git Fusion Trigger version p4keys."""
    validate_port()
    if not user_exists(P4GF_USER):
        print(_("'{user}' does not exist. Have you run configure-git-fusion.sh? Exiting.")
              .format(user=P4GF_USER))
        return P4FAIL

    remove_obsolete_version_p4keys()

    _version = "{0} : {1}".format(p4gf_const.P4GF_TRIGGER_VERSION, datetime.datetime.now())
    set_p4key(p4gf_const.P4GF_P4KEY_TRIGGER_VERSION, _version)
    print(_("Setting '{key}' = '{value}'")
          .format(key=p4gf_const.P4GF_P4KEY_TRIGGER_VERSION, value=_version))
    return P4PASS  # Not real failure but trigger should not continue


def verify_version_p4key():
    """ Return P4PASS if p4d trigger version key matches that in this file."""
    validate_port()
    if not user_exists(P4GF_USER):
        print(_("'{user}' does not exist. Have you run configure-git-fusion.sh? Exiting.")
              .format(user=P4GF_USER))
        return P4FAIL
    _version = get_p4key(p4gf_const.P4GF_P4KEY_TRIGGER_VERSION)
    if ':' not in _version:
        print(_("Git Fusion trigger: '{file}' with version: '{version}' does not match\n"
                "   trigger version key '{key}={value}' in the Perforce Server.\n"
                "Have your administrator install the latest Git Fusion Perforce triggers.")
              .format(file=__file__,
                      version=p4gf_const.P4GF_TRIGGER_VERSION,
                      key=p4gf_const.P4GF_P4KEY_TRIGGER_VERSION,
                      value=_version))
        return P4FAIL
    _version = _version.split(':')[0].strip()

    if _version == p4gf_const.P4GF_TRIGGER_VERSION:
        fmt = _("Git Fusion trigger: '{file}' with version: '{version}' matches\n"
                "   trigger version key '{key}={value}' in the Perforce Server.")
        result = P4PASS
    else:
        fmt = _("Git Fusion trigger: '{file}' with version: '{version}' does not match\n"
                "   trigger version key '{key}={value}' in the Perforce Server.")
        result = P4FAIL
    print(fmt.format(file=__file__,
                     version=p4gf_const.P4GF_TRIGGER_VERSION,
                     key=p4gf_const.P4GF_P4KEY_TRIGGER_VERSION,
                     value=_version))
    return result


def validate_port():
    """Call sys_exit if we cannot connect."""
    colon = re.match(r'(.*)(:{1,1})(.*)', P4PORT)
    if colon:
        port = colon.group(3)
    else:
        port = P4PORT
    if not port.isdigit():  # pylint: disable=maybe-no-member
        print(_("Server port '{p4port}' is not numeric. Stopping.").format(p4port=P4PORT))
        print(_("args: {argv}").format(argv=sys.argv))
        sys.exit(P4FAIL)
    p4_run(["info", "-s"])


def p4_has_login(user):
    """Return login state for user."""
    login = p4_run(['login', '-s'], user=user, exit_on_error=False)[0]
    if 'TicketExpiration' in login:
        login = LOGIN_HAS_TICKET
    elif ( 'data' in login and
            login['data'].startswith( 'Perforce password (P4PASSWD) invalid or unset.')):
        if 'Perforce password (P4PASSWD) invalid or unset.' in login['data'] or \
           'Your session was logged out, please login again.' in login['data'] or \
           'Your session has expired, please login again.' in login['data']:
            login = LOGIN_NEEDS_TICKET
        else:
            print(_("Error in checking password status.\n{login}").format(login=login))
            sys.exit(P4FAIL)
    else:
        login = LOGIN_NO_PASSWD

    return login

def get_user_from_args(option_args, super_user_index=None, super_passwd_index=None):
    """Return P4GF_USER or super user if present."""
    validate_port()  # uses global P4PORT
    user = P4GF_USER
    if super_user_index and len(option_args) >= super_user_index+1:
        super_user = option_args[super_user_index]
    else:
        super_user = None
    if super_passwd_index and len(option_args) >= super_passwd_index+1:
        super_passwd = option_args[super_passwd_index]
    else:
        super_passwd = None
    if super_user:
        if super_passwd:
            p4_run_ztag(['login'], user=super_user, stdin=super_passwd)

        if not is_super(super_user):
            print(_("'{user}' is not super user. Exiting.").format(user=super_user))
            sys.exit(P4FAIL)
        else:
            user = super_user
    return user


class Args:

    """an argparse-like class to receive arguments from getopt parsing."""

    def __init__(self):
        self.use_config               = True
        self.no_config                = False
        self.reset                    = None
        self.rebuild_all_gf_reviews   = None
        self.set_version_p4key        = None
        self.verify_version_p4key     = None
        self.generate_trigger_entries = None
        self.install_trigger_entries  = None
        self.generate_tickets         = None
        self.show_config              = None
        self.install                  = None
        self.optional_command         = None
        self.oldchangelist            = None
        self.trigger_type             = None
        self.change                   = None
        self.user                     = None
        self.client                   = None
        self.port                     = None
        self.serverport               = None
        self.command                  = None   # parsing invalid value will remain = None
        self.args                     = None   # parsing invalid value will remain = None
        self.parameters               = []

    def __str__(self):
        return '  '.join(self.parameters)

    def __repr__(self):
        return self.__str__()


def display_usage_and_exit(mini=False, invalid=False):
    """Display mini or full usage."""
    if mini:
        mini_usage(invalid)
    else:
        usage()
    sys.stdout.flush()
    if invalid:
        sys.exit(P4FAIL)
    else:
        sys.exit(P4PASS)


def validate_option_or_exit(minimum, maximum, positional_len):
    """Validate option count."""
    if positional_len >= minimum and positional_len <= maximum:
        return True
    else:
        display_usage_and_exit(True, True)


def parse_argv():
    """Parse the command line options. """
    # pylint: disable=too-many-statements, too-many-branches
    global P4PORT
    trigger_opt_base_count = 5
    args = Args()
    short_opt = 'h'
    long_opt = NTR(['reset', 'rebuild-all-gf-reviews',
                'set-version-counter','set-version-p4key',
                'verify-version-counter','verify-version-p4key',
                'generate-trigger-entries',
                'install-trigger-entries', 'generate-tickets',
                'install', 'show-config', 'no-config', 'help'])
    try:
        options, positional = getopt.getopt(sys.argv[1:], short_opt, long_opt)
    except getopt.GetoptError as err:
        print(_("Command line options parse error: {error}").format(error=err))
        display_usage_and_exit(True, True)
    positional_len = len(positional)
    options_len    = len(options)
    if options_len > 1 and options[0][0] != '--install':
        print(_("options {options}").format(options=options))
        display_usage_and_exit(True, True)
    elif options_len >= 1:
        opt = options[0][0]
        args.optional_command = opt
        if opt in ("-h", "--help"):
            display_usage_and_exit(opt == '-h')
        elif opt == "--reset" and validate_option_or_exit(1, 2, positional_len):
            args.reset = positional
            args.port = args.reset[0]
        elif opt == "--rebuild-all-gf-reviews" and validate_option_or_exit(1, 3, positional_len):
            args.rebuild_all_gf_reviews = positional
            args.port = args.rebuild_all_gf_reviews[0]
            if len(args.rebuild_all_gf_reviews) > 2 and args.rebuild_all_gf_reviews[2] == "nocfg":
                args.use_config = False
        elif opt == "--set-version-counter" and validate_option_or_exit(1, 1, positional_len):
            args.set_version_p4key = positional
            args.port = args.set_version_p4key[0]
        elif opt == "--set-version-p4key" and validate_option_or_exit(1, 1, positional_len):
            args.set_version_p4key = positional
            args.port = args.set_version_p4key[0]
        elif opt == "--verify-version-counter" and validate_option_or_exit(1, 1, positional_len):
            args.verify_version_p4key = positional
            args.port = args.verify_version_p4key[0]
        elif opt == "--verify-version-p4key" and validate_option_or_exit(1, 1, positional_len):
            args.verify_version_p4key = positional
            args.port = args.verify_version_p4key[0]
        # --generate-trigger-entries - the /absolute/path/python is optional
        # thus the parameter count is variable
        elif opt == "--generate-trigger-entries" and validate_option_or_exit(
                MAX_GENERATE_TRIG_ARGS-3, MAX_GENERATE_TRIG_ARGS, positional_len):
            args.generate_trigger_entries = positional
            #args.port = args.generate_trigger_entries[len(args.generate_trigger_entries) - 1]
        # --install-trigger-entries has +1 parameters (superuser) than --generate-trigger-entries
        # and like --generate-trigger-entries - the /absolute/path/python is optional
        elif opt == "--install-trigger-entries" and validate_option_or_exit(
                MAX_INSTALL_TRIG_ARGS-2, MAX_INSTALL_TRIG_ARGS, positional_len):
            args.install_trigger_entries = positional
        elif opt == "--generate-tickets" and validate_option_or_exit(2, 2, positional_len):
            args.generate_tickets = positional
            args.port = args.generate_tickets[0]
        elif opt == "--install" and validate_option_or_exit(2, 4, positional_len):
            args.install = positional
            args.port    = args.install[0]
            if options_len >= 2 and options[1][0] == '--no-config':
                args.no_config = True
                args.use_config = False
        elif opt == "--show-config":
            args.show_config = [True]
        if args.port:  #
            P4PORT = args.port
            if DEBUG_LOG_FILE:
                debug_log("trigger optional command: {0} overriding P4PORT from argument: {1}".
                            format(opt, args.port))
            p4d_version_check()
    else:  # we have a trigger invocation from the server
        if positional_len >= trigger_opt_base_count:
            args.parameters = positional
            args.trigger_type = positional[0]
            args.change = positional[1]
            args.user = positional[2]
            args.client = positional[3]
            args.serverport = positional[4]
            idx = 5
            # the change-commit server contains the %oldchangelist% parameter
            if positional_len >= (idx + 1) and args.trigger_type == 'change-commit':
                args.oldchangelist = positional[idx]
                idx = idx + 1

            if (positional_len >= (idx + 1) and
                    positional[idx] != '%command%'):
                args.command = positional[idx]
                idx = idx + 1
            if (positional_len >= (idx + 1) and
                    positional[idx] != '%args%'):
                args.args = []
                while idx < positional_len:
                    args.args.append(positional[idx])
                    idx = idx + 1
        else:
            display_usage_and_exit(True, True)

    return args


def _substitute_host(serverport):
    """Substitute the host name portion of serverport with our default host."""
    parts = serverport.split(":")
    if len(parts) == 3:
        parts[1] = DEFAULT_P4HOST
    elif len(parts) == 2:
        parts[0] = DEFAULT_P4HOST
    return ":".join(parts)


def _set_p4gf_depot_from_p4key(user=P4GF_USER):
    """ p4gf_super_init sets the p4gf_const.P4GF_DEPOT p4key.
    This trigger requires this p4key to obtain this
    Git Fusion's depot name."""
    # global p4gf_const.P4GF_DEPOT

    # p4gf_const.P4GF_DEPOT = get_p4key(p4gf_const.P4GF_P4KEY_P4GF_DEPOT, user)
    P4gfConst.P4GF_DEPOT = get_p4key(p4gf_const.P4GF_P4KEY_P4GF_DEPOT, user)
    if p4gf_const.P4GF_DEPOT == '0':
        print_log(_("{p4gf_depot} is not set. Contact your admin to run configure-git-fusion.sh.")
                  .format(p4gf_depot=p4gf_const.P4GF_P4KEY_P4GF_DEPOT))
        sys.exit(P4FAIL)




def main():
    """Execute Git Fusion submit triggers."""
    # pylint: disable=too-many-branches, too-many-statements
    args = parse_argv()
    global P4PORT
    exitcode = P4PASS
    missing_args = False
    if args.use_config:
        # export P4TICKETS if set at top of this script
        if P4TICKETS:
            os.environ['P4TICKETS'] = P4TICKETS
        # export P4TRUST if set at top of this script
        if P4TRUST:
            os.environ['P4TRUST'] = P4TRUST
    if not args.use_config:
        # Use the P4CHARSET from the environment
        global CHARSET
        CHARSET = []
    if not args.optional_command:
        # we have been called as a p4d trigger
        if len(args.parameters) < 5:
            missing_args = True
        else:
            # Set P4PORT from %serverport% only if not set above to non-empty string
            # See P4PORT global override at top of this file
            if DEBUG_LOG_FILE:
                debug_log("START TRIGGER: user={0} change={1} trigger_type={2}".
                            format(args.user, args.change, args.trigger_type))
            if not P4PORT:
                P4PORT = _substitute_host(args.serverport)
            p4d_version_check()
            _set_p4gf_depot_from_p4key()
            if args.trigger_type in TRIGGER_TYPES:
                if args.trigger_type == 'change-content':
                    exitcode = change_content_trigger(args.change, args.client
                                                    , args.command, args.trigger_type, args.args)
                elif args.trigger_type == 'change-commit':
                    # the change-commit trigger sets the oldchangelist - use it
                    if args.oldchangelist:
                        args.change = args.oldchangelist
                    exitcode = change_commit_trigger(args.change, args.trigger_type)
                elif args.trigger_type == 'change-failed':
                    exitcode = change_failed_trigger(args.change, args.trigger_type)
                elif args.trigger_type == 'change-commit-p4gf-config':
                    exitcode = change_commit_p4gf_config(args.change)
                elif args.trigger_type == 'change-content-p4gf-config':
                    exitcode = change_content_p4gf_config(args.change)
                elif args.trigger_type in DVCS_PRE_TRIGGER_TYPES:
                    exitcode = pre_push_fetch_trigger(args)
                elif args.trigger_type in DVCS_POST_TRIGGER_TYPES:
                    exitcode = post_push_fetch_trigger(args)
            else:
                print_log(_("Invalid trigger type: {trigger_type}")
                          .format(trigger_type=args.trigger_type))
                exitcode = P4FAIL
            if DEBUG_LOG_FILE:
                debug_log("END   TRIGGER: user={0} change={1} trigger_type={2}".
                        format(args.user, args.change, args.trigger_type))
    else:
        # we have been called with optional command arguments to perform a support task
        # parse_argv() has set P4PORT from the command line port argument

        if args.set_version_p4key:
            sys.exit(set_version_p4key())
        elif args.verify_version_p4key:
            sys.exit(verify_version_p4key())
        elif args.optional_command == '--generate-trigger-entries':
            print(generate_trigger_entries(args.generate_trigger_entries))

        elif args.install_trigger_entries:
            #pylint: disable=unused-variable
            (path_to_python, path_to_trigger, p4port, suser) = \
                get_generate_install_args(args.install_trigger_entries)
            if not p4port or not suser:
                display_usage_and_exit(True, True)
            P4PORT = p4port
            _set_p4gf_depot_from_p4key()
            triggers = generate_trigger_entries(args.install_trigger_entries[:-2])
            install_trigger_entries(triggers, suser)

        elif args.generate_tickets:
            user = get_user_from_args(args.generate_tickets, super_user_index=1)
            generate_tickets(user)

        elif args.show_config:
            print_config()

        elif args.install:
            install(args.install, args.no_config)

        elif args.reset:
            # Check if an optional user arg was passed and whether it is a super user
            user = get_user_from_args(args.reset, super_user_index=1)
            _set_p4gf_depot_from_p4key(user=user)
            if SERVER_ID_NAME:
                reset_one(user)
            else:
                # Remove all the p4keys and reviews to reset
                reset_all(user)

        elif args.rebuild_all_gf_reviews:
            # Check if an optional user arg was passed and whether it is a super user
            user = get_user_from_args(args.rebuild_all_gf_reviews, super_user_index=1)
            _set_p4gf_depot_from_p4key(user=user)
            exitcode = rebuild_all_gf_reviews(user=user)

    if missing_args:
        mini_usage(invalid=True)
        exitcode = P4FAIL

    sys.exit(exitcode)

if __name__ == "__main__":
    if sys.hexversion < 0x02060000 or \
            (sys.hexversion > 0x03000000 and sys.hexversion < 0x03020000):
        print(_("Python 2.6+ or Python 3.2+ is required"))
        sys.exit(P4FAIL)
    try:
        main()
    finally:
        if DEBUG_LOG_FILE:
            DEBUG_LOG_FILE.flush()
            DEBUG_LOG_FILE.close()

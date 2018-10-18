#! /usr/bin/env python
"""Git Fusion edit / lock  triggers.


These triggers coordinate with Git Fusion to support git atomic pushes.
Service user accounts use p4 user Reviews to manage list of locked files.
There is one service user per Git Fusion instance
and one for non Git Fusion submits.
This trigger is compatible with python versions 2.x >= 2.6 and >= 3.3
The trigger is compatible with p4d versions >= 2014.1.
"""
# pylint:disable=W9903
# Skip localization/translation warnings about config strings
# here at the top of the file.

# -- Configuration ------------------------------------------------------------
# Edit these constants to match your p4d server and environment.

# Set the external configuration file location (relative to the script location
# or an absolute path)
P4GF_TRIGGER_CONF      = "p4gf_submit_trigger.cfg"
CONFIG_PATH_ARG_PREFIX = '--config-path='    # prefix to identify alternate config path argument
CONFIG_PATH_ARG        = ''  # actual arg set into trigger entries by --generate
CONFIG_PATH_INDEX      = None  # used to insert CONFIG_PATH_ARG back into sys.argv
                               # for sudo re-invocation with  --install option

# If a trigger configuration file is found, the configuration will be read from
# that file: Anything set there will override the configuration below.

# For unicode servers uncomment the following line
# CHARSET = ['-C', 'utf8']
CHARSET = []

P4GF_P4_BIN_PATH_VAR_NAME = "P4GF_P4_BIN_PATH"
P4GF_P4_BIN_CONFIG_OPTION_NAME = "P4GF_P4_BIN"

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
import sys

# Determine python version
PYTHON3 = True
if sys.hexversion < 0x03000000:
    PYTHON3 = False

# Exit codes for triggers, sys.exit(CODE)
P4PASS = 0
P4FAIL = 1

# Import the configparser - either from python2 or python3
try:
    # python3.x import
    import configparser                 # pylint: disable=import-error
    PARSING_ERROR = configparser.Error  # pylint: disable=invalid-name
except ImportError:
    # python2.x import
    import ConfigParser
    PARSING_ERROR = ConfigParser.Error  # pylint: disable=invalid-name
    configparser = ConfigParser         # pylint: disable=invalid-name

P4GF_USER = "git-fusion-user"


# these imports here to avoid unneeded processing before the early exit test above

import os
import re
from   subprocess import Popen, PIPE
import marshal
import tempfile

                        # Optional localization/translation support.
                        # If the rest of Git Fusion's bin folder
                        # was copied along with this file p4gf_submit_trigger.py,
                        # then this block loads LC_MESSAGES .mo files
                        # to support languages other than US English.
try:
    from p4gf_l10n import _, NTR
except ImportError:
                        # pylint:disable=invalid-name
                        # Invalid name NTR()
    def NTR(x):
        """No-TRanslate: Localization marker for string constants."""
        return x
    _ = NTR

USAGE = _("""
    This edit/lock trigger prevents a p4 client from locking a depot path which
    exists under the view definitions of a concurrent Git Fusion repo push.
    The following client commands are rejected:
      'p4 edit' - reject if any file is of type '+l' (such files would have a exclusive edit lock)
      'p4 lock' - reject

    Deploy this trigger using these trigger entries:
      GF-edit command pre-user-edit " /path/to/python /path/to/p4gf_edit_lock_trigger.py %command% %user% %serverport% %client% %clientcwd% %args%"
      GF-lock command pre-user-lock " /path/to/python /path/to/p4gf_edit_lock_trigger.py %command% %user% %serverport% %client% %clientcwd% %args%"

    'python' or 'python3' may be used to execute the script.
""")

# Get and store the full paths to this script and the interpreter
SCRIPT_PATH = os.path.realpath(__file__)
PYTHON_PATH = sys.executable

# Try and load external configuration
CFG_SECTION = "configuration"
CFG_EXTERNAL = False

def is_trigger_install():
    """Always False for edit_lock_trigger."""

    return False

MSG_CONFIG_FILE_MISSING    =_("Git Fusion Edit Lock Trigger: config file does not exist:'{0}'")
MSG_CONFIG_PATH_NOT_ABSOLUTE = \
        _("Git Fusion Edit Lock Trigger: argument must provide an absolute path:'{0}'")

def validate_config_path(config_path_file_path):
    """Ensure the config_path is absolute and exists."""

    if not len(config_path_file_path) or not os.path.isabs(config_path_file_path):
        print(MSG_CONFIG_PATH_NOT_ABSOLUTE.format(CONFIG_PATH_ARG))
        sys.exit(P4FAIL)
    if  not is_trigger_install() and not os.path.exists(config_path_file_path):
        print(MSG_CONFIG_FILE_MISSING.format(config_path_file_path))
        sys.exit(P4FAIL)
    return config_path_file_path

def extract_config_file_path_from_args():
    """Remove and set the --config-path argument from sys.arg.

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
            print(_("'{path_arg}' argument must provide an absolute path.")
                  .format(path_arg=CONFIG_PATH_ARG))
            sys.exit(P4FAIL)
    return None


def set_globals_from_config_file():
    """Detect and parse the p4gf_submit_trigger.cfg setting configuration variables.

    A missing config file is passed. A missing default path is acceptable.
    A missing --config-path=<path> is reported and failed earlier
    in extract_config_file_path_from_args().
    """
    global P4GF_TRIGGER_CONF, DEFAULT_P4HOST, CHARSET
    global P4GF_P4_BIN_PATH, P4PORT, P4TICKETS, P4TRUST, CFG_EXTERNAL
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
                if TRIG_CONFIG.has_option(CFG_SECTION, "P4CHARSET"):
                    CHARSET = ["-C", TRIG_CONFIG.get(CFG_SECTION, "P4CHARSET")]
                if TRIG_CONFIG.has_option(CFG_SECTION, P4GF_P4_BIN_CONFIG_OPTION_NAME):
                    P4GF_P4_BIN_PATH = TRIG_CONFIG.get(CFG_SECTION, P4GF_P4_BIN_CONFIG_OPTION_NAME)
                if TRIG_CONFIG.has_option(CFG_SECTION, "P4PORT"):
                    P4PORT = TRIG_CONFIG.get(CFG_SECTION, "P4PORT")
                if TRIG_CONFIG.has_option(CFG_SECTION, "P4TICKETS"):
                    P4TICKETS = TRIG_CONFIG.get(CFG_SECTION, "P4TICKETS")
                if TRIG_CONFIG.has_option(CFG_SECTION, "P4TRUST"):
                    P4TRUST = TRIG_CONFIG.get(CFG_SECTION, "P4TRUST")
            else:
                raise Exception(_("Didn't find section {0} in configuration file {1}")
                                .format(CFG_SECTION, P4GF_TRIGGER_CONF))
        except Exception as config_e:   # pylint: disable=broad-except
            print(_("Failed to load configuration from external file {config_path}\n"
                    "Error: {exception}").
                  format(config_path=P4GF_TRIGGER_CONF, exception=config_e))
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

    user_index = 2
    if len(sys.argv) >= user_index + 1 and sys.argv[user_index] == P4GF_USER:
        sys.exit(P4PASS)   # do nothing

# Fetch configuration before testing for presence of p4 executable just below
CONFIG_PATH_FILE_PATH = extract_config_file_path_from_args()
if CONFIG_PATH_FILE_PATH:
    P4GF_TRIGGER_CONF = CONFIG_PATH_FILE_PATH
set_globals_from_config_file()


# Find the 'p4' command line tool.
# If this fails, edit P4GF_P4_BIN_PATH in the "Configuration"
# block at the top of this file.
import distutils.spawn
P4GF_P4_BIN = distutils.spawn.find_executable(P4GF_P4_BIN_PATH)
if not P4GF_P4_BIN:
    print(_("Git Fusion Edit Lock Trigger cannot find p4 binary: '{bin_path}'"
            "\nPlease update this trigger using the full path to p4").
          format(bin_path=P4GF_P4_BIN_PATH))
    sys.exit(P4FAIL)  # Cannot find the binary

# disallow SPACE in path name
if ' ' in P4GF_P4_BIN:
    print(_("Please edit p4gf_submit_trigger.py and set P4GF_P4_BIN to a path without spaces."))
    sys.exit(P4FAIL)  # Space in binary path

skip_trigger_if_gf_user()

# -----------------------------------------------------------------------------
#                 Begin block of Globals defined in p4gf_const.py
#
#                 This variables must be consistent with Git Fusion.
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

P4GF_DEPOT         = NTR('.git-fusion')


P4GF_REVIEWS_GF                     = NTR('git-fusion-reviews-') # Append GF server_id.
P4GF_REVIEWS__NON_GF                = P4GF_REVIEWS_GF + NTR('-non-gf')
P4GF_REVIEWS__ALL_GF                = P4GF_REVIEWS_GF + NTR('-all-gf')

#
#                 End block copied to both p4gf_const.py
#                 and p4gf_submit_trigger.py.
# -----------------------------------------------------------------------------

MSG_CANNOT_EXCL_EDIT = _("\nYou may not exclusively 'edit' a file" \
                      " already locked by Git Fusion user '{file}'.")
MSG_CANNOT_LOCK = _("\nYou may not 'lock' a file" \
                      " already locked by Git Fusion user '{file}'.")


# Edit these as needed for non-English p4d error messages
NOLOGIN_REGEX         = re.compile(r'Perforce password \(P4PASSWD\) invalid or unset')
CONNECT_REGEX         = re.compile(r'.*TCP connect to.*failed.*')
TRUST_REGEX  = re.compile(r"^.*authenticity of '(.*)' can't.*fingerprint.*p4 trust.*$",
    flags=re.DOTALL)
TRUST_MSG  = _("""
\nThe Git Fusion trigger has not established trust with its ssl enabled server.
Contact your adminstrator and have them run {command}""").format(
    command=NTR("'p4 trust'."))


def usage():
    """Display full usage."""
    print(USAGE)
    print(_("args: {argv}").format(argv=sys.argv))


def locked_by_review(depot_files):
    """Call the p4 reviews methods to check if GF has a lock on these files."""
    return get_reviews_using_filelist(depot_files)


def p4_write_data(cmd, data, stdout=None):
    """Execute command with data passed to stdin."""
    cmd = [P4GF_P4_BIN, "-p", P4PORT] + CHARSET + cmd
    process = Popen(cmd, bufsize=-1, stdin=PIPE, shell=False, stdout=stdout)
    pipe = process.stdin
    val = pipe.write(data)
    pipe.close()
    if stdout is not None:
        pipe = process.stdout
        pipe.read()
    if process.wait():
        raise Exception(_('Command failed: {command}').format(command=cmd))
    return val


def _encoding_list():
    """Return a list of character encodings.

    List in preferred order to use when attempting to read bytes of unknown
    encoding.
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
            print(str(e))
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
            print(str(e))
    # Give up, re-create and raise the first error.
    bites.decode(_encoding_list[0])


def _convert_bytes(data):
    """For python3, convert the keys in maps from bytes to strings.

    Recurses through the data structure, processing all lists and maps. Returns
    a new object of the same type as the argument. Any value with a decode()
    method will be converted to a string.
    For python2 - return data
    """
    def _maybe_decode(key):
        """Convert the key to a string using its decode() method.

        If no decode() method available, return the key as-is.
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


_unicode_error = [{'generic': 36,
                   'code': NTR('error'),
                   'data': _('Unicode server permits only unicode enabled clients.\n'),
                   'severity': 3}]


def p4_run(cmd, stdin=None, user=P4GF_USER):
    """Use the -G option to return a list of dictionaries."""
    # pylint: disable=too-many-branches
    raw_cmd = cmd
    global CHARSET
    while True:
        cmd = [P4GF_P4_BIN, "-p", P4PORT, "-u", user, "-G"] + CHARSET + raw_cmd
        try:
            process = Popen(cmd, shell=False, stdin=stdin, stdout=PIPE, stderr=PIPE)
        except (OSError, ValueError) as e:
            print(_("Error calling Popen with cmd: {command}\n"
                    "Error: {exception}")
                  .format(command=cmd, exception=e))
            sys.stdout.flush()
            sys.exit(1)

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
            if (not CHARSET) and (not stdin) and data == _unicode_error:
                # set charset and retry
                CHARSET = ['-C', 'utf8']
                continue

            else:
                error = process.stderr.read().splitlines()
                if error and len(error) > 1:
                    for err in error:
                        if CONNECT_REGEX.match(_convert_bytes(err)):
                            print(_("Cannot connect to P4PORT: {P4PORT}")
                                  .format(P4PORT=P4PORT))
                            sys.stdout.flush()
                            os._exit(P4FAIL)    # pylint: disable=protected-access
            data.append({"Error": ret})
        break
    if len(data) and 'code' in data[0] and data[0]['code'] == 'error':
        if NOLOGIN_REGEX.match(data[0]['data']):
            print(_("\nGit Fusion Submit Trigger user '{user}' is not logged in.\n{error}").
                  format(user=user, error=data[0]['data']))
            sys.exit(P4FAIL)
        m = TRUST_REGEX.match(data[0]['data'])
        if m:
            print(TRUST_MSG)
            sys.exit(P4FAIL)
    if ret:
        print(_("Error in Git Fusion Trigger \n{ret_code}").format(ret_code=ret))
        if data:
            print(_("Error in Git Fusion Trigger \n{error}").format(error=data))
        sys.exit(P4FAIL)
    return data


def remove_file(file_):
    """Remove file from file system."""
    try:
        os.remove(file_.name)
    except IOError:
        pass


def get_reviews_using_filelist(depot_paths):
    """Check if locked files in changelist are locked by GF in user spec Reviews."""
    gf_user = None
    is_locked = False
    ofile = write_lines_to_tempfile(NTR("islocked"), depot_paths)

    cmd = NTR(['-x', ofile.name, 'reviews'])
    users = p4_run(cmd)
    for user in users:
        if 'code' in user and user['code'] == 'error':
            remove_file(ofile)
            raise Exception(user['data'])
        _user = user['user']
        if _user.startswith(P4GF_REVIEWS_GF):
            if _user != P4GF_REVIEWS__NON_GF and _user != P4GF_REVIEWS__ALL_GF:
                # reject this submit which conflicts with GF
                is_locked = True
                gf_user = user['user']
                break
    return (is_locked, gf_user, ofile)


def write_lines_to_tempfile(prefix_, lines):
    """Write list of lines to tempfile."""
    file_ = tempfile.NamedTemporaryFile(prefix='p4gf-trigger-' + prefix_, delete=False)
    for line in lines:
        ll = "%s\n" % dequote(line)
        file_.write(encode(ll))
    file_.flush()
    file_.close()
    return file_


def dequote(path):
    """Remove wrapping double quotes."""
    if path.startswith('"'):
        path = path[1:-1]
    return path


def edit_lock_trigger(command, client, client_cwd, paths):
    """Check whether Git Fusion has a lock on these files."""
    # convert the local/depot paths all to depotPath
    cmd = ['-c', client, '-d', client_cwd, 'where']
    cmd = cmd + paths
    depot_files = p4_run(cmd)
    depot_paths = []
    ofile = None
    for file_dict in depot_files:
        if isinstance(file_dict, dict) and 'depotFile' in file_dict:
            depot_paths.append(file_dict['depotFile'])
    (is_locked, gf_user, ofile) = locked_by_review(depot_paths)
    if not is_locked:
        remove_file(ofile)
        return P4PASS
    if command == 'user-lock':
        print(MSG_CANNOT_LOCK.format(file=gf_user))
        remove_file(ofile)
        return P4FAIL
    # p4 edit - there is an overlap with GF  - check for files with +l
    cmd = ['-x', ofile.name, 'fstat', '-F', 'headType=*+l', '-m1']
    cmd = cmd + depot_paths
    try:
        plus_l_files = p4_run(cmd)
    finally:
        remove_file(ofile)
    if plus_l_files:
        print(MSG_CANNOT_EXCL_EDIT.format(file=gf_user))
        return P4FAIL
    else:
        return P4PASS


def remove_edit_options(paths):
    """Return the file list from the argument list by removing the -c, -k, -n, -t options."""
    preview = False
    ffi = 0   # will be the index of first file argument
    while paths[ffi] in ['-c', '-t', '-k', '-n']:
        arg = paths[ffi]
        ffi += 1
        if arg in ['-c', '-t']:
            ffi += 1
        if arg == '-n':
            preview = True
    return (paths[ffi:], preview)


def main():
    """Execute Git Fusion submit triggers."""
    # pylint: disable=unused-variable
    # Unused variable 'user' - script exits at top if user=git-fusion-user
    global P4PORT
    if len(sys.argv) < 7:
        usage()
        sys.exit(1)
    command    = sys.argv[1]
    user       = sys.argv[2]
    server     = sys.argv[3]
    client     = sys.argv[4]
    client_cwd = sys.argv[5]
    paths      = sys.argv[6:]

    exitcode   = P4PASS
    if not P4PORT:
        P4PORT = server
    if command not in ('user-edit', 'user-lock'):
        print(_("Invalid trigger type: {command}").format(command=command))
        exitcode = P4FAIL
    else:
        (paths, is_preview) = remove_edit_options(paths)
        exitcode = edit_lock_trigger(command, client, client_cwd, paths)

    sys.exit(exitcode)


if __name__ == "__main__":
    if sys.hexversion < 0x02060000 or \
            (sys.hexversion > 0x03000000 and sys.hexversion < 0x03020000):
        print(_("Python 2.6+ or Python 3.2+ is required"))
        sys.exit(P4FAIL)
    main()

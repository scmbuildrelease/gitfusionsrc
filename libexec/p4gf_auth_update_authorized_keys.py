#! /usr/bin/env python3.3
"""Update the authorized_keys file based on content in depot.

This script copies all of the keys from //P4GF_DEPOT/users/*/keys/* to the
SSH directory, either in a single authorized_keys file, in the case of
OpenSSH, or in separate files for SSH2.

"""

import base64
import binascii
# workaround pylint bug where it can't find hashlib
import hashlib  # pylint: disable=import-error
import logging
import os
import re
import shutil
import struct
import sys

from P4 import P4Exception

import p4gf_env_config    # pylint: disable=unused-import
import p4gf_const
import p4gf_create_p4
import p4gf_file_action as FA
import p4gf_p4key       as P4Key
from   p4gf_l10n import _, NTR, log_l10n
import p4gf_log
from   p4gf_p4changelist import P4Changelist
import p4gf_util

LOG = p4gf_log.get_auth_keys_logger()
LOG_NOP = LOG.getChild(NTR('nop'))   # A very frequent and noisy logger

# Path in depot where user keys are found.
# Note: this should be under 50k files otherwise performance will degrade
#       max of first * is num of users, second is keys per user
KEYS_PATH = NTR('//{}/users/*/keys/*').format(p4gf_const.P4GF_DEPOT)

# Directory under ~/.ssh2 where public keys are written.
KEYS_DIR = NTR('git-user-keys')

# Config file key for collecting unmanaged lines from configuration file.
NO_FP = NTR('no:fp')
USERNM_RE = re.compile(NTR('--user=([^ ]+)'))
KEYFP_RE = re.compile(NTR('--keyfp=([0-9A-Fa-f:]{47})'))
SSHKEY_RE = re.compile(NTR('(ssh-dss|ssh-rsa|pgp-sign-dss|pgp-sign-rsa) ([0-9A-Za-z+=/]+)'))
KEYPATH_RE = re.compile(NTR('//{}/users/([^/]+)/keys/.+').format(p4gf_const.P4GF_DEPOT))
SSH2_HEADER_LINE = NTR('---- BEGIN SSH2 PUBLIC KEY ----')
SSH2_FOOTER_LINE = NTR('---- END SSH2 PUBLIC KEY ----')

# Actions recored to debug log.
_ADD     = NTR('add')
_REBUILD = NTR('rebuild')
_EDIT    = NTR('edit')
_REMOVE  = NTR('remove')

P4KeyName = None
# If True, producing "SSH2" output, otherwise assumes OpenSSH.
Ssh2 = False
# Path to the ~/.ssh directory, set in main().
SshDirectory = None
# Specifies the path and name of the "authorized keys" file.
SshKeysFile = None
# If True, print informative messages about script's actions.
Verbose = False


#
# For SSH2 support, update Key/Options pairs in ~/.ssh2/authorization file
# and write individual public keys in ~/.ssh2/keys directory. Template for
# Options line look like (definition of terms below):
#
#    Options command="p4gf_auth_server.py --user={user} --keyfp={keyfp} $SSH2_ORIGINAL_COMMAND"
#
# For OpenSSH, the template for each line in the authorized_keys file looks like:
#
#    command="p4gf_auth_server.py --user={user} --keyfp={keyfp} $SSH_ORIGINAL_COMMAND",\
#           no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty {key}
#
# user = the Perforce user account associated with this key, the first
#        asterisk from //P4GF_DEPOT/users/*/keys/*
# keyfp = the key fingerprint of this key associated. Try `ssh-keygen -lf
#         'path-to-key-file'` for one way to generate the fingerprint
# key = the actual public key contents, copied verbatim from the file
#       contents of //P4GF_DEPOT/users/*/keys/*
#
# Optimization: run `p4 key p4gf_auth_update_authorized_keys_last_changenum`
# and then `p4 changes -m1 //P4GF_DEPOT/users/*/keys/*` to see if there's any
# reason to update the SSH configuration files. Update this p4key after writing a
# new SSH configuration file. Use one p4key for all users.
#
# Audit log: any change to the SSH configuration file? Record the user, keyfp,
# and add/edit/delete copied to the file.
#


def _print_debug(msg, nop=False):
    """If in verbose mode, print the text to the console. In all cases,
    the text is sent to the log (no-op log if nop=True) as a debug message.

    l10n: Prefer _() to NTR() for strings passed to _print_debug() because these
          strings become human-visible on
              p4gf_auth_update_authorized_keys.py --verbose
    """
    if Verbose:
        print(msg)
    if nop:
        LOG_NOP.debug(msg)
    else:
        LOG.debug(msg)


def _print_warn(msg, error=False):
    """Print the given message to the error stream, as well as to the log,
    either as "warn" or as "error" (if error=True).
    """
    sys.stderr.write(msg + '\n')
    if error:
        LOG.error(msg)
    else:
        LOG.warning(msg)


def _get_p4key_name():
    """Generate the name of the p4key for keeping track of the last change
    that this server has retrieved for the user keys.
    """
    global P4KeyName
    if not P4KeyName:
        host = p4gf_util.get_server_id()
        P4KeyName = p4gf_const.P4GF_P4KEY_UPDATE_AUTH_KEYS.format(host)
    return P4KeyName


def get_last_change(p4):
    """Fetch the last change number with which we synced the keys to the
    SSH configuration file. Returns a positive number, or zero if no saved
    p4key value.
    """
    v = P4Key.get(p4, _get_p4key_name())
    if v:
        return int(v)
    else:
        return 0


def update_last_change_num(p4, change_num):
    """Update the last change number with which we synced the keys to the
    SSH configuration file. The change_num must be a positive number.
    """
    fmt = _("invalid change number '{change_num}'")
    try:
        v = int(change_num)
        if v < 1:
            raise RuntimeError(fmt.format(change_num=change_num))
        P4Key.set(p4, _get_p4key_name(), v)
    except ValueError:
        raise RuntimeError(fmt.format(change_num=change_num))


def get_keys_latest_change(p4):
    """Retrieve the most recent change made to the user keys in Perforce.

    Returns a postive number, or zero if no changes have been made to the
    keys (i.e. no keys have been added).
    """
    try:
        change = p4.run('changes', '-m', '1', KEYS_PATH)
        if change:
            return int(p4gf_util.first_value_for_key(change, 'change'))
        else:
            return 0
    except P4Exception:
        return 0


def get_keys_changes(p4, low, high):
    """Retrieve the set of changes made to the user keys between the two changes.

    Keyword arguments:
    p4   -- P4 API
    low  -- earliest change for which to retrieve changes
    high -- latest change for which to retrieve changes
    """
    rev_range = '@{},{}'.format(low, high)
    changes = P4Changelist.create_changelist_list_as_dict(p4, KEYS_PATH + rev_range)
    changes = sorted(changes.keys())
    root = '//{}/users'.format(p4gf_const.P4GF_DEPOT)
    changes = [P4Changelist.create_using_describe(p4, c, root) for c in changes]
    return changes


def read_key_type(key):
    """Decode the SSH key and returns the key format (e.g. ssh-dss).

    The input is expected to be a single line of base64 encoded data.
    """
    try:
        # Based on RFC 4253 section 6.6 "Public Key Algorithms"
        keydata = base64.b64decode(key.encode())
        parts = []
        # Decode the entire string to ensure it is valid base64 and not something
        # nefarious (e.g. control characters, shell escapes, etc).
        while keydata:
            # read the length of the data
            dlen = struct.unpack(NTR('>I'), keydata[:4])[0]
            # read in <length> bytes
            data, keydata = keydata[4:dlen + 4], keydata[4 + dlen:]
            parts.append(data)
        # only need the first part, the format specifier
        return parts[0].decode('utf-8')
    except binascii.Error as e:
        _print_warn(_("apparently invalid SSH key '{key}' caused '{error}'")
                    .format(key=key, error=e))
    except UnicodeDecodeError:
        _print_warn(_("error decoding SSH key type for key '{key}'")
                    .format(key=key), error=True)
        return None


def read_key_data(lines):
    """Retrieve the contents of the public key from the given text.

    Returns None if the text does not contain a valid key.
    """
    # Check if an SSH2-formatted public key.
    # RFC 4716 says keys MUST have BEGIN/END marker lines.
    if len(lines) > 2 and lines[0] == SSH2_HEADER_LINE and lines[-1] == SSH2_FOOTER_LINE:
        lines = lines[1:-1]
        # skip over header lines
        continued = False
        while lines:
            if not continued and ':' not in lines[0]:
                # yay, past the header
                break
            # check for a line continuation
            continued = lines[0][-1] == '\\'
            lines.pop(0)
        # lines now contains just the body
        if lines:
            return ''.join(lines)
    else:
        # Otherwise assume this is an OpenSSH formatted key.
        for ln in lines:
            ln = ln.strip()
            if len(ln) > 0 and ln[0] != '#':
                m = SSHKEY_RE.search(ln)
                if m:
                    return m.group(2)
    # Did not contain a valid key.
    return None


class KeyKeeper(object):

    """KeyKeeper is a container for public key data and the associated
    lines from an "authorized keys" file. Each entry consists of a key
    fingerprint, a username, and the data from the authorization file.
    """

    def __init__(self):
        """Create a new instance of KeyKeeper."""
        self.keys = dict()
        self.iterator = None

    def add(self, fp, user, data):
        """Add a new entry to the container."""
        key = fp + '/' + user
        entry = self.keys.get(key, None)
        if entry:
            entry.append(data)
        elif isinstance(data, list):
            self.keys[key] = data
        else:
            self.keys[key] = [data]

    def get(self, fp, user):
        """Retrieve the data for the corresponding fingerprint and user.

        The data will be in list form, with each element corresponding to
        a line from the authorized keys file.
        Returns None if there is no such mapping.
        """
        key = fp + '/' + user
        return self.keys.get(key, None)

    def remove(self, fp, user, data):
        """Remove the entry corresponding to the arguments from the container."""
        key = fp + '/' + user
        entry = self.keys.get(key, None)
        if entry and data in entry:
            entry.remove(data)
            if len(entry) == 0:
                del self.keys[key]

    def clear(self):
        """Remove all mappings from the collection."""
        self.keys.clear()

    def __getitem__(self, key):
        # make pylint happy; we cannot really implement this
        pass

    def __setitem__(self, key, value):
        # make pylint happy; we cannot really implement this
        pass

    def __delitem__(self, key):
        # make pylint happy; we cannot really implement this
        pass

    def __len__(self):
        return len(self.keys)

    def __iter__(self):
        if self.iterator is None:
            self.iterator = iter(self.keys.items())
        return self

    def __next__(self):
        """Return a triple of (fingerprint, username, data).

        Data is that which was provided to the add() method.
        """
        if self.iterator:
            try:
                (k, v) = next(self.iterator)
                # separate the key fingerprint from the username
                i = k.index('/')
                return (k[:i], k[i + 1:], v) if i > 0 else (k, "", v)
            except StopIteration:
                self.iterator = None
                raise
        else:
            raise StopIteration()


def extract_fp_and_user(line):
    """The line is examined to see if it matches that which is produced
    by this script, and if so, extract the public key fingerprint and
    the username associated with that fingerprint, returning them as a
    tuple (fingerprint, username). If the line is not one managed by
    this script, the fingerprint will be 'no:fp' and username will be
    the empty string.
    """
    fp = NO_FP
    user = ""
    m = KEYFP_RE.search(line)
    if m:
        fp = m.group(1)
        m = USERNM_RE.search(line)
        if m:
            user = m.group(1)
    return (fp, user)


def openssh_key_generator(itr):
    """A generator function that produces (fingerprint, username, data)
    tuples suitable for writing to the authorized keys file. Reads lines
    from the given line generator, which is assumed to yield results in
    OpenSSH "authorized_keys" format.
    """
    # OpenSSH authorized_keys file has everything we need on each line.
    for line in itr:
        fp, user = extract_fp_and_user(line)
        yield (fp, user, line)


def ssh2_key_generator(itr):
    """A generator function that produces (fingerprint, username, data)
    tuples suitable for writing to the authorized keys file. Reads lines
    from the given line generator, which is assumed to yield results in
    a format common to several SSH2 implementations.
    """
    # Typical "SSH2" authorization file stores related information on separate
    # lines, need to piece it back together again.
    try:
        # Ugly code, but I want to iterate the file line by line while also
        # having look-ahead behavior since user information may be split
        # across multiple lines. What's more, not all lines are managed by
        # this script, so must allow for arbitrary lines of text.
        while True:
            line = next(itr)
            fp = NO_FP
            user = ''
            if line.lower().startswith(NTR('key ')):
                try:
                    # read the next line, possibly finding "Options"
                    ln = next(itr)
                    if ln and ln.lower().startswith(NTR('options ')):
                        fp, user = extract_fp_and_user(ln)
                    yield (fp, user, line)
                    yield (fp, user, ln)
                except StopIteration:
                    yield (fp, user, line)
            else:
                yield (fp, user, line)
    except StopIteration:
        return


def read_ssh_configuration():
    """Read the authorized keys file, if it exists, into a an instance of
    KeyKeeper. Each entry consists of the key fingerprint, username, and
    the data from the authorization file (e.g. "command" for SSH to run).
    Lines that do not have a fingerprint will be stored under the key 'no:fp'.
    Returns an empty container if the file is missing or empty.
    """
    keys = KeyKeeper()
    if os.path.exists(SshKeysFile):
        def line_chomper(fin):
            """Strip trailing whitespace from lines in file object."""
            for line in fin:
                yield line.rstrip()
        with open(SshKeysFile) as f:
            if Ssh2:
                generator = ssh2_key_generator
            else:
                generator = openssh_key_generator
            for (fp, user, data) in generator(line_chomper(f)):
                keys.add(fp, user, data)
    return keys


def write_ssh_configuration(keys):
    """Write the keys to the authorized keys file.

    Arguments:
        keys - instance of KeyKeeper
    """
    if not os.path.exists(SshDirectory):
        os.makedirs(SshDirectory)
        # some SSH2 implementations will not consider world-writable directories
        os.chmod(SshDirectory, 0o700)
    existed = os.path.exists(SshKeysFile)
    with open(SshKeysFile, 'w') as f:
        for _, _, data in iter(keys):
            for ln in data:
                f.write(ln + '\n')
    if not existed:
        # some SSH2 implementations will not read world-writable files
        os.chmod(SshKeysFile, 0o600)
    # check if file is empty, in which case it can be removed
    if os.path.getsize(SshKeysFile) == 0:
        os.remove(SshKeysFile)


def ssh_key_to_fingerprint(key):
    """Produce the fingerprint of the given SSH key."""
    try:
        key64 = base64.b64decode(key.encode())
        fp_plain = hashlib.md5(key64).hexdigest()       # pylint: disable=no-member
        return ':'.join(a + b for a, b in zip(fp_plain[::2], fp_plain[1::2]))
    except binascii.Error as e:
        _print_warn(_("apparently invalid SSH key '{key}' caused '{error}'")
                    .format(key=key, error=e))
    except TypeError as e:
        _print_warn(_("failed to hash SSH key: '{error}'").format(error=e))
    return None


def extract_key_data(p4, depot_path, rev=None):
    """For the given depot path, extract the user name, SSH key, and the key
    fingerprint generated from that key, returning them as a tuple. Any of
    the returned values may be None if the data is missing (e.g. if path is
    malformed, then user cannot be determined; if key file is malformed, no
    key; likewise for the fingerprint).
    """
    user = None
    m = KEYPATH_RE.search(depot_path)
    if m:
        user = m.group(1)
    fp = None
    if rev:
        depot_path = "{}#{}".format(depot_path, rev)

    # Read all key files as raw bytes, assume they are encoded in UTF-8.
    # Git Fusion does not support other encodings for key file content.
    b = p4gf_util.print_depot_path_raw(p4, depot_path)
    s = b.decode()  # as UTF-8

    lines = s.splitlines()
    lines = [line for line in lines if len(line) > 0]   # Strip blank lines

    key = read_key_data(lines)
    if key:
        fp = ssh_key_to_fingerprint(key)
    return (user, key, fp)


def generate_openssh_key(user, fp, key):
    """Generate an OpenSSH style key entry for the authorized keys file
    using the given arguments, and return the generated line.
    """
    key_type = read_key_type(key)
    openssh_key = key_type + ' ' + key
    ln = NTR('command="p4gf_auth_server.py --user={user} --keyfp={keyfp} $SSH_ORIGINAL_COMMAND",'
             'no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty {key}').format(
        user=user, keyfp=fp, key=openssh_key)
    return ln


def ssh_key_add(p4, depot_path, keys, action=None):
    """Read the contents of the named file and use it to produce a
    fingerprint of the presumed SSH key, formatting the results into
    a line suitable for adding to the SSH configuration file. The line
    is added to the set of keys, keyed by a generated fingerprint.

    Keyword arguments:
    p4         -- P4 API object
    depot_path -- path to keys file
    keys       -- instance of KeyKeeper
    action     -- string describing the action being performed (e.g. 'edit'),
                  defaults to ADD. For debug log only.
    """
    user, key, fp = extract_key_data(p4, depot_path)
    if not user:
        _print_warn(_('Could not extract user name from unrecognized depot path: {depot_path}')
                    .format(depot_path=depot_path))
        return
    if not fp:
        if p4gf_util.depot_file_exists(p4, depot_path):
            _print_warn(_("File '{depot_path}' does not conform to a valid SSH key, ignoring...")
                        .format(depot_path=depot_path))
        return
    if not action:
        action = _ADD
    _print_debug(_('action {}, user {}, key {}, FP {}').format(action, user, key, fp))
    # $SSH[2]_ORIGINAL_COMMAND is there to get the command being invoked
    # by the client via SSH (e.g. git-upload-pack 'foo') -- we need that
    # in order to take the appropriate action, and for auditing purposes.
    if Ssh2:
        fname = os.path.join(KEYS_DIR, user, fp.replace(':', '') + NTR('.pub'))
        fpath = os.path.join(SshDirectory, fname)
        fdir = os.path.dirname(fpath)
        if not os.path.exists(fdir):
            os.makedirs(fdir)
        with open(fpath, 'w') as f:
            f.write(SSH2_HEADER_LINE + '\n')
            keydata = key
            while keydata:
                f.write(keydata[:72] + '\n')
                keydata = keydata[72:]
            f.write(SSH2_FOOTER_LINE + '\n')
        ln = NTR('Key {file}\nOptions command="p4gf_auth_server.py --user={user} --keyfp={keyfp}'
                 ' $SSH2_ORIGINAL_COMMAND"').format(file=fname, user=user, keyfp=fp)
        # No options are included since not all SSH2 implementations support them.
    else:
        ln = generate_openssh_key(user, fp, key)
    keys.add(fp, user, ln)


def ssh_key_remove(p4, depot_path, rev, keys, action):
    """For the named key file at the specified revision, generate an SSH
    fingerprint, look it up in the map of keys, and remove the corresponding
    entry.

    Keyword arguments:
    p4         -- P4 API object
    depot_path -- path to keys file
    rev        -- revision in which file was deleted
    keys       -- instance of KeyKeeper
    action     -- string describing the action being performed; if None then
                  the action is not recorded in the log.
    """
    user, key, fp = extract_key_data(p4, depot_path, rev)
    if not fp:
        return
    if action:
        _print_debug(_('action {action}, user {user}, key {key}, FP {fingerprint}')
                     .format(action=action, user=user, key=key, fingerprint=fp))
    if Ssh2:
        fname = os.path.join(KEYS_DIR, user, fp.replace(':', '') + NTR('.pub'))
        fname = os.path.join(SshDirectory, fname)
        if os.path.exists(fname):
            os.remove(fname)
        keys.remove(fp, user, key)
    else:
        ln = generate_openssh_key(user, fp, key)
        keys.remove(fp, user, ln)


def update_by_changes(p4):
    """Update the authorized keys based on the changes that have occurred
    since the last time the keys were updated.
    """
    latest_change = get_keys_latest_change(p4)
    if latest_change == 0:
        # no user keys, nothing to do
        _print_debug(_('NOP. No changes to {path}').format(path=KEYS_PATH), nop=True)
        return
    last_change = get_last_change(p4)
    if latest_change <= last_change:
        # no new user keys, nothing to do
        _print_debug(_('NOP. Changes to {path}={ch} < p4key {p4key}={ct}').format(
            path=KEYS_PATH,
            ch=latest_change,
            p4key=_get_p4key_name(),
            ct=last_change), nop=True)
        return

    # get the latest changes and update the keys in SSH configuration file
    changes = get_keys_changes(p4, last_change + 1, latest_change)
    keys = read_ssh_configuration()
    for change in changes:
        _print_debug(_('processing change @{change_num}: {change_description}')
                     .format(change_num=change.change, change_description=change.description))
        if int(change.change) > last_change:
            last_change = int(change.change)
        for detail in change.files:
            name = detail.depot_path
            if not KEYPATH_RE.search(name):
                # Skip over files that are not key files.
                continue
            _print_debug(_('file {file_name}, action {action}')
                         .format(file_name=name, action=detail.action))
            if detail.action in [FA.ADD, FA.MOVE_ADD, FA.BRANCH]:
                ssh_key_add(p4, name, keys)
            elif detail.action in [FA.DELETE, FA.MOVE_DELETE]:
                ssh_key_remove(p4, name, int(detail.revision) - 1, keys, _REMOVE)
            elif detail.action == FA.EDIT:
                ssh_key_remove(p4, name, int(detail.revision) - 1, keys, None)
                ssh_key_add(p4, name, keys, _EDIT)
            else:
                _print_warn(_("unhandled change type '{action}'")
                            .format(action=detail.action))
    write_ssh_configuration(keys)
    update_last_change_num(p4, last_change)


def rebuild_all_keys(p4):
    """Rebuild the set of keys by reading all active files from the depot."""
    latest_change = get_keys_latest_change(p4)
    if not latest_change:
        _print_warn(_("No files found in '{path}'")
                    .format(path=KEYS_PATH))
        return
    _print_debug(_('rebuilding all keys through change {change}')
                 .format(change=latest_change))
    keys = read_ssh_configuration()
    # retain only the lines not managed by our script
    custom_keys = keys.get(NO_FP, '')
    keys.clear()
    if custom_keys:
        keys.add(NO_FP, '', custom_keys)
    # now fetch all current keys and add to mapping
    files = p4.run('files', '-e', '{}@{}'.format(KEYS_PATH, latest_change))

    # wipe out ~/.ssh2/git-user-keys directory tree
    keypath = os.path.join(SshDirectory, KEYS_DIR)
    if os.path.exists(keypath):
        shutil.rmtree(keypath)
    if files:
        for fi in files:
            _print_debug(_('adding file {depot_file}')
                         .format(depot_file=fi['depotFile']))
            ssh_key_add(p4, fi['depotFile'], keys, _REBUILD)
    write_ssh_configuration(keys)
    update_last_change_num(p4, latest_change)


def main():
    """Copy the SSH keys from Perforce to the authorized keys file."""
    p4gf_util.has_server_id_or_exit()

    log_l10n()

    # Set up argument parsing.
    parser = p4gf_util.create_arg_parser(_("""Copies SSH public keys from
Perforce depot to current user's directory. This script assumes OpenSSH
is the SSH implementation in use, and as such, writes to 'authorized_keys'
in the ~/.ssh directory. If --ssh2 is used, then writes to 'authorization'
in the ~/.ssh2 directory, writing the SSH2 formatted public keys in the
'keys' directory under ~/.ssh2, using the Perforce user names to avoid
name collisions. If public keys read from the depot are the wrong format
(OpenSSH vs. SSH2), they will be converted when written to disk.
"""))
    parser.add_argument('-r', '--rebuild', action=NTR('store_true'),
                        help=_('rebuild keys file'))
    parser.add_argument('-v', '--verbose', action=NTR('store_true'),
                        help=_('print details of update process'))
    parser.add_argument('-2', '--ssh2', action=NTR('store_true'),
                        help=_("produce 'SSH2' output"))
    parser.add_argument('-f', '--file', help=_('path to authorized keys file'))
    args = parser.parse_args()

    # Since this script is called often (by cron), try to reduce the lines
    # that appear in the log by raising the log level for the p4gf_create_p4
    # module.
    logging.getLogger('p4gf_create_p4').setLevel('WARN')
    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4_temp_client()
        if not p4:
            return 2
        # Sanity check the connection (e.g. user logged in?) before proceeding.
        try:
            p4.fetch_client()
        except P4Exception as e:
            _print_warn(_('P4 exception occurred: {error}').format(error=e), error=True)
            sys.exit(1)

        # Update global settings based on command line arguments.
        global Verbose
        Verbose = args.verbose
        global Ssh2
        Ssh2 = args.ssh2
        global SshKeysFile
        SshKeysFile = args.file
        if not SshKeysFile:
            SshKeysFile = NTR('~/.ssh2/authorization') if Ssh2 else NTR('~/.ssh/authorized_keys')
        if SshKeysFile[0] == '~':
            SshKeysFile = os.path.expanduser(SshKeysFile)
        global SshDirectory
        SshDirectory = os.path.dirname(SshKeysFile)

        # Update the keys file based either on latest changes or existing files.
        try:
            if args.rebuild:
                rebuild_all_keys(p4)
            else:
                update_by_changes(p4)
        except P4Exception as e:
            _print_warn(_('P4 exception occurred: {error}').format(error=e), error=True)

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

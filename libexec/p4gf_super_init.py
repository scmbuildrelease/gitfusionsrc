#! /usr/bin/env python3.3
"""
Perform Git Fusion initialization that requires "super" permissions.

* Create group git-fusion-group
* Create user  git-fusion-user
* Create depot P4GF_DEPOT
* Grant admin permission to git-fusion-user
* Configure dm.protects.allow.admin=1

Must be run with current P4USER set to a super user.
"""

import configparser
import getpass
import io
import logging
import os
import re
from   subprocess import Popen, PIPE
import sys
import uuid

from P4 import P4Exception
import p4gf_const
import p4gf_create_p4
import p4gf_p4key     as     P4Key
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_log
import p4gf_util
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_p4spec
from   p4gf_verbosity import Verbosity
import p4gf_version_3
from p4gf_missing_config_path import MissingConfigPath

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_super_init")

P4D_VERSION_2014_1 = 2014.1
P4_TRIGGER_NAMES = {'change-commit', 'change-content',
            'change-failed', 'change-commit-p4gf-config', 'change-content-p4gf-config',
            'pre-rmt-Push', 'post-rmt-Push', 'pre-user-fetch', 'post-user-fetch'}
P4_TRIGGER_FILE = 'p4gf_submit_trigger.py'
P4PORT = None
P4USER = None
P4CLIENT = None
p4     = None
P4_PASSWD = None
PROMPT_FOR_PASSWD = True
OVERRULE_SERVERID_CONFLICT = False
CREATE_UNKNOWN_GIT = False

ID_FROM_ARGV    = None
SHOW_IDS        = False

KEY_PERM_MAX    = NTR('permMax')
KEY_PROTECTIONS = NTR('Protections')
KEY_VALUE       = NTR('Value')
KEY_TRIGGERS    = NTR('Triggers')

LOGIN_NO_PASSWD    = 0
LOGIN_NEEDS_TICKET = 1
LOGIN_HAS_TICKET   = 2

CONFIGURABLE_ALLOW_ADMIN = 'dm.protects.allow.admin'
# server proxy congiguration
CONFIGURABLE_PROXY_PROTECTS = 'dm.proxy.protects'


Create_P4GF_CONFIG = False


def check_and_create_default_p4gf_env_config():
    """If p4gf_env_config threw the MissingConfigPath exception,
    because P4GF_ENV names a non-existing filepath
    then save the required (two) default items
    into the user configured P4GF_ENV environment config file.
    """
    if not Create_P4GF_CONFIG:
        LOG.debug('not creating configuration file')
        return
    LOG.debug('creating missing configuration file')
    Verbosity.report(
        Verbosity.INFO, _("Git Fusion environment var P4GF_ENV = {path} names a non-existing file.")
        .format(path=p4gf_const.P4GF_ENV))
    Verbosity.report(
        Verbosity.INFO, _("Creating {path} with the default required items.")
        .format(path=p4gf_const.P4GF_ENV))
    Verbosity.report(
        Verbosity.INFO, _("Review the file's comments and edit as needed."))
    Verbosity.report(
        Verbosity.INFO, _("You may unset P4GF_ENV to use no config file.")
        .format(p4gf_const.P4GF_ENV))
    config = configparser.ConfigParser(interpolation  = None,
                                       allow_no_value = True)
    config.optionxform = str
    config.add_section(p4gf_const.SECTION_ENVIRONMENT)
    config.set(p4gf_const.SECTION_ENVIRONMENT, p4gf_const.P4GF_HOME_NAME, p4gf_const.P4GF_HOME)
    Verbosity.report(
        Verbosity.INFO, _("Setting {home_name} = {home} in {env}.")
        .format(home_name=p4gf_const.P4GF_HOME_NAME,
                home=p4gf_const.P4GF_HOME,
                env=p4gf_const.P4GF_ENV))
    config.set(p4gf_const.SECTION_ENVIRONMENT, NTR('P4PORT'), P4PORT)
    Verbosity.report(
        Verbosity.INFO, _("Setting {p4port} = {p4port_value} in {env}.")
        .format(p4port=NTR('P4PORT'),
                p4port_value=P4PORT,
                env=p4gf_const.P4GF_ENV))
    header = p4gf_util.read_bin_file(NTR('p4gf_env_config.txt'))
    if header is False:
        sys.stderr.write(_('no p4gf_env_config.txt found\n'))
        header = _('# Missing p4gf_env_config.txt file!')
    out = io.StringIO()
    out.write(header)
    config.write(out)
    file_content = out.getvalue()
    out.close()
    p4gf_util.ensure_dir(p4gf_util.parent_dir(p4gf_const.P4GF_ENV))
    with open(p4gf_const.P4GF_ENV, 'w') as f:
        f.write(file_content)
    LOG.debug('created configuration file %s', p4gf_const.P4GF_ENV)

# p4gf_env_config will apply the config set by P4GF_ENV,
# but throw an exception if the P4GF_ENV is defined but the path does not exist.
# super_init will catch this exception and write the defaults to the P4GF_ENV path.
#
try:
    import p4gf_env_config  # pylint: disable=unused-import
except MissingConfigPath:
    Create_P4GF_CONFIG = True
except Exception as exc:    # pylint: disable=broad-except
    Verbosity.report(Verbosity.ERROR, str(exc))
    sys.exit(2)


def get_auth_method():
    """Return the configured auth method perforce or ldap."""
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('configure', 'show', 'auth.default.method')
    if p4.errors:
        Verbosity.report(Verbosity.ERROR,
                _("Unable to run 'p4 configure show auth.default.method'."))
        for e in p4.errors:
            Verbosity.report(Verbosity.ERROR, e)
        sys.exit(2)

    auth_method = None
    if len(r):
        auth_method = p4gf_util.first_value_for_key(r, 'Value')
    LOG.debug('auth.default.method = %s', auth_method)
    return auth_method


def get_passwd(msg):
    """Prompt for and confirm password."""
    print("\n")
    if msg:
        print(msg)
    pw1 = NTR('pw1')
    pw2 = NTR('pw2')
    while pw1 != pw2:
        print(_("To cancel: CTL-C + ENTER."))
        pw1 = getpass.getpass(_('Password: '))
        if '\x03' in pw1:
            raise KeyboardInterrupt()
        pw2 = getpass.getpass(_('Retype password: '))
        if '\x03' in pw2:
            raise KeyboardInterrupt()
        if pw1 != pw2:
            print(_("Passwords do not match. Try again."))
        if p4_security_level() > 0 and not strong_passwd(pw1):
            print(_("This Perforce server requires a strong password: >= 8 characters and"))
            print(_("with mixed case or contain non alphabetic characters."))
            pw2 = pw1 + "...."   # force mismatch to continue loop
    return pw1


def set_passwd(user, passwd):
    """Set the P4 passwd for user. Assumes super user priviledge."""
    with p4.at_exception_level(p4.RAISE_NONE):
        p4.input = passwd
        r = p4.run_passwd(user)
        Verbosity.report(Verbosity.DEBUG, NTR('p4 passwd\n{}').format(r))
    if p4.errors:
        Verbosity.report(
            Verbosity.ERROR, _("Unable to run 'p4 passwd -P xxx {user}'.").format(user=user))
        for e in p4.errors:
            Verbosity.report(Verbosity.ERROR, e)
        Verbosity.report(
            Verbosity.ERROR,
            _("You must set passwords for Git Fusion users and re-run configure-git-fusion.sh"))
        Verbosity.report(
            Verbosity.ERROR,
            _("Git Fusion users are: {gf_user}  {reviews_user}  {non_gf}  {all_gf}")
            .format(gf_user=p4gf_const.P4GF_USER,
                    reviews_user=p4gf_util.gf_reviews_user_name(),
                    non_gf=p4gf_const.P4GF_REVIEWS__NON_GF,
                    all_gf=p4gf_const.P4GF_REVIEWS__ALL_GF))
        sys.exit(2)


def fetch_protect():
    """Return protect table as a list of protect lines."""
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('protect', '-o')
        Verbosity.report(Verbosity.DEBUG, NTR('p4 protect:\n{result}').format(result=r))
    if p4.errors:
        Verbosity.report(Verbosity.ERROR, _("Unable to run 'p4 protect -o'."))
        for e in p4.errors:
            Verbosity.report(Verbosity.ERROR, e)
        sys.exit(2)

    protections = p4gf_util.first_value_for_key(r, KEY_PROTECTIONS)
    LOG.debug('retrieved protections: %s', protections)
    return protections


def fetch_triggers():
    """Return trigger table as a list of lines."""
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('triggers', '-o')
    if p4.errors:
        Verbosity.report(Verbosity.ERROR, _("Unable to run 'p4 triggers -o'."))
        for e in p4.errors:
            Verbosity.report(Verbosity.ERROR, e)
        sys.exit(2)

    triggers = p4gf_util.first_value_for_key(r, KEY_TRIGGERS)
    LOG.debug('retrieved triggers: %s', triggers)
    return triggers


def show_all_server_ids():
    """List current Git Fusion server ids."""
    server_ids = P4Key.get_all(p4, p4gf_const.P4GF_P4KEY_SERVER_ID + '*')
    ids = []
    this_server = p4gf_util.read_server_id_from_file()
    for key, value in server_ids.items():
        id_ = key.replace(p4gf_const.P4GF_P4KEY_SERVER_ID, '')
        if this_server == id_:
            id_ = id_ + " *"
        ids.append((id_, value))

    if ids:
        Verbosity.report(
            Verbosity.INFO,
            _("Git Fusion server IDs:  {server_id: <30}   {hostname: <30}"
              "  (* marks this instance)")
            .format(server_id="server-id", hostname="hostname"))
        for sid in ids:
            Verbosity.report(
                Verbosity.INFO,
                _("                        {server_id: <30}   {hostname: <30}")
                .format(server_id=sid[0], hostname=sid[1]))


def server_id_p4key_exists(server_id):
    """Return True if server-id p4key exists."""
    if server_id:
        return P4Key.is_set(p4, p4gf_const.P4GF_P4KEY_SERVER_ID + server_id)
    return False


def set_server_id_p4key(server_id):
    """Set the server-id p4key value to the hostname to identify GF hosts."""
    if server_id:
        P4Key.set(p4, p4gf_const.P4GF_P4KEY_SERVER_ID + server_id,
                  p4gf_util.get_hostname())


def unset_server_id_p4key(server_id):
    """Delete the server-id p4key."""
    if server_id:
        P4Key.delete(p4, p4gf_const.P4GF_P4KEY_SERVER_ID + server_id)


def check_for_localhost(id_from_file, server_id):
    """Validate that server_id is not being set to 'localhost'."""
    needs_exit = False
    if id_from_file == 'localhost' and server_id == 'localhost':
        Verbosity.report(
            Verbosity.INFO,
            _("Your server_id file '{path}' is set to 'localhost'."
              " Use the --id argument to choose another id.").
            format(path=p4gf_util.server_id_file_path()))
        needs_exit = True
    if server_id == 'localhost' and not id_from_file:
        if not ID_FROM_ARGV:
            Verbosity.report(
                Verbosity.INFO,
                _("Git Fusion is attempting to use the default hostname "
                  "'localhost' as the server_id which is not permitted.\n"
                  "Use the --id argument to choose another id."))
            needs_exit = True
        else:
            Verbosity.report(
                Verbosity.INFO,
                _("server_id 'localhost' is not permitted. "
                  " Use the --id argument to choose another id."))
        needs_exit = True
    if server_id == 'localhost' and id_from_file:
        Verbosity.report(
            Verbosity.INFO,
            _("Your server_id file '{path}' is already set to '{server_id}'."
              "\nYou may not override it with 'localhost'."
              " Use the --id argument to choose another id.").
            format(path=p4gf_util.server_id_file_path(), server_id=id_from_file))
        needs_exit = True
    if needs_exit:
        sys.exit(1)


def ensure_server_id():
    """Write this machine's permanent server-id assignment to P4GF_HOME/server-id.

    NOP if we already have a server-id stored in that file.
    We'll just keep using it.
    """
    # pylint: disable=too-many-branches
    if ID_FROM_ARGV:
        if re.search(r'[/@#*%]|\.\.\.', ID_FROM_ARGV):
            msg = _('Special characters (*, #, %) not allowed in Git Fusion server ID {server_id}.')
            Verbosity.report(Verbosity.ERROR, msg.format(server_id=ID_FROM_ARGV))
            sys.exit(1)
    id_from_file = p4gf_util.read_server_id_from_file()
    if id_from_file and ID_FROM_ARGV and id_from_file != ID_FROM_ARGV:
        if not OVERRULE_SERVERID_CONFLICT:
            # msg = _("Git Fusion server ID already set to '{0}', cannot initialize again.")
            Verbosity.report(
                Verbosity.ERROR,
                _("Git Fusion server ID already set to '{server_id}'. " +
                  "To reinitialize Git Fusion with '{id_from_argv}' use the --force option.")
                .format(server_id=id_from_file, id_from_argv=ID_FROM_ARGV))
            sys.exit(1)

    server_id = ID_FROM_ARGV if ID_FROM_ARGV else p4gf_util.get_hostname()
    # when re-running super_init, do not replace the server-id file when
    # the server_id file exists an no --id parameter is present
    # assume in the case that the existing file is correct
    if id_from_file and not ID_FROM_ARGV:
        server_id = id_from_file

    check_for_localhost(id_from_file, server_id)
    do_reset = True
    if server_id_p4key_exists(server_id):
        do_reset = False
        if id_from_file == server_id:
            Verbosity.report(
                Verbosity.INFO,
                _("Git Fusion server ID already set to '{server_id}'.")
                .format(server_id=id_from_file))
        else:
            if not OVERRULE_SERVERID_CONFLICT:
                Verbosity.report(
                    Verbosity.INFO,
                    _("Git Fusion server ID is already assigned: "
                      "'{server_id}' set on host on '{exists}'.\n"
                      "Retry with a different --id server_id.")
                    .format(server_id=server_id,
                            exists=server_id_p4key_exists(server_id)))
                Verbosity.report(
                    Verbosity.INFO,
                    _("If you are certain no other Git Fusion instance is using this server ID,"
                      "\nyou may overrule this conflict and set the local server-id file to"
                      "\n'{server_id}' with:"
                      "\n    configure-git-fusion.sh --id")
                    .format(server_id=server_id))
                if id_from_file:
                    Verbosity.report(
                        Verbosity.INFO, _("Git Fusion server ID already set to '{server_id}'.")
                        .format(server_id=id_from_file))
                else:
                    Verbosity.report(
                        Verbosity.INFO,
                        _("This Git Fusion's server ID is unset. Stopping."))
                    show_all_server_ids()
                    sys.exit(1)

            else:
                do_reset = True

    if do_reset:
        if id_from_file and id_from_file != server_id:  # delete the previous p4key
            if server_id_p4key_exists(id_from_file):
                unset_server_id_p4key(id_from_file)
        set_server_id_p4key(server_id)
        p4gf_util.write_server_id_to_file(server_id)
        Verbosity.report(
            Verbosity.INFO,
            _("Git Fusion server ID set to '{server_id}' in file '{path}'")
            .format(server_id=server_id, path=p4gf_util.server_id_file_path()))
    show_all_server_ids()


def set_passwd_and_login(created, user, passwd=None):
    """If creating the user, conditionally prompt for and set the passwd."""
    global P4_PASSWD, PROMPT_FOR_PASSWD
    if created:
        if PROMPT_FOR_PASSWD:
            prompt_msg = _("Set one password for Perforce users 'git-fusion-user'"
                           "\nand 'git-fusion-reviews-*'.")
            # When creating additional Git Fusion instance only the new reviews will be created.
            # Catch this case and avoid a misleading prompt.
            if user == p4gf_util.gf_reviews_user_name():
                prompt_msg = _("Enter a new password for Perforce user '{user}'.").format(
                    user=user)
            try:
                P4_PASSWD = get_passwd(prompt_msg)
            except KeyboardInterrupt:
                Verbosity.report(
                    Verbosity.INFO,
                    _("\n Stopping. Passwords not set."))
                sys.exit(1)
            # If we prompted, do so once and use for all the service users,
            # even if the user enters no password at all.
            PROMPT_FOR_PASSWD = False
            if not P4_PASSWD:
                Verbosity.report(
                    Verbosity.INFO,
                    _("Empty password. Not setting passwords."))

        # passwd may be suppressed with --nopasswd option, which also suppresses the prompt.
        # We always set the passwd for unknown_git - from random passwd=xxx parameter
        # The global P4_PASSWd for the GF users may not be set (--nopasswd)
        if P4_PASSWD or passwd:
            if passwd:
                set_passwd(user, passwd)
            else:
                set_passwd(user, P4_PASSWD)
            Verbosity.report(
                Verbosity.INFO, _("Password set for Perforce user '{user}'.")
                .format(user=user))
            r = p4.run_login(user)
            msg = re.sub(user, "'" + user + "'", r[0])
            Verbosity.report(Verbosity.INFO, msg)
    else:
        if user in (p4gf_const.P4GF_USER, p4gf_const.P4GF_REVIEWS__NON_GF,
                    p4gf_const.P4GF_REVIEWS__ALL_GF, p4gf_util.gf_reviews_user_name()):
            if p4_has_login(user) == LOGIN_NEEDS_TICKET:
                r = p4.run_login(user)
                msg = re.sub(user, "'" + user + "'", r[0])
                Verbosity.report(Verbosity.INFO, msg)


def has_auth_check_trigger():
    """Return True/False - is auth_check trigger/service_check installed."""
    auth_check = False
    service_check = False
    triggers = fetch_triggers()
    if triggers:
        for trig in triggers:
            if 'auth-check' in trig:
                auth_check = True
            if 'service-check' in trig:
                service_check = True
    return (auth_check, service_check)


def ensure_users():
    """Create Perforce user git-fusion-user, and reviews users if not already extant."""
    default_auth_method = get_auth_method()
    has_auth_check, has_service_check = has_auth_check_trigger()
    set_perforce_auth_method = default_auth_method and default_auth_method != 'perforce'

    # These ensure_user-* methods always set AuthMethod to 'perforce'
    created = p4gf_p4spec.ensure_user_gf(p4, set_perforce_auth_method)
    log_user_info(created, has_auth_check, p4gf_const.P4GF_USER, default_auth_method)
    if not has_auth_check:
        set_passwd_and_login(created, p4gf_const.P4GF_USER)

    created = p4gf_p4spec.ensure_user_reviews(p4, set_perforce_auth_method)
    log_user_info(created, has_service_check, p4gf_util.gf_reviews_user_name(), default_auth_method)
    if not has_service_check:
        set_passwd_and_login(created, p4gf_util.gf_reviews_user_name())

    created = p4gf_p4spec.ensure_user_reviews_non_gf(p4, set_perforce_auth_method)
    log_user_info(created, has_service_check, p4gf_const.P4GF_REVIEWS__NON_GF, default_auth_method)
    if not has_service_check:
        set_passwd_and_login(created, p4gf_const.P4GF_REVIEWS__NON_GF)

    created = p4gf_p4spec.ensure_user_reviews_all_gf(p4, set_perforce_auth_method)
    log_user_info(created, has_service_check, p4gf_const.P4GF_REVIEWS__ALL_GF, default_auth_method)
    if not has_service_check:
        set_passwd_and_login(created, p4gf_const.P4GF_REVIEWS__ALL_GF)

    if CREATE_UNKNOWN_GIT:
        created = p4gf_p4spec.ensure_unknown_git(p4, set_perforce_auth_method)
        log_user_info(created, has_auth_check, p4gf_const.P4GF_UNKNOWN_USER, default_auth_method)
        if not has_auth_check:
            passwd = str(uuid.uuid4().hex).upper()[-10:] + '$#z'
            set_passwd_and_login(created, p4gf_const.P4GF_UNKNOWN_USER, passwd=passwd)

    # Report whether 'unknown_git' exists.
    e = p4gf_util.service_user_exists(p4, p4gf_const.P4GF_UNKNOWN_USER)
    _exists = ( _("Git Fusion user '{user}' does not exist.")
              , _("Git Fusion user '{user}' exists."))
    Verbosity.report(Verbosity.INFO, _exists[e].format(user=p4gf_const.P4GF_UNKNOWN_USER))


def strong_passwd(password):
    """Test whether password passes P4 strong requirements."""
    if len(password) < 8:
        return False
    reqs = 0

    # Perforce requires 2 of these 3 requirements
    if re.search(r'[A-Z]', password):
        reqs += 1
    if re.search(r'[a-z]', password):
        reqs += 1
    if re.search(r'[^a-zA-Z]', password):
        reqs += 1

    return reqs >= 2


def p4_security_level():
    """Return p4d security level as int."""
    level = P4Key.get_counter(p4, 'security')
    try:
        level = int(level)
    except ValueError:
        level = 0
    LOG.debug('retrieved security level: %s', level)
    return level


def p4_has_login(user):
    """Return login state for user."""
    login_p4 = p4gf_create_p4.create_p4(port=P4PORT, user=user, client=P4CLIENT)
    if not login_p4:
        raise RuntimeError(_("Failed to connect to P4."))
    try:
        login = login_p4.run('login', '-s')[0]
        if 'TicketExpiration' in login:
            login = LOGIN_HAS_TICKET
        else:
            login = LOGIN_NO_PASSWD
    except P4Exception as e:
        if 'Perforce password (P4PASSWD) invalid or unset.' in str(e) or \
           'Your session was logged out, please login again.' in str(e) or \
           'Your session has expired, please login again.' in str(e):
            login = LOGIN_NEEDS_TICKET
        else:
            emsg = _("Failed to check login status for user={0}\n{1}").format(user, str(e))
            raise RuntimeError(emsg)

    p4gf_create_p4.p4_disconnect(login_p4)
    return login


def log_user_info(created, has_auth_check, user, default_auth_method):
    """Create Perforce user git-fusion-user if not already exists."""
    if created:
        Verbosity.report(Verbosity.INFO, _("User '{}' created.").format(user))
        if default_auth_method and default_auth_method != 'perforce':
            Verbosity.report(Verbosity.INFO,
                _("User '{user}' setting AuthMethod to 'perforce'. "
                  "Default AuthMethod is '{method}'")
                .format(user=user, method=default_auth_method))
    else:
        Verbosity.report(Verbosity.INFO, _("User '{user}' already exists. Not creating.")
                         .format(user=user))
    if has_auth_check:
        Verbosity.report(Verbosity.WARN,
            _("  !!  The Perforce server has an auth_check/service_check trigger this user type.")
           .format(user))
        Verbosity.report(Verbosity.WARN,
            _("  !!  You must ensure that the user '{0}' has a password and is logged in")
           .format(user))
        Verbosity.report(Verbosity.WARN,
            _("  !!  before this Git Fusion instance is fully configured."))
    return created


def ensure_group():
    """Create Perforce group git-fusion-group if not already exists."""
    users = []
    # Keep the order of the users in the same order that P4 insists on
    # (if the order doesn't match then the group is updated repeatedly).
    users.append(p4gf_const.P4GF_REVIEWS__ALL_GF)
    users.append(p4gf_const.P4GF_REVIEWS__NON_GF)
    users.append(p4gf_util.gf_reviews_user_name())
    users.append(p4gf_const.P4GF_USER)
    args = [p4, NTR("group")]
    spec = {'Timeout': NTR('unlimited'), 'Users': users}
    kwargs = {'spec_id': p4gf_const.P4GF_GROUP, 'values': spec}
    if not p4gf_p4spec.ensure_spec(*args, **kwargs):
        # We change the list of users in the group from time to time,
        # so ensure the membership is up to date.
        users = p4gf_util.first_dict(p4.run('group', '-o', p4gf_const.P4GF_GROUP))['Users']
        # Add the gf_reviews_user_name if not already in the group.
        # This avoids removing already existing reviews users from multiple GF instances.
        if p4gf_util.gf_reviews_user_name() not in users:
            users.append(p4gf_util.gf_reviews_user_name())
            spec = {'Timeout': NTR('unlimited'), 'Users': users}
            kwargs = {'spec_id': p4gf_const.P4GF_GROUP, 'values': spec}
            if p4gf_p4spec.ensure_spec_values(*args, **kwargs):
                Verbosity.report(Verbosity.INFO, _("Group '{group}' updated.")
                                 .format(group=p4gf_const.P4GF_GROUP))
            else:
                Verbosity.report(Verbosity.INFO, _("Group '{group}' already up to date.")
                                 .format(group=p4gf_const.P4GF_GROUP))
        else:
            Verbosity.report(Verbosity.INFO, _("Group '{group}' already up to date.")
                             .format(group=p4gf_const.P4GF_GROUP))
        return False
    else:
        Verbosity.report(Verbosity.INFO, _("Group '{group}' created.")
                         .format(group=p4gf_const.P4GF_GROUP))
    return True


def ensure_depot():
    """Create depot P4GF_DEPOT if not already exists."""
    # We use P4GF_P4KEY_P4GF_DEPOT to send the depot name
    # to p4gf_submit_trigger.py. Ensure that P4GF_P4KEY_P4GF_DEPOT matches
    # requested depot name from P4GF_ENV.
    # pylint: disable=line-too-long
    p4gf_depot_key = P4Key.get(p4, p4gf_const.P4GF_P4KEY_P4GF_DEPOT)
    if p4gf_depot_key != '0' and p4gf_depot_key != p4gf_const.P4GF_DEPOT:
        Verbosity.report(
            Verbosity.ERROR,
            _("\n**ERROR: There is a configuration conflict for this Git Fusion instance of the P4GF_DEPOT.\n"
              "  P4GF_DEPOT is configured to a value different than that recorded in the p4d server.\n"
              "  You must correct this as described here.\n"
              "  This key must either be:\n"
              "   1) unset, in which case this initialization will set it to the correct value, or\n"
              "   2) match the value set in the P4GF_ENV configuration file, or\n"
              "   3) set to the default '.git-fusion' if not set to a different value in the P4GF_ENV configuration file.\n"
              "\n"
              "  The p4d server p4key '{key}' is set to '{value}'.\n")
            .format(key=p4gf_const.P4GF_P4KEY_P4GF_DEPOT, value=p4gf_depot_key))
        depot_configured = (p4gf_const.P4GF_ENV and p4gf_env_config.P4GF_ENV_CONFIG_DICT and
                            p4gf_const.P4GF_DEPOT_NAME in p4gf_env_config.P4GF_ENV_CONFIG_DICT)
        if depot_configured:
            msg = _("  This Git Fusion instance is configured with Git Fusion depot = '{depot}'"
                    "\n    from P4GF_ENV file '{env}' containing these settings:\n") \
                .format(depot=p4gf_const.P4GF_DEPOT, env=p4gf_const.P4GF_ENV)
            for k, v in p4gf_env_config.P4GF_ENV_CONFIG_DICT.items():
                msg = msg + "        {0} = {1}\n".format(k, v)
        else:
            msg = _("  This Git Fusion instance is configured for the default P4GF_DEPOT : '{depot}'.\n")\
                .format(depot=p4gf_const.P4GF_DEPOT)
        Verbosity.report(Verbosity.ERROR, msg)
        Verbosity.report(
            Verbosity.ERROR,
            _("  To correct this mismatch , determine the required P4GF_DEPOT value.\n"
              "    1) If the value of p4key '{key}'='{value}' is different than the required value, remove the p4key.\n"
              "           Make certain the current p4key value was not correctly set by another Git Fusion instance.\n"
              "    2) If the required value is the default 'git-fusion', and there is a P4GF_DEPOT setting in the P4GF_ENV file, remove it.\n"
              "    2) If the required value is not the default 'git-fusion', set P4GF_DEPOT to the required value in the P4GF_ENV file.\n"
              "    3) Re-run this configure-git-fusion.sh script.")
            .format(key=p4gf_const.P4GF_P4KEY_P4GF_DEPOT, value=p4gf_depot_key))
        sys.exit(1)

    try:
        created = p4gf_p4spec.ensure_depot_gf(p4)
        if created:
            Verbosity.report(Verbosity.INFO, _("Depot '{depot}' created.")
                             .format(depot=p4gf_const.P4GF_DEPOT))
        else:
            Verbosity.report(
                Verbosity.INFO, _("Depot '{depot}' already exists. Not creating.")
                .format(depot=p4gf_const.P4GF_DEPOT))
    except Exception as depote:
        Verbosity.report(Verbosity.INFO, _("Error creating Depot '{depot}'.")
                         .format(depot=p4gf_const.P4GF_DEPOT))
        raise depote

    # Set the P4GF_DEPOT p4key to send the depot name to p4gf_submit_trigger.py.
    # This will either be the default or the value set from P4GF_ENV config file
    P4Key.set(p4, p4gf_const.P4GF_P4KEY_P4GF_DEPOT, p4gf_const.P4GF_DEPOT)
    return created


def ensure_protect(protect_lines):
    """Require that 'p4 protect' table includes grant of admin to git-fusion-user.

    And review to git-fusion-reviews-*
    """
    with p4.at_exception_level(p4.RAISE_NONE):
        r = p4.run('protects', '-m', '-u', p4gf_const.P4GF_USER)

    if p4gf_p4msg.find_msgid(p4, p4gf_p4msgid.MsgDm_ProtectsEmpty):
        Verbosity.report(Verbosity.INFO, _("Protect table empty. Setting...."))

    l = None
    gfuser_perms_set = False
    reviews_users_perms_set = False
    Verbosity.report(Verbosity.DEBUG, NTR('p4 protects -mu git-fusion-user\n{}').format(r))
    perm = p4gf_util.first_value_for_key(r, KEY_PERM_MAX)
    if perm and perm in ['admin', 'super']:
        Verbosity.report(Verbosity.INFO,
                         _("Protect table already grants 'admin' to user '{user}'. Not changing")
                         .format(user=p4gf_const.P4GF_USER))
    else:
        l = protect_lines
        l.append('admin user {user} * //...'.format(user=p4gf_const.P4GF_USER))
        gfuser_perms_set = True

    review_perm = 'review user git-fusion-reviews-* * //...'
    review_perm_user_mask = 'review user git-fusion-reviews-*'
    review_perm_exists = False
    for perm in protect_lines:
        if perm.startswith(review_perm_user_mask):
                        # Do not insert a newline into this line even
                        # though it is long. Makes it too hard to test
                        # in p4gf_super_init.t
            Verbosity.report(
                Verbosity.INFO,
                _("Protect table already grants 'review' to users 'git-fusion-reviews-*'."
                  " Not changing"))
            review_perm_exists = True
            break

    if not review_perm_exists:
        if not l:
            l = protect_lines
        l.append(review_perm)
        reviews_users_perms_set = True

    if l:
        p4gf_p4spec.set_spec(p4, 'protect', values={KEY_PROTECTIONS: l})
        if gfuser_perms_set:
            Verbosity.report(
                Verbosity.INFO,
                _("Protect table modified. User '{user}' granted admin permission.")
                .format(user=p4gf_const.P4GF_USER))
        if reviews_users_perms_set:
            Verbosity.report(
                Verbosity.INFO,
                _("Protect table modified. git-fusion-reviews-* granted reviews permission."))


def ensure_protects_configurable():
    """Grant 'p4 protects -u' permission to admin users."""
    v = p4gf_util.first_value_for_key(
        p4.run('configure', 'show', CONFIGURABLE_ALLOW_ADMIN),
        KEY_VALUE)
    if v == '1':
        Verbosity.report(
            Verbosity.INFO, _("Configurable '{configurable}' already set to 1. Not setting.")
            .format(configurable=CONFIGURABLE_ALLOW_ADMIN))
        return False

    p4.run('configure', 'set', '{}=1'.format(CONFIGURABLE_ALLOW_ADMIN))
    Verbosity.report(Verbosity.INFO, _("Configurable '{configurable}' set to 1.")
                     .format(configurable=CONFIGURABLE_ALLOW_ADMIN))
    return True


def set_proxy_protects_key():
    """Get server's dm.proxy_protects and store as a key for Git Fusion."""
    v = p4gf_util.first_value_for_key(
        p4.run('configure', 'show', CONFIGURABLE_PROXY_PROTECTS),
        KEY_VALUE)
    v = 'false' if v == '0' else 'true'
    P4Key.set(p4, p4gf_const.P4GF_P4KEY_PROXY_PROTECTS, v)
    Verbosity.report(
        Verbosity.INFO, _("Configurable '{configurable}' is set to {value}.")
        .format(configurable=CONFIGURABLE_PROXY_PROTECTS, value=v))


def initialize_all_gf_reviews():
    """Execute p4gf_submit_trigger.py as super user to reset the
       git-fusion-reviews--all-gf reviews used by the submit trigger"""
    trigger_path = "{0}/{1}".format(os.path.dirname(os.path.abspath(sys.argv[0])),
                                    P4_TRIGGER_FILE)
    if not os.path.exists(trigger_path):
        print(_("Unable to find and execute '{trigger}'").format(trigger=trigger_path))
        return

    python = 'python3.3'
    try:
        cmd = [python, trigger_path, '--rebuild-all-gf-reviews', P4PORT, P4USER, "nocfg"]
        p = Popen(cmd, stdout=PIPE, stderr=PIPE)
        fd = p.communicate()
        if p.returncode:
            print(_("Error '{ec}' returned from command '{cmd}'")
                  .format(cmd=' '.join(cmd),
                          ec=p.returncode))
            out = fd[0] if isinstance(fd[0], str) else fd[0].decode()
            out = out.strip()
            err = fd[1] if isinstance(fd[1], str) else fd[1].decode()
            err = err.strip()
            if len(out):
                print(out)
            if len(err):
                print(err)
            if 'Perforce password (P4PASSWD) invalid or unset' in out+err:
                print(_("Ensure 'git-fusion-user' has a password and is logged in."))
                print(_("Then run this script again."))
            sys.exit(1)
    except Exception:   # pylint: disable=broad-except
        print(_("Error rebuilding all GF reviews, unable to locate "
                "and/or run '{python} {trigger}'").
              format(python=python, trigger=trigger_path))
        sys.exit(1)

    if len(fd[0]):
        Verbosity.report(
            Verbosity.INFO,
            _("Re-setting 'git-fusion-reviews--all-gf' with {num_views:d} repo views").
            format(num_views=len(fd[0].splitlines())))


def check_triggers():
    """Check all of the GF triggers are installed and the trigger version is correct."""
    # pylint: disable=too-many-branches
    triggers = fetch_triggers()
    if not triggers:
        Verbosity.report(Verbosity.INFO, _('Git Fusion Triggers are not installed.'))
        return

    # Do we have a "p4gf_submit_trigger.py change-blah" entry for each
    # of the P4_TRIGGER_NAMES that we expect? If so, then the trigger
    # is fully installed.
    seen = set()
    for trig in triggers:
        if "p4gf_submit_trigger.py" not in trig:
            continue
        for tn in P4_TRIGGER_NAMES:
            if tn in trig:
                seen.add(tn)
    have_all_triggers = seen.issuperset(P4_TRIGGER_NAMES)
    if not have_all_triggers:
        Verbosity.report(Verbosity.INFO, _('Git Fusion Triggers are not installed.'))
        return

    # Is the installed version what we require?
    version = P4Key.get(p4, p4gf_const.P4GF_P4KEY_TRIGGER_VERSION)
    if version != '0':
        version = version.split(":")[0].strip()
    if version != p4gf_const.P4GF_TRIGGER_VERSION:
        Verbosity.report(Verbosity.INFO, _('Git Fusion Triggers are not up to date.'))
        Verbosity.report(Verbosity.INFO,
            _('This version of Git Fusion expects p4 key {key}={value}')
            .format(key=p4gf_const.P4GF_P4KEY_TRIGGER_VERSION,
                    value=p4gf_const.P4GF_TRIGGER_VERSION))
        return
    else:
        Verbosity.report(Verbosity.INFO, _('Git Fusion triggers are up to date.'))
        return


def main():
    """Do the thing."""
    # pylint: disable=too-many-statements, too-many-branches
    try:
        log_l10n()
        parse_argv()
        global P4PORT, P4USER, P4CLIENT
        needs_exit = False
        if not P4PORT and "P4PORT" not in os.environ:
            Verbosity.report(
                Verbosity.INFO,
                _('P4PORT is neither set in the environment nor passed as an option.'))
            needs_exit = True
        if not P4USER and "P4USER" not in os.environ:
            Verbosity.report(
                Verbosity.INFO,
                _('P4USER is neither set in the environment nor passed as an option.'))
            needs_exit = True
        # Check that a pre-existing P4GF_ENV config file P4PORT conflicts with the --port option
        if p4gf_const.P4GF_ENV and not Create_P4GF_CONFIG and P4PORT:
            if P4PORT != os.environ['P4PORT']:
                Verbosity.report(
                    Verbosity.INFO,
                    _("conflicting P4PORT in args: {p4port} and "
                      "P4GF_ENV {env} : P4PORT = {env_p4port}. Stopping.")
                    .format(p4port=P4PORT,
                            env=p4gf_const.P4GF_ENV,
                            env_p4port=os.environ['P4PORT']))
                needs_exit = True
            else:
                Verbosity.report(
                    Verbosity.INFO,
                    _("P4PORT argument is identically configured in {0}. Proceeding.")
                    .format(p4gf_const.P4GF_ENV))
        if needs_exit:
            sys.exit(1)

        p4gf_version_3.version_check()
        # Connect.
        global p4
        if not P4USER:
            P4USER = os.environ['P4USER']
        # if needed, set a bogus client name so that the default to hostname will not be used.
        if "P4CLIENT" not in os.environ:
            P4CLIENT = 'GF-' + str(uuid.uuid4().hex).lower()[-10:]
            os.environ['P4CLIENT'] = P4CLIENT
        else:
            P4CLIENT = os.environ['P4CLIENT']
        p4 = p4gf_create_p4.create_p4(port=P4PORT, user=P4USER, client=P4CLIENT)
        if not p4:
            raise RuntimeError(_("Failed to connect to P4."))
        P4PORT = p4.port
        P4USER = p4.user
        check_and_create_default_p4gf_env_config()
        if SHOW_IDS:
            show_all_server_ids()
            sys.exit(0)
        Verbosity.report(Verbosity.INFO, "P4PORT : {}".format(p4.port))
        Verbosity.report(Verbosity.INFO, "P4USER : {}".format(p4.user))

        # Require that we have super permission.
        # Might as well keep the result in case we need to write a new protect
        # table later. Saves a 'p4 protect -o' trip to the server
        protect_lines = fetch_protect()

        if P4_PASSWD and p4_security_level() > 0 and not strong_passwd(P4_PASSWD):
            Verbosity.report(
                Verbosity.ERROR,
                _("This Perforce server requires a strong password: >= 8 characters and"))
            Verbosity.report(
                Verbosity.ERROR,
                _("with mixed case or contain non alphabetic characters."))
            sys.exit(1)

        ensure_server_id()
        ensure_group()
        ensure_users()
        ensure_depot()
        ensure_protect(protect_lines)
        ensure_protects_configurable()
        set_proxy_protects_key()
        check_triggers()
        initialize_all_gf_reviews()

    except Exception as e:  # pylint: disable=broad-except
        sys.stderr.write(str(e) + '\n')
        p4gf_create_p4.close_all()
        sys.exit(1)


def parse_argv():
    """Copy optional port/user args into global P4PORT/P4USER."""
    # pylint:disable=line-too-long
    # Keep tabular code tabular.
    parser = p4gf_util.create_arg_parser(_("Creates Git Fusion users, depot, and protect entries."))
    parser.add_argument('--port',    '-p', metavar='P4PORT', nargs=1, help=_('P4PORT of server'))
    parser.add_argument('--user',    '-u', metavar='P4USER', nargs=1, help=_('P4USER of user with super permissions.'))
    Verbosity.add_parse_opts(parser)
    parser.add_argument('--id',                              nargs=1, help=_("Set this Git Fusion server's unique id"))
    parser.add_argument('--showids',            action='store_true',  help=_('Display all Git Fusion server ids'))
    parser.add_argument('--force', action='store_true', help=_("Force set local server-id file when server-id already registered in Git Fusion or change it to a new value if --id option is provided."))
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--passwd',                              nargs=1, help=_("Do not prompt for password, use PASSWD when creating new service users (ex. 'git-fusion-user' and 'git-fusion-reviews-*')"))
    group.add_argument('--no-passwd',           action='store_true', help=_("Do not prompt for nor set password when creating new service users (ex. 'git-fusion-user' and 'git-fusion-reviews-*')"))
    parser.add_argument('--unknown-git', action='store_true', help=_("Create the unknown_git user and if --passwd set passwd"))
    args = parser.parse_args()

    Verbosity.parse_level(args)

    # Optional args, None if left unset
    global P4PORT, P4USER, ID_FROM_ARGV, SHOW_IDS, P4_PASSWD, PROMPT_FOR_PASSWD
    global OVERRULE_SERVERID_CONFLICT, CREATE_UNKNOWN_GIT
    if args.port:
        P4PORT = args.port[0]
    if args.user:
        P4USER = args.user[0]
    if args.id:
        ID_FROM_ARGV = args.id[0]
    if args.showids:
        SHOW_IDS = True
    if args.passwd:
        P4_PASSWD = args.passwd[0]
        PROMPT_FOR_PASSWD = False
    elif args.no_passwd:
        PROMPT_FOR_PASSWD = False
    if args.force:
        OVERRULE_SERVERID_CONFLICT = True
    if args.unknown_git:
        CREATE_UNKNOWN_GIT = True

if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

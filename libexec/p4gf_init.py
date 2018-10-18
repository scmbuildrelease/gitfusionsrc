#! /usr/bin/env python3.3
"""Create the user and client that Git Fusion uses when communicating with Perforce.

Does NOT set up any git repos yet: see p4gf_init_repo.py for that.

Eventually there will be more options and error reporting. For now:
* current environment's P4PORT + P4USER is used to connect to Perforce.
* current P4USER must have enough privileges to create users, create clients.

Do not require super privileges for current P4USER or
git-fusion-user. Some customers reject that requirement.
"""

import logging
import os
import sys
import time
import re

import pygit2
import pytz

import P4
import p4gf_env_config    # pylint: disable=unused-import
import p4gf_config
import p4gf_const
import p4gf_create_p4
import p4gf_group
import p4gf_p4key     as     P4Key
from   p4gf_l10n      import _, NTR, log_l10n
import p4gf_log
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_p4spec
import p4gf_proc
import p4gf_util
from   p4gf_verbosity import Verbosity
import p4gf_version_3

# cannot use __name__ since it will often be "__main__"
LOG = logging.getLogger("p4gf_init")
OLD_OBJECT_CLIENT = "git-fusion--p4"
OLDER_OBJECT_CLIENT = "git-fusion-p4"


def _create_file(p4, client_name, local_path, file_content):
    """Create and submit a file.

    Write a file to the local Git Fusion workspace and then add and submit to
    Perforce. NOP if file already exists in Perforce after a 'p4 sync'.
    """
    filename = os.path.basename(local_path)
    with p4gf_util.restore_client(p4, client_name):
        try:
            with p4.at_exception_level(p4.RAISE_NONE):
                # Sync the file and ensure we really have it.
                p4.run('sync', '-q', local_path)
                results = p4.run('have', local_path)
            if not results:
                LOG.debug("_write_file(): {} does not exist, will create...".format(local_path))
                # Perms are probably read-only, need to remove before writing.
                if os.path.exists(local_path):
                    os.remove(local_path)
                else:
                    p4gf_util.ensure_parent_dir(local_path)
                with open(local_path, 'w') as mf:
                    mf.write(file_content)
                desc = _("Creating initial '{filename}' file via p4gf_init.py")\
                    .format(filename=filename)
                with p4gf_util.NumberedChangelist(p4=p4, description=desc) as nc:
                    nc.p4run('add', local_path)
                    nc.submit()
                LOG.debug("_write_file(): successfully created {}".format(local_path))
                _info(_("File '{path}' created.").format(path=local_path))
            else:
                _info(_("File '{path}' already exists.").format(path=local_path))
        except P4.P4Exception as e:
            LOG.warning('error setting up {file} file: {e}'
                     .format(file=filename, e=str(e)))


def _create_user_map(p4, client_name, rootdir):
    """Create the template user map file and submit to Perforce.

    Write the template user map file to the Git Fusion workspace and
    submit to Perforce, if such a file does not already exist.
    """
    file_content = _(
        '# Git Fusion user map'
        '\n# Format: Perforce-user [whitespace] Email-addr [whitespace] "Full-name"'
        '\n#joe joe@example.com "Joe User"'
    )
    _create_file(p4, client_name, rootdir + '/users/p4gf_usermap', file_content)


def _create_client(p4):
    """Create the host-specific Perforce client.

    This enables working with the object cache in the P4GF_DEPOT depot.
    """
    # See if the old object clients exist, in which case we will remove them.
    for old_client_name in [OLD_OBJECT_CLIENT,
                            OLDER_OBJECT_CLIENT,
                            p4gf_util.get_object_client_name()]:
        if p4gf_p4spec.spec_exists(p4, 'client', old_client_name):
            p4.run('client', '-df', old_client_name)
            _info(_("Old client '{client_name}' deleted.").format(client_name=old_client_name))


def _check_for_old_p4key(p4):
    """Raise an exception if 2013.1 upgrade incomplete.

    If a proper upgrade from 2012.2 to 2013.1+ is not done, an old p4key will
    be present.  Raise an exception if it is.
    """
    old_p4key_pattern = p4gf_const.P4GF_P4KEY_OLD_UPDATE_AUTH_KEYS.format('*')
    if P4Key.get_all(p4, old_p4key_pattern):
        raise RuntimeError(_('error: Git Fusion 2012.2 artifacts detected.'
                             ' Upgrade required for use with 2013.1+.'
                             ' Please contact your administrator.'))
    Verbosity.report(Verbosity.DEBUG, _('Old 2012.2 p4key not present.'))


def _init_if_needed(p4, started_p4key, complete_p4key, func, nop_msg):
    """Call func to do initialization if keys are not set.

    Check that the completed p4key is non-zero, as that indicates that
    initialization has already been done.

    If neither the started nor complete key are set, set the started key,
    call the function, and finally set the complete key.

    If only the started key is set, there may be an initialization attempt in
    progress.  Give it a chance to complete before assuming it has failed and
    restarting the initialization.

    Arguments:
      p4 -- P4 API object
      started_p4key -- name of init started p4key
      complete_p4key -- name of init completed p4key
      func -- initialization function to be called, takes a P4 argument.
              Must be idempotent since it is possible initialization may
              be performed more than once.
      nop_msg -- message to report if no initialization is needed

    Return True if initialization performed, False it not needed.
    """
    if P4Key.is_set(p4, complete_p4key):
        # Short-circuit when there is nothing to be done.
        Verbosity.report(Verbosity.INFO, nop_msg)
        return False

    check_times = 5

    while True:
        # If initialization has not been started, start it now.
        # Check before set to avoid a needless database write,
        # especially since this is run on every pull and push.
        if not P4Key.is_set(p4, started_p4key):
            if P4Key.acquire(p4, started_p4key):
                func(p4)
                # Set a p4key so we will not repeat initialization later.
                P4Key.acquire(p4, complete_p4key)
                return True

        # If initialization has been completed, we're done.
        if P4Key.is_set(p4, complete_p4key):
            Verbosity.report(Verbosity.INFO, nop_msg)
            return False

        # Another process started initialization.  It may still be working on
        # it or it may have died.  Wait a bit to give it a chance to complete.
        time.sleep(1)
        check_times -= 1
        if not check_times:
            # Other process failed to finish perhaps.
            # Steal the "lock" and do the init ourselves.
            P4Key.delete(p4, started_p4key)
        elif check_times < 0:
            raise RuntimeError(_('error: unable to aquire lock for initialization'))


def _info(msg):
    """Print msg to CLI output if Verbosity level is high enough."""
    Verbosity.report(Verbosity.INFO, msg)


def _ensure_specs_created(p4):
    """Ensure that various specs have been created by p4gf_super_init."""
    spec_list = [
        ['user', p4gf_const.P4GF_USER],
        ['user', p4gf_util.gf_reviews_user_name()],
        ['user', p4gf_const.P4GF_REVIEWS__NON_GF],
        ['group', p4gf_const.P4GF_GROUP],
        ['depot', p4gf_const.P4GF_DEPOT]
    ]
    for spec_type, spec_id in spec_list:
        if not p4gf_p4spec.spec_exists(p4, spec_type, spec_id):
            raise RuntimeError(_("error: {spec_type} '{spec_id}' does not exist."
                                 " Please contact your administrator.")
                               .format(spec_type=spec_type, spec_id=spec_id))


def _ensure_permission_groups(p4):
    """Create permissiog groups if they don't exist."""
    for group in [p4gf_group.PERM_PULL, p4gf_group.PERM_PUSH]:
        c = p4gf_group.create_global_perm(p4, group)
        if c:
            _info(_("Global permission group '{group}' created.").format(group=group))
        else:
            _info(_("Global permission group '{group}' already exists.").format(group=group))

    c = p4gf_group.create_default_perm(p4)
    if c:
        _info(_("Default permission p4key '{key}' set to '{value}'.")
              .format(key=p4gf_const.P4GF_P4KEY_PERMISSION_GROUP_DEFAULT,
                      value=p4gf_group.DEFAULT_PERM))
    else:
        _info(_("Default permission p4key '{key}' already exists.")
              .format(key=p4gf_const.P4GF_P4KEY_PERMISSION_GROUP_DEFAULT))


def _ensure_admin_privileges(p4):
    """Ensure that single git-fusion-user has admin privileges over GF depot."""
    is_protects_empty = False
    try:
        p4.run('protects', '-u', p4gf_const.P4GF_USER, '-m',
               '//{depot}/...'.format(depot=p4gf_const.P4GF_DEPOT))
    except P4.P4Exception:
        # Why MsgDm_ReferClient here? Because p4d 11.1 returns
        # "must refer to client" instead of "Protections table is empty" when
        # given a depot path to 'p4 protects -m -u'. Surprise!
        if p4gf_p4msg.find_msgid(p4, [p4gf_p4msgid.MsgDm_ProtectsEmpty,
                                      p4gf_p4msgid.MsgDm_ReferClient]):
            is_protects_empty = True
        # All other errors are fatal, propagated.

    if is_protects_empty:
        # - order the lines in increasing permission
        # - end with at least one user (even a not-yet-created user) with super
        #     write user * * //...
        #     admin user git-fusion-user * //...
        #     super user super * //...
        p4gf_p4spec.set_spec(p4, 'protect', values={
            'Protections': ["super user * * //...",
                            "super user {user} * //...".format(user=p4gf_const.P4GF_USER),
                            "admin user {user} * //{depot}/..."
                            .format(user=p4gf_const.P4GF_USER, depot=p4gf_const.P4GF_DEPOT)]})
        _info(_('Protects table set.'))


def _global_init(p4):
    """Check that p4gf_super_init has been run."""
    #
    # The global initialization process below must be idempotent in the sense
    # that it is safe to perform more than once. As such, there are checks to
    # determine if work is needed or not, and if that work results in an
    # error, log and carry on with the rest of the steps, with the assumption
    # that a previous attempt had failed in the middle (or possibly that
    # another instance of Git Fusion has started at nearly the same time as
    # this one).
    #

    p4gf_util.has_server_id_or_exit()
    _ensure_specs_created(p4)
    _ensure_permission_groups(p4)
    _ensure_admin_privileges(p4)
    _ensure_proxy_protects_key(p4)


def _ensure_proxy_protects_key(p4):
    """Ensure super_init has set the proxy_protects key."""
    if not P4Key.is_set(p4, p4gf_const.P4GF_P4KEY_PROXY_PROTECTS):
        raise RuntimeError(_('Git Fusion key {key} is not set. Contact your administrator. '
                             'Run configure-git-fusion.sh and try again.')
                           .format(key=p4gf_const.P4GF_P4KEY_PROXY_PROTECTS))


def _ensure_git_config_non_empty(key, value):
    """If Git lacks a global config value for a given key, set one.

    Returns value found (if any) or set (if none found).

    :param key: configuration key.
    :param value: new configuration value.

    """
    # pygit2 does not like the config file to be missing
    fpath = os.path.expanduser(NTR('~/.gitconfig'))
    if not os.path.exists(fpath):
        with open(fpath, 'w') as f:
            f.write(_("# Git Fusion generated"))
            f.write(NTR('\n'))
    config = pygit2.Config.get_global_config()
    if key in config:
        return config[key]
    config[key] = value
    return value


def _upgrade_p4gf(p4):
    """Perform upgrade from earlier versions of P4GF.

    This should be invoked using _maybe_perform_init() to avoid race conditions
    across hosts.
    """
    # If updating from 12.2 to 13.1 we need to create global config file
    # (this does nothing if file already exists)

    p4gf_config.GlobalConfig.init(p4)
    with p4.at_exception_level(p4.RAISE_ERROR):
        if p4gf_config.GlobalConfig.write_if(p4):
            _info(_("Global config file '{path}' created/updated.")
                  .format(path=p4gf_config.depot_path_global()))
        else:
            _info(_("Global config file '{path}' already exists.")
                  .format(path=p4gf_config.depot_path_global()))
    # Ensure the time zone name has been set, else default to something sensible.
    tzname = P4Key.get(p4, p4gf_const.P4GF_P4KEY_TIME_ZONE_NAME)
    if tzname == "0" or tzname is None:
        msg = _("p4 key '{key}' not set, using UTC as default."
                " Change this to your Perforce server's time zone.") \
            .format(key=p4gf_const.P4GF_P4KEY_TIME_ZONE_NAME)
        LOG.warning(msg)
        sys.stderr.write(_('Git Fusion: {message}\n').format(message=msg))
        tzname = None
    else:
        # Sanity check the time zone name.
        try:
            pytz.timezone(tzname)
        except pytz.exceptions.UnknownTimeZoneError:
            LOG.warning("Time zone name '{}' unrecognized, using UTC as default".format(tzname))
            tzname = None
    if tzname is None:
        P4Key.set(p4, p4gf_const.P4GF_P4KEY_TIME_ZONE_NAME, 'UTC')


def _delete_old_init_p4keys(p4, server_id):
    """Remove the old host-specific initialization p4keys, if any."""
    names = []
    names.append("git-fusion-{}-init-started".format(server_id))
    names.append("git-fusion-{}-init-complete".format(server_id))
    with p4.at_exception_level(p4.RAISE_NONE):
        for name in names:
            P4Key.delete(p4, name)


def init(p4):
    """Ensure both global and host-specific initialization are completed."""
    _check_for_old_p4key(p4)

    started_p4key = p4gf_const.P4GF_P4KEY_INIT_STARTED
    complete_p4key = p4gf_const.P4GF_P4KEY_INIT_COMPLETE
    _init_if_needed(p4, started_p4key, complete_p4key, _global_init,
                    _('Permissions already initialized. Not changing.'))

    server_id = p4gf_util.get_server_id()
    started_p4key = p4gf_const.P4GF_P4KEY_INIT_STARTED + '-' + server_id
    complete_p4key = p4gf_const.P4GF_P4KEY_INIT_COMPLETE + '-' + server_id
    p4gf_dir = p4gf_const.P4GF_HOME

    def client_init(p4):
        """Perform host-specific initialization (and create sample usermap)."""
        # Set up the host-specific client.
        _create_client(p4)
        # Ensure the default user map and global config files are in place.
        _create_user_map(p4, p4.client, p4gf_dir)
        p4gf_config.GlobalConfig.init(p4)
        with p4.at_exception_level(p4.RAISE_ERROR):
            p4gf_config.GlobalConfig.write_if(p4)
        _delete_old_init_p4keys(p4, server_id)

    if not _init_if_needed(p4, started_p4key, complete_p4key, client_init,
                           _('Client and usermap already initialized. Not changing.')):

        # If client already created, make sure it hasn't been tweaked.
        # ##: do we really need to handle this case? this is here just to pass the tests
        view = ['//{depot}/... //{client}/...'.format(depot=p4gf_const.P4GF_DEPOT,
                client=p4.client)]
        p4gf_p4spec.ensure_spec_values(p4, 'client', p4.client,
                                     {'Root': p4gf_dir, 'View': view})

    # Perform any necessary upgrades within a "lock" to avoid race conditions.
    # For now, the lock is global, but could conceivably loosen to host-only.
    started_p4key = p4gf_const.P4GF_P4KEY_UPGRADE_STARTED
    complete_p4key = p4gf_const.P4GF_P4KEY_UPGRADE_COMPLETE
    _init_if_needed(p4, started_p4key, complete_p4key, _upgrade_p4gf,
                    _('Global config file already initialized. Not changing.'))

    # Require non-empty Git config user.name and user.email.
    _ensure_git_config_non_empty('user.name',  _('Git Fusion Machinery'))
    _ensure_git_config_non_empty('user.email', _('nobody@example.com'))

    # Turn on CVE-CVE-2014-9390 rejection added in Git 2.2.1.
    # This linux server isn't vulnerable, but we still don't want to host
    # offending repos that could hit Windows/Mac OS X workstations.
    _ensure_git_config_non_empty('receive.fsckObjects', True)
    _ensure_git_config_non_empty('core.protectHFS',     True)
    _ensure_git_config_non_empty('core.protectNTFS',    True)

def remove_old_temp_and_repo_clients(p4):
    """Remove the old style temp clients and repo clients."""

    server_id = p4gf_util.get_server_id()

    # Get a list of old style temp repo clients which may still exist.
    template = "git-fusion-{server_id}-{repo_name}-temp-{n}"
    pattern = template.format( server_id=server_id,
                               repo_name='*',
                               n='*')
    clients = [client['client'] for client
               in p4.run(NTR(["clients", "-e", pattern]))]

    for client in clients:
        try:
            p4.run(NTR(["client", "-d", "-f", client]))
            print(_("removed temp client {client}").format(client=client))
        except P4.P4Exception:
            print(_("cannot remove temp client {client}").format(client=client))

    # Get a list of old style permanent repo clients.
    old_client_repo_re = "git-fusion-{0}-[^-]+$".format(server_id)
    old_client_repo_re = re.compile(old_client_repo_re)
    pattern = p4gf_const.P4GF_REPO_CLIENT.format( server_id=server_id,
                               repo_name='*')
    clients = []
    for client in [client['client'] for client in p4.run(NTR(["clients", "-e", pattern]))]:
        if old_client_repo_re.match(client):
            clients.append(client)

    for client in clients:
        try:
            p4.run(NTR(["client", "-d", "-f", client]))
            print(_("removed temp client {client}").format(client=client))
        except P4.P4Exception:
            print(_("cannot remove temp client {client}").format(client=client))


def _parse_argv():
    """Parse command line arguments.

    Only version, help options for now.
    """
    parser = p4gf_util.create_arg_parser(_('Initializes a Git Fusion server.'))
    Verbosity.add_parse_opts(parser)
    args = parser.parse_args()
    Verbosity.parse_level(args)


def main():
    """Create Perforce user and client for Git Fusion."""
    p4gf_version_3.log_version_extended(include_checksum=True)
    try:
        log_l10n()
        p4gf_version_3.version_check()
    except Exception as e:  # pylint: disable=broad-except
        sys.stderr.write(e.args[0] + '\n')
        sys.exit(1)

    # To fetch the object client below we need to ensure there is a server
    # ID available on this system, and since we require that anyway, may as
    # well check it now, when we need it.
    p4gf_util.has_server_id_or_exit()
    with p4gf_create_p4.Closer():
        p4 = p4gf_create_p4.create_p4_temp_client()
        if not p4:
            return 2

        Verbosity.report(Verbosity.INFO, "P4PORT : {}".format(p4.port))
        Verbosity.report(Verbosity.INFO, "P4USER : {}".format(p4.user))

        p4gf_util.reset_git_enviro()
        p4gf_proc.init()

        try:
            init(p4)
            remove_old_temp_and_repo_clients(p4)
        except PermissionError:
            LOG.exception("unable to initialize Git Fusion")
            sys.stderr.write(_("File permissions error, please check ownership"
                               " and mode of ~/.git-fusion directory.\n"))
            sys.exit(os.EX_NOPERM)

    return 0

if __name__ == "__main__":
    _parse_argv()
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)
else:
    Verbosity.VERBOSE_LEVEL = Verbosity.QUIET

#! /usr/bin/env python3.3
"""Set Git Fusion environment from optional environment config file named in P4GF_ENV."""

import configparser
import logging
import os
from subprocess import check_output, STDOUT, CalledProcessError
import sys

import p4gf_bootstrap  # pylint: disable=unused-import
import p4gf_const
from   p4gf_l10n    import _, NTR
import p4gf_log
from p4gf_missing_config_path import MissingConfigPath

# This module is intended to execute prior to any main() statements.
# This is necessary to configure the process environment at
# the earliest opportunity from the P4GF_ENV named config file.
# We accomplish this by including this module at the top of all
# python scripts which run as main().
# For example:
#        import p4gf_env_config  # pylint: disable=unused-import
# The including module will make no references to this module,
# thus we disable W0611 - 'Unused import'

# Contains the environment settings loaded from the P4GF_ENV file
P4GF_ENV_CONFIG_DICT = None
# list of prohibited vars - will raise error
# rather than be ignored to prevent failing to enforce user's expected behavior
Prohibited_vars = [NTR('PATH'), NTR('LANG')]
Required_vars = [NTR('P4GF_HOME'), NTR('P4PORT')]  # list of required config items

Unset = NTR('unset')   # value to cause ENV key to be unset (case insensitive test)
# from p4 help environment
P4_vars = [
    NTR('P4CHARSET'),                 # Client's local character set
    NTR('P4COMMANDCHARSET'),          # Client's local character set (for command line operations)
    NTR('P4CLIENT'),                  # Name of client workspace
    NTR('P4CLIENTPATH'),              # Directories client can access
    NTR('P4CONFIG'),                  # Name of configuration file
    NTR('P4DIFF'),                    # Diff program to use on client
    NTR('P4DIFFUNICODE'),             # Diff program to use on client
    NTR('P4EDITOR'),                  # Editor invoked by p4 commands
    NTR('P4HOST'),                    # Name of host computer
    NTR('P4IGNORE'),                  # Name of ignore file
    NTR('P4LANGUAGE'),                # Language for text messages
    NTR('P4LOGINSSO'),                # Client side credentials script
    NTR('P4MERGE'),                   # Merge program to use on client
    NTR('P4MERGEUNICODE'),            # Merge program to use on client
    NTR('P4PAGER'),                   # Pager for 'p4 resolve' output
    NTR('P4PASSWD'),                  # User password passed to server
    NTR('P4PORT'),                    # Port to which client connects
    NTR('P4SSLDIR'),                  # SSL server credential director
    NTR('P4TICKETS'),                 # Location of tickets file
    NTR('P4TRUST'),                   # Location of ssl trust file
    NTR('P4USER')]                    # Perforce user name

# We wish to log config file processing during load of this module.
# This is before __ main __ instantiates the ExceptionLogger
# So explicitly configure logging
p4gf_log._lazy_init()   # pylint: disable=protected-access

LOG = logging.getLogger(__name__)
_configured = False  # python loads only once, but nevertheless set on load

Client_configurables = [
        NTR('filesys.binaryscan'),
        NTR('filesys.bufsize'),
        NTR('lbr.verify.out'),
        NTR('net.keepalive.disable'),
        NTR('net.keepalive.idle'),
        NTR('net.keepalive.interval'),
        NTR('net.keepalive.count'),
        NTR('net.maxwait'),
        NTR('net.rfc3484'),
        NTR('net.tcpsize'),
        NTR('sys.rename.max'),
        NTR('sys.rename.wait')
]

class EnvironmentConfig:

    """Set the os.environ from a config file named in P4GF_ENV."""

    def __init__(self):
        self.p4_vars = []                               # P4vars explicitly set byconfigfile
        if p4gf_const.P4GF_ENV_NAME in os.environ:      # GF's copy - defaults to None
            p4gf_const.P4GF_ENV = os.environ[p4gf_const.P4GF_ENV_NAME]
        module = os.path.basename(get_main_module())
        self.raise_ =  module == 'p4gf_super_init.py'

    def log_gf_env(self):
        """Log the resulting Git Fusion environment."""
        LOG.info("Git Fusion P4GF_HOME = {0}".format(p4gf_const.P4GF_HOME))
        LOG.info("Git Fusion GIT_BIN = {0}".format(p4gf_const.GIT_BIN))
        LOG.info("Git Fusion P4GF_DEPOT = {0}".format(p4gf_const.P4GF_DEPOT))
        try:
            git_path = check_output(['which', p4gf_const.GIT_BIN], stderr=STDOUT)
        except CalledProcessError:
            msg = _("Cannot find git at {git_bin}").format(git_bin=p4gf_const.GIT_BIN)
            self.raise_error(msg)

        git_path = git_path.decode().strip()
        git_version = check_output(
            [git_path, '--version'], stderr=STDOUT).decode().strip()
        LOG.info("Git Fusion is configured for git: " + git_path + "  " + git_version)

        for var in P4_vars:
            if var in os.environ:
                LOG.info("Git Fusion P4 vars in environment: {0} = {1}".
                         format(var, os.environ[var] if var != 'P4PASSWD' else '********'))

    @staticmethod
    def unset_environment(env_vars):
        """Unset the environment variables named in the string/list."""
        if isinstance(env_vars, str):
            evars = [env_vars]
        else:
            evars = env_vars

        for var in evars:
            if var in os.environ:
                del os.environ[var]
                LOG.info("Unsetting environment var {0}".format(var))

    def reset_gf_environment(self):
        """Reset the Git Fusion environment as if for the first time."""
        global _configured
        _configured = False
        self.set_gf_environment()

    def set_gf_environment(self):
        """Set the os.environ from a config file named in P4GF_ENV.

        If the var is set but the value is not a file, raise error.
        """
        global _configured
        if not _configured:
            if not p4gf_const.P4GF_ENV:
                self.set_gf_environ_from_environment()
            else:
                self.raise_if_not_absolute_file(p4gf_const.P4GF_ENV, p4gf_const.P4GF_ENV_NAME)
                self.set_gf_environ_from_config()
                self.version_p4gf_env_config()
            self.log_gf_env()
            _configured = True

    @staticmethod
    def check_required(key):
        """Remove eligible key from required list ."""
        # pylint: disable=global-variable-not-assigned
        # pylint fails to detect the remove as causing a modification
        global Required_vars
        if key in Required_vars:
            Required_vars.remove(key)

    def check_prohibited(self, key):
        """Raise error if key is in prohibited list."""
        if key in Prohibited_vars:
            msg = _("Git Fusion environment: config_file {config_file} :"
                    " {key} may not be set in this config file.") \
                .format(config_file=p4gf_const.P4GF_ENV, key=key)
            self.raise_error(msg)

    @staticmethod
    def set_gf_environ_from_environment():
        """Use the inherited environment and the Git Fusion default GFHOME."""
        # Default behavior
        LOG.info("P4GF_ENV not set. Using default environment.")

    def set_gf_environ_from_config(self):
        """Load the Git Fusion environment config file and
        set the os.environ from its values.
        """
        # pylint: disable=too-many-statements, too-many-branches
        global P4GF_ENV_CONFIG_DICT
        p4_vars_in_config = []
        p4config_path = None
        config_path = p4gf_const.P4GF_ENV
        LOG.info("Attempting to set environment from config file {0}.".format(config_path))
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        try:
            config.read(config_path)
        except configparser.Error as e:
            msg = _("Unable to read Git Fusion environment config file"
                    " '{config_file}'.\n{exception}") \
                .format(config_file=config_path, exception=e)
            self.raise_error(msg)

        if config.has_section(p4gf_const.SECTION_ENVIRONMENT):
            p4gf_config_dict = dict(config.items(p4gf_const.SECTION_ENVIRONMENT))
            for key, val in p4gf_config_dict.items():
                value = val
                if NTR('#') in value:
                    value = value[:value.index(NTR('#'))].rstrip()
                value = value.strip(NTR("'")).strip(NTR('"')).rstrip(NTR('/'))
                if key == p4gf_const.P4GF_HOME_NAME:
                    p4gf_const.P4GF_HOME = value
                    p4gf_const.P4GF_DIR = os.path.basename(p4gf_const.P4GF_HOME)
                    LOG.info("setting P4GF_HOME {0}  P4GF_DIR {1}".
                             format(p4gf_const.P4GF_HOME, p4gf_const.P4GF_DIR))
                    create_if_not_existing_dir(p4gf_const.P4GF_HOME)
                    self.check_required(key)
                    continue
                if key == p4gf_const.GIT_BIN_NAME:
                    p4gf_const.GIT_BIN = value
                    if value != p4gf_const.GIT_BIN_DEFAULT:
                        self.raise_if_not_exe_path(p4gf_const.GIT_BIN, p4gf_const.GIT_BIN_NAME)
                    self.check_required(key)
                    continue
                if key == p4gf_const.P4GF_DEPOT_NAME:
                    p4gf_const.P4GF_DEPOT = value
                    LOG.info("setting P4GF_DEPOT {0}".
                             format(p4gf_const.P4GF_DEPOT))
                    continue
                if key == p4gf_const.READ_ONLY_NAME:
                    p4gf_const.READ_ONLY = config.getboolean(
                        p4gf_const.SECTION_ENVIRONMENT, p4gf_const.READ_ONLY_NAME)
                    continue
                if key == p4gf_const.MAX_TEMP_CLIENTS_NAME:
                    try:
                        p4gf_const.MAX_TEMP_CLIENTS = int(value)
                    except ValueError:
                        msg = _("Git Fusion environment: config file {config_file} "
                                "MAX_TEMP_CLIENTS set incorrectly to a non "
                                "integer value {max_temp_clients1}.") \
                            .format(config_file=config_path, max_temp_clients=value)
                        self.raise_error(msg)
                    continue
                self.check_prohibited(key)
                if value.lower() == Unset:     # permit unset
                    del os.environ[key]
                    LOG.info("unsetting shell var {0}".format(key))
                    continue
                os.environ[key] = value
                LOG.info("setting shell var {0} = {1}".format(
                    key, value if key != 'P4PASSWD' else '********'))
                if key.startswith(NTR('P4')):
                    if key == 'P4CONFIG':
                        # raises exception if P4CONFIG file fails to meet guardrail requirements
                        p4config_path = value
                        self.validate_p4config(p4config_path)
                    p4_vars_in_config.append(key)
                self.check_required(key)
            LOG.info("P4GF_ENV setting P4 vars: {0}   :\n  any missing required items? {1}".
                     format(p4_vars_in_config, Required_vars))
            # Unset any P4 vars not set in the config file.
            self.unset_environment(set(P4_vars) - set(p4_vars_in_config))
            if len(Required_vars):
                msg = _("Git Fusion environment: config file {config_file} is "
                        "missing required item(s): {vars}.") \
                    .format(config_file=config_path, vars=Required_vars)
                self.raise_error(msg)
            if 'P4PASSWD' in p4gf_config_dict:
                p4gf_config_dict['P4PASSWD'] = '********'
            P4GF_ENV_CONFIG_DICT = p4gf_config_dict

        else:
            msg = _("Git Fusion environment: config file '{config_file}' has "
                    "no 'environment' section.") \
                .format(config_file=config_path)
            self.raise_error(msg)

    def version_p4gf_env_config(self):
        """If the user defined P4GF_ENV file has changed then save it to Perforce."""
        pass

    def raise_error(self, msg):
        """Raise the error OR write the message to stderr and sys.exit().
        This module is imported prior to __main__ execution, which
        usually wraps main with p4gf_log.run_with_exception_logger
        to suppress writing errors and the stacktrace to stderr.
        In these cases, fatal errors in this module must be reported
        to stderr followed by sys.exit.
        The one case when the MissingConfigPath Exception needs to be
        thrown is for p4gf_super_init which catches this exception
        and then creates the P4GF_ENV named config path."""
        LOG.error(msg)
        if self.raise_:
            raise RuntimeError(msg)
        else:
            sys.stderr.write(msg)
            sys.exit(1)

    def raise_if_not_exe_path(self, fpath, pseudonym):
        """Raise if not absolute, exists, and executable."""
        if (os.path.isabs(fpath)
                and os.path.isfile(fpath)
                and os.access(fpath, os.X_OK)):
            return
        msg = _("Git Fusion environment: config_file {config_file} :\n"
                " '{pseudonym}' path is relative, missing, or not executable: {path}.") \
            .format(config_file=p4gf_const.P4GF_ENV,
                    pseudonym=pseudonym,
                    path=fpath)
        self.raise_error(msg)


    def raise_if_not_absolute_file(self, fpath, pseudonym):
        """Raise if not absolute and exists."""
        # Raise this named Exception for use only by p4gf_super_init.py
        if not os.path.exists(fpath):
            msg = _("Git Fusion environment: path {0} in {1} does not exist."
                    ).format(fpath, p4gf_const.P4GF_ENV_NAME)
            if self.raise_:
                # raise only for super_init which reacts to creat the config file
                raise MissingConfigPath(fpath)
            else:
                self.raise_error(msg)
        if os.path.isabs(fpath) and os.path.isfile(fpath):
            return
        msg = _("Git Fusion environment: invalid path {path} in {pseudonym}:\n"
                "is not an existing absolute file.") \
            .format(path=fpath, pseudonym=pseudonym)
        self.raise_error(msg)


    def validate_p4config(self, p4config_path):
        """Verify the path is exists, is absolute, and contains only acceptable values."""
        # pylint: disable=line-too-long
        path = os.path.expanduser(p4config_path)
        self.raise_if_not_absolute_file(path, p4gf_const.P4GF_ENV)
        with open(path, 'r') as fd:
            lines = fd.readlines()

        for line in lines:
            line = line.rstrip()
            if not '=' in line:
                msg = _("Git Fusion environment: P4CONFIG {path} is improperly formatted: {line}.")\
                       .format( path=p4config_path, line=line)
                self.raise_error(msg)
            key = line.split('=')[0]
            if key not in Client_configurables:
                msg = _("Git Fusion environment: P4CONFIG {path} contains unsupported setting: {key}.")\
                        .format(path=p4config_path, key=key)
                self.raise_error(msg)
            else:
                msg = _("Git Fusion environment: P4CONFIG {path} contains supported setting: {line}.")\
                        .format(path=p4config_path, line=line)
                LOG.debug(msg)


def create_if_not_existing_dir(dpath):
    """Create the path if it does not already exist."""
    if os.path.isabs(dpath) and os.path.isdir(dpath):
        return
    try:
        os.makedirs(dpath)
    except OSError:
        msg = "Git Fusion environment: unable to create directory {}".format(dpath)
        LOG.exception(msg)


def get_main_module():
    """Return the name of the module which defined the __main__ module."""
    module = str(sys.modules[NTR('__main__')])
    idx = module.find(NTR('from')) + 4
    return module[idx:].rstrip('>').strip('\'"')

LOG.info('p4gf_env_config imported by {0}'.format(get_main_module()))
Env_config = EnvironmentConfig()
Env_config.set_gf_environment()

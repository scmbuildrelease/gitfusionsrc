#! /usr/bin/env python3.3
"""Test hooks for Git Fusion configuration files.

Invoked from config_file.t and used to modify individual options and test
shadowing of options between global and repo configs.

Split out to break cyclic import p4gf_config <-> p4gf_context
"""
import configparser

import p4gf_env_config    # pylint: disable=unused-import
from p4gf_config import RepoConfig, SECTION_REPO
import p4gf_create_p4
from   p4gf_l10n import NTR     # All NTR, no _. No need to translate this test-only file.
import p4gf_util


def _create_p4():
    """create a P4 for reading config files."""
    return p4gf_create_p4.create_p4_temp_client()


def _test_read(repo_name, section, option):
    """Unit test hook to see if we actually read the correct values from the correct files."""
    if repo_name == 'global':
        rc = RepoConfig.make_default("global", _create_p4())
    else:
        rc = RepoConfig.from_depot_file(repo_name, _create_p4(), create_if_missing=False)

    try:
        value = rc.get(section, option)
        if value is None or value is '':
            value = 'none'
        print(value)
    except configparser.NoSectionError:
        print(NTR('section not found: {}').format(section))
    except configparser.NoOptionError:
        print(NTR('option not found: [{section}] {option}')
              .format(section=section, option=option))


def _test_read_branch(repo_name, branch, option):
    """Unit test hook to see if we actually read the correct values from the correct files."""
    config = RepoConfig.from_depot_file(repo_name, _create_p4(), create_if_missing=False)
    section = config.section_for_branch(branch)
    if section:
        try:
            value = config.get(section, option)
            if value is None:
                value = 'none'
            print(value)
        except configparser.NoOptionError:
            print(NTR('option not found: {}').format(option))
    else:
        print(NTR('branch not found: {}').format(branch))


def _test_write(repo_name, option, value):
    """Unit test hook to see if we actually write the correct values to the correct files."""
    if repo_name == 'global':
        print(NTR('write to global config not implemented.'))
        return
    p4 = _create_p4()
    config = RepoConfig.from_depot_file(repo_name, p4, create_if_missing=False)
    config.set(SECTION_REPO, option, value)
    config.write_repo_if(p4=p4)


def _test_write_branch(repo_name, branch, option, value):
    """Unit test hook to see if we actually write the correct values to the correct files."""
    p4 = _create_p4()
    config = RepoConfig.from_depot_file(repo_name, p4, create_if_missing=False)
    section = config.section_for_branch(branch)
    if section:
        config.set(section, option, value)
        config.write_repo_if(p4=p4)
    else:
        print(NTR('branch not found: {}').format(branch))


def _test_delete(repo_name, section):
    """Unit test hook to see remove a section from a config file."""
    if repo_name == 'global':
        print(NTR('delete from global config not implemented.'))
        return
    p4 = _create_p4()
    config = RepoConfig.from_depot_file(repo_name, p4, create_if_missing=False)
    print("before remove, have: {}".format(config.sections()))
    if not config.remove_section(section):
        print(NTR('section not found: {}').format(section))
        return
    print("section removed, now have: {}".format(config.sections()))
    config.write_repo_if(p4=p4)
    config = RepoConfig.from_depot_file(repo_name, p4, create_if_missing=False)
    print("read back, section removed, now have: {}".format(config.sections()))


def main():
    """Parse the command-line arguments and perform the desired function."""
    desc = "Helper script for testing the configuration code."
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument('command', metavar='<command>')
    parser.add_argument('repo_name', metavar='<repo-name>')
    parser.add_argument('section', metavar='<section>')
    parser.add_argument('--key', metavar='<key>')
    parser.add_argument('--value', metavar='<value>')
    args = parser.parse_args()

    if args.command == 'read':
        _test_read(args.repo_name, args.section, args.key)
    elif args.command == 'write':
        _test_write(args.repo_name, args.key, args.value)
    # for read/write-branch, section is actually git-branch-name
    elif args.command == 'read-branch':
        _test_read_branch(args.repo_name, args.section, args.key)
    elif args.command == 'write-branch':
        _test_write_branch(args.repo_name, args.section, args.key, args.value)
    elif args.command == 'delete':
        _test_delete(args.repo_name, args.section)
    else:
        print(NTR("unknown command: {}").format(args.command))


if __name__ == "__main__":
    main()

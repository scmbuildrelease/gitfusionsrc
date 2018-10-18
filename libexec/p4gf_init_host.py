#! /usr/bin/env python3.3
"""Initialize the Git Fusion host."""

import logging
import os
import shutil
import sys

import pygit2

from p4gf_l10n import _, NTR
import p4gf_proc
import p4gf_ensure_dir

LOG = logging.getLogger(__name__)


def is_init_needed(repo_dirs):
    """Return True if any host initialization is needed."""
    if not os.path.exists(os.path.join(repo_dirs.GIT_DIR, 'HEAD')):
        return True
    if not os.path.exists(repo_dirs.p4root):
        return True
    return False


def init_host(repo_dirs, ctx):
    """Initialize this Git Fusion host for the given repository.

    The caller should have the git-repo write lock before calling.

    """
    create_git_repo(repo_dirs.GIT_DIR, ctx.git_autopack, ctx.git_gc_auto, ctx.p4)
    p4gf_ensure_dir.ensure_dir(repo_dirs.p4root)


def create_git_repo(git_dir, git_autopack, git_gc_auto, p4):
    """Create the git repository in the given root directory."""
    LOG.debug3("create_git_repo")
    # Test if the Git repository has already been created.
    if os.path.exists(os.path.join(git_dir, 'HEAD')):
        # Repo exists, but it may need its pack settings corrected
        LOG.debug3("create_git_repo repo exists  - check git config")
        update_git_config_and_repack(git_dir, git_autopack, git_gc_auto)
        return

    # Prepare the Git repository directory, cleaning up if necessary.
    work_tree = os.path.dirname(git_dir)
    if not os.path.exists(git_dir):
        if os.path.exists(work_tree):
            # weird case where git view dir exists but repo was deleted
            LOG.warning("mirror Git repository {} in bad state, repairing...".format(git_dir))
            shutil.rmtree(work_tree)
        LOG.debug("creating directory %s for Git repo", git_dir)
        os.makedirs(git_dir)

    # Initialize the Git repository for that directory. Make it --bare.
    LOG.debug("creating bare Git repository in %s", git_dir)
    pygit2.init_repository(git_dir, bare=True)

    cwd = os.getcwd()
    os.chdir(work_tree)
    settings = {
        # Prevent conflicting change history by disallowing rewinds.
        'receive.denyNonFastForwards': 'true'
    }
    settings.update(get_case_handling_settings(p4))
    # Disable git autopack if so configured
    # Default autopack is enabled - no settings required
    LOG.debug("autopack={0} git_gc_auto={1}".format(git_autopack, git_gc_auto))
    settings.update(get_autopack_settings(git_autopack, git_gc_auto))
    LOG.debug("setting git config with {0}".format(settings))
    set_git_config(git_dir, settings)
    os.chdir(cwd)
    install_hook(git_dir)
    # Don't bother changing branches in a --bare repo.


def update_git_config_and_repack(git_dir, git_autopack, git_gc_auto):
    """Unset git config settings which disable packing.

    Repos created prior to 14.1 disabled packing.
    Detect these old settings, remove them and call git repack.

    """
    # See if there is anything to do before doing a bunch of work.
    repo = pygit2.Repository(git_dir)
    if 'receive.autogc' not in repo.config:
        return

    cwd = os.getcwd()
    os.chdir(os.path.dirname(git_dir))
    remove_pre_14_settings = {
        'receive.autogc': 'unset',
        'receive.unpacklimit': 'unset',
        'gc.auto': 'unset',
        'gc.autopacklimit': 'unset',
        'pack.compression': 'unset',
        'transfer.unpacklimit': 'unset',
    }
    set_git_config(git_dir, remove_pre_14_settings)
    gc_settings = get_autopack_settings(git_autopack, git_gc_auto)
    set_git_config(git_dir, gc_settings)
    cmd = ['git', 'repack', '-ad']
    result = p4gf_proc.popen_no_throw(cmd)
    if result['ec']:
        LOG.error("update_git_config_and_repack: git repack failed with: %s", result['err'])
        sys.stderr.write(_("Perforce: error: git repack failed with '{error}'\n").format(
            error=result['ec']))
    else:
        LOG.debug3("update_git_config_and_repack: %s", result)
    os.chdir(cwd)


def get_autopack_settings(git_autopack, git_gc_auto):
    """Return a git config settings dictionary based on input values."""
    auto_pack = {}
    if not git_autopack:
        auto_pack = {
            # Turn off garbage collection
            'gc.auto': '0',
            'gc.autopacklimit': '0',
            # git-receive-pack will not run gc
            'receive.autogc': 'false'
        }
    # If autopack is enabled and we have a value for gc.auto - set it
    elif git_gc_auto and git_gc_auto.isdigit():
        auto_pack = {'gc.auto': git_gc_auto}
    return auto_pack


def get_case_handling_settings(p4):
    """Return a git config settings dictionary with core.ignorecase set."""
    ignorecase = 'true' if p4.server_case_insensitive else 'false'
    return {'core.ignorecase': ignorecase}


def set_git_config(git_dir, settings):
    """Set the git config settings.

    Value on 'unset' causes the value to be removed.
    """
    repo = pygit2.Repository(git_dir)
    for k, v in settings.items():
        if v == 'unset':
            if k in repo.config:
                del repo.config[k]
        elif k not in repo.config or repo.config[k] != v:
            repo.config[k] = v


def install_hook(git_dir, overwrite=False, hook_abs_path=None):
    """Install Git Fusion's pre-receive hook script, if it is missing.

    :param git_dir: path to the .git directory.
    :param overwrite: if True, overwrite the hook scripts unconditionally.

    """
    # hook script names and our equivalent Python script
    hooks = {
        NTR('pre-receive'): 'p4gf_pre_receive_hook.py',
        NTR('post-receive'): 'p4gf_post_receive_hook.py',
    }
    for hook, script in hooks.items():
        hook_path = os.path.join(git_dir, NTR('hooks'), hook)
        if overwrite or not os.path.exists(hook_path):
            with open(hook_path, 'w') as f:
                f.write(hook_file_content(script, hook_abs_path))
            LOG.debug("install_hook(): writing {0}".format(hook_path))
            os.chmod(hook_path, 0o755)    # -rwxr-xr-x


def hook_file_content(script_name, abs_path=None):
    """Return the text of a script that can call the named script."""
    lines = [NTR('#! /usr/bin/env bash'),
             NTR(''),
             NTR('export PYTHONPATH={bin_dir}:$PYTHONPATH'),
             NTR('{bin_dir}/{script_name}'),
             NTR('')]

    if not abs_path:
        abs_path = os.path.abspath(__file__)
    bin_dir = os.path.dirname(abs_path)
    file_content = '\n'.join(lines).format(bin_dir=bin_dir, script_name=script_name)
    return file_content

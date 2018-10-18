#! /usr/bin/env python3.3
"""Report on Git Fusion locks."""

from contextlib import ExitStack
import json
import os
import re
import stat
import sys
import subprocess
import time

import p4gf_env_config  # pylint: disable=unused-import
import p4gf_const
import p4gf_create_p4
import p4gf_git_repo_lock
from p4gf_l10n import _
import p4gf_lock
import p4gf_log
import p4gf_p4key
import p4gf_util

LOCK_OWNERS_NAME_RE = re.compile(r'git-fusion-view-(.*)-lock-owners')
FAILED_PUSH_RE = re.compile(r'^Push (\d+) failed:.*')
SUCCESSFUL_PUSH_RE = re.compile(r'^Push (\d+) completed successfully.*')

DEAD_PROC_MSG = _('  ==> Process {pid} no longer exists.')
ACTIVE_PROC_MSG = _(
    '  ==> Process {pid} is still running. Do not release this lock until that process ends.')
FAILED_PUSH_MSG = _(
    '  ==> Most recent push {push_id} failed. The repo may need repair. Contact Perforce support.')
SOME_DEAD_AND_FAILED_PUSH_MSG = _(
    '  ==> {repo_name}: Some processes no longer exist and the most recent push '
    '{push_id} failed. The repo may need repair. Contact Perforce support.')
SOME_DEAD_AND_SUCCESSFUL_MSG = _(
    '  ==> {repo_name}: Some processes no longer exist and the most recent push '
    '{push_id} completed successfully.\n'
    '  ==> Query the status again to check if the running process completes.\n'
    '  ==> The repo may need repair. Contact Perforce support.')
ALL_DEAD_WITH_LOCK = _(
    '  ==> {repo_name}: All process no longer exists, and the most recent push '
    '{push_id} completed successfully, so it is safe to manually release these locks.')
ALL_DEAD_AND_FAILED_PROCESS = _(
    '  ==> {repo_name}: All the processes no longer exist and the most recent push '
    '{push_id} failed. The repo may need repair. Contact Perforce support.')
RELEASE_VIEW_LOCK_MSG   = _('  ==> To release the view lock:  \n         p4 key -d {}')
RELEASE_OWNERS_LOCK_MSG = _('  ==> To release the owners lock:\n         p4 key -d {}')
NEED_TO_RELEASE_MSG = _(
    '  ==> You may need to release {numkeys} key(s) after determining it is safe.')


SYSLOG         = '/var/log/syslog'
SIGKILL_MSG    = [_('send sigkill to {pid} (python3.3)')]
NOSIGKILL_MSG  = _('No kernel sigkill detected for pid {pid}.')
OOM_PREFIX     = '    ==>>    '
OOM_MSG1       = _("'sigkill' has been detected for pid {pid} in {log}.")
OOM_MSG2       = _('This sigkill was likely cause by an OutOfMemory condition.')
SUDO_MSG       = _("Using sudo to check '{log}' for sigterm message:")


def check_syslog_for_sigkill(pid, pfunc=print):
    """Display message if this dead pid has a kernel sigkill message.

    Prompt for sudo - which is necessary to read /var/log/syslog.
    """
    if not sys.stdout.isatty():
        return
    for m in SIGKILL_MSG:
        msg = m.format(pid=pid)
        try:

            pfunc("{0} {1}".format(OOM_PREFIX, SUDO_MSG.format(log=SYSLOG)))
            res = subprocess.check_output(['sudo', 'grep', msg, SYSLOG])
            res = res.decode('utf-8').rstrip()
            pfunc("{0} {1}.".format(OOM_PREFIX, OOM_MSG1.format(pid=pid, log=SYSLOG)))
            pfunc("{0} '{1}'".format(OOM_PREFIX, res))
            pfunc("{0} '{1}'".format(OOM_PREFIX, OOM_MSG2))

        except subprocess.CalledProcessError:
            pfunc("{0} {1}".format(OOM_PREFIX, NOSIGKILL_MSG.format(pid=pid)))


def _pid_status(pid):
    """Check on the liveness of a process and return 'DEAD' or 'LIVE'."""
    if not p4gf_git_repo_lock.pid_exists(pid):
        return "DEAD"
    else:
        return "LIVE"


def print_broken_p4key_lock(p4, pfunc=print):
    """Report on all of the repo locks that appear to be broken.

    :param p4: P4API to query Perforce.
    :param pfunc: either 'print' or some logger : 'LOG.debug'

    """
    # Get all of the existing lock keys, sleep briefly, then check again.
    pattern = p4gf_const.P4GF_P4KEY_LOCK_VIEW.format(repo_name='*')
    repo_locks = p4gf_p4key.get_all(p4, pattern)
    time.sleep(1)
    lock_name_re = re.compile(r'git-fusion-view-(.*)-lock')
    for name, old_value in repo_locks.items():
        new_value = p4gf_p4key.get(p4, name)
        # If the value is unchanged or increasing, that is a bad sign.
        if int(new_value) >= int(old_value):
            repo_name = lock_name_re.match(name).group(1)
            pfunc("Possibly broken repo lock: {}".format(repo_name))
            pfunc("May need to delete key {} if repo is inaccessible".format(name))


def print_p4key_lock_status(p4, server_id, pfunc=print):
    """Report on all of the repo locks and their status.

    :param p4: P4API to query Perforce.
    :param server_id: identifier for this Git Fusion instance.
    :param pfunc: either 'print' or some logger : 'LOG.debug'

    """
    # pylint: disable=too-many-branches, too-many-statements, maybe-no-member
    # Instance of 'bool' has no 'group' member
    pattern = p4gf_const.P4GF_P4KEY_LOCK_VIEW_OWNERS.format(repo_name='*')
    repo_locks = p4gf_p4key.get_all(p4, pattern)
    dead_processes_exist = False
    for name, raw_value in repo_locks.items():
        content = json.loads(raw_value)
        if "owners" not in content:
            pfunc(_("Malformed lock {lock_name}").format(lock_name=name))
        else:
            repo_name = LOCK_OWNERS_NAME_RE.match(name).group(1)
            pfunc(_("***************** {repo_name} Status  *****************")
                  .format(repo_name=repo_name))
            lock_key_name = p4gf_const.P4GF_P4KEY_LOCK_VIEW.format(repo_name=repo_name)
            have_lock_key = p4gf_p4key.get(p4, lock_key_name) != '0'
            if have_lock_key:
                pfunc(_("View lock {lock_key_name} is set")
                      .format(lock_key_name=lock_key_name))
            status_key_name = p4gf_p4key.calc_repo_status_p4key_name(repo_name, None)
            repo_status = p4gf_p4key.get(p4, status_key_name)
            pushid_key_name = p4gf_p4key.calc_repo_push_id_p4key_name(repo_name)
            repo_pushid = p4gf_p4key.get(p4, pushid_key_name)
            failed_push = False
            if repo_pushid != '0':
                pfunc(_("Most recent push for repo '{repo_name}'... '{push_id}'")
                      .format(repo_name=repo_name, push_id=repo_pushid))
            if repo_status != '0':
                pfunc(_("Most recent push status for repo '{repo_name}'... '{status}'")
                      .format(repo_name=repo_name, status=repo_status))
                failed_push = FAILED_PUSH_RE.match(repo_status)
                if failed_push:
                    repo_pushid = failed_push.group(1)
                    pfunc(FAILED_PUSH_MSG.format(push_id=repo_pushid))
                else:
                    successful_push = SUCCESSFUL_PUSH_RE.match(repo_status)
                    if successful_push:
                        repo_pushid = successful_push.group(1)

            pfunc(_("P4 key based locks for '{repo_name}'...").format(repo_name=repo_name))
            lock_server_id = content["server_id"]
            pfunc(_("  Owning instance: {server_id}").format(server_id=lock_server_id))
            pfunc(_("  Initial process: {pid}").format(pid=content["group_id"]))
            process_number = 0
            dead_process_count = 0
            for owner in content["owners"]:
                process_number += 1
                pid = owner["process_id"]
                start_time = owner["start_time"]
                status = _pid_status(pid) if lock_server_id == server_id else "UNKNOWN"
                pfunc(_("  Owner #{process_number}: PID {pid}, started at "
                        "{start_time}, status {status}")
                      .format(process_number=process_number, pid=pid,
                              start_time=start_time, status=status))
                if status == 'DEAD':
                    dead_processes_exist = True
                    dead_process_count += 1
                    pfunc(DEAD_PROC_MSG.format(pid=pid))
                    check_syslog_for_sigkill(pid, pfunc)
                else:
                    pfunc(ACTIVE_PROC_MSG.format(pid=pid))

            if dead_processes_exist:
                pfunc('\n')
                numkeys = 2 if have_lock_key else 1
                if process_number == dead_process_count:
                    if failed_push:
                        pfunc(ALL_DEAD_AND_FAILED_PROCESS
                              .format(repo_name=repo_name, push_id=repo_pushid))
                    else:
                        pfunc(ALL_DEAD_WITH_LOCK
                              .format(repo_name=repo_name, push_id=repo_pushid))
                    pfunc(NEED_TO_RELEASE_MSG.format(numkeys=numkeys))
                    if have_lock_key:
                        pfunc(RELEASE_VIEW_LOCK_MSG.format(lock_key_name))
                    pfunc(RELEASE_OWNERS_LOCK_MSG.format(name))
                else:
                    if failed_push:
                        pfunc(SOME_DEAD_AND_FAILED_PUSH_MSG
                              .format(repo_name=repo_name, push_id=repo_pushid))
                    else:
                        pfunc(SOME_DEAD_AND_SUCCESSFUL_MSG
                              .format(repo_name=repo_name, push_id=repo_pushid))
                    pfunc(NEED_TO_RELEASE_MSG.format(numkeys=numkeys))
                    if have_lock_key:
                        pfunc(RELEASE_VIEW_LOCK_MSG.format(lock_key_name))
                    pfunc(RELEASE_OWNERS_LOCK_MSG.format(name))
    if len(repo_locks):
        pfunc("")


def print_file_lock_status(pfunc=print):
    """Report on all file-based locks in this instance.

    :param pfunc: print function -  either 'print' or some logger : 'LOG.debug'
    """
    # pylint: disable=too-many-branches
    repo_names = []
    lock_dir_name = "{0}/locks".format(p4gf_const.P4GF_HOME)
    if not os.path.isdir(lock_dir_name):
        pfunc("The Git Fusion lock directory is missing: {}".format(lock_dir_name))
        return
    for entry in os.listdir(lock_dir_name):
        entry_name = os.path.join(lock_dir_name, entry)
        if stat.S_ISDIR(os.stat(entry_name).st_mode):
            repo_names.append(entry)
    for repo_name in repo_names:
        repo_lock_dir_name = os.path.join(lock_dir_name, repo_name)
        locks = dict()
        for lock in os.listdir(repo_lock_dir_name):
            if lock == "write":
                with open(os.path.join(repo_lock_dir_name, lock)) as fobj:
                    writer_pid = fobj.read()
                    locks["writer"] = writer_pid
            elif lock == "lock":
                continue
            else:
                locks.setdefault("readers", []).append(lock)
        if len(locks):
            pfunc("File based locks for '{}'...".format(repo_name))
        if "writer" in locks:
            pid = locks["writer"]
            status = _pid_status(pid)
            pfunc("  Writer PID: {}, status {}".format(pid, status))
        if "readers" in locks:
            for reader in locks["readers"]:
                status = _pid_status(reader)
                pfunc("  Reader PID: {}, status {}".format(reader, status))

    if len(locks):
        pfunc("")


def print_reviews_lock_status(p4, server_id, pfunc=print):
    """Report on the status of the reviews locks.

    :param p4: P4API to query Perforce.
    :param server_id: identifier for this Git Fusion instance.
    :param pfunc: print function -  either 'print' or some logger : 'LOG.debug'

    """
    raw_value = p4gf_p4key.get(p4, p4gf_const.P4GF_REVIEWS_COMMON_LOCK_OWNER)
    if raw_value and raw_value != '0':
        try:
            content = json.loads(raw_value)
        except ValueError:
            # 15.1 had a bug in which the key value was not properly formed JSON
            # (in fact, it was just a str()-formatted Python dict).
            pfunc("Reviews common lock invalid format.\nRaw value: {}".format(raw_value))
            return
        if 'server_id' not in content:
            pfunc("Reviews common lock malformed")
            return
        lock_server_id = content['server_id']
        pid = content['process_id']
        start_time = content['start_time']
        status = _pid_status(pid) if lock_server_id == server_id else "UNKNOWN"
        pfunc("Reviews common: server {}, PID {}, started at {}, status {}\n".format(
            lock_server_id, pid, start_time, status))


def print_reviews_status(p4, server_id, pfunc=print):
    """Report on the status of the reviews locks.

    :param p4: P4API to query Perforce.
    :param server_id: identifier for this Git Fusion instance.
    :param pfunc: print function -  either 'print' or some logger : 'LOG.debug'

    """
    reviews_users = [ p4gf_const.P4GF_REVIEWS_GF + server_id,
                      p4gf_const.P4GF_REVIEWS__NON_GF ]

    trigger_lock_msg = \
        "Git Fusion Triggers have locks on these repo Views for the indicated GF-<changelist>:"
    for user in reviews_users:
        args_ = ['-o', user]
        r = p4.run('user', args_)
        vardict = p4gf_util.first_dict(r)
        reviews_locks_exist = False
        if "Reviews" in vardict:
            reviews_locks_exist = True
            current_reviews = vardict["Reviews"]
            if current_reviews:
                if user == p4gf_const.P4GF_REVIEWS__NON_GF:
                    pfunc(trigger_lock_msg)
                    current_reviews[0] = '  ' + current_reviews[0]
                    pfunc( "{0}".format('\n  '.join(current_reviews)))
                else:
                    current_reviews[0] = '  ' + current_reviews[0]
                    pfunc("Git Fusion has locks on these repo Views:")
                    pfunc( "{0}".format('\n  '.join(current_reviews)))

    if reviews_locks_exist:
        pfunc("Reviews locks are expected to exist during normal activity.")
        pfunc("Specific locks will not persist after a completed Git Fusion push or p4 submit.")
        pfunc("Check again to see that these Review locks do not continue to exist.")


def print_space_lock_status(p4, pfunc=print):
    """Report on the status of the disk usage lock.

    :param p4: P4API to query Perforce.
    :param pfunc: print function -  either 'print' or some logger : 'LOG.debug'
    """
    raw_value = p4gf_p4key.get(p4, p4gf_const.P4GF_P4KEY_LOCK_SPACE)
    if raw_value and raw_value != '0':
        pfunc("Space lock ({}) is set".format(p4gf_const.P4GF_P4KEY_LOCK_SPACE))


def print_lock_status(p4, server_id, pfunc=print):
    """Report on the status of all known locks.

    :param p4: P4API to query Perforce.
    :param server_id: identifier for this Git Fusion instance.
    :param pfunc: print function -  either 'print' or some logger : 'LOG.debug'

    """
    print_broken_p4key_lock(p4, pfunc)
    print_p4key_lock_status(p4, server_id, pfunc)
    print_file_lock_status(pfunc)
    print_reviews_lock_status(p4, server_id, pfunc)
    print_reviews_status(p4, server_id, pfunc)
    print_space_lock_status(p4, pfunc)


def main():
    """Parse the command-line arguments and report on locks."""
    # pylint: disable=too-many-statements
    desc = _("Report the currently held locks in Git Fusion.")
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument('--test', action='store_true',
                        help=_('invoke test mode, acquire locks and report'))
    parser.add_argument('--test2', action='store_true',
                        help=_('invoke test mode, acquire locks and report, set dead processes.'))
    args = parser.parse_args()

    p4gf_util.has_server_id_or_exit()
    server_id = p4gf_util.get_server_id()
    p4 = p4gf_create_p4.create_p4_temp_client()
    if not p4:
        sys.exit(1)
    print("Connecting to P4PORT={} as P4USER={}".format(p4.port, p4.user))
    if args.test or args.test2:
        repo_name = "p4gf_test_status_repo"
        status_key_name = p4gf_p4key.calc_repo_status_p4key_name(repo_name, None)
        p4gf_p4key.set(p4, status_key_name, 'Push 1 completed successfully')
        pushid_key_name = p4gf_p4key.calc_repo_push_id_p4key_name(repo_name)
        p4gf_p4key.set(p4, pushid_key_name, '1')
        # create a process and kill it and set its dead pid as a RepoLock owner below.

        if args.test:
            # A test with nothing stale
            with ExitStack() as stack:
                stack.enter_context(p4gf_lock.ReviewsLock(p4))
                stack.enter_context(p4gf_lock.RepoLock(p4, repo_name))
                stack.enter_context(p4gf_git_repo_lock.read_lock(repo_name))
                stack.enter_context(p4gf_git_repo_lock.write_lock(repo_name, upgrade=True))
                print_lock_status(p4, server_id)

        else:  # if args.test2
            # Now a test with some DEAD processes and a stale view Lock
            dead_process = subprocess.Popen(['echo', 'x'], stdout=subprocess.DEVNULL)
            dead_process.kill()
            while dead_process.returncode is None:
                dead_process.communicate()
            lock2 = None
            with ExitStack() as stack:
                stack.enter_context(p4gf_lock.ReviewsLock(p4))
                # first lock owner
                lock1 = p4gf_lock.RepoLock(p4, repo_name)
                # second lock owner with same group_id and a dead pid
                lock2 = p4gf_lock.RepoLock(p4, repo_name, group_id=lock1.group_id)
                lock2.process_id = dead_process.pid
                # acquire the first RepoLock
                stack.enter_context(lock1)
                # Use low level method to add this DEAD pid to the group's lock owners
                lock2.do_acquire()
                stack.enter_context(p4gf_git_repo_lock.read_lock(repo_name))
                stack.enter_context(p4gf_git_repo_lock.write_lock(repo_name, upgrade=True))
                print("Test 1:")
                print_lock_status(p4, server_id)
                p4gf_p4key.set(p4, pushid_key_name, '2')
                p4gf_p4key.set(p4, status_key_name, 'Push 2 failed: some error')
                # Finally lets set the P4GF_P4KEY_LOCK_VIEW - the least likley to be stale
                p4gf_p4key.set(p4, lock2.lock_key, '1')
                print("Test 2:")
                print_lock_status(p4, server_id)
                # Cant exit the ExistStack unless we clean this
                p4gf_p4key.delete(p4, lock2.lock_key)
            # Clean up this lock so the test may be run again
            p4gf_p4key.delete(p4, lock2.owners_key)
        # remove test keys
        p4gf_p4key.delete(p4, status_key_name)
        p4gf_p4key.delete(p4, pushid_key_name)
    else:
        print_lock_status(p4, server_id)


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

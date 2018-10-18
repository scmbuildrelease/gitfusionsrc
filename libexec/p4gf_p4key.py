#! /usr/bin/env python3.3
"""Gratuitous 'p4 key' wrapper.

All of these functions take a p4_or_ctx argument, which can be either a
P4.P4 or p4gf_context.Context instance. If Context, will use that
Context.p4run() function to record the request/response with that context's
recorder. If just a P4 instance, run logged via monkey patched p4.run().

We called 'p4 counter' a LOT throughout our code. This is a pointless
attempt to collect all counter calls into a single place so that we can
switch over to the public 'p4 key' API instead of continuing to use its
never-published 'p4 counter -u' precursor.

"""

import p4gf_const
import p4gf_p4msg
import p4gf_p4msgid

from P4 import P4Exception


def _first_value_for_key(result_list, key):
    """Return the first value for dict with key.

    Copied and pasted from p4gf_util to break a circular import loop.

    """
    for e in result_list:
        if isinstance(e, dict) and key in e:
            return e[key]
    return None


def _p4_run(p4_or_ctx, *cmd):
    """Prefer Context.p4run() over P4.run() when possible."""
    try:
        return p4_or_ctx.p4run(*cmd)
    except AttributeError:
        pass
    return p4_or_ctx.run(*cmd)


def get(p4_or_ctx, key_name):
    """Return a single Perforce 'p4 key's value, or None if none found."""
    rr = _p4_run(p4_or_ctx, 'key', key_name)
    if rr:
        return _first_value_for_key(rr, 'value')
    return None


def get_all(p4_or_ctx, key_pattern):
    """Return a {key:value} dict of all 'p4 keys -e' that match the requested pattern.

    Return empty dict if no results.

    """
    rr = _p4_run(p4_or_ctx, 'keys', '-e', key_pattern)
    result = {}
    for r in rr:
        if isinstance(r, dict) and 'key' in r:
            result[r['key']] = r['value']
    return result


def set(p4_or_ctx, key_name, key_value):
    """Set the Perforce key_name to key_value."""
    # pylint:disable=redefined-builtin
    # Redefining built-in 'set'
    # Yes, because "set" is the
    # proper counterpart to "get"
    _p4_run(p4_or_ctx, 'key', key_name, key_value)


def is_set(p4_or_ctx, key_name):
    """Test if key set to something than the default string "0"."""
    return "0" != get(p4_or_ctx, key_name)


def increment(p4_or_ctx, key_name):
    """Atomic increment of a 'p4 key'.

    See acquire() if you just need to check for "1" for mutex/lock.

    """
    rr = _p4_run(p4_or_ctx, 'key', '-i', key_name)
    return _first_value_for_key(rr, 'value')


def acquire(p4_or_ctx, key_name):
    """Acquire a lock via 'p4 key -i' atomic increment.

    Return True if caller now exclusively owns this key and all it
    controls. Caller must call delete() when done.

    Return False if some other process owns this key and all it controls.

    """
    try:
        value = increment(p4_or_ctx, key_name)
        return "1" == value
    except P4Exception:
        # incrementing a non-integer value raises without modifying the p4key
        # that's expected and not a real error, just a failure to acquire
        if p4gf_p4msg.find_msgid(p4_or_ctx, p4gf_p4msgid.MsgServer_KeyNotNumeric):
            return False
        raise


def delete(p4_or_ctx, key_name):
    """Delete a single 'p4 key'.

    Return True if deleted, False if not.

    """
    r = _p4_run(p4_or_ctx, 'key', '-d',  key_name)
    if _first_value_for_key(r, 'key'):
        return True
    else:
        return False


def get_counter(p4_or_ctx, counter_name):
    """Retrieve a key value, or None if not defined.

    This is the only function allowed to call 'p4 counter'. Use this solely
    for non-key non-dash-u counters such as the server's security level or
    changelist counter.

    """
    # The double-tick '' here breaks up the prohibited command and gets
    # past the Ministry of Truth without being caught and sent to room 101.
    rr = _p4_run(p4_or_ctx, 'cou''nter', counter_name)
    if rr:
        return _first_value_for_key(rr, 'value')
    return None


def calc_last_copied_change_p4key_name(repo_name, server_id):
    """Return the name of the key that holds the highest copied changelist number."""
    return p4gf_const.P4GF_P4KEY_LAST_COPIED_CHANGE.format(
        repo_name=repo_name, server_id=server_id)


def calc_repo_status_p4key_name(repo_name, push_id=None):
    """Return the name for the repo-specific status key.

    :type repo_name: str
    :param repo_name: name of the repository for which status will be assessed.

    :type push_id: str
    :param push_id: push identifier to further narrow status (defaults to None).

    :rtype: str
    :return: name of p4 key.

    """
    if push_id:
        return p4gf_const.P4GF_P4KEY_REPO_STATUS_PUSH_ID.format(
            repo_name=repo_name, push_id=push_id)
    return p4gf_const.P4GF_P4KEY_REPO_STATUS.format(repo_name=repo_name)


def calc_repo_push_id_p4key_name(repo_name):
    """Return the name of the key that holds the push id of the given repo."""
    return p4gf_const.P4GF_P4KEY_REPO_PUSH_ID.format(repo_name=repo_name)


def set_many(ctx, key_value):
    """ 'p4 key -m key1 value1, key2 value2, ... '

    Bulk-set several keys at once.
    """
    cmd = ['key', '-m']
    for k, v in key_value.items():
        cmd.append(k)
        cmd.append(v)
    r = ctx.p4gfrun(cmd)
    return r

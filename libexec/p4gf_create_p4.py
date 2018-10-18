#! /usr/bin/env python3.3
"""Create a new P4.P4() instance."""

from contextlib import contextmanager
import logging
import os
import sys
import uuid

import P4
import p4gf_const
import p4gf_p4key
from   p4gf_l10n import _, NTR
import p4gf_log
import p4gf_repo_dirs
import p4gf_util
import p4gf_version_3

LOG = logging.getLogger(__name__)
# debug3 = create/connect/destroy tracking for leaked connections.
#          If enabled, p4_connect() sleeps for a few seconds after
#          each p4.connect(), to make it easiser to line up timestamps between
#          Git Fusion debug logs with Perforce server or proxy logs.
#          Also dumps stack traces to each call to p4_connect() so you can
#          see who is responsible.

# Every known connection we've created. So that we can close them when done.
_CONNECTION_LIST = []


def create_p4(port=None, user=None, client=None, connect=True, warn_no_client=True):
    """Return a new P4.P4() instance.

    Its prog will be set to 'P4GF/2012.1.PREP-TEST_ONLY/415678 (2012/04/14)'.

    By default the P4 is connected; call with connect=False to skip connection.

    There should be NO bare calls to P4.P4().
    """
    if 'P4PORT' in os.environ:
        LOG.debug("os.environment['P4PORT'] %s", os.environ['P4PORT'])
    p4 = P4.P4()
    LOG.debug("default port = %s, requested port = %s", p4.port, port)

    p4.prog = p4gf_version_3.as_single_line()
    p4.exception_level = P4.P4.RAISE_ERRORS

    if port:
        p4.port = port
    if user:
        p4.user = user
    else:
        p4.user = p4gf_const.P4GF_USER
    if client:
        p4.client = client
    elif connect and warn_no_client:
        # connecting without an explicit client is to be avoided
        caller = p4gf_log.caller(depth=2)
        LOG.warning("create_p4(): client not provided: {}:{}/{}()".format(
            caller['file'], caller['line'], caller['func']))

    _CONNECTION_LIST.append(p4)

    if connect:
        try:
            p4_connect(p4)

        except P4.P4Exception as e:
            LOG.exception('failed to connect to %s', p4)
            sys.stderr.write(_('error: cannot connect, p4d not running?\n'))
            sys.stderr.write(_('Failed P4 connect: {error}').format(error=str(e)))
            return None
        p4gf_version_3.p4d_version_check(p4)

    return p4


def p4_connect(p4):
    """Route ALL calls to p4.connect() through this function so we track who
    connected from where. Who created the leaking connection?
    """
    cm = p4.connect()
    LOG.debug2('p4_connect()    : %s, %s', id(p4), p4)
    # Ensure we speak to the commit server in a cluster environment.
    # Note that this must be done before any commands are run, since
    # by that point the protocol has already been established.
    #
    # Note the extra empty string arugment: this is the difference between
    # -zroute and -Zroute (without the second argument, -z is used, which
    # is wrong).
    #
    p4.protocol('route', '')
    return cm


def p4_disconnect(p4):
    """Route ALL calls to p4.disconnect() through this function so we track who
    disconnected from where. Who created the leaking connection?
    """
    # Log the p4 instance before disconnecting so we can check the
    # connection status in the log.
    LOG.debug2('p4_disconnect() : %s, %s', id(p4), p4)
    cm = p4.disconnect()
    return cm


def close_all():
    """Close every connection we created."""
    for p4 in _CONNECTION_LIST:
        try:
            LOG.debug3('close_all()     : %s, %s', id(p4), p4)
            if p4.connected():
                p4_disconnect(p4)

            # Catching too general exception Exception
            # This is cleanup code. If we fail, that's okay. At worst we
            # leave a connection around for a few more seconds.
        except Exception:  # pylint: disable=broad-except
            LOG.exception('closing p4 connections failed')

    del _CONNECTION_LIST[:]


def _count_active(prefix):
    """Return the number of open connections where the client name matches the prefix."""
    result = 0
    for p4 in _CONNECTION_LIST:
        if p4.connected():
            if p4.client.startswith(prefix):
                result += 1
    return result


# def log_connections(prefix):
#     """Report the list of p4 connections to the log."""
#     if _CONNECTION_LIST:
#         if LOG.isEnabledFor(logging.DEBUG2):
#             for p4 in _CONNECTION_LIST:
#                 LOG.debug2("%s: known connection: %s, %s", prefix, id(p4), p4)
#         else:
#             LOG.debug("%s: %s known connections", prefix, len(_CONNECTION_LIST))
#     else:
#         LOG.debug("%s: no known connections", prefix)


def destroy(p4):
    """Disconnect and unregister and delete."""
    LOG.debug3('destroy()       : %s, %s', id(p4), p4)
    if p4.connected():
        p4_disconnect(p4)
    unregister(p4)
    del p4


def unregister(p4):
    """Some code is smart enough to close and destroy its own P4 connection.

    Let go of the object so that it can leave our heap.
    """
    LOG.debug3('unregister()    : %s, %s', id(p4), p4)
    assert not p4.connected()    # Require that the caller really did disconnect.
    if p4 in _CONNECTION_LIST:
        _CONNECTION_LIST.remove(p4)


def create_p4_temp_client(port=None, user=None, skip_count=False):
    """Create a connected P4 instance with a generic temporary client.

    Dropping the connection will automatically delete the client. This is
    useful for owning the locks and permitting reliable lock stealing by
    other processes.

    :return: P4API instance.

    """
    p4 = create_p4(port, user, warn_no_client=False)
    if p4 is None:
        # Propagate the error (that has already been reported).
        return None
    name = p4gf_const.P4GF_OBJECT_CLIENT_UNIQUE.format(server_id=p4gf_util.get_server_id(),
                                                       uuid=str(uuid.uuid1()))
    client = p4.fetch_client(name)
    client['Owner'] = p4gf_const.P4GF_USER
    client['LineEnd'] = NTR('unix')
    client['View'] = ['//{0}/... //{1}/...'.format(p4gf_const.P4GF_DEPOT, name)]
    # to prevent the mirrored git commit/tree objects from being retained in the
    # git-fusion workspace, set client option 'rmdir' and sync #none in p4gf_gitmirror
    client['Options'] = p4gf_const.CLIENT_OPTIONS.replace("normdir", "rmdir")
    client['Root'] = p4gf_const.P4GF_HOME
    # The -x option is a deep undoc feature that signals to p4d that this
    # is a temporary client, which will be automatically deleted upon
    # disconnect. Requires passing the client specification using -i flag.
    # N.B. this client cannot shelve changes. See @465851 for details.
    # N.B. only one temporary client per connection will be auto-deleted
    p4.client = name
    p4.save_client(client, '-x')
    LOG.debug("create_p4_temp_client() created temp client %s", name)
    if 'P4T4TEST_ORIG_LANG' in os.environ and not skip_count:
        # In the testing environment, we check that each process created no
        # more than one temporary client (concurrently). Keep the highest
        # value, rather than whatever the last count happened to be.
        prefix = p4gf_const.P4GF_OBJECT_CLIENT_UNIQUE.format(server_id=p4gf_util.get_server_id(),
                                                             uuid='')
        count = _count_active(prefix)
        # Some tests set up an unnatural environment, so don't let those
        # blow up in unexpected ways (i.e. fail gracefully elsewhere).
        with p4.at_exception_level(P4.P4.RAISE_NONE):
            value = p4gf_p4key.get(p4, "git-fusion-temp-clients-{}".format(os.getpid()))
            if int(value) < count:
                p4gf_p4key.set(p4, "git-fusion-temp-clients-{}".format(os.getpid()), str(count))
    return p4


def create_p4_repo_client(port, user, repo_name):
    """Create a connected P4 instance with temporary repo client.

    Note that this client cannot shelve changes. Use the client pool for that.

    :param str port: P4PORT value
    :param str user: P4USER value
    :param str repo_name: repository name
    :return: P4API instance

    """
    p4 = create_p4(port, user, warn_no_client=False)
    if p4 is None:
        # Propagate the error (that has already been reported).
        return None
    name = p4gf_const.P4GF_REPO_CLIENT_UNIQUE.format(repo_name=repo_name, uuid=str(uuid.uuid1()))
    repo_dirs = p4gf_repo_dirs.from_p4gf_dir(p4gf_const.P4GF_HOME, repo_name)
    client = p4.fetch_client(name)
    client['LineEnd'] = NTR('unix')
    client['Options'] = p4gf_const.CLIENT_OPTIONS
    client['Owner'] = p4gf_const.P4GF_USER
    client['Root'] = repo_dirs.p4root
    # The view for this client is basically irrelevant, anything will do.
    client['View'] = ['//{0}/... //{1}/...'.format(p4gf_const.P4GF_DEPOT, name)]
    # The -x option is a deep undoc feature that signals to p4d that this
    # is a temporary client, which will be automatically deleted upon
    # disconnect. Requires passing the client specification using -i flag.
    # N.B. this client cannot shelve changes. See @465851 for details.
    # N.B. only one temporary client per connection will be auto-deleted
    p4.client = name
    p4.save_client(client, '-x')
    LOG.debug("Successfully created Git Fusion client %s for %s", name, repo_name)
    return p4


class Connector:

    """RAII object that connects and disconnects a P4 connection."""

    def __init__(self, p4):
        self.p4 = p4

    def __enter__(self):
        p4_connect(self.p4)
        return self.p4

    def __exit__(self, _exc_type, _exc_value, _traceback):
        p4_disconnect(self.p4)
        return False  # False == do not squelch any current exception


class Closer:

    """RAII object that closes all P4 connections on exit."""

    def __init__(self):
        pass

    def __enter__(self):
        return None

    def __exit__(self, _exc_type, _exc_value, _traceback):
        """Close all registered connections."""
        close_all()
        return False  # False == do not squelch any current exception


@contextmanager
def routeless(p4):
    """Change a connection protocol to unrouted.

    Special server cluster handling of the client spec, in which we create
    a connection matching the one given, but without the 'route' setting.
    The purpose is to ensure we retrieve client specifications correctly.

    :param p4: template P4 connection (unchanged).
    :return: new default P4 connection.

    """
    new_p4 = create_p4(port=p4.port, user=p4.user, client=p4.client, connect=False)
    # connect without using p4_connect() so we do *not* have the 'route' set
    new_p4.connect()
    try:
        yield new_p4
    finally:
        p4_disconnect(new_p4)

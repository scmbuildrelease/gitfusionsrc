#! /usr/bin/env python3.3
"""p4gf_client_pool.

A pool of temporary Perforce client spec objects that we can use when querying
Perforce for files within a defined view, usually a single branch view.
"""
import logging
import os
import uuid

import p4gf_branch
import p4gf_const
from p4gf_l10n import _, NTR
import p4gf_path
import p4gf_util
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_p4spec

LOG = logging.getLogger(__name__)
HM_LOG = LOG.getChild("hit_miss")


def _to_key(view):
    """Return hash of stream name or view lines."""
    if type(view) is str:
        return hash(view)
    return sum(hash(line) for line in view)


class ClientPool:

    """A pool of temporary Perforce client spec objects.

    We use these when querying Perforce for files within a defined view,
    usually a single branch view.

    We query the same set of branches over and over: each time we switch a client
    view, that's a write to the db.view table. Rather than switch a single client's
    view over and over, create a pool of clients, each switched to a view, and use
    the appropriate client for the query.

    Use for_view() or for_stream() to get a configured client to use, and
    call release_client() when you're done with it.  If for_xxx() is called
    more than once for the same view without an intervening call to
    release_client(), the same client will be returned each time.  each
    call to for_xxx() must be balanced by a matching call to release_client().

    Deletes clients upon exit.
    """

    class TempClient:

        """Track usage of a temp client.

        If client is currently being used, refcount is the number of current
        users.

        If client is not being used, refcount is zero or a negative value
        indicating the number of times a different client has been referenced
        since the last ref to this client was released.
        """

        def __init__(self, name, client_view_map, client_root):
            """Init with refcount of 1."""
            self.name = name
            self.client_view_map = client_view_map
            self.client_root = client_root
            self.refcount = 1

        def add_ref(self):
            """Add a ref."""
            if self.refcount < 0:
                self.refcount = 0
            self.refcount += 1

        def remove_ref(self):
            """Remove a ref."""
            assert self.refcount > 0
            self.refcount -= 1

        def age_if_unused(self):
            """Decrement refcount of unused clients."""
            if self.refcount < 1:
                self.refcount -= 1

        def __str__(self):
            """Return a string representation of self."""
            return "[name: {}, refcount: {}]".format(self.name, self.refcount)

    def __init__(self, ctx):
        """Init the pool."""
        self.ctx = ctx

        # TempClient keyed by hash of view lines or stream name
        self.clients = {}

        # use root dir for permanent client as prefix for root dirs for temporary clients
        self.local_root_prefix = p4gf_path.strip_trailing_delimiter(self.ctx.repo_dirs.p4root)

        self._miss_ct = 0
        self._hm_log('init', None, None)

    def matches_view(self, client, view_lines):
        """return True if the named temp client is currently configured with view_lines.

        used to detect when a view change can be avoided
        """
        key = _to_key(view_lines)
        return key in self.clients and self.clients[key].name == client

    def for_view(self, view_lines):
        """Return the name of a Perforce client spec that has the requested view."""
        return self._acquire_client(view_lines)

    def for_stream(self, stream_name):
        """Retrieve a client for the named stream."""
        return self._acquire_client(stream_name)

    def release_client(self, client_name):
        """Decrement the ref count of the named client."""
        for client in self.clients.values():
            if client.name == client_name:
                client.remove_ref()
                LOG.debug("release_client, releasing {}".format(client))
                return
        raise RuntimeError(_("Can't release unknown client {client_name}")
                           .format(client_name=client_name))

    def get_client_with_name(self, client_name):
        """Return the temp client for the named client.

        This should be used only on referenced clients.
        """
        for client in self.clients.values():
            if client.name == client_name and client.refcount > 0:
                return client
        raise RuntimeError(_("Can't get view_map from unknown or unreferenced client {client_name}"
                           .format(client_name=client_name)))

    def cleanup(self):
        """Clear the references to the temporary clients."""
        LOG.debug2("cleanup() removing our temp clients...")
        for c in self.clients.values():
            if c.refcount > 0:
                LOG.error("client pool cleanup called with client still in use: %s", c.name)
        for client in self.clients.values():
            self.ctx.p4gfrun('client', '-d', '-f', client.name)
            LOG.debug("removing temp client {0}".format(client.name))
        LOG.debug2("cleanup() removed %s temp clients", len(self.clients))
        self.clients.clear()

    def cleanup_all(self):
        """Find and delete all temporary clients for the associated repo.

        Assumes the caller has the lock on the repository.

        """
        LOG.debug2("cleanup_all() removing all temp clients...")
        pattern = p4gf_const.P4GF_REPO_TEMP_CLIENT.format(
            server_id=p4gf_util.get_server_id(), repo_name=self.ctx.config.repo_name, uuid='*')
        clients = self.ctx.p4gfrun('clients', '-e', pattern)
        for client in clients:
            client_name = client['client']
            self.ctx.p4gfrun('client', '-d', '-f', client_name)
            LOG.debug("removing temp client {0}".format(client_name))
        LOG.debug2("cleanup_all() removed %s temp clients", len(clients))

    @staticmethod
    def _view_lhs0(view):
        """Helper for _hm_log() to return the first lhs half of a view."""
        line0 = view[0].strip()
        lhs0 = line0.split()[0]
        return lhs0

    def _hm_log(self, pre, client, view=None):
        """One-line debugging details when tracking pool hit/miss/recycle."""
        if not HM_LOG.isEnabledFor(logging.DEBUG3):
            return
        if view:
            v = self._view_lhs0(view)
        elif client and client.client_view_map:
            v = self._view_lhs0(client.client_view_map.as_array())
        else:
            v = ""
        if client:
            rc = client.refcount
        else:
            rc = ""

        HM_LOG.debug3("{pre:<10s} {miss_ct:>4d} misses   ref_ct={ref_ct:<4}  {view}"
                      .format( pre     = pre
                             , ref_ct  = rc
                             , view    = v
                             , miss_ct = self._miss_ct
                             ))

    def _acquire_client(self, view):
        """Acquire a client for the given view."""
        # view is either a stream name or a list of view lines
        self._hm_log('acquire ', None, view)
        if type(view) is str:
            set_func = self._set_stream
            create_func = self._create_client_for_stream
        else:
            set_func = self._set_view_lines
            create_func = self._create_client_for_view_lines

        key = _to_key(view)

        # age any currently unreferenced clients
        self._age_clients()

        # Already have one?
        if key in self.clients:
            client = self.clients[key]
            client.add_ref()
            self._hm_log("hit", client)
            return client.name

        self._miss_ct += 1
        self._hm_log("miss", client=None, view=view)

        # Do we have room to create a new client?
        if len(self.clients) < p4gf_const.MAX_TEMP_CLIENTS:
            (client_name, client_map, root) = create_func(view)
            self.clients[key] = ClientPool.TempClient(client_name,
                                                      client_map,
                                                      root)
            return client_name

        # No room. Recycle an old client.
        # will raise if all clients are in use.  If this happens, either
        # the thread limit is too high or the client limit is too low.
        client = self._recycled_client()
        client.add_ref()
        client.client_view_map = set_func(client.name, view)
        self.clients[key] = client
        self._hm_log("reset", client)
        return client.name

    def _age_clients(self):
        """Increase the age of any unused clients."""
        for c in self.clients.values():
            c.age_if_unused()

    def _recycled_client(self):
        """Return least recently used client, removing it from self.clients.

        Return None if all clients are currently in use.
        """
        oldest = None
        oldest_key = None
        for k, c in self.clients.items():
            if c.refcount < 1 and (not oldest or c.refcount < oldest.refcount):
                oldest = c
                oldest_key = k
                self._hm_log("r=", c)
            else:
                self._hm_log("r no", c)
        self._hm_log("recyc", oldest)

        if oldest_key:
            del self.clients[oldest_key]
            return oldest
        raise RuntimeError(_("No client available to recycle."))

    def _create_client_for_view_lines(self, view_lines):
        """Create a new client spec with the requested view_lines.

        Return its name.
        """
        if not len(view_lines):
            raise RuntimeError(_("Can't create client with empty view."))

        client_name, client_root, desc = self._create_client_name_root_desc()

        # Replace RHS lines with new client name.
        new_view_map = p4gf_branch.replace_client_name(
            view_lines, self.ctx.config.p4client, client_name)

        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('_create_client_for_view_lines() name={} view={}'
                       .format(client_name, new_view_map.as_array()))
        else:
            LOG.debug2('_create_client_for_view_lines() name={}'
                       .format(client_name))
        try:
            _create_temporary_client(
                self.ctx.p4gf, client_name, client_root, desc, view_map=new_view_map)
        except Exception as e:  # pylint: disable=broad-except

            # If git-fusion-user does not have write permission to the repo view paths
            # then 'p4 client -i' command fails with:
            #   Error in client specification. Mapping '%depotFile%' is not under '%prefix%'."
            # This is caused by the protects table not granting write permissions to those views.
            # Return a message indicating the permissions failure and the Exception's message.
            if p4gf_p4msg.find_msgid(self.ctx.p4gf, p4gf_p4msgid.MsgDm_MapNotUnder):
                LOG.error(_("git-fusion-user not granted sufficient privileges.\n"
                            "          Check the P4 protects entry for git-fusion-user.\n"
                            "          The IP field must match the IP set in P4PORT={p4port}")
                          .format(p4port=os.environ['P4PORT']))
                raise RuntimeError(_("Perforce: git-fusion-user not granted sufficient privileges."
                                     " Please contact your admin.\n")) from e
            else:
                raise

        return (client_name, new_view_map, client_root)

    def _create_client_for_stream(self, stream_name):
        """Create a new client spec with the requested stream.

        Return its name.
        """
        client_name, client_root, desc = self._create_client_name_root_desc()

        LOG.debug2('_create_client_for_stream() name={} stream={} root={}'
                   .format(client_name, stream_name, client_root))
        _create_temporary_client(
            self.ctx.p4gf, client_name, client_root, desc, stream_name=stream_name)
        return (client_name, None, client_root)

    def _set_view_lines(self, client_name, view_lines):
        """Change an existing client's view."""
        new_view_map = p4gf_branch.replace_client_name(
            view_lines, self.ctx.config.p4client, client_name)

        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3('_set_view_lines() name={} view={}'
                       .format(client_name, new_view_map.as_array()))
        else:
            LOG.debug2('_set_view_lines() name={}'
                       .format(client_name))
        p4gf_p4spec.ensure_spec_values(self.ctx.p4gf,
                                       spec_type='client',
                                       spec_id=client_name,
                                       values={'View': new_view_map.as_array(),
                                               'Stream': None})
        # return the view_map to set into the TempClient
        return new_view_map

    def _set_stream(self, client_name, stream_name):
        """Change an existing client to use a stream."""
        LOG.debug2('_set_stream() name={} stream={}'
                   .format(client_name, stream_name))
        self.ctx.p4gfrun('client', '-f', '-s', '-S', stream_name, client_name)

        # return the view map - which for stream in yet unknown
        return None

    def _create_client_name_root_desc(self):
        """Return a tuple of (temp client name, root dir, description).

        Used to create temp clients.

        Name is of the form: "git-fusion-{server_id}-{repo_name}-temp-{uuid}".
        Ensure root dir exists.
        """
        desc = (_("Created by Perforce Git Fusion for queries in '{view}'.")
                .format(view=self.ctx.config.repo_name))
        n = len(self.clients) + 1
        # There is a slight chance we could collide here with other
        # processes operating within the same root directory. But, as long
        # as that work is happening within the p4key lock, we should be
        # okay. We are mostly concerned with having unique client names.
        client_root = "{}-temp-{}/".format(self.ctx.repo_dirs.p4root, n)
        if not os.path.exists(client_root):
            os.makedirs(client_root)
        client_name = p4gf_const.P4GF_REPO_TEMP_CLIENT.format(server_id=p4gf_util.get_server_id(),
                                                              repo_name=self.ctx.config.repo_name,
                                                              uuid=uuid.uuid1())
        return (client_name, client_root, desc)


def _create_temporary_client(p4, client_name, client_root, desc, view_map=None,
                             stream_name=None):
    """Create a new temporary client with the given name.

    :param p4: P4 connection.
    :param str client_name: name of client to create.
    :param str client_root: value for Root of client.
    :param str desc: value for Description of client.
    :param view_map: value for View of client (if not stream based).
    :param str stream_name: value for Stream of client (if stream based).

    """
    # pylint:disable=too-many-arguments
    client = p4.fetch_client(client_name)
    client['LineEnd'] = NTR('unix')
    client['Options'] = p4gf_const.CLIENT_OPTIONS
    client['Owner'] = p4gf_const.P4GF_USER
    if view_map:
        client['View'] = view_map.as_array()
    elif stream_name:
        client['Stream'] = stream_name
    client['Root'] = client_root
    client.pop('Host')
    client['Description'] = desc
    saved_client = p4.client
    p4.client = client_name
    p4.save_client(client)
    p4.client = saved_client
    LOG.debug("created temp client {0}".format(client_name))

#! /usr/bin/env python3.3
"""Path conversion utilities to go between depot, client, Git work tree, and
local filesystem formats.


X.to_depot()  : return a single path in depot syntax     : '//depot/file'
X.to_client() : return a single path in client syntax    : '//myclient/file'
X.to_gwt()    : return a single path in Git work tree    : 'file'
X.to_local()  : return a single path in local filesystem : '/User/bob/file'

Paths in depot and client syntax are escaped: The evil chars @#%* are
converted to %-escape sequences.

Paths in Git work tree and local syntax are unescaped: %-escaped sequences
are converted to the evil chars @#%*.

Depot/client conversion requires a P4.Map (aka MapApi) object.
gwt/local conversion requires a client root.

Usually you'll use Context to create the object, then convert. Something like this:

    depot_file = ctx.gwt_path(blob['file']).to_depot()

"""

import logging
import os
import re

import P4

from p4gf_util import escape_path, unescape_path

LOG = logging.getLogger(__name__)


class BasePath:

    """Base class to hold what we need and cover some common conversions.

    You MUST override either to_depot() or to_client(). Failure to do so
    will result in infinite loops. Even on really fast CPUs.
    """

    def __init__(self, p4map, client_name, client_root, path):
        self.p4map       = p4map
        self.client_name = client_name
        self.client_root = client_root  # Must not end in trailing delimiter / .
        self.path        = path         # Syntax unknown by Base,
                                        # known by derived class.

    def to_depot(self):
        """Return path in depot syntax, escaped.

        Suitable for use with Perforce commands
        except for 'p4 add' of evil @#%* chars.
        """
        ### Zig knows it's possible to have a single RHS map to multiple LHS.
        ### The P4 C API returns a collection of strings here.
        ### Why does the P4Python API return only a single string?
        return self.p4map.translate(self.to_client(), self.p4map.RIGHT2LEFT)

    def to_client(self):
        """Return path in client syntax, escaped.

        Suitable for use with Perforce commands
        except for 'p4 add' of evil @#%* chars.
        """
        return self.p4map.translate(self.to_depot())

    def to_gwt(self):
        """Return path relative to client root, unescaped.

        Suitable for use with Git and some filesystem operations
        as long as current working directory is GIT_WORK_TREE.
        """
        c = self.to_client()
        if not c:
            return None
        c_rel_esc = c[3+len(self.client_name):]
        return unescape_path(c_rel_esc)

    def to_local(self):
        """Return absolute path in local filesystem syntax, unescaped.

        Suitable for use in all filesystem operations.
        """
        gwt = self.to_gwt()
        if not gwt:
            return None
        return os.path.join(self.client_root, gwt)


class ClientPath(BasePath):

    """A path in client syntax: //myclient/foo."""

    def __init__(         self, p4map, client_name, client_root, path):
                        # Why set_client_path_client() here?  Force client path
                        # to start with current client_name. The results
                        # sometimes come in from commands run with a different
                        # client. We need the client to match whatever p4map
                        # uses.
        BasePath.__init__( self, p4map, client_name, client_root
                         , set_client_path_client(path, client_name))

    def to_client(self):
        return self.path


class DepotPath(BasePath):

    """A path in depot syntax: //depot/foo."""

    def __init__(         self, p4map, client_name, client_root, path):
        BasePath.__init__(self, p4map, client_name, client_root, path)

    def to_depot(self):
        return self.path


class GWTPath(BasePath):

    """A path in Git Work Tree syntax."""

    def __init__(         self, p4map, client_name, client_root, path):
        BasePath.__init__(self, p4map, client_name, client_root, path)

    def to_gwt(self):
        return self.path

    def to_client(self):
        gwt_esc = escape_path(self.path)
        return '//{}/'.format(self.client_name) + gwt_esc


class LocalPath(BasePath):

    """An absolute path in local filesystem syntax."""

    def __init__(         self, p4map, client_name, client_root, path):
        BasePath.__init__(self, p4map, client_name, client_root, path)

    def to_gwt(self):
        return self.path[1 + len(self.client_root):]

    def to_local(self):
        return self.path

    def to_client(self):
        gwt_esc = escape_path(self.to_gwt())
        return '//{}/'.format(self.client_name) + gwt_esc


def view_map_to_client_name(view):
    """Return the "myclient" portion of the first line in a client view
    mapping "//depot/blah/... //myclient/blah/..."
    """
    p4map = P4.Map(view)
    LOG.debug("view='{}'".format(view))
    LOG.debug("rhs={}".format(p4map.rhs()))
    m = re.search('//([^/]+)/', p4map.rhs()[0])
    if (not m) or (not 1 <= len(m.groups())):
        return None
    return m.group(1)


def convert_view_to_no_client_name(view):
    """Convert a view mapping's right-hand-side from its original client
    name to a new client name:

        //depot/dir/...  //client/dir/...
        //depot/durr/... //client/durr/...

    becomes

        //depot/dir/...  dir/...
        //depot/durr/... durr/...
    """
    if not view:
        return []
    old_client_name = view_map_to_client_name(view)

    old_map = P4.Map(view)
    lhs = old_map.lhs()
    old_prefix = '//{}/'.format(old_client_name)
    new_prefix = ''
    rhs = [r.replace(old_prefix, new_prefix) for r in old_map.rhs()]
    new_map = P4.Map()
    for (l, r) in zip(lhs, rhs):
        new_map.insert(l, r)
    return '\n'.join(new_map.as_array())


def set_client_path_client(path, client_name=''):
    """Return result of replacing client name in client path with client_name."""
    p = re.compile('^//[^/]*/')
    replaced = re.sub(p, '//{}/'.format(client_name), path)
    return replaced


def strip_client_path_client(path):
    """Return result of stripping client name from client path."""
    return set_client_path_client(path)

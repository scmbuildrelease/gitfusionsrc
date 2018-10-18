#! /usr/bin/env python3.3
"""Functions related to Perforce specs (e.g. clients)."""

from contextlib import ExitStack
import logging

import p4gf_create_p4
import p4gf_const
from p4gf_l10n import _, NTR
import p4gf_util

LOG = logging.getLogger(__name__)


def _ids_equal_sensitive(id1, id2):
    """Case sensitive spec id equality test."""
    return id1 == id2


def _ids_equal_insensitive(id1, id2):
    """Case insensitive spec id equality test."""
    return id1.lower() == id2.lower()


def _spec_in_list(p4, spec_info, spec_list, spec_id):
    """Return True if spec_id exists in spec_list."""
    id_key = spec_info['id_list']
    if p4.server_case_insensitive:
        eq = _ids_equal_insensitive
    else:
        eq = _ids_equal_sensitive
    return any(eq(spec[id_key], spec_id) for spec in spec_list)


def _spec_exists_by_list_scan(p4, spec_info, spec_id):
    """"Table scan for an exact match of id."""
    # Scanning for users when there are NO users? p4d returns ERROR "No such
    # user(s)." instead of an empty result. That's not an error to us, so
    # don't raise it.
    with p4.at_exception_level(p4.RAISE_NONE):
        return _spec_in_list(p4, spec_info, p4.run(spec_info['cmd_list']), spec_id)


def _spec_exists_by_e(p4, spec_info, spec_id):
    """run 'p4 clients -e <name>' to test for existence."""
    return _spec_in_list(p4, spec_info, p4.run(spec_info['cmd_list'], '-e', spec_id), spec_id)


def _spec_exists_by_F(p4, spec_info, spec_id):  # pylint: disable=invalid-name
    """run 'p4 streams -F "Stream=<name>"' to test for existence."""
    spec_list = p4.run(spec_info['cmd_list'], '-F', "{}={}".format(spec_info['id_list'], spec_id))
    return _spec_in_list(p4, spec_info, spec_list, spec_id)

# Instructions on how to operate on a specific spec type.
#
# How do we get a single user? A list of users?
# How do we determine whether a spec already exists?
#
# Fields:
#     cmd_one         p4 command to fetch a single spec: 'p4 client -o'
#                     (the '-o' is implied, not part of this value)
#     cmd_list        p4 command to fetch a list of specs: 'p4 clients'
#     id_one          dict key that holds the spec ID for results of cmd_one: 'Client'
#     id_list         dict key that holds the spec ID for results of cmd_list: 'client'
#     test_exists     function that tells whether a single specific spec
#                     already exists or not
SpecInfo = NTR({
    'client':  {'cmd_one':     'client',
                'cmd_list':    'clients',
                'id_one':      'Client',
                'id_list':     'client',
                'test_exists': _spec_exists_by_e},
    'depot':   {'cmd_one':     'depot',
                'cmd_list':    'depots',
                'id_one':      'Depot',
                'id_list':     'name',
                'test_exists': _spec_exists_by_list_scan},
    'protect': {'cmd_one':     'protect',
                'cmd_list':    None,
                'id_one':      None,
                'id_list':     None,
                'test_exists': None},
    'user':    {'cmd_one':     'user',
                'cmd_list':    ['users', '-a'],
                'id_one':      'User',
                'id_list':     'User',
                'test_exists': _spec_exists_by_list_scan},
    'group':   {'cmd_one':     'group',
                'cmd_list':    'groups',
                'id_one':      'Group',
                'id_list':     'group',
                'test_exists': _spec_exists_by_list_scan},
    'stream':  {'cmd_one':     'stream',
                'cmd_list':    'streams',
                'id_one':      'Stream',
                'id_list':     'Stream',
                'test_exists': _spec_exists_by_F},
})


def spec_exists(p4, spec_type, spec_id):
    """Return True if the requested spec already exists, False if not.

    Raises KeyError if spec type not known to SpecInfo.
    """
    si = SpecInfo[spec_type]
    return si['test_exists'](p4, si, spec_id)


def _to_list(x):
    """Convert the argument to a list, if it is not already.

    Convert a set_spec() args value into something you can += to a list to
    produce a longer list of args. A list is fine, pass through unchanged.
    But a string must first be wrapped as a list, otherwise it gets
    decomposed into individual characters, and you really don't want '-f'
    to turn into ['-', 'f']. That totally does not work in 'p4 user -i -f'.

    No support for other types.
    """
    cases = {
        str: lambda t: [t],
        list: lambda t: t
    }
    return cases[type(x)](x)


def set_spec(p4, spec_type, spec_id=None, values=None, args=None, cached_vardict=None):
    """Create a new spec with the given ID and values.

    :type p4: :class:`P4API`
    :param p4: P4 instance for setting spec

    :param str spec_type: type of specification (e.g. 'client')

    :param str spec_id: name of fetch+set

    :type values: dict
    :param values: spec values to set

    :type args: str or list
    :param args: additional flags to pass for set

    :param dict cached_vardict:
            A dict returned by a prior call to set_spec(). Saves us a call to
            '<spec> -o' to fetch the contents of the spec before modifying it.
            CHANGED IN PLACE. If you don't want the dict modified,
            pass us a copy.

    :return: the vardict used as input to <spec> -i
    :rtype: dict

    :raises KeyError: if spec_type not known to SpecInfo.

    """
    # pylint: disable=too-many-arguments
    si = SpecInfo[spec_type]
    _args = ['-o']
    if spec_id:
        _args.append(spec_id)

    if cached_vardict:
        vardict = cached_vardict
    else:
        if spec_type == 'client':
            vardict = fetch_client(p4, spec_id)
        else:
            r = p4.run(si['cmd_one'], _args)
            vardict = p4gf_util.first_dict(r)

    if values:
        for key in values:
            if values[key] is None:
                if key in vardict:
                    del vardict[key]
            else:
                vardict[key] = values[key]

    _args = ['-i']
    if args:
        _args += _to_list(args)
    p4.input = vardict
    try:
        p4.run(si['cmd_one'],  _args)
        return vardict
    except:
        LOG.debug("failed cmd: set_spec {type} {id} {dict}"
                  .format(type=spec_type, id=spec_id, dict=vardict))
        raise


def ensure_spec(p4, spec_type, spec_id, args=None, values=None):
    """Create spec if it does not already exist, NOP if already exist.

    Return True if created, False if already existed.

    You probably want to check values (see ensure_spec_values) if
    ensure_spec() returns False: the already-existing spec might
    contain values that you do not expect.
    """
    if not spec_exists(p4, spec_type, spec_id):
        LOG.debug("creating %s %s", spec_type, spec_id)
        set_spec(p4, spec_type, spec_id, args=args, values=values)
        return True
    else:
        LOG.debug("%s %s already exists", spec_type, spec_id)
        return False


def ensure_user_gf(p4, auth_method=None):
    """Create user git-fusion-user it not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    if auth_method:
        spec = {'FullName': NTR('Git Fusion'),
                'AuthMethod': 'perforce'}
    else:
        spec = {'FullName': NTR('Git Fusion')}
    return ensure_spec(p4, NTR('user'), spec_id=p4gf_const.P4GF_USER,
                       args='-f',
                       values=spec)


def ensure_user_reviews(p4, auth_method=None):
    """Create user git-fusion-reviews it not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    user = p4gf_util.gf_reviews_user_name()
    if auth_method:
        spec = {
            'FullName': _('Git Fusion Reviews'),
            'AuthMethod': 'perforce',
            'Type': 'service'
        }
    else:
        spec = {
            'FullName': _('Git Fusion Reviews'),
            'Type': 'service'
        }
    return ensure_spec(p4, NTR('user'), spec_id=user, args='-f', values=spec)


def ensure_user_reviews_non_gf(p4, auth_method=None):
    """Create user git-fusion-reviews--non-gf it not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    if auth_method:
        spec = {
            'FullName': _('Git Fusion Reviews Non-GF'),
            'AuthMethod': 'perforce',
            'Type': 'service'
        }
    else:
        spec = {
            'FullName': _('Git Fusion Reviews Non-GF'),
            'Type': 'service'
        }
    return ensure_spec(p4, NTR('user'), spec_id=p4gf_const.P4GF_REVIEWS__NON_GF, args='-f',
                       values=spec)


def ensure_user_reviews_all_gf(p4, auth_method=None):
    """Create user git-fusion-reviews--non-gf_union it not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    if auth_method:
        spec = {
            'FullName': _('Git Fusion Reviews Non-GF Union'),
            'AuthMethod': 'perforce',
            'Type': 'service'
        }
    else:
        spec = {
            'FullName': _('Git Fusion Reviews Non-GF Union'),
            'Type': 'service'
        }
    return ensure_spec(p4, NTR('user'), spec_id=p4gf_const.P4GF_REVIEWS__ALL_GF, args='-f',
                       values=spec)

def ensure_unknown_git(p4, auth_method=None):
    """Create user unknown_git it not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    if auth_method:
        spec = {'FullName': NTR('Unknown Git Contributor'),
                'AuthMethod': 'perforce',
                'Email': 'unknown_git@helixenterprise'
                }
    else:
        spec = {'FullName': NTR('Unknown Git Contributor'),
                'Email': 'unknown_git@helixenterprise'
                }
    return ensure_spec(p4, NTR('user'), spec_id=p4gf_const.P4GF_UNKNOWN_USER,
                       args='-f',
                       values=spec)

def ensure_depot_gf(p4):
    """Create depot P4GF_DEPOT if not already exists.

    Requires that connection p4 has super permissions.

    Return True if created, False if already exists.
    """
    spec = {
        'Owner':       p4gf_const.P4GF_USER,
        'Description': _('Git Fusion data storage.'),
        'Type':        NTR('local'),
        'Map':         '{depot}/...'.format(depot=p4gf_const.P4GF_DEPOT)
    }
    return ensure_spec(p4, NTR('depot'), spec_id=p4gf_const.P4GF_DEPOT, values=spec)


def spec_values_match(p4, spec_type, spec_id, values):
    """Verify that the spec holds desired values, returning True if okay."""
    spec = p4gf_util.first_dict(p4.run(spec_type, '-o', spec_id))
    for key in values:
        if spec.get(key) != values[key]:
            return False
    return True


def ensure_spec_values(p4, spec_type, spec_id, values):
    """
    Spec exists but holds unwanted values? Replace those values.

    Does NOT create spec if missing. The idea here is to ensure VALUES,
    not complete spec. If you want to create an entire spec, you
    probably want to specify more values that aren't REQUIRED to match,
    such as Description.
    """
    if spec_type == 'client':
        spec = fetch_client(p4, spec_id)
    else:
        spec = p4gf_util.first_dict(p4.run(spec_type, '-o', spec_id))
    mismatches = {key: values[key] for key in values if spec.get(key) != values[key]}
    LOG.debug2("ensure_spec_values(): want={want} got={spec} mismatch={mismatch}".format(
        spec=spec, want=values, mismatch=mismatches))

    if mismatches:
        set_spec(p4, spec_type, spec_id=spec_id, values=mismatches)
        LOG.debug("successfully updated %s %s", spec_type, spec_id)
    return mismatches


def fetch_client(p4, client_name=None, routeless=False):
    """Retrieve the specification for the named client.

    :param p4: P4API instance.
    :param str client_name: name of P4 client to temporarily switch to (default None).
    :param bool routeless: if True, switch connection to not have a 'route' flag (default False).

    """
    with ExitStack() as stack:
        if routeless:
            p4 = stack.enter_context(p4gf_create_p4.routeless(p4))
        if client_name:
            stack.enter_context(p4gf_util.restore_client(p4, client_name))
        spec = p4gf_util.first_dict(p4.run('client', '-o'))
    return spec


def fetch_client_raw(p4, client_name, routeless=False):
    """Retrieve the client specification as lines of text (i.e. untagged).

    :param p4: P4API instance.
    :param str client_name: name of P4 client to temporarily switch to (default None).
    :param bool routeless: if True, switch connection to not have a 'route' flag (default False).

    """
    with ExitStack() as stack:
        stack.enter_context(p4.while_tagged(False))
        if routeless:
            p4 = stack.enter_context(p4gf_create_p4.routeless(p4))
        stack.enter_context(p4gf_util.restore_client(p4, client_name))
        raw_lines = p4.run('client', '-o')[0].splitlines()
    return raw_lines


def get_client_view(p4, client_name=None, routeless=False):
    """Retrieve the view mapping for the named client.

    :param p4: P4API instance.
    :param str client_name: name of P4 client to temporarily switch to (default None).
    :param bool routeless: if True, switch connection to not have a 'route' flag (default False).

    """
    spec = fetch_client(p4, client_name, routeless)
    return spec['View']


def fetch_client_template(p4, client_name, template_client, routeless=False):
    """Create a specification for the named client using the given template.

    :param p4: P4API instance.
    :param str client_name: name of P4 client to temporarily switch to (default None).
    :param str template_client: name of P4 client to use as a template.
    :param bool routeless: if True, switch connection to not have a 'route' flag (default False).

    """
    with ExitStack() as stack:
        if routeless:
            p4 = stack.enter_context(p4gf_create_p4.routeless(p4))
        stack.enter_context(p4gf_util.restore_client(p4, client_name))
        spec = p4gf_util.first_dict(p4.run('client', '-o', '-t', template_client))
    return spec


def get_client_template_view(p4, client_name, template_client, routeless=False):
    """Retrieve the client view for the named client using the given template.

    :param p4: P4API instance.
    :param str client_name: name of P4 client to temporarily switch to (default None).
    :param str template_client: name of P4 client to use as a template.
    :param bool routeless: if True, switch connection to not have a 'route' flag (default False).

    """
    spec = fetch_client_template(p4, client_name, template_client, routeless)
    return spec['View']

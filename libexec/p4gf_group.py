#! /usr/bin/env python3.3

"""Create, modify, and query Perforce groups for user membership."""
import logging

import p4gf_const
import p4gf_p4key   as     P4Key
from   p4gf_l10n    import _, NTR
import p4gf_util

# Keys for Perforce spec 'group'
KEY_OWNERS    = NTR('Owners')
KEY_SUBGROUPS = NTR('Subgroups')
KEY_USERS     = NTR('Users')
KEY_GROUP     = NTR('Group')

PERM_PULL = NTR('pull')
PERM_PUSH = NTR('push')

PERM_TO_GROUP       = {PERM_PULL : p4gf_const.P4GF_GROUP_PULL,
                       PERM_PUSH : p4gf_const.P4GF_GROUP_PUSH }

PERM_TO_GROUP_REPO  = {PERM_PULL : p4gf_const.P4GF_GROUP_REPO_PULL,
                       PERM_PUSH : p4gf_const.P4GF_GROUP_REPO_PUSH }

DEFAULT_PERM        = PERM_PUSH

SPEC_TYPE_GROUP     = NTR('group')

LOG = logging.getLogger(__name__)

### Promote to p4gf_spec_writer.py


class SpecWriter:

    """An object that knows when a value has changed and thus needs to be saved."""

    def __init__(self, p4, spec_type, name=None):
        self._p4          = p4
        self._spec_type   = spec_type
        self._spec        = None
        self._needs_write = False
        if name:
            self.fetch(name)

    def _fetch_spec(self, spec_type, spec_id):
        """Read one spec and return it."""
        return p4gf_util.first_dict(self._p4.run(spec_type, "-o", spec_id))

    def fetch(self, name):
        """Read from Perforce.

        Usually getting a default or empty spec,
        sometimes picking up some values or changes forced by a
        customer-installed server trigger, sometimes finding an existing
        spec chock full of values that the customer would rather keep.
        """
        self._spec = self._fetch_spec(self._spec_type, name)
        self._needs_write = False
        return self._spec

    def needs_write(self):
        """Have we changed anything that needs writing?"""
        return self._needs_write

    def write_if(self):
        """Write, only if necessary."""
        if self._needs_write:
            return self.write()
        return None

    def write(self):
        """Write, unconditionally. You probably want to call write_if()."""
        p4 = self._p4

        LOG.debug("SpecWriter.write({})".format(self._spec_type))
        p4.input = self._spec
        return p4.run(self._spec_type, "-i")

    def force_list_element(self, key, element):
        """Make sure that <key> exists as a list and contains <element>."""
        if key not in self._spec:
            self._spec[key] = [element]
            self._needs_write = True
            return True

        if element not in self._spec[key]:
            self._spec[key].append(element)
            self._needs_write = True
            return True

        # Already had it, nothing changed
        return False

    ### Add force_value(key, value) later if you actually need it.


class GroupWriter(SpecWriter):

    """A SpecWriter that understands 'p4 group' and its -a/-A rules.

    Rather than running the rather expensive 'p4 groups' command just to
    detect whether or not we need to create this group, we instead look at
    the value for field 'Owners': if our caller changes it via
    force_list_element(), then our caller is most likely creating this
    group from scratch, and needs to be an Owner. Good enough for our
    needs, and avoids a call to 'p4 groups'.
    """

    def __init__(self, p4, name=None):
        """After override."""
        SpecWriter.__init__(self, p4, SPEC_TYPE_GROUP, name)
        self._owner_changed = False
        self._other_changed = False

        if LOG.isEnabledFor(logging.DEBUG):
            r = p4.run('groups')
            n = {g['group']: 1 for g in r if g['group'].startswith('git-fusion-')}
            LOG.debug3('GroupWriter.__init__() current git-fusion-* groups are...\n{}'
                       .format('\n'.join(sorted(n.keys()))))

    def force_list_element(self, key, element):
        """After override to note if we changed Owners."""
        value = SpecWriter.force_list_element(self, key, element)
        if value:
            if key == KEY_OWNERS:
                self._owner_changed = True
            else:
                self._other_changed = True
        return value

    def create_if(self):
        """call create() if necessary."""
        if self.needs_create():
            return self.create()
        return None

    def needs_create(self):
        """Are we the first to write to this group?"""
        return self._owner_changed

    def create(self):
        """Create the group with ourself as the owner.

        Don't bother setting all the fields: Owner is all we need for
        future 'p4 group -a' modification requests to succeed.
        """
        invalidate_groups_i()
        spec_id = self._spec[KEY_GROUP]
        spec = self._fetch_spec(SPEC_TYPE_GROUP, spec_id)
        spec[KEY_OWNERS] = self._spec[KEY_OWNERS]

        self._p4.input = self._spec
        return self._p4.run(SPEC_TYPE_GROUP, "-i", "-A")

    def write(self):
        """Create before writing. Pass -a to modify existing group.

        Complete override.
        """
        invalidate_groups_i()
        r = self.create_if()
        # +++ don't need to write if the only thing that changed was Owner.
        if not self._other_changed:
            return r

        # LOG.debug("GroupWriter.write() spec=\n{}".format(self._spec))
        self._p4.input = self._spec
        return self._p4.run(self._spec_type, "-i", "-a")


def create_global_perm(p4, perm):
    """Create git-fusion-pull or git-fusion-push."""
    group_name = PERM_TO_GROUP[perm]
    spec = GroupWriter(p4, group_name)
    spec.force_list_element(KEY_OWNERS, p4gf_const.P4GF_USER)
    return spec.write_if()


def create_repo_perm(p4, repo_name, perm):
    """Create git-fusion-<repo>-pull or -push."""
    group_name = PERM_TO_GROUP_REPO[perm].format(repo=repo_name)
    subgroup_name = PERM_TO_GROUP[perm]
    spec = GroupWriter(p4, group_name)
    spec.force_list_element(KEY_OWNERS, p4gf_const.P4GF_USER)
    spec.force_list_element(KEY_SUBGROUPS, subgroup_name)
    spec.write_if()


def create_default_perm(p4, perm=DEFAULT_PERM):
    """Create the 'stick all users into this pull/push permission group' default p4key.

    If p4key already exists with non-zero value, leave it unchanged.
    """
    p4key = P4Key.get(p4, p4gf_const.P4GF_P4KEY_PERMISSION_GROUP_DEFAULT)
    if p4key is not None and p4key != '0':
        # Somebody already set it.
        return False
    P4Key.set(p4, p4gf_const.P4GF_P4KEY_PERMISSION_GROUP_DEFAULT, perm)
    return True


def _can_push(pull, push):
    """If a group grants pull but not push, then nope, no push for you.

    Even if some other group grants push.

    If the group grants nothing, return None.
    """
    if pull and not push:
        return False
    if push:
        return True
    return None


# Cache of p4user => `p4 groups -i {user}` results
_GROUPS_I = {}


def _groups_i(p4, p4user):
    """Run `p4 groups -i {p4user}` and return result.
    Cache results so we only run this once per user.
    """
    if p4user not in _GROUPS_I:
        _GROUPS_I[p4user] = p4.run('groups', '-i', p4user)
    return _GROUPS_I[p4user]


def invalidate_groups_i():
    """The 'groups -i' cache is only valid until the next time you
    write a group spec. Also not valid between runs, so if you're
    a long-lived HTTP server process, call this before each
    fetch/push/whatever run.
    """
    global _GROUPS_I
    _GROUPS_I = {}


# Cache of git-fusion-permission-group-default
_PERM_KEY_DEFAULT = None


def _perm_key_default(p4):
    """Run `p4 key git-fusion-permission-group-default` to get
    default permissions. Once.
    """
    global _PERM_KEY_DEFAULT
    if _PERM_KEY_DEFAULT is None:
        _PERM_KEY_DEFAULT = P4Key.get(p4, p4gf_const.P4GF_P4KEY_PERMISSION_GROUP_DEFAULT)
    return _PERM_KEY_DEFAULT


# Convert True/False/None to 1/0/' ' for shorter printing.
PERM_TO_CHAR = {True: '1', False: '0', None: ' '}


class RepoPerm:

    """Struct containing a single user's permissions on a repo.

    Keeper of the actual pull/push authorization logic: if user is a
    member of pull/push group X, then user has pull/pull perm. Honors
    default p4key value, too.

    for_user_and_repo() is the factory.
    can_pull() and can_push() query for permission.
    write_if() writes user to appropriate repo group if necessary.
    """

    def __init__(self):
        self.p4user_name  = None
        self.repo_name    = None
        self.repo_pull    = None
        self.repo_push    = None
        self.global_pull  = None
        self.global_push  = None
        self.default_pull = None
        self.default_push = None
        self.user_read_permission_checked = False
        self.user_perm_repo_pull    = None
        self.error_msg   = None

    def __str__(self):
        s = ( "user={user} repo={repo}"
             + " repo:{vpull}{vpush}"
             + " global:{gpull}{gpush}"
             + " default:{dpull}{dpush}").format(
             user = self.p4user_name,
             repo = self.repo_name,
             vpull = PERM_TO_CHAR[self.repo_pull],
             vpush = PERM_TO_CHAR[self.repo_push],
             gpull = PERM_TO_CHAR[self.global_pull],
             gpush = PERM_TO_CHAR[self.global_push],
             dpull = PERM_TO_CHAR[self.default_pull],
             dpush = PERM_TO_CHAR[self.default_push])
        return s

    @staticmethod
    def for_user_and_repo(p4, p4user, repo_name, required_perm):
        """Factory to fetch user's permissions on a repo."""
        LOG.debug("for_user_and_repo() {u} {r} {p}".format(u=p4user, r=repo_name, p=required_perm))

        group_list = _groups_i(p4, p4user)
        group_dict = {group['group']: group for group in group_list}
        LOG.debug3("group_dict.keys()={}".format(group_dict.keys()))

        vp = RepoPerm()
        vp.p4user_name = p4user
        vp.repo_name   = repo_name

        vp.repo_pull   = p4gf_const.P4GF_GROUP_REPO_PULL.format(repo=repo_name) in group_dict
        vp.repo_push   = p4gf_const.P4GF_GROUP_REPO_PUSH.format(repo=repo_name) in group_dict
        vp.global_pull = p4gf_const.P4GF_GROUP_PULL                             in group_dict
        vp.global_push = p4gf_const.P4GF_GROUP_PUSH                             in group_dict

        value = P4Key.get(p4, p4gf_const.P4GF_P4KEY_PERMISSION_GROUP_DEFAULT)
        if value == '0':
            value = DEFAULT_PERM
        vp.default_pull = value == PERM_PULL
        vp.default_push = value == PERM_PUSH
        LOG.debug("p4key={}".format(value))

        LOG.debug(vp)
        return vp

    def can(self, perm):
        """Where perm is either 'pull' or 'push', call can_pull() or can_push()."""
        if perm == PERM_PULL:
            return self.can_pull()
        elif perm == PERM_PUSH:
            return self.can_push()
        raise RuntimeError(_('invalid permission {permission}').format(permission=perm))

    def can_pull(self):
        """If any group grants pull or push permission, or we grant either
        permission by default, then yes, you may pull.
        """
        return (   self.repo_pull
                or self.repo_push
                or self.global_pull
                or self.global_push
                or self.default_pull
                or self.default_push)

    def can_push(self):
        """If any group grants push permission, or we grant push
        permission by default, then yes, you may push.

        If a group grants pull but not push, then nope, no push for you,
        even if some other group grants push.
        """
        if None != _can_push(self.repo_pull,    self.repo_push):
            return _can_push(self.repo_pull,    self.repo_push)

        if None != _can_push(self.global_pull,  self.global_push):
            return _can_push(self.global_pull,  self.global_push)

        if None != _can_push(self.default_pull, self.default_push):
            return _can_push(self.default_pull, self.default_push)

        return None

    def write_if(self, p4):
        """If this user's permission come only from the default p4key and
        not from group membership, write this user to the appropriate group
        for this repo."""
        if self.needs_write():
            self._write(p4)

    def perm(self):
        """Return best of PERM_PUSH, PERM_PULL, or None."""
        if self.can_push():
            return PERM_PUSH
        if self.can_pull():
            return PERM_PULL
        return None

    def _write(self, p4):
        """Unconditionally add this user to the git-fusion-<repo>-pull or
        git-fusion-<repo>-push.
        """
        _perm = self.perm()

        subgroup_name   = PERM_TO_GROUP     [_perm]
        group_name      = PERM_TO_GROUP_REPO[_perm].format(repo=self.repo_name)

        spec = GroupWriter(p4, group_name)
        spec.force_list_element(KEY_OWNERS,    p4gf_const.P4GF_USER)
        spec.force_list_element(KEY_SUBGROUPS, subgroup_name)
        spec.force_list_element(KEY_USERS,     self.p4user_name)
        spec.write_if()

    def needs_write(self):
        """If we have no permissions granted by group membership,
        but at least one permission granted by default, then that default
        could be written to the group by adding this user to that group.
        """
        return (    not self.repo_pull
                and not self.repo_push
                and not self.global_pull
                and not self.global_push
                and (   self.default_pull
                     or self.default_push))

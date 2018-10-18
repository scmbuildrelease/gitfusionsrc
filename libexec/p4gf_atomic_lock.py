#! /usr/bin/env python3.3
"""Use Reviews feature per gf-instance service user accounts to enforce atomic view locks."""

import logging

import p4gf_bootstrap  # pylint: disable=unused-import
import p4gf_branch
import p4gf_const
import p4gf_create_p4
import p4gf_p4key as P4Key
import p4gf_p4spec
import p4gf_l10n
import p4gf_lock
import p4gf_p4msg
import p4gf_p4msgid
import p4gf_path
import p4gf_util
import re
from P4 import Map, P4Exception

LOG = logging.getLogger(__name__)
_   = p4gf_l10n._
NTR = p4gf_l10n.NTR

SEPARATOR = '...'
INTERSECT = True
NO_INTERSECT = False

REMOVE = NTR('remove')
ADD    = NTR('add')
ADD_UNIQUE = NTR('add_unique')


class LockConflict(Exception):

    """
    Raised when the reviews user acquires a conflicting lock.

    The caller should abandon the operation it was about to begin.
    """

    pass


def remove_exclusionary_maps_and_plus_prefix(viewlist):
    """Remove exlcusionary maps from lh map list."""
    cleaned = []
    for view in viewlist:
        if re.match(r'^\"?-', view):  # starts with - or "-
            continue
        view = re.sub(r'\+', '', view)  # remove possible '+' from Review line
        cleaned.append(view)
    return cleaned


def get_local_stream_depots(p4):
    """Get list of local depots."""
    depot_pattern = re.compile(r"^" + re.escape(p4gf_const.P4GF_DEPOT))
    data = p4.run('depots')
    depots = []
    for depot in data:
        if ((depot['type'] == 'local' or depot['type'] == 'stream')
                and not depot_pattern.search(depot['name'])):
            depots.append(depot['name'])
    LOG.debug("get_local_stream_depots: {0}".format(depots))
    return depots


def has_files_at_change(p4, change):
    """Determine if any files exist in the given changelist."""
    depots = get_local_stream_depots(p4)
    for depot in depots:
        cmd = ['files', '-m1']
        cmd.append("//{0}/...@={1}".format(depot, change))
        r = p4.run(cmd)
        for rr in r:
            if not isinstance(rr, dict):
                continue
            if 'depotFile' in rr:
                return True
    return False


def can_cleanup_change(p4, change):
    """Determine whether the Reviews may be cleaned from a non-longer pending changelist."""
    try:
        int(change)
    except ValueError:
        return False

    result = p4.run('describe', '-s', str(change))
    vardict = p4gf_util.first_dict_with_key(result, 'change')
    if not vardict:
        LOG.debug("can_cleanup_change: change {0} does not exist : return True".format(change))
        return True

    LOG.debug("can_cleanup_change  describe on {0}: status={1} shelved={2} depotFile={3}".format(
        change, vardict['status'], 'shelved' in vardict, 'depotFile' in vardict))
    if 'code' in vardict and vardict['code'] == 'error' and 'data' in vardict:
        if re.search('no such changelist', vardict['data']):
            return True
        else:
            raise RuntimeError(
                _("Git Fusion: error in describe for change '{change}': '{vardict}'")
                .format(change=change, vardict=vardict))

    submitted = False
    pending = False
    no_files = True

    shelved = 'shelved' in vardict
    if 'status' in vardict:
        pending   = vardict['status'] == 'pending'
        submitted = vardict['status'] == 'submitted'
    if not shelved and pending:
        if 'depotFile' in vardict:
            no_files = False
        else:
            no_files = not has_files_at_change(p4, change)

    if pending and shelved:
        return False
    if pending and no_files:
        return True
    if submitted:
        return True
    return False

def remove_non_gf_reviews(p4, p4_reviews_non_gf, p4key, data, change):
    """ Remove non-Git Fusion Reviews which are now unlocked."""
    LOG.debug3("p4key {}  change {}".format(p4key, change))
    LOG.debug3("non_gf submit data  {}".format(data))
    filecount = 0
    if len(data) >= 3:
        try:
            filecount = int(data[2])
        except ValueError:
            LOG.debug("Cannot convert non_gf submit_p4key filecount - skipping remove for : {0}".
                      format(p4key))
    if filecount:
        update_repo_reviews(p4_reviews_non_gf, p4gf_const.P4GF_REVIEWS__NON_GF,
                            None, action=REMOVE, change=change)
        P4Key.delete(p4, p4key)


def remove_non_gf_changelist_files(change, current_reviews):
    """Changelist files in the non-gf user Reviews are bounded
    by changelist markers. Remove that set of files."""

    change_found_begin = False
    change_found_end = False
    gf_begin_marker = p4gf_const.NON_GF_REVIEWS_BEGIN_MARKER_PATTERN.format(change)
    gf_end_marker = p4gf_const.NON_GF_REVIEWS_END_MARKER_PATTERN.format(change)

    def review_path_in_changelist(path):
        """Return True if path lies between (inclusive) the GF change markers.

        The path argument is passed in the list sequence from Reviews.

        """
        nonlocal change_found_begin
        nonlocal change_found_end
        if not change_found_begin:
            if path == gf_begin_marker:
                change_found_begin = True
                return True
            else:
                return False
        else:
            if change_found_end:
                return False
            else:
                if path == gf_end_marker:
                    change_found_end = True
                return True

    current_reviews = [x for x in current_reviews if not review_path_in_changelist(x)]
    return current_reviews


class _FakeP4Map(object):
    """A replacement for P4.Map that avoids O(n^2) behavior.
    Knows ONLY enough to work for update_all_gf_reviews()'s pass
    to update_repo_reviews(ADD_UNIQUE).
    """
    def __init__(self, map_tuple_list):
        self.map_tuple_list = map_tuple_list

    def lhs(self):
        """Return left half of each map tuple."""
        return (t.lhs for t in self.map_tuple_list)

    def __str__(self):
        return "\n".join( ["{} {}".format(t.lhs, t.rhs)
                          for t in self.map_tuple_list] )

def update_all_gf_reviews(ctx, map_tuple_list):
    """Update the set of git-fusion-reviews--all-gf Reviews with
    the union of the writable branches' views.
    :param map_tuple_list:
        list of lines to add to -all-gf's Reviews list.
        Must be a list of p4gf_branch.MapTuple.
    """
    LOG.debug("update_all_gf_reviews for {0}".format(ctx.p4.client))

    ### It sure would be nice if we could wean ourself off of P4.Map for
    ### -all-gf.
    ###
    ### This creates a P4.Map of extraordinary magnitude. Internally that
    ### P4.Map() will run O(n^2) operations which can take MINUTES for n>5,000.
    fake_p4map = _FakeP4Map(map_tuple_list)
    p4 = ctx.p4gf
    with p4gf_create_p4.Connector(ctx.p4gf_reviews_all_gf) as p4_reviews:
        with p4gf_lock.ReviewsLock(p4):
            update_repo_reviews(p4_reviews, p4gf_const.P4GF_REVIEWS__ALL_GF,
                                fake_p4map, action=ADD_UNIQUE, change=None)


def update_repo_reviews(p4_reviews, user, clientmap, action=None, change=None):
    """Add or remove view left maps to the review user Reviews.

    Using Map.join, check for a conflict with self - this gf_reviews user.
    This check handles the case of overlapping views pushed to the same GF server.
    If conflict, return INTERSECT and do not update the user reviews
    """
    # pylint: disable=too-many-branches
    repo_views = []
    if clientmap:
        repo_views = remove_exclusionary_maps_and_plus_prefix(clientmap.lhs())

    LOG.debug3("update_repo_reviews: user={0} clientmap={1} repo_views={2}"
               .format(user, clientmap, repo_views))

    args_ = ['-o', user]
    r = p4_reviews.run('user', args_)
    vardict = p4gf_util.first_dict(r)
    current_reviews = []
    if "Reviews" in vardict:
        current_reviews = vardict["Reviews"]
        if action == ADD:
            if has_intersecting_views(current_reviews, clientmap):
                return INTERSECT

    if action == ADD:
        reviews = current_reviews + repo_views
    elif action == ADD_UNIQUE:
        reviews = set(current_reviews)
        reviews.update(set(repo_views))
        reviews = list(reviews)     # list required below

    elif action == REMOVE:
        if user == p4gf_const.P4GF_REVIEWS__NON_GF:
            reviews = remove_non_gf_changelist_files(change, current_reviews)
        else:  # for Git Fusion reviews
            reviews = list(current_reviews)  # make a copy
            for path in repo_views:
                try:
                    reviews.remove(path)
                except ValueError:
                    pass
    else:
        raise RuntimeError(_("Git Fusion: update_repo_reviews incorrect action '{action}'")
                           .format(action=action))
    LOG.debug3("for user {} setting reviews {}".format(user, reviews))
    p4gf_p4spec.set_spec(p4_reviews, 'user', user, values={"Reviews": reviews})
    return NO_INTERSECT


def lock_update_repo_reviews(ctx, action=None):
    """Lock on this gf-instance p4key lock then add the repo views to the
    service user account. Use 'p4 reviews' to check whether views are locked.
    Raise exception if the repo views are already locked by p4, this or any GF instance.
    Cleanup reviews on rejection.
    """
    p4 = ctx.p4gf
    #clientmap = Map(ctx.client_view_union(ctx.branch_dict().values()))
    map_tuple_list = p4gf_branch.calc_branch_union_tuple_list(ctx.p4.client,
                        ctx.branch_dict().values())
    clientmap = _FakeP4Map(map_tuple_list)
    user = p4gf_util.gf_reviews_user_name()
    if not p4gf_util.service_user_exists(p4, user):
        raise RuntimeError(_("Git Fusion: GF instance reviews user '{user}' does not exist")
                           .format(user=user))
    LOG.debug3("user:{} repo:{} action:{}".format(user, ctx.config.repo_name, action))

    with p4gf_create_p4.Connector(ctx.p4gf_reviews) as p4_reviews:
        with p4gf_lock.ReviewsLock(p4):
            # When action == ADD, before updating reviews
            # update_repo_reviews checks for a conflict with self - this p4_reviews user.
            # This check handles the case of overlapping views pushed to the same GF server
            # since shared views for two repos on the same GF could not otherwise be detected.
            # If it detects a conflict it returns INTERSECT and does not update the user reviews.
            intersects = update_repo_reviews(p4_reviews, user, clientmap, action=action)

            if intersects == INTERSECT:
                msg = p4gf_const.P4GF_LOCKED_BY_MSG.format(user=user)
                LOG.error(msg)
                LOG.error("clientmap: {}".format(clientmap))
                raise LockConflict(msg)

            # No intersection and our views were added, so while we retain the reviews_lock
            # Check if another P4GF_REVIEWS_GF/NON_GF service user already has locked the view
            if action == ADD:
                is_locked, by_user = is_locked_by_review(p4, clientmap)
                if is_locked:
                    update_repo_reviews(p4_reviews, user, clientmap, action=REMOVE)
                    msg = p4gf_const.P4GF_LOCKED_BY_MSG.format(user=by_user)
                    LOG.error(msg)
                    LOG.error("clientmap: {}".format(clientmap))
                    raise LockConflict(msg)


def is_locked_by_review(p4, clientmap, check_for_self=False):
    """Check whether any other GF/submit users have my views under Review."""
    gf_user = p4gf_util.gf_reviews_user_name()
    repo_views = remove_exclusionary_maps_and_plus_prefix(clientmap.lhs())
    LOG.debug3("calling reviews with {0}".format(repo_views))
    cmd = [NTR('reviews')] + [p4gf_path.dequote(l) for l in repo_views]
    try:
        reviewers = p4.run(cmd)
    except P4Exception:
        if p4gf_p4msg.find_msgid(p4, p4gf_p4msgid.MsgDm_ReferClient):
            p4gf_util.raise_gfuser_insufficient_perm(p4port=p4.port)
        else:
            raise

    for user in reviewers:
        _user = user['user']
        if _user.startswith(p4gf_const.P4GF_REVIEWS_GF):
            if _user == p4gf_const.P4GF_REVIEWS__ALL_GF:
                continue        # skip the union Reviews - used only by trigger
            if check_for_self:
                if _user == gf_user:
                    return True, gf_user
            if _user != gf_user:    # always check if another user has this view locked
                return True, _user
    return False, None


def has_intersecting_views(current_reviews, clientmap):
    """Determine whether the clientmap intersects the
    current set of reviews for this GF reviews user.
    """
    reviews_map = Map()
    for v in current_reviews:
        reviews_map.insert(v)

    repo_map = Map()
    for l in clientmap.lhs():
        repo_map.insert(l)

    joined = Map.join(reviews_map, repo_map)

    for l in joined.lhs():
        if not l.startswith('-'):
            return INTERSECT

    return NO_INTERSECT

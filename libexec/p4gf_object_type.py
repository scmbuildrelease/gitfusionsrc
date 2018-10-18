#! /usr/bin/env python3.3
"""Return the type and extra info of an object stored in the
//P4GF_DEPOT/objects/... hierarchy.

Checks only the local filesystem for .git-fusion/...
"""

import logging

from   p4gf_object_type_change_num_to_commit_cache  import ChangeNumToCommitCache
import p4gf_branch
import p4gf_const
import p4gf_p4key                   as     P4Key
from   p4gf_l10n                    import NTR
from   p4gf_object_type_cache       import ObjectTypeCache
from   p4gf_object_type_list        import ObjectTypeList
from   p4gf_object_type_tree_cache  import TreeCache
import p4gf_object_type_util        as     util
import p4gf_path
import p4gf_util

LOG = logging.getLogger(__name__)

# Values for ObjectType.type. Must match types for 'git cat-file -t <type>'
COMMIT = "commit"
# Character that is used to delineate branch ID in commit object path
# (allowed in depot paths but not in view names, making it very useful).
BRANCH_SEP = ','

DEPOT_PATH_PATTERN = '{objects_root}/repos/{repo}/commits/{slashed}-{branch_id},{change_num}'

# pylint:disable=too-many-public-methods
# Probably true: ObjectType sure does a lot.


class ObjectType:
    """A single sha1 maps to a single type: commit, tree, or blob.

    If commit, maps to 1 or more (changelist, repo, branch) tuples.
    """
    # Cache of object details: keys are SHA1, values are ObjectType lists.
    object_cache = ObjectTypeCache()
    tree_cache = TreeCache()
    last_commits_cache = {}
    last_commits_cache_complete = False
    change_num_to_commit_cache = ChangeNumToCommitCache()

    def __init__(self, *
                , sha1
                , otype
                , change_num = None
                , repo_name  = None
                , branch_id  = None
            ):
        self.sha1       = sha1
        self.type       = otype
        self.change_num = change_num
        self.repo_name  = repo_name
        self.branch_id  = p4gf_branch.BranchId(branch_id)

    def __eq__(self, b):
        return (    self.sha1       == b.sha1
                and self.type       == b.type
                and self.change_num == b.change_num
                and self.repo_name  == b.repo_name
                and self.branch_id  == b.branch_id
                )

    def __ne__(self, b):
        return (   self.sha1       != b.sha1
                or self.type       != b.type
                or self.change_num != b.change_num
                or self.repo_name  != b.repo_name
                or self.branch_id  != b.branch_id
                )

    def __str__(self):
        return "{} {} {} {} {}".format(
                  p4gf_util.abbrev(self.sha1)
                , self.type
                , self.change_num
                , self.repo_name
                , self.branch_id
                )

    def __repr__(self):
        return str(self)

    @property
    def details(self):
        """Scaffolding to convert ot.details.xxx to just ot.xxx.

        Read-only. Who sets our old .details field?
        """
        return self

    @property
    def changelist(self):
        """Scaffolding to convert ot.details.changelist to just ot.change_num

        Read-only. Who sets our old .details field?
        """
        return self.change_num

    @staticmethod
    def reset_cache():
        """After gitmirror submits new ObjectCache files to Perforce, our cache
        is no longer correct.
        """
        ObjectType.object_cache.clear()
        ObjectType.tree_cache.clear()
        ObjectType.last_commits_cache = {}
        ObjectType.last_commits_cache_complete = False
        ObjectType.change_num_to_commit_cache.clear()
        LOG.debug2("cache cleared")

    @staticmethod
    def commit_from_filepath(filepath):
        """Take a (client or depot) file path and parse off the "xxx-commit-nnn" suffix.

        Return an ObjectType instance.
        """
        LOG.debug("from_filepath({})".format(filepath))
        m = util.OBJPATH_COMMIT_REGEX.search(filepath)
        if not m:
            return None
        sha1       = m.group(NTR('slashed_sha1')).replace('/', '')
        repo       = m.group(NTR('repo'))
        change_num = m.group(NTR('change_num'))
        branch_id  = m.group(NTR('branch_id'))
        return ObjectType.create_commit( sha1       = sha1
                                       , repo_name  = repo
                                       , change_num = change_num
                                       , branch_id  = branch_id )

    @staticmethod
    def create_commit(*, sha1, repo_name, change_num, branch_id):
        """Factory to create COMMIT ObjectType."""
        return ObjectType( sha1       = sha1
                         , otype      = COMMIT
                         , change_num = change_num
                         , repo_name  = repo_name
                         , branch_id  = branch_id
                         )

    @staticmethod
    def tree_p4_path(tree_sha1):
        """Return depot path to a tree."""
        return p4gf_path.tree_p4_path(tree_sha1)

    @staticmethod
    def commit_p4_path(ctx, commit_ot):
        """Return depot path to a commit ObjectType."""
        return commit_depot_path(commit_ot.sha1,
                               commit_ot.change_num,
                               ctx.config.repo_name,
                               commit_ot.branch_id)

    @staticmethod
    def tree_exists_in_p4(p4, sha1):
        """Returns true if sha1 identifies a tree in the //P4GF_DEPOT/objects/... hierarchy."""
        return ObjectType.tree_cache.tree_exists(p4, sha1)

    @staticmethod
    def _load_last_commits_cache(ctx):
        """If this is the first time called, load the cache of last commits."""
        if ObjectType.last_commits_cache_complete:
            return

        key_value = P4Key.get_all( ctx.p4gf
                                 , p4gf_const.P4GF_P4KEY_INDEX_LCN_ON_BRANCH_REPO_ALL
                                   .format(repo_name=ctx.config.repo_name) )
        for key, value in key_value.items():
            mk = util.KEY_LAST_REGEX.search(key)
            if not mk:
                LOG.debug("ignoring unexpected p4key: {}".format(key))
                continue
            mv = util.VALUE_LAST_REGEX.search(value)
            if not mv:
                LOG.debug("ignoring invalid p4key value: {}={}"
                          .format(key, value))
            ObjectType.last_commits_cache[mk.group('branch_id')] = value
            LOG.debug2('last change_num,commit for branch {} is {}'
                       .format(mk.group('branch_id'), value))
        ObjectType.last_commits_cache_complete = True

    @staticmethod
    def last_change_num_for_branches(ctx, branch_ids, must_exist_local=False):
        """Return highest changelist number for all branches which exists in p4.

        Searches //P4GF_DEPOT/objects/... for commits and returns ObjectType
        for commit with highest change_num, or None if no matching commit.

        If must_exist_local is True, only commits which also exist in the
        repo are considered in the search.
        """
        # pylint: disable=too-many-branches
        # if only one branch_id given, don't fetch them all
        if len(branch_ids) == 1:
            branch_id = branch_ids[0]
            if branch_id not in ObjectType.last_commits_cache:
                        # Using get_all() instead of get() to avoid annoying
                        # default value "0" for unset keys.
                key = p4gf_const.P4GF_P4KEY_INDEX_LCN_ON_BRANCH\
                      .format( repo_name = ctx.config.repo_name
                             , branch_id = branch_id)
                d = P4Key.get_all(ctx.p4gf, key)
                if d and d.get(key):
                    ObjectType.last_commits_cache[branch_id] = d[key]
            if branch_id not in ObjectType.last_commits_cache:
                return None
            change_num, sha1 = ObjectType.last_commits_cache[branch_id].split(',')
            if must_exist_local and not p4gf_util.sha1_exists(sha1):
                return None
            return ObjectType.create_commit( sha1       = sha1
                                           , repo_name  = ctx.config.repo_name
                                           , change_num = int(change_num)
                                           , branch_id  = branch_id )

        # if more than one branch, load up all branches into the cache
        ObjectType._load_last_commits_cache(ctx)
        highest = {}
        k = None
        for branch_id, v in ObjectType.last_commits_cache.items():
            if branch_id not in branch_ids:
                continue
            change_num, sha1 = v.split(',')
            if branch_id in highest:
                if int(change_num) > highest[branch_id][0]:
                    if must_exist_local and not p4gf_util.sha1_exists(sha1):
                        continue
                    highest[branch_id] = (int(change_num), sha1)
            elif not branch_ids or branch_id in branch_ids:
                if must_exist_local and not p4gf_util.sha1_exists(sha1):
                    continue
                highest[branch_id] = (int(change_num), sha1)
            else:
                continue
            if not k or int(change_num) > highest[k][0]:
                k = branch_id
        if not k:
            return None
        return ObjectType.create_commit( sha1       = highest[k][1]
                                       , repo_name  = ctx.config.repo_name
                                       , change_num = highest[k][0]
                                       , branch_id  = k )

    @staticmethod
    def commits_for_sha1(ctx, sha1, branch_id=None):
        """Return ObjectTypeList of matching commits.

        If branch_id is specified, result will contain at most one match.
        """
        assert sha1
        otl = ObjectType.object_cache.get(sha1)
        if otl:
            otl = otl.ot_list
        else:
            path = commit_depot_path(sha1, '*', ctx.config.repo_name, '*')
            otl = _otl_for_p4path(ctx.p4gf, path)
            ObjectType.object_cache.append(ObjectTypeList(sha1, otl))
        if not branch_id:
            return otl
        return [ot for ot in otl if ot.branch_id == branch_id]

    @staticmethod
    def sha1_to_change_num(ctx, sha1, branch_id=None):
        """If a commit exists as specified, return the change_num, else None.

        If no branch_id specified, return highest change number of matching commits.
        """
        if not sha1:
            return None
        otl = ObjectType.commits_for_sha1(ctx, sha1, branch_id)
        if len(otl):
            return max([int(ot.change_num) for ot in otl])
        return None

    @staticmethod
    def change_num_to_commit(ctx, change_num, branch_id=None):
        """If a commit exists as specified, return an ObjectType for the commit, else None.

        If no branch_id specified, return first found matching commit.
        """
        if not change_num:
            return None

        # first, try cache
        from_cache = ObjectType.change_num_to_commit_cache.get(change_num, branch_id)
        if from_cache:
            return ObjectType.create_commit( sha1       = from_cache[1]
                                           , repo_name  = ctx.config.repo_name
                                           , change_num = change_num
                                           , branch_id  = from_cache[0] )

        # not in cache, use index to find commit(s)
        if not branch_id:
            branch_id = '*'
        key_pattern = p4gf_const.P4GF_P4KEY_INDEX_OT\
                      .format( repo_name  = ctx.config.repo_name
                             , change_num = change_num
                             , branch_id  = branch_id )
        result_sha1 = None
        result_branch = None
        key_value = P4Key.get_all(ctx.p4gf, key_pattern)
        for key, value in key_value.items():
            m = util.KEY_BRANCH_REGEX.search(key)
            found_branch = m.group('branch_id')
            found_sha1 = value
            ObjectType.change_num_to_commit_cache.append(change_num, found_branch, found_sha1)
            if branch_id != '*':
                if found_branch != branch_id:
                    continue
            result_sha1 = found_sha1
            result_branch = found_branch
        if not result_sha1:
            return None
        return ObjectType.create_commit( sha1       = result_sha1
                                       , repo_name  = ctx.config.repo_name
                                       , change_num = change_num
                                       , branch_id  = result_branch )

    @staticmethod
    def update_indexes(ctx, r):
        """Call with result of submit to update indexes in p4 keys.

        Ignore trees, but update for any commits.
        """
        for rr in r:
            if 'depotFile' not in rr:
                continue
            depot_file = rr['depotFile']
            commit = ObjectType.commit_from_filepath(depot_file)
            if commit:
                ObjectType.update_last_change_num(ctx, commit)

    def to_index_key_value(self):
        """Return a (name, value) pair for our index P4Key."""
        key_name = p4gf_const.P4GF_P4KEY_INDEX_OT\
                      .format( repo_name  = self.repo_name
                             , change_num = self.change_num
                             , branch_id  = self.branch_id )
        return (key_name, self.sha1)

    @staticmethod
    def write_index_p4key(ctx, commit_ot):
        """Record the p4key index that goes with this commit."""
        (key_name, value) = commit_ot.to_index_key_value()
        P4Key.set(ctx.p4gf, key_name, value)

    def to_index_last_key_value(self):
        """Return a (name, value) pair for our "last copied up to" P4Key.

        Read update_last_change_num() for how to use this:
        don't clobber an existing value with a lower one.
        """
        key_pattern = p4gf_const.P4GF_P4KEY_INDEX_LCN_ON_BRANCH.format(
            repo_name=self.repo_name, branch_id=self.branch_id)
        value = "{},{}".format(self.change_num, self.sha1)
        return (key_pattern, value)

    @staticmethod
    def update_last_change_num(ctx, commit_ot):
        """Update p4 key that tracks the last change_num on a branch."""
        # unconditionally add a p4key mapping change_num -> commit sha1
        ObjectType.write_index_p4key(ctx, commit_ot)
        branch_id = commit_ot.branch_id
        # only update last change_num p4key if this commit has a higher change_num
        if branch_id in ObjectType.last_commits_cache and\
                (int(ObjectType.last_commits_cache[branch_id].split(',')[0]) >
                 int(commit_ot.change_num)):
            return
        (key_pattern, value) = commit_ot.to_index_last_key_value()
        P4Key.set(ctx.p4gf, key_pattern, value)
        ObjectType.last_commits_cache[branch_id] = value

    def is_commit(self):
        """Return True if this object is a commit object, False otherwise."""
        return COMMIT == self.type

    def applies_to_view(self, repo_name):
        """If we're a BLOB or TREE object, we apply to all view names. Yes.
        If we're a COMMIT object, we only apply to the repo name in our
        repo_name member.
        """
        if COMMIT != self.type:
            return True
        match = self._repo_name_to_change_num(repo_name)
        return None != match

    def _repo_name_to_change_num(self, repo_name):
        """Return the matching Perforce change_num associated with the given repo_name.

        Only works for commit objects.

        Return None if no match.
        """
        if self.repo_name == repo_name:
            return self.change_num
        return None

    def to_p4_client_path(self):
        """Generate relative path to object in Perforce mirror.

        Omit the preceding depot path (e.g. //P4GF_DEPOT/).
        """
        if self.type == 'tree':
            return "objects/trees/" + p4gf_path.slashify_sha1(self.sha1)
        assert self.type == 'commit'
        return (NTR('objects/repos/{repo}/commits/{sha1}-{branch_id},{change_num}')
                .format(repo=self.repo_name,
                        sha1=p4gf_path.slashify_sha1(self.sha1),
                        branch_id=self.branch_id.replace('/', '-'),
                        change_num=self.change_num))

    def to_depot_path(self):
        """Return path to this Git object as stored in Perforce."""
        client_path = self.to_p4_client_path()
        if not client_path:
            return None
        return '//{}/{}'.format(p4gf_const.P4GF_DEPOT, client_path)

    @staticmethod
    def log_otl(otl, level=logging.DEBUG3, log=LOG):
        """Debugging dump."""
        if not log.isEnabledFor(level):
            return
        for ot in otl:
            log.log(level, repr(ot))

    def to_dict(self):
        """Convert to a pickle-friendly dict."""
        return { "sha1"       : self.sha1
               , "otype"      : self.type
               , "change_num" : self.change_num
               , "repo_name"  : self.repo_name
               , "branch_id"  : self.branch_id
               }

    @staticmethod
    def from_dict(d):
        """Construct from a pickle-friendly dict."""
        return ObjectType( sha1       = d["sha1"]
                         , otype      = d["otype"]
                         , change_num = d["change_num"]
                         , repo_name  = d["repo_name"]
                         , branch_id  = d["branch_id"] )

# -- module-wide functions ---------------------------------------------------


def _depot_path_to_commit_sha1(depot_path):
    """Return just the sha1 portion of an commit object stored in our depot."""
    m = util.OBJPATH_COMMIT_REGEX.search(depot_path)
    if not m:
        return None
    return m.group('slashed_sha1').replace('/', '')


def known_commit_sha1_list(ctx):
    """Return a list of every known commit sha1 for the current repo."""
    path = commit_depot_path('*', '*', ctx.config.repo_name, '*')
    return [_depot_path_to_commit_sha1(f) for f in util.run_p4files(ctx.p4gf, path)]


def _otl_for_p4path(p4, path):
    """Return list of ObjectType for files reported by p4 files <path>."""
    return [ot for ot in [ObjectType.commit_from_filepath(f) for
                          f in util.run_p4files(p4, path)] if ot]


def commit_depot_path(commit_sha1, change_num, repo, branch_id):
    """Return depot path to a commit."""
    if commit_sha1 == '*':
        assert branch_id == '*' and change_num == '*'
        return (NTR('{objects_root}/repos/{repo}/commits/...')
                .format(objects_root=p4gf_const.objects_root(),
                        repo=repo))
    if branch_id == '*':
        assert change_num == '*'
        return (NTR('{objects_root}/repos/{repo}/commits/{slashed}-*')
                .format(objects_root=p4gf_const.objects_root(),
                        repo=repo,
                        slashed=p4gf_path.slashify_sha1(commit_sha1)))

    #    '{objects_root}/repos/{repo}/commits/{slashed}-{branch_id},{change_num}'
    return (DEPOT_PATH_PATTERN
            .format(objects_root=p4gf_const.objects_root(),
                    repo=repo,
                    slashed=p4gf_path.slashify_sha1(commit_sha1),
                    branch_id=branch_id,
                    change_num=change_num))

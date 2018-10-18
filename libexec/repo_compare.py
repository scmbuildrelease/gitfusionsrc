#! /usr/bin/env python3.3
"""Compare two repos for mismatch.

Identify what mismatches where.

    repo_compare.py <path-a> <path-b>

TODO:
    does that branch loop really work?
    detect and report file resurrection as its own diff cause: likely a
        file resurrection due to lack of branch-for-delete
    Better explanations of each cause.
    world peace
    fancy-schmancy usage and help text.
    wiki page with examples
"""

from   collections import namedtuple, deque, Counter
from   difflib     import SequenceMatcher
import logging
import operator
import os
import re
import sys

import pygit2

import p4gf_pygit2
import p4gf_util
import p4gf_const

                        # debug   : internal tracking
                        # info    : verbose, matching commits included
                        #           for context of a mismatched push
                        # warning : something differs
                        # error   : cannot diff
LOG = logging.getLogger()
LOG.getChild("iter").setLevel(logging.INFO)   # Noisy

                        # Git reference prefixes
_LOCAL  = "refs/heads/"
_REMOTE = "refs/remotes/origin/"

_EMPTY_TREE_SHA1 = '4b825dc642cb6eb9a060e54bf8d69288fbee4904'

KTEXT  = False
                        # pygit2 repos
REPO_A = None
REPO_B = None
                        # Conversion dict, commit sha1 to commit id_str().
SHA1_TO_ID_STR_A = None
SHA1_TO_ID_STR_B = None

RE_WHITESPACE = re.compile(r'\s+')

# Ravenbrook local change.  Match "Copied from Perforce" followed by any number
# of RFC822-mail-header-like lines containing information about the Perforce
# origin of a commit.  See <https://swarm.workshop.perforce.com/jobs/job000442>.
COPIED_FROM_PERFORCE = re.compile(r"(\n[^\n]*" +
                                  p4gf_const.P4GF_EXPORT_HEADER +
                                  r"[^\n]*(?:\n[^\n]*\w+:[^\n]*)+\n?)")    

SHA1_WITH_COPIED_FROM_PERFORCE = []

COUNTS = Counter()


def main():
    """Do the thing."""
    # pylint:disable=too-many-branches, too-many-statements
    global REPO_A
    global REPO_B
    global KTEXT

    args = _argparse()

    logging.basicConfig(format="%(message)s", stream=sys.stdout)
    if args.debug:
        LOG.setLevel(logging.DEBUG)
    elif args.verbose:
        LOG.setLevel(logging.INFO)
    else:
        LOG.setLevel(logging.WARNING)

    LOG.debug("args={}".format(args))

    KTEXT = args.ktext

    try:
        REPO_A = pygit2.Repository(args.repo_a)
    except KeyError:
        sys.stderr.write('Unable to find repo {}\n'.format(args.repo_a))
        sys.exit(1)
    try:
        REPO_B = pygit2.Repository(args.repo_b)
    except KeyError:
        sys.stderr.write('Unable to find repo {}\n'.format(args.repo_b))
        sys.exit(1)

                        # Compare which branches?
    branch_a_local  = sorted(list(branch_list_iter(REPO_A, _LOCAL)))
    branch_a_remote = sorted(list(branch_list_iter(REPO_A, _REMOTE)))
    branch_b_local  = sorted(list(branch_list_iter(REPO_B, _LOCAL)))
    branch_b_remote = sorted(list(branch_list_iter(REPO_B, _REMOTE)))

    ec = 2

    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug('== branches ==')
        LOG.debug('repo_a local  : {}'.format(' '.join(branch_a_local)))
        LOG.debug('repo_a remote : {}'.format(' '.join(branch_a_remote)))
        LOG.debug('repo_b local  : {}'.format(' '.join(branch_b_local)))
        LOG.debug('repo_b remote : {}'.format(' '.join(branch_b_remote)))

    if branch_a_local == branch_b_local:
        ec = compare_branch_lists( branch_a_local, _LOCAL, _LOCAL
                                 , "Comparing a.local to b.local")
    elif branch_a_local == branch_b_remote:
        ec = compare_branch_lists( branch_a_local, _LOCAL, _REMOTE
                                 , "Comparing a.local to b.remote")
    elif branch_a_remote == branch_b_local:
        ec = compare_branch_lists( branch_a_remote, _REMOTE, _LOCAL
                                 , "Comparing a.remote to b.local")
    elif branch_a_remote == branch_b_remote:
        ec = compare_branch_lists( branch_a_remote, _LOCAL, _REMOTE
                                 , "Comparing a.remote to b.remote")
    elif ("master" in branch_a_local) and ("master" in branch_b_local):
        if args.branch:
            ec = compare_branch_lists( ["master"] +  args.branch, _LOCAL, _LOCAL
                                 , "Comparing a.local.master to b.local.master")
        else:
            ec = compare_branch_lists( ["master"], _LOCAL, _LOCAL
                                 , "Comparing a.local.master to b.local.master")
    else:
        LOG.error("Branch lists differ too much to compare anything.")
        LOG.error("Cannot even find 'master' in both repo_a and repo_b.")
        LOG.error('== branches ==')
        LOG.error('repo_a local  : {}'.format(' '.join(branch_a_local)))
        LOG.error('repo_a remote : {}'.format(' '.join(branch_a_remote)))
        LOG.error('repo_b local  : {}'.format(' '.join(branch_b_local)))
        LOG.error('repo_b remote : {}'.format(' '.join(branch_b_remote)))
        ec = 1

    if COUNTS:
        level = logging.WARNING if ec else logging.INFO
        LOG.log(level, "")
        LOG.log(level, "Summary:")
        for k in sorted(COUNTS.keys()):
            LOG.log(level, "{:<25}: {:>4} {}"
                    .format( k
                           , COUNTS[k]
                           , "fatal" if MISMATCH_EXIT_CODE[k] else ""))

    sys.exit(ec)


def _argparse():
    """Pull args and repo paths out of argv.

    Return an args object.
    """
    desc = "Compare two Git repos. Identify what differs where."
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument('-v', '--verbose', default=False
                        , action="store_true"
                        , help="Report INFO-level progress")
    parser.add_argument('--debug', default=False
                        , action="store_true"
                        , help="Report DEBUG-level progress")
    parser.add_argument('--ktext', default=False
                        , action="store_true"
                        , help="Permit sha1 difference due to RCS $keyword: expansion $")

    parser.add_argument('--branch', metavar='branch', nargs=1
            , help='one extra local branch to compare with master when branch lists do not match')

    parser.add_argument('repo_a', metavar='repo_a')
    parser.add_argument('repo_b', metavar='repo_b')

    args = parser.parse_args()
    return args


def _abbrev(sha1):
    """Shorten our output noise."""
    return sha1[:7]


def branch_list_iter(repo, prefix=_LOCAL):
    """Return all known branches as a list of strings."""
    assert (0 == len(prefix)) or prefix.endswith("/")
    for ref in repo.listall_references():
        if ref.startswith(prefix):
            r = ref[len(prefix):]
            if r == "HEAD":
                continue
            yield r


def compare_branch_lists(short_branch_name_list, prefix_a, prefix_b, comment):
    """Iterate through branch references, find and report differences."""
    LOG.info(comment)
    ec = 0
    for short_branch_name in short_branch_name_list:
        ref_a    = prefix_a + short_branch_name
        ref_b    = prefix_b + short_branch_name
        try:
            pg_ref_a = REPO_A.lookup_reference(ref_a)
        except KeyError:
            LOG.warning("Cannot lookup ref_a={} in REPO_A.".format(ref_a))
            return 1
        try:
            pg_ref_b = REPO_B.lookup_reference(ref_b)
        except KeyError:
            LOG.warning("Cannot lookup ref_b={} in REPO_B.".format(ref_b))
            return 1
        sha1_a   = p4gf_pygit2.ref_to_sha1(pg_ref_a)
        sha1_b   = p4gf_pygit2.ref_to_sha1(pg_ref_b)
        LOG.debug("Comparing branch references: a={sha1_a} b={sha1_b}"
                  " ref_a={ref_a} ref_b{ref_b}"
                     .format( sha1_a = _abbrev(sha1_a)
                            , sha1_b = _abbrev(sha1_b)
                            , ref_a  = ref_a
                            , ref_b  = ref_b))
        if sha1_a == sha1_b:
            LOG.info("Perfect head commit sha1 match: a={sha1_a} b={sha1_b}"
                     " ref_a={ref_a} ref_b={ref_b}"
                     .format( sha1_a = _abbrev(sha1_a)
                            , sha1_b = _abbrev(sha1_b)
                            , ref_a  = ref_a
                            , ref_b  = ref_b))
            continue
        else:
            LOG.warning("Mismatched head commit sha1s: a={sha1_a} b={sha1_b}"
                        " ref_a={ref_a} ref_b={ref_b}"
                        .format( sha1_a = _abbrev(sha1_a)
                               , sha1_b = _abbrev(sha1_b)
                               , ref_a  = ref_a
                               , ref_b  = ref_b))
            ec |= compare_history(pg_ref_a, pg_ref_b)
    return ec


def diff_iter(list_a, list_b):
    """Pull the next element off of a, b, or both."""
    matcher = SequenceMatcher( a        = list_a
                             , b        = list_b
                             , isjunk   = None
                             , autojunk = False )
    for opcode in matcher.get_opcodes():
                        # get_opcode() yields unnamed tuples. Grr.
                        # < o=tag, 1=i1, 2=i2, 3=j1, 4=j2 >
        if opcode[0] in ['equal', 'delete']:
            for aa in list_a[opcode[1]: opcode[2]]:
                yield aa
        elif opcode[0] == 'insert':
            for bb in list_b[opcode[3]: opcode[4]]:
                yield bb
        else:  # opcode[0] == 'replace'
            for aa in list_a[opcode[1]: opcode[2]]:
                yield aa
            for bb in list_b[opcode[3]: opcode[4]]:
                yield bb


#SUMMARY_COMMIT = "{commit_a:<7.7} {tree_a:<7.7}  {commit_b:<7.7} {tree_b:7.7}   {subject:<30.30}"

def diff_how(commit_a, commit_b):
    """Compare two commits and return a set of how they differ or match."""
    # pylint:disable=too-many-branches
    if not (commit_a and commit_b):
        return {DIFF_COMMIT_MISSING}

    if commit_a.sha1 == commit_b.sha1:
        return MATCH
    result = set([DIFF_COMMIT_SHA1])

                        # Commit message differs?
                        #
                        # Since we USE the commit message as the matching ID,
                        # there's no way these two commit messages can
                        # differ by more than whitespace.
                        # (Could change in the future if we further
                        # soften id_str()).
    if commit_a.message != commit_b.message:
        result.add(DIFF_MESSAGE_WHITESPACE_ONLY)
        if ( (commit_a.sha1 in SHA1_WITH_COPIED_FROM_PERFORCE and
              not commit_b.sha1 in SHA1_WITH_COPIED_FROM_PERFORCE) or
             (not commit_a.sha1 in SHA1_WITH_COPIED_FROM_PERFORCE and
              commit_b.sha1 in SHA1_WITH_COPIED_FROM_PERFORCE)):
            result.add(DIFF_MESSAGE_COPIED_FROM_P4)

    if commit_a.encoding != commit_b.encoding:
        result.add(DIFF_COMMIT_ENCODING)

    if commit_a.author_offset != commit_b.author_offset:
        result.add(DIFF_COMMIT_AUTHOR_OFFSET)
    if commit_a.committer_offset != commit_b.committer_offset:
        result.add(DIFF_COMMIT_COMMITTER_OFFSET)

    if commit_a.parent_list != commit_b.parent_list:
        par_id_list_a = [SHA1_TO_ID_STR_A.get(p) for p in commit_a.parent_list]
        par_id_list_b = [SHA1_TO_ID_STR_B.get(p) for p in commit_b.parent_list]

                        # sha1 mismatch, but id match. Just inheriting a
                        # mismatch from some ancestor.
        if par_id_list_a == par_id_list_b:
            result.add(DIFF_PARENT_SHA1_MISMATCH)

                        # Same parent lists (by id), but in different order.
        elif sorted(par_id_list_a) == sorted(par_id_list_b):
            result.add(DIFF_PARENT_ID_ORDER)

                        # At least one parent is missing from one side.
        elif len(par_id_list_a) != len(par_id_list_b):
            result.add(DIFF_PARENT_MISSING)

                        # At least one parent doesn't match the
                        # other side at all.
        else:
            result.add(DIFF_PARENT_ID_MISMATCH)

                        # Tree differs.
                        #
                        # Do elsewhere: dive into tree and list exactly
                        # which files differ and how.
    if commit_a.tree != commit_b.tree:
        result.add(DIFF_TREE_MISMATCH)

                        # If commit sha1 did not match, then at least one of
                        # the above tests should have identified the cause of
                        # the mismatch. If not, then there's a source we do not
                        # yet know about.
                        #
    assert result
    return result


def _any_parent_mismatch(diff_how_set):
    """Does the diff_how() result set contain any parent mismatch other than
    parent.sha1?
    """
    for x in [ DIFF_PARENT_ID_ORDER
             , DIFF_PARENT_MISSING
             , DIFF_PARENT_ID_MISMATCH ]:
        if x in diff_how_set:
            return True
    return False


def message_to_subject(message):
    """Return first line of a commit message."""
    try:
        return message[:message.index('\n')].replace('\r','')
    except ValueError:
        return message


def dump_parent_list(list_a, list_b):
    """Print parent list to log."""
    q_sha1_a = deque(list_a)
    q_sha1_b = deque(list_b)
    q_id_a   = deque([SHA1_TO_ID_STR_A.get(p) for p in list_a])
    q_id_b   = deque([SHA1_TO_ID_STR_B.get(p) for p in list_b])
    for par_id in diff_iter(list(q_id_a), list(q_id_b)):
        sha1_a = None
        sha1_b = None
        if q_id_a and q_id_a[0] == par_id:
            sha1_a = q_sha1_a.popleft()
            q_id_a.popleft()
            message = REPO_A[sha1_a].message
        if q_id_b and q_id_b[0] == par_id:
            sha1_b = q_sha1_b.popleft()
            q_id_b.popleft()
            message = REPO_B[sha1_b].message

        LOG.warning(" parent {sha1_a:7.7}          {sha1_b:7.7}         {subject:<80.80}"
                    .format( sha1_a  = sha1_a if sha1_a else ""
                           , sha1_b  = sha1_b if sha1_b else ""
                           , subject = message_to_subject(message)
                           ))


def compare_commits(commit_a, commit_b):
    """Compare two commits and dump result to log."""
    ec = 0
    r = diff_how(commit_a, commit_b)
    msg = "commit  {sha1_a:7.7} {tree_a:7.7}  {sha1_b:7.7} {tree_b:7.7} {subject:<80.80} {diff}" \
          .format( sha1_a = commit_a.sha1 if commit_a else ""
                 , tree_a = commit_a.tree if commit_a else ""
                 , sha1_b = commit_b.sha1 if commit_b else ""
                 , tree_b = commit_b.tree if commit_b else ""
                 , subject = message_to_subject(commit_a.message if commit_a else commit_b.message)
                 , diff   = ' '.join(sorted(r))
                 )
    level = logging.WARNING if r else logging.INFO
    LOG.log(level, msg)
    for rr in r:
        ec |= MISMATCH_EXIT_CODE[rr]
        COUNTS[rr] += 1

    if _any_parent_mismatch(r):
        dump_parent_list(commit_a.parent_list, commit_b.parent_list)

    if DIFF_TREE_MISMATCH in r:
        t = diff_tree(commit_a.tree, commit_b.tree, REPO_A, REPO_B)

                        # Dump log until we can get a refined summary.
        for row in t:
            mode_a = "{:06o}".format(row.a.mode) if row.a else ""
            mode_b = "{:06o}".format(row.b.mode) if row.b else ""
            LOG.warning(" tree    {mode_a:6.6} {sha1_a:7.7}   {mode_b:6.6}"
                        " {sha1_b:7.7} {gwt_path:<80} {diff}"
                     .format( sha1_a   = row.a.sha1 if row.a else ""
                            , mode_a   = mode_a
                            , sha1_b   = row.b.sha1 if row.b else ""
                            , mode_b   = mode_b
                            , gwt_path = row.a.gwt_path if row.a else row.b.gwt_path
                            , diff     = ' '.join(sorted(row.how))
                            ))
            for rr in row.how:
                ec |= MISMATCH_EXIT_CODE[rr]
                COUNTS[rr] += 1
    return ec


def flatten_tree_1(gwt_path_tree, tree, result_list, tree_q, repo):
    """Process a single pygit2 tree object.

    Translate any files to elements appended to result_list.
    Translate any directories to (gwt_path, Tree) tuples appended to tree_q.
    """
    for tree_entry in tree:
        gwt_element_path = os.path.join(gwt_path_tree, tree_entry.name)
        git_file = GitFile( gwt_path = gwt_element_path
                          , mode     = tree_entry.filemode
                          , sha1     = p4gf_pygit2.object_to_sha1(tree_entry) )
        result_list.append(git_file)

        if tree_entry.filemode == 0o040000:
            tree_q.append( ( gwt_element_path
                           , p4gf_pygit2.tree_object(repo, tree_entry)
                           ) )


def flatten_tree(tree_sha1, repo):
    """Recurse through a tree and return it as a list of GitFile named tuples.

    Returned list includes directories! Sometimes we get a tree object with
    missing/extra leading zeroes that won't rerepo.
    """
    result_list = []        # of GitFile
    tree_q      = deque()   # of (gwt_par_path, Tree) tuples


                        # Seed the walk with root-level entries.
    flatten_tree_1( gwt_path_tree   = ""
                  , tree            = repo[tree_sha1]
                  , result_list     = result_list
                  , tree_q          = tree_q
                  , repo            = repo )

                        # "Recurse" into directories.
    while tree_q:
        (gwt_path_tree, tree) = tree_q.popleft()
        flatten_tree_1( gwt_path_tree   = gwt_path_tree
                      , tree            = tree
                      , result_list     = result_list
                      , tree_q          = tree_q
                      , repo            = repo )
    return sorted(result_list, key=operator.attrgetter('gwt_path'))


def _both_are_trees(git_file_a, git_file_b):
    """Are both a dn b entries for directory trees, not files of any sort?

    Return False if either is not a 040000 directory.
    Return False if either missing.
    """
    return (    git_file_a and git_file_a.mode == 0o040000
            and git_file_b and git_file_b.mode == 0o040000)


def _both_are_symlinks(git_file_a, git_file_b):
    """Are both a dn b entries for symlinks?

    Return False if either is not a 120000 symlink.
    Return False if either missing.
    """
    return (    git_file_a and git_file_a.mode == 0o120000
            and git_file_b and git_file_b.mode == 0o120000)


def _tree_mismatch_internal(git_file_a, git_file_b, repo_a, repo_b):
    """Do these two ls-tree objects differ by sha1 but match by actual
    ls-tree values?

    Probably due to GF-909 ls-tree object lacks leading zeros.
    """
                        # Why are you wasting time byte-by-byte comparing two
                        # ls-tree objects that already match by sha1?
    assert git_file_a.sha1 != git_file_b.sha1

    bytes_orig_a = repo_a[git_file_a.sha1].read_raw()
    bytes_orig_b = repo_b[git_file_b.sha1].read_raw()
    bytes_stripped_a = bytes_orig_a.replace(b'040000', b'40000')
    bytes_stripped_b = bytes_orig_b.replace(b'040000', b'40000')
    return bytes_stripped_a == bytes_stripped_b


def chomp(s):
    """Remove one trailing newline."""
    if len(s) and s[-1] == "\n":
        return s[:-1]
    else:
        return s


def _symlink_mismatch_trailing_newline(git_file_a, git_file_b, repo_a, repo_b):
    """Do these two symlink objects differ only by a trailing newline?
    Perforce strips one trailing newline from the symlink content during
    'p4 print', and inserts an extra newline during 'p4 add' but NOT during
    'p4 unzip'.
    """
    if not _both_are_symlinks(git_file_a, git_file_b):
        return False

    bytes_orig_a = repo_a[git_file_a.sha1].read_raw()
    bytes_orig_b = repo_b[git_file_b.sha1].read_raw()

    try:
        text_orig_a = bytes_orig_a.decode()
        text_orig_b = bytes_orig_b.decode()
    except UnicodeDecodeError:
        LOG.debug("UnicodeDecodeError so not checking for symlink.trailing_newline: {}"
                  .format(git_file_a.gwt_path))
        return False

    text_stripped_a = chomp(text_orig_a)
    text_stripped_b = chomp(text_orig_b)

    return text_stripped_a == text_stripped_b


def _file_or_dir_missing(git_file_a, git_file_b):
    """Return either file.missing.x or dir.missing.x
    If you want to check for something deeper (like submodule)
    do that before calling _file_or_dir_missing()
    """
    if git_file_a:
        if git_file_a.mode == 0o040000:
            return {DIR_MISSING_B}
        else:
            return {FILE_MISSING_B}
    else:
        if git_file_b.mode == 0o040000:
            return {DIR_MISSING_A}
        else:
            return {FILE_MISSING_A}


def diff_tree_how( git_file_a,  git_file_b
                 , repo_a,      repo_b
                 , tree_sha1_a, _tree_sha1_b
                 ):
    """Do two git-ls-tree entries match? If not, what differs?

    Return empty set if match.
    Return set of FILE_XXX constants if not.
    """
    if not git_file_a:
        return _file_or_dir_missing(git_file_a, git_file_b)
    if not git_file_b:
                        # 160000 files without a .gitmodules are broken,
                        # not guaranteed to re-repo.
        if git_file_a.mode == 0o160000:
            tree_a = repo_a[tree_sha1_a]
            if ".gitmodules" not in tree_a:
                return {FILE_BAD_SUBMODULE_A}
        return _file_or_dir_missing(git_file_a, git_file_b)

    result = set()

    if git_file_a.sha1 != git_file_b.sha1:
        if (    _both_are_trees(git_file_a, git_file_b)
            and _tree_mismatch_internal( git_file_a, git_file_b
                                       , repo_a, repo_b )):
            result.add(TREE_SHA1_INTERNAL)
        elif KTEXT and diff_ktext_only(git_file_a, git_file_b, repo_a, repo_b):
            result.add(FILE_KTEXT_SHA1)
        elif _symlink_mismatch_trailing_newline(git_file_a, git_file_b, repo_a, repo_b):
            result.add(FILE_SYMLINK_TRAILING_NEWLINE)
        else:
            result.add(FILE_SHA1)
    if git_file_a.mode != git_file_b.mode:
        result.add(FILE_MODE)
    return result


def diff_ktext_only(git_file_a, git_file_b, repo_a, repo_b):
    """Do two file revisions differ only between the buck signs
    of a ktext keyword like "$Date: 2014/01/01 $" or "$Date$"
    """
    bytes_orig_a = repo_a[git_file_a.sha1].read_raw()
    bytes_orig_b = repo_b[git_file_b.sha1].read_raw()

    try:
        text_orig_a = bytes_orig_a.decode()
        text_orig_b = bytes_orig_b.decode()
    except UnicodeDecodeError:
        LOG.debug("UnicodeDecodeError so not checking for ktext: {}"
                  .format(git_file_a.gwt_path))
        return False

    rcs = re.compile(r'\$[A-Za-z]+\s*(:[^$\n]*)?\$')
    text_stripped_a = rcs.sub("$RCS-KEYWORD$", text_orig_a)
    text_stripped_b = rcs.sub("$RCS-KEYWORD$", text_orig_b)

    return text_stripped_a == text_stripped_b


def diff_tree( tree_sha1_a, tree_sha1_b, repo_a, repo_b
             , include_identical = False):
    """Compare two git trees and return an iterator/generator of
    DiffTreeRow(how, GitFile a, GitFile b) tuples.
    """

    tree_flat_a = flatten_tree(tree_sha1_a, repo_a)
    tree_flat_b = flatten_tree(tree_sha1_b, repo_b)

    gwt_path_list_a = [x.gwt_path for x in tree_flat_a]
    gwt_path_list_b = [x.gwt_path for x in tree_flat_b]
    gwt_path_to_git_file_a = {x.gwt_path: x for x in tree_flat_a}
    gwt_path_to_git_file_b = {x.gwt_path: x for x in tree_flat_b}

    row_list = []
    for gwt_path in diff_iter(gwt_path_list_a, gwt_path_list_b):
        git_file_a = gwt_path_to_git_file_a.get(gwt_path)
        git_file_b = gwt_path_to_git_file_b.get(gwt_path)
        if git_file_b is None and git_file_a.mode == 0o040000 and \
           git_file_a.sha1 == _EMPTY_TREE_SHA1:
            row_list.append(DiffTreeRow({DIR_MISSING_EMPTY}, git_file_a, git_file_b))
            continue

        result = diff_tree_how( git_file_a,  git_file_b
                              , repo_a,      repo_b
                              , tree_sha1_a, tree_sha1_b
                              )
        if (not result) and (not include_identical):
            continue
        row_list.append(DiffTreeRow(result, git_file_a, git_file_b))

                        # Remove any directory diffs due to
                        # contained file diffs.
    result = strip_boring_dir_diffs(row_list)
    return result


def strip_boring_dir_diffs(row_list):
    """Return the subset of row_list that is everything except
    boring "this directory differs because a contained file differs"
    """
                        # Pass 1: all known ancestor directories that contain
                        # no tree diffs of their own. A tree with nothing but
                        # FILE_SHA1 due to child differences is boring.
    boring_dir_set = set()
    for f in row_list:
        if _is_dir(f) and f.how.issubset(set([FILE_SHA1])):
            continue

        gwt_path          = f.a.gwt_path if f.a else f.b.gwt_path
        path_elements = gwt_path.split("/")[:-1]
        path = ""
        for e in path_elements:
            path = os.path.join(path, e)
            boring_dir_set.add(path)

                        # Pass 2: copy files and non-boring directories
                        # to result list.
    unboring = []
    for f in row_list:
        if _is_dir(f):
            gwt_path = f.a.gwt_path if f.a else f.b.gwt_path
            if gwt_path in boring_dir_set:
                continue
        unboring.append(f)
    return unboring


def _is_dir(diff_tree_row):
    """Does this DiffTreeRow describe only a difference in directory sha1s?

    Differentiate between "this directory differs because a child differs" and
    "this directory differs yet no child differs". The former is common,
    boring, and unworthy of reporting. The latter is rare, usually due to
    leading zeroes within the Git ls-tree objects, and quite important to know.
    """
    return (    diff_tree_row.a and diff_tree_row.a.mode == 0o040000
            and diff_tree_row.b and diff_tree_row.b.mode == 0o040000)


def compare_history(pg_ref_a, pg_ref_b):
    """Collect each repo's history and look for mismatched commit sha1s,
    parents, trees.
    """
    global SHA1_TO_ID_STR_A
    global SHA1_TO_ID_STR_B

    history_a = [c for c in commit_iter(REPO_A, pg_ref_a)]
    history_b = [c for c in commit_iter(REPO_B, pg_ref_b)]

    SHA1_TO_ID_STR_A    = {c.sha1: c.id_str() for c in history_a}
    SHA1_TO_ID_STR_B    = {c.sha1: c.id_str() for c in history_b}
    id_str_to_commit_a  = {c.id_str(): c for c in history_a}
    id_str_to_commit_b  = {c.id_str(): c for c in history_b}

                        # Assume that most of history will appear in both repos.
    list_a   = [c.id_str() for c in history_a]
    list_b   = [c.id_str() for c in history_b]

    ec = 0
    for id_str in diff_iter(list_a, list_b):
        commit_a = id_str_to_commit_a.get(id_str)
        commit_b = id_str_to_commit_b.get(id_str)
        ec |= compare_commits(commit_a, commit_b)
    return ec


def commit_iter(pg_repo, pg_ref):
    """Return iterator/generator of Commit objects, one for
    pg_ref and all its ancestors within pg_repo.
    """
    for pg_commit in pg_repo.walk(p4gf_pygit2.ref_to_target(pg_ref), pygit2.GIT_SORT_TOPOLOGICAL):
        yield Commit.from_pygit(pg_repo, pg_commit)


def sanitize_whitespace(text):
    """Return text with all runs of whitespace converted to
    single space characters. Help ignore or identify differences
    in whitespace only.
    """
    return RE_WHITESPACE.sub(" ", text)

def sanitize_copied_from_p4(text, sha1):
    """Return text with the 'Copied from Perforce' block removed.
    This helps ignore or identify differences arising when
    when pushing a commit which is then pulled into another repo.
    This latter commit will have the 'Copied from Perforce' block added.
    """
    copied_from_p4 = re.search(COPIED_FROM_PERFORCE, text)
    if copied_from_p4:
        SHA1_WITH_COPIED_FROM_PERFORCE.append(sha1)
        return text.replace(copied_from_p4.group(1),'')
    else:
        return text

# -----------------------------------------------------------------------------

GitFile     = namedtuple('GitFile', ['gwt_path', 'mode', 'sha1'])
DiffTreeRow = namedtuple('DiffTreeRow', ['how', 'a', 'b'])

# -----------------------------------------------------------------------------


class Commit:

    """A single Git commit."""

    def __init__( self
                , sha1             = None
                , tree             = None
                , parent_list      = None
                , subject          = None
                , message          = None
                , commit_time      = None
                , encoding         = None
                , author_offset    = None
                , committer_offset = None
                ):
        # pylint:disable=too-many-arguments
        self.sha1             = sha1
        self.tree             = tree
        self.parent_list      = parent_list
        self.subject          = subject
        self.message          = message
        self.commit_time      = commit_time
        self.encoding         = encoding
        self.author_offset    = author_offset
        self.committer_offset = committer_offset

    def __str__(self):
        lines = [
                  "sha1             : {}".format(self.sha1        )
                , "tree             : {}".format(self.tree        )
                , "parent_list      : {}".format(self.parent_list )
                , "subject          : {}".format(self.subject     )
                , "commit_time      : {}".format(self.commit_time )
                , "author_offset    : {}".format(self.author_offset)
                , "committer_offset : {}".format(self.author_offset)
                ]
        return '\n'.join(lines)

    @staticmethod
    def from_pygit(pg_repo, pg_commit):
        """Factory to convert pygit2 objects to our own."""
        message = pg_commit.message
        encoding = pg_commit.message_encoding or 'UTF-8'
        if '\ufffd' in message:
            # Original message failed to decode using UTF-8
            _, raw_commit = pg_repo.read(pg_commit.oid)
            try:
                commit_text = raw_commit.decode('latin_1')
                encoding = 'latin_1'
                message = commit_text[commit_text.index('\n\n')+2:]
                # convert to UTF-8 for easier comparison
                message = message.encode('UTF-8').decode('UTF-8')
            except UnicodeDecodeError:
                LOG.exception('commit message decoding failed')
        subj_end = message.find('\n')
        if 0 < subj_end:
            subject = message[:subj_end]
        else:
            subject = message

        return Commit( sha1             = p4gf_pygit2.object_to_sha1(pg_commit)
                     , tree             = p4gf_pygit2.object_to_sha1(pg_commit.tree)
                     , parent_list      = [p4gf_pygit2.object_to_sha1(p) for p in pg_commit.parents]
                     , subject          = subject
                     , message          = message
                     , commit_time      = pg_commit.commit_time
                     , encoding         = encoding
                     , author_offset    = pg_commit.author.offset
                     , committer_offset = pg_commit.committer.offset
                     )

    def id_str(self):
        """Return a string that kinda-sorta uniquely identifies this commit.

        Usually commit date + subject is enough to find a matching commit in
        either repo. But some repos have mechanical and repeating subjects
        that might recur within one second (really? Show me one.)

        We _definitely_ see CRLF/CR/LF and other whitespace garbling in rerepo.
        Sanitize whitespace so that we have a much better chance of finding a
        rerepo-garbled match.
        """
        msg = sanitize_copied_from_p4(self.message, self.sha1)
        msg = sanitize_whitespace(msg)
        return " ".join([ str(self.commit_time)
                        , str(len(msg))
                        , msg
                        ])


DIFF_COMMIT_SHA1                    = 'commit.sha1'
DIFF_COMMIT_MISSING                 = 'commit.missing'
DIFF_COMMIT_ENCODING                = 'commit.encoding'
DIFF_MESSAGE_WHITESPACE_ONLY        = 'message.whitespace'
DIFF_MESSAGE_COPIED_FROM_P4         = 'message.copied.from.p4'

DIFF_COMMIT_AUTHOR_OFFSET           = 'commit.author.offset'
DIFF_COMMIT_COMMITTER_OFFSET        = 'commit.committer.offset'

DIFF_PARENT_ID_ORDER                = 'parent.id_order'
DIFF_PARENT_MISSING                 = 'parent.missing'

                        # Completely different parent commit.
DIFF_PARENT_ID_MISMATCH             = 'parent.id_mismatch'

                        # Parent IDs match, but sha1s do not. We're
                        # just inheriting (and propagating) a diff
                        # from our ancestry.
DIFF_PARENT_SHA1_MISMATCH           = 'parent.sha1'
DIFF_TREE_MISMATCH                  = 'tree'

                        # How a single file in git-ls-tree can differ.
DIR_MISSING_A                       = 'dir.missing.a'
DIR_MISSING_B                       = 'dir.missing.b'
FILE_MISSING_A                      = 'file.missing.a'
FILE_MISSING_B                      = 'file.missing.b'
FILE_MODE                           = 'file.mode'
FILE_SHA1                           = 'file.sha1'
FILE_BAD_SUBMODULE_A                = 'file.bad.submodule.a'
FILE_KTEXT_SHA1                     = 'file.ktext.sha1'
FILE_SYMLINK_TRAILING_NEWLINE       = 'file.symlink.trailing_newline'
TREE_SHA1_INTERNAL                  = 'ls-tree.leading_zero'

DIR_MISSING_EMPTY                   = 'dir.missing.empty'

MATCH                               = set()

                        # Some mismatches won't cause us to return non-zero.
                        # They're innocuous or just known limitations of
                        # current Git Fusion.
                        #
                        # Others are NEVER permissible, always cause non-zero
                        # exit code.
                        #
MISMATCH_EXIT_CODE = {
      DIFF_COMMIT_SHA1                    : 0
    , DIFF_COMMIT_MISSING                 : 1
    , DIFF_COMMIT_ENCODING                : 0
    , DIFF_MESSAGE_COPIED_FROM_P4         : 0
    , DIFF_MESSAGE_WHITESPACE_ONLY        : 0
    , DIFF_COMMIT_AUTHOR_OFFSET           : 0
    , DIFF_COMMIT_COMMITTER_OFFSET        : 0
    , DIFF_PARENT_ID_ORDER                : 1
    , DIFF_PARENT_MISSING                 : 1
    , DIFF_PARENT_ID_MISMATCH             : 1
    , DIFF_PARENT_SHA1_MISMATCH           : 0
    , DIFF_TREE_MISMATCH                  : 0

    , DIR_MISSING_A                       : 0
    , DIR_MISSING_B                       : 0
    , FILE_MISSING_A                      : 1
    , FILE_MISSING_B                      : 1
    , FILE_MODE                           : 1
    , FILE_SHA1                           : 1
    , FILE_BAD_SUBMODULE_A                : 0
    , FILE_KTEXT_SHA1                     : 0
    , FILE_SYMLINK_TRAILING_NEWLINE       : 0
    , TREE_SHA1_INTERNAL                  : 0
    , DIR_MISSING_EMPTY                   : 0
}

if __name__ == "__main__":
    main()

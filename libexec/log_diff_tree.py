#! /usr/bin/env python3.3
"""
Write a 'git log' output to stdout, following each commit with what
was changed in that commit.

--generate can generate T4 code but will fail for:
    -- 100755 +x files
    -- multiple orphan (no parent) commits

"""
import argparse
from   collections import defaultdict
import logging
import sys

import pygit2

import repo_compare

                        # debug   : internal tracking
                        # info    : verbose, matching commits included
                        #           for context of a mismatched push
                        # warning : something differs
                        # error   : cannot diff
LOG = logging.getLogger()

REPO = None
BRANCH_TO_SHA1 = None
SHA1_TO_BRANCH_NAME_LIST = None

EMPTY_TREE_SHA1 = '4b825dc642cb6eb9a060e54bf8d69288fbee4904'
HEAD            = None

ALL_FILES = False


def main():
    """Do the thing."""
    args = _argparse()

    logging.basicConfig(format="%(message)s", stream=sys.stdout)
    if args.debug:
        LOG.setLevel(logging.DEBUG)
    else:
        LOG.setLevel(logging.INFO)

    LOG.debug("args={}".format(args))

    global REPO
    global BRANCH_TO_SHA1
    global SHA1_TO_BRANCH_NAME_LIST
    global ALL_FILES

    if args.all_files:
        ALL_FILES = True

    REPO = pygit2.Repository('.')
    BRANCH_TO_SHA1 = { bn:to_sha1(bn) for bn in args.branch }
    SHA1_TO_BRANCH_NAME_LIST = defaultdict(list)
    for bn, sha1 in BRANCH_TO_SHA1.items():
        SHA1_TO_BRANCH_NAME_LIST[sha1].append(bn)

    for commit in git_rev_list_iter(args.branch):
        if args.generate:
            generate_code(commit)
        else:
            log_commit(commit)


def _argparse():
    """Pull args and git-branch-names out of argv

    Return an args object.
    """
    desc = "Print a git log that includes what changed in each commit."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--debug', default=False
                       , action="store_true"
                       , help="Report DEBUG-level progress")
    parser.add_argument('--generate', '-g', default=False
                        , action="store_true"
                        , help="Generate T4 code")
    parser.add_argument('--all-files', '-a', default=False
                        , action="store_true"
                        , help="report all files, not just diffs")

    parser.add_argument('branch', nargs='*', default=['master'])

    args = parser.parse_args()
    return args


def to_sha1(branch_name):
    """Convert a branch ref to its sha1 string."""
    if branch_name.startswith("refs/heads/"):
        bn = branch_name
    else:
        bn = "refs/heads/" + branch_name
    commit = REPO.revparse_single(bn)
    return commit.hex


def git_rev_list_iter(branch_list):
    """git-rev-list [branches]

    Produce iterator/generator of pygit2.Commit objects reachable
    by by supplied branch names.
    """

    walker = None

    for branch in branch_list:
        bn = "refs/heads/" + branch if not branch.startswith("refs/heads/") else branch
        commit = REPO.revparse_single(bn)

        if not walker:
            walker = REPO.walk(commit.oid, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE)
            LOG.debug("walker created: {sha1:7.7} {branch}"
                      .format(sha1=commit.hex, branch=bn))
        else:
            walker.push(commit.oid)
            LOG.debug("walker added  : {sha1:7.7} {branch}"
                      .format(sha1=commit.hex, branch=bn))

    return walker


def log_commit(commit):
    """Show a single Git commit's info.

    Just the parts that Zig needs.
    """
    decorate = ""
    if commit.hex in SHA1_TO_BRANCH_NAME_LIST:
        decorate = " ".join(SHA1_TO_BRANCH_NAME_LIST[commit.hex])
    LOG.info("{:7.7} {} {:40.40}".format(commit.hex, decorate, _subject(commit)))
    LOG.info("par: {}".format(" ".join(["{:7.7}".format(p.hex) for p in commit.parents])))

    if not commit.parents:
        for gitfile in repo_compare.flatten_tree(commit.tree.hex, REPO):
            if gitfile.mode == 0o040000:
                continue
            LOG.info("{action:1.1} {mode:6.6} {sha1:3.3} {gwt_path}"
                     .format( action   = 'A'
                            , mode     = "{:06o}".format(gitfile.mode)
                            , sha1     = gitfile.sha1
                            , gwt_path = gitfile.gwt_path ))

    else:
        for dtr in repo_compare.diff_tree(
                  tree_sha1_a       = commit.parents[0].tree.hex
                , tree_sha1_b       = commit.tree.hex
                , repo_a            = REPO
                , repo_b            = REPO
                , include_identical = ALL_FILES ):
            action = '-'
            if dtr.b:
                gwt_path = dtr.b.gwt_path
                mode     = "{:06o}".format(dtr.b.mode)
                sha1     = dtr.b.sha1
                if dtr.a:
                    if dtr.a.mode != dtr.b.mode:
                        action = 'T'
                    if dtr.a.sha1 != dtr.b.sha1:
                        action = 'M'
                else:
                    action = 'A'
            else:
                gwt_path = dtr.a.gwt_path
                mode     = ""
                sha1     = ""
                action   = 'D'

            LOG.info("{action:1.1} {mode:6.6} {sha1:3.3} {gwt_path}"
                     .format( action   = action
                            , mode     = mode
                            , sha1     = sha1
                            , gwt_path = gwt_path ))
    LOG.info("")


def _file_list(commit):
    """Return a list of ["f1_sha1", "f1_gwt",  "f2_sha1", "f2_gwt", ...]"""

    file_list = []

    if commit.parents:
        for dtr in repo_compare.diff_tree(
              tree_sha1_a       = commit.parents[0].tree.hex
            , tree_sha1_b       = commit.tree.hex
            , repo_a            = REPO
            , repo_b            = REPO
            , include_identical = ALL_FILES ):

            if dtr.b:
                        # I guess diff_tree() doesn't strip ALL directories.
                        # I just saw some go by.
                if dtr.b.mode == 0o040000:
                    continue

                gwt_path = dtr.b.gwt_path
                mode     = "{:06o}".format(dtr.b.mode)
                x_bit    = {
                             "100644" : " "
                           , "100755" : "*"
                           , "120000" : "@"
                           }.get(mode)
                fsha1     = dtr.b.sha1
            else:
                        # Delete file.
                gwt_path = dtr.a.gwt_path
                x_bit    = " "
                fsha1    = "---"    # file sha1 "---" means "delete"

            file_list.append("{fsha1:3.3}{x}".format(fsha1=fsha1, x=x_bit))
            file_list.append(gwt_path)

    else:               # No parents? Add all files in the ls-tree.
        for gitfile in repo_compare.flatten_tree(commit.tree.hex, REPO):
            if gitfile.mode == 0o040000:
                continue
            file_list.append("{:3.3}".format(gitfile.sha1))
            file_list.append(gitfile.gwt_path)

    return file_list


def generate_code(commit):
    """Show a single line of T4 code for this commit."""
    global HEAD

                        # Merge commit.
    commit_subject = _subject(commit)
    file_list      = _file_list(commit)
    if file_list:
        file_string = ', "{}"'.format(" ".join(file_list))
    else:
        file_string = ""

    if 2 <= len(commit.parents):
        parent_string  = " ".join( ["{:7.7}".format(par.hex)
                                    for par in commit.parents])


        LOG.info('gm "{parent_list:15}", "{commit_sha1:7.7}"{file_list:40} # {msg}"'
                 .format( parent_list = parent_string
                        , commit_sha1 = commit.hex
                        , file_list   = file_string + ";"
                        , msg         = commit_subject
                        ))
    else:
                        # Non-Merge commit. Make sure its one parent is HEAD
                        # before committing.
        if commit.parents:
            if commit.parents[0].hex != HEAD:
                LOG.info('co "{:7.7}";'.format(commit.parents[0].hex))

                        # Orphan/root commit. No parent.
        if not commit.parents:
            if HEAD:
                LOG.info('orphan;')
                # raise RuntimeError("--generate does not know how to create"
                #                    " multiple parent-less commits:"
                #                    " commit {sha1:7.7}"
                #                    .format(sha1=commit.hex))

        LOG.info('gc  {parent_list:15}   "{commit_sha1:7.7}"{file_list:40} # {msg}"'
                 .format( parent_list = ""
                        , commit_sha1 = commit.hex
                        , file_list   = file_string + ";"
                        , msg         = commit_subject
                        ))

    HEAD = commit.hex


def _subject(commit):
    """Return the first line of a commit message."""
    return commit.message.splitlines()[0]


if __name__ == "__main__":
    main()

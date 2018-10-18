#! /usr/bin/env python3.3
"""ObjectType support constants, functions."""

import re

# Regular expression for parsing cached object filepath.
OBJPATH_COMMIT_REGEX = re.compile("/objects/repos/(?P<repo>[^/]+)/commits/(?P<slashed_sha1>[^-]+)"
                                  "-(?P<branch_id>[^,]+),(?P<change_num>\\d+)")
OBJPATH_TREE_REGEX = re.compile("/objects/trees/(?P<slashed_sha1>[^-]+)")
KEY_LAST_REGEX = re.compile("git-fusion-index-last-(?P<repo>[^,]+),(?P<branch_id>(.*))")
VALUE_LAST_REGEX = re.compile("(?P<change_num>\\d+),(?P<sha1>\\w{40})")
KEY_BRANCH_REGEX = re.compile("git-fusion-index-branch-(?P<repo>[^,]+),(?P<change_num>\\d+)," +
                              "(?P<branch_id>(.*))")


def run_p4files(p4, path):
    """Run p4 files on path and return depot paths of any files reported."""
    files = p4.run('files', '-e', path)
    return [f['depotFile'] for f in files if isinstance(f, dict) and 'depotFile' in f]

#! /usr/bin/env python3

# pylint doesn't want to see '-' in module names.  Until this module is
# renamed, any other invalid names will be masked.
# pylint:disable=invalid-name

"""preflight-commit-require-job.py.

A Git Fusion preflight-commit script that prohibits any 'git push' of a commit
that does not have at least one Perforce Job attached.

To use globally, edit //.git-fusion/p4gf_config to include these lines:
  [git-to-perforce]
  preflight-commit = /path/to/preflight-commit-require-job.py %jobs%

To use for just a specific repo, edit //.git-fusion/repos/<repo-name>/p4gf_config
to include these lines:
  [@repo]
  preflight-commit = /path/to/preflight-commit-require-job.py %jobs%
"""
import sys

JOBS = [x for x in sys.argv[1:] if x]

if not JOBS:
    sys.stderr.write('Jobs required\n')
    sys.exit(1)

sys.exit(0)

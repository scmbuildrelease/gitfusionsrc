#! /usr/bin/env python3.3
r"""Script for 'git filter-branch --parent-filter <this>

Linearizes history: only the first parent of any commit is retained.

Rewrites history: any time <orig_par_sha1> appears as a parent, change the
child commit to list <new_par_sha1> as its one and only parent.

Call like this:
    git filter-branch -f
     --parent-filter "/abs/path/to/linearizing_parent_filter.py <orig_par_sha1> <new_par_sha1>"
     -- --first-parent master

Just want to linearize history? Sure, you can use this script:
    git filter-branch -f
     --parent-filter /abs/path/to/linearizing_parent_filter.py
     -- --first-parent master
but you'll probably have faster results with awk:
    git filter-branch -f --parent-filter "awk '{ print \$1, \$2 }'" master

Want to cut history off at some point? Omit <new_par_sha1>:
    git filter-branch -f
     --parent-filter "/abs/path/to/linearizing_parent_filter.py <orig_par_sha1>"
     -- --first-parent master

"""

import sys


def main():
    """Do the thing."""
    #log  = open("/Volumes/case/last/big_repo/bob_#log", "a")
    orig_par_sha1 = None
    new_par_sha1  = None
    if 1 < len(sys.argv):
        orig_par_sha1 = sys.argv[1]
    if 2 < len(sys.argv):
        new_par_sha1 = sys.argv[2]

    for line_in in sys.stdin.readlines():
                            # Replace parent(s) if orig found.
        if orig_par_sha1 in line_in:
            if new_par_sha1 is not None:
                line_out = "-p {}".format(new_par_sha1)
                sys.stdout.write(line_out)
            continue

                            # Linearize history.
        words = line_in.split()
        par_sha1s = [w for w in words if w != "-p"]
        if not par_sha1s:
            continue
        line_out = "-p {}".format(par_sha1s[0])
        sys.stdout.write(line_out)
        #log.write("line {} => {}\n".format(line_in, line_out))

if __name__ == "__main__":
    main()

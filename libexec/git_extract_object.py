#! /usr/bin/env python3.3
"""Extract an object from the current Git repo.

In particular, extract the object as a zlib-compressed binary stream that
includes Git's object header, same as the file would be as a loose object
in a Git repo, or as submitted to Perforce in the gitmirror.

"""
import logging
import os
import pygit2

import p4gf_git
import p4gf_util

LOG = logging.getLogger("p4_hash_object_stdout")


def _argparse():
    """Pull args and repo paths out of argv.

    Return an args object.
    """
    desc = "Extract Git objects and add to Perforce."
    parser = p4gf_util.create_arg_parser(desc=desc
                , add_log_args  = True
                , add_debug_arg = True
                )
    parser.add_argument('-o','--outfile'
                        , help="where to write results, default=git_extract_object.out"
                        , default="git_extract_object.out"
                        , required=False)
    parser.add_argument('sha1', help="Full 40-char sha1")

    args = parser.parse_args()
    p4gf_util.apply_log_args(args, LOG)
    # pylint:disable=maybe-no-member
    LOG.debug2(args)
    return args


def main():
    """Do the thing."""
    repo = pygit2.Repository(".")
    args = _argparse()
    outfile_abspath = os.path.abspath(args.outfile)
    # pylint:disable=maybe-no-member
    LOG.debug2("outfile={}".format(outfile_abspath))
    p4gf_git.write_git_object_from_sha1(repo, args.sha1, outfile_abspath)


if __name__ == "__main__":
    main()

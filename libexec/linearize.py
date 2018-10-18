#! /usr/bin/env python3
"""Take some Git history and create a linear version of it.

Intended as a step in an automated cron job to update Git Fusion with a subset
of a huge Git repo.

Requires:
    - called with CWD = the giant repo.
    - local branch master points to history that you want to
      linearize then push.
    - remote "gf_origin" already set up with at least one successful push to
      its own "master" branch, with more than just 1 commit.
    - tag GF_LTP_LAST_LINEARIZED_BEFORE points to the last original
      Git commit that was later linearized via this script. This commit is the
      starting point for linearize_then_push.py's work load.
      linearize_then_push.py will advance this tag at the end of this script.
      GF_LTP_LAST_LINEARIZED_BEFORE must exist somewhere along master's
      --first-parent history: if not, call Perforce for help threading a linear
      path through some alternate history that's a lot harder to specify than
      the single word "--first-parent". And stop merging mainline into your dev
      branches. ^_^
    - tag GF_LTP_LAST_LINEARIZED_AFTER points to the Git commit that is
      a linearized counterpart to GF_LTP_LAST_LINEARIZED_BEFORE.
      This is the commit (and its preceding history) that we push
      through Git Fusion into Perforce.

Results:
    - Original history from GF_LTP_LAST_LINEARIZED_BEFORE..master converted
      to a linear version, no branches, no merges, just the --first-parent
      chain from master back to GF_LTP_LAST_LINEARIZED_BEFORE. This linearized
      history is connected previous value of GF_LTP_LAST_LINEARIZED_AFTER.
    - GF_LTP_LAST_LINEARIZED_BEFORE advanced to point to master.
    - GF_LTP_LAST_LINEARIZED_AFTER advanced to point to newly linearized
      history.
    - GF_LTP_LAST_LINEARIZED_AFTER pushed thorugh Git Fusion into Perforce
      remote gf_origin's branch master.
"""
import logging
import os
import pprint
import sys

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_log
import p4gf_proc
import p4gf_util

                        # Log level reminder from p4gf_util.apply_log_args():
                        # verbose   = INFO
                        # <nothing> = WARNING
                        # quiet     = ERROR
                        #
                        # keep it as p4gf_rollback_stdout so it goes to stdout
LOG = logging.getLogger("p4gf_rollback_stdout")

# tag/branch names
GF_LTP_LAST_LINEARIZED_BEFORE        = "GF_LTP_LAST_LINEARIZED_BEFORE"
GF_LTP_LAST_LINEARIZED_AFTER         = "GF_LTP_LAST_LINEARIZED_AFTER"

                        # (optional) markers for each time you invoke
                        # this script.
                        #
                        # If you pass "--tag-num N" then this script will leave
                        # behind  tags in the before and after histories
                        # showing our original input 'master' and resulting
                        # pushed 'master'.
                        #
GF_LTP_UPDATE_N_BEFORE = "GF_LTP_UPDATE_{:>03s}_BEFORE"
GF_LTP_UPDATE_N_AFTER  = "GF_LTP_UPDATE_{:>03s}_AFTER"

GF_WORKING          = "GF_WORKING"

MASTER              = "master"
GF_ORIGIN           = "gf_origin"

                        # For less typing
_abbr = p4gf_util.abbrev
_gref = p4gf_util.git_rev_list_1


def main():
    """Do the thing."""
    p4gf_proc.init()
    args = _parse_argv()

    sha1_last_linearized_before = _gref(GF_LTP_LAST_LINEARIZED_BEFORE)
    sha1_last_linearized_after  = _gref(GF_LTP_LAST_LINEARIZED_AFTER)
    _report_begin()
    _require_preconditions( sha1_last_linearized_before
                          , sha1_last_linearized_after )

                    # Move our filter-branch ref to the end of
                    # history that we want to rewrite.
    _git_tag(GF_WORKING, MASTER)

                    # Linearize history.
                    # This can take a while.
    _linearize(
          orig_begin_ref         = GF_LTP_LAST_LINEARIZED_BEFORE
        , orig_end_ref           = GF_WORKING
        , orig_parent_sha1       = sha1_last_linearized_before
        , linearized_parent_sha1 = sha1_last_linearized_after
        )
                    # Advance our "last linearized" marker to
                    # the new location.
    _git_tag(GF_LTP_LAST_LINEARIZED_AFTER, GF_WORKING)
    _git_tag(GF_LTP_LAST_LINEARIZED_BEFORE, MASTER)
    if args.tag_num:
        tag_update_before = GF_LTP_UPDATE_N_BEFORE.format(str(args.tag_num))
        tag_update_after  = GF_LTP_UPDATE_N_AFTER.format(str(args.tag_num))
        _git_tag(tag_update_before, MASTER)
        _git_tag(tag_update_after, GF_WORKING)
    _git_tag(GF_WORKING, None)

    _report_end()

def _require_preconditions( sha1_last_linearized_before
                          , sha1_last_linearized_after ):
    """Raise exception if missing a required ref, or if master's
    --first-parent chain doesn't reach back to its previous value.
    """
    if not (sha1_last_linearized_before and sha1_last_linearized_after):
        raise RuntimeError("Missing required refs."
                  " Stopping before changing anything.")
    sha1_master = _gref(MASTER)
    if sha1_master == sha1_last_linearized_before:
        raise RuntimeError("Nothing to do: {} == {} == {}"
            .format(MASTER, GF_LTP_LAST_LINEARIZED_BEFORE, _abbr(sha1_master)))
    _require_reachable( GF_LTP_LAST_LINEARIZED_BEFORE
                      , sha1_last_linearized_before
                      , MASTER )


def _linearize( *
              , orig_begin_ref, orig_end_ref
              , orig_parent_sha1, linearized_parent_sha1 ):
    """ Run 'git filter-branch --parent-filter' on a subset of
    original history, linearizing it and connecting to linearized_parent_sha1.
    """
    script_py = _par_fil_abspath()
    cmd = [ "git", "filter-branch", "-f"
          , "--parent-filter"
          , '{script_py} {orig_parent_sha1} {linearized_parent_sha1}'
              .format(
                  script_py              = script_py
                , orig_parent_sha1       = orig_parent_sha1
                , linearized_parent_sha1 = linearized_parent_sha1)
          , "--"
          , "--first-parent"
          , "{orig_begin_ref}..{orig_end_ref}"
                .format(
                    orig_begin_ref  = orig_begin_ref
            ,       orig_end_ref    = orig_end_ref )
          ]
    LOG.info("Linearizing history {bn}/{bs}..{en}/{es} and connecting to"
             " previously linearized history at {lin_par} ..."
             .format( bn = orig_begin_ref
                    , bs = _abbr(_gref(orig_begin_ref))
                    , en = orig_end_ref
                    , es =_abbr(_gref(orig_end_ref))
                    , lin_par = _abbr(linearized_parent_sha1)
                    ))
    _run(cmd)


def _run(cmd):
    """Log a shell command."""
    LOG.debug(' '.join(cmd))
    r = p4gf_proc.popen(cmd)
    # pylint:disable=maybe-no-member
    LOG.debug2(pprint.pformat(r))
    return r


def _par_fil_abspath():
    """Return the absolute path to our --parent-filter script."""
    bin_dir = os.path.dirname(sys.argv[0])
    script_abspath = os.path.join(bin_dir, "linearizing_parent_filter.py")
    return script_abspath


def _git_tag(tag_name, tag_points_to):
    """ git tag -f MYTAG some_sha1 """
    if tag_points_to:
        cmd = ["git", "tag", "-f", tag_name, tag_points_to ]
    else:
        cmd = ["git", "tag", "-d", tag_name ]
    _run(cmd)


def _require_reachable( require_name, require_sha1
                      , reachable_from_ref_name):
    """Require that the given sha1 exists somewhere in ref_name's
    --first-parent history. If not, raise a runtime error.
    """
    reachable_from_sha1 = _abbr(_gref(reachable_from_ref_name))
    cmd = ["git", "rev-list", "--first-parent", reachable_from_ref_name]
    d = _run(cmd)
    for line in d['out'].splitlines():
        if require_sha1 in line:
            LOG.debug("Found {rn}/{rs} in --first-parent history of {fn}/{fs}"
                      .format( rn = require_name
                             , rs = _abbr(require_sha1)
                             , fn = reachable_from_ref_name
                             , fs = reachable_from_sha1
                             ))
            return True
    raise RuntimeError(
"""Previous {rn}/{rs} not found in --first-parent history of {fn}/{fs}.
Someone needs to write a 'git filter-branch --parent-filter' script
that can carve a linear path between old {rn} and current {fn}.
"""
          .format( rn = require_name
                 , rs = _abbr(require_sha1)
                 , fn = reachable_from_ref_name
                 , fs = reachable_from_sha1
                 ))


def _report_begin():
    """Dump the world before we change anything."""
    LOG.info("Starting ref locations:")
    refs = [ MASTER
           , GF_LTP_LAST_LINEARIZED_BEFORE
           , GF_LTP_LAST_LINEARIZED_AFTER
           ]
    _report_refs(refs)


def _report_end():
    """Dump the world before we change anything."""
    LOG.info("Ending ref locations:")
    refs = [ GF_LTP_LAST_LINEARIZED_BEFORE
           , GF_LTP_LAST_LINEARIZED_AFTER ]
    _report_refs(refs)


def _report_refs(ref_list):
    """Dump a bunch of "ref : sha1" lines to LOG.info."""
    for ref in ref_list:
        sha1 = _gref(ref)
        LOG.info("{:<40} {}".format(ref, _abbr(sha1)))


def _parse_argv():
    """Convert command line into a usable dict."""
    parser = p4gf_util.create_arg_parser(
          add_log_args  = True
        , add_debug_arg = True
        )
    parser.add_argument('--tag-num', default=0)
    args = parser.parse_args()
    p4gf_util.apply_log_args(args, LOG)
    LOG.debug("args={}".format(args))
    args.tag_num = int(args.tag_num)
    return args


if __name__ == "__main__":
    p4gf_log.run_with_exception_logger(main, write_to_stderr=True)

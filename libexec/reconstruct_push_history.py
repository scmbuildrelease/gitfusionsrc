#! /usr/bin/env python3.3
"""reconstruct_push_history.py repo_name.

Pore over changelist descriptions and //.git-fusion/ data
to reconstruct push history.

WARNING: runs 'p4 changes' unbounded over the entire Perforce server.
If you have a lot of changes, this can take a while.
If you need to restrict this to specific depot paths, change

"""

import logging
import sys

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_const
import p4gf_context
from   p4gf_desc_info   import DescInfo
from   p4gf_object_type import ObjectType
import p4gf_util

                        # debug   : internal tracking
                        # info    : verbose, per-changelist status
                        # warning : something differs
                        # error   : cannot diff
LOG = logging.getLogger("reconstruct_push_history")


def main():
    """Do the thing."""
    args = _argparse()
    _log_config(args)

    LOG.debug("args={}".format(args))

    ctx = _create_ctx( p4port    = args.p4port
                     , p4user    = args.p4user
                     , repo_name = args.repo_name
                     , server_id = args.server_id )

    LOG.info("P4PORT={}".format(ctx.p4.port))
    LOG.info("P4USER={}".format(ctx.p4.user))

    di_list = changes_to_desc_info_list(ctx, entire_depot=args.entire_depot)

                        # Load branch view definitions.
                        # Gives us Git branch names to go with pushed
                        # commmit sha1s.
    branch_dict = ctx.branch_dict()
                        # Create index for fast dbid->branch lookups later.
    dbid_to_branch = {b.depot_branch.depot_branch_id: b
                      for b in branch_dict.values()
                      if b.depot_branch}

                        # Use DescInfo and ObjectType backup to
                        # find branch_id assignments.
    for di in di_list:
        di.branch_id       = None
        di.git_branch_name = None
        change_num_str     = str(di.change_num)

                        # Use depot branch ID recorded in DescInfo.
        if di.depot_branch_id:
            branch = dbid_to_branch.get(di.depot_branch_id)
            if branch:
                di.branch_id = branch.branch_id
                di.git_branch_name = branch.git_branch_name
                continue

                        # Fetch from ObjectType.
        otl = ObjectType.commits_for_sha1(ctx, di.sha1)
        for ot in otl:
            if ot.change_num == change_num_str:
                di.branch_id = ot.branch_id
                if di.branch_id:
                    branch = branch_dict.get(di.branch_id)
                    if branch:
                        di.git_branch_name = branch.git_branch_name
                    continue


                        # Report time
    attr_list = ['change_num', 'sha1', 'push_state', 'branch_id', 'git_branch_name']

    print("# " + "\t".join(attr_list))
    for di in di_list:
        if args.complete_only and di.push_state != "complete":
            continue
        v = [str(getattr(di, a)) for a in attr_list]
        print("\t".join(v))


def changes_to_desc_info_list(ctx, entire_depot):
    """Return a list of DescInfo objects, one for each 'p4 changes' result that
    contains a DescInfo block. Ordered by change_num, ascending.

    Returned DescInfo instances have an integer change_num attribute
    injected, not part of DescInfo.__init__().
    """
    if entire_depot:
        cmd = ['changes', '-l', '//...']
        LOG.info("p4 " + " ".join(cmd))
        p4changes_r = ctx.p4run(cmd)
    else:
        d = {}
        for b in ctx.branch_dict().values():
            with ctx.switched_to_branch(b):
                cmd = ['changes', '-l', ctx.client_view_path()]
                LOG.info("p4 " + " ".join(cmd))
                for r in ctx.p4run(cmd):
                    change_num = r.get("change")
                    if change_num and change_num not in d:
                        d[change_num] = r
        p4changes_r = d.values()

    LOG.info("changelist count: {}".format(len(p4changes_r)))
    di_list = []
    for r in p4changes_r:
        try:
            di = DescInfo.from_text(r['desc'])
            if not di:
                continue
            di.change_num = int(r['change'])
            di_list.append(di)
        except TypeError:   # r is not a dict? Skip it.
            continue
    LOG.info("DescInfo count  : {}".format(len(di_list)))

    di_list_sorted = sorted(di_list, key=lambda x: x.change_num)
    return di_list_sorted


# -- Top-level administrivia -------------------------------------------------

def _argparse():
    """Pull args and repo paths out of argv.

    Return an args object.
    """
    desc = """Reconstruct the order in which Git commits were pushed.
           Reads 'Imported from Git' DescInfo blocks from Perforce
           changelists, matches them up with a Git Fusion repo's
           p4gf_config/p4gf_config2 branch definitions. Lines with push-state
           'complete' are the branch heads that were pushed. 'incomplete' are
           the commits leading up to the pushed heads.
           """
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument( '-v', '--verbose'
                       , default=False
                       , action="store_true"
                       , help="Report INFO-level progress.")
    parser.add_argument( '--debug'
                       , default=False
                       , action="store_true"
                       , help="Report DEBUG-level progress.")
    parser.add_argument( '-p', '--p4port'
                       , metavar='p4port'
                       , help='Perforce server' )
    parser.add_argument( '-u', '--p4user'
                       , metavar='p4user'
                       , help='Admin-privileged Perforce user')
    parser.add_argument( '--complete-only'
                       , default=False
                       , action="store_true"
                       , help='List only push-state:complete changelists?' )
    parser.add_argument( '--server-id'
                       , metavar='gfserver-id'
                       , default=None
                       , help="""Git Fusion server id. If running from a
                              computer that doesn't have a
                              ~/.git-fusion/server-id, pass any existing
                              Git Fusion server-id here to bypass some
                              annoying internal Git Fusion warnings.
                              """ )
    parser.add_argument( '--entire-depot'
                       , default=False
                       , action="store_true"
                       , help="""Report every changelist in the Perforce
                              server
                              that has an 'Imported from Git' block,
                              regardless of which repo originally pushed it.
                              Runs 'p4 changes //...' across entire Perforce
                              server, so this can take a lot of time and
                              memory.
                              """ )
    parser.add_argument( 'repo_name'
                       , metavar='<repo-name>'
                       , help="Which Git Fusion repo's p4gf_config/p4gf_config2"
                              " to use for branch definitions.")

    args = parser.parse_args()
    return args


def _log_config(args):
    """Context will call p4gf_log which reconfigures our log for Git Fusion server mode.
    We want "human user at the console" stdout mode.
    """
    handler = logging.StreamHandler()
    LOG.addHandler(handler)

    logging.basicConfig(format="%(message)s", stream=sys.stdout)
    if args.debug:
        LOG.setLevel(logging.DEBUG)
    elif args.verbose:
        LOG.setLevel(logging.INFO)
    else:
        LOG.setLevel(logging.WARNING)


def _create_ctx(p4port, p4user, server_id, repo_name):
    """Connect to Perforce using environment with optional p4port/p4user overrides.

    Set to None of no override.
    """
    ctx = p4gf_context.create_context(repo_name)
    if p4port:
        ctx.config.p4port = p4port
    if p4user:
        ctx.config.p4user = p4user
    if server_id:
        ctx.p4gf_client = p4gf_const.P4GF_OBJECT_CLIENT.format(server_id=server_id)
    ctx.connect()
    return ctx


if __name__ == "__main__":
    main()

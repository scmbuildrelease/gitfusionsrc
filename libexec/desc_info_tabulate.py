#! /usr/bin/env python3.3
"""
desc_info_tabulate.py [<p4 changes args>]

Runs 'p4 changes' on the given args (or the whole Perforce server if no args),
then extracts any DescInfo blocks and dumps them out as a table.

Script must live inside Git Fusion's 14.1/857005 (or later) bin directory
alongside other p4gf_xxx.py scripts so that it can import p4gf_desc_info and
other modules. This bin directory does not have to be the active/installed
version of Git Fusion.
"""

import logging
import sys

import p4gf_env_config  # pylint:disable=unused-import
import p4gf_create_p4
from   p4gf_desc_info   import DescInfo
import p4gf_util

                        # debug   : internal tracking
                        # info    : verbose, per-changelist status
                        # warning : something differs
                        # error   : cannot diff
LOG = logging.getLogger("desc_info_tabulate")


def main():
    """Do the thing."""
    args = _argparse()
    _log_config(args)

    LOG.debug("args={}".format(args))

    p4 = p4gf_create_p4.create_p4_temp_client(port=args.p4port, user=args.p4user)
    LOG.info("P4PORT={}".format(p4.port))
    LOG.info("P4USER={}".format(p4.user))
    cmd = ['changes', '-l'] + args.p4changes_arg_list
    LOG.info("p4 " + " ".join(cmd))
    p4changes_r = p4.run(cmd)
    LOG.info("result count: {}".format(len(p4changes_r)))
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
    LOG.info("DescInfo count: {}".format(len(di_list)))

    attr_list = [ 'change_num'
                , 'sha1'
                , 'push_state'
                ]
    print ("# " + "\t".join(attr_list))

    di_list_sorted = sorted(di_list, key=lambda x: x.change_num)
    for di in di_list_sorted:
        v = [str(getattr(di, a)) for a in attr_list]
        print("\t".join(v))


# -- Top-level administrivia -------------------------------------------------

def _argparse():
    """Pull args and repo paths out of argv.

    Return an args object.
    """
    desc = "Runs 'p4 changes' and output DescInfo blocks as a table."
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument('-v', '--verbose', default=False
                        , action="store_true"
                        , help="Report INFO-level progress.")
    parser.add_argument('--debug', default=False
                        , action="store_true"
                        , help="Report DEBUG-level progress.")
    parser.add_argument('-p', '--p4port', metavar='p4port', help='Perforce server')
    parser.add_argument('-u', '--p4user', metavar='p4user', help='Admin-privileged Perforce user')

    parser.add_argument('p4changes_arg_list', metavar='<p4 changes arguments>', nargs='*')

    args = parser.parse_args()
    return args


def _log_config(args):
    """
    Context will call p4gf_log which reconfigures our log for Git Fusion server mode.

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


if __name__ == "__main__":
    main()

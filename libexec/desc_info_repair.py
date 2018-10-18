#! /usr/bin/env python3.3
"""
desc_info_repair.py <repo name>

Script to update existing Perforce changelist descriptions with additional
information that helps Git Fusion reproduce Git history.

Uses data from //.git-fusion/objects/... to fix these fatal
repo_compare.py mismatches:

* parent.id_mismatch
* parent.missing

Script must live inside Git Fusion's 14.1/857005 (or later) bin directory
alongside other p4gf_xxx.py scripts so that it can import p4gf_desc_info and
other modules. This bin directory does not have to be the active/installed
version of Git Fusion.
"""

from   collections import namedtuple, defaultdict
import copy
import json
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
LOG = logging.getLogger("desc_info_repair")

SHA1_TO_OTL = defaultdict(list)
SHA1_TO_PAR_SHA1_LIST = {}

Repair = namedtuple("Repair", ["change_num", "old_desc", "new_desc"])


def main():
    """Do the thing."""
    args = _argparse()
    _log_config(args)

    LOG.debug("args={}".format(args))

    if 1 < sum([1 for x in [args.apply, args.revert, args.save] if x]):
        LOG.error('Can perform only one of: --apply, --revert or --save.')
        sys.exit(1)

    ctx = _create_ctx( p4port    = args.p4port
                     , p4user    = args.p4user
                     , repo_name = args.repo_name)

    if args.apply:
        repair_list = _read_repairs(args.apply)
    elif args.revert:
        repair_list = _read_repairs(args.revert)
    else:
        repair_list = _calc_repairs(ctx)

    if args.save:
        _save_repairs(args.save, repair_list)

    if not args.preview:
        if args.revert:
            _revert_repairs(ctx, repair_list)
        else:
            _apply_repairs(ctx, repair_list)
    else:
        _preview_repairs(repair_list)


def _save_repairs(save_file, repair_list):
    """Write a list of repairs to file."""
    LOG.info("Saving {ct} repaired changelists to {file}."
            .format( ct   = len(repair_list)
                   , file = save_file ))
    dict_list = [{ 'change_num' : r.change_num
                 , 'old_desc'   : r.old_desc
                 , 'new_desc'   : r.new_desc }
                 for r in repair_list]
    with open (save_file, mode='w') as f:
        json.dump(obj = dict_list, fp = f, indent = 2, sort_keys = True)


def _read_repairs(apply_file):
    """Load a list of repairs from file."""
    with open(apply_file, mode='r') as f:
        dict_list = json.load(f)
    result = [Repair( change_num = int(d['change_num'])
                    , old_desc   =     d['old_desc']
                    , new_desc   =     d['new_desc'] )
              for d in dict_list]
    LOG.info("Read {ct} repaired changelists from {file}."
            .format( ct   = len(result)
                   , file = apply_file ))
    return result


# -- Calculating Necessary Repairs -------------------------------------------

def _calc_repairs(ctx):
    """
    Scan Perforce for Git commit data and Perforce changelist descriptions,
    calculate which Perforce changelists need more data copied from Git
    backing store //.git-fusion/objects/...
    """

                        # Load repo's entire set of Commit/Changelist metadata
                        # into memory.
    LOG.info("Fetching list of Git commits/changelists from %s/objects/...",
             p4gf_const.objects_root())
    r = ctx.p4run( 'files'
                 , '{root}/repos/{repo}/commits/...'
                    .format(root=p4gf_const.objects_root(),
                            repo = ctx.config.repo_name))
                        # 'p4 print' each Git commit from its backup in
                        # //.git-fusion/objects/...
    LOG.info("Fetched commit objects: {ct}".format(ct=len(r)))
    for rr in r:
        depot_path = rr.get('depotFile')
        if not depot_path:
            continue
        ot = ObjectType.commit_from_filepath(depot_path)
        SHA1_TO_OTL[ot.sha1].append(ot)
        LOG.debug('p4 print {}'.format(depot_path))
        blob_raw = p4gf_util.print_depot_path_raw(ctx.p4, depot_path)
        blob     = p4gf_util.bytes_to_git_object(blob_raw)
        par_list = commit_to_parent_list(blob)
        SHA1_TO_PAR_SHA1_LIST[ot.sha1] = par_list
        LOG.debug("{sha1:7.7} parents={par}"
                  .format( sha1 = ot.sha1
                         , par  = [p4gf_util.abbrev(p) for p in par_list]))

                        # Loop through changelists, comparing against
                        # backup and calculating if additional data
                        # needs to be copied to its changelist description.
    return _calc_repairs_loop(ctx)


def _calc_repairs_loop(ctx):
    """
    Return a list of repair tuples, one for each changelist that needs more
    data copied from //.git-fusion/objects/...
    """
    cn_to_repair = {}
    for otl in SHA1_TO_OTL.values():
        for ot in otl:
            r = _calc_repair(ctx, ot)
            if r:
                cn_to_repair[int(r.change_num)] = r

    return [cn_to_repair[k] for k in sorted(cn_to_repair.keys())]


def _calc_repair(ctx, ot):
    """
    Return repair tuple for a single commit/changelist,
    or None if no repair needed.
    """
    LOG.debug('_calc_repair() {}'.format(ot))
    fmt = '{sha1:7.7} @{change_num:<5} {msg}'

    describe = p4gf_util.first_dict_with_key(
                              ctx.p4run('describe', ot.change_num)
                            , 'desc')
    if not describe:
        LOG.warning("Unable to find changelist {change_num}"
                    " for Git commit {sha1:7.7}. Skipping."
                    .format( sha1       = ot.sha1
                           , change_num = ot.change_num ))
        return
    old_desc = describe['desc']
    di = DescInfo.from_text(old_desc)
    if not di:
                        # Changelists that originate in Perforce neither need
                        # nor get Git Fusion DescInfo blocks.
        LOG.info(fmt.format( sha1       = ot.sha1
                           , change_num = ot.change_num
                           , msg        = "Skipping: no desc info" ))
        return
    copy.copy(di)

    _repair_parents(ot, di)

    new_desc = di.to_text()
    if new_desc.strip() == old_desc.strip():
        LOG.info(fmt.format(
              sha1       = ot.sha1
            , change_num = ot.change_num
            , msg        = "Skipping: already correct, no repair required" ))
        return None
    # else:
    #    LOG.debug("OLD @{}:\n{}".format(ot.change_num, old_desc))
    #    LOG.debug("NEW @{}:\n{}".format(ot.change_num, new_desc))

    LOG.info(fmt.format( sha1       = ot.sha1
                       , change_num = ot.change_num
                       , msg        = "Repair needed" ))
    return Repair( change_num = int(ot.change_num)
                 , old_desc   = old_desc
                 , new_desc   = new_desc )


def _repair_parents(ot, di):
    """Set a parent-changes: line in DescInfo."""
    par_sha1_list = SHA1_TO_PAR_SHA1_LIST[ot.sha1]

    par_to_change_num_list = {}
    for par_sha1 in par_sha1_list:
        par_otl = SHA1_TO_OTL.get(par_sha1)
        if not par_otl:
            LOG.warning('Unable to find parent {sha1:7.7} for changelist {change_num}. Skipping.'
                        .format(sha1 = par_sha1, change_num = ot.change_num ))
            return
        par_to_change_num_list[par_sha1] = [par_ot.change_num for par_ot in par_otl]

    di.parents = par_sha1_list
    di.parent_changes = par_to_change_num_list

# -- Acutally changing Perforce changelist descriptions ----------------------


def _apply_repairs(ctx, repair_list):
    """Update changelist descriptions to include additional data."""
    for repair in repair_list:
        _update_changelist_description(
                  ctx         = ctx
                , change_num  = repair.change_num
                , description = repair.new_desc )


def _revert_repairs(ctx, repair_list):
    """Update changelist descriptions to include original data."""
    for repair in repair_list:
        _update_changelist_description(
                  ctx         = ctx
                , change_num  = repair.change_num
                , description = repair.old_desc )


def _update_changelist_description(ctx, change_num, description):
    """Set on echangelist's description."""
    fmt = '@{change_num:<5} {msg}'
    change = p4gf_util.first_dict_with_key(
                   ctx.p4run('change', '-o', str(change_num))
                , 'Description')
    old_desc = change['Description']
    if not old_desc:
        LOG.warning(fmt.format( change_num = change_num
                              , msg = "Skipping: cannot find changelist."))
        return

    LOG.debug("_update_changelist_description() @{cn}"
              .format(cn=change_num))

    if old_desc.strip() == description.strip():
        LOG.info(fmt.format( change_num = change_num
                           , msg        = "Skipping. No change." ))
        return

    LOG.info(fmt.format( change_num = change_num
                       , msg        = "Updating" ))
    change['Description'] = description
    ctx.p4.save_change(change, '-f')


def _preview_repairs(repair_list):
    """Tell user what would be changed."""
    LOG.warning("Would update {ct} Perforce changelists:"
                .format(ct=len(repair_list)))
    LOG.warning('\n'.join([str(repair.change_num)
                           for repair in repair_list]))


# -- Top-level administrivia -------------------------------------------------

def _argparse():
    """Pull args and repo paths out of argv.

    Return an args object.
    """
    desc = "Compare two Git repos. Identify what differs where."
    parser = p4gf_util.create_arg_parser(desc=desc)
    parser.add_argument('-v', '--verbose', default=False
                        , action="store_true"
                        , help="Report INFO-level progress.")
    parser.add_argument('--debug', default=False
                        , action="store_true"
                        , help="Report DEBUG-level progress.")
    parser.add_argument('-p', '--p4port', metavar='p4port', help='Perforce server')
    parser.add_argument('-u', '--p4user', metavar='p4user', help='Admin-privileged Perforce user')

    parser.add_argument('-n', '--preview', default=False
                        , action="store_true"
                        , help="Display a preview of what would be changed,"
                               " without actually doing anything.")

    parser.add_argument('--save'
                        , metavar='<save-file>'
                        , help="Save a JSON file recording what changed."
                               " Can combine with --preview to save without"
                               " doing anything."
                               " Pass this file to --apply or --revert." )

    parser.add_argument('--apply'
                        , metavar='<save-file>'
                        , help="Repair changelist descriptions listed in the"
                               " given JSON file."
                               " Use --save [--preview] to create this file." )

    parser.add_argument('--revert'
                        , metavar='<save-file>'
                        , help="Undo repairs to changelist descriptions listed"
                               " in the given JSON file."
                               " Use --save [--preview] to create this file." )

    parser.add_argument('repo_name', metavar='<repo-name>')

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


def _create_ctx(p4port, p4user, repo_name):
    """
    Connect to Perforce using environment with optional
    p4port/p4user overrides. Set to None of no override.
    """
    ctx = p4gf_context.create_context(repo_name)
    if p4port:
        ctx.config.p4port = p4port
    if p4user:
        ctx.config.p4user = p4user
    ctx.connect()
    return ctx


# -- move to p4gf_git --------------------------------------------------------

def commit_to_parent_list(blob):
    """
    For the given commit object data (an instance of bytes), extract the
    corresponding parent SHA1s (as a str).
    """
    if not isinstance(blob, bytes):
        LOG.error("commit_to_parent_list() expected bytes, got {}".format(type(blob)))
        return None
    if len(blob) == 0:
        LOG.error("commit_to_parent_list() expected non-zero bytes")
        return None

    try:
        blob.index(b'\n\n')
    except ValueError:
        LOG.error("commit_to_parent_list() expected \n\n, not found.")
        return None

    PARENT = b'\nparent '   # pylint:disable=invalid-name
    par_list = []
    idx = 0
    try:
        while idx < len(blob):
            idx = blob.index(PARENT, idx)
            sha1 = blob[idx + len(PARENT): idx + len(PARENT) + 40]
            par_list.append(sha1)
            idx += len(PARENT) + 40
    except ValueError:  # fell off the end
        pass
    return [b.decode() for b in par_list]


if __name__ == "__main__":
    main()

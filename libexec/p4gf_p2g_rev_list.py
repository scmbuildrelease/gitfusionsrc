#! /usr/bin/env python3.3
"""RevList."""
import copy
import logging

LOG = logging.getLogger('p4gf_copy_to_git').getChild('rev_list')


class RevList:

    """A dict of lists of P4File revisions, keyed by int(change_num).

    Fill with append(). (Called by p2g's PrintHandler.
    """

    def __init__(self):
                        # dict of str(change_num) => list[ P4File ]
        self.changes = {}

    def append(self, p4file):
        """Add a p4file to list of revs for corresponding change.

        Intentionally allows duplicates: you can 'p4 print' the same changelist
        multiple times if it overlaps multiple branch views. This is rare
        enough that it's not worth spending CPU time or memory on filtering
        out.
        """
        self.changes.setdefault(p4file.change, []).append(p4file)

    def __iter__(self):
        for change in self.changes.values():
            for p4file in change:
                yield p4file

    def files_for_change(self, change):
        """Return list of P4File objects for revisions matching this change number."""
        if change not in self.changes:
            return []
        return self.changes[change]

    def files_for_graft_change(self, graft_change, branch):
        """Return list of P4File objects for revisions matching this change number."""
        result = {}
        for changenum, change in self.changes.items():
            if changenum > graft_change:
                continue
            for p4file in change:
                if p4file.depot_path in result and p4file.change < result[p4file.depot_path].change:
                    continue
                if not branch.intersects_depot_path(p4file.depot_path):
                    continue
                result[p4file.depot_path] = p4file
        result = [copy.copy(p4file) for p4file in result.values()]
        for p4file in result:
            p4file.change = graft_change
        LOG.debug("RevList files_for_graft_change: {}".format(result))
        return result

    def __len__(self):
        return sum(len(change) for change in self.changes.values())

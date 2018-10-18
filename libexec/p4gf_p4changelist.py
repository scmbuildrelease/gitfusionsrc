#! /usr/bin/env python3.3
"""P4Changelist class."""

import logging
import re

from P4          import OutputHandler

from p4gf_l10n   import NTR
from p4gf_p4file import P4File

import p4gf_util

LOG = logging.getLogger(__name__)


class ChangesHandler(OutputHandler):

    """OutputHandler for p4 changes, passes changelists to callback function.

    revs   : (input)  RevList
    """

    def __init__(self, callback):
        OutputHandler.__init__(self)
        self.callback = callback
        self.count = 0

    def outputStat(self, h):
        """Grab clientFile from fstat output."""
        try:
            change = P4Changelist.create_using_changes(h)
            self.callback(change)
            self.count = self.count + 1
        except Exception:  # pylint: disable=broad-except
            LOG.exception("outputStat")
        return OutputHandler.HANDLED


class P4Changelist:

    """A changelist, as reported by p4 describe.

    Run p4 describe of a changelist and filter the files reported
    against a specified root path, e.g. //depot/main/p4/
    """

    def __init__(self):
        self.change = None
        self.description = None
        self.user = None
        self.time = None
        self.path = None
        self.files = []   # P4Files in this changelist

    @staticmethod
    def create_using_describe(p4, change, depot_root):
        """Create a P4Changelist by running p4 describe."""
        result = p4.run("describe", "-s", str(change))
        cl = P4Changelist()
        vardict = p4gf_util.first_dict_with_key(result, 'change')
        cl.change = int(vardict["change"])
        cl.description = vardict["desc"]
        cl.user = vardict["user"]
        cl.time = vardict["time"]
        if 'path' in vardict:
            cl.path = vardict['path']
        for i in range(len(vardict["depotFile"])):
            p4file = P4File.create_from_describe(vardict, i)
            # filter out files not under our root right now
            if not p4file.depot_path.startswith(depot_root):
                continue
            cl.files.append(p4file)
        return cl

    @staticmethod
    def create_using_changes(vardict):
        """Create a P4Changelist from the output of p4 changes."""
        cl = P4Changelist()
        cl.change = int(vardict["change"])
        cl.description = vardict["desc"]
        cl.user = vardict["user"]
        cl.time = vardict["time"]
        if 'path' in vardict:
            cl.path = vardict['path']
        return cl

    @staticmethod
    def create_using_change(p4, change):
        """p4 change -o nnn."""
        result = p4.run('change', '-o', str(change))
        cl = P4Changelist()
        vardict = p4gf_util.first_dict_with_key(result, 'Change')
        cl.change = int(vardict["Change"])
        cl.description = vardict["Description"]
        cl.user = vardict["User"]
        cl.time = vardict["Date"]   # string "2012/01/31 23:59:59"
        return cl

    @staticmethod
    def get_changelists(p4, path, callback):
        """Run p4 changes to get a list of changes.

        Call callback with each found changelist.

        p4: initialized P4 object
        path: path + revision specifier, e.g. //depot/main/p4/...@1,#head
        callback: function taking P4Changelist
        """
        cmd = NTR(["changes", "-l"])
        # replace: p4 changes //depot/...@N,#head
        # with:    p4 changes -e N //depot/...
        m = re.match(r'(.+)@(\d+),#head', path)
        if m:
            cmd.append("-e")
            cmd.append(m.groups()[1])
            cmd.append(m.groups()[0])
        else:
            cmd.append(path)
        LOG.debug(cmd)
        handler = ChangesHandler(callback)
        with p4.using_handler(handler):
            p4.run(cmd)
        return handler.count

    @staticmethod
    def create_changelist_list_as_dict(p4, path, _limit=None):
        """Run p4 changes to get a list of changes.

        Return the result as a dict indexed by changelist number (as string).

        Returns a dict["change"] ==> P4Changelist

        p4: initialized P4 object
        path: path + revision specifier, e.g. //depot/main/p4/...@1,#head
        """
        changes = {}

        def append(change):
            """Append a change to the list."""
            changes[change.change] = change

        P4Changelist.get_changelists(p4, path, append)
        if LOG.isEnabledFor(logging.DEBUG):
            cll = []
            for c in changes.keys():
                if len(cll) == 10:
                    cll.append(NTR('...{} total').format(len(changes)))
                    break
                cll.append(str(c))
            LOG.debug("create_changelist_list_as_dict returning\n{}"
                      .format(' '.join(cll)))
        return changes

    def __str__(self):
        return "change {0} with {1} files".format(self.change, len(self.files))

    def __repr__(self):
        files = [repr(p4file) for p4file in self.files]
        result = "\n".join(["change: " + str(self.change),
                            "description: " + self.description,
                            "user: " + self.user,
                            "time: " + self.time,
                            "files:",
                            ] + files)
        return result

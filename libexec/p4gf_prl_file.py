#! /usr/bin/env python3.3
"""PRLFile: A file that contains the pre-receive tuple list from a push."""
import logging
import os
from pprint import pformat

import p4gf_const
from p4gf_l10n import NTR
from p4gf_receive_hook import PreReceiveTupleLists
import p4gf_util

LOG = logging.getLogger(__name__)


class PRLFile(object):
    """A file that contains a PreReceiveTupleLists collection.

    Used to rollback a failed push.
    """
    def __init__(self, repo_name):
        self.repo_name = repo_name

    def filename(self):
        """Path this repo's prl file.
        Copypasta from p4gf_receive_hook._packet_filename()
        """
        fn = NTR('push-prl-{repo}.json').format(repo=self.repo_name)
        return os.path.join(p4gf_const.P4GF_HOME, fn)

    def delete(self):
        """Delete our file, if it exists. NOP if not."""
        fn = self.filename()
        if os.path.exists(fn):
            LOG.debug('deleted {}'.format(fn))
            os.unlink(fn)

    def write(self, prl):
        """Write the given pre-receive list, and copies of all the pushed
        ref's original sha1s, to our file.
        """
        d = {"prl" : prl.to_dict()}
        p4gf_util.write_dict_to_file(d, self.filename())
        LOG.debug("wrote {fn}".format(fn=self.filename()))
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3(pformat(d))

    def read(self):
        """Read the serialized Git pre-receive-tuple list from a JSON file."""
        fn = self.filename()
        if not os.path.exists(fn):
            return None
        d   = p4gf_util.read_dict_from_file(fn)
        prl = PreReceiveTupleLists.from_dict(d["prl"])
        LOG.debug("read {fn}".format(fn=fn))
        if LOG.isEnabledFor(logging.DEBUG3):
            LOG.debug3(pformat(d))
        return prl

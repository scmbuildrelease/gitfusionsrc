#! /usr/bin/env python3.3
"""A set of Git LFS text pointers mentioned in a 'git push',
indexed by <commit, gwt>.
"""
from collections import namedtuple

from p4gf_lfs_file_spec import LFSFileSpec

Key = namedtuple("Key", [ "commit_sha1", "gwt_path" ] )

class LFSFileSpecDict(dict):
    """A set of Git LFS text pointers mentioned in a 'git push',
    indexed by <commit, gwt>.

    many-to-1 relation: same LFSFileSpec can appear
    multiple times throughout history or filesystem.
    """
    def addfs(self, commit_sha1, gwt_path, lfsfs):
        """Associate this file at this commit with its LFS text pointer."""
        k = self.key(commit_sha1, gwt_path)
        self[k] = lfsfs

    def getfs(self, commit_sha1, gwt_path):
        """Return previously add()ed association."""
        k = self.key(commit_sha1, gwt_path)
        return self.get(k)

    @staticmethod
    def key(commit_sha1, gwt_path):
        """Return a single object suitable for use as a dict key."""
        return Key(commit_sha1, gwt_path)

    def to_list(self):
        """For JSON formatting."""
        return [ [k.commit_sha1, k.gwt_path, v.to_dict()]
                 for k, v in self.items() ]

    @staticmethod
    def from_list(lisst):
        """For JSON parsing."""
        d = { Key(l[0], l[1]) : LFSFileSpec.from_dict(l[2])
              for l in lisst }
        lfsfsd = LFSFileSpecDict()
        lfsfsd.update(d)
        return lfsfsd

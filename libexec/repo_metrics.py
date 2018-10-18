#! /usr/bin/env python3.3
"""Count the number of blobs and trees in the current repo.

Slower than, and gets a slightly different blob count from,
the simpler shell version of this. But the shell version doesn't
see trees or count commits. This Python version does.
"""

from   collections import deque
import pygit2

import p4gf_const
import p4gf_eta
import p4gf_histogram
import p4gf_pygit2


class GitBlobCt:
    """Counter of commits, blobs, and trees."""
    def __init__(self):
        self.repo = pygit2.Repository(".")
        self.trees = set()
        self.blobs = set()
        self.commits = set()
        self.gwt_paths = set()
        self.commit_byte_ct = []
        self.blob_byte_ct   = []
        self.tree_byte_ct   = []
        self.gwt_ct          = []
        self.file_action_ct  = []
        self.eta             = p4gf_eta.ETA()



    def main(self):
        """Do the thing."""
        self.git_rev_list()
        self.eta.start(total_ct = len(self.commits))
        for i, commit_oid in enumerate(self.commits):
            self.walk_file_hierarchy(i, commit_oid)

        tabulines = tabular(
                [ "commits"
                , "blobs"
                , "trees"
                , "gwt_paths"
                , "commit bytes"
                , "blob bytes"
                , "tree bytes"
                ]
              , [ len(self.commits)
                , len(self.blobs)
                , len(self.trees)
                , len(self.gwt_paths)
                , sum(self.commit_byte_ct)
                , sum(self.blob_byte_ct)
                , sum(self.tree_byte_ct)
                ])
        print("\n".join(tabulines))

        self.histo( coll  = self.commit_byte_ct
                  , log   = True
                  , title = "Commit size distribution (logarithmic buckets):" )
        self.histo( coll  = self.commit_byte_ct
                  , log   = False
                  , title = "Commit size distribution (linear buckets):" )
        self.histo( coll  = self.blob_byte_ct
                  , log   = True
                  , title = "Blob size distribution (logarithmic buckets):" )
        self.histo( coll  = self.blob_byte_ct
                  , log   = False
                  , title = "Blob size distribution (linear buckets):" )
        self.histo( coll  = self.tree_byte_ct
                  , log   = True
                  , title = "Tree size distribution (logarithmic buckets):" )
        self.histo( coll  = self.tree_byte_ct
                  , log   = False
                  , title = "Tree size distribution: (linear buckets)" )
        self.histo( coll  = self.gwt_ct
                  , log   = True
                  , title = "GWT ct distribution (sampled): (logarithmic buckets)" )
        self.histo( coll  = self.gwt_ct
                  , log   = False
                  , title = "GWT ct distribution (sampled): (linear buckets)" )
        self.histo( coll  = self.file_action_ct
                  , log   = True
                  , title = "File action ct distribution (sampled): (logarithmic buckets)" )
        self.histo( coll  = self.file_action_ct
                  , log   = False
                  , title = "File action ct distribution (sampled): (linear buckets)" )

    @staticmethod
    def histo(coll, log, title):
        """Dump a size distribution graph."""
        histo = p4gf_histogram.to_histogram(
                      coll          = coll
                    , bucket_ct     = 25
                    , bucket_prefix = []
                    , log           = log
                    )
        print("\n")
        print(title)
        print("\n".join(p4gf_histogram.to_lines(histo)))

    def git_rev_list(self):
        """Fill self.commits with all commits in the repo."""
        for ref_name in self.all_ref_names():
            if ref_name.startswith("refs/remotes/"):
                continue
            self.commits.update(self.commit_oid_history(ref_name))
            print("{:>8,} commits   {}".format(len(self.commits), ref_name))

    def commit_oid_history(self, ref_name):
        """Return all commits in ref_name's history."""
        ref_commit = self.ref_to_commit(ref_name)
        if not ref_commit or ref_commit.oid in self.commits:
            return []

        result = []
        for commit in self.repo.walk( ref_commit.oid
                                    , pygit2.GIT_SORT_TOPOLOGICAL):
            if commit.oid in self.commits:
                break
            result.append(commit.oid)
        return result

    def walk_file_hierarchy(self, commit_i, commit_oid):
        """Visit every tree, every file, in this commit's file hierarchy.
        Skip any we've already visited from some previous walk.
        """
        commit = self.repo[commit_oid]
        self.commit_byte_ct.append(len(commit.read_raw()))

        tree = commit.tree
        if tree.oid not in self.trees:
            self.trees.add(tree.oid)
            self.tree_byte_ct.append(len(tree.read_raw()))
            te_queue = deque( [te for te in tree] )
            while te_queue:
                te = te_queue.popleft()
                if 0o040000 == te.filemode:
                    if te.oid in self.trees:
                        continue
                    self.trees.add(te.oid)
                    self.tree_byte_ct.append(len(tree.read_raw()))
                    te_queue.extend([child_te
                        for child_te in p4gf_pygit2.tree_object(
                            self.repo, te)])
                elif te.filemode in [0o100644, 0o100755, 0o120000]:
                    if te.oid in self.blobs:
                        continue
                    self.blobs.add(te.oid)
                    self.blob_byte_ct.append(p4gf_pygit2.tree_object(
                        self.repo, te).size)

        self.eta.increment()
        if not commit_i % 1000:
                        # recursing through the GWT of every single commit
                        # consumes FAR too much time, turns a 2-minute scan
                        # into a 45-minute scan. So just sample it.
            self.gwt_ct.append(self.commit_to_gwt_ct(commit))
            self.file_action_ct.append(self.commit_to_diff_ct(commit))
            print(
                "{:>7,}/{:<,} commits    {:>9,} blobs    {:>9,} trees     eta {} {}"
                 .format( commit_i
                        , len(self.commits)
                        , len(self.blobs)
                        , len(self.trees)
                        , self.eta.eta_str()
                        , self.eta.eta_delta_str()
                        ))

    def ref_to_commit(self, ref_name):
        """Convert a reference string to a Commit object."""
        try:
            ref_obj = self.repo.lookup_reference(ref_name)
        except (ValueError, KeyError):  # .DS_Store shows up as a "ref". Nope.
            print("Skipping ref {}".format(ref_name))
            return None

        x = ref_obj.get_object()
        if x.type == pygit2.GIT_OBJ_TAG:
                        # Dereference tags.
            x = self.repo.get(x.target)
        if x.type == pygit2.GIT_OBJ_COMMIT:
            return x
        #raise RuntimeError("Huh? what's this: {}".format(x.hex))
        print("Skipping ref to non-commit: {}".format(x.hex))
        return None

    def all_ref_names(self):
        """Return a list of all references: branches, tags, etc."""
        return self.repo.listall_references()

    def commit_to_diff_ct(self, commit):
        """How many files touched in this commit?"""
        if not commit.parents:
            return self._diff_ct(p4gf_const.EMPTY_TREE_SHA1, commit)
        else:
            return self._diff_ct(commit.parents[0], commit)

    def commit_to_gwt_ct(self, commit):
        """How many gwt paths in the commit?"""
        return self._diff_ct(p4gf_const.EMPTY_TREE_SHA1, commit)

    def _diff_ct(self, old_commit, new_commit):
        """How many files touched?"""
        gwt_paths = set()
        diff = self.repo.diff(old_commit, new_commit)
        for patch in diff:
            gwt_paths.add(patch.old_file_path)
            gwt_paths.add(patch.new_file_path)
            self.gwt_paths.add(patch.new_file_path)
        return len(gwt_paths)

def tabular(labels, int_values):
    """Produce a tabular list of strings, "label: value"

    Integer values are right-aligned and commafied.
    """
    lwidth = max([len(s) for s in labels])
    rwidth = len("{:,}".format(max(int_values)))
    line_fmt = "{:<" + str(lwidth) + "} : {:>" + str(rwidth) + ",}"
    return [ line_fmt.format(l, r) for l, r in zip(labels, int_values) ]


if __name__ == "__main__":
    ME = GitBlobCt()
    ME.main()


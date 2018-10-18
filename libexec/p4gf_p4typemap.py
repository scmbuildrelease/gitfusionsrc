#! /usr/bin/env python3.3
"""Return a p4filetype for a given depot path."""
import re
import P4

import p4gf_p4filetype
import p4gf_util
# Convert the P4 typemap to a P4 Map ensuring
# p4gf_fast_push uses this P4 Map to map.translate(path, RIGHT2LEFT) path to type.
# Create the P4 Map from the P4 typemap as follows:
#   construct the <lhs> as <type><rhs> - thus enforcing <lhs> map uniqueness.
#   Map a typemap line: "<type> <rhs>" => P4.Map line: "<type><rhs> <rhs>"
#   Ensure that the new <lhs> inserted into the P4 Map delimits the <type> with '/':  <type>/<rhs>
#
# This permits multiple rhs P4 typemap patterns to be used in a P4 Map
# Return only <type> by stripping the <rhs>
# from the result of the map.translate() results: <type>/<rhs>

FILETYPE = re.compile(r'^([^/]*?)/.*')   # return leftmost string delimited by first '/'

class P4TypeMap:
    """Use the Perforce server's 'p4 typemap' list to calculate
    a p4filetype for a given depot path.
    """
    def __init__(self, ctx):
        self.p4map = _fetch(ctx)

    def for_depot_path(self, depot_path):
        """If depot_path matches an entry in 'p4 typemap', return that type.
        If not, return None.
        """
        if self.p4map and depot_path:
            r = self.p4map.translate(depot_path, self.p4map.RIGHT2LEFT)
            if r:
                # now strip of the de-duplicating <rhs> suffix from the lhs '<type>/<rhs>'
                # and return only the <type>
                m = FILETYPE.search(r)
                if m:
                    r = m.group(1)
            return p4gf_p4filetype.restore_plus(r)
        else:
            return None

    def has_typemap(self):
        """Do we have a non-empty typemap?
        Some calling code can avoid wasting time on calculating
        inputs to for_depot_path() if for_depot_path() will always
        answer None.
        """
        return self.p4map is not None


def _fetch(ctx):
    """If the Perforce server has a 'p4 typemap' configured, return it
    as a P4.Map instance.
    To accomodate mappings of multiple right-hand patterns to the same <type>
    append the <rhs> to the left hand <type>.
    Mapping will be unique, but require stripping the appended <rhs> postfix.
    If not, return None.
    """
    r = ctx.p4gfrun('typemap', '-o')
    raw_lines = p4gf_util.first_value_for_key(r, "TypeMap")
    if not raw_lines:
        return None

    typem = P4.Map()
    for mapline in raw_lines:
        lhs,sep,rhs = mapline.partition(' ') # the P4 typemap types are delimited by the first ' '
                # if <rhs> does not start with '/',
                # insert one when constructing new <lhs> = <type>/<rhs>
                # We dont care that '"' are embedded in the constructed new <lhs>
                # We do need to ensure that the <type> is followed immediately by a '/'
                # So we may append the <type> with the <rhs> as  <type><rhs> or <type>/<rhs>
                # and <rhs> may itself start with '/' or '"' or neither.
                # Our regex lookup will detect on the first '/'
        sep = '' # concatenate <lhs><sep><rhs>
        if not rhs.startswith('/'):  # if there is no '/' .. then add '/'
            sep = '/'
        lhs = lhs + sep + rhs
        typem.insert(lhs,rhs)

    return typem

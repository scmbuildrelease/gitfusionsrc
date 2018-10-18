#! /usr/bin/env python3.3
"""ObjectTypeList."""
from collections import Sequence


# pylint:disable=R0924
# pylint does not realize this is an immutable sequence
class ObjectTypeList(Sequence):

    """Immutable list of ObjectType instances.

    Compares to other instances using sha1.
    Simply wraps an instance of the built-in list type.
    """

    def __init__(self, sha1, ot_list):
        self.sha1 = sha1
        self.ot_list = ot_list

    def __hash__(self):
        return hash(self.sha1)

    def __eq__(self, other):
        return self.sha1 == other.sha1

    def __ne__(self, other):
        return self.sha1 != other.sha1

    def __ge__(self, other):
        return self.sha1 >= other.sha1

    def __gt__(self, other):
        return self.sha1 > other.sha1

    def __le__(self, other):
        return self.sha1 <= other.sha1

    def __lt__(self, other):
        return self.sha1 < other.sha1

    def __getitem__(self, index):
        return self.ot_list[index]

    def __len__(self):
        return len(self.ot_list)

    def __str__(self):
        return "ObjectTypeList[{}]".format(self.sha1[:7])
# pylint:enable=R0924

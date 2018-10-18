#! /usr/bin/env python3.3
"""ObjectTypeCache."""

import bisect
from   collections import MutableSequence
import random

from   p4gf_l10n      import _
from   p4gf_object_type_list import ObjectTypeList


class ObjectTypeCache(MutableSequence):

    """
    Maintains a limited number of sorted ObjectTypeList objects. When more
    objects than MAX_LEN have been appended, a random selection of elements
    will be removed to make room for any new additions. The objects are
    sorted according to their natural order.

    Insertion into the sequence is always done in a sorted manner (i.e.
    index is ignored). Because the list is backed by an array, insertions
    incur a O(N) complexity cost.
    """

    MAX_LEN = 1000

    def __init__(self):
        self.__count = 0
        self.__array = [None] * ObjectTypeCache.MAX_LEN

    def __len__(self):
        return self.__count

    def __contains__(self, value):
        if isinstance(value, str):
            value = ObjectTypeList(value, None)
        i = bisect.bisect_left(self.__array, value, hi=self.__count)
        if i != self.__count and self.__array[i] == value:
            return True
        return False

    def index(self, value):
        i = bisect.bisect_left(self.__array, value, hi=self.__count)
        if i != self.__count and self.__array[i] == value:
            return i
        raise ValueError

    def __getitem__(self, index):
        if index == self.__count or abs(index) > self.__count:
            # pylint disable R0801 (similar lines) does not work?
            raise IndexError(_('index out of bounds'))
        if index < 0:
            # special case for negative indices
            return self.__array[self.__count + index]
        return self.__array[index]

    def get(self, sha1):
        """Retrieve item based on the given SHA1 value. Return None if not found."""
        value = ObjectTypeList(sha1, None)
        i = bisect.bisect_left(self.__array, value, hi=self.__count)
        if i != self.__count and self.__array[i] == value:
            return self.__array[i]
        return None

    def __setitem__(self, index, value):
        raise IndexError(_('only append operations allowed'))

    def __delitem__(self, index):
        # pylint disable R0801 (similar lines) does not work?
        raise IndexError(_('special list type, no deletions'))

    def __str__(self):
        return str(self.__array[:self.__count])

    def clear(self):
        """Remove all elements from this cache."""
        self.__count = 0
        for i in range(len(self.__array)):
            self.__array[i] = None

    def insert(self, index, value):
        raise IndexError(_('only append operations allowed'))

    def append(self, value):
        """Add the given value to the list, maintaining length and order."""
        idx = bisect.bisect_left(self.__array, value, hi=self.__count)
        # pylint disable R0801 (similar lines) does not work?
        if self.__count == len(self.__array):
            # reached size limit, remove a random element
            mark = random.randrange(self.__count)
            if idx == self.__count:
                # inserting beyond the end
                idx -= 1
            if mark < idx:
                for i in range(mark, idx - 1):
                    self.__array[i] = self.__array[i + 1]
            else:
                # pylint disable R0801 (similar lines) does not work?
                for i in range(mark, idx, -1):
                    self.__array[i] = self.__array[i - 1]
            self.__array[idx] = value
        else:
            # there is room enough for more
            if idx == self.__count:
                # goes at the end
                self.__array[idx] = value
                # pylint disable R0801 (similar lines) does not work?
            else:
                # goes somewhere other than the end
                for i in range(self.__count, idx, -1):
                    self.__array[i] = self.__array[i - 1]
                # pylint disable R0801 (similar lines) does not work?
                self.__array[idx] = value
            self.__count += 1

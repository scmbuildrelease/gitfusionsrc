#! /usr/bin/env python3.3
"""Calculating and dumping a histogram.

Used for internal instrumentation.
"""

import logging
import math

# pylint:disable=W9903
# non-gettext-ed string
# This is a debug dump, no L10N required.

LOG = logging.getLogger(__name__)


def _round(val):
    """Return some round-ish integer number like 5, 10, 25, etc."""
    if val < 1:
        return 1
    e = pow(10, int(math.log10(val)))
    m = val / e
    # pylint: disable=multiple-statements
    # More than one statement on a single line; keep tabular code tabular.
    if   m <= 1.0: m =  1.0
    elif m <= 2.5: m =  2.5
    elif m <= 5.0: m =  5.0
    else:          m = 10.0
    return int(m * e)


def bucket_ends_linear(min_val, max_val, bucket_ct):
    """Return a list of maximum values, one for each bucket.

    Often returns fewer than bucket_ct buckets due to choosing nice round
    bucket sizes.
    """
    val_range  = max_val - min_val
    if val_range < 1:
        val_range = 1
    step_width = _round(float(val_range) / float(bucket_ct))
    if step_width < 1:
        step_width = 1
    step_ct    = int(val_range / step_width)
    if step_ct < 1:
        step_ct = 1
    if bucket_ct % step_width:
        step_ct += 1
    return [min_val + step_width * i for i in range(1, 1 + step_ct)]


def bucket_ends_logarithmic(min_val, max_val):
    """Return a list of maximum values, one for each bucket."""
    min_pow10 =     int(math.log10(min_val)) if 1 < min_val else 1
    max_pow10 = 1 + int(math.log10(max_val)) if 1 < max_val else 1
    bucket_ends = []
    for pow10 in range(min_pow10, 1+max_pow10):
        x10 = int(math.pow(10, pow10))
        for k in [1, 2, 5]:
            end = k * x10
            bucket_ends.append(end)
            if max_val < end:
                        # Early return if we only need "200" and not "500"
                return bucket_ends
    return bucket_ends


def to_histogram(coll, bucket_ct=10, bucket_prefix=[1, 2, 5], log=False):
    """How many elements of coll have a value of X?

    bucket_prefix is a set of (usually very small-count) buckets to place at
    the front of the bucket list. This helps dig deeper into long-tail
    counts like the 500 different entries with a count of 1 or 2.
    """
    # pylint: disable=dangerous-default-value
    # Yes, it is indeed dangerous since the default value is NOT const and
    # to_histogram() could (evil!) modify the contents of the default. But it
    # doesn't. It treats it as const and only reads.
    min_value = min(coll)
    max_value = max(coll)
    if log:
        bucket_ends = bucket_ends_logarithmic(min_value, max_value)
    else:
        bucket_ends = bucket_ends_linear(min_value, max_value, bucket_ct)

    # Force long-tail buckets:
    for be in reversed(bucket_prefix):
        if be < bucket_ends[0]:
            bucket_ends = [be] + bucket_ends
    hist = {be: 0 for be in bucket_ends}
    for val in coll:
        for be in bucket_ends:
            if val <= be:
                hist[be] += 1
                break
    return hist


def bar_of_stars(nom, denom, max_stars=60):
    """Return a string of *, suitable for a bar for a bar graph."""
    if not (denom and max_stars):
        return ''

    ct = float(nom) / float(denom) * float(max_stars)
    return '*' * int(ct + 0.5)


def digit_count(n):
    """How many digits in n, including space for commas?"""
    if n < 1:
        return 1
    return len("{:,d}".format(n))


def to_lines(histo):
    """Return a list of lines that bring out a histogram."""
    max_val = max(histo.values())
    bucket_ends   = sorted(histo.keys())
    be_fmt_width  = str(digit_count(bucket_ends[-1]))
    val_fmt_width = str(digit_count(max_val))
    fmt = ( "{bb:"  + be_fmt_width  + ",d}-"
          + "{be:"  + be_fmt_width  + ",d} : "
            "{val:" + val_fmt_width + ",d} {bar}")
    bb = 0
    lines = []
    for be in bucket_ends:
        val = histo[be]
        _bar = bar_of_stars(val, max_val)
        s = fmt.format(bb=bb, be=be, val=histo[be], bar=_bar)
        lines.append(s)
        bb = be + 1
    return lines

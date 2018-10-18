#! /usr/bin/env python3.3
"""Tools for converting to/from different possible time zones.

Optimally lazy programmers do all their work in UTC, convert to
time zone only at the last minute before displaying a value.

Common stuff:

    dt = git_to_utc_dt("1423782419 -0800")

    dt = now_utc_dt()   # current time in UTC

                        # from seconds since the epoch
    dt = datetime.datetime.fromtimestamp(seconds_since_epoch)

    dt.timestamp()      # to seconds since the epoch

                        # in Git Fusion server's timezone
    dt.astimezone(local_tzinfo())

                        # in Perforce server's timezone
    dt.astimezone(server_tzinfo(ctx))
"""
                        # lru_cache(maxsize=1) is easier to write than
                        # our own module-wide caching variables and
                        # "if x is not None then do work" code.
from   functools    import lru_cache
import datetime
import logging
import pytz

import p4gf_const
from   p4gf_l10n    import _, NTR
import p4gf_p4key   as P4Key

LOG = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def server_tzname(ctx):
    """Return the Perforce server's timezone name, such as "US/Pacific".

    Does NOT return ambiguous and annoying local abbreviations such as
    "EST" or "PDT".

    Cache this result, or use get_server_time(), to avoid pointless
    round-trips to the server.
    """
    value = P4Key.get(ctx.p4gf, p4gf_const.P4GF_P4KEY_TIME_ZONE_NAME)
    if value == '0' or value is None:
        # Upgrade from an EA system, perhaps, in which the upgrade p4keys
        # have been set but the later changes where not applied during init.
        msg = _("p4key '{key}' not set, using UTC as default."
                " Change this to your Perforce server's time zone.") \
            .format(key=p4gf_const.P4GF_P4KEY_TIME_ZONE_NAME)
        LOG.warning(msg)
        value = NTR('UTC')
    return value


@lru_cache(maxsize=1)
def server_tzinfo(ctx):
    """Return the pytz timezone object for the Perforce server."""
    tzname = server_tzname(ctx)
    try:
        return pytz.timezone(tzname)
    except pytz.exceptions.UnknownTimeZoneError:
        LOG.warning("Time zone name '{}' unrecognized, using UTC as default"
                 .format(tzname))
    return pytz.utc


@lru_cache(maxsize=1)
def local_tzinfo(ctx):
    """Return this Git Fusion server's local timezone pytz object.

    Uses optional module tzlocal to fetch the local computer's timezone object.
    Module pytz lacks an API for this (!).
    If tzlocal not installed, assume this Git Fusion server runs on the
    same timezone as the Perforce server.

    ctx used only if we have to fallback to server timezone.
    """
    try:
        import tzlocal  # Optional, see https://github.com/regebro/tzlocal
        return tzlocal.get_localzone()
    except ImportError:
        pass
    return server_tzinfo(ctx)


def now_utc_dt():
    """Return the current time, in UTC."""
    return datetime.datetime.now(tz=pytz.utc)


def git_to_utc_dt(git_str):
    """Convert a `git fast-export` string such as "1423782419 -0800"
    to a datetime object in UTC.

    If tzoffset is present, applies it.
    """
    if " " not in git_str:
        seconds       = int(git_str)
        return datetime.datetime.fromtimestamp(seconds, pytz.utc)

    w = git_str.split(" ")
    return git_strs_to_utc_dt( seconds_str  = w[0]
                             , tzoffset_str = w[1] )


def seconds_to_utc_dt(seconds_int):
    """Convert integer seconds since the epoch to a utc_dt object.
    No input timezone: assumes those seconds are already in UTC.
    """
    return datetime.datetime.fromtimestamp(seconds_int, pytz.utc)


def git_strs_to_utc_dt(seconds_str, tzoffset_str):
    """Convert `git fast-export` time strings, already split
    into two such as "1423782419", "-0800", into a datetime object
    in UTC.
    """
    seconds       = int(seconds_str)
    source_tzinfo = datetime.datetime.strptime(tzoffset_str, "%z")
    source_dt     = datetime.datetime.fromtimestamp(seconds, source_tzinfo)
    return source_dt.astimezone(pytz.utc)


@lru_cache(maxsize=1)
def utc_dt_to_p4d_secs(utc_dt, ctx):
    """Convert UTC to seconds-since-the-epoch, in the server's timezone."""
    return int(utc_dt.astimezone(server_tzinfo(ctx)).timestamp())

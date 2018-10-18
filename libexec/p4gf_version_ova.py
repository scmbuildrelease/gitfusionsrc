#! /usr/bin/env python
"""Functions to check p4d version."""

import sys
import os
import re
import p4gf_const
InfoResults = None

# git-fusion/bin/Version file contains standard Perforce version
# info. This Python script loads that file and parses it.
_VERSION_FILENAME = 'Version'

# Constant strings that are part of as_dict()'s response.
_PRODUCT_DICT = {
'product_abbrev' : 'Git Fusion',
'product_long'   : 'Perforce Git Fusion',
'copyright'      : 'Copyright 2012-2016 Perforce Software.  All rights reserved.',
'company'        : 'Perforce - The Fast Software Configuration Management System.',
}
def fetch_info(p4):
    """Fetch the 'info' from the Perforce server."""
    global InfoResults
    if InfoResults is None:
        InfoResults = _first_dict(p4.run('info', '-s'))
    return InfoResults


# Copied from p4gf_util to avoid a cyclic import.
def _first_dict(result_list):
    """Return the first dict result in a p4 result list."""
    for e in result_list:
        if isinstance(e, dict):
            return e
    return None

def p4d_version_string(p4):
    """Return the serverVersion string from 'p4 info'.

    Typically this looks like:

    P4D/LINUX26X86_64/2012.2.PREP-TEST_ONLY/506265 (2012/08/07)
    """
    _version_string = None
    info = fetch_info(p4)
    version_key = 'serverVersion'
    if version_key in info:
        _version_string = info[version_key]

    return _version_string

def parse_p4d_version_string(version_string):
    """Convert a long server version string to a dict of parts:

    P4D/LINUX26X86_64/2012.2.PREP-TEST_ONLY/506265 (2012/08/07)

    product_abbrev  : P4D
    platform        : LINUX26X86_64
    release_year    : 2012
    release_sub     : 2
    release_codeline: PREP-TEST_ONLY
    patchlevel      : 506265
    date_year       : 2012
    date_month      : 08
    date_day        : 07
    """

    a = version_string.split(' ')
    b = a[0].split('/')
    result = {}
    result['product_abbrev' ] = b[0]
    result['platform'       ] = b[1]

    r = b[2]
    m = re.search(r'^(\d+)\.(\d+)', r)
    result['release_year'   ] = m.group(1)
    result['release_sub'    ] = m.group(2)

    # Optional codeline
    m = re.search(r'^(\d+)\.(\d+)\.(.*)', r)
    if m:
        result['release_codeline'] = m.group(3)

    result['patchlevel'     ] = b[3]

    m = re.search(r'(\d+)/(\d+)/(\d+)', a[1])
    result['date_year'      ] = m.group(1)
    result['date_month'     ] = m.group(2)
    result['date_day'       ] = m.group(3)
    return result

# What patch level required for each year.sub?
_P4D_REQ_PATCH = p4gf_const.P4GF_REQ_PATCH

_P4D_REQ_YEAR_SUB = min(_P4D_REQ_PATCH.keys())
_P4D_MAX_YEAR_SUB = max(_P4D_REQ_PATCH.keys())


def p4d_version_required(version_string):
    """Return a 2-tuple (2010.2, 503309) of the (year.sub, patch) REQUIRED
    for the given p4d year.sub.
    """
    d = parse_p4d_version_string(version_string)
    year_sub_string = d['release_year'] + '.' + d['release_sub']
    year_sub = float(year_sub_string)

    if year_sub < _P4D_REQ_YEAR_SUB:
        return (_P4D_REQ_YEAR_SUB, _P4D_REQ_PATCH[_P4D_REQ_YEAR_SUB])

    if year_sub in _P4D_REQ_PATCH:
        return (year_sub, _P4D_REQ_PATCH[year_sub])

    return (_P4D_REQ_YEAR_SUB, _P4D_REQ_PATCH[_P4D_REQ_YEAR_SUB])


def p4d_version_acceptable(version_string):
    """Is this a P4D with the 'p4 reviews -C' patch?"""
    d = parse_p4d_version_string(version_string)
    year_sub_string = d['release_year'] + '.' + d['release_sub']
    year_sub = float(year_sub_string)
    patch_level = int(d['patchlevel'])

    if year_sub < _P4D_REQ_YEAR_SUB:
        return False

    if _P4D_MAX_YEAR_SUB < year_sub:
        return True

    if year_sub not in _P4D_REQ_PATCH:
        return False

    return _P4D_REQ_PATCH[year_sub] <= patch_level
def p4d_version_check(p4):
    """Check that p4d version is acceptable.

    if p4 is not connected, it will be connected/disconnected
    if already connected, it will be left connected
    """

    version_string = p4d_version_string(p4)

    acceptable = p4d_version_acceptable(version_string)
    if not acceptable:
        vr = p4d_version_required(version_string)
        msg = ('Unsupported p4d version: {actual}'
                 '\nGit Fusion requires version {req_v}/{req_ch} or later'
               .format(actual=version_string,
                       req_v=vr[0],
                       req_ch=vr[1]))
        raise RuntimeError(msg)

def _load_version_file():
    """Read file git-fusion/bin/Version and return it as a string array."""
    bin_dir_path =  "/opt/perforce/git-fusion/libexec"
    version_file_path = os.path.join(bin_dir_path, _VERSION_FILENAME)
    with open(version_file_path, 'r') as f:
        return f.readlines()

def _parse_release_val(release_val):
    """Return release year and sub, and optional codeline"""
    ysc = re.match(r'(20..) (\d) ([\w-]*)', release_val)
    result = {}
    if ysc:
        result['release_year'    ] = ysc.group(1).strip()
        result['release_sub'     ] = ysc.group(2).strip()
        result['release_codeline'] = ysc.group(3).strip()
        return result

    ys = re.match(r'(20..) (\d)', release_val)
    result = {}
    if ys:
        result['release_year'    ] = ys.group(1).strip()
        result['release_sub'     ] = ys.group(2).strip()
        return result

    return result

def _parse_version_file_contents(line_array):
    """Extract RELEASE, PATCHLEVEL and SUPPDATE out of a Version file
    and return them as fields in a dict.
    Further break down RELEASE and SUPPDATE into subfields.
    """
    result = {}
    re_release    = re.compile(r'RELEASE\s*=\s*(.*)\s*;')
    re_patchlevel = re.compile(r'PATCHLEVEL =\s*(.*)\s*;')
    re_suppdate   = re.compile(r'SUPPDATE\s*=\s*(.*)\s*;')

    for line in line_array:

        m = re_release.match(line)
        if m:
            release = m.group(1).strip()
            result['release'] = release
            r = _parse_release_val(release)
            for k in r:
                result[k] = r[k]
            continue

        m = re_patchlevel.match(line)
        if m:
            result['patchlevel'] = m.group(1).strip()
            continue

        m = re_suppdate.match(line)
        if m:
            date = m.group(1).strip()
            result['date'] = date
            ymd = re.match(r'(20..) (\d+) (\d+)', date)
            if ymd:
                result['date_year' ] = ymd.group(1).strip()
                result['date_month'] = ymd.group(2).strip()
                result['date_day'  ] = ymd.group(3).strip()
            continue

    return result

def as_dict():
    """Return all of our version fields broken out into dict elements.

    :param include_checksum: if True, checksum scripts and include SHA1 result.

    """
    result = _parse_version_file_contents(_load_version_file())
    for key in _PRODUCT_DICT:
        result[key] = _PRODUCT_DICT[key]
    return result

def as_single_line():
    """Return a single-line version string suitable for use as a program name.

    P4GF/2012.1.PREP-TEST_ONLY/415678 (2012/04/14)
    """
    d = as_dict()

    if 'release_codeline' in d:
        return "{product_abbrev}/{release_year}.{release_sub}" \
                  + ".{release_codeline}/{patchlevel} ({date_year}" \
                  + "/{date_month}/{date_day})".format(**d)
    else:
        return "{product_abbrev}/{release_year}.{release_sub}" \
                  + "/{patchlevel} ({date_year}" \
                  + "/{date_month}/{date_day})".format(**d)

def p4d_supports_protects(p4):
    """Check that p4d is ALREADY configured to give git-fusion-user
    permission to run 'p4 protects -u'. Return False if not.

    *** This function does not really belong here! ***
    This is NOT a version check, but I'm sticking it in this
    file anyway because the OVA web UI needs this function, and the OVA
    web UI already has access to p4gf_version_26.py.
    """
    okay = False
    try:
        p4.run('protects', '-u', p4gf_const.P4GF_USER, '-m')
        # if we can use -u, we're good to go
        okay = True
    except Exception:   # pylint: disable=W0703
        # Catching too general exception
        e = sys.exc_info()[1]
        if re.search('Protections table is empty.',str(e)):
            okay = True
        # All other errors are fatal
    return okay


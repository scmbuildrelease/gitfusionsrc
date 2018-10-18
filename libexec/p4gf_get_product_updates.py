#! /usr/bin/env python3.3
"""Program to check for product updates at updates.perforce.com.

Using an https call,
retrieve as json the current Git Fusion product version from updates.perforce.com
and send the local Git Fusion and P4D versions to Perforce.
Parse the returned json containing the current Git Fusion version and report a message
whether updates are available.
"""


import urllib.request
import urllib.error
import json
import p4gf_util
import p4gf_version_3
from   p4gf_l10n      import _

NO_UPDATES_EXISTS = _("""
Your Git Fusion version {version} is up to date.
""")
UPDATES_EXIST = _("""
There are updates available for Git Fusion.
You have version {have_version}. The current version is {current_version}.
""")
PATCH_EXISTS = _("""
There are updates available for Git Fusion.
Your Git Fusion version {version} is up to date.
However there is a patch available.
""")
URL_ERROR_MSG = _("""
Error attempting to determine current Git Fusion product versions.
url: {url}
{error}
""")
ERROR_MSG = _("""
Error attempting to determine current Git Fusion product versions.
""")
JSON_KEY_ERROR_MSG = _("""
Unexpected json format from updates.perforce.com.
""")
GF_PRODUCT_URL = 'https://updates.perforce.com/static/Git%20Fusion/Git%20Fusion.json'


# url format for retrieving as json the current product versions and reporting local versions
# https://updates.perforce.com/static/Git%20Fusion/Git%20Fusion.json?product=Git%20Fusion/NOARCH/2014.1/1013672
#  %26product=P4D/DARWIN90X86_64/2015.2.MAIN-TEST_ONLY/1032249
#
def main():
    """Program to check for product updates at updates.perforce.com."""

    desc = _("Report if updates are available by checking at updates.perforce.com.")
    parser = p4gf_util.create_arg_parser(desc=desc, add_debug_arg=True)
    parser.add_argument('--p4port', '-p', metavar='P4PORT',
                        help=_('P4PORT of server - optional - also report P4D version '
                               'data to Perforce'))
    args = parser.parse_args()

    # get the Git Fusion and P4D product version strings
    (gf_version, server_version) = get_product_version_strings(args)

    # Get the local GF version info as dict
    this_version = p4gf_version_3.as_dict(include_checksum=True)

    # Munge version strings into url paramters required by updates.perforce.com
    # add NOARCH to gf version
    gf_version = gf_version.replace('Git Fusion', 'Git%20Fusion/NOARCH')
    url = GF_PRODUCT_URL + '?product=' + gf_version
    if server_version:
        url = url + '%26product=' + server_version
    # Ensure all spaces are encoded
    url = url.replace(' ', '%20')
    if 'debug' in args:
        print("debug: url:{}".format(url))
    try:
        webfile = urllib.request.urlopen(url)
    except (urllib.error.URLError, urllib.error.HTTPError) as e:
        return URL_ERROR_MSG.format(url=url, error=str(e))

    # make the query to the url
    data = webfile.read()
    if not data:
        return ERROR_MSG
    product_version = json.loads(data.decode())
    if 'debug' in args:
        print("debug: json data:{}".format(product_version))
    if 'current' not in product_version:
        return JSON_KEY_ERROR_MSG

    # Parse the data and compare
    c = product_version['current']
    current_year_sub = year_sub_to_float(c['major'], c['minor'])
    this_version_year_sub = year_sub_to_float(this_version['release_year'],
                                              this_version['release_sub'])

    message = NO_UPDATES_EXISTS.format(version=current_year_sub)
    if this_version_year_sub < current_year_sub:
        message = UPDATES_EXIST.format(have_version=this_version_year_sub,
                                       current_version=current_year_sub)
    elif this_version_year_sub == current_year_sub and this_version['patchlevel'] < c['build']:
        message = PATCH_EXISTS.format(version=current_year_sub)
    return message


def get_product_version_strings(args):
    """Return the Git Fusion and (optionally) P4D version strings.
     The Git Fusion version line looks like this:
        'Rev. Git Fusion/2015.1/1013673 (2014/11/21).'
     The P4D version line looks like this:
        'Server version: P4D/DARWIN90X86_64/2015.2.MAIN-TEST_ONLY/1032249 (2015/03/26)'
    """
    if args and 'p4port' in args and args.p4port:
        this_version_array = p4gf_version_3.as_string_extended(
            args=args, include_checksum=True).split('\n')
    else:
        this_version_array = p4gf_version_3.as_string(include_checksum=True).split('\n')
    gf_version = None
    server_version = None
    for l in this_version_array:
        if l.startswith('Rev'):
            gf_version = l.split('(')[0].split(' ', 1)[1].strip()
        if l.startswith('Server version:'):
            server_version = l.split(' ')[2].strip()
    return (gf_version, server_version)


def year_sub_to_float(year, sub):
    """Convert year, sub to float."""
    return float(year + '.' + sub)


if __name__ == "__main__":
    print(main())

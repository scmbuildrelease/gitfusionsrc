#! /usr/bin/env python3.3
"""Cache of results fetched from Perforce."""

# Run 'p4 info' un-tagged to get human-friendly server info labels.
# But un-tagged doesn't return 'unicode' so run tagged to get that.
InfoResults = None
InfoResultsTagged = None


def fetch_info(p4, tagged=True):
    """Fetch the 'info' from the Perforce server.

    For tagged output result is a dict.
    For un-tagged output result is a list of str.
    """
    global InfoResults, InfoResultsTagged
    if tagged:
        if InfoResultsTagged is None:
            with p4.while_tagged(True):
                InfoResultsTagged = _first_dict(p4.run('info', '-s'))
        return InfoResultsTagged
    if InfoResults is None:
        with p4.while_tagged(False):
            InfoResults = p4.run('info', '-s')
    return InfoResults


# Copied from p4gf_util to avoid a cyclic import.
def _first_dict(result_list):
    """Return the first dict result in a p4 result list."""
    for e in result_list:
        if isinstance(e, dict):
            return e
    return None

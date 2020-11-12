#!/usr/bin/env python3

""" These functions are modified for python3 from the implementations
found at:
http://ezid.lib.purdue.edu/doc/apidoc.html#request-response-bodies
"""

import re


def escape_string(s):
    """ Escape a string for ANVL. """

    return re.sub("[%:\r\n]", lambda c: "%%%02X" % ord(c.group(0)), s)


def unescape_string(s):
    """ Unescape an ANVL string. """

    return re.sub("%([0-9A-Fa-f][0-9A-Fa-f])",
                  lambda m: chr(int(m.group(1), 16)), s)


def escape_dictionary(d):
    """ Converts a python dict to an ANVL string."""

    return "\n".join("%s: %s" % (escape_string(name), escape_string(value)) for name, value in d.items())


def unescape_dictionary(d):
    """ Takes an escaped ANVL string and returns the corresponding
    python dictionary."""

    return dict(tuple(unescape_string(v).strip() for v in l.split(":", 1)) \
                for l in d.splitlines())

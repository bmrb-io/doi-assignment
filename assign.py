#!/usr/bin/env python

""" Script to assign the DOIs. """

from __future__ import print_function

import anvl

import sys
import optparse
import requests
import psycopg2
import pynmrstar

# Specify some basic information about our command
usage = "usage: %prog"
parser = optparse.OptionParser(usage=usage,version="%prog .1", description="Assign DOIs to new entries and make sure existing entries DOI information is up to date.")

# Specify the common arguments
parser.add_option("--full-run", action="store_true", dest="full", default=False, help="Try to add or update all entries in the DB and not just new ones.")
parser.add_option("--dry-run", action="store_true", dest="dry_run", default=False, help="Do a dry run. Print what would be done but don't do it.")
parser.add_option("--withdraw", action="store_true", dest="withdrawn", default=False, help="Withdraw withdrawn entries.")
parser.add_option("--verbose", action="store_true", dest="verbose", default=False, help="Be verbose.")
parser.add_option("--days", action="store", dest="days", default=7, type="int", help="How many days back should we assign DOIs?")
parser.add_option("--manual", action="store", dest="override", type="str", help="One entry ID to manually test.")

# Options, parse 'em
(options, args) = parser.parse_args()

class ServerError(EnvironmentError):
    """ An error on the EZID server. """
    pass

class AuthenticationError(EnvironmentError):
    """ An authentication error on the EZID server. """
    pass

class EZIDSession():
    """ A session with the EZID server."""

    ezid_base = 'https://ezid.cdlib.org'
    ezid_username = 'apitest'
    ezid_password = ''
    shoulder = 'doi:10.5072/FK2'

    def __init__(self):
        pass

    def __enter__(self):
        """ Get a session cookie to use for future requests. """

        if options.verbose:
            print("Establishing session...")

        self.session = requests.Session()
        self.session.headers.update({'Accept': 'text/plain',
                                     'Content-Type': 'text/plain'})

        # First make sure the server is online
        r = self.session.get(self.ezid_base + "/status")
        if r.status_code != 200 or not "success: EZID is up" in r.text:
            raise ServerError("EZID server appears offline: (%s, '%s')" % (r.status_code, r.text))

        # Then get the session key
        r = self.session.get(self.ezid_base + "/login",
                             auth=(self.ezid_username, self.ezid_password))

        if "error" in r.text or r.status_code != 200:
            raise AuthenticationError("Server responded with: %s" % r.text.rstrip())

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ End the current session."""

        if options.verbose:
            print("Closing session...")

        # End the EZID session
        r = self.session.get(self.ezid_base + "/logout")
        # End the HTTP session
        self.session.close()
        # If there was an error closing the session raise it
        r.raise_for_status()

    def determine_doi(self, entry):
        """ Determines the DOI for an entry."""

        if entry.startswith("bmse") or entry.startswith("bmst"):
            return '%s%s' % (self.shoulder, entry.upper())
        elif entry.startswith("bmr"):
            return '%s%s' % (self.shoulder, entry.upper())
        else:
            return '%sBMR%s' % (self.shoulder, entry)

    def get_id(self, doi):
        """ Returns the information about a DOI."""

        r = self.session.get("%s/id/%s" % (self.ezid_base, doi))
        r.raise_for_status()

        return anvl.unescape_dictionary(r.text)

    def create_doi(self, entry):
        """ Creates a new DOI for an entry without one. """

        if options.verbose:
            print("Creating DOI for entry: %s" % entry)

        entry_meta = get_entry_metadata(entry)
        doi = self.determine_doi(entry)
        url = "%s/id/%s" % (self.ezid_base, doi)

        r = self.session.put(url, data=anvl.escape_dictionary(entry_meta))

        # Success
        if r.status_code < 300:
            return True
        # DOI already exists
        elif r.status_code == 400:
            if options.verbose:
                print("Failed! DOI already exists.")
            return False
        # An actual error
        else:
            raise ServerError("Server responded with: %s" % r.text.rstrip())

    def withdraw(self, entry):
        """ Withdrawn an entry. """

        try:
            ent = self.get_id(self.determine_doi(entry))
            if "unavailable" in ent['_status']:
                if options.verbose:
                    print("No need to do anything - already withdrawn: %s" % entry)
            else:
                doi = self.determine_doi(entry)
                url = "%s/id/%s" % (self.ezid_base, doi)
                r = self.session.post(url, data=anvl.escape_dictionary({'_status': 'unavailable | withdrawn by author'}))
                if r.status_code < 300:
                    if options.verbose:
                        print("Withdrew entry: %s" % entry)
                else:
                    if options.verbose:
                        print("Failed to withdraw entry: %s" %s)
        # No entry assigned, don't need to withdraw
        except requests.exceptions.HTTPError:
            if options.verbose:
                print("Withdraw - entry never assigned: %s" % entry)

    def update_doi(self, entry):
        """ Update the metadata for an entry. """

        entry_meta = get_entry_metadata(entry)
        doi = self.determine_doi(entry)
        url = "%s/id/%s" % (self.ezid_base, doi)

        r = self.session.post(url, data=anvl.escape_dictionary(entry_meta))

        # Success
        if r.status_code < 300:
            if options.verbose:
                print("Success!")
            return True
        # An actual error
        else:
            raise ServerError("Server responded with: %s" % r.text.rstrip())

    def create_or_update_doi(self, entry):
        """ Try to create a new DOI. If that fails update the
        existing one."""

        try:
            ent = self.get_id(self.determine_doi(entry))
            meta = get_entry_metadata(entry)
            for key in ['_target', '_profile', 'datacite.title', 'datacite.resourcetype', 'datacite.publisher', 'datacite.creator', 'datacite.publicationyear', 'datacite.Date', 'datacite.dateType', '_status']:
                if ent[key].strip() != meta[key].strip():
                    if options.verbose:
                        print("%s: Updating DOI because of data change." % entry)
                        print("Key %s\nOld: '%s'\nNew: '%s'" % (key, meta[key], ent[key]))
                    status = self.update_doi(entry)
                    return

            if options.verbose:
                print("%s: Skipping up to date entry." % entry)

        except requests.exceptions.HTTPError:
            status = session.create_doi(entry)
            if options.verbose:
                print("%s: Created new DOI for entry." % entry)
            return

def get_entry_metadata(entry):
    """ Returns a python dict with all of the known information about
    an entry."""

    ent = pynmrstar.Entry.from_database(entry)

    meta = {'_profile': 'datacite'}

    # Link should be to entry directory
    if entry.startswith("bmse"):
        meta['_target'] = 'http://www.bmrb.wisc.edu/metabolomics/mol_summary/show_data.php?id=%s' % entry
    elif entry.startswith("bmst"):
        meta['_target'] = 'http://www.bmrb.wisc.edu/metabolomics/mol_summary/show_theory.php?id=%s' % entry
    else:
        meta['_target'] = 'http://www.bmrb.wisc.edu/data_library/summary/?bmrbId=%s' % entry

    # Title
    meta['datacite.title'] = ent.get_tag("entry.title")[0].replace("\n","")
    meta['datacite.resourcetype'] = 'Dataset'
    meta['datacite.publisher'] = 'Biological Magnetic Resonance Bank'

    # Get the authors in the form Last Name, Middle Initial, First Name,;
    authors = ent.get_loops_by_category('entry_author')[0].filter(['_Entry_author.Family_name', '_Entry_author.Middle_initials', '_Entry_author.Given_name']).data
    mod_auths = []
    for auth in authors:
        # Remove missing values
        for pos, item in enumerate(auth):
            if item == "" or item == ".":
                auth.pop(pos)
        # This should never fail since we shouldn't have empty authors
        assert len(auth) > 0
        mod_auths.append(", ".join(auth) + ",")
    meta['datacite.creator'] = ";".join(mod_auths)

    # Get first release date
    release_loop = ent.get_loops_by_category('release')[0]
    release_loop.sort_rows('release_number')
    meta['datacite.publicationyear'] = release_loop.get_tag('date')[0][:4]
    meta['datacite.Date'] = release_loop.get_tag('date')[0]
    meta['datacite.dateType'] = 'Available'
    meta['_status'] = "public"

    return meta

if __name__ == "__main__":

    # Fetch entries
    cur = psycopg2.connect(user='ets', host='torpedo', database='ETS').cursor()

    if options.full:
        entries = requests.get("https://webapi.bmrb.wisc.edu/v2/list_entries?database=macromolecules").json()
        entries.extend(requests.get("https://webapi.bmrb.wisc.edu/v2/list_entries?database=metabolomics").json())
    else:
        cur.execute("""SELECT bmrbnum FROM entrylog WHERE status LIKE 'rel%%' AND accession_date  > current_date - interval '%d days';""" % options.days)
        entries = [str(x[0]) for x in cur.fetchall()]

    if options.withdrawn:
        cur.execute("""SELECT bmrbnum FROM entrylog WHERE status LIKE 'awd%' AND bmrbnum IS NOT NULL ORDER BY bmrbnum;""")
        withdrawn = [str(x[0]) for x in cur.fetchall()]

    if options.override:
        entries = [options.override]

    if options.dry_run:
        for en in entries:
            print("Create or update: %s" % EZIDSession().determine_doi(en))
        if options.withdrawn:
            for en in withdrawn:
                print("Withdraw: %s" % EZIDSession().determine_doi(en))
        sys.exit(0)

    # Start a session
    with EZIDSession() as session:
        # Assign or update
        for entry in entries:
            session.create_or_update_doi(entry)

        # Withdraw
        if options.withdrawn:
            for entry in withdrawn:
                session.withdraw(entry)

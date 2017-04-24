#!/usr/bin/env python3

""" Script to assign the DOIs. """

import anvl

import optparse
import requests
import pynmrstar

# Specify some basic information about our command
usage = "usage: %prog"
parser = optparse.OptionParser(usage=usage,version="%prog .1", description="Assign DOIs to new entries and make sure existing entries DOI information is up to date.")

# Specify the common arguments
parser.add_option("--new-only", action="store_true", dest="new", default=False, help="Only assign new DOIs. Do not update old DOIs.")
parser.add_option("--update-only", action="store_true", dest="update", default=False, help="Only update existing entries. Do no assign new DOIs")
parser.add_option("--dry-run", action="store_true", dest="dry_run", default=False, help="Do a dry run. Print what would be done but don't do it.")
parser.add_option("--verbose", action="store_true", dest="verbose", default=False, help="Be verbose.")

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
    
    def __init__(self):
        pass
        
    def __enter__(self):
        """ Get a session cookie to use for future requests. """
        
        if options.verbose:
            print("Establishing session...")
        
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'text/plain'})
        
        # First make sure the server is online
        r = self.session.get(self.ezid_base + "/status")
        if r.status_code != 200 or not "success: EZID is up" in r.text:
            raise ServerError("EZID server appears offline: (%s, '%s')" % (r.status_code, r.text))
        
        # Then get the session key
        r = self.session.get(self.ezid_base + "/login",
                             auth=(self.ezid_username, self.ezid_password))
             
        if "error" in r.text or r.status_code != 200:
            raise AuthenticationError("Server responded with %s" % r.text.rstrip())

        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        """ End the current session."""
        
        if options.verbose:
            print("Closing session...")
        
        # End the EZID session
        r = self.session.get(self.ezid_base + "/logout")
        r.raise_for_status()
        # End the HTTP session
        self.session.close()

    def get_id(self, doi):
        """ Returns the information about a DOI."""
        
        if options.verbose:
            print("Fetching DOI: %s" % doi)
        
        r = self.session.get("%s/id/%s" % (self.ezid_base, doi))
        r.raise_for_status()
        
        return anvl.unescape_dictionary(r.text)
            
    def create_doi(self, entry):
        """ Creates a new DOI for an entry without one. """

        if options.verbose:
            print("Creating DOI for entry: %s" % entry)
        
        entry_meta = get_entry_metadata(entry)
        doi = "doi:10.5072/FK2bmr%s" % entry
        url = "%s/id/%s" % (self.ezid_base, doi)
        
        r = self.session.put(url, data=anvl.escape_dictionary(entry_meta))
        print(r.status_code, r.text, "\n", r.request.body)
        
        # Return status
        if r.status_code < 300:
            return True
        else:
            return False
        
    def update_doi(self, entry):
        """ Update the metadata for an entry. """
        
        if options.verbose:
            print("Updating DOI for entry: %s" % entry)
        
        entry_meta = get_entry_metadata(entry)
        doi = "doi:10.5072/FK2bmr%s"
        url = "%s/id/%s" % (self.ezid_base, doi)
        
        r = self.session.post(url, data=anvl.escape_dictionary(entry_meta))
        print(r.status_code, r.text)
        r.raise_for_status()
    
    def create_or_update_doi(self, entry):
        """ Try to create a new DOI. If that fails update the
        existing one."""
        
        if not self.create_doi(entry):
            self.update_doi(entry)

def get_entry_metadata(entry):
    """ Returns a python dict with all of the known information about
    an entry."""
    
    ent = pynmrstar.Entry.from_database(entry)
    
    meta = {'_profile': 'datacite'}
    
    # Link should be to entry directory
    if entry.startswith("bmse") or entry.startswith("bmst"):
        meta['_target'] = 'http://www.bmrb.wisc.edu/ftp/pub/bmrb/metabolomics/entry_directories/%s/' % entry
    else:
        meta['_target'] = 'http://www.bmrb.wisc.edu/ftp/pub/bmrb/entry_directories/bmr%s/' % entry
    
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
        # This should never evaluate false since we shouldn't have empty authors
        if len(auth) > 0:
            mod_auths.append(", ".join(auth) + ",")
    meta['datacite.creator'] = ";".join(mod_auths)
    
    # Get first release date
    release_loop = ent.get_loops_by_category('release')[0]
    release_loop.sort_rows('release_number')
    meta['datacite.publicationyear'] = release_loop.get_tag('date')[0][:4]
    #meta['datacite.Date'] = release_loop.get_tag('date')[0]
    #meta['datacite.dateType'] = 'Available'
    
    return meta

if __name__ == "__main__":
    
    # Start a session
    with EZIDSession() as session:
        session.create_doi('15000')


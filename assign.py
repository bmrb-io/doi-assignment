#!/usr/bin/env python3

""" Script to assign the DOIs. """

import json
import logging
import optparse
import os
import sys
import time
import xml.etree.cElementTree as eTree
from xml.etree.ElementTree import tostring as xml_tostring

import psycopg2
import pynmrstar
import requests

import anvl


class EZIDSession:
    """ A session with the EZID server."""

    config_file = os.path.join(os.path.join(os.path.dirname(os.path.realpath(__file__))), 'configuration.json')
    config = json.load(open(config_file, 'r'))
    ezid_base = config['ezid_base']
    ezid_username = config['ezid_username']
    ezid_password = config['ezid_password']
    shoulder = config['shoulder']

    def __init__(self):
        pass

    def __enter__(self):
        """ Get a session cookie to use for future requests. """

        logging.info("Establishing session...")

        self.session = requests.Session()
        self.session.auth = (self.ezid_username, self.ezid_password)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ End the current session."""

        logging.info("Closing session...")

        # End the HTTP session
        self.session.close()

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

        r = self.session.get("%s/metadata/%s" % (self.ezid_base, doi))
        r.raise_for_status()

        return r.text

    def withdraw(self, entry):
        """ Withdrawn an entry. """

        doi = self.determine_doi(entry)
        url = "https://ez.datacite.org/id/%s" % doi
        r = requests.post(url,
                          data=anvl.escape_dictionary({'_status': 'unavailable | withdrawn by author'}),
                          auth=(self.ezid_username, self.ezid_password),
                          headers={'Accept': 'text/plain', 'Content-Type': 'text/plain'})

        if r.status_code < 300:
            logging.info("Withdrew entry: %s" % entry)
        r.raise_for_status()

    def create_or_update_doi(self, entry, timeout=1):
        """ Assign the metadata, then update the URL/release the DOI if not yet created. """

        timeout = timeout * 2
        try:
            entry_meta = self.get_entry_metadata(entry)
            doi = self.determine_doi(entry)

            # First create the metadata
            url = "%s/metadata/%s" % (self.ezid_base, doi)
            r = self.session.put(url, data=entry_meta, headers={'Content-Type': 'application/xml'})

            r.raise_for_status()

            url = "%s/doi/%s" % (self.ezid_base, doi)
            if entry.startswith("bmse"):
                content_url = 'https://bmrb.io/metabolomics/mol_summary/show_data.php?id=%s' % entry
            elif entry.startswith("bmst"):
                content_url = 'https://bmrb.io/metabolomics/mol_summary/show_theory.php?id=%s' % entry
            else:
                content_url = 'https://bmrb.io/data_library/summary/?bmrbId=%s' % entry
            release_string = "doi=%s\nurl=%s" % (doi, content_url)

            r = self.session.put(url, data=release_string, headers={'Content-Type': 'text/plain'})
            r.raise_for_status()

            logging.info("Created or updated entry: %s" % entry)
        except requests.HTTPError as e:
            logging.warning("A HTTP exception occurred for entry %s: %s" % (entry, e))
            logging.info("Trying again...")
            if timeout <= 128:
                time.sleep(timeout)
                return self.create_or_update_doi(entry, timeout)
            else:
                logging.error('Multiple attempts to assign entry %s failed.' % entry)
        except IOError as err:
            logging.exception("Entry %s not loaded in the DB: %s" % (entry, err))
        finally:
            time.sleep(1)

    def get_entry_metadata(self, entry):
        """ Returns a python dict with all of the known information about
        an entry."""

        try:
            ent = pynmrstar.Entry.from_database(entry)
        except ValueError as err:
            raise ValueError("Something went wrong when getting an entry (%s) from the database: %s" % (entry, err))
        # Get the data we will need
        release_loop = ent.get_loops_by_category('release')[0]
        release_loop.sort_rows('release_number')
        release_loop.add_missing_tags()
        release_loop = release_loop.get_tag(['date', 'detail'])

        root = eTree.Element("resource")
        root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.set("xmlns", 'http://datacite.org/schema/kernel-4')
        root.set("xsi:schemaLocation",
                 "http://datacite.org/schema/kernel-4 http://schema.datacite.org/meta/kernel-4/metadata.xsd")

        identifier = eTree.SubElement(root, "identifier")
        identifier.set("identifierType", 'DOI')
        identifier.text = self.determine_doi(entry)

        # Get the authors in the form Last Name, Middle Initial, First Name,;
        creators = eTree.SubElement(root, "creators")

        # Get the authors - with middle initial if possible, but if not, just use first and last
        try:
            authors = ent.get_loops_by_category('entry_author')[0].filter(
                ['_Entry_author.Family_name', '_Entry_author.Given_name', '_Entry_author.Middle_initials']).data
            for auth in authors:
                if auth[2] and auth[2] != ".":
                    auth[1] += " " + auth[2]
        except (ValueError, KeyError):
            authors = ent.get_loops_by_category('entry_author')[0].filter(
                ['_Entry_author.Family_name', '_Entry_author.Given_name']).data
        for auth in authors:
            creator = eTree.SubElement(creators, "creator")
            creator_name = eTree.SubElement(creator, 'creatorName')
            creator_name.text = ", ".join([auth[0], auth[1]])
            creator_name.set('nameType', 'Personal')

            # Datacite doesn't like it when we provide these...
            #if auth[0]:
             #   eTree.SubElement(creator, 'familyName').text = auth[0]
            #if auth[1] and auth[2]:
             #   eTree.SubElement(creator, 'givenName').text = auth[1] + " " + auth[2]
            #elif auth[1]:
             #   eTree.SubElement(creator, 'givenName').text = auth[1]

        titles = eTree.SubElement(root, "titles")
        eTree.SubElement(titles, "title").text = ent.get_tag("entry.title")[0].replace("\n", "")
        eTree.SubElement(root, "publisher").text = 'Biological Magnetic Resonance Bank'
        eTree.SubElement(root, "publicationYear").text = release_loop[0][0][:4]
        resource_type = eTree.SubElement(root, 'resourceType')

        dates = eTree.SubElement(root, 'dates')
        release_date = eTree.SubElement(dates, 'date')
        release_date.set('dateType', 'Available')
        release_date.set('dateInformation', release_loop[0][1])
        release_date.text = release_loop[0][0]

        for release in release_loop[1:]:
            update_date = eTree.SubElement(dates, 'date')
            update_date.set('dateType', 'Updated')
            update_date.text = release[0]
            update_date.set('dateInformation', release[1])

        resource_type.set('resourceTypeGeneral', 'Dataset')

        return xml_tostring(root, encoding="UTF-8")


if __name__ == "__main__":

    # Specify some basic information about our command
    usage = "usage: %prog"
    parser = optparse.OptionParser(usage=usage, version="%prog .1",
                                   description="Assign DOIs to new entries and make sure existing entries DOI "
                                               "information is up to date.")

    # Specify the common arguments
    parser.add_option("--full-run", action="store_true", dest="full", default=False,
                      help="Try to add or update all entries in the DB and not just new ones.")
    parser.add_option("--dry-run", action="store_true", dest="dry_run", default=False,
                      help="Do a dry run. Print what would be done but don't do it.")
    parser.add_option("--verbose", action="store_true", dest="verbose", default=False, help="Be verbose.")
    parser.add_option("--days", action="store", dest="days", default=0, type="int",
                      help="How many days back should we assign DOIs?")
    parser.add_option("--manual", action="store", dest="override", type="str", help="One entry ID to manually test.")
    parser.add_option("--database", action="store", type="choice", choices=['macromolecules', 'metabolomics', 'both'],
                      default='both', dest="database", help="Select which DB to update, or 'both' to do both.")

    # Options, parse 'em
    (options, args) = parser.parse_args()

    logging.basicConfig()
    logger = logging.getLogger()
    if options.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.ERROR)

    # Fetch entries
    cur = psycopg2.connect(user='ets', host='ets.bmrb.io', database='ETS').cursor()

    entries = []
    if options.database == "metabolomics":
        entries = requests.get("https://api.bmrb.io/v2/list_entries?database=metabolomics").json()
    elif options.database == "macromolecules":
        entries = requests.get("https://api.bmrb.io/v2/list_entries?database=macromolecules").json()
    elif options.database == "both":
        entries = requests.get("https://api.bmrb.io/v2/list_entries?database=macromolecules").json()
        entries.extend(requests.get("https://api.bmrb.io/v2/list_entries?database=metabolomics").json())

    if options.days != 0:
        cur.execute("""
SELECT bmrbnum
FROM entrylog
WHERE status LIKE 'rel%%'
  AND accession_date > current_date - INTERVAL '%d days';""" % options.days)
        entries = [str(x[0]) for x in cur.fetchall()]

    if options.override:
        entries = [options.override]

    if options.dry_run:
        for en in entries:
            logger.setLevel(logging.INFO)
            logger.info("Create or update: %s" % EZIDSession().determine_doi(en))
        sys.exit(0)

    # Start a session
    with EZIDSession() as session:
        # Assign or update
        for an_entry in entries:
            session.create_or_update_doi(an_entry)

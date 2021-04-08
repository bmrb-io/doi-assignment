#!/usr/bin/env python3

""" Script to assign the DOIs. """
import base64
import json
import logging
import multiprocessing
import optparse
import os
import sys
import time
import xml.etree.cElementTree as eTree
from xml.etree.ElementTree import tostring as xml_tostring

import psycopg2
import pynmrstar
import requests

# from datacite import DataCiteMDSClient

config_file = os.path.join(os.path.join(os.path.dirname(os.path.realpath(__file__))), 'configuration.json')
config = json.load(open(config_file, 'r'))
base_url = config['base_url']
username = config['username']
password = config['password']
shoulder = config['shoulder']


def get_id(doi):
    """ Returns the information about a DOI."""
    r = requests.get("%s/dois/%s" % (base_url, doi))
    r.raise_for_status()
    return r.json()


def determine_doi(entry):
    """ Determines the DOI for an entry."""

    if entry.startswith("bmse") or entry.startswith("bmst"):
        return '%s%s' % (shoulder, entry.upper())
    elif entry.startswith("bmr"):
        return '%s%s' % (shoulder, entry.upper())
    else:
        return '%sBMR%s' % (shoulder, entry)


def determine_entry_url(entry, data_type='string') -> str:
    """ Determines the location for an entry."""

    if data_type == 'string':
        if entry.startswith("bmse"):
            return f'https://bmrb.io/metabolomics/mol_summary/show_data.php?id={entry}'
        elif entry.startswith("bmst"):
            return f'https://bmrb.io/metabolomics/mol_summary/show_theory.php?id={entry}'
        else:
            return f'https://bmrb.io/data_library/summary/?bmrbId={entry}'
    elif data_type == 'star':
        if entry.startswith("bmse") or entry.startswith("bmst"):
            return f'https://bmrb.io/ftp/pub/bmrb/metabolomics/entry_directories/{entry}/{entry}.str'
        else:
            return f'https://bmrb.io/ftp/pub/bmrb/entry_directories/bmr{entry}/bmr{entry}_3.str'


def withdraw(entry):
    """ Withdrawn an entry. """

    doi = determine_doi(entry)
    url = f"{base_url}/dois/{doi}"
    full_data = {
        "data": {
            "id": doi,
            "type": "dois",
            "attributes": {
                "event": "hide",
                "doi": doi
            }
        }
    }
    r = requests.put(url, json=full_data, auth=(username, password),
                     headers={'Content-Type': 'application/vnd.api+json'})
    if r.status_code < 300:
        logging.info("Withdrew entry: %s" % entry)
    r.raise_for_status()


def get_entry_metadata(entry) -> str:
    """ Returns a base-64 encoded XML string with all of the known information about
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
    identifier.text = determine_doi(entry)

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
        # if auth[0]:
        #   eTree.SubElement(creator, 'familyName').text = auth[0]
        # if auth[1] and auth[2]:
        #   eTree.SubElement(creator, 'givenName').text = auth[1] + " " + auth[2]
        # elif auth[1]:
        #   eTree.SubElement(creator, 'givenName').text = auth[1]

    titles = eTree.SubElement(root, "titles")
    eTree.SubElement(titles, "title").text = ent.get_tag("entry.title")[0].replace("\n", "")
    eTree.SubElement(root, "publisher").text = 'Biological Magnetic Resonance Bank'
    eTree.SubElement(root, "publicationYear").text = release_loop[0][0][:4]
    resource_type = eTree.SubElement(root, 'resourceType')

    dates = eTree.SubElement(root, 'dates')
    release_date = eTree.SubElement(dates, 'date')
    release_date.set('dateType', 'Available')
    if release_loop[0][1] and release_loop[0][1] not in pynmrstar.definitions.NULL_VALUES:
        release_date.set('dateInformation', release_loop[0][1])
    release_date.text = release_loop[0][0]

    for release in release_loop[1:]:
        update_date = eTree.SubElement(dates, 'date')
        update_date.set('dateType', 'Updated')
        update_date.text = release[0]
        update_date.set('dateInformation', release[1])

    resource_type.set('resourceTypeGeneral', 'Dataset')

    xml_string = xml_tostring(root, encoding="UTF-8")
    b64_bytes = base64.b64encode(xml_string)
    return b64_bytes.decode('UTF-8')


def create_or_update_doi(entry, timeout=1):
    """ Assign the metadata, then update the URL/release the DOI if not yet created. """

    timeout = timeout * 2
    try:
        doi = determine_doi(entry)
        full_data = {
            "data": {
                "id": doi,
                "type": "dois",
                "attributes": {
                    "event": "publish",
                    "doi": doi,
                    "url": determine_entry_url(entry),
                    "xml": get_entry_metadata(entry)
                }
            }
        }
        url = f"{base_url}/dois/{doi}"
        r = requests.put(url, json=full_data, auth=(username, password),
                         headers={'Content-Type': 'application/vnd.api+json'})
        r.raise_for_status()

        logging.info("Created or updated entry: %s" % entry)
    except requests.HTTPError as e:
        logging.warning("A HTTP exception occurred for entry %s: %s" % (entry, e))
        logging.info("Trying again...")
        if timeout <= 128:
            time.sleep(timeout)
            return create_or_update_doi(entry, timeout)
        else:
            logging.error('Multiple attempts to assign entry %s failed.' % entry)
    except IOError as err:
        logging.exception("Entry %s not loaded in the DB: %s" % (entry, err))
    finally:
        time.sleep(1)


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
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="Be verbose.")
    parser.add_option("--days", action="store", dest="days", default=0, type="int",
                      help="How many days back should we assign DOIs?")
    parser.add_option("-m", "--manual", action="store", dest="override", type="str",
                      help="One entry ID to manually test.")
    parser.add_option("--database", action="store", type="choice", choices=['macromolecules', 'metabolomics', 'both'],
                      default='both', dest="database", help="Select which DB to update, or 'both' to do both.")

    # Options, parse 'em
    (options, args) = parser.parse_args()

    # Set up logging. This allows us to use the standard logging module but have
    #  warnings and above go to STDERR.
    class InfoFilter(logging.Filter):
        def filter(self, rec):
            return rec.levelno in [logging.DEBUG, logging.INFO]


    if options.verbose:
        logging.basicConfig(format='%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                            level=logging.INFO)

    logger = logging.getLogger("__name__")

    # Set up the stderr handler
    h2 = logging.StreamHandler()
    h2.setLevel(logging.WARNING)
    logger.addHandler(h2)

    # Set up the stdout handler
    h1 = logging.StreamHandler(sys.stdout)
    h1.addFilter(InfoFilter())
    logger.addHandler(h1)

    entries = []
    if options.database == "metabolomics":
        entries = requests.get("https://api.bmrb.io/v2/list_entries?database=metabolomics").json()
    elif options.database == "macromolecules":
        entries = requests.get("https://api.bmrb.io/v2/list_entries?database=macromolecules").json()
    elif options.database == "both":
        entries = requests.get("https://api.bmrb.io/v2/list_entries?database=macromolecules").json()
        entries.extend(requests.get("https://api.bmrb.io/v2/list_entries?database=metabolomics").json())

    if options.days != 0:
        with psycopg2.connect(user='ets', host='ets.bmrb.io', database='ETS') as conn:
            cur = conn.cursor()
            cur.execute("""
SELECT bmrbnum
FROM entrylog
WHERE status LIKE 'rel%%'
  AND accession_date > current_date - INTERVAL '%d days';""" % options.days)
            entries = [str(x[0]) for x in cur.fetchall()]

            cur.execute("""
SELECT *
FROM entrylog
WHERE status LIKE 'awd%';""")
            withdrawn = cur.fetchall()

    if options.override:
        entries = [options.override]

    if options.dry_run:
        for en in entries:
            logger.setLevel(logging.INFO)
            logger.info("Create or update: %s" % determine_doi(en))
        sys.exit(0)

    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        p.map(create_or_update_doi, entries)

    #withdraw(entries[0])

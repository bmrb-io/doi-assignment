#!/usr/bin/env python3

""" Script to assign the DOIs. """
import base64
import hashlib
import json
import logging
import optparse
import os
import sqlite3
import sys
import threading
import time
import xml.etree.cElementTree as eTree
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple
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

# DataCite asks integrators to identify themselves via User-Agent w/ mailto.
USER_AGENT = 'bmrb-doi-assignment/1.0 (mailto:wedell@uchc.edu)'

# Shared session so we get connection pooling + the User-Agent on every call.
session = requests.Session()
session.headers.update({'User-Agent': USER_AGENT})


class RateLimiter:
    """Ensures at most ``rate_per_second`` ``acquire()`` calls cross per second,
    across all threads. Simple monotonic-clock spacing — no burst allowance."""

    def __init__(self, rate_per_second: float):
        self._min_interval = 1.0 / rate_per_second
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._next_allowed = now + self._min_interval


_cache_lock = threading.Lock()
_cache_conn: Optional[sqlite3.Connection] = None


def _open_payload_cache() -> sqlite3.Connection:
    """Open (and lazily initialize) the local sqlite cache that records the
    hash of the last DataCite payload we successfully PUT for each DOI."""

    global _cache_conn
    if _cache_conn is not None:
        return _cache_conn

    cache_path = config.get('payload_cache_path') or os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'doi_payload_cache.sqlite3')
    # check_same_thread=False is safe here because every read/write is guarded
    # by _cache_lock — sqlite itself is fine with cross-thread use under a lock.
    _cache_conn = sqlite3.connect(cache_path, check_same_thread=False)
    _cache_conn.execute("""
        CREATE TABLE IF NOT EXISTS doi_payload_cache (
            doi TEXT PRIMARY KEY,
            payload_hash TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _cache_conn.commit()
    return _cache_conn


def payload_hash(full_data: dict) -> str:
    """Stable hash of the parts of the DataCite payload we actually send.
    Covers both the XML metadata and the URL so a URL-only change still PUTs."""

    canonical = json.dumps(full_data['data']['attributes'], sort_keys=True)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def payload_cache_lookup(doi: str) -> Optional[str]:
    conn = _open_payload_cache()
    with _cache_lock:
        cur = conn.execute("SELECT payload_hash FROM doi_payload_cache WHERE doi = ?", (doi,))
        row = cur.fetchone()
    return row[0] if row else None


def payload_cache_store(doi: str, digest: str) -> None:
    conn = _open_payload_cache()
    with _cache_lock:
        conn.execute(
            "INSERT INTO doi_payload_cache (doi, payload_hash, updated_at) "
            "VALUES (?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(doi) DO UPDATE SET "
            "payload_hash=excluded.payload_hash, updated_at=CURRENT_TIMESTAMP",
            (doi, digest))
        conn.commit()


def get_id(doi):
    """ Returns the information about a DOI."""
    r = session.get(f"{base_url}/dois/{doi}")
    r.raise_for_status()
    return r.json()


def determine_doi(entry):
    """ Determines the DOI for an entry."""

    if entry.startswith("bmse") or entry.startswith("bmst"):
        return f'{shoulder}{entry.upper()}'
    elif entry.startswith("bmr"):
        return f'{shoulder}{entry.upper()}'
    elif entry.startswith("bmrbig"):
        return f'{shoulder}{entry.upper()}'
    else:
        return '%sBMR%s' % (shoulder, entry)


def determine_entry_url(entry, data_type='string') -> str:
    """ Determines the location for an entry."""

    if data_type == 'string':
        if entry.startswith("bmse"):
            return f'https://bmrb.io/metabolomics/mol_summary/show_data.php?id={entry}'
        elif entry.startswith("bmrbig"):
            return f'https://bmrbig.bmrb.io/released/{entry}'
        elif entry.startswith("bmst"):
            return f'https://bmrb.io/metabolomics/mol_summary/show_theory.php?id={entry}'
        else:
            return f'https://bmrb.io/data_library/summary/?bmrbId={entry}'
    elif data_type == 'star':
        if entry.startswith("bmse") or entry.startswith("bmst"):
            return f'https://bmrb.io/ftp/pub/bmrb/metabolomics/entry_directories/{entry}/{entry}.str'
        elif entry.startswith("bmrbig"):
            return f'https://bmrbig.bmrb.io/deposition/released/{entry}/{entry}.str'
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
    r = session.put(url, json=full_data, auth=(username, password),
                    headers={'Content-Type': 'application/vnd.api+json'})
    if r.status_code < 300:
        logging.info("Withdrew entry: %s" % entry)
    r.raise_for_status()


def get_entry_metadata(entry) -> str:
    """ Returns a base-64 encoded XML string with all of the known information about
    an entry."""

    try:
        if entry.startswith('bmrbig'):
            ent = pynmrstar.Entry.from_file(determine_entry_url(entry, data_type='star'))
        else:
            ent = pynmrstar.Entry.from_database(entry)
    except ValueError as err:
        raise ValueError("Something went wrong when getting an entry (%s) from the database: %s" % (entry, err))
    # Get the data we will need
    try:
        release_loop = ent.get_loops_by_category('release')[0]
        release_loop.sort_rows('release_number')
        release_loop.add_missing_tags()
        release_loop = release_loop.get_tag(['date', 'detail'])
    except IndexError:
        release_loop = [[ent.get_tag('_Entry.Original_release_date')[0], 'Original release date']]

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
        authors = ent.get_loops_by_category('entry_author')[0].filter(['Family_name', 'Given_name']).data
    except IndexError:
        authors = ent.get_loops_by_category('contact_person')[0].filter(['Family_name', 'Given_name']).data
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


def build_doi_payload(entry):
    """Build the DataCite PUT payload for an entry. Fetches the entry's
    NMR-STAR record from BMRB — meant to be called from a single thread so
    pynmrstar's own 403 backoff isn't fighting concurrent fetches."""

    doi = determine_doi(entry)
    return doi, {
        "data": {
            "id": doi,
            "type": "dois",
            "attributes": {
                "event": "publish",
                "doi": doi,
                "url": determine_entry_url(entry),
                "xml": get_entry_metadata(entry),
            },
        },
    }


def put_doi(entry, doi, full_data, digest: str, rate_limiter: 'RateLimiter', timeout=1):
    """PUT a pre-built payload to DataCite. Pace is controlled by ``rate_limiter``;
    HTTP errors retry on the same worker with an exponential backoff. On success,
    records ``digest`` in the local cache so a future run can skip an unchanged
    payload."""

    timeout = timeout * 2
    try:
        url = f"{base_url}/dois/{doi}"
        rate_limiter.acquire()
        r = session.put(url, json=full_data, auth=(username, password),
                        headers={'Content-Type': 'application/vnd.api+json'})
        r.raise_for_status()
        payload_cache_store(doi, digest)
        logging.info("Created or updated entry: %s" % entry)
    except requests.HTTPError as e:
        logging.warning("A HTTP exception occurred for entry %s: %s" % (entry, e))
        logging.info("Trying again...")
        if timeout <= 128:
            time.sleep(timeout)
            return put_doi(entry, doi, full_data, digest, rate_limiter, timeout)
        else:
            logging.error('Multiple attempts to assign entry %s failed.' % entry)

def get_bmrbig_entries(days_back: int = 0) -> List[str]:
    import sqlite3
    conn = sqlite3.connect(config['bmrbig_database_path'])
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if days_back > 0:
        cur.execute('SELECT * from entrylog WHERE date(release_date) <= CURRENT_DATE AND date(release_date) > date("now", ?)', (f'-{days_back} days',))
    else:
        cur.execute('SELECT * from entrylog WHERE date(release_date) <= CURRENT_DATE')
    entries = [f'bmrbig{_["bmrbig_id"]}' for _ in cur.fetchall()]
    cur.close()
    conn.close()
    return entries


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
    parser.add_option("--database", action="store", type="choice", choices=['macromolecules', 'metabolomics',
                                                                            'bmrbig', 'both', 'all'],
                      default='all', dest="database", help="Select which DB to update, or 'both' to do both main"
                                                            " databases. 'all' for all three, including BMRbig.")

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
        entries = session.get("https://api.bmrb.io/v2/list_entries?database=metabolomics").json()
    elif options.database == "macromolecules":
        entries = session.get("https://api.bmrb.io/v2/list_entries?database=macromolecules").json()
    elif options.database == 'bmrbig':
        entries = get_bmrbig_entries()
    elif options.database == "both":
        entries = session.get("https://api.bmrb.io/v2/list_entries?database=macromolecules").json()
        entries.extend(session.get("https://api.bmrb.io/v2/list_entries?database=metabolomics").json())
    elif options.database == 'all':
        entries = session.get("https://api.bmrb.io/v2/list_entries?database=macromolecules").json()
        entries.extend(session.get("https://api.bmrb.io/v2/list_entries?database=metabolomics").json())
        entries.extend(get_bmrbig_entries())

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

        if options.database == "bmrbig" or options.database == "all":
            entries.extend(get_bmrbig_entries(options.days))

    if options.override:
        entries = [options.override]

    if options.dry_run:
        for en in entries:
            logger.setLevel(logging.INFO)
            logger.info("Create or update: %s" % determine_doi(en))
        sys.exit(0)

    # Main thread fetches NMR-STAR records from BMRB (one at a time, so pynmrstar's
    # own 403-rate-limit backoff actually relieves pressure on the API). PUTs to
    # DataCite go to a single background worker, paced by a rate limiter — keeps
    # writes strictly serial (DataCite returns 429 under concurrent load) while
    # still pipelining fetch and PUT across the two threads.
    put_workers = 1
    datacite_rate_per_second = 7.0
    rate_limiter = RateLimiter(datacite_rate_per_second)

    with ThreadPoolExecutor(max_workers=put_workers) as executor:
        futures = []
        skipped = 0
        for entry in entries:
            try:
                doi, full_data = build_doi_payload(entry)
            except (requests.HTTPError, IOError, ValueError, KeyError) as err:
                logging.warning("Could not fetch entry %s from BMRB: %s" % (entry, err))
                continue
            digest = payload_hash(full_data)
            if payload_cache_lookup(doi) == digest:
                skipped += 1
                logging.debug("Payload unchanged for %s, skipping PUT" % entry)
                continue
            futures.append(executor.submit(put_doi, entry, doi, full_data, digest, rate_limiter))
        for future in as_completed(futures):
            future.result()
        logging.info("Submitted %d PUTs, skipped %d unchanged" % (len(futures), skipped))

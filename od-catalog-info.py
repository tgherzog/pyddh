#!/usr/bin/python -B -u

"""
This script is used to maintain links between the open data catalog and DDH.
Given a list of dataset identifiers (from http://api.worldbank.org/v2/datacatalog)
and partial URLs to their counterparts in DDH,the script uses a combination of 
scraping and APIs to find the nid and uuid for each dataset, and outputs the result
as a CSV. This is process intensive and error prone, so the idea is to cache the
results for use by other scripts

Typically run like this:
  ./od-catalog-info.py > catalog-info.csv
  ./od-catalog-info.py | tee catalog-info.csv

or for resuming after error:
  ./od-catalog-info.py --start=DATASET_ID >> catalog-info.csv
  ./od-catalog-info.py --start=DATASET_ID | tee -a catalog-info.csv

Usage:
  od-catalog-info.py [--start=DATASET_ID]

Options:
  --start=DATASET_ID     Begin at the specified dataset ID. Useful for recovering from API failures

"""

from docopt import docopt
from open_datasets import open_datasets
import requests
import re
import sys
import csv
from pyquery import PyQuery

config = docopt(__doc__, version='version ' + '0.1')

ddh_root = 'https://newdatacatalog.worldbank.org'

writer = csv.writer(sys.stdout, quoting=csv.QUOTE_MINIMAL)
for ds in open_datasets:
  if config.get('--start') and int(config['--start']) > ds['od_id']:
    continue

  if ds.get('ddh_path') is None or ds['ddh_path'] == '':
    continue

  url = ddh_root + '/dataset/' + ds['ddh_path']
  # sys.stderr.write('Fetching: ' + url + '\n')
  response = requests.get(url)

  # scan HTML response for "/node/[nid].json" which is in an href attribute
  # this is really kludgy, but the DDH HTML is not well formed, and PyQuery isn't working
  m = re.search('/node/(\d+).json', response.text)
  if m is None:
    nid = uuid = 'NA'
  else:
    nid = m.group(1)
    url = ddh_root + '/node/' + nid + '.json'
    response = requests.get(url)
    uuid = response.json()['uuid']

  writer.writerow([ds['od_id'], ds['ddh_path'], nid, uuid])

#!/usr/bin/python

"""
This script compares essential metadata across the open data catalog and DDH to identify
inconsistencies. It looks only at datasets defined in the catalog-info.csv file

The first column in the output is a status field defined as follows:
0: record only found in catalog-info.csv
1: record in open data catalog but not catalog-info.csv or DDH (catalog-info.csv is out of date)
2: record in open data catalog and catalog-info.csv but not DDH
10: record in catalog-info.csv and DDH but not open data catalog
12: record in all 3 sources

Usage:
  od-compare.py [--infofile=FILE]

Options:
  --infofile=FILE      name of CSV dataset info file, as written by od-catalog-info.py [default: catalog-info.csv]

"""

from docopt import docopt
import requests
import re
import sys
import csv
import datetime

config = docopt(__doc__, version='version ' + '0.1')

store = {}
with open(config['--infofile'], 'r') as csvref:
  reader = csv.reader(csvref)
  for row in reader:
    store[row[0]] = {'ddh_path': row[1], 'nid': row[2], 'uuid': row[3], 'status': 0, 'odcat': {}}
  
ddh_root = 'https://newdatacatalog.worldbank.org'
api_root = 'http://api.worldbank.org/v2'

url = api_root + '/datacatalog?per_page=1000&format=json'
response = requests.get(url)
dc = response.json()['datacatalog']

# read the entire open data catalog API
for dataset in dc:
  # skip harvested datasets
  if int(dataset['id']) > 1000:
    continue

  meta = {}
  for elem in dataset['metatype']:
    meta[elem['id']] = elem['value']

  # Try to translate the catalog revision date to a Python date object. DCS does not enforce
  # a date format. We try to convert mm-dd-yyyy or dd-mmm-yyyy (the 2nd is more common)
  if meta.get('lastrevisiondate'):
    meta['_last_updated'] = meta['lastrevisiondate'].strip()
    if re.search('\d+\-\d+\-\d{4}', meta['_last_updated']):
      meta['_last_updated'] = datetime.datetime.strptime(meta['_last_updated'], '%m-%d-%Y').date()
    elif re.search('\d+\-\w{3}\-\d{4}', meta['_last_updated']):
      meta['_last_updated'] = datetime.datetime.strptime(meta['_last_updated'], '%d-%b-%Y').date()

  if store.get(dataset['id']) is None:
    store[dataset['id']] = {'status': 1}
  else:
    store[dataset['id']]['status'] = 2

  store[dataset['id']]['odcat'] = meta

# now go get metadata from DDH
for key,row in store.iteritems():
  if row.get('uuid') is None or row['uuid'] == 'NA':
    # no DDH uuid in the info file
    store[key]['ddhcat'] = {'name': 'n/a'}
    continue

  url = ddh_root + '/api/3/action/package_show?id=' + row['uuid']
  sys.stderr.write('Fetching: ' + url + '\n')

  response = requests.get(url)
  ddh = response.json()['result']
  if type(ddh) is dict:
    # DKAN returns a list if successful, and a dict on error
    store[key]['ddhcat'] = {'name': 'API ERROR: ' + ddh.get('error')}
    continue

  # else, this will be a 1-element list
  ddh = ddh[0]
  store[key]['status'] += 10

  # DDH provides dates in standard, consistent formats, so conversion is much easier
  meta = {
    'last_updated': ddh.get('last_update'),
    '_last_updated': datetime.datetime.strptime(ddh['last_update'], '%Y-%m-%d %H:%M:%S').date() if ddh.get('last_update') else 'n/a',
    'name': ddh.get('title'),
  }

  store[key]['ddhcat'] = meta

# everything loaded - iterate and write report
writer = csv.writer(sys.stdout, quoting=csv.QUOTE_MINIMAL)
writer.writerow(['STATUS','DCS_ID', 'DDH_UUID', 'DCS_TITLE', 'DDH_TITLE', 'DCS_LASTUPDATED', 'DDH_LASTUPDATED', 'DATE_MATCH'])

for key, row in store.iteritems():
  od_name = row['odcat'].get('name') or ''
  ddh_name = row['ddhcat'].get('name') or ''

  date_match = type(row['odcat'].get('_last_updated')) is datetime.date and type(row['ddhcat'].get('_last_updated')) is datetime.date and row['odcat']['_last_updated'] == row['ddhcat']['_last_updated']
  report = [row.get('status'), key, row.get('uuid'),
    od_name.encode('utf-8'), ddh_name.encode('utf-8'),
    row['odcat'].get('lastrevisiondate'), row['ddhcat'].get('last_updated'), date_match]

  writer.writerow(report)

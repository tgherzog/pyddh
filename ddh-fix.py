#!/usr/bin/python

"""
Basic script that shows how to update a bunch of DDH records using an array of constant values

Usage:
  ddh-fix.py NID...

Options:
  NID                  node id(s) to fix

"""

from docopt import docopt
import sys
import os
import ddh
import csv

config = docopt(__doc__, version='version ' + '0.1')

target   = 'datacatalog.worldbank.org'

ddh.load(target)
# ddh.dataset.debug = True

for nid in config['NID']:
    try:
        print 'Updating: {}'.format(nid)
        ddh.dataset.update_dataset(nid, {
          'field_wbddh_copyright': '',
          'field_wbddh_type_of_license': '',
          'workflow_status': 'published',
        })
    except ddh.dataset.APIError as err:
        sys.stderr.write('Error updating dataset [{}]: {}\n'.format(err.type, nid))
        sys.stderr.write(err.response + '\n')

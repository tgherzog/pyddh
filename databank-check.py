
"""
Usage:
  This script compares the modification dates in Databank and DDH to see if they match
"""

import wbgapi as wb
import ddh
import sys
import csv

reload(sys)
sys.setdefaultencoding('utf-8')

# TODO: convert date fields to dates and show difference in an extra column

writer = csv.writer(sys.stdout)
writer.writerow(['NID','DDH_TITLE', 'DDH_MODIFIED', 'DBANK_ID', 'DBANK_TITLE', 'DBANK_MODIFIED'])

ddh.load('datacatalog.worldbank.org')
for k,v in ddh.dataset.search(['field_wbddh_modified_date', 'field_ddh_harvest_sys_id'], {'field_ddh_harvest_src': 'Indicators API'}):
    database_id = v['field_ddh_harvest_sys_id']['und'][0]['value']
    mod_date    = v['field_wbddh_modified_date']['und'][0]['value'] if v['field_wbddh_modified_date'] else ''
    mod_date = mod_date.split(' ')[0]
    dbank = wb.source.get(database_id)

    writer.writerow([k, v['title'], mod_date, database_id, dbank['name'], dbank['lastupdated']])

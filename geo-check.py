#!/usr/bin/python -u

"""
Usage:
  geo-check.py
  geo-check.py ID
  geo-check.py --start=ID
  geo-check.py --from FILE

Options:
  ID             check a single node ID. Otherwise, search all geospatial datasets
  --start=ID     resume searching at ID
  --from FILE    take dataset node ID's from file (one per line) or - for stdin
"""

import ddh
import re
import requests
import sys
import csv
from docopt import docopt

reload(sys)
sys.setdefaultencoding('utf-8')

config = docopt(__doc__)

writer = csv.writer(sys.stdout, quoting=csv.QUOTE_MINIMAL)

host = 'datacatalog.worldbank.org'

ddh.load(host)

users = {}

if config['ID']:
    datasets = [config['ID']]
elif config['--from']:
    datasets = []
    if config['--from'] == '-':
        fd = sys.stdin
    else:
        fd = open(config['--from'], 'r')

    for row in fd:
        row = row.strip()
        if re.search(r'^\d+$',row):
            datasets.append(row)

    if config['--from'] != '-':
        fd.close()
else:
    datasets = []
    for k,v in ddh.dataset.search({'field_wbddh_data_type': 'geospatial'}):
        datasets.append(k)

scanning = False
for k in datasets:
    if scanning == False and config['--start'] and config['--start'] == k:
        scanning = True
    elif scanning == False and config['--start'] is None:
        scanning = True
        
    if scanning == False:
        continue
    try:
        print 'Scanning {}'.format(k)

        ds = ddh.dataset.get(k)
        ds_type  = ddh.taxonomy.term('field_wbddh_data_type', ds)
        ds_class = ddh.taxonomy.term('field_wbddh_data_class', ds)
        ds_license = ddh.taxonomy.term('field_license_wbddh', ds)
        ds_except  = ddh.taxonomy.term('field_exception_s_', ds)

        ds_user = ''
        if ds['field_wbddh_dsttl_upi']:
            ds_user  = ds['field_wbddh_dsttl_upi'].get('und',[{}])[0].get('target_id') or ''
            if ds_user:
                if ds_user not in users:
                    user = ddh.dataset.get(ds_user, 'user')
                    if user:
                        users[ds_user] = user['field_wbddh_first_name']['und'][0]['value'] + ' ' + user['field_wbddh_last_name']['und'][0]['value']

                ds_user = users[ds_user]

        issues = []
        if not ds_user:
            issues.append({'msg': 'TTL missing'})

        if not ds_license:
            issues.append({'msg': 'License missing'})

        if ds_class != 'Public' and not ds_except:
            issues.append({'msg': 'Exception missing for non-public dataset'})

        if not ddh.taxonomy.term('field_wbddh_country', ds):
            issues.append({'msg': 'Country missing (required field)'})

        if not ddh.taxonomy.term('field_wbddh_economy_coverage', ds):
            issues.append({'msg': 'Economy Coverage missing (required field)'})

        # if not ddh.taxonomy.term('field_frequency', ds):
        #     issues.append({'msg': 'Periodicity missing (required field)'})

        if not ddh.taxonomy.term('field_topic', ds):
            issues.append({'msg': 'Topic missing (required field)'})

        if ds['field_resources']:
            resource_list = ds['field_resources'].get('und') or []
        else:
            resource_list = []

        resources = {}
        for elem in resource_list:
            resource_id = elem['target_id']
            resource = ddh.dataset.get(resource_id)
            is_visualization   = resource['field_wbddh_resource_type']['und'][0]['tid'] == '984'

            if is_visualization:
                continue

            resources[elem['target_id']] = resource
            rs_class = ddh.taxonomy.term('field_wbddh_data_class', resource)

            if resource.get('field_upload') and resource['field_upload'].get('und'):
                url = resource['field_upload']['und'][0].get('uri', '')
                if re.search(r'^public://', url):
                    url2 = re.sub(r'^public://', 'https://{}/sites/default/files/'.format(host), url)
                else:
                    url2 = url
            elif resource.get('field_link_api') and resource['field_link_api'].get('und'):
                url2 = resource['field_link_api']['und'][0]['url']
            elif resource.get('field_link_remote_file') and resource['field_link_remote_file'].get('und'):
                url2 = resource['field_link_remote_file']['und'][0]['uri']
            else:
                continue

            if rs_class == 'Not Specified':
                issues.append({'id': resource_id, 'msg': 'resource classification is unspecified', 'url': url2, 'class': rs_class})

            if rs_class == 'Public' and ds_class != 'Public':
                issues.append({'id': resource_id, 'msg': 'public resource for private dataset', 'url': url2, 'class': rs_class})

            if re.search('^https://{}/'.format(host), url2):
                is_internal_path = re.search(r'/ddhfiles/internal/', url2) is not None
                if is_internal_path == (rs_class == 'Public'):
                    issues.append({'id': resource_id, 'msg': 'data classification doesn\'t match path', 'url': url2, 'class': rs_class})

                response = requests.head(url2)
                if response.status_code != 200:
                    issues.append({'id': resource_id, 'msg': 'file doesn\'t exist ({})'.format(response.status_code), 'url': url2, 'class': rs_class})
                elif int(response.headers.get('Content-Length', '-1')) == 0:
                    issues.append({'id': resource_id, 'msg': 'zero length file', 'url': url2, 'class': rs_class})

            # check if:
            # HEAD request returns 200 (else it's a broken link)
            # No public resources for official use datasets
            # path matches data classification 'ddhfiles/public' or 'ddhfiles/internal'
            # response.headers['Content-Length'] is non-zero
        
        if len(resources) == 0 and 'CONFIDENTIAL' not in ds_class.upper():
            issues.append({'msg': 'empty resource list'})

        issue_count = len(issues)
        for issue in issues:
            row = [k, issue_count, ds['title'], ds_type, ds_class, ds_user, issue['msg']]
            if 'id' in issue:
                row.extend([issue['id'], issue['class'], issue['url']])

            writer.writerow(row)

    except:
        raise

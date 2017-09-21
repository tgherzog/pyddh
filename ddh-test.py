#!/usr/local/bin/python

"""
Conduct DDH API tests

Usage:
    ddh-test.py dataset [--source=FILE] [--resource=ID]
    ddh-test.py update ID RESOURCE
    ddh-test.py resource [--dataset=ID] [--title=TITLE]
    ddh-test.py attach ID FILE
    ddh-test.py get ID
    ddh-test.py token
"""

import requests
import json
import sys
from docopt import docopt

config = docopt(__doc__)

session_key = 'SSESSf70b342115438dbd206e1df9422509d2'
session_val = 'bZun_9ai3ag0KP-eIV24cr9LfzNnTqkSSzxupkPA2KE'

host = 'datacatalogbetastg.worldbank.org'
uid  = '19764' # becomes both the node's uid and the TTL

def get_token():
    global host, session_key, session_val
    url = 'https://{}/{}'.format(host, 'services/session/token')
    response = requests.post(url, cookies={session_key: session_val})
    return response.text

data_source = None
if config.get('--source'):
    with open(config['--source'], 'r') as src:
        data_source = json.load(src)
        
if config['token']:
    print get_token()

elif config['get']:
    endpoint = 'api/dataset/node/{}'.format(config['ID'])
    url = 'https://{}/{}'.format(host, endpoint)
    response = requests.get(url, cookies={session_key: session_val})
    try:
        data = response.json()
        t_response = json.dumps(data, indent=4)
    except:
        t_response = response.text

    print t_response

elif config['dataset']:
    endpoint = 'api/dataset/node'
    token = get_token()
    url = 'https://{}/{}'.format(host, endpoint)
    dataset = {
      'type': 'dataset',
      'title': 'API test dataset with upload',
      # 'uid': uid,
      'body': {'und':[{ 'value': 'Description of an API test dataset', }]},
      # 'field_wbddh_data_type': {'und': ['853']},              # other
      # 'field_wbddh_data_type': {'und': ['294']},              # microdata
      'field_wbddh_data_type': {'und': {'tid': '294'}},              # microdata
      # 'field_wbddh_country': {'und':['874']},            # not specified
      'field_wbddh_country': {'und':['254']},            # not specified
      # 'field_wbddh_dsttl_upi': { 'und': {'autocomplete_hidden_value': uid} },
      # 'field_topic': {'und':['366']},                    # energy & extractives
      # TEST
      'field_topic': {'und': {'tid': '366'}},                    # energy & extractives
      # 'field_wbddh_languages_supported': {'und':['873']},   # other
      # TEST
      'field_wbddh_languages_supported': {'und': {'tid': '873'}},   # other
      # 'field_wbddh_data_class': {'und':['358']},          # open data
      'field_wbddh_data_class': {'und': {'tid': '358'}},          # open data
      # 'field_wbddh_terms_of_use': {'und':['434']},          # open data
      'field_wbddh_terms_of_use': {'und': {'tid': '434'}},          # open data
      # 'field_granularity_list': {'und':['946']},          # unspecified
      # TEST (add)
      'field_granularity_list': {'und': {'tid': '946'}},          # unspecified
      # 'field_frequency': {'und':['18']},          # unspecified
      # TEST (add)
      'field_frequency': {'und': {'tid': '18'}},          # unspecified
      # 'field_wbddh_economy_coverage': {'und':['1318']},          # unspecified - might be different in staging and production
      # TEST (add)
      # 'field_wbddh_economy_coverage': {'und': {'tid': '1318'}},          # unspecified - might be different in staging and production
      # 'og_group_ref': {'und': {'target_id': '102732'}},
      # 'field_wbddh_start_date': {'und': [{'value': '2008-06-10'}]},
      # 'field_wbddh_end_date': {'und': [{'value': '2008-06-30'}]},
      # 'field_wbddh_release_date': {'und': [{'value': '2016-11-16 22:30:52', 'date_type': 'datetime', 'timezone': 'America/New_York', 'timezone_db': 'America/New_York' }]},
      'field_wbddh_modified_date': {'und': [{'value': '2016-11-16 22:30:52' }]},
      # 'field_wbddh_start_date': {'und': [{'value': '2016-03-01 10:00:00' }]},
      # 'field_wbddh_end_date': {'und': [{'value': '2016-03-15 10:30:00' }]},
      # 'field_wbddh_period_start_date': {'und': [{'value': '2016-04-01 11:00:00' }]},
      # 'field_wbddh_period_end_date': {'und': [{'value': '2016-04-15 11:30:00' }]},

      # 'field_ddh_harvest_sys_id': {'und': [{ 'value': '1234abcd'}]},
    }

    if data_source is not None:
        dataset = data_source

    if config['--resource'] is not None:
        dataset['field_resources'] = {'und': [{'target_id': 'API test resource with upload ({})'.format(config['--resource'])}]}

    response = requests.post(url, cookies={session_key: session_val}, headers={'X-CSRF-Token': token, 'Content-Type': 'application/json'}, json=dataset)
    try:
        data = response.json()
        t_response = json.dumps(data, indent=4)
    except:
        t_response = response.text

    print 'SUBMITTED DATASET'
    print json.dumps(json.loads(response.request.body), indent=4)
    # print json.dumps(dataset, indent=4)
    print 'REQUEST HEADERS'
    print response.request.headers
    print 'RESPONSE HEADERS'
    print response.headers
    print 'RESPONSE'
    print t_response

elif config['resource']:
    endpoint = 'api/dataset/node'
    token = get_token()
    url = 'https://{}/{}'.format(host, endpoint)

    dataset = {
        'type': 'resource',
        'title': 'API test resource with upload',
        'uid': uid,
        'field_wbddh_resource_type': { 'und': ['986']},
        'field_wbddh_data_class': { 'und': ['358']},
        # 'field_link_api': {'und': [{'url': 'http://cait.wri.org'}]},
    }

    if config['--dataset'] is not None:
        dataset['field_dataset_ref'] = {'und': {'target_id': config['--dataset']}}

    response = requests.post(url, cookies={session_key: session_val}, headers={'X-CSRF-Token': token, 'Content-Type': 'application/json'}, json=dataset)
    try:
        data = response.json()
        t_response = json.dumps(data, indent=4)
    except:
        t_response = response.text

    print 'SUBMITTED DATASET'
    print json.dumps(dataset, indent=4)
    print 'REQUEST HEADERS'
    print response.request.headers
    print 'RESPONSE HEADERS'
    print response.headers
    print 'RESPONSE'
    print t_response

elif config['update']:
    endpoint = 'api/dataset/node'
    token = get_token()
    url = 'https://{}/{}/{}'.format(host, endpoint, config['ID'])
    dataset = {
        'field_resources': {
            'und': [
                {'target_id': 'afgexistingpowerplants.json (107336)' },
                {'target_id': 'afgfuturepowerplants.json (107337)' },
                {'target_id': 'afghanistanpowerplants2015.zip (107338)' },
                {'target_id': 'futurepowerplants.zip (107339)' },
            ]
        }
    }
    response = requests.put(url, cookies={session_key: session_val}, headers={'X-CSRF-Token': token, 'Content-Type': 'application/json'}, json=dataset)
    try:
        data = response.json()
        t_response = json.dumps(data, indent=4)
    except:
        t_response = response.text

    print 'SUBMITTED DATASET'
    print response.request.body
    # print json.dumps(dataset, indent=4)
    print 'REQUEST HEADERS'
    print response.request.headers
    print 'RESPONSE HEADERS'
    print response.headers
    print 'RESPONSE'
    print t_response

elif config['attach']:
    token = get_token()
    url = 'https://{}/api/dataset/node/{}/attach_file'.format(host, config['ID'])

    post_info = {'files[1]': open(config['FILE'], 'rb'), 'field_name': (None,'field_upload'), 'attach': (None,'1')}
    response = requests.post(url, cookies={session_key: session_val}, headers={'X-CSRF-Token': token}, files=post_info)
    print 'SUBMITTED FILE'
    print response.request.body
    print 'REQUEST HEADERS'
    print response.request.headers
    print 'RESPONSE HEADERS'
    print response.headers
    print 'RESPONSE'
    print response.text


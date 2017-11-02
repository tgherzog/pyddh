#!/usr/local/bin/python

"""
Conduct DDH API tests

Usage:
    ddh-test.py dataset --json=FILE [--resource=ID]
    ddh-test.py update ID RESOURCE
    ddh-test.py resource --json=FILE [--dataset=ID] [--title=TITLE]
    ddh-test.py attach --resource=ID FILE
    ddh-test.py get ID
    ddh-test.py put --json=FILE ID
    ddh-test.py token
"""

import requests
import json
import os
import sys
import yaml
from docopt import docopt

config = docopt(__doc__)

session_key = ''
session_val = ''
session_token = ''

host = 'newdatacatalogstg.worldbank.org'
uid  = '19764' # becomes both the node's uid and the TTL

def login(host, user=None, pswd=None):
    global ddh_host, session_key, session_val, session_token

    if not user or not pswd:
        try:
            path = os.path.join(os.getcwd(), 'config.yaml')
            with open(path, 'r') as fd:
                try:
                    config = yaml.load(fd)
                    user = config[host]['user']
                    pswd = config[host]['password']
                except:
                    raise InitError('Incorrect yaml file format in {}'.format(path))
        except:
            sys.stderr.write('user/password not specified, and config.yaml not found\n')

    body = {'username': user, 'password': pswd}
    url = 'https://{}/api/dataset/user/login'.format(host)

    response = requests.post(url, json=body)
    json = response.json()
    if type(json) is not dict:
        sys.stderr.write('login access denied\n')

    session_key = json['session_name']
    session_val = json['sessid']
    session_token = json['token']

def get_token():
    global host, session_key, session_val, session_token

    return session_token

    url = 'https://{}/{}'.format(host, 'services/session/token')
    response = requests.post(url, cookies={session_key: session_val})
    return response.text

data_source = None
if config.get('--json'):
    with open(config['--json'], 'r') as src:
        data_source = json.load(src)
        
login(host)

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

    dataset = data_source

    if config['--dataset'] is not None:
        dataset['field_dataset_ref'] = {'und': {'target_id': config['--dataset']}}

    if config['--title'] is not None:
        dataset['title'] = config['--title']

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

elif config['put']:
    endpoint = 'api/dataset/node'
    token = get_token()
    url = 'https://{}/{}/{}'.format(host, endpoint, config['ID'])

    print "PUT to {}".format(url)
    response = requests.put(url, cookies={session_key: session_val}, headers={'X-CSRF-Token': token, 'Content-Type': 'application/json'}, json=data_source)
    try:
        data = response.json()
        t_response = json.dumps(data, indent=4)
    except:
        t_response = response.text

    print 'SUBMITTED DATASET'
    print response.request.body
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
    url = 'https://{}/api/dataset/node/{}/attach_file'.format(host, config['--resource'])

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


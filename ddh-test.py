#!/usr/local/bin/python

"""
Conduct DDH API tests and demonstrate use of the the DDH API.

While updating existind datasets/resources is straight-forward, adding a NEW dataset
typically involves multiple API calls. The ddh.dataset.new_dataset function demonstrates
this process in a way that supports multiple resources.

  1) dataset call to create the dataset (POST)
  2) resource call to create the child resource (POST)
  3) attach call to attach a file to the resource in step 2 (PUT)
  4) put call to attach the resource in step 2 to the dataset in step 1 (PUT)

All ID parameters are Drupal node IDs

Modes:
    dataset:  create a new dataset via POST
    resource: create a new resource via POST; optionally attached to a dataset
    attach:   attach a file from disk to a (previously recreated) resource
    get:      fetch a dataset or resource object
    put:      update an existing dataset/resource
    delete:   delete an existing dataset/resource
    login:    login only (unnecessary, since all operations perform this)

Usage:
    ddh-test.py dataset [--host=HOST] --json=FILE [--resource=ID]
    ddh-test.py resource [--host=HOST] --json=FILE [--dataset=ID] [--title=TITLE]
    ddh-test.py attach [--host=HOST] --resource=ID FILE
    ddh-test.py get [--host=HOST] ID
    ddh-test.py put [--host=HOST] --json=FILE ID
    ddh-test.py delete [--host=HOST] ID
    ddh-test.py login [--host=HOST]

Options:
    --host=HOST      Alternate server: defaults to datacatalog.worldbank.org
    --json=FILE      The filename of the json object for creating/updating datasets/resources
    --dataset=ID     Dataset to which to attach the resource (I'm not sure this works correctly)
    --title=TITLE    Override "title" attribute in JSON file
    --resource=ID    Resource to which the file should be attached

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

host = config['--host'] or 'datacatalog.worldbank.org'

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

if config['login']:
    token = get_token()
    print 'Cookie: {}={}'.format(session_key, session_val)
    print 'X-CSRF-Token: {}'.format(token)

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
    response = None
    response_result = None
    return_type = 'Normal'
    try:
        response = requests.put(url, cookies={session_key: session_val}, headers={'X-CSRF-Token': token, 'Content-Type': 'application/json'}, json=data_source)
        data = response.json()
        response_result = json.dumps(data, indent=4)
    except requests.exceptions.ConnectionError as err:
        raise
        response = err.response
        return_type = type(err).__name__
    except Exception as err:
        return_type = type(err).__name__

    # sanity checks
    if response is None:
        response = requests.Response()

    if response.request is None:
        response.request = requests.Request()
        response.request.body = ''

    if response_result is None:
        response_result = response.text

    print 'RESULT'
    print return_type
    print 'SUBMITTED DATA'
    print response.request.body
    print 'RESPONSE STATUS'
    print response.status_code
    print 'REQUEST HEADERS'
    print response.request.headers
    print 'RESPONSE HEADERS'
    print response.headers
    print 'RESPONSE'
    print response_result

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

elif config['delete']:
    token = get_token()
    url = 'https://{}/api/dataset/node/{}/delete'.format(host, config['ID'])
    print "DELETE to {}".format(url)

    response = requests.post(url, cookies={session_key: session_val}, headers={'X-CSRF-Token': token}, json={})
    print 'RESPONSE STATUS'
    print response.status_code
    print 'RESPONSE'
    print response.text

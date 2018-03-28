
import taxonomy
import requests
import copy
import datetime
import time
import json
import os
import yaml

ddh_host = None
ddh_session_key = None
ddh_session_value = None
ddh_token = None
debug = False

class Error(Exception):
    
    pass

class InitError(Error):
    
    def __init(self, msg):
        self.message = msg

class APIError(Error):

    def __init__(self, type, id, response):
        self.type = type
        self.response = response

class ValidationError(Error):
    
    def __init__(self, field):
        self.field = field
        self.message = '{} is undefined'.format(field)

def login(host, user, pswd):
    global ddh_host, ddh_session_key, ddh_session_value, ddh_token

    ddh_host = host

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
            raise InitError('user/password not specified, and config.yaml not found')

    body = {'username': user, 'password': pswd}
    url = 'https://{}/api/dataset/user/login'.format(host)

    response = requests.post(url, json=body)
    json = response.json()
    if type(json) is not dict:
        raise InitError('login access denied')

    ddh_session_key = json['session_name']
    ddh_session_value = json['sessid']

def token():
    global ddh_host, ddh_session_key, ddh_session_value, ddh_token

    url = 'https://{}/services/session/token'.format(ddh_host)
    response = requests.post(url, cookies={ddh_session_key: ddh_session_value})
    ddh_token = response.text

def load(host, user=None, pswd=None):

    login(host, user, pswd)
    token()

def get(url):
    global ddh_host, ddh_session_key, ddh_session_value, ddh_token

    response = requests.get(url, cookies={ddh_session_key: ddh_session_value})
    try:
        return response.json()
    except:
        raise APIError('put', id, response.text)

    
def ds_template():

    template = {
      'title': None,
      'body': None,
      'type': 'dataset',
      'status': '1',
      'workflow_status': 'published',
      'field_wbddh_dsttl_upi': None,
      'resources': [],
    }

    # NB: 'frequency' is actually periodicity
    tax_fields = ['wbddh_data_class', 'frequency', 'topic', 'granularity_list',
      'wbddh_country', 'wbddh_economy_coverage', 'wbddh_languages_supported']

    for elem in tax_fields:
        template['field_'+elem] = taxonomy.get('field_'+elem, None, default=True)

    return template

def rs_template():
    template = {
        'title': None,
        'type': 'resource',
        'status': '1',
        'workflow_status': 'published',
    }

    tax_fields = ['wbddh_resource_type', 'wbddh_data_class']
    for elem in tax_fields:
        template['field_'+elem] = taxonomy.get('field_'+elem, None, default=True)

    return template

def _set_values(d, elem):

    for k,v in elem.iteritems():
        if v is None:
            continue

        if k != 'workflow_status' and taxonomy.is_tax(k):
            if type(v) is list:
                d[k] = {'und': v}
            else:
                d[k] = {'und': [v]}
        elif k in ['body', 'field_wbddh_copyright', 'field_wbddh_type_of_license', 'field_wbddh_source', 'field_wbddh_publisher_name', 'field_wbddh_search_tags', 'field_ddh_external_contact_email', 'field_wbddh_depositor_notes', 'field_ddh_harvest_sys_id', 'field_wbddh_reference_system', 'field_related_links_and_publicat', 'field_external_metadata']:
            if type(v) is str or type(v) is unicode:
                d[k] = {'und': [{'value': v}]}
            else:
                d[k] = {'und': [v]}
        elif k in ['field_link_api']:
            if type(v) is str or type(v) is unicode:
                d[k] = {'und': [{'url': v}]}
            else:
                d[k] = {'und': [v]}
        elif k in ['field_wbddh_dsttl_upi', 'field_wbddh_collaborator_upi']:
            d[k] = {'und': {'autocomplete_hidden_value': v}}
        elif k in ['field_wbddh_release_date', 'field_wbddh_modified_date', 'field_wbddh_start_date', 'field_wbddh_end_date']:
            d[k] = {'und': [{
                'value': v.strftime('%Y-%m-%d %H:%M:%S'),
                'timezone': 'America/New_York',
                'timezone_db': 'America/New_York',
                'date_type': 'datetime',
            }]}
        elif k in ['field_wbddh_start_date', 'field_wbddh_end_date']:
            d[k] = {'und': [{ 'value': {
              'day': 1,
              'month': 1,
              'year': 2001
            }}]}
            d[k] = {'und': [{ 'value': v.strftime('%Y-%m-%d %H:%M:%S') }]}
            d[k] = {'und': [{ 'value': str(int(time.mktime(v.timetuple()))) }]}
            d[k] = {'und': [{ 'value': 20010101 }]}
        elif k in ['field_wbddh_time_periods']:
            d[k] = {'und': [{ 'value': str(int(time.mktime(v.timetuple()))), 'value2': str(int(time.mktime(v.timetuple()))) }]}
            d[k] = {'und': [{ 'value': v.strftime('%Y-%m-%d %H:%M:%S'), 'value2': v.strftime('%Y-%m-%d %H:%M:%S') }]}
        elif k in ['og_group_ref']:
            d[k] = {'und': {'target_id': v}}
        else:
            d[k] = v


def new_object(ds):

    obj = {}
    _set_values(obj, ds)
    return obj

def update_dataset(nid, ds):

    global ddh_host, ddh_session_key, ddh_session_value, ddh_token

    obj = new_object(ds)
    url = 'https://{}/api/dataset/node/{}'.format(ddh_host, nid)
    debug_report('Update dataset - {}'.format(url), obj)
    response = requests.put(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
    try:
        data = response.json()
        return data['nid']
    except:
        raise APIError('put', id, response.text)
    

def new_dataset(ds, id=None):

    global ddh_host, ddh_session_key, ddh_session_value, ddh_token
    
    if id is None:
        id = ds.get('field_ddh_harvest_sys_id', None)

    # this variable determines how the module tries to attach child resources to the dataset
    # 'concurrent'     - resource references are included with the initial dataset POST (most efficient)
    # 'posthoc'        - resource references are attached to dataset in a subsequent PUT, as one object
    # 'posthoc-single' - attached to data in multiple subsequent PUTs (least efficient)
    #
    # Currently the API only works in 'post-single' mode

    rsrc_approach = 'posthoc2'

    # step 1: create resources
    resource_references = []
    for elem in ds['resources']:
        e = copy.deepcopy(elem)
        post_info = None
        if e.get('upload'):
            post_info = {'files[1]': open(e['upload'],'rb'), 'field_name': (None,'field_upload'), 'attach': (None,'1')}
            del e['upload']

        obj = new_object(e)
        obj['field_resource_weight'] = {'und': [{'value': len(resource_references)}]}

        url = 'https://{}/api/dataset/node'.format(ddh_host)
        debug_report('Resource Create - {}'.format(url), obj)
        response = requests.post(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
        try:
            data = response.json()
            resource_references.append({'nid': data['nid'], 'title': obj['title']})
        except:
            raise APIError(e['type'], id, response.text)

        # attach files
        if post_info is not None:
            url = 'https://{}/api/dataset/node/{}/attach_file'.format(ddh_host, data['nid'])
            response = requests.post(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, files=post_info)
            try:
                data = response.json()
                fid  = data[0]['fid']
            except:
                raise APIError('upload', id, response.text)

    # step 2: create dataset with resources attached
    e = copy.deepcopy(ds)
    del e['resources']
    obj = new_object(e)
    url = 'https://{}/api/dataset/node'.format(ddh_host)
    if len(resource_references) > 0 and rsrc_approach == 'concurrent':
        obj['field_resources'] = {'und': []}
        for elem in resource_references:
            obj['field_resources']['und'].append({'target_id': u'{} ({})'.format(elem['title'], elem['nid'])})

    debug_report('Dataset Create - {}'.format(url), obj)
    response = requests.post(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
    try:
        data = response.json()
        dataset_node = data['nid']
    except:
        raise APIError(e['type'], id, response.text)

    # step 3: attach resources
    if len(resource_references) > 0 and rsrc_approach == 'posthoc':
        obj = {
          'workflow_status': 'published',
          'field_resources': {'und': []}
        }
        for elem in resource_references:
            obj['field_resources']['und'].append({'target_id': u'{} ({})'.format(elem['title'], elem['nid'])})

        url = 'https://{}/api/dataset/node/{}'.format(ddh_host, dataset_node)
        debug_report('Resource Attach - {} (multiple)'.format(url), obj)
        response = requests.put(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
        # print json.dumps(obj, indent=4)
        try:
            data = response.json()
            nid =  data['nid']
        except:
            raise APIError('put', id, response.text)

    if len(resource_references) > 0 and rsrc_approach == 'posthoc2':
        obj = {
          'workflow_status': 'published',
          'field_resources': {'und': []}
        }
        for elem in resource_references:
            obj['field_resources']['und'].append({'target_id': u'{} ({})'.format(elem['title'], elem['nid'])})

        url = 'https://{}/api/dataset/node/{}'.format(ddh_host, dataset_node)
        debug_report('Resource Attach - {} (multiple2)'.format(url), obj)
        for i in range(len(resource_references)):
            # Unfortunately, errors or anomalies for these calls usually indicate that the resource was successfully
            # attached but that the server subsequently died without returning a valid HTTP response or JSON object
            # so we just continue on
            try:
                response = None
                response = requests.put(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
                data = response.json()
                nid =  data['nid']

            except requests.exceptions.ConnectionError as err:
                print 'Warning: ConnectionError enountered attaching resources to {} - proceeding ({})'.format(dataset_node, i)

            except:
                print 'Warning: Error enountered attaching resources to {} - proceeding ({})'.format(dataset_node, i)

    elif len(resource_references) > 0 and rsrc_approach == 'posthoc-single':
        url = 'https://{}/api/dataset/node/{}'.format(ddh_host, dataset_node)
        for elem in resource_references:
            obj = {
              'workflow_status': 'published',
              'field_resources': {'und': [{ 'target_id': u'{} ({})'.format(elem['title'], elem['nid'])}] }
            }

            debug_report('Resource Attach - {} (single)'.format(url), obj)
            response = requests.put(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
            try:
                data = response.json()
                nid =  data['nid']
            except:
                raise APIError('put', id, response.text)

    return {'nid': dataset_node, 'resources': resource_references}
 
def debug_report(label, obj=None):
    global debug

    if debug:
        print label
        if obj is not None:
          print json.dumps(obj, indent=4)

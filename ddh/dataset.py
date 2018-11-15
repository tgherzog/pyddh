
import taxonomy
import requests
import copy
import datetime
import time
import json
import os
import yaml
import re
import copy

ddh_host = None
ddh_session_key = None
ddh_session_value = None
ddh_token = None
ddh_protocol = 'https'

debug = False
safe_mode = True

class Error(Exception):
    
    pass

class InitError(Error):
    
    def __init(self, msg):
        self.message = msg

class APIError(Error):

    def __init__(self, type, id, response):
        self.type = type
        self.url  = id
        self.response = response

class TaxonomyError(Error):
    
    def __init__(self, field, value):
        self.field = field
        self.value = value
        self.message = '{} is undefined for {}'.format(value, field)

def login(config, user=None, pswd=None):
    global ddh_host, ddh_session_key, ddh_session_value, ddh_token, ddh_protocol

    ddh_host = host = config['host']
    ddh_protocol = config.get('protocol', ddh_protocol)

    if not user or not pswd:
        user = config['user']
        pswd = config['password']

    body = {'username': user, 'password': pswd}
    url = '{}://{}/api/dataset/user/login'.format(ddh_protocol, host)

    response = requests.post(url, json=body)
    try:
        json = response.json()
    except:
        print 'Login response error'
        print response.text
        raise

    if type(json) is not dict:
        raise InitError('login access denied')

    ddh_session_key = json['session_name']
    ddh_session_value = json['sessid']

def token():
    global ddh_host, ddh_session_key, ddh_session_value, ddh_token, ddh_protocol

    url = '{}://{}/services/session/token'.format(ddh_protocol, ddh_host)
    response = requests.post(url, cookies={ddh_session_key: ddh_session_value})
    ddh_token = response.text

def load(config, user=None, pswd=None):

    login(config, user, pswd)
    token()

def search(fields=[], filter={}, obj_type='dataset'):
    global ddh_host, ddh_protocol

    # if 1st argument is a dict then it's the filter, not fields
    if type(fields) is dict:
        filter = fields
        fields = []

    query = copy.copy(filter) # shallow copy should suffice
    taxonomy.update(query, filter)
    query['type'] = obj_type
    for k,v in query.iteritems():
        if v == None:
            raise TaxonomyError(k, filter[k])

    query = {'filter['+k+']':v for k,v in query.iteritems()}

    _fields = set(fields)
    _fields.update(['title'])

    # NB: nid must be the first element always
    query['fields'] = '[nid,' + ','.join(_fields) + ',]'

    totalRecords = None
    recordsRead  = 0
    query['limit'] = str(250)

    while totalRecords is None or recordsRead < totalRecords:
        query['offset'] = str(recordsRead)
    
        # crude urlencode so as not to escape the brackets
        query_string = '&'.join([k + '=' + v for k,v in query.iteritems()])

        url = '{}://{}/search-service/search_api/datasets?{}'.format(ddh_protocol, ddh_host, query_string)
        debug_report('Search - {}'.format(url))

        response = get(url)
        totalRecords = response['count']
        if type(response['result']) is dict:
            recordsRead += len(response['result'])
            for k,v in response['result'].iteritems():
                yield k,v


def get(url, obj_type='node'):
    global ddh_host, ddh_session_key, ddh_session_value, ddh_token, ddh_protocol

    url = str(url)
    if re.match(r'^\d+$', url):
        url = '{}://{}/api/dataset/{}/{}'.format(ddh_protocol, ddh_host, obj_type, url)

    response = requests.get(url, cookies={ddh_session_key: ddh_session_value})
    try:
        result = response.json()
        if type(result) is not dict:
            return None

        return result

    except:
        raise APIError('get', url, response.text)

    
def ds_template():

    template = {
      'title': None,
      'body': None,
      'type': 'dataset',
      'status': '1',
      'moderation_next_state': 'published',
      'field_wbddh_dsttl_upi': None,
      'field_wbddh_responsible': 'No',
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
        'moderation_next_state': 'published',
    }

    tax_fields = ['wbddh_resource_type', 'wbddh_data_class']
    for elem in tax_fields:
        template['field_'+elem] = taxonomy.get('field_'+elem, None, default=True)

    return template

def _set_values(d, elem):

    for k,v in elem.iteritems():
        if v is None:
            continue

        if k in ['_field_tags']:
            # freetagging fields have a different format than other taxonomy fields
            if type(v) is list:
                tags = v
            else:
                tags = [v]

            d[k] = {'und': { 'value_field': ' '.join(['"" {} ""'.format(i) for i in tags]) }}
                
        elif k == 'field_tags' or taxonomy.is_tax(k):
            if type(v) is not list:
                v = [v]
            d[k] = {'und': map(lambda x: {'tid': x}, v)}
        elif k in ['body', 'field_wbddh_copyright', 'field_wbddh_type_of_license', 'field_wbddh_source', 'field_wbddh_publisher_name', 'field_wbddh_search_tags', 'field_ddh_external_contact_email', 'field_wbddh_depositor_notes', 'field_ddh_harvest_sys_id', 'field_wbddh_reference_system', 'field_related_links_and_publicat', 'field_external_metadata', 'field_wbddh_responsible']:
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
            if type(v) is not list:
                v = [v]

            d[k] = {'und': map(lambda x: {'target_id': x}, v)}
        elif k in ['og_group_ref', 'field_wbddh_dsttl_upi', 'field_wbddh_collaborator_upi']:
            if type(v) is not list:
                v = [v]

            d[k] = {'und': map(lambda x: {'target_id': str(x)}, v)}
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
            # note: this currently supports only 1 value in the field, but this could be easily improved
            d[k] = {'und': [{ 'value': v.strftime('%Y-%m-%d %H:%M:%S'), 'value2': v.strftime('%Y-%m-%d %H:%M:%S') }]}
            d[k]['und'][0]['show_todate'] = 0
        else:
            d[k] = v


def new_object(ds):

    obj = {}
    _set_values(obj, ds)
    return obj

def update_dataset(nid, ds):

    global ddh_host, ddh_session_key, ddh_session_value, ddh_token, ddh_protocol

    obj = new_object(ds)

    # workflow status defaults to published if undefined 
    if not 'moderation_next_state' in obj:
        obj['moderation_next_state'] = 'published'

    url = '{}://{}/api/dataset/node/{}'.format(ddh_protocol, ddh_host, nid)
    debug_report('Update dataset - {}'.format(url), obj)
    response = requests.put(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
    try:
        data = safe_json(response)
        return data['nid']
    except:
        raise APIError('put', id, response.text)
    

def new_dataset(ds, id=None):

    global ddh_host, ddh_protocol, ddh_session_key, ddh_session_value, ddh_token
    
    if id is None:
        id = ds.get('field_ddh_harvest_sys_id', None)

    # this variable determines how the module tries to attach child resources to the dataset
    # 'concurrent'     - resource references are included with the initial dataset POST (most efficient)
    # 'posthoc'        - resource references are attached to dataset in a subsequent PUT, as one object
    # 'posthoc2'       - like posthoc, but PUT request is repeated multiple times until all resources are attached (kludgy but works)
    # 'posthoc-single' - attached to data in multiple subsequent PUTs, one per resource (least efficient)
    #
    # Currently the API only works in 'posthoc2' mode

    # 'dataset_first'  - datasets are created first. resources are appended by including the field_dataset_ref element
    #                    which appends them to the dataset
    rsrc_approach = 'dataset_first'

    # step B-1: create dataset with resources attached
    e = copy.deepcopy(ds)
    del e['resources']
    obj = new_object(e)
    url = '{}://{}/api/dataset/node'.format(ddh_protocol, ddh_host)

    debug_report('Dataset Create - {}'.format(url), obj)
    response = requests.post(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
    try:
        data = safe_json(response)
        dataset_node = data['nid']
    except:
        raise APIError(e['type'], id, response.text)

    # step B-2: create resources
    resource_references = []
    for elem in ds['resources']:
        e = copy.deepcopy(elem)
        post_info = None
        if e.get('upload'):
            post_info = {'files[1]': open(e['upload'],'rb'), 'field_name': (None,'field_upload'), 'attach': (None,'1')}
            del e['upload']

        obj = new_object(e)
        obj['field_dataset_ref'] = {'und': [{'target_id': dataset_node}]}
        obj['field_resource_weight'] = {'und': [{'value': len(resource_references)}]}

        url = '{}://{}/api/dataset/node'.format(ddh_protocol, ddh_host)
        debug_report('Resource Create - {}'.format(url), obj)
        response = requests.post(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
        try:
            data = safe_json(response)
            resource_references.append({'nid': data['nid'], 'title': obj['title']})
        except:
            raise APIError(e['type'], id, response.text)

        # attach files
        if post_info is not None:
            url = '{}://{}/api/dataset/node/{}/attach_file'.format(ddh_protocol, ddh_host, data['nid'])
            response = requests.post(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, files=post_info)
            try:
                data = safe_json(response)
                fid  = data[0]['fid']
            except:
                raise APIError('upload to {}'.format(data['nid']), id, response.text)

    # NB: on this branch the remaining code in this function is all legacy and never gets executed
    # step 3: attach resources
    if len(resource_references) > 0 and rsrc_approach == 'posthoc':
        obj = {
          'moderation_next_state': 'published',
          'field_resources': {'und': []}
        }
        for elem in resource_references:
            # obj['field_resources']['und'].append({'target_id': u'{} ({})'.format(elem['title'], elem['nid'])})
            obj['field_resources']['und'].append({'target_id': u'{}'.format(elem['nid'])})

        url = '{}://{}/api/dataset/node/{}'.format(ddh_protocol, ddh_host, dataset_node)
        debug_report('Resource Attach - {} (multiple)'.format(url), obj)
        response = requests.put(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
        # print json.dumps(obj, indent=4)
        try:
            data = safe_json(response)
            nid =  data['nid']
        except:
            raise APIError('put', id, response.text)

    if len(resource_references) > 0 and rsrc_approach == 'posthoc2':
        obj = {
          'moderation_next_state': 'published',
          'field_resources': {'und': []}
        }
        for elem in resource_references:
            obj['field_resources']['und'].append({'target_id': u'{} ({})'.format(elem['title'], elem['nid'])})

        url = '{}://{}/api/dataset/node/{}'.format(ddh_protocol, ddh_host, dataset_node)
        debug_report('Resource Attach - {} (multiple2)'.format(url), obj)
        for i in range(len(resource_references)):
            # Unfortunately, errors or anomalies for these calls usually indicate that the resource was successfully
            # attached but that the server subsequently died without returning a valid HTTP response or JSON object
            # so we just continue on
            try:
                response = None
                response = requests.put(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
                data = safe_json(response)
                nid =  data['nid']

            except requests.exceptions.ConnectionError as err:
                print 'Warning: ConnectionError encountered attaching resources to {} - proceeding ({})'.format(dataset_node, i)

            except:
                print 'Warning: Error encountered attaching resources to {} - proceeding ({})'.format(dataset_node, i)

    elif len(resource_references) > 0 and rsrc_approach == 'posthoc-single':
        url = '{}://{}/api/dataset/node/{}'.format(ddh_protocol, ddh_host, dataset_node)
        for elem in resource_references:
            obj = {
              'moderation_next_state': 'published',
              'field_resources': {'und': [{ 'target_id': u'{} ({})'.format(elem['title'], elem['nid'])}] }
            }

            debug_report('Resource Attach - {} (single)'.format(url), obj)
            response = requests.put(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
            try:
                data = safe_json(response)
                nid =  data['nid']
            except:
                raise APIError('put', id, response.text)

    return {'nid': dataset_node, 'resources': resource_references}
 
def delete(node_id):
    global ddh_host, ddh_protocol, ddh_session_key, ddh_session_value, ddh_token

    url = '{}://{}/api/dataset/node/{}/delete'.format(ddh_protocol, ddh_host, node_id)

    response = requests.post(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json={})
    try:
        result = safe_json(response)

        # result will now be a string either 'success' or 'Invalid node Id'
        return result == 'success'

    except:
        raise APIError('delete', node_id, response.text)


def safe_json(response):
    global safe_mode

    if safe_mode:
        # in safe mode, we assume that server responses are well formed
        return response.json()
    else:
        # in non-safe mode, we try to remove bogus messages that result from bugs in the server code. These show up
        # as HTML-formatted text added to the end of a JSON response
        return json.loads(re.sub(r'<br />.+', '', response.text, 0, re.S))

def debug_report(label, obj=None):
    global debug

    if debug:
        print label
        if obj is not None:
          print json.dumps(obj, indent=4)

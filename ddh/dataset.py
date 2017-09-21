
import taxonomy
import requests
import copy
import datetime
import time
import json

ddh_host = None
ddh_session_key = None
ddh_session_value = None
ddh_token = None

class Error(Exception):
    
    pass

class APIError(Error):

    def __init__(self, type, id, response):
        self.type = type
        self.response = response

class ValidationError(Error):
    
    def __init__(self, field):
        self.field = field
        self.message = '{} is undefined'.format(field)

def load(host, session_key, session_value):
    global ddh_host, ddh_session_key, ddh_session_value, ddh_token

    ddh_host = host
    ddh_session_key = session_key
    ddh_session_value = session_value

    url = 'https://{}/services/session/token'.format(ddh_host)
    response = requests.post(url, cookies={session_key: session_value})
    ddh_token = response.text

def ds_template():

    template = {
      'title': None,
      'body': None,
      'type': 'dataset',
      'field_wbddh_dsttl_upi': None,
      'resources': [],
    }

    # NB: 'frequency' is actually periodicity
    tax_fields = ['wbddh_data_class', 'wbddh_terms_of_use', 'frequency', 'topic', 'granularity_list', 'wbddh_country', 'wbddh_economy_coverage', 'wbddh_languages_supported']

    for elem in tax_fields:
        template['field_'+elem] = taxonomy.get('field_'+elem, None, default=True)

    return template

def rs_template():
    template = {
        'title': None,
        'type': 'resource',
    }

    tax_fields = ['wbddh_resource_type', 'wbddh_data_class']
    for elem in tax_fields:
        template['field_'+elem] = taxonomy.get('field_'+elem, None, default=True)

    return template

def _set_values(d, elem):

    for k,v in elem.iteritems():
        if taxonomy.is_tax(k):
            if type(v) is list:
                d[k] = {'und': v}
            else:
                d[k] = {'und': [v]}
        elif k in ['body', 'field_wbddh_source', 'field_wbddh_publisher_name', 'field_wbddh_search_tags', 'field_ddh_external_contact_email', 'field_wbddh_depositor_notes', 'field_ddh_harvest_sys_id', 'field_wbddh_reference_system']:
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
        elif k in ['field_wbddh_release_date', 'field_wbddh_modified_date']:
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
        elif k in ['og_group_ref']:
            d[k] = {'und': {'target_id': v}}
        else:
            d[k] = v


def new_object(ds):

    obj = {}
    _set_values(obj, ds)
    return obj

def new_dataset(ds, id=None):

    global ddh_host, ddh_session_key, ddh_session_value, ddh_token
    
    if id is None:
        id = ds.get('field_ddh_harvest_sys_id', None)

    # step 1: create resources
    resource_references = []
    for elem in ds['resources']:
        e = copy.deepcopy(elem)
        post_info = None
        if e.get('upload'):
            post_info = {'files[1]': open(e['upload'],'rb'), 'field_name': (None,'field_upload'), 'attach': (None,'1')}
            del e['upload']

        obj = new_object(e)

        url = 'https://{}/api/dataset/node'.format(ddh_host)
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
            if len(response.text.strip()) > 0:
                raise APIError('upload', id, response.text)

    # step 2: create dataset with resources attached
    e = copy.deepcopy(ds)
    del e['resources']
    obj = new_object(e)
    url = 'https://{}/api/dataset/node'.format(ddh_host)
    if len(resource_references) > 0:
        obj['field_resources'] = {'und': []}
        for elem in resource_references:
            obj['field_resources']['und'].append({'target_id': '{} ({})'.format(elem['title'], elem['nid'])})

    response = requests.post(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
    try:
        data = response.json()
        dataset_node = data['nid']
        return dataset_node
    except:
        raise APIError(e['type'], id, response.text)

    # step 3: attach resources
    if len(resource_references) > 0 and False:
        obj = {
          'field_resources': {'und': []}
        }
        for elem in resource_references:
            obj['field_resources']['und'].append({'target_id': '{} ({})'.format(elem['title'], elem['nid'])})

        url = 'https://{}/api/dataset/node/{}'.format(ddh_host, dataset_node)
        response = requests.put(url, cookies={ddh_session_key: ddh_session_value}, headers={'X-CSRF-Token': ddh_token}, json=obj)
        # print json.dumps(obj, indent=4)
        try:
            data = response.json()
            return dataset_node
        except:
            raise APIError('put', id, response.text)

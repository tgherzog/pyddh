
import requests
import re
import json
import os

ddh_terms = {}

def _add_definitions(json, id, fields, fields_to_add):

    global ddh_terms

    # some countries we need to match manually
    custom_map = {
      'STP': u'226',    # Sao Tome & Principe
      'PRK': u'150',    # North Korea
      'CUW': u'33',     # Curacao
      'CIV': u'88',     # Cote d'Ivoire
      'LCR': u'842',    # LAC
    }

    for elem in json:
        if custom_map.get(elem['id']):
            ddh_terms['field_wbddh_country']['keywords'][elem[id].lower()] = custom_map.get(elem['id'])
            for f2 in fields_to_add:
                ddh_terms['field_wbddh_country']['keywords'][elem[f2].lower()] = custom_map.get(elem['id'])
        else:
            name = re.sub('\s*\(excluding high income\)$', '', elem[id].lower())
            for f in fields:
                if ddh_terms[f]['keywords'].get(name):
                    for f2 in fields_to_add:
                        ddh_terms[f]['keywords'][elem[f2].lower()] = ddh_terms[f]['keywords'][name]


# return True if term is a recognized taxonomy field
def is_tax(field):
    global ddh_terms

    return ddh_terms.get(field) is not None


# get DDH term for a given field and value with optional default if not found
def get(field, key, default=False):
    global ddh_terms

    if key is None:
        return ddh_terms[field]['default']

    fail_term = ddh_terms[field]['default'] if default else None

    return ddh_terms[field]['keywords'].get(key.lower(), fail_term)

def set_default(field, key):
    global ddh_terms

    ddh_terms[field]['default'] = ddh_terms[field]['keywords'].get(key.lower())

def update(obj, values, default=False):

    for k,v in values.iteritems():
        obj[k] = get(k, v, default=default)

# get all keywords for a given term
def get_keywords(field, tid):

    global ddh_terms
    keywords = []
    for k,v in ddh_terms[field]['keywords'].iteritems():
        if v == tid:
            keywords.append(k)

    return keywords

# load from APIs: required once on module load
def load(host):
    global ddh_terms

    ddh_terms = {}
    response = requests.get('https://{}/internal/listvalues'.format(host))
    api_data = response.json()

    for elem in api_data:
        # Fix incorrect information in the API
        if elem['machine_name'] == 'field_wbddh_global_practices':
            name = 'field_wbddh_gps_ccsas'
        elif elem['machine_name'] == 'field_wbddh_source':
            name = 'field_wbddh_ds_source'
        else:
            name = elem['machine_name']

        if ddh_terms.get(name) is None:
            ddh_terms[name] = {'default': None, 'keywords': {}}

        if re.search('not specified$', elem['list_value_name'].lower()) is None:
            ddh_terms[name]['keywords'][elem['list_value_name'].lower()] = elem['tid']
        else:
            ddh_terms[name]['default'] = elem['tid']

    # now get the country names from the WB API
    response = requests.get('http://api.worldbank.org/v2/en/country?per_page=500&format=json')
    api_data = response.json()
    api_data = api_data[1]

    _add_definitions(api_data, 'name', ['field_wbddh_region', 'field_wbddh_country', 'field_wbddh_economy_coverage'], ['id', 'iso2Code'])

    # hack additional terms here
    ddh_terms['field_wbddh_country']['keywords']['sar'] = u'843'

    response = requests.get('http://api.worldbank.org/v2/en/lendingtypes?per_page=500&format=json')
    api_data = response.json()
    api_data = api_data[1]

    _add_definitions(api_data, 'value', ['field_wbddh_economy_coverage'], ['id', 'iso2code'])

def sanity_check():

    global ddh_terms

    response = requests.get('http://api.worldbank.org/v2/en/country?per_page=500&format=json')
    api_data = response.json()
    api_data = api_data[1]

    for elem in api_data:
        if not get('field_wbddh_region', elem['name']) and \
        not get('field_wbddh_country', elem['name']) and \
        not get('field_wbddh_economy_coverage', elem['name']):
          # capitalCity indicates this is a country not an aggregate. Those are of special interest
          print '  {}{} {}'.format(' ' if elem['region']['id'] == 'NA' else '*', elem['id'], elem['name'].encode('utf-8'))

    response = requests.get('http://api.worldbank.org/v2/en/lendingtypes?per_page=500&format=json')
    api_data = response.json()
    api_data = api_data[1]

    print '----'
    for elem in api_data:
        if not get('field_wbddh_economy_coverage', elem['value']):
            print '   {} {}'.format(elem['id'], elem['value'])


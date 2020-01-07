from . import taxonomy
from . import dataset
from . import util
import os
import yaml
import requests

ddh_config = {}
session_key = None
session_value = None
token = None

host = ''
protocol = 'https'

debug = False
hack_mode = False   # take certain steps to 'gracefully' recover from some cases of bad server behavior

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


def load(host, user=None, pswd=None):
    '''Load the  module targeted at a specified host. This function is required on startup

    Parameters:
        host:  hostname. This parameter is also used as a key into config.yaml to load configuration parameters

        user:  user name. Set to None to use the default in config.yaml

        pswd:  password. Set to None to use the default in config.yaml

    Examples:
        ddh.load('datacatalog.worldbank.org')
    '''

    global ddh_config, session_key, session_value, token, protocol

    globals()['host'] = host

    possible_paths = [os.path.join(os.path.expanduser('~'), '.ddh_config.yaml'), os.path.join(os.getcwd(), 'config.yaml')]
    for path in possible_paths:
        if os.path.isfile(path):
            break

    with open(path, 'r') as fd:
        try:
            config = yaml.safe_load(fd)
        except:
            raise ValueError('Incorrect yaml file format in {}'.format(path))

        ddh_config = config.get(host) or {}
        if not ddh_config:
            raise ValueError('No config information for {} in {}'.format(host, path))

        ddh_config['host'] = host

        protocol = config.get('protocol', protocol)

    # login and get token
    if not user or not pswd:
        user = ddh_config['user']
        pswd = ddh_config['password']

    body = {'username': user, 'password': pswd}
    url = '{}://{}/api/dataset/user/login'.format(protocol, host)

    response = requests.post(url, json=body)
    try:
        json = response.json()
    except:
        # print 'Login response error'
        # print response.text
        raise

    if type(json) is not dict:
        raise InitError('login access denied')

    session_key = json['session_name']
    session_value = json['sessid']

    # request a session token
    url = '{}://{}/services/session/token'.format(protocol, host)
    response = requests.post(url, cookies={session_key: session_value})
    token = response.text

    # load taxonomy tables - this also does a lot of sanity checks and conversions to ISO3 country codes
    taxonomy.load(ddh_config)

def debug_report(label, obj=None):
    "Internal function to report debug information"
    global debug

    if debug:
        print(label)
        if obj is not None:
          print(json.dumps(obj, indent=4))

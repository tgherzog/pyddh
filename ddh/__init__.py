import taxonomy
import dataset
import util
import os
import yaml

ddh_host = ''
ddh_config = {}

def load(host, user=None, pswd=None):
  global ddh_host, ddh_config

  ddh_host = host

  path = os.path.join(os.getcwd(), 'config.yaml')
  with open(path, 'r') as fd:
    try:
        config = yaml.load(fd)
    except:
        raise ValueError('Incorrect yaml file format in {}'.format(path))

    ddh_config = config.get(host) or {}
    if not ddh_config:
        raise ValueError('No config information for {} in {}'.format(host, path))

    ddh_config['host'] = host

  taxonomy.load(ddh_config)
  dataset.load(ddh_config)


import requests
import sys
import os
import urlparse
import datetime
from docopt import docopt

"""
Harvest data from eex and load into DDH. The name is a slight misnomer; the script
imports data, it doesn't synchronize it.

Usage:
  eex-harvest.py [--overwrite]

Options:
  --overwrite, -w     Overwrite all downloaded files (otherwise, just download new files)

"""

config = docopt(__doc__, version='version 0.1')

host     = 'https://energydata.info'
api_base = '{}/api/3/action'.format(host)

downloads = './eex-files'
if not os.path.exists(downloads):
    os.mkdir(downloads)

response = requests.get('{}/package_list'.format(api_base))
packages = response.json()['result']
for id in packages:
    response = requests.get('{}/package_show?id={}'.format(api_base, id))
    pkg = response.json()
    info = pkg.get('result', {})
    if pkg['success'] is False:
        sys.stderr.write('ERROR: ' + id)
    elif pkg['success'] and info.get('organization') and info['organization']['title'] == 'World Bank Group':
        print info['id']
        if info.get('resources'):
            dir = '{}/{}'.format(downloads, info['id'])

            for r in info['resources']:
                print '  {}'.format(r['url'])
                if r.get('url') and r['url'].startswith('{}/dataset/'.format(host)):
                    # only download files from the local store

                    # make sure a local directory exists
                    if not os.path.exists(dir):
                        os.mkdir(dir)

                    f_response = requests.get(r['url'], stream=True)
                    url_parts  = urlparse.urlparse(r['url'])

                    # stream the download to a file
                    download = '{}/{}'.format(dir, os.path.basename(url_parts.path))
                    if config['--overwrite'] or not os.path.exists(dir):
                        os.remove(download)
                        with open(download, 'wb') as fd:
                            for chunk in f_response.iter_content(chunk_size=1024):
                                fd.write(chunk)

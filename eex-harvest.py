#!/usr/bin/python -u
"""
Harvest data from eex and load into DDH. The name is a slight misnomer; the script
imports data, it doesn't synchronize it.

Usage:
  eex-harvest.py --summary
  eex-harvest.py [--overwrite] NAME
  eex-harvest.py [--overwrite] --all
  eex-harvest.py --from=FILE
  eex-harvest.py --test=FILE

Options:
  --overwrite, -w     Overwrite all downloaded files (otherwise, just download new files)
  --all               Import all datasets from EEX
  --summary           Generate summary list of qualifying datasets to harvest (serves as input to --from)
  --from=FILE         Load FILE as list of identifiers to process (one per line, # comments recognized)
  --test=FILE         Load FILE as a test JSON object instead of reading the energydata API

"""

# TODO:
# better error recovery: should probably pre-flight resources before loading
# support for loading a single dataset by CKAN ID

import requests
import sys
import os
import json
import yaml
import urlparse
from datetime import datetime
from docopt import docopt

extension_map = {
  '.tif': '.tiff',
}

# this list is taken from the Drupal admin interface
supported_extensions = [
'csv', 'html', 'xls', 'json', 'xlsx', 'doc', 'docx', 'rdf', 'txt', 'jpg',
'png', 'gif', 'tiff', 'pdf', 'odf', 'ods', 'odt', 'tsv', 'geojson', 'xml',
'zip', 'dta', 'do', 'apf', 'dbf', 'mdb', 'gdb', 'mpk', 'ppts', 'ppt', 'py',
'r', 'sav', 'zsav', 'sys', 'por', 'dat']

sys.dont_write_bytecode = True
import ddh


config = docopt(__doc__)

host     = 'https://energydata.info'
target   = 'newdatacatalogstg.worldbank.org'
group_ref = 124846 # need to confirm the node ID reference for the dataset group
api_base = '{}/api/3/action'.format(host)
time_fmt = '%Y-%m-%dT%H:%M:%S'

try:
    ddh.load(target)
    # ddh.dataset.debug = True
except ddh.dataset.InitError as err:
    sys.stderr.write(err.message + '\n')
    sys.exit(-1)
    

downloads = './eex-files'
if not os.path.exists(downloads):
    os.mkdir(downloads)

test_pkg = None

# load test package if specified, along with a placeholder package list
if config['--test']:
    # a one-item package list just so we go through the loop one time
    src_package_list = ['placeholder']
    with open(config['--test'], 'r') as fd:
        test_pkg = {'success': True, 'result': json.load(fd)}
elif config['--from']:
    # use specified package list in lieu of the site's package list
    # presumably, this is based on output from --summary mode
    with open(config['--from'], 'r') as fd:
        src_package_list = [s.strip() for s in fd.readlines() if len(s.strip()) > 0 and s[0] != '#']
else:
    response = requests.get('{}/package_list'.format(api_base))
    src_package_list = response.json()['result']


for id in src_package_list:
    if test_pkg:
        pkg = test_pkg
    elif config.get('NAME') and config['NAME'] != id:
        continue
    else:
        response = requests.get('{}/package_show?id={}'.format(api_base, id))
        pkg = response.json()

    info = pkg.get('result', {})
    if pkg['success'] is False:
        sys.stderr.write('ERROR: ' + id + '\n')
    elif pkg['success'] and info.get('organization') and info['organization']['title'] == 'World Bank Group':
        if config['--summary']:
            print id
            continue

        print 'Processing {} ({})'.format(info['name'], info['id'])
        dataset_type = 'Other' # default dataset type
        if info.get('resources'):
            dir = '{}/{}'.format(downloads, info['id'])

            for r in info['resources']:
                if r.get('format', '') in ['SHP', 'GeoJSON', 'KML', 'shapefiles', 'geotiff', 'Esri REST', 'geopackage']:
                    dataset_type = 'Geospatial'

                if r.get('url') and r['url'].startswith('{}/dataset/'.format(host)):
                    # only download files from the local store

                    # make sure a local directory exists
                    if not os.path.exists(dir):
                        os.mkdir(dir)

                    url_parts  = urlparse.urlparse(r['url'])
                    filename = os.path.basename(url_parts.path)
                    (basename,ext) = os.path.splitext(filename)

                    if ext == '':
                        print 'Warning: no support for files with no file extension: {} in {} ({})'.format(filename, info['name'], info['id'])
                        next

                    ext = extension_map.get(ext.lower(), ext.lower())

                    if ext[1:] not in supported_extensions:
                        print 'Warning: {} is not a supported file type in DDH: {} ({})'.format(filename, info['name'], info['id'])
                        next

                    # stream the download to a file
                    download = '{}/{}'.format(dir, basename + ext)
                    r['local_path'] = download
                    if config['--overwrite'] or not os.path.exists(download):
                        if os.path.exists(download):
                            os.remove(download)

                        f_response = requests.get(r['url'], stream=True)
                        with open(download, 'wb') as fd:
                            for chunk in f_response.iter_content(chunk_size=1024):
                                fd.write(chunk)

        # map country codes
        countries = info['country_code'] + info['region']
        country_tids = []
        for elem in countries:
            tid = ddh.taxonomy.get('field_wbddh_country', elem)
            if tid:
                country_tids.append(tid)
            elif elem == 'AFR':
                # map Africa to MENA+SSA
                country_tids.extend([ddh.taxonomy.get('field_wbddh_country', 'MEA'), ddh.taxonomy.get('field_wbddh_country', 'SSA')])
            else:
                print 'Warning: unrecognized country/region code {} in {} ({})'.format(elem, info['name'], info['id'])

        # We stuff a bunch of miscellanous information into the keywords field expressed as a yaml object
        # including for the moment, the external dataset identifier
        keywords = {'id': 'energydata.info'}

        if info.get('group'):
            keywords['group']  = info['group']

        if info.get('topic'):
            keywords['topic'] = info['topic']

        # initialize dataset with values that are constant for this platform
        ds = ddh.dataset.ds_template()
        ds.update({
            'field_wbddh_dsttl_upi': '21812', # Yann Tanvez
            'field_wbddh_collaborator_upi': '23715', # Jemire (Jemi) Lacle
            'og_group_ref': group_ref,
            'field_wbddh_search_tags': yaml.safe_dump(keywords, default_flow_style=False),
        })

        ddh.taxonomy.update(ds, {
            'field_ddh_harvest_src': 'Energy Info',
            'field_wbddh_data_class': 'Public',
            'field_topic': 'Energy and Extractives',
            'field_wbddh_gps_ccsas': 'Energy & Extractives',
            'field_wbddh_languages_supported': 'English',
            'field_wbddh_ds_source': 'World Bank Group',
            'field_tags': 'energydata.info',
        })

        # add metadata that varies by dataset
        ds.update({
            'title': info['title'],
            'body': info['notes'],
            'field_ddh_harvest_sys_id': info['id'],
            'field_wbddh_country': country_tids,
            'field_wbddh_release_date': ddh.util.date(info['metadata_created'].split('.')[0]),
            'field_wbddh_modified_date': ddh.util.date(info['metadata_modified'].split('.')[0]),
            'field_ddh_external_contact_email': info['author_email'],
            'field_wbddh_source': info['url'],
            'field_wbddh_publisher_name': info['author'],
            'field_wbddh_reference_system': info['ref_system'],
            'field_wbddh_start_date': ddh.util.date(info['start_date']),
            'field_wbddh_end_date': ddh.util.date(info['end_date']),
            'field_wbddh_time_periods': ddh.util.date(info['release_date']),
        })

        ddh.taxonomy.update(ds, {
            'field_wbddh_data_type': dataset_type,
            # 'field_wbddh_terms_of_use': 'Open Data Access',
            'field_license_wbddh': 'Open Database License' if info['license_id'] in ['ODbL-1.0', 'ODC-BY-1.0'] else 'Creative Commons Attribution 4.0'
        })

        # Same thing for resources
        for r in info.get('resources', []):
          rs = ddh.dataset.rs_template()
          rs.update({
            'title': r['name'],
            'body': r['description'],
            'field_format': ddh.taxonomy.get('field_format', r['format'], ddh.taxonomy.get('field_format', 'Other')),
          })

          ddh.taxonomy.update(rs, {
            'field_wbddh_data_class': 'Public',
            'field_wbddh_resource_type': 'Download',
          })

          if r.get('local_path'):
            if os.stat(r['local_path']).st_size > 90 * (1024*1024):
                print 'Warning: file exceeds maximum upload size: {} in {} ({})'.format(r['local_path'], info['name'], info['id'])
            else:
                rs['upload'] = r['local_path']
          elif r.get('url'):
            rs['field_link_api'] = r['url']

          ds['resources'].append(rs)

        try:
            nodeid = ddh.dataset.new_dataset(ds)
            print 'Created {},{}'.format(info['name'], nodeid)
        except ddh.dataset.APIError as err:
            print 'Error creating dataset [{}]: {} ({})'.format(err.type, info['name'], info['id'])
            print err.response


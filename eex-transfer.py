#!/usr/bin/python -u
"""
transfer data from eex and load into DDH.

Usage:
  eex-transfer.py [--debug] --summary
  eex-transfer.py [--debug --overwrite] NAME
  eex-transfer.py [--debug --overwrite] --all
  eex-transfer.py [--debug] --from=FILE
  eex-transfer.py [--debug] --test=FILE

Options:
  --overwrite, -w     Overwrite all downloaded files (otherwise, just download new files)
  --debug             Debug mode: reports submitted objects
  --all               Import all datasets from EEX
  --summary           Generate summary list of qualifying datasets to transfer (serves as input to --from)
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

formats_map = {
  'shp': 'SHP ZIP',
  'shapefiles': 'SHP ZIP',
  'xlsx': 'Excel',
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

# defaults are now stored locally
with open('config.yaml', 'r') as fd:
    try:
        yaml_config = yaml.load(fd)
        for k,v in yaml_config['defaults'].iteritems():
            if k not in config:
                config[k] = v
    except:
        raise

# sanity checks
for k in ['host', 'target', 'ttl', 'max_size']:
    if k not in config:
        sys.exit('{} is undefined'.format(k))

# for convenience, some config paarams are converted to local vars
(host,target) = (config['host'], config['target'])
api_base = '{}/api/3/action'.format(host)
time_fmt = '%Y-%m-%dT%H:%M:%S'

try:
    ddh.load(target)
    ddh.taxonomy.set_default('field_format', 'Other')
    if config['--debug']:
        ddh.dataset.debug = True

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
        src_package_list = [s.strip(',\n') for s in fd.readlines() if len(s.strip(',\n')) > 0 and s[0] != '#']
else:
    response = requests.get('{}/package_list'.format(api_base))
    src_package_list = response.json()['result']


for id in src_package_list:
    if test_pkg:
        pkg = test_pkg
    elif config.get('NAME') and config['NAME'] != id:
        continue
    elif id in config.get('black_list',[]):
        print 'Skipping: {} is on the blacklist.'.format(id)
        continue
    else:
        response = requests.get('{}/package_show?id={}'.format(api_base, id))
        pkg = response.json()

    info = pkg.get('result', {})
    if pkg['success'] is False:
        sys.stderr.write('ERROR: ' + id + '\n')
    elif pkg['success'] and info.get('organization') and info['organization']['title'] == 'World Bank Group':
        if config['--summary']:
            # print '{}'.format(id)
            print '{},{}'.format(id, len(info.get('resources', [])))
            continue

        print 'Processing {} ({})'.format(info['name'], info['id'])
        dataset_type = 'Other' # default dataset type
        resource_failure = False
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
                        print 'Skipping: no support for files with no file extension: {} in {} ({}).'.format(filename, info['name'], info['id'])
                        resource_failure = True
                        break

                    ext = extension_map.get(ext.lower(), ext.lower())

                    if ext[1:] not in supported_extensions:
                        print 'Skipping: {} is not a supported file type in DDH: {} ({}).'.format(filename, info['name'], info['id'])
                        resource_failure = True
                        break

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

        if resource_failure:
            continue

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

        # We stuff a bunch of miscellanous information into the external_metadata field expressed as a yaml object
        # including for the moment, the external dataset identifier
        external_metadata = {'id': 'energydata.info'}

        if info.get('group'):
            external_metadata['group']  = info['group']

        if info.get('topic'):
            external_metadata['topic'] = info['topic']

        if info.get('release_date'):
            external_metadata['publish_date'] = info['release_date']

        # initialize dataset with values that are constant for this platform
        ds = ddh.dataset.ds_template()
        ds.update({
            'field_wbddh_dsttl_upi': config['ttl'],
            'field_wbddh_collaborator_upi': config.get('collab'),
            'og_group_ref': config.get('group_ref'),
            'field_external_metadata': yaml.safe_dump(external_metadata, default_flow_style=False),
            'field_tags': config.get('field_tag'),
        })

        ddh.taxonomy.update(ds, {
            'field_ddh_harvest_src': config.get('harvest_src'),
            'field_wbddh_data_class': 'Public',
            'field_topic': 'Energy and Extractives',
            'field_wbddh_gps_ccsas': 'Energy & Extractives',
            'field_wbddh_languages_supported': 'English',
            'field_wbddh_ds_source': 'World Bank Group',
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
            'field_wbddh_reference_system': info.get('ref_system',''),
            'field_wbddh_start_date': ddh.util.date(info.get('start_date')),
            'field_wbddh_end_date': ddh.util.date(info.get('end_date')),
            'field_wbddh_time_periods': ddh.util.date(info.get('release_date')),
        })

        ddh.taxonomy.update(ds, {
            'field_wbddh_data_type': dataset_type,
            # 'field_wbddh_terms_of_use': 'Open Data Access',
            'field_license_wbddh': 'Open Database License' if info['license_id'] in ['ODbL-1.0', 'ODC-BY-1.0'] else 'Creative Commons Attribution 4.0'
        })

        # Same thing for resources
        for r in info.get('resources', []):
          rs = ddh.dataset.rs_template()
          format = formats_map.get(r['format'].lower(), r['format'])
          rs.update({
            'title': r['name'],
            'body': r['description'],
            'field_format': ddh.taxonomy.get('field_format', format, True),
          })

          ddh.taxonomy.update(rs, {
            'field_wbddh_data_class': 'Public',
            'field_wbddh_resource_type': 'Download',
          })

          if r.get('local_path'):
            if os.stat(r['local_path']).st_size > config['max_size']*(1024*1024):
                print 'Warning: file exceeds maximum upload size: {} in {} ({})'.format(r['local_path'], info['name'], info['id'])
                r['large_file_url'] = r['url']
            else:
                rs['upload'] = r['local_path']
          elif r.get('url'):
            rs['field_link_api'] = r['url']

          ds['resources'].append(rs)

        try:
            result = ddh.dataset.new_dataset(ds)
            print 'Created {},{}'.format(info['name'], result['nid'])
            for i in range(len(result['resources'])):
                if info['resources'][i].get('large_file_url'):
                    print 'ATTACH: {}/{}'.format(result['resources'][i]['nid'], info['resources'][i]['large_file_url'])
        except ddh.dataset.APIError as err:
            print 'Error creating dataset [{}]: {} ({})'.format(err.type, info['name'], info['id'])
            print err.response


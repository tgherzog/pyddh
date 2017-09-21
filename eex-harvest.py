
"""
Harvest data from eex and load into DDH. The name is a slight misnomer; the script
imports data, it doesn't synchronize it.

Usage:
  eex-harvest.py [--overwrite]

Options:
  --overwrite, -w     Overwrite all downloaded files (otherwise, just download new files)

"""

# TODO:
# implement license mapping once new license framework comes online
# better error recovery: should probably pre-flight resources before loading
# support for loading a single dataset by CKAN ID

import requests
import sys
import os
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

session_key = 'SSESSf70b342115438dbd206e1df9422509d2'
session_val = 'bZun_9ai3ag0KP-eIV24cr9LfzNnTqkSSzxupkPA2KE'

host     = 'https://energydata.info'
target   = 'datacatalogbetastg.worldbank.org'
group_ref = '102372' # need to confirm the node ID reference for the dataset group
api_base = '{}/api/3/action'.format(host)
time_fmt = '%Y-%m-%dT%H:%M:%S'

ddh.load(target, session_key, session_val)

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
        dataset_type = 'Other' # default dataset type
        if info.get('resources'):
            dir = '{}/{}'.format(downloads, info['id'])

            for r in info['resources']:
                if r.get('format', '') in ['SHP', 'GeoJSON', 'KML', 'shapefiles', 'geotiff', 'Esri REST', 'geopackage']:
                    datset_type = 'Geospatial'

                if r.get('url') and r['url'].startswith('{}/dataset/'.format(host)):
                    # only download files from the local store

                    # make sure a local directory exists
                    if not os.path.exists(dir):
                        os.mkdir(dir)

                    url_parts  = urlparse.urlparse(r['url'])
                    filename = os.path.basename(url_parts.path)
                    (basename,ext) = os.path.splitext(filename)

                    if ext == '':
                        sys.stderr.write('Warning: no support for files with no file extension: {} in {} ({})\n'.format(filename, info['name'], info['id']))
                        next

                    ext = extension_map.get(ext.lower(), ext.lower())

                    if ext[1:] not in supported_extensions:
                        sys.stderr.write('Warning: {} is not a supported file type in DDH: {} ({})\n'.format(ext, info['name'], info['id']))
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
                country_tids.extend([ddh.taxonomy.get('field_wbddh_country', 'MEA'), ddh.taxonomy.get('field_wb_country', 'SSA')])
            else:
                sys.stderr.write('Warning: unrecognized country/region code {} in {} ({})\n'.format(elem, info['name'], info['id']))

        # initialize dataset with values that are constant for this platform
        ds = ddh.dataset.ds_template()
        ds.update({
            'field_ddh_dsttl_upi': '21812', # Yann Tanvez
            'field_ddh_collaborator_upi': '23715', # Jemire (Jemi) Lacle
            'og_group_ref': group_ref,
        })

        ddh.taxonomy.update(ds, {
            'field_ddh_harvest_src': 'Energy Info',
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
            'field_wbddh_release_date': datetime.strptime(info['metadata_created'].split('.')[0], time_fmt),
            'field_wbddh_modified_date': datetime.strptime(info['metadata_modified'].split('.')[0], time_fmt),
            'field_ddh_external_contact_email': info['author_email'],
            'field_wbddh_source': info['url'],
            'field_wbddh_publisher_name': info['author'],
            'field_wbddh_reference_system': info['ref_system'],
            # 'field_wbddh_start_date': datetime.strptime(info['start_date'], '%d-%b-%Y'),
            # 'field_wbddh_end_date': datetime.strptime(info['end_date'], '%d-%b-%Y'),
            # 'field_wbddh_time_periods': datetime.strptime(info['release_date'], '%Y'),
        })

        ddh.taxonomy.update(ds, {
            'field_wbddh_data_type': dataset_type,
            'field_wbddh_terms_of_use': 'Open Data Access', # for now
        })

        # Same thing for resources
        for r in info.get('resources', []):
          rs = ddh.dataset.rs_template()
          rs.update({
            'title': r['name'],
            'field_format': ddh.taxonomy.get('field_format', r['format'], ddh.taxonomy.get('field_format', 'Other')),
          })

          ddh.taxonomy.update(rs, {
            'field_wbddh_data_class': 'Public',
            'field_wbddh_resource_type': 'Download',
          })

          if r.get('local_path'):
            if os.stat(r['local_path']).st_size > 90 * (1024*1024):
                sys.stderr.write('Warning: file exceeds maximum upload size: {} in {} ({})\n'.format(r['local_path'], info['name'], info['id']))
            else:
                rs['upload'] = r['local_path']
          elif r.get('url'):
            rs['field_link_api'] = r['url']

          ds['resources'].append(rs)

        try:
            nodeid = ddh.dataset.new_dataset(ds)
            print nodeid
            print ddh.dataset.new_object(ds)
        except ddh.dataset.APIError as err:
            sys.stderr.write('Error creating dataset [{}]: {} ({})\n'.format(err.type, info['name'], info['id']))
            sys.stderr.write(err.response + '\n')

        break

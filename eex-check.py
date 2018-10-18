
import requests
import re

# quick & dirty script to scan EEX and look for links that may already be in the data catalog or DECDG systems

url = 'https://energydata.info/api/3/action/package_list'
regex_str = '^https?://(data|databank|datacatalog).worldbank.org/';
response = requests.get(url).json()
for id in response['result']:
    url = 'https://energydata.info/api/3/action/package_show?id={}'.format(id)
    response = requests.get(url).json()
    dataset = response['result']
    try:
        if dataset.get('organization',{}).get('name','') == 'world-bank-grou':
            if re.match(regex_str, dataset.get('url', '')):
                print 'Dataset URL: {} {}'.format(id, dataset['url'])

            for i in dataset.get('resources',[]):
                if re.match('^https?://(data|databank|datacatalog).worldbank.org/', i['url']):
                    print 'Resource URL: {} {}'.format(id, i['url'])
    except:
        raise

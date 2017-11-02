
import re
from datetime import datetime

def date(s):

    if not s:
        return None

    s = str(s).strip()

    templates = {
        '\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}:\d{1,2}': '%Y-%m-%dT%H:%M:%S',
        '\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}': '%Y-%m-%d %H:%M:%S',

        '\d{4}-\d{1,2}-\d{1,2}': '%Y-%m-%d',
        '\d{4}/\d{1,2}/\d{1,2}': '%Y/%m/%d',

        '\d{1,2}-\d{1,2}-\d{4}': '%d-%m-%Y',
        '\d{1,2}/\d{1,2}/\d{4}': '%d/%d/%Y',

        '\d{1,2}-\w{3}-\d{4}': '%d-%b-%Y',
        '\d{1,2}/\w{3}/\d{4}': '%d/%b/%Y',

        '\d{4}': '%Y',
    }

    for k,v in templates.iteritems():
        if re.search('^'+k+'$', s):
            return datetime.strptime(s, v)

    raise TypeError()

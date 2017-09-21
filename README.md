

### open_datasets.py

Defines "native" (not externally harvested) datasets in DCS, and their partial paths (e.g., dataset/{path}) in DDH
Some datasets have no path because they were not imported into DDH, for a variety of reasons. This file is derived
from the curator's google spreadsheet.

### od-catalog-info.py

Obtains the nid and uuid for each DCS dataset's counterpart in DDH, through a combination of scraping and API calls

###  od-compare.py

Compares metadata from DCS and DDH to identify inconsistencies


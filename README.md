

### open_datasets.py

Defines "native" (not externally harvested) datasets in DCS, and their partial paths (e.g., dataset/{path}) in DDH
Some datasets have no path because they were not imported into DDH, for a variety of reasons. This file is derived
from the curator's google spreadsheet.

### od-catalog-info.py

Obtains the nid and uuid for each DCS dataset's counterpart in DDH, through a combination of scraping and API calls

###  od-compare.py

Compares metadata from DCS and DDH to identify inconsistencies


### DKAN Taxonomy Dictionary

Useful information for using the DKAN endpoints as well as `/search-service/search_api/datasets`

Field                 | Key                       | Taxonomy Term       | tid
--------------------- | ------------------------- | ------------------- | ---
Data Type             | field_wbddh_data_type     | Time Series         | 293
                      |                           | Microdata           | 294
                      |                           | Geospatial          | 295
                      |                           | Other               | 853
Data Classification   | field_wbddh_data_class    | Public              | 358
                      |                           | Official Use Only   | 359
                      |                           | Confidential        | 360
Terms of Use          | field_wbddh_terms_of_use  | Open Data Access    | 434
                      |                           | Direct Access       | 435
                      |                           | External Repository | 438
                      |                           | ESRI Credits        | 442
                      |                           | Licensed            | 437
                      |                           | No access           | 439
                      |                           | Public use          | 436
                      |                           | Restricted          | 441
                      |                           | Not specified       | 876
Contact Details       | field_contact_email       | n/a (text)          |


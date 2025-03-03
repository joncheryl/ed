'''
John Sherrill
Feb 9, 2025

Script to download and organize NAEP data for all states.
API documentation: https://www.nationsreportcard.gov/api_documentation.aspx
'''
#%%
import sqlite3
from itertools import product
import json
import pandas as pd
import requests as rq

PRE_PATH = ""
PRE_PATH_DATA = PRE_PATH + "data/fiscal/fiscal_"

#%%
###############################################################################
# Function for accessing NAEP API.
###############################################################################

def get_data(payload):
    """
    Example payload:
    
    payload = {'type': 'data',
               'subject': 'mathematics',
               'grade': 4,
               'subscale': 'MRPCM',
               'variable': 'TOTAL',
               'jurisdiction': 'UT',
               'stattype': 'MN:MN',
               'Year': '1990R2'}
    """
    url = "https://www.nationsreportcard.gov/DataService/GetAdhocData.aspx"
    r = rq.get(url, params=payload, timeout=100)

    raw_text = r.text.encode('unicode_escape').decode()
    data_json = json.loads(raw_text)

    if data_json['status'] == 200:
        return data_json['result']

    return None

# API is super slow so getting data one state at a time. With the loop below
# over these jurisdictions, if a request times out, then not everything is
# lost.
JURISDICTIONS = [
    'NT', 'NP', 'NR', 'NL', 'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE',
    'DC', 'DS', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA',
    'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN',
    'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DD', 'DO', 'XQ', 'XA',
    'XU', 'XM', 'XB', 'XT', 'XC', 'XX', 'XV', 'XS', 'XY', 'XR', 'XW', 'XE',
    'XZ', 'XF', 'XG', 'XO', 'XH', 'XJ', 'XL', 'XI', 'XK', 'XN', 'XP', 'XD',
    'YA', 'AS', 'GU', 'PR', 'VI']

data_returned = []

TYPES = ['data']
VARIABLES = ['TOTAL']
STATTYPES = ['MN:MN', 'SD:SD']

KEYS = ['type', 'subject', 'grade', 'subscale', 'variable', 'jurisdiction',
        'stattype', 'Year']

# Have to put the parameters SUBJECT, SCALE, GRADE, and YEAR together because
# not all subjects were tested for all grades in all years.
# Also, making individual API requests for each year was very slow.
SUBJECT_SCALE_GRADE_YEAR = [
    ['mathematics', 'MRPCM', 4, ','.join(
         ['1990R2', '1992R2', '1996R2', '1996R3', '2000R2', '2000R3', '2003R3',
          '2005R3', '2007R3', '2009R3', '2011R3', '2013R3', '2015R3', '2017R3',
          '2019R3', '2022R3', '2024R3'])],
    ['mathematics', 'MRPCM', 8, ','.join(
         ['1990R2', '1992R2', '1996R2', '1996R3', '2000R2', '2000R3', '2003R3',
          '2005R3', '2007R3', '2009R3', '2011R3', '2013R3', '2015R3', '2017R3',
          '2019R3', '2022R3', '2024R3'])],
    ['reading', 'RRPCM', 4, ','.join(
         ['1992R2', '1994R2', '1998R2', '1998R3', '2000R2', '2000R3', '2002R3',
          '2003R3', '2005R3', '2007R3', '2009R3', '2011R3', '2013R3', '2015R3',
          '2017R3', '2019R3', '2022R3', '2024R3'])],
    ['reading', 'RRPCM', 8, ','.join(
         ['1992R2', '1994R2', '1998R2', '1998R3', '2002R3', '2003R3', '2005R3',
          '2007R3', '2009R3', '2011R3', '2013R3', '2015R3', '2017R3', '2019R3',
          '2022R3', '2024R3'])]
    ]

#%%
###############################################################################
# Loop for each jurisdiction. Can be restarted because data is appended to
# data_returned list when it's ready.
###############################################################################

for jurisdiction in JURISDICTIONS:
    payload_product = list(product(TYPES, SUBJECT_SCALE_GRADE_YEAR, VARIABLES,
                                   [jurisdiction], STATTYPES))
    payload_product = [(w, x[0], x[2], x[1], y, z, aa, x[3]) \
                        for w, x, y, z, aa in payload_product]
    payloads = [dict(zip(KEYS, tup)) for tup in payload_product]

    data_returned = data_returned + [get_data(payload) for payload in payloads]

    print('----- Got ' + jurisdiction + ' -----')

    JURISDICTIONS = [x for x in JURISDICTIONS if x != jurisdiction]

#%%
# Write to file just to have for always and forever.
with open('data/naep/raw_naep.txt', 'w', encoding="utf-8") as file:
    json.dump(data_returned, file, indent=4)

#%%
# # If needed, read the data from file instead of downloading again.
# with open('data/naep/raw_naep.txt', 'r') as file:
#     data_returned = json.load(file)

#%%
# Flatten returned data for DataFrame and remove extreneous data.
data_flattened = [
    dictionary
    for sublist in data_returned
    if sublist is not None
    for dictionary in sublist
]

data_all = pd.DataFrame(data_flattened)
naep = data_all[data_all['isStatDisplayable'].astype(bool)]

# Make DataFrame more readable.
naep = naep.pivot(index=['year',
                         'sample',
                         'subject',
                         'grade',
                         'scale',
                         'jurisdiction',
                         'errorFlag'],
                  columns='stattype',
                  values='value')
naep = naep.reset_index()
naep = naep.rename(columns = {'MN:MN': 'mean',
                              'SD:SD': 'sd'}).rename_axis(columns=None)
naep = naep.rename(columns={'year': 'end_year',
                            'sample': 'accommodations',
                            'subject': 'math_read'})


# %%
###############################################################################
# Write the NAEP table to the database
###############################################################################

# Make a mapping from python/pandas dtypes to SQLITE dtypes
type_map = {'object': 'TEXT',
            'int64': 'INTEGER',
            'Int64': 'INTEGER',
            'float64': 'REAL'}
col_dtypes = naep.dtypes.map(lambda x: type_map.get(str(x)))

# Connect to database and append to created table.
conn = sqlite3.connect('data/state.db')
cursor = conn.cursor()

naep.to_sql('naep',
            con=conn,
            if_exists='append',
            index=False,
            dtype=col_dtypes.to_dict())

conn.close()

#%%

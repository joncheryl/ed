'''
John Sherrill
Dec 8, 2024

Script to organize 2015-2023 directory data from the CCD nonfiscal datasets
and then to concat that with the prepped columns from whole dataframe (2014
and earlier)
'''
#%%

import sqlite3
import pandas as pd

PRE_PATH = "data/nonfiscal/directory/directory_"
files = {year: [f'{PRE_PATH}{year}.csv', '', ',', {}]
         for year in range(2023, 2014, -1)}

# some files/years have a windows/cp1252 encoding
for year in [2021]:
    files[year][1] = 'cp1251'

# some files/years are tab delimited encoding
for year in [2015]:
    files[year][2] = '\t'

# some files/years need better dtype-ing
for year in [2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015
             ]:
    files[year][3] = {'MSTREET1': 'string',
                      'MSTREET3': 'string',
                      'LSTREET3': 'string',
                      'FIPST': int,
                      'STATE_AGENCY_NO': 'Int64',
                      'SY_STATUS': 'Int64',
                      'UPDATED_STATUS': 'Int64',
                      'OUT_OF_STATE_FLAG': 'category'}


pre_directory = {year: pd.read_csv(file, encoding=format,
                                   sep=delim, dtype=kind) for
                  year, (file, format, delim, kind) in files.items()}

directory = (pd.
              concat(pre_directory, names=['END_YEAR', 'fdsa'])
              .reset_index(level='END_YEAR')
              .reset_index(drop=True)
)

# Organizing column names in this cell.
directory['ST'] = directory['ST'].combine_first(directory['STABR'])

directory['OPERATIONAL_SCHOOLS'] = (directory['OPERATIONAL_SCHOOLS']
                                    .combine_first(directory['SCH'])
                                    .astype('Int64')
)

offered_names = {
    'G_PK_OFFERED': 'PKOFFERED',
    'G_KG_OFFERED': 'KGOFFERED',
    'G_1_OFFERED': 'G1OFFERED',
    'G_2_OFFERED': 'G2OFFERED',
    'G_3_OFFERED': 'G3OFFERED',
    'G_4_OFFERED': 'G4OFFERED',
    'G_5_OFFERED': 'G5OFFERED',
    'G_6_OFFERED': 'G6OFFERED',
    'G_7_OFFERED': 'G7OFFERED',
    'G_8_OFFERED': 'G8OFFERED',
    'G_9_OFFERED': 'G9OFFERED',
    'G_10_OFFERED': 'G10OFFERED',
    'G_11_OFFERED': 'G11OFFERED',
    'G_12_OFFERED': 'G12OFFERED',
    'G_13_OFFERED': 'G13OFFERED',
    'G_UG_OFFERED': 'UGOFFERED',
    'G_AE_OFFERED': 'AEOFFERED'
}

for new, old in offered_names.items():
    directory[new] = directory[new].fillna(directory[old])


directory = (directory
    .drop(columns=['STABR',
                   'SCH',
                   'SCHOOL_YEAR', 'SURVYEAR',
                   'SEANAME'
                   ])
    .drop(columns=list(offered_names.values()))
)

# %%

# Concat with 2014 and earlier data
directory = pd.concat([directory,
                       pd.read_csv("data/nonfiscal/directory/" + \
                           "directory_through_2014.csv",
                           dtype={'UNION': 'category',
                                  'FIPST': int,
                                  'MSTREET1': str,
                                  'MZIP': str,
                                  'MZIP4': str,
                                  'LSTREET1': str,
                                  'LCITY': str,
                                  'LSTATE': str,
                                  'LZIP': str,
                                  'LZIP4': str,
                                  'PHONE': str,
                                  'CHARTER_LEA': 'category',
                                  'OPERATIONAL_SCHOOLS': 'Int64',
                                  'AGCHRT': 'category'
                                  },
                           na_values={'OPERATIONAL_SCHOOLS': ['N']})],
                      ignore_index=True)

def bool_mapper(value):
    ''' Clean up the boolean columns '''
    return {'No': False,
            'N': False,
            'Yes': True,
            'Y': True,
            1: True,
            2: False,
            'Not reported': float('nan')}.get(value, float('nan'))

bool_cols = ['OUT_OF_STATE_FLAG', 'NOGRADES', 'G_PK_OFFERED', 'G_KG_OFFERED',
             'G_1_OFFERED', 'G_2_OFFERED', 'G_3_OFFERED', 'G_4_OFFERED',
             'G_5_OFFERED', 'G_6_OFFERED', 'G_7_OFFERED', 'G_8_OFFERED',
             'G_9_OFFERED', 'G_10_OFFERED', 'G_11_OFFERED', 'G_12_OFFERED',
             'G_13_OFFERED', 'G_UG_OFFERED', 'G_AE_OFFERED', 'BIEA']

for col in bool_cols:
    directory[col] = (
        directory[col].map(bool_mapper)
    ).astype("boolean")


def grade_span_mapper(value):
    ''' Clean up the grade span columns'''
    return {'SP': float('nan'),
            '00': float('nan'),
            '.': float('nan')}.get(value, value)

directory['GSLO'] = directory['GSLO'].map(grade_span_mapper)
directory['GSHI'] = directory['GSHI'].map(grade_span_mapper)

# Rename UNION and LEVEL variable/column because both are sql keywords
directory = directory.rename(columns = {'UNION': 'UNION_CODE',
                                        'LEVEL': 'SCHOOL_LEVEL'})

# %%
###############################################################################
# Write the directory table to the database
###############################################################################

# Need to check to make sure OUT_OF_STATE_FLAG, etc. correctly imported as
# a boolean. SQLITE3 doesn't support booleans. Uses ints instead. Might need
# convert boolean categories to integers and go to 0/1 instead of False/True.

# Columns that are in the database directory table
directory_columns = [x for x in directory.columns
                     if x not in ['STATENAME', 'ST']]

col_dtypes = {
    'end_year': 'INTEGER',
    'fipst': 'INTEGER',
    'lea_name': 'TEXT',
    'state_agency_no': 'INTEGER',
    'union_code': 'TEXT',
    'st_leaid': 'TEXT',
    'leaid': 'INTEGER',
    'mstreet1': 'TEXT',
    'mstreet2': 'TEXT',
    'mstreet3': 'TEXT',
    'mcity': 'TEXT',
    'mstate': 'TEXT',
    'mzip': 'TEXT',
    'mzip4': 'TEXT',
    'lstreet1': 'TEXT',
    'lstreet2': 'TEXT',
    'lstreet3': 'TEXT',
    'lcity': 'TEXT',
    'lstate': 'TEXT',
    'lzip': 'TEXT',
    'lzip4': 'TEXT',
    'phone': 'TEXT',
    'website': 'TEXT',
    'sy_status': 'INTEGER',
    'sy_status_text': 'TEXT',
    'updated_status': 'INTEGER',
    'updated_status_text': 'TEXT',
    'effective_date': 'TEXT',
    'lea_type': 'INTEGER',
    'lea_type_text': 'TEXT',
    'out_of_state_flag': 'INTEGER',
    'charter_lea': 'TEXT',
    'charter_lea_text': 'TEXT',
    'nogrades': 'INTEGER',
    'g_pk_offered': 'INTEGER',
    'g_kg_offered': 'INTEGER',
    'g_1_offered': 'INTEGER',
    'g_2_offered': 'INTEGER',
    'g_3_offered': 'INTEGER',
    'g_4_offered': 'INTEGER',
    'g_5_offered': 'INTEGER',
    'g_6_offered': 'INTEGER',
    'g_7_offered': 'INTEGER',
    'g_8_offered': 'INTEGER',
    'g_9_offered': 'INTEGER',
    'g_10_offered': 'INTEGER',
    'g_11_offered': 'INTEGER',
    'g_12_offered': 'INTEGER',
    'g_13_offered': 'INTEGER',
    'g_ug_offered': 'INTEGER',
    'g_ae_offered': 'INTEGER',
    'gslo': 'TEXT',
    'gshi': 'TEXT',
    'level': 'TEXT',
    'igoffered': 'TEXT',
    'operational_schools': 'INTEGER',
    'biea': 'INTEGER',
    'agchrt': 'TEXT'}

# Creates a new database file if it doesn't exist
conn = sqlite3.connect('district.db')
cursor = conn.cursor()

(directory[directory_columns]
 .rename(dict(zip(directory.columns, directory.columns.str.lower())))
 .to_sql('directory',
         con=conn,
         if_exists='append',
         index=False,
         dtype=col_dtypes)
)

conn.close()

# %%
###############################################################################
# Write the state table to the database
# Kindof a stand-in script for the moment to populate the state table.
# Preferably, we'll put in state level data from ccd
###############################################################################

# Columns that are in the database directory table
directory_state_columns = ['FIPST', 'ST', 'STATENAME']

col_dtypes = {
    'fipst': 'INTEGER',
    'st': 'TEXT',
    'statename': 'TEXT'
}

# Creates a new database file if it doesn't exist
conn = sqlite3.connect('district.db')
cursor = conn.cursor()

(directory[directory_state_columns].drop_duplicates(subset='FIPST')
 .rename(dict(zip(directory.columns, directory.columns.str.lower())))
 .to_sql('state',
         con=conn,
         if_exists='append',
         index=False,
         dtype=col_dtypes)
)

conn.close()

# %%

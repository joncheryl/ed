'''
John Sherrill
Feb 1, 2025

Script to organize 2015-2024 directory data from the CCD nonfiscal datasets
and then to concat that with the prepped columns from whole dataframe (2014
and earlier). And then write the final dataframe to a SQLITE database.
'''
#%%
import sqlite3
import pandas as pd

PRE_PATH = "data/nonfiscal/directory/directory_"
files = {year: [f'{PRE_PATH}{year}.csv', '', ',', {}]
         for year in range(2015, 2025)}

# Some files/years are tab delimited encoding.
for year in [2015]:
    files[year][2] = '\t'

# Dictionary of dataframes for each year.
pre_directory = {year: pd.read_csv(file, encoding=format,
                                   sep=delim, dtype=kind) for
                  year, (file, format, delim, kind) in files.items()}

# Combine old and new directory data into one dataframe
dir_2015 = (
    pd.concat(pre_directory, names=['end_year'])
    .reset_index(level='end_year')
)
dir_1987 = (
    pd.read_csv("data/nonfiscal/directory/directory_through_2014.csv")
)
directory = pd.concat([dir_2015, dir_1987], ignore_index=True)

# Organize column names.
combos = {'ST': 'STABR',
          'STATENAME': 'STNAME',
          'SEA_NAME': 'SEANAME'}
for keep, destroy in combos.items():
    directory[keep] = directory[keep].combine_first(directory[destroy])
directory = directory.drop(columns=['STABR', 'SCHOOL_YEAR',
                                    'SURVYEAR', 'SEANAME',
                                    'STNAME'])

# Years 1990, 1994, 1995, 1996 had lower case statename.
directory['STATENAME'] = directory['STATENAME'].str.upper()

# Year 2000 had underscores in statename. Eg. 'NORTH_CAROLINA
directory['STATENAME'] = directory['STATENAME'].str.replace('_', ' ')

# Years 2012, 2013, 2014 had stabr instead of statename in statename column.
stabr_statename_mapping = (
    directory.loc[directory['ST'] != directory['STATENAME'],
                  ['ST', 'STATENAME']]
    .drop_duplicates()
    .set_index('ST')['STATENAME']
    .to_dict()
)

directory['STATENAME'] = (
    directory['STATENAME']
    .map(stabr_statename_mapping)
    .combine_first(directory['STATENAME'])
    )

# STATE_AGENCY_NO doesn't seem to have any information so drop it.
directory = directory.drop(columns=['STATE_AGENCY_NO'])

# Properly assign type for columns that should be int.
int_columns = ['MZIP', 'MZIP4', 'LZIP', 'LZIP4', 'ZIP', 'ZIP4',
               'OPERATIONAL_SCHOOLS', 'OPERATIONAL_LEAS']
directory = directory.astype({column: 'Int64' for column in int_columns})

# Make columns names lowercase.
directory = directory.rename(columns=str.lower)

# %%
###############################################################################
# Write the directory table to the database
###############################################################################

# Make a mapping from python/pandas dtypes to SQLITE dtypes
type_map = {'object': 'TEXT',
            'int64': 'INTEGER',
            'Int64': 'INTEGER'}
col_dtypes = directory.dtypes.map(lambda x: type_map.get(str(x)))

# Creates a new database file if it doesn't exist
conn = sqlite3.connect('data/state.db')
cursor = conn.cursor()

directory.to_sql('directory',
                 con=conn,
                 if_exists='replace',
                 index=False,
                 dtype=col_dtypes.to_dict())

conn.close()

# %%

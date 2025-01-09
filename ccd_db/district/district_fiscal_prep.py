'''
John Sherrill
Jan 1, 2025

Script to organize fiscal data from district level CCD datasets.
'''
#%%
import sqlite3
from io import StringIO
import pandas as pd

PRE_PATH = "data/fiscal/fiscal_"

base_cats = {'CENSUSID': 'category',
             'CONUM': 'string',
             'GSHI': 'category',
             'LEAID': str,
             'AGCHRT': 'category'}

files = {year: [f'{PRE_PATH}{year}.csv', '', '\t', base_cats, None]
         for year in [1990, 1992] + list(range(1995, 2023))}

# some files/years have a windows/cp1252 encoding
for year in [2021]:
    files[year][1] = 'cp1251'

# In the year 1992, there was no header, so we're gonna do it manually.
# Record layout accessed on Jan 1, 2025:
with open("data/fiscal/sdf921alay.txt", 'r', encoding='cp1251') as file:
    # Filter lines with leading tabs
    cleaned_lines = (line for line in file if not line.startswith('\t'))
    cleaned_buffer = StringIO(''.join(cleaned_lines))

special_header = pd.read_csv(cleaned_buffer, sep=r'\s+', skiprows=6,
                             usecols=[0,1,2,3]).loc[:, 'Name']

for year in [1992]:
    files[year][4] = special_header

pre_fiscal = {year: pd.read_csv(file, encoding=format, sep=delim, dtype=kind,
                                names=top, na_values={'LEAID': 'N',
                                                      'SCHLEV': 'M',
                                                      'CENSUSID': 'N',
                                                      'AGCHRT': 'N'}) for
                  year, (file, format, delim, kind, top) in files.items()}

fiscal = (pd.
              concat(pre_fiscal, names=['END_YEAR', 'fdsa'])
              .reset_index(level='END_YEAR')
              .reset_index(drop=True)
)

#%%
###############################################################################
# Clean up the LEAIDs
###############################################################################

# There are many rows where the LEAID is NA but some of those have LEAID in
# other years so we're gonna back propagate if possible.

fiscal['NAME'] = fiscal['NAME'].str.upper()
fiscal['LEAID'] = fiscal['LEAID'].replace({'M': pd.NA})

fiscal['LEAID'] = (
    fiscal
    .groupby('NAME')['LEAID']
    .transform(lambda x: x.ffill().bfill())
)

# I'm nervous about non-unique CENSUSID values to use the above method
# chain to fill in things. But not so nervous that I won't use it
# to fill in already missing values.
spare_id = fiscal.loc[fiscal['LEAID'].isna(), 'CENSUSID']

fdsa = (
    fiscal
    .loc[fiscal['CENSUSID'].isin(spare_id), ['LEAID', 'CENSUSID']]
    .dropna()
    .drop_duplicates()
)

fiscal['LEAID'] = (
    fiscal['LEAID']
    .fillna(
        fiscal['CENSUSID']
        .map(fdsa.set_index('CENSUSID')['LEAID'].to_dict())
        )
)

# Use this again to fill in any more holes.
fiscal['LEAID'] = (
    fiscal
    .groupby('NAME')['LEAID']
    .transform(lambda x: x.ffill().bfill())
)

# After the above filling in of LEAID, for each year, less that 1% of LEAID
# entries are NA and less than 0.1% of LEAIDs are NA. So we're just gonna
# drop the missing NAs here. BTW, Illinois is the biggest problem. Then
# Minnesota.

fiscal = fiscal.dropna(subset='LEAID').drop(columns='YEAR')

#%%
###############################################################################
# Make dtypes flattering
###############################################################################

# It looks like for 2022 there are ~354 columns and there are ~394 in the
# fiscal dataframe so not too far off. In fact, after reviewing the columns
# it doesn't look like any should probably be combined or dropped.
# FOR THE FIRST TIME IN THE HISTORY OF MY EXPLORATION WITH CCD
# THE FUCKING NAMES ARE CONSISTENT ACROSS TIME!

# - drop STNAME, STABBR, GSLO, GSHI, AGCHRT, SCHLEV
# - flags should be categories
# - floats should probably be Int64
# - watch out for negative numbers. Those should be NAs. -1 means missing
# and -2 means not applicable (which is flagged as such in corresponding flag
# column). Presumably, the other negative numbers: -3, -1000, -11000, -34000
# are typos.

float_cols = fiscal.dtypes[fiscal.dtypes == float].index
int_cols = fiscal.dtypes[fiscal.dtypes == int].index
true_float_cols = ['V12', 'V14', 'V16', 'V18', 'V20',
                   'V22', 'V24', 'V26','V28']
id_cols = ['LEAID', 'CENSUSID']
flag_cols = fiscal.columns[fiscal.columns.str.contains('FL_')]

types = {col: 'Int64' for col in float_cols.union(int_cols)}
types.update({col: float for col in true_float_cols})
types.update({col: 'category' for col in id_cols})
types.update({col: 'category' for col in flag_cols})

fiscal = (
    fiscal
    .drop(columns=['STNAME', 'STABBR', 'GSLO', 'GSHI', 'AGCHRT', 'CONUM',
                   'CMSA', 'SCHLEV'])
    .astype(types)
)

# Replace negative numbers with NA
fiscal[fiscal.select_dtypes(include='number').columns] = (
    fiscal[fiscal.select_dtypes(include='number').columns]
    .select_dtypes(include='number')
    .where(fiscal[fiscal.select_dtypes(include='number').columns] >= 0, pd.NA)
)

# %%
###############################################################################
# Write the fiscal table to the database
###############################################################################

# Have to rename the NAME column and WEIGHT column so they don't interfere with
# SQL keywords.
fiscal = fiscal.rename(columns = {'NAME': 'DISTRICT_NAME',
                                  'WEIGHT': 'DISTRICT_WEIGHT'})

# Create the correct sqlite3 datatype mapping for columns
cat_cols = fiscal.dtypes.loc[fiscal.dtypes == 'category']
object_cols = fiscal.dtypes.loc[fiscal.dtypes == 'object']
int64_cols = fiscal.dtypes.loc[fiscal.dtypes == 'Int64']
float_cols = fiscal.dtypes.loc[fiscal.dtypes == float]

col_dtypes = {
    **{col: 'TEXT' for col in cat_cols.index},
    **{col: 'TEXT' for col in object_cols.index},
    **{col: 'INTEGER' for col in int64_cols.index},
    **{col: 'REAL' for col in float_cols.index}
}

#%%
# Creates a new database file if it doesn't exist
conn = sqlite3.connect('district.db')
cursor = conn.cursor()

(fiscal
 .rename(dict(zip(fiscal.columns, fiscal.columns.str.lower())))
 .to_sql('fiscal',
         con=conn,
         if_exists='append',
         index=False,
         dtype=col_dtypes)
)

conn.close()

# %%

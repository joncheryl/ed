'''
John Sherrill
Feb 9, 2025

Script to organize fiscal data from state level CCD datasets.
'''
#%%
import sqlite3
import os
import pandas as pd

PRE_PATH = "ccd_db/state"
PRE_PATH_DATA = PRE_PATH + "/data/fiscal/fiscal_"

#%%
###############################################################################
# Import fwf encoded data
###############################################################################

layout = pd.read_csv('ccd_db/state/data/fiscal/layouts/layouts.csv')
years_fwf = layout['end_year'].unique()

files_fwf = {year: [f'{PRE_PATH_DATA}{year}.csv',
                    list(zip(layout.loc[layout['end_year'] == year, 'start']-1,
                        layout.loc[layout['end_year'] == year, 'end'])),
                    layout.loc[layout['end_year'] == year, 'variable']]
             for year in years_fwf}

# The comment='.' below is for a bad line at the end of end_year=1987 file.
pre_fiscal = {year: pd.read_fwf(file,
                                    colspecs=start_end,
                                    encoding='cp1252',
                                    names=variables,
                                    comment='.') for
                  year, (file, start_end, variables) in files_fwf.items()}

# There was a big change in 1989 so converting column names via a crosswalk
# for end_year 1987 and 1988.
fiscal_cross = pd.read_csv('ccd_db/state/fiscal_var_crosswalk.csv').dropna()
crosswalk_dict = dict(zip(fiscal_cross['old_var'],
                          fiscal_cross['new_var']))
pre_fiscal[1987] = pre_fiscal[1987].rename(columns=crosswalk_dict)
pre_fiscal[1988] = pre_fiscal[1988].rename(columns=crosswalk_dict)

#%%
###############################################################################
# Import csv encoded data
###############################################################################
years_csv = [year for year in range(1987, 2023) if year not in years_fwf]
files_csv = {year: f'{PRE_PATH_DATA}{year}.csv' for year in years_csv}

# The csv file for end_year=2016 has a bunch of trailing tabs on the final
# line which causes an error. Removing them here.
FILE_PATH = PRE_PATH_DATA + '2016.csv'
TEMP_FILE = FILE_PATH + '.tmp'
with open(FILE_PATH, 'r', encoding='utf-8') as infile, \
    open(TEMP_FILE, 'w', encoding='utf-8') as outfile:
    # Just looking to chop off extra tabs at the end of lines...
    outfile.writelines(line.rstrip() + '\n' for line in infile)
    os.replace(TEMP_FILE, FILE_PATH)

pre_fiscal_csv = {year: pd.read_csv(file, sep='\t') for
                  year, file in files_csv.items()}
pre_fiscal.update(pre_fiscal_csv)

# For end_year=2002 through 2004, there were unfortunately lower-case letters
# in the column names. Convert all column names for these years to upper-case.
for year in range(2002, 2005):
    pre_fiscal[year].columns = pre_fiscal[year].columns.str.upper()

# Put data from all years together into one dataframe.
fiscal = (
    pd.concat(pre_fiscal, names=['end_year'])
    .reset_index(level='end_year')
    .reset_index(drop=True)
    .drop(columns='SURVYEAR')
)

#%%
###############################################################################
# Organize column names and data types.
###############################################################################

# Put together columns FIPS AND STFIPS, rename to FIPST and drop STFIPS.
fiscal.insert(1, 'FIPST', (
    fiscal['FIPS']
    .combine_first(fiscal['STFIPS'])
    .astype(int))
)
fiscal = fiscal.drop(columns=['FIPS', 'STFIPS'])

# Put together columns STNAME and NAME. Drop NAME.
fiscal['STNAME'] = fiscal['STNAME'].combine_first(fiscal['NAME'])
fiscal = fiscal.drop(columns=['NAME'])

# Drop all the MEMBER columns. Have that data already in the membership table.
mem_cols = fiscal.columns[fiscal.columns.str.match(r'.*MEMB.*')]
fiscal = fiscal.drop(columns=mem_cols)

# Need to coerce all the non-numeric values in the numeric columns (eg 'R1A')
float_cols = [
    'R1A', 'R1B', 'R1C', 'R1D', 'R1E', 'R1F', 'R1G', 'R1H', 'R1I', 'R1J',
    'R1K', 'R1L', 'R1M', 'R1N', 'STR1', 'R2', 'R3', 'R4A', 'R4B', 'R4C',
    'R4D', 'STR4', 'R5', 'TR', 'E11', 'E12', 'E13', 'E14', 'E15', 'E16',
    'E17', 'E18', 'STE1', 'E212', 'E213', 'E214', 'E215', 'E216', 'E217',
    'E218', 'TE21', 'E222', 'E223', 'E224', 'E225', 'E226', 'E227', 'E228',
    'TE22', 'E232', 'E233', 'E234', 'E235', 'E236', 'E237', 'E238', 'TE23',
    'E242', 'E243', 'E244', 'E245', 'E246', 'E247', 'E248', 'TE24', 'E252',
    'E253', 'E254', 'E255', 'E256', 'E257', 'E258', 'TE25', 'E262', 'E263',
    'E264', 'E265', 'E266', 'E267', 'E268', 'TE26', 'STE22', 'STE23', 'STE24',
    'STE25', 'STE26', 'STE27', 'STE28', 'STE2T', 'E3A11', 'E3A12', 'E3A13',
    'E3A14', 'E3A2', 'E3A16', 'E3A1', 'E3B11', 'E3B12', 'E3B13', 'E3B14',
    'E3B2', 'E3B16', 'E3B1', 'STE3', 'E4A1', 'E4A2', 'E4B1', 'E4B2', 'E4C1',
    'E4C2', 'E4D', 'E4E1', 'E4E2', 'STE4', 'TE5', 'E61', 'E62', 'E63', 'STE6',
    'E7A1', 'E7A2', 'STE7', 'E81', 'E82', 'E9A', 'E9B', 'E9C', 'E9D', 'E91',
    'STE9', 'TE10', 'TE11', 'X12C', 'X12D', 'X12E', 'X12F', 'TX12', 'NCE13',
    'ADA', 'A14A', 'A14B', 'E611', 'E612', 'E62A', 'E62B', 'R__01', 'R__02',
    'R__03', 'R__04', 'T__01', 'E__01', 'E__02', 'T__02', 'E__03', 'E__04',
    'T__03', 'E__05', 'E__06', 'T__04', 'T__05', 'T__06', 'T__07', 'X__01',
    'X__02', 'T__08', 'X__03', 'X__04', 'T__09', 'T__12', 'T__13', 'T__14',
    'C__01', 'C__02', 'T__15', 'T__16', 'F__01', 'F__03', 'T__27', 'F__02',
    'F__04', 'T__28', 'T__25', 'T__26', 'T__29', 'X__05', 'X__06', 'T__10',
    'R__01R', 'R__03R', 'R__04R', 'T__01R']

# I don't want to reformat the above list because it could likely change. So
# just gonna subtract elements from it here.
not_cols = ['R__01', 'R__02', 'R__03', 'R__04', 'T__01', 'T__02', 'T__03',
            'T__07']
float_cols = [x for x in float_cols if x not in not_cols]
fiscal[float_cols] = (
    fiscal[float_cols]
    .apply(lambda col: pd.to_numeric(col, errors='coerce'))
)

# Truncate (not round) all values to make all dtypes int.
numeric_cols = fiscal.select_dtypes(include='number', exclude=int)
fiscal = fiscal.astype({col: 'Int64' for col in numeric_cols})

# Replace negative values with pd.NA
fiscal[fiscal.select_dtypes('number').columns] = (
    fiscal.select_dtypes('number').where(lambda x: x >= 0, pd.NA)
)

#%%
###############################################################################
# Write the fiscal table to the database
###############################################################################

# Create the correct sqlite3 datatype mapping for columns
object_cols = fiscal.dtypes.loc[fiscal.dtypes == 'object']
int_cols = fiscal.dtypes.loc[fiscal.dtypes == int]
int64_cols = fiscal.dtypes.loc[fiscal.dtypes == 'Int64']

col_dtypes = {
    **{col: 'TEXT' for col in object_cols.index},
    **{col: 'INTEGER' for col in int_cols.index},
    **{col: 'INTEGER' for col in int64_cols.index}
}

# Creates a new database file if it doesn't exist
conn = sqlite3.connect(PRE_PATH + 'data/state.db')
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

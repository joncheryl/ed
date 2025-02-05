'''
John Sherrill
Feb 1, 2025

Script to organize 2015-2024 staff data from the CCD nonfiscal datasets
and then to concat that with the prepped columns from whole dataframe (2014
and earlier). Then write the final dataframe to a sqlite database.
'''
#%%
import sqlite3
import pandas as pd
import numpy as np

PRE_PATH = "data/nonfiscal/staff/staff_"
files = {year: [f'{PRE_PATH}{year}.csv', '', ',', {}]
         for year in range(2015, 2025)}

# some files/years are tab delimited encoding
for year in [2015]:
    files[year][2] = '\t'

pre_staff = {year: pd.read_csv(file, encoding=format,
                               sep=delim, dtype=kind) for
             year, (file, format, delim, kind) in files.items()}

#%%
# "Revised for 2019-20: changed
# Student Support Services Staff to Student Support Services Staff
# (w/o Psychology); added School Psychologists"

# Will need to sum STUSUPWOPSYCH and SCHPSYCH for 2017 on to get
# STUSUP

# Pivot new staff dataframes to wide format. Long is dumb and will likely
# make visualizations and further analysis a pain in the ass.
# Maybe do this before dealing with student support category above.

pivot_dict = {
    'Elementary School Counselors':'ELMGUI',
    'Secondary School Counselors': 'SECGUI',
    'School Counselors': 'GUI',
    'Pre-kindergarten Teachers': 'PKTCH',
    'Kindergarten Teachers': 'KGTCH',
    'Elementary Teachers': 'ELMTCH',
    'Secondary Teachers': 'SECTCH',
    'Ungraded Teachers': 'UGTCH',
    'School Administrators': 'SCHADM',
    'School administrators': 'SCHADM',
    'School Administrative Support Staff': 'SCHSUP',
    'Paraprofessionals/Instructional Aides': 'PARA',
    'Librarians/media specialists': 'LIBSPE',
    'Library/Media Support Staff': 'LIBSUP',
    'Student Support Services Staff': 'STUSUP',
    'Student Support Services Staff (w/o Psychology)': 'STUSUPWOPSYCH',
    'School Psychologists': 'SCHPSYCH',
    'LEA Administrators': 'LEAADM',
    'LEA Administrative Support Staff': 'LEASUP',
    'Instructional Coordinators and Supervisors to the Staff': 'CORSUP',
    'All Other Support Staff': 'OTHSUP',
    'Missing': 'MISSING',
    'Teachers': 'TOTTCH',
    'Guidance Counselors': 'TOTGUI',
    'School Staff': 'SCHSTA',
    'LEA Staff': 'LEASTA',
    'Other Staff': 'OTHSTA',
    'No Category Codes': 'TOTAL'
}

# We have to change the names of the STAFF column for 2017-2023
for year in range(2017, 2025):
    pre_staff[year]['STAFF'] = (
        pre_staff[year]['STAFF'].map(pivot_dict)
    )

# Then convert 2017-2023 from long to wide
for  year in range(2017, 2025):
    pre_staff[year] = (
        pre_staff[year]
        .pivot(index=list(pre_staff[year].columns[1:6]),
               columns='STAFF',
               values='STAFF_COUNT')
        .reset_index()
    )

# Combine all years together
staff = (
    pd.concat(pre_staff, names=['end_year'])
    .reset_index(level='end_year')
)

# Clean up columns
staff['ST'] = staff['ST'].combine_first(staff['STABR'])
staff['TOTAL'] = staff['TOTAL'].combine_first(staff['STAFF'])
staff['SEA_NAME'] = staff['SEA_NAME'].combine_first(staff['SEANAME'])

staff = staff.drop(columns=['SURVYEAR', 'STABR', 'STAFF', 'SEANAME',
                            'STATE_AGENCY_NO', 'MISSING'])

# Fill in any missing values appropriately
staff = staff.assign(
    STUSUP=lambda x: np.where(x['end_year'] > 2019,
                              x['STUSUPWOPSYCH'] + x['SCHPSYCH'],
                              x['STUSUP'])
)

# Concat with 2014 and earlier data
staff = pd.concat([staff,
                   pd.read_csv("data/nonfiscal/staff/staff_through_2014.csv",
                               na_values=['M', 'N', '.'])
                   ],
                  ignore_index=True)

# Make columns names lowercase.
staff = staff.rename(columns=str.lower)

# %%
###############################################################################
# Write the staff table to the database
###############################################################################

# Make a mapping from python/pandas dtypes to SQLITE dtypes
type_map = {'object': 'TEXT',
            'int': 'INTEGER',
            'Int64': 'INTEGER',
            'float': 'REAL'}
col_dtypes = dict(zip(staff.dtypes.index,
                      staff.dtypes.map(type_map)))

# Connect to database and append to created table.
conn = sqlite3.connect('data/state.db')
cursor = conn.cursor()

staff.to_sql('staff',
              con=conn,
              if_exists='append',
              index=False,
              dtype=col_dtypes)

conn.close()

# %%

'''
John Sherrill
Feb 7, 2025

Script to organize 2015-2024 membership data from the CCD nonfiscal datasets
and then to concat that with the prepped columns from whole dataframe (2014
and earlier). Then write the final dataframe to a sqlite database.

The IAMEMPUP/ZZZZZ variable I guess turned into being more specific in 2017
and later. In years 2016/2015 it is flagged for the entire district.

TODO:
- Doesn't put any info into the dms_flag column. No distinguishing between
missing values and not-applicable values. Could use some code like:
# Take flags in student counts (negative numbers) and convert
dms_col = pd.Series('Reported', index=pre_membership[year].index)
dms_col[pre_membership[year]['STUDENT_COUNT'] == -1] = 'Missing'
dms_col[pre_membership[year]['STUDENT_COUNT'] == -2] = 'Not applicable'
pre_membership[year]['DMS_FLAG'] = dms_col

# Place nan values where student counts are negative or strings.
pre_membership[year].loc[pre_membership[year]['STUDENT_COUNT'] < 0,
                         'STUDENT_COUNT'] = None
pre_membership[year]['STUDENT_COUNT'] = \
    pd.to_numeric(pre_membership[year]['STUDENT_COUNT'], errors='coerce')

'''

# %%
import re
import sqlite3
import pandas as pd
import numpy as np # just for a few np.where uses

PRE_PATH = "data/nonfiscal/membership/membership_"
files = {year: [f'{PRE_PATH}{year}.csv', '', ',', {}]
         for year in range(2015, 2025)}

# some files/years are tab delimited encoding
for year in [2015]:
    files[year][2] = '\t'

# Dictionary of dataframes for 2015-2024
pre_membership = {year: pd.read_csv(file,
                                    encoding=format,
                                    sep=delim,
                                    dtype=kind) for
                  year, (file, format, delim, kind) in files.items()}

# Add a proper end_year column.
for year in range(2015, 2025):
    pre_membership[year].insert(0, 'end_year', year)
    YEAR_COLUMN = (
        'SCHOOL_YEAR' if 'SCHOOL_YEAR' in pre_membership[year].columns 
        else 'SURVYEAR'
        )
    pre_membership[year] = pre_membership[year].drop(columns=YEAR_COLUMN)

###############################################################################
# Put together wide format years.
###############################################################################
# add files/years from 1987 to 2014
membership_wide = pd.read_csv('data/nonfiscal/membership/' + \
                              'membership_through_2014.csv',
                              na_values=['N', 'M', -1, -2])

# Adding end_year=2015 and 2016 because they're in wide format.
membership_wide = pd.concat([membership_wide,
                             pre_membership[2015],
                             pre_membership[2016]])

# Replace negative numbers with np.nan and make columns Int64
num_cols = membership_wide.select_dtypes(include='float').columns
membership_wide[num_cols] = (
    membership_wide[num_cols]
    .where(membership_wide[num_cols] >= 0, np.nan)
    .astype('Int64')
)

# Drop columns we don't need anymore.
membership_wide = membership_wide.drop(
    columns=[col for col in membership_wide.columns if re.match(r'^I', col)]
).drop(
    columns=['STABR', 'STATENAME', 'SEANAME']
)

###############################################################################
# Making column naming consistent with aggregtion across years.
###############################################################################
membership_wide = (
    membership_wide
    .rename(columns=lambda col: re.sub(r'G(\d{2})', r'XX\1X', col))
)
# Do similarly for column names of form AB99 -> AB99X. All columns with names
# of length=4 are over form AB99.
membership_wide = (
    membership_wide
    .rename(columns=lambda col: re.sub(r'^(.{4})$', r'\1X', col))
)
# Manually rename these columns
races = ['AM', 'AS', 'HI', 'BL', 'WH', 'HP', 'TR']
membership_wide = (
    membership_wide
    .rename(columns={'PK': 'XXPKX',
                     'KG': 'XXKGX',
                     'UG': 'XXUGX',
                     'AE': 'XXAEX',
                     'MEMBER': 'YYYYY',
                     'TOTAL': 'XXXXX'})
    .rename(columns=dict(zip(races, [race + 'XXX' for race in races])))
)

###############################################################################
# Converting all years to long format.
###############################################################################
id_cols = ['end_year', 'FIPST', 'RACECAT']
value_cols = [col for col in membership_wide.columns if col not in id_cols]

membership_wide = (
    membership_wide
    .melt(id_vars=id_cols,
          value_vars=value_cols,
          value_name='STUDENT_COUNT')
)

# correctly label the id variables
membership_wide['RACE_ETHNICITY'] = (
    membership_wide['variable'].str[:2]
    .map({'AM': 'American Indian or Alaska Native',
          'AS': 'Asian',
          'HI': 'Native Hawaiian or Other Pacific Islander',
          'BL': 'Black or African American',
          'WH': 'White',
          'HP': 'Hispanic/Latino',
          'TR': 'Two or more races',
          'XX': 'Aggregation',
          'YY': 'Aggregation Less AE',
          'ZZ': 'IAMEMPUP'})
)
membership_wide['GRADE'] = (
    membership_wide['variable'].str[2:4]
    .map({'01': 'Grade 1',
          '02': 'Grade 2',
          '03': 'Grade 3',
          '04': 'Grade 4',
          '05': 'Grade 5',
          '06': 'Grade 6',
          '07': 'Grade 7',
          '08': 'Grade 8',
          '09': 'Grade 9',
          '10': 'Grade 10',
          '11': 'Grade 11',
          '12': 'Grade 12',
          '13': 'Grade 13',
          'KG': 'Kindergarten',
          'PK': 'Pre-Kindergarten',
          'UG': 'Ungraded',
          'AE': 'Adult Education',
          'AL': 'Aggregation',
          'XX': 'Aggregation',
          'YY': 'Aggregation Less AE',
          'ZZ': 'IAMEMPUP'})
)
membership_wide['SEX'] = (
    membership_wide['variable'].str[4:]
    .map({'F': 'Female',
          'M': 'Male',
          'X': 'Aggregation',
          'Y': 'Aggregation Less AE',
          'Z': 'IAMEMPUP'})
)
# Don't need this column anymore.
membership_wide = membership_wide.drop(columns=['variable'])

# Create flags for the total district enrollment (includes adult education)
mask_aggregation = (
    (membership_wide.loc[:, 'RACE_ETHNICITY'] == "Aggregation") &
    (membership_wide.loc[:, 'GRADE'] == "Aggregation") &
    (membership_wide.loc[:, 'SEX'] == "Aggregation")
)
mask_aggregation_less_ae = (
    (membership_wide.loc[:, 'RACE_ETHNICITY'] == "Aggregation Less AE") &
    (membership_wide.loc[:, 'GRADE'] == "Aggregation Less AE") &
    (membership_wide.loc[:, 'SEX'] == "Aggregation Less AE")
)
result = pd.Series("Running out of steam...",
                    index=membership_wide.index)
result[mask_aggregation] = "Education Unit Total"
result[mask_aggregation_less_ae] = (
    "Derived - Education Unit Total minus Adult Education Count"
)
membership_wide['TOTAL_INDICATOR'] = result

###############################################################################
# Combine all years and remove unneeded rows and columns
###############################################################################
membership = (
    pd.concat(
        [membership_wide] + [pre_membership[year] for year in range(2017,2025)]
        )
    .dropna(subset=['STUDENT_COUNT'])
    .drop(columns = ['STATENAME',
                     'ST',
                     'SEA_NAME',
                     'STATE_AGENCY_NO'])
    .astype({'STUDENT_COUNT': 'Int64'})
)

# Make columns names lowercase.
membership = membership.rename(columns=str.lower)

# %%
############################################################################
# Write the membership table to the database
############################################################################

# Make a mapping from python/pandas dtypes to SQLITE dtypes
type_map = {'object': 'TEXT',
            'int64': 'INTEGER',
            'Int64': 'INTEGER',
            'float': 'REAL'}
col_dtypes = membership.dtypes.map(lambda x: type_map.get(str(x)))

# Connect to database and append to created table.
conn = sqlite3.connect('data/state.db')
cursor = conn.cursor()

membership.to_sql('membership',
              con=conn,
              if_exists='append',
              index=False,
              dtype=col_dtypes.to_dict())

conn.close()

#%%

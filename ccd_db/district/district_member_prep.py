'''
John Sherrill
Dec 2, 2024

Script to prepare membership files for import into a sqlite database.

NOTES:

These are the columns for 22/23 (2023 fiscal year):

0   SCHOOL_YEAR      object 
1   FIPST            int64  
2   STATENAME        object 
3   ST               object 
4   LEA_NAME         object 
5   STATE_AGENCY_NO  int64  
6   UNION            float64
7   ST_LEAID         object 
8   LEAID            int64  
9   GRADE            object 
10  RACE_ETHNICITY   object 
11  SEX              object 
12  STUDENT_COUNT    float64
13  TOTAL_INDICATOR  object 
14  DMS_FLAG         object 

Type conversions:
- SCHOOL_YEAR object -> int (such that ending year is used 1993/1994 -> 1994)
- STATENAME object -> category
- ST object -> category
- GRADE -> category
- RACE_ETHNICITY -> category
- SEX -> category
- TOTAL_INDICATOR -> category
- DMS_FLAG -> category

For the categorical columns, use a TEXT type and a check constraint like this:
CREATE TABLE membership (
    id INTEGER PRIMARY KEY,
    leaid TEXT,
    race TEXT CHECK(category IN ('red', 'yellow', 'black', 'white'))
);
Since each file is it's own year, SCHOOL_YEAR should be dropped and replaced
with a different column with the ending year called END_YEAR.

STATENAME, ST, and FIPST are (should be) redundant with the info in LEAID
so drop them all.

Probably need to check all the combos of grade/race/sex. Don't drop but don't
use the combos.

'''

# %%
import sqlite3
import pandas as pd
import numpy as np # just for a few np.where uses

PRE_PATH = "data/nonfiscal/membership/membership_"
files = {year: [f'{PRE_PATH}{year}.csv', '', ',', {}]
         for year in range(2015, 2024)}

# some files/years have a windows/cp1252 encoding
for year in [2021]:
    files[year][1] = 'cp1251'

# some files/years are tab delimited encoding
for year in [2015]:
    files[year][2] = '\t'

# some files/years need better dtype-ing
for year in [2016, 2015]:
    files[year][3] = {'ST_LEAID': object}

pre_membership = {year: pd.read_csv(file,
                                    encoding=format,
                                    sep=delim,
                                    dtype=kind) for
                  year, (file, format, delim, kind) in files.items()}

# add files/years from 1987 to 2014
membership_2014 = pd.read_csv('data/nonfiscal/memberhsip/' + \
                              'membership_through_2014.csv',
                              dtype={'ST_LEAID': 'string',
                                     'UG': 'Int64',
                                     'MEMBER': 'Int64',
                                     'IAMEMPUP': 'category'},
                              na_values={'UG':['N', 'M'],
                                         'MEMBER': ['N', 'M']})

pre_membership.update({x: membership_2014.loc[membership_2014['END_YEAR'] == x,
                                              :].copy()
        for x in range(1987, 2015)})

# add proper END_YEAR column to 2015/2016
for year in range(2015, 2017):
    pre_membership[year]['END_YEAR'] = year

# concat-ing so that the indexing for the columns is consistent for the purpose
# of the prep work below
pre_2016 = pd.concat([pre_membership[x] for x in range(2016, 1986, -1)])
pre_membership.update({x: pre_2016.loc[pre_2016['END_YEAR'] == x, :].copy()
        for x in range(1987, 2015)})

#%%
###############################################################################
# This cell preps the 1987-2014 and 2015-2016 years
###############################################################################

# Some years had different column names. We're gonna use the names from the
# most recent year (2023).
for year in range(1987, 2017):
    pre_membership[year].rename(columns={'STABR': 'ST',
                                         'SURVYEAR': 'SCHOOL_YEAR',
                                         'PK': 'XXPKX',
                                         'KG': 'XXKGX',
                                         'G01': 'XX01X',
                                         'G02': 'XX02X',
                                         'G03': 'XX03X',
                                         'G04': 'XX04X',
                                         'G05': 'XX05X',
                                         'G06': 'XX06X',
                                         'G07': 'XX07X',
                                         'G08': 'XX08X',
                                         'G09': 'XX09X',
                                         'G10': 'XX10X',
                                         'G11': 'XX11X',
                                         'G12': 'XX12X',
                                         'G13': 'XX13X',
                                         'UG': 'XXUGX',
                                         'AE': 'XXAEX',
                                         'AM': 'AMXXX',
                                         'AS': 'ASXXX',
                                         'HI': 'HIXXX',
                                         'BL': 'BLXXX',
                                         'WH': 'WHXXX',
                                         'HP': 'HPXXX',
                                         'TR': 'TRXXX',
                                         'TOTAL': 'XXXXX',
                                         'MEMBER': 'YYYYY',
                                         'IAMEMPUP': 'ZZZZZ'},
                                inplace=True)

    # Convert from wide to long format.
    pre_membership[year] = (
        pre_membership[year].
        melt(id_vars=pre_membership[year].columns[0:8],
             value_vars=pre_membership[year].columns[8:-2],
             value_name='STUDENT_COUNT')
    )

    # correctly label the id variables
    pre_membership[year]['RACE_ETHNICITY'] = pre_membership[year] \
        ['variable'].str[:2].map({'AM': 'American Indian or Alaska Native',
                                  'AS': 'Asian',
                                  'HI': 'Native Hawaiian or' + \
                                      'Other Pacific Islander',
                                  'BL': 'Black or African American',
                                  'WH': 'White',
                                  'HP': 'Hispanic/Latino',
                                  'TR': 'Two or more races',
                                  'XX': 'Aggregation',
                                  'YY': 'Aggregation Less AE',
                                  'ZZ': 'IAMEMPUP'})
    pre_membership[year]['GRADE'] = pre_membership[year] \
        ['variable'].str[2:4].map({'01': 'Grade 1',
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
                                   'ZZ': 'IAMEMPUP'
                                   })
    pre_membership[year]['SEX'] = pre_membership[year] \
        ['variable'].str[4:].map({'F': 'Female',
                                  'M': 'Male',
                                  'X': 'Aggregation',
                                  'Y': 'Aggregation Less AE',
                                  'Z': 'IAMEMPUP'})

    # Create flags for the total district enrollment (includes adult education)
    mask_aggregation = (
        (pre_membership[year].iloc[:, 0] == "Aggregation") &
        (pre_membership[year].iloc[:, 1] == "Aggregation") &
        (pre_membership[year].iloc[:, 2] == "Aggregation")
    )
    mask_aggregation_less_ae = (
        (pre_membership[year].iloc[:, 0] == "Aggregation Less AE") &
        (pre_membership[year].iloc[:, 1] == "Aggregation Less AE") &
        (pre_membership[year].iloc[:, 2] == "Aggregation Less AE")
    )
    result = pd.Series("Running out of steam...",
                       index=pre_membership[year].index)
    result[mask_aggregation] = "Education Unit Total"
    result[mask_aggregation_less_ae] = (
        "Derived - Education Unit Total minus Adult Education Count"
    )
    pre_membership[year]['TOTAL_INDICATOR'] = result

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

    # Drop the no longer necessary columns
    pre_membership[year] = pre_membership[year].drop(columns=['SEANAME',
                                                              'variable'])

# %%
# Really this is just a continuation of the above but was being
# done below on the entire membership dataframe. Putting it here,
# doing it on each year, should help with memory management.

cat_cols = ['GRADE', 'RACE_ETHNICITY', 'SEX', 'TOTAL_INDICATOR', 'DMS_FLAG']

for year in range(1987, 2024):
    pre_membership[year] = (pre_membership[year]
          .drop(columns=['SCHOOL_YEAR', 'ST', 'STATENAME'])
          .assign(
              GRADE=lambda x: np.where(
                  x['TOTAL_INDICATOR'] == 'Education Unit Total',
                  'TOTAL',
                  x['GRADE']),
              RACE_ETHNICITY=lambda x: np.where(
                  x['TOTAL_INDICATOR'] == 'Education Unit Total',
                  'TOTAL',
                  x['RACE_ETHNICITY']),
              SEX=lambda x: np.where(
                  x['TOTAL_INDICATOR'] == 'Education Unit Total',
                  'TOTAL',
                  x['SEX'])
          )
          .assign(
              GRADE=lambda x: np.where(
                  x['TOTAL_INDICATOR'] == 'Derived - Education Unit Total ' + \
                      'minus Adult Education Count',
                  'TOTAL less AE',
                  x['GRADE']),
              RACE_ETHNICITY=lambda x: np.where(
                  x['TOTAL_INDICATOR'] == 'Derived - Education Unit Total ' + \
                      'minus Adult Education Count',
                  'TOTAL less AE',
                  x['RACE_ETHNICITY']),
              SEX=lambda x: np.where(
                  x['TOTAL_INDICATOR'] == 'Derived - Education Unit Total ' + \
                      'minus Adult Education Count',
                  'TOTAL less AE',
                  x['SEX'])
          )
          .assign(
              GRADE=lambda x: np.where(
                  x['TOTAL_INDICATOR'] == 'Derived - Subtotal by ' + \
                      'Race/Ethnicity and Sex minus Adult Education Count',
                  'SUBTOTAL less AE',
                  x['GRADE'])
          )
          .assign(
              RACE_ETHNICITY=lambda x: np.where(
                  x['TOTAL_INDICATOR'] == 'Subtotal 4 - By Grade',
                  'SUBTOTAL',
                  x['RACE_ETHNICITY']),
              SEX=lambda x: np.where(
                  x['TOTAL_INDICATOR'] == 'Subtotal 4 - By Grade',
                  'SUBTOTAL',
                  x['SEX'])
          )
          .astype({'FIPST': int})
          .astype({col: 'category' for col in cat_cols})
          .dropna(subset=['STUDENT_COUNT'])
          # With the dropna the size of membership is 2.8+ GB
          # Without the dropna the size of membership is 11.2+ GB
    )

# %%
# This cell puts everything together into the final membership dataframe.

# TOTAL_INDICATOR column has the type of sum of the categories: including
# adult ed or not.

# The IAMEMPUP/ZZZZZ variable I guess turned into being more specific in 2017
# and later. In years 2016/2015 it is flagged for the entire district.

membership = (pd.
              concat(pre_membership,
                     names=['END_YEAR', 'fdsa'])
              .reset_index(level='END_YEAR')
              .reset_index(drop=True)
              .astype({col: 'category' for col in cat_cols})
              .rename(columns={'UNION': 'SU'})
              )

# The concat above was
            #   concat(pre_membership.dropna(subset=['STUDENT_COUNT']),
            #          names=['END_YEAR', 'fdsa'])

# could probably drop_na some observations from membership

# vcxz = membership.pivot(index=['END_YEAR', 'LEAID'],
#                         columns=['GRADE', 'RACE_ETHNICITY', 'SEX'],
#                         values=['STUDENT_COUNT',
#                                 'TOTAL_INDICATOR',
#                                 'DMS_FLAG'])

# %%
############################################################################
# Write the membership table to the database
############################################################################

# columns that are in the database table membership
membership_columns = ['END_YEAR', 'LEAID', 'STUDENT_COUNT',
                      'RACE_ETHNICITY', 'GRADE', 'SEX', 'TOTAL_INDICATOR',
                      'DMS_FLAG']

col_dtypes = {
    'end_year': 'INTEGER',
    'leaid': 'INTEGER',
    'student_count': 'INTEGER',
    'race_ethnicity': 'TEXT',
    'grade': 'TEXT',
    'sex': 'TEXT',
    'total_indicator': 'TEXT',
    'dms_flag': 'TEXT'}

# Creates a new database file if it doesn't exist
conn = sqlite3.connect('district.db')
cursor = conn.cursor()

(membership[membership_columns]
 .rename(dict(zip(membership.columns, membership.columns.str.lower())))
 .to_sql('membership',
         con=conn,
         if_exists='append',
         index=False,
         dtype=col_dtypes)
)

conn.close()

# %%
############################################################################
# I'm not using the below anymore.
#
# Make a sqlite database with just this smaller membership table
# and figure that out.
############################################################################

dtype_to_sql = {
    'object': 'TEXT',
    'int64': 'INTEGER',
    'float64': 'REAL',
    'category': 'TEXT'
}

# Convert DataFrame dtypes to SQL column definitions
TABLE_NAME = "membership_big"
columns = ["id INTEGER PRIMARY KEY"]
for col, dtype in membership.dtypes.items():
    # Default to TEXT if dtype is unknown
    sql_type = dtype_to_sql.get(str(dtype), "TEXT")
    columns.append(f"{col} {sql_type}")
# Create SQL CREATE TABLE statement
create_table_query = f"CREATE TABLE {TABLE_NAME} (\n    " + \
                        ",\n    ".join(columns) + "\n);"

# Creates a new database file if it doesn't exist
conn = sqlite3.connect('membership_test.db')
cursor = conn.cursor()

# Create the table
cursor.execute(create_table_query)
conn.commit()

# Write membership table to sql database
membership.to_sql('membership_big', conn, if_exists='append', index_label='id')

#%%

# Get the schema
cursor.execute("SELECT sql FROM sqlite_master WHERE type='table';")
schema = cursor.fetchall()

# Print the schema
for table_schema in schema:
    print(table_schema[0])

#%%

# maybe we want to get rid of this encoding?
with open('membership_schema.sql', 'r', encoding="utf-8") as sql_file:
    sql_script = sql_file.read()

cursor.executescript(sql_script)
conn.commit()

# conn.close()
#%%

## %%time
# CPU times: user 609 ms, sys: 363 ms, total: 971 ms
# Wall time: 993 ms
#
# average ratio returned = 1.353566
fdsa = (
    membership[['LEAID', 'SEX','STUDENT_COUNT']]
    .groupby(['LEAID', 'SEX'])
    .mean()
    .loc[lambda x: x['STUDENT_COUNT'] > 0]
    )

rewq = (
    fdsa.xs('Male', level='SEX')
    .div(fdsa.xs('Female', level='SEX'))
    # .mean()
)

rewq.mean()
#%%

###%%time
# CPU times: user 15.4 s, sys: 1.54 s, total: 17 s
# Wall time: 17.1 s
#
# # average ratio returned = 1.353566

cursor.execute('''
-- Step 1: Calculate the average score per sex and school_id
WITH AvgScorePerSex AS (
    SELECT
        LEAID,
        sex,
        AVG(student_count) AS avg_score
    FROM
        membership inner join sex_cats on membership.sex_id=sex_cats.sex_id
    GROUP BY
        LEAID, sex
),

-- Step 2: Calculate the male-to-female ratio per school
MaleFemaleRatio AS (
    SELECT
        m.leaid,
        m.avg_score AS male_avg_score,
        f.avg_score AS female_avg_score,
        m.avg_score / NULLIF(f.avg_score, 0) AS male_female_ratio
    FROM
        AvgScorePerSex m
    JOIN
        AvgScorePerSex f
    ON
        m.leaid = f.leaid
        AND m.sex = 'Male'
        AND f.sex = 'Female'
    WHERE
        m.avg_score > 0
    AND
        f.avg_score > 0
)

-- Step 3: Calculate the average of all ratios
SELECT
    AVG(male_female_ratio) AS avg_male_female_ratio
FROM
    MaleFemaleRatio;
''')

value = cursor.fetchall()

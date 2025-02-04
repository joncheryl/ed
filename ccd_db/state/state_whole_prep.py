'''
John Sherrill
Feb 1, 2025

Script to organize 1987-2024 datasets from the CCD nonfiscal datasets
'''
#%%

import pandas as pd

###############################################################################
# Import end_years = 2008-2014
###############################################################################
PRE_PATH = "data/nonfiscal/whole/whole_"
files = {year: [f'{PRE_PATH}{year}.csv', '', '\t', {}]
         for year in range(2008, 2015)}

pre_whole = {year: pd.read_csv(file, encoding=format,
                               sep=delim, dtype=kind) for
             year, (file, format, delim, kind) in files.items()}

# %%
###############################################################################
# Import end_years = 1987-2007
###############################################################################

# For years 2007 and earlier, the files are saved in a fixed width format
layout = pd.read_csv('data/nonfiscal/whole/layouts.csv')

files_fwf = {year: [f'{PRE_PATH}{year}.txt',
                    list(zip(layout.loc[layout['end_year'] == year, 'start']-1,
                        layout.loc[layout['end_year'] == year, 'end'])),
                    layout.loc[layout['end_year'] == year, 'variable']]
             for year in range(1987, 2008)}

pre_whole_fwf = {year: pd.read_fwf(file,
                                   colspecs=start_end,
                                   encoding='cp1252',
                                   names=variables) for
                 year, (file, start_end, variables) in files_fwf.items()}

pre_whole.update(pre_whole_fwf)

#%%
###############################################################################
# Make names consistent across years.
###############################################################################

# Other programs should likely be other diplomas
pre_whole[1987] = pre_whole[1987].rename(columns={'OTHPRG': 'OTHDIP'})

# end_year = 1996, 1999 are special with respect to naming conventions.
# Manual edits to crosswalk.csv:
# "WHIT" -> "WHT"
# "BLAC" -> "BLK"
# "ASIP" -> "ASIN"
# "AIAN" -> "AMIN"
crosswalk = pd.read_csv("crosswalk.csv", names=['old', 'new', 'description'])
crosswalk_dict = dict(zip(crosswalk['old'], crosswalk['new']))
pre_whole[1996] = pre_whole[1996].rename(columns=crosswalk_dict)
pre_whole[1999] = pre_whole[1999].rename(columns=crosswalk_dict)

# Ungraded student flag field
for year in range(2000, 2004):
    pre_whole[year] = pre_whole[year].rename(columns={'IGUG': 'IUG'})

# end_year = 1994, 1995 don't have FIPST for some reason
fipst_crosswalk = dict(zip(pre_whole[2011]['STNAME'],
                           pre_whole[2011]['FIPST']))
for year in [1994, 1995]:
    pre_whole[year]['FIPST'] = (
        pre_whole[year]['STATE']
        .str.upper()
        .map(fipst_crosswalk)
    )
# Only missing value left for FIPST is the Marianas observation in 1995.
pre_whole[1995].loc[pre_whole[1995]['STATE'] == 'Marianas', 'FIPST'] = 69

# Combine all years together.
whole = (pd.
         concat({x: y for x, y in pre_whole.items()},
                names=['end_year', 'fdsa'])
         .reset_index(level='end_year')
         .reset_index(drop=True)
)

# Combining different columns with different names but same data.
# For example: in 1987 (maybe later) FIPST is called FIPS
whole['FIPST'] = whole['FIPST'].combine_first(whole['FIPS'])
whole['FIPST'] = whole['FIPST'].combine_first(whole['STFIPS'])
whole = whole.drop(columns=['FIPS', 'STFIPS'])
whole['FIPST'] = whole['FIPST'].astype(int)

# WHITE -> WH in 2000
whole['AS'] = whole['AS'].combine_first(whole['ASIAN'])
whole['BL'] = whole['BL'].combine_first(whole['BLACK'])
whole['HI'] = whole['HI'].combine_first(whole['HISP'])
whole['WH'] = whole['WH'].combine_first(whole['WHITE'])
whole = whole.drop(columns=['ASIAN', 'BLACK', 'HISP', 'WHITE'])

whole['STNAME'] = whole['STNAME'].combine_first(whole['STATE'])
whole = whole.drop(columns=['STATE'])

# OTHGUI column name changes to GUI in end_year=2015
whole = whole.rename(columns={'OTHGUI': 'GUI', 'IOTHGUI': 'IGUI'})

# Drop the columns that have no value
whole = whole.drop(columns=['SURVYEAR', 'STED'])

# TAKEN CARE OF LATER IN DIRECTORY, STAFF, MEMBERSHIP prep scripts
# Do not want missing values, whether represented by letters or negative
# numbers to get in the way. Thus, convert all non-numeric values
# and negative numbers to NA.
# mem_cols = whole.columns[29: 125]
# whole[mem_cols] = (
#     whole[num_cols]
#     .apply(lambda col: (pd.to_numeric(col, errors='coerce')
#                         .where(lambda x: x > 0)
#                         )
#            )
#     .astype('Int64')
# )

# # Staffing is sometimes recorded as Full Time Equivalent (FTE) so not always
# # an integer.
# staff_cols = whole.columns[10:29]

# whole[staff_cols] = (
#     whole[staff_cols]
#     .apply(lambda col: (pd.to_numeric(col, errors='coerce')
#                         .where(lambda x: x > 0)
#                         )
#            )
# )

# Not interested in columns concerning diplomas or dropout stuff
# at the current time (Feb 2025)

# %%
###############################################################################
# Prepare columns for merging with data end_year=2015 and beyond.
###############################################################################

directory_cols = [
    'end_year', 'FIPST', 'STABR', 'SEANAME', 'STREET', 'CITY', 'STNAME', 'ZIP',
    'ZIP4', 'PHONE']

staff_cols = [
    'end_year', 'FIPST', 'PKTCH', 'KGTCH', 'ELMTCH', 'SECTCH', 'UGTCH',
    'TOTTCH', 'AIDES', 'CORSUP', 'ELMGUI', 'SECGUI', 'TOTGUI', 'LIBSPE',
    'LIBSUP', 'LEAADM', 'LEASUP', 'SCHADM', 'SCHSUP', 'STUSUP', 'OTHSUP',
    'IPKTCH', 'IKGTCH', 'IELMTCH', 'ISECTCH', 'IUGTCH', 'ITOTTCH', 'IAIDES',
    'ICORSUP', 'IELMGUI', 'ISECGUI', 'ITOTGUI', 'ILIBSPE', 'ILIBSUP',
    'ILEAADM', 'ILEASUP', 'ISCHADM', 'ISCHSUP', 'ISTUSUP', 'IOTHSUP', 'GUI', 
    'IGUI']

# These columns contain dropout/graduation data that I'm not presently
# concerned with.
drop_cols = [
    'REGDIP', 'OTHDIP', 'EQUIV', 'OTHCOM', 'AMREGDIP', 'ASREGDIP', 'HIREGDIP',
    'BLREGDIP', 'WHREGDIP', 'AMODIP', 'ASODIP', 'HIODIP', 'BLODIP', 'WHODIP',
    'AMEQUIV', 'ASEQUIV', 'HIEQUIV', 'BLEQUIV', 'WHEQUIV', 'AMOHC', 'ASOHC',
    'HIOHC', 'BLOHC', 'WHOHC', 'IREGDIP', 'IOTHDIP', 'IEQUIV', 'IOTHCOM',
    'IAMRGDIP', 'IASRGDIP', 'IHIRGDIP', 'IBLRGDIP', 'IWHRGDIP', 'IAMODIP',
    'IASODIP', 'IHIODIP', 'IBLODIP', 'IWHODIP', 'IAMEQUIV', 'IASEQUIV',
    'IHIEQUIV', 'IBLEQUIV', 'IWHEQUIV', 'IAMOHC', 'IASOHC', 'IHIOHC', 'IBLOHC',
    'IWHOHC']

membership_cols = ['end_year', 'FIPST', 'RACECAT'] + \
    [x for x in whole.columns
     if x not in directory_cols
     and x not in staff_cols
     and x not in drop_cols]

#%%
###############################################################################
# For merging DIRECTORY
###############################################################################

# To see difference in columns between directory and whole-directory:
# pd.Index(directory_cols).difference(whole.columns)

# - could POSSIBLY drop ST and STATE_NAME from new staff dataframe
# - could drop OUT_OF_STATE_FLAG from new staff dataframe
# NEW FOR 2015:
# - G_13_OFFERED, G_AE_OFFERED, NOGRADES, IGOFFERED
# - EFFECTIVE_DATE, UPDATED_STATUS, UPDATED_STATUS_TEXT
# - SYS_STATUS, SYS_STATUS_TEXT
# - LEA_TYPE_TEXT
# - STATENAME, MSTREET2, MSTREET3, LSTREET2, LSTREET3
# - SEANAME
# - CHARTER_LEA_TEXT
# NEW FOR 2016:
# - WEBSITE
# NEW FOR 2017:
# - STATE_AGENCY_NO
# - LEVEL - level derived from what grades offered. See document:
# Changes to CCD-assigned school and LEA levels
# at https://nces.ed.gov/ccd/reference_library.asp

# whole = whole.rename({'MSTREE': 'MSTREET1',
#                       'LSTREE': 'LSTREET1',
#                       'MSTREET': 'MSTREET1',
#                       'LSTREET': 'LSTREET1',
#                     'PKOFFRD': 'G_PK_OFFERED',
#                     'KGOFFRD': 'G_KG_OFFERED',
#                     'G01OFFRD': 'G_1_OFFERED',
#                     'G02OFFRD': 'G_2_OFFERED',
#                     'G03OFFRD': 'G_3_OFFERED',
#                     'G04OFFRD': 'G_4_OFFERED',
#                     'G05OFFRD': 'G_5_OFFERED',
#                     'G06OFFRD': 'G_6_OFFERED',
#                     'G07OFFRD': 'G_7_OFFERED',
#                     'G08OFFRD': 'G_8_OFFERED',
#                     'G09OFFRD': 'G_9_OFFERED',
#                     'G10OFFRD': 'G_10_OFFERED',
#                     'G11OFFRD': 'G_11_OFFERED',
#                     'G12OFFRD': 'G_12_OFFERED',
#                     'UGOFFRD': 'G_UG_OFFERED',
#                     'SCH': 'OPERATIONAL_SCHOOLS',
#                     'TYPE': 'LEA_TYPE',
#                     'CHRTLEASTAT': 'CHARTER_LEA'}, axis=1)

# Then something like the following:
# directory_cols_we_want = [x for x in directory_cols if x in whole.columns]
whole[directory_cols].to_csv(
    'data/nonfiscal/directory/directory_through_2014.csv',
    index=False)

# %%
###############################################################################
# Prepare STAFF columns.
###############################################################################

# To see difference in columns between staff and whole-staff:
# pd.Index(staff_cols).difference(whole.columns)

# - STATE_AGENCY_NO new for 2017
# - SCHPSYCH and STUSUPWOPSYCH new for 2020 and STUSUP depricated
# - could probably drop ST and STATE_NAME from new staff dataframe
# - need to figure out what TOTAL means in new staff dataframe
# - I beleive LEASTA, OTHSTA, SCHSTA were new in about 2020 but not sure
# Derived anyway so not mission critical.

# whole = whole.rename({'OTHGUI': 'GUI',
#                       'AIDES': 'PARA',
#                       'NAME': 'LEA_NAME',
#                       'STID': 'ST_LEAID',
#                       'STABR': 'ST'}, axis=1)

# # Then something like the following:
# staff_cols_we_want = [x for x in staff_cols if x in whole.columns]
whole[staff_cols].to_csv(
    'data/nonfiscal/staff/staff_through_2014.csv',
    index=False)

#%%
###############################################################################
# For merging MEMBERSHIP
###############################################################################

# To see difference in columns between membership and whole-membership:
# pd.Index(membership_cols).difference(whole.columns)

# STABR is dumb
# Probably can drop STATE_NAME and STABR from new membership dataframe
# SURVYEAR in new memebrship dataframe should be end_year

# New in 2015:
# STATENAME, SEANAME, G13, AE, TOTAL, AM13M, AM13F, AS13M, AS13F, HI13M,
# HI13F, BL13M, BL13F, WH13M, WH13F, HP13M, HP13F, TR13M, TR13F, AMAEM,
# AMAEF, ASAEM, ASAEF, HIAEM, HIAEF, BLAEM, BLAEF, WHAEM, WHAEF, HPAEM,
# HPAEF, TRAEM, TRAEF

# whole = whole.rename({'WHITE': 'WH',
#                       'BLACK': 'BL',
#                       'HISP': 'HI',
#                       'ASIAN': 'AS',
#                       'PACIFIC': 'HP'}, axis=1)

# Then something like the following:
# membership_cols_we_want = [x for x in membership_cols if x in whole.columns]
whole[membership_cols].to_csv(
    'data/nonfiscal/membership/membership_through_2014.csv',
    index=False)

# %%
'''
John Sherrill
Dec 8, 2024

Script to organize 1987-2024 datasets from the CCD nonfiscal datasets
'''
#%%

import pandas as pd

################################
# YEARS 2008-2014
################################
PRE_PATH = "data/nonfiscal/whole/whole_"
files = {year: [f'{PRE_PATH}{year}.csv', '', '\t', {}]
         for year in range(2014, 2007, -1)}

# some files/years have a windows/cp1252 encoding
for year in [2013, 2012, 2011, 2010, 2009]:
    files[year][1] = 'cp1251'

column_types = {'STID': object,
                'PHONE': object,
                'UNION': str,
                'CONUM': object,
                'ULOCAL': object,
                'CDCODE': object,
                'LZIP': object,
                'CDCODE_112': object,
                'GSHI': object,
                'METMIC': object
                }

# some files/years need better dtype-ing
for year in [2014, 2013, 2012, 2011, 2010, 2009, 2008]:
    files[year][3] = dict(column_types,
                          **{key + str(year-1)[2:]: value for key,
                             value in column_types.items()})

pre_whole = {year: pd.read_csv(file, encoding=format,
                               sep=delim, dtype=kind) for
             year, (file, format, delim, kind) in files.items()}

# need to remove trailing last two digits of year from variable names in
# year 2008, 2009, 2010
for year in [2008, 2009, 2010]:
    col_dict = {old_col: old_col[:-2] for old_col in pre_whole[year].columns \
        if old_col[-2:] == str(year-1)[-2:]}
    pre_whole[year] = pre_whole[year].rename(columns=col_dict)

# %%
################################
# YEARS 1987-20007
#
# Manual edits:
# - 1987: CO STERLING LEAID = 0806690 changed ', ' to 'CO' for state name
# - 1987: LEAID = 4651790 changed 'NA' to 'SD' for state name
# - 1988, 1989, 1992, 1993 deleted last line extra character
# - 1997, 1998: LEAID = 0807410
# - 1997, 1998: LEAID = 0807410 MSTATE changed from 'co' to 'CO'
################################

# For years 2007 and earlier, the files are saved in a fixed width format
layout = pd.read_csv('data/nonfiscal/whole/layouts.csv')

# Could use something like this to get the fields we want:
# layout = layout[~layout['description'].str.contains('DROPOUT') \
#     & ~layout['description'].str.contains('If this field') ]

cat_vars = {'LEAID': 'category',
            'FIPST': 'category',
            'UNION': str}

files_fwf = {year: [f'{PRE_PATH}{year}.txt',
                    list(zip(layout.loc[layout['END_YEAR'] == year, 'start']-1,
                        layout.loc[layout['END_YEAR'] == year, 'end'])),
                    layout.loc[layout['END_YEAR'] == year, 'variable']]
             for year in range(2007, 1986, -1)}

pre_whole_fwf = {year: pd.read_fwf(file,
                                   colspecs=start_end,
                                   encoding='cp1252',
                                   dtype=cat_vars,
                                   names=variables) for
                 year, (file, start_end, variables) in files_fwf.items()}

pre_whole.update(pre_whole_fwf)

# The column names for end_year=1987 are different
# C05_1987 = 'MEMBER'
pre_whole[1987] = (
    pre_whole[1987]
    .rename(columns={'C05': 'MEMBER'})
    .drop(columns=['C01', 'C02', 'C03', 'C04'])
)

# %%
whole = (pd.
         concat({x: y for x, y in pre_whole.items()},
                names=['END_YEAR', 'fdsa'])
         .reset_index(level='END_YEAR')
         .reset_index(drop=True)
)

# Need to make names consistent
# For example: in 1987 (maybe later) FIPST is called FIPS
whole['FIPST'] = whole['FIPST'].combine_first(whole['FIPS'])
whole['MSTREE'] = whole['MSTREE'].combine_first(whole['STREET'])
whole['MCITY'] = whole['MCITY'].combine_first(whole['CITY'])
whole['MSTATE'] = whole['MSTATE'].combine_first(whole['ST'])
whole['MZIP'] = whole['MZIP'].combine_first(whole['ZIP'])
whole['MZIP4'] = whole['MZIP4'].combine_first(whole['ZIP4'])
whole['ELL'] = whole['ELL'].combine_first(whole['LEP'])
whole['UG'] = whole['UG'].combine_first(whole['UNG'])
whole['UG'] = whole['UG'].combine_first(whole['C01'])
whole['PK12'] = whole['PK12'].combine_first(whole['C02'])
whole['SPECED'] = whole['SPECED'].combine_first(whole['C03'])
whole['REGDIP'] = whole['REGDIP'].combine_first(whole['C04'])
whole['OTHDIP'] = whole['OTHDIP'].combine_first(whole['C05'])

whole['OTHCOM'] = whole['OTHCOM'].combine_first(whole['C07'])
whole['TOTOHC'] = whole['TOTOHC'].combine_first(whole['OTHCOM'])

whole['GRSPAN'] = whole['GRSPAN'].combine_first(whole['GRSPAN97']) # fix typo
whole['GSLO'] = whole['GSLO'].combine_first(whole['GSL0']) # fix typo

whole.drop(columns=['FIPS', 'STREET', 'CITY', 'ST', 'ZIP', 'ZIP4', 'LEP',
                    'UNG', 'GRSPAN97', 'GSL0', 'C01', 'C02', 'C03', 'C04',
                    'C05', 'C07', 'OTHCOM'], inplace=True)

whole = whole.drop(columns=['YEAR',
                            'SURVYEAR',
                            'FILL',
                            'ADCD',
                            'SEL'])

losers = layout.loc[layout['description'].str.contains('DROPOUT') \
    | layout['description'].str.contains('If this field') \
        | layout['description'].str.contains('FLAG'),
    'variable']

whole = whole.drop(columns=losers)

# %%

directory_cols = [
    'END_YEAR', 'FIPST', 'STATENAME', 'ST', 'LEA_NAME','STATE_AGENCY_NO',
    'UNION', 'ST_LEAID', 'LEAID', 'MSTREET1', 'MSTREET2', 'MSTREET3',
    'MCITY', 'MSTATE', 'MZIP', 'MZIP4', 'LSTREET1', 'LSTREET2', 'LSTREET3',
    'LCITY', 'LSTATE', 'LZIP', 'LZIP4', 'PHONE', 'WEBSITE', 'SY_STATUS',
    'SY_STATUS_TEXT', 'UPDATED_STATUS', 'UPDATED_STATUS_TEXT',
    'EFFECTIVE_DATE', 'LEA_TYPE', 'LEA_TYPE_TEXT', 'OUT_OF_STATE_FLAG',
    'CHARTER_LEA', 'CHARTER_LEA_TEXT', 'NOGRADES', 'G_PK_OFFERED',
    'G_KG_OFFERED', 'G_1_OFFERED', 'G_2_OFFERED', 'G_3_OFFERED',
    'G_4_OFFERED', 'G_5_OFFERED', 'G_6_OFFERED', 'G_7_OFFERED',
    'G_8_OFFERED', 'G_9_OFFERED', 'G_10_OFFERED', 'G_11_OFFERED',
    'G_12_OFFERED', 'G_13_OFFERED', 'G_UG_OFFERED', 'G_AE_OFFERED', 'GSLO',
    'GSHI', 'LEVEL', 'IGOFFERED', 'OPERATIONAL_SCHOOLS', 'BIEA', 'AGCHRT'
    ]

staff_cols = [
    'END_YEAR', 'FIPST', 'STATENAME', 'ST', 'LEA_NAME', 'STATE_AGENCY_NO',
    'UNION', 'ST_LEAID', 'LEAID', 'CORSUP', 'ELMGUI', 'ELMTCH', 'GUI',
    'KGTCH', 'LEAADM', 'LEASTA', 'LEASUP', 'LIBSPE', 'LIBSUP', 'OTHSTA',
    'OTHSUP', 'PARA', 'PKTCH', 'SCHADM', 'SCHPSYCH', 'SCHSTA', 'SCHSUP',
    'SECGUI', 'SECTCH', 'STUSUPWOPSYCH', 'TOTAL', 'TOTGUI', 'TOTTCH',
    'UGTCH', 'STUSUP'
    ]

# these are columns from the 2015 membership. It switches to a long format in
# 2017 and the columns names are in three different columns for sex, race, and
# gender.
# Also replacing 'SURVYEAR' with 'END_YEAR'
membership_cols = [
    'END_YEAR', 'FIPST', 'STABR', 'STATENAME', 'SEANAME', 'LEAID', 'ST_LEAID',
    'LEA_NAME', 'PK', 'KG', 'G01', 'G02', 'G03', 'G04', 'G05', 'G06', 'G07',
    'G08', 'G09', 'G10', 'G11', 'G12', 'G13', 'UG', 'AE', 'TOTAL', 'MEMBER',
    'AMPKM', 'AMPKF', 'ASPKM', 'ASPKF', 'HIPKM', 'HIPKF', 'BLPKM', 'BLPKF',
    'WHPKM', 'WHPKF', 'HPPKM', 'HPPKF', 'TRPKM', 'TRPKF', 'AMKGM', 'AMKGF',
    'ASKGM', 'ASKGF', 'HIKGM', 'HIKGF', 'BLKGM', 'BLKGF', 'WHKGM', 'WHKGF',
    'HPKGM', 'HPKGF', 'TRKGM', 'TRKGF', 'AM01M', 'AM01F', 'AS01M', 'AS01F',
    'HI01M', 'HI01F', 'BL01M', 'BL01F', 'WH01M', 'WH01F', 'HP01M', 'HP01F',
    'TR01M', 'TR01F', 'AM02M', 'AM02F', 'AS02M', 'AS02F', 'HI02M', 'HI02F',
    'BL02M', 'BL02F', 'WH02M', 'WH02F', 'HP02M', 'HP02F', 'TR02M', 'TR02F',
    'AM03M', 'AM03F', 'AS03M', 'AS03F', 'HI03M', 'HI03F', 'BL03M', 'BL03F',
    'WH03M', 'WH03F', 'HP03M', 'HP03F', 'TR03M', 'TR03F', 'AM04M', 'AM04F',
    'AS04M', 'AS04F', 'HI04M', 'HI04F', 'BL04M', 'BL04F', 'WH04M', 'WH04F',
    'HP04M', 'HP04F', 'TR04M', 'TR04F', 'AM05M', 'AM05F', 'AS05M', 'AS05F',
    'HI05M', 'HI05F', 'BL05M', 'BL05F', 'WH05M', 'WH05F', 'HP05M', 'HP05F',
    'TR05M', 'TR05F', 'AM06M', 'AM06F', 'AS06M', 'AS06F', 'HI06M', 'HI06F',
    'BL06M', 'BL06F', 'WH06M', 'WH06F', 'HP06M', 'HP06F', 'TR06M', 'TR06F',
    'AM07M', 'AM07F', 'AS07M', 'AS07F', 'HI07M', 'HI07F', 'BL07M', 'BL07F',
    'WH07M', 'WH07F', 'HP07M', 'HP07F', 'TR07M', 'TR07F', 'AM08M', 'AM08F',
    'AS08M', 'AS08F', 'HI08M', 'HI08F', 'BL08M', 'BL08F', 'WH08M', 'WH08F',
    'HP08M', 'HP08F', 'TR08M', 'TR08F', 'AM09M', 'AM09F', 'AS09M', 'AS09F',
    'HI09M', 'HI09F', 'BL09M', 'BL09F', 'WH09M', 'WH09F', 'HP09M', 'HP09F',
    'TR09M', 'TR09F', 'AM10M', 'AM10F', 'AS10M', 'AS10F', 'HI10M', 'HI10F',
    'BL10M', 'BL10F', 'WH10M', 'WH10F', 'HP10M', 'HP10F', 'TR10M', 'TR10F',
    'AM11M', 'AM11F', 'AS11M', 'AS11F', 'HI11M', 'HI11F', 'BL11M', 'BL11F',
    'WH11M', 'WH11F', 'HP11M', 'HP11F', 'TR11M', 'TR11F', 'AM12M', 'AM12F',
    'AS12M', 'AS12F', 'HI12M', 'HI12F', 'BL12M', 'BL12F', 'WH12M', 'WH12F',
    'HP12M', 'HP12F', 'TR12M', 'TR12F', 'AM13M', 'AM13F', 'AS13M', 'AS13F',
    'HI13M', 'HI13F', 'BL13M', 'BL13F', 'WH13M', 'WH13F', 'HP13M', 'HP13F',
    'TR13M', 'TR13F', 'AMUGM', 'AMUGF', 'ASUGM', 'ASUGF', 'HIUGM', 'HIUGF',
    'BLUGM', 'BLUGF', 'WHUGM', 'WHUGF', 'HPUGM', 'HPUGF', 'TRUGM', 'TRUGF',
    'AMAEM', 'AMAEF', 'ASAEM', 'ASAEF', 'HIAEM', 'HIAEF', 'BLAEM', 'BLAEF',
    'WHAEM', 'WHAEF', 'HPAEM', 'HPAEF', 'TRAEM', 'TRAEF', 'AM', 'AMALM',
    'AMALF', 'AS', 'ASALM', 'ASALF', 'HI', 'HIALM', 'HIALF', 'BL', 'BLALM',
    'BLALF', 'WH', 'WHALM', 'WHALF', 'HP', 'HPALM', 'HPALF', 'TR', 'TRALM',
    'TRALF', 'IAMEMPUP'
    ]

# %%
################################################
# Cleanup and Formatting
################################################

# At this point, I think it makes sense to make a multiindex for the columns:
#
#         DIRECTORY         |         STAFF         |       MEMBERSHIP
#                           |  STAFF  |  SUBTOTALS  |          SEX
#                           |       STAFF_COUNTS    |     RACE_ETHNICITY
#                           |                       |         GRADE
#                           |                       |     STUDENT_COUNTS
#
# Could also make a multiindex for rows with END_YEAR and LEAID.
# This should work because
# whole[['END_YEAR', 'LEAID']].duplicated().any() returns False.

# whole = whole.set_index(['END_YEAR', 'LEAID'])
# whole['FIPST'] = pd.to_numeric(whole['FIPST']).astype(int)
# whole['MSTATE'] = whole['MSTATE'].str.upper()

################################################
# For merging STAFF
################################################

# To see difference in columns between staff and whole-staff:
# pd.Index(staff_cols).difference(whole.columns)

# - STATE_AGENCY_NO new for 2017
# - SCHPSYCH and STUSUPWOPSYCH new for 2020 and STUSUP depricated
# - could probably drop ST and STATE_NAME from new staff dataframe
# - need to figure out what TOTAL means in new staff dataframe
# - I beleive LEASTA, OTHSTA, SCHSTA were new in about 2020 but not sure
# Derived anyway so not mission critical.

whole = whole.rename({'OTHGUI': 'GUI',
                      'AIDES': 'PARA',
                      'NAME': 'LEA_NAME',
                      'STID': 'ST_LEAID',
                      'STABR': 'ST'}, axis=1)

# Then something like the following:
staff_cols_we_want = [x for x in staff_cols if x in whole.columns]
whole[staff_cols_we_want].to_csv(
    'data/nonfiscal/staff/staff_through_2014.csv',
    index=False)

################################################
# For merging DIRECTORY
################################################

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

whole = whole.rename({'MSTREE': 'MSTREET1',
                      'LSTREE': 'LSTREET1',
                      'MSTREET': 'MSTREET1',
                      'LSTREET': 'LSTREET1',
                    'PKOFFRD': 'G_PK_OFFERED',
                    'KGOFFRD': 'G_KG_OFFERED',
                    'G01OFFRD': 'G_1_OFFERED',
                    'G02OFFRD': 'G_2_OFFERED',
                    'G03OFFRD': 'G_3_OFFERED',
                    'G04OFFRD': 'G_4_OFFERED',
                    'G05OFFRD': 'G_5_OFFERED',
                    'G06OFFRD': 'G_6_OFFERED',
                    'G07OFFRD': 'G_7_OFFERED',
                    'G08OFFRD': 'G_8_OFFERED',
                    'G09OFFRD': 'G_9_OFFERED',
                    'G10OFFRD': 'G_10_OFFERED',
                    'G11OFFRD': 'G_11_OFFERED',
                    'G12OFFRD': 'G_12_OFFERED',
                    'UGOFFRD': 'G_UG_OFFERED',
                    'SCH': 'OPERATIONAL_SCHOOLS',
                    'TYPE': 'LEA_TYPE',
                    'CHRTLEASTAT': 'CHARTER_LEA'}, axis=1)

# Then something like the following:
directory_cols_we_want = [x for x in directory_cols if x in whole.columns]
whole[directory_cols_we_want].to_csv(
    'data/nonfiscal/directory/directory_through_2014.csv',
    index=False)

################################################
# For merging MEMBERSHIP
################################################

# To see difference in columns between membership and whole-membership:
# pd.Index(membership_cols).difference(whole.columns)

# STABR is dumb
# Probably can drop STATE_NAME and STABR from new membership dataframe
# SURVYEAR in new memebrship dataframe should be END_YEAR

# New in 2015:
# STATENAME, SEANAME, G13, AE, TOTAL, AM13M, AM13F, AS13M, AS13F, HI13M,
# HI13F, BL13M, BL13F, WH13M, WH13F, HP13M, HP13F, TR13M, TR13F, AMAEM,
# AMAEF, ASAEM, ASAEF, HIAEM, HIAEF, BLAEM, BLAEF, WHAEM, WHAEF, HPAEM,
# HPAEF, TRAEM, TRAEF

whole = whole.rename({'WHITE': 'WH',
                      'BLACK': 'BL',
                      'HISP': 'HI',
                      'ASIAN': 'AS',
                      'PACIFIC': 'HP'}, axis=1)

# Then something like the following:
membership_cols_we_want = [x for x in membership_cols if x in whole.columns]
whole[membership_cols_we_want].to_csv(
    'data/nonfiscal/membership/membership_through_2014.csv',
    index=False)

#%%
################################################
# What columns are left after removing all the columns that were in the
# new dataframes?
################################################

whole_columns_less_new = (
    whole
    .columns
    .difference(membership_cols + directory_cols + staff_cols)
)

# these are all 'agency' flags. Probably can drop.
dropping = [
    'AMEMPUP', 'IAMEMPUP', 'AFTEPUP', 'IAFTEPUP', 'ASPECED', 'IASPECED',
    'AELL', 'IAELL', 'AAIDCORSUP', 'IAAIDCORSUP', 'AGUID', 'IAGUID',
    'ALIBSTF', 'IALIBSTF', 'ALEAADM', 'IALEAADM', 'ASCHADM', 'IASCHADM',
    'ASUPSTF', 'IASUPSTF'
    ]

# stuff that eventually gets dropped
# C06 1988-1991
# CMSA 1987-2002
# GRSPAN 1987-1998
# LOCALE 2001-2006 turns into something similar to ULOCAL
# MSC 1987-2007
# MIGRNT 1999-2008
# OTHDIP 1987-1998
# PK12 1987-2010
# REGDIP 1987-1998
# RACECAT 2010-2013
# TEACH 1988-2008

# Dropped in 2015:
# CBSA, CSA, CONAME, CONUM, LATCOD, LONCOD, ULOCAL, METMIC, SPECED, TOTETH
# ELL entered into different file 2015 on. No longer reported in 2023
# CDCODE 2007-2014 but CDCODE_112 2012 only
eventually_dropped = ['C06', 'CBSA', 'CDCODE', 'CDCODE_112', 'CMSA', 'CONAME',
                      'CONUM', 'GRSPAN', 'CBSA', 'CSA', 'CONAME', 'CONUM',
                      'LATCOD', 'LONCOD', 'ELL', 'LOCALE', 'ULOCAL', 'METMIC',
                      'MSC', 'MIGRNT', 'OTHDIP', 'PK12', 'REGDIP', 'SPECED',
                      'RACECAT', 'TEACH', 'TOTETH']

# And adding diploma flag columns here
dropping = dropping + eventually_dropped + (layout
               .loc[layout['description']
                    .str.contains('Diploma Recipients'), 'variable']
               .to_list()) + \
(layout
               .loc[layout['description']
                    .str.contains('Other High School Completers'), 'variable']
               .to_list())

# This collection of columns should be empty.
left_over_columns = [x for x in whole_columns_less_new if x not in dropping]
# %%

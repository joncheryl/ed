''' 
Jan 2025

Data files obtained Jan 21, 2025 from:
https://www.schools.utah.gov/assessment/resources

Urls for exact files on Jan 21, 2025:
https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/23_%20RankList.xlsx
https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/Accountability2022RankList.xlsx
https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilityRankList2021.xlsx
https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilityRankList2018.xlsx
https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilityRankList2017.xlsx
https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilityRankList2016.xlsx

The 2018 rank % nonsense reminds me of something above. Can't find it.
in 2021 a sheet in the in xlsx file is titled 'School Report Card'

What the hell is SchoolID? cause SchoolNumber is the last three digits of
ST_SCHID in other datasets

Can use 'LEA' column (the names of school districts) but 'DistrictID' is 
just as functional as an identifier. There are some LEAs that have name
changes. Eg. 'REAL SALT LAKE ACADEMY HIGH SCHOOL' and "UTAH ARTS ACADEMY OLD'

'''

#%%
import pandas as pd

# Unfortunately, file names are inconsistent.
FILE_NAMES = {2024: '23_RankList.xlsx',
              2023: 'Accountability2022RankList.xlsx',
              2022: 'AccountabilityRankList2021.xlsx',
              2019: 'AccountabilityRankList2018.xlsx',
              2018: 'AccountabilityRankList2017.xlsx',
              2017: 'AccountabilityRankList2016.xlsx'}

# Unfortunately, some files have multiple sheets. Only last sheet in desired.
pre_rank = {year: pd.read_excel(file_name,
                                sheet_name=(
                                    pd.ExcelFile(file_name).
                                    sheet_names[-1]
                                ))
            for year, file_name in FILE_NAMES.items()}

# Combine year-specific dataframes together into one: rank
rank = (
    pd.concat(pre_rank, names=['end_year'])
    .reset_index(level='end_year')
    .reset_index(drop=True)
    .drop(columns=['SchoolYear',
                   'schoolID',
                   'Score', 
                   'SchoolType.1',
                   'SchoolYear.1',
                   'LetterGrade.1',
                   'TotalScore.1',
                   'TotalPossible.1',
                   'IsSplitSchool.1',
                   'SchoolNumber.1',
                   'DistrictID.1'])
    .rename(columns={'District': 'lea_name',
                     'SchoolName': 'school_name',
                     'SchoolType': 'school_type',
                     'SchoolID': 'st_schoolid',
                     'SchoolNumber': 'st_school_number',
                     'DistrictID': 'st_leaid'})
)

# Make percent ranking name consistent:
rank['percent_ranking'] = (
    rank['PERCENT Ranking']
    .combine_first(rank['PERCENT Ranking '])
    .combine_first(rank['% Ranking'])
    .combine_first(rank['PERCENT Ranking2'])
    .combine_first(rank['Percent RANKING'])
    )
rank = rank.drop(columns=['PERCENT Ranking',
                          'PERCENT Ranking ',
                          '% Ranking',
                          'PERCENT Ranking2',
                          'Percent RANKING'])

# Make text attributes uppercase and remove erroneous commas
rank['lea_name'] = rank['lea_name'].str.upper()
rank['lea_name'] = rank['lea_name'].str.replace(',', '')
rank['school_name'] = rank['school_name'].str.upper()
rank['school_name'] = rank['school_name'].str.replace(',', '')

# Columns to become float
columns_to_make_numeric =[
    'Post Secondary Readiness Score',
    'Post Secondary Readiness Points',
    'ACTScore',
    'ACTPointsEarned',
    'GradRateScore',
    'GradeRatePoints',
    'ELProgressScore',
    'ELProgressPoints',
    'LQGrowthScore',
    'LQGrowthPoints']
for col in columns_to_make_numeric:
    rank[col] = pd.to_numeric(rank[col].replace('-', value=pd.NA))

# Columns to become Int64
rank['GrowthRating'] = rank['GrowthRating'].astype('Int64')
rank['AchievementRating'] = rank['AchievementRating'].astype('Int64')
rank['Bottom 5% Flag'] = rank['Bottom 5% Flag'].astype('Int64')
rank['ELProgressRating'] = (
    rank['ELProgressRating']
    .replace('-', value=pd.NA)
    .astype('Int64')
)
rank['PSReadinessRating'] = (
    rank['PSReadinessRating']
    .replace('-', value=pd.NA)
    .astype('Int64')
)

# Write to csv
rank.to_csv("rank.csv", index=False)

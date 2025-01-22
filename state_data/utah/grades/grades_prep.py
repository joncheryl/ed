'''
Currently used for growth and achievement data. (Jan 2025)

Accessed at https://www.schools.utah.gov/assessment/resources on Jan 20, 2025.

Urls for exact files on Jan 20, 2025:
- https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilitySchoolGrades2016.xlsx 
- https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilitySchoolGrades2015.xlsx
- https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilitySchoolGrades2014.xlsx
- https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilitySchoolGrades2013.xlsx
- https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilitySchoolGrades2012.xlsx

PDF summary for end_year=2013 (labeled 2012 for 2012-2013 school year):
- https://www.schools.utah.gov/assessment/_assessment_/_resources_/_accountability_/AccountabilitySchoolGrades2012Summary.pdf

'''
#%%
import pandas as pd

FILE_PREFIX = "AccountabilitySchoolGrades"
pre_grades = {year: pd.read_excel(FILE_PREFIX + str(year) + ".xlsx",
                                  header=1)
              for year in range(2012, 2017)}

# make attribute names consistent
renames = {
    'District': 'LEA',
    'Science Proficient': 'SC Proficient',
    'Science Proficient Possible': 'SC Proficient Possible',
    'Students Meeting All Four CCR benchmarks ACT':
        'Students Meeting All Four CCR Benchmarks ACT'
    }

for year in range(2012, 2017):
    pre_grades[year] = pre_grades[year].rename(columns = renames)

# drop an erroneous row in 2012
pre_grades[2012] = (
    pre_grades[2012]
    .dropna(subset=pre_grades[2012].columns.difference(['LEA']),
            how='all')
)

# Put years together into final dataframe: grades
grades = (
    pd.concat(pre_grades, names=['end_year'])
    .reset_index(level='end_year')
    .reset_index(drop=True)
    .drop(columns=['School Type*',
                   'Students Meeting All Four CCR Benchmarks ACT*',
                   'College Career Readiness ACT Possible*',
                   'School Year'])
    .rename(columns={'LEA': 'lea_name',
                     'School': 'school_name',
                     'School Type': 'school_type',
                     'School Number': 'st_schoolid',
                     'LEA Number': 'st_leaid'})
)

# Change school_type column for future convenience
grades['school_type'] = grades['school_type'].astype('category')
grades['school_type'] = grades['school_type'] \
    .cat.rename_categories({'Elem/Middle/Jr High School': 'E',
                            'High School': 'H'})

# fix some school typos/change in naming conventions
grades['lea_name'] = grades['lea_name'].str.upper()
grades['school_name'] = grades['school_name'].str.upper()

labels = {
    'ACADEMY FOR MATH ENGINEERING & SCIENCE (AMES)':
        'ACADEMY FOR MATH ENGINEERING & SCIENCE',
    'BEEHIVE SCIENCE & TECHNOLOGY ACADEMY (BSTA)':
        'BEEHIVE SCIENCE & TECHNOLOGY ACADEMY',
    'CS LEWIS ACADEMY':
         'C.S. LEWIS ACADEMY',
    'KARL G MAESER PREPARATORY ACADEMY':
        'KARL G. MAESER PREPARATORY ACADEMY',
    'NO UT ACAD FOR MATH ENGINEERING & SCIENCE (NUAMES)':
        'NO. UT. ACAD. FOR MATH ENGINEERING & SCIENCE',
    'UTAH COUNTY ACADEMY OF SCIENCE (UCAS)':
        'UTAH COUNTY ACADEMY OF SCIENCE'
}

for old, new in labels.items():
    grades.loc[grades['lea_name'] == old, 'lea_name'] = new

# Need to fill in the missing values for st_leaid and st_schoolid for
# later merging grades with CCD data.

lea_names_to_ids = (
    grades
    .dropna(subset='st_leaid')
    .set_index(['lea_name', 'school_name'])
    .loc[:, 'st_leaid']
    .to_dict()
)
grades['st_leaid'] = (
    grades
    .apply(lambda row: lea_names_to_ids.get((row['lea_name'],
                                         row['school_name'])),
           axis=1)
)

school_names_to_ids = (
    grades
    .dropna(subset='st_schoolid')
    .set_index(['lea_name', 'school_name'])
    .loc[:, 'st_schoolid']
    .to_dict()
)
grades['st_schoolid'] = (
    grades
    .apply(lambda row: school_names_to_ids.get((row['lea_name'],
                                         row['school_name'])),
           axis=1)
    .astype('Int64')
)

grades.to_csv("grades.csv", index=False)

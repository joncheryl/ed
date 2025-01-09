'''
John Sherrill
Dec 9, 2024

Script to download CCD district data file record layouts and export to a
single csv file layouts.csv.
'''

# %%

import re
import os
import requests
import pandas as pd

######################################################################
# Download files
######################################################################
LAYOUT_FOLDER = "./data/nonfiscal/whole/layouts"
PRE_PATH = "data/nonfiscal/whole/layouts/layout_"

def just_download(url_, new_name, extract_to):
    '''
    Function to just download a file
    '''
    # Step 0: Ensure the folder you want to put it in, exists
    os.makedirs(extract_to, exist_ok=True)

    local_zip_path = os.path.join(extract_to, new_name)
    response = requests.get(url_, stream=True, timeout=1000)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Write the ZIP file to the specified directory
    with open(local_zip_path, 'wb') as zip_file:
        # Download in chunks
        for chunk in response.iter_content(chunk_size=8192):
            zip_file.write(chunk)

layout_urls = {
    2007: 'https://nces.ed.gov/ccd/data/txt/pau061clay.txt',
    2006: 'https://nces.ed.gov/ccd/data/txt/pau051alay.txt',
    2005: 'https://nces.ed.gov/ccd/data/txt/pau041clay.txt',
    2004: 'https://nces.ed.gov/ccd/data/txt/pau031blay.txt',
    2003: 'https://nces.ed.gov/ccd/data/txt/pau021alay.txt',
    2002: 'https://nces.ed.gov/ccd/data/txt/pau01lay.txt',
    2001: 'https://nces.ed.gov/ccd/data/txt/pau00lay.txt',
    2000: 'https://nces.ed.gov/ccd/data/txt/pau99lay.txt',
    1999: 'https://nces.ed.gov/ccd/data/txt/pau98lay.txt',
    1998: 'https://nces.ed.gov/ccd/data/txt/pau97lay.txt',
    1997: 'https://nces.ed.gov/ccd/data/txt/pau96lay.txt',
    1996: 'https://nces.ed.gov/ccd/data/txt/pau95lay.txt',
    1995: 'https://nces.ed.gov/ccd/data/txt/pau94lay.txt',
    1994: 'https://nces.ed.gov/ccd/data/txt/pau93lay.txt',
    1993: 'https://nces.ed.gov/ccd/data/txt/pau92lay.txt',
    1992: 'https://nces.ed.gov/ccd/data/txt/pau91lay.txt',
    1991: 'https://nces.ed.gov/ccd/data/txt/pau90lay.txt',
    1990: 'https://nces.ed.gov/ccd/data/txt/pau89lay.txt',
    1989: 'https://nces.ed.gov/ccd/data/txt/pau88lay.txt',
    1988: 'https://nces.ed.gov/ccd/data/txt/pau87lay.txt',
    1987: 'https://nces.ed.gov/ccd/data/txt/pau86lay.txt'
}


for year, url in layout_urls.items():
    just_download(url, "layout_pre_" + str(year) + ".txt", LAYOUT_FOLDER)

######################################
# FORMATTING
######################################

# For some fucking reason, I have to manually edit some of these goddamn files
with open(LAYOUT_FOLDER + '/layout_pre_1989.txt', 'r',
            encoding='cp1252') as file:
    content = file.read()
modified_content = re.sub('C0798', 'C0788', content)
with open(LAYOUT_FOLDER + '/layout_pre_1989.txt', 'w',
          encoding='cp1252') as file:
    file.write(modified_content)

with open(LAYOUT_FOLDER + '/layout_pre_2003.txt', 'r',
            encoding='cp1252') as file:
    content = file.read()
modified_content = re.sub('.BOUND02', '\nBOUND02', content)
with open(LAYOUT_FOLDER + '/layout_pre_2003.txt', 'w',
          encoding='cp1252') as file:
    file.write(modified_content)

# Some of the files need some massaging to be readable with
# Pandas. Eg. Replace all tabs with four spaces:
def process_line(splited_line):
    ''' Split the line into parts separated by commas '''
    parts = splited_line.split(',')
    if len(parts) > 6:
        # Keep the first 5 parts and join the rest with spaces
        return ','.join(parts[:6]) + ' ' + ' '.join(parts[6:])
    return splited_line

def edit_file_in_place(file_path):
    ''' Replace commas in description. Do this line by line. '''
    temp_file = file_path + '.tmp'

    with open(file_path, 'r', encoding='utf-8') as infile, \
        open(temp_file, 'w', encoding='utf-8') as outfile:
        for changed_line in infile:
            # Process the line
            edited_line = process_line(changed_line.strip())
            # Write the edited line to the temporary file
            outfile.write(edited_line + '\n')

    # Replace the original file with the temporary file
    os.replace(temp_file, file_path)

for year in range(1987, 2008):
    with open(PRE_PATH + str(year) + '.txt', 'r', encoding='cp1252') as file:
        content = file.read()

    # These mess up the field widths
    modified_content = re.sub(r'\*', '', content)
    # In some years, the start/ends are recorded as
    # 'start-end' instead of as two distinct columns
    modified_content = re.sub(r'-', ',', modified_content)
    modified_content = re.sub(r'   \+', '', modified_content)
    modified_content = re.sub(r'  \+', '', modified_content)
    # for 1993 +GSHI
    modified_content = re.sub(r'\n\+', '\n', modified_content)
    modified_content = re.sub(r'[ \t]+', ',', modified_content)
    modified_content = re.sub('\n,', '#', modified_content)

    with open(PRE_PATH + str(year) + '.txt', 'w', encoding='utf-8') as file:
        file.write(modified_content)

    # And get rid of the commas in the variable descriptions
    edit_file_in_place(PRE_PATH + str(year) + '.txt')

######################################
# Now read those downloaded and formatted files into a dataframe
######################################

files = {year: [f'{PRE_PATH}{year}.txt', 1, []]
         for year in range(1987, 2008)}

# How many lines to skip each year
for year in range(1987, 2008):
    with open(PRE_PATH + str(year) + '.txt', 'r',
              encoding='utf-8') as file:
        # Start counting lines from 1
        for line_number, line in enumerate(file, start=0):
            if 'LEAID' in line:
                files[year][1] = line_number

# files[2007][1] = 26
# files[2006][1] = 18
# files[2005][1] = 18
# files[2004][1] = 18
# files[2003][1] = 18
# files[2002][1] = 18
# files[2001][1] = 18
# files[2000][1] = 18
# files[1999][1] = 13
# files[1998][1] = 16 # fucked up order of columns
# files[1997][1] = 16 # fucked up order of columns
# files[1996][1] = 16 # fucked up order of columns
# files[1995][1] = 16 # fucked up order of columns
# files[1994][1] = 16 # fucked up order of columns
# files[1993][1] = 16 # fucked up order of columns
# files[1992][1] = 16 # fucked up order of columns
# files[1991][1] = 5 # fucked up order of columns
# files[1990][1] = 5 # fucked up order of columns
# files[1989][1] = 5 # fucked up order of columns
# files[1988][1] = 5 # fucked up order of columns
# files[1987][1] = 5 # fucked up order of columns

# %%
# The correct order of the columns
for year in range(1987, 1997):
    files[year][2] = ['variable', 'type', 'start',
                      'end', 'width', 'description']
for year in range(1997, 1999):
    files[year][2] = ['variable', 'width', 'start',
                      'end', 'type', 'description']
for year in range(1999, 2008):
    files[year][2] = ['variable', 'start', 'end',
                      'width', 'type', 'description']

pre_layout = {year: pd.read_csv(file, comment='#',
                                usecols=[0,1,2,3,4,5],
                                names=correct_names,
                                skiprows=skip) for
              year, (file, skip, correct_names) in files.items()}

# Remove trailing year identifier from variable names
for year in range(1987, 2008):
    pre_layout[year]['variable'] = (
        pre_layout[year]['variable']
        .apply(lambda x, year=year: x[:-2]
               if x[-2:] == str((year-1))[-2:]
               else x)
    )

# Put all years together
layouts = (
    pd.concat(pre_layout, names=['END_YEAR', 'fdsa'])
    .reset_index(level='END_YEAR')
    .reset_index(drop=True)
    .drop(columns=['width'])
)

layouts.to_csv('data/nonfisal/whole/layouts.csv', index=False)

# Could use something like this to get the fields we want:
# layouts[~layouts['description'].str.contains('DROPOUT') \
    # & ~layouts['description'].str.contains('If this field') ]
# %%

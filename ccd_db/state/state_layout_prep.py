'''
John Sherrill
Jan 27, 2025

Script to download CCD state data file record layouts and export to a
single csv file layouts.csv.

For 1998, the layout file was a .doc file that was manually downloaded and
formatted into a text file "layout_pre_1998.txt"
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
PRE_PATH = "data/nonfiscal/whole/layouts/layout_pre_"

def just_download(url_, new_name, extract_to):
    ''' Function to just download a file '''
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
    2007: "https://nces.ed.gov/ccd/data/txt/stnfis061clay.txt",
    2006: "https://nces.ed.gov/ccd/data/txt/stnfis051blay.txt",
    2005: "https://nces.ed.gov/ccd/data/txt/stnfis041flay.txt",
    2004: "https://nces.ed.gov/ccd/data/txt/stnfis031blay.txt",
    2003: "https://nces.ed.gov/ccd/data/txt/stnfis021blay.txt",
    2002: "https://nces.ed.gov/ccd/data/txt/stnfis011clay.txt",
    2001: "https://nces.ed.gov/ccd/data/txt/stnfis011clay.txt", # 2002 layout
    2000: "https://nces.ed.gov/ccd/data/txt/stnfis99lay.txt",
    1999: "https://nces.ed.gov/ccd/data/txt/stnfis98lay.txt",
    # For 1998, have to use a manually formatted file. Was a .doc file
    1997: "https://nces.ed.gov/ccd/data/txt/stNfis961clay.txt",
    1996: "https://nces.ed.gov/ccd/data/txt/stNfis95lay.txt",
    1995: "https://nces.ed.gov/ccd/data/txt/stNfis94lay.txt",
    1994: "https://nces.ed.gov/ccd/data/txt/stNfis93lay.txt",
    1993: "https://nces.ed.gov/ccd/data/txt/stNfis92lay.txt",
    1992: "https://nces.ed.gov/ccd/data/txt/stNfis911clay.txt",
    1991: "https://nces.ed.gov/ccd/data/txt/stNfis901clay.txt",
    1990: "https://nces.ed.gov/ccd/data/txt/stNfis89lay.txt",
    1989: "https://nces.ed.gov/ccd/data/txt/stNfis881clay.txt",
    1988: "https://nces.ed.gov/ccd/data/txt/stNfis871clay.txt",
    1987: "https://nces.ed.gov/ccd/data/txt/stNfis861clay.txt"
}

for year, url in layout_urls.items():
    just_download(url, "layout_pre_" + str(year) + ".txt", LAYOUT_FOLDER)

#%%
######################################
# FORMATTING
######################################

def process_line(splited_line):
    ''' Split the line into parts separated by commas '''
    parts = splited_line.split(',')
    if len(parts) > 6:
        # Keep the first 5 parts and join the rest with spaces
        return ','.join(parts[:6]) + ' ' + ' '.join(parts[6:])
    return splited_line

def edit_file_in_place(file_path):
    ''' Replace commas in description variable. Do this line by line. '''
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
    # Hyphens in 1998 are messing everything up. It's an encoding issue.
    if year == 1998:
        with open(PRE_PATH + str(year) + '.txt', 'r',
                  encoding='utf-8') as file:
            content = file.read()
    else:
        with open(PRE_PATH + str(year) + '.txt', 'r',
                  encoding='cp1252') as file:
            content = file.read()

    # These mess up the field widths
    modified_content = re.sub(r'\*', '', content)
    # In some years, the start/ends are recorded as 'start-end', or
    # 'start- end' or 'start - end' instead of as two distinct columns.
    modified_content = re.sub(r' - ', ',', modified_content)
    modified_content = re.sub(r' – ', ',', modified_content)
    modified_content = re.sub(r'- ', ',', modified_content)
    modified_content = re.sub(r'– ', ',', modified_content)
    modified_content = re.sub(r'-', ',', modified_content)
    modified_content = re.sub(r'–', ',', modified_content)
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

#%%
######################################
# Now read those downloaded and formatted files into a dataframe
######################################

files = {year: [f'{PRE_PATH}{year}.txt', 1, []]
         for year in range(1987, 2008)}

# How many lines to skip each year
for year in range(1987, 2008):
    with open(PRE_PATH + str(year) + '.txt', 'r',
              encoding='cp1252') as file:
        # Start counting lines from 1
        for line_number, line in enumerate(file, start=0):
            if 'SURVYEAR' in line:
                files[year][1] = line_number

# Manually enter how many lines to skip in each file
files[1996][1] = 5
files[1995][1] = 19
files[1994][1] = 19

# The correct order of the columns
for year in range(1987, 2008):
    files[year][2] = ['variable', 'width', 'start',
                      'end', 'type', 'description']
for year in [1996, 1999]:
    files[year][2] = ['variable', 'type', 'start',
                      'end', 'width', 'description']

pre_layout = {year: pd.read_csv(file, comment='#',
                                usecols=[0,1,2,3,4,5],
                                names=correct_names,
                                skiprows=skip) for
              year, (file, skip, correct_names) in files.items()}

# For end_year = 1989, the ZIP column should be split into ZIP (5 long)
# and ZIP4 (5 long including dash (or something))
pre_layout[1989].loc[pre_layout[1989]['variable'] == 'ZIP', 'width'] = 5
pre_layout[1989].loc[pre_layout[1989]['variable'] == 'ZIP', 'end'] = 130

# For end_year = 1991, the STATE width and end are incorrect (6 too long).
pre_layout[1991].loc[pre_layout[1991]['variable'] == 'STATE', 'width'] = 29
pre_layout[1991]['end'] = pre_layout[1991]['width'].cumsum()
pre_layout[1991]['start'] = 1 + (
    pre_layout[1991]['width']
    .cumsum()
    .shift(fill_value=0)
)

# For end_year = 1996, all the widths for the numeric columns should be 10.
pre_layout[1996].loc[pre_layout[1996]['variable'].str.match(r'[B-E]\d{2}'),
                      'width'] = 10
pre_layout[1996]['end'] = pre_layout[1996]['width'].cumsum()
pre_layout[1996]['start'] = 1 + (
    pre_layout[1996]['width']
    .cumsum()
    .shift(fill_value=0)
)

#%%
# Put all years together
layouts = (
    pd.concat(pre_layout, names=['end_year', 'fdsa'])
    .reset_index(level='end_year')
    .reset_index(drop=True)
    .drop(columns=['width'])
)

#%%
layouts.to_csv('data/nonfiscal/whole/layouts.csv', index=False)

# %%

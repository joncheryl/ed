'''
John Sherrill
Feb 10, 2025

Script to download CCD state data file record layouts for the fiscal datasets
and export to a single csv file layouts_fiscal.csv.

Used for end_years=1987-1999 excluding 1993 and 1998.
'''

# %%

import re
import os
import requests
import pandas as pd

######################################################################
# Download files
######################################################################
LAYOUT_FOLDER = "./data/fiscal/layouts"
PRE_PATH = "data/fiscal/layouts/layout_pre_"

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
    1999: "https://nces.ed.gov/ccd/data/txt/stfis99rlay.txt",

    1997: "https://nces.ed.gov/ccd/data/txt/stfis97rlay.txt",
    1996: "https://nces.ed.gov/ccd/data/txt/stfis96rlay.txt",
    1995: "https://nces.ed.gov/ccd/data/txt/stfis95rlay.txt",
    1994: "https://nces.ed.gov/ccd/data/txt/stfis94rlay.txt",

    1992: "https://nces.ed.gov/ccd/data/txt/stfis92rlay.txt",
    1991: "https://nces.ed.gov/ccd/data/txt/stfis91rlay.txt",
    1990: "https://nces.ed.gov/ccd/data/txt/stfis90rlay.txt",
    1989: "https://nces.ed.gov/ccd/data/txt/stfis89rlay.txt",
    1988: "https://nces.ed.gov/ccd/data/txt/stfis88rlay.txt",
    1987: "https://nces.ed.gov/ccd/data/txt/stfis87rlay.txt"
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

years = layout_urls.keys()
for year in years:
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

files = {year: [f'{PRE_PATH}{year}.txt', 1] for year in years}

# How many lines to skip each year
for year in years:
    with open(PRE_PATH + str(year) + '.txt', 'r',
              encoding='cp1252') as file:
        # Start counting lines from 1
        for line_number, line in enumerate(file, start=0):
            if 'SURVYEAR' in line:
                files[year][1] = line_number

# The correct order of the columns
col_names = ['variable', 'type', 'start', 'end', 'width', 'description']

pre_layout = {year: pd.read_csv(file, comment='#',
                                usecols=[0,1,2,3,4,5],
                                names=col_names,
                                skiprows=skip) for
              year, (file, skip) in files.items()}

#########
# Keeping the below that's commented out in case some of the years are
# fucked up. Keeping this repair code could be useful in such a case.
##########

# # For end_year = 1989, the ZIP column should be split into ZIP (5 long)
# # and ZIP4 (5 long including dash (or something))
# pre_layout[1989].loc[pre_layout[1989]['variable'] == 'ZIP', 'width'] = 5
# pre_layout[1989].loc[pre_layout[1989]['variable'] == 'ZIP', 'end'] = 130

# # For end_year = 1991, the STATE width and end are incorrect (6 too long).
# pre_layout[1991].loc[pre_layout[1991]['variable'] == 'STATE', 'width'] = 29
# pre_layout[1991]['end'] = pre_layout[1991]['width'].cumsum()
# pre_layout[1991]['start'] = 1 + (
#     pre_layout[1991]['width']
#     .cumsum()
#     .shift(fill_value=0)
# )

# # For end_year = 1996, all the widths for the numeric columns should be 10.
# pre_layout[1996].loc[pre_layout[1996]['variable'].str.match(r'[B-E]\d{2}'),
#                       'width'] = 10
# pre_layout[1996]['end'] = pre_layout[1996]['width'].cumsum()
# pre_layout[1996]['start'] = 1 + (
#     pre_layout[1996]['width']
#     .cumsum()
#     .shift(fill_value=0)
# )

#%%
# Put all years together
layouts = (
    pd.concat(pre_layout, names=['end_year', 'fdsa'])
    .reset_index(level='end_year')
    .reset_index(drop=True)
    .drop(columns=['width'])
)

#%%
layouts.to_csv(LAYOUT_FOLDER + '/layouts.csv', index=False)

# %%

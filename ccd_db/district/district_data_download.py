'''
John Sherrill
Nov 30, 2024

Script to download CCD district data files and extract to specified folders.

Some years are missing in the fiscal section. They're not listed on the
CCD Data Files page:
https://nces.ed.gov/ccd/files.asp#Fiscal:1,LevelId:5,Page:1
'''

# %%

import os
import zipfile
import requests

def download_and_unzip(url_, new_name, extract_to):
    '''
    Function to download and unzip a ZIP file
    '''
    # Step 0: Ensure the folder you want to put it in, exists
    os.makedirs(extract_to, exist_ok=True)

    # Step 1: Download the ZIP file
    local_zip_path = os.path.join(extract_to, "downloaded.zip")
    response = requests.get(url_, stream=True, timeout=1000)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Write the ZIP file to the specified directory
    with open(local_zip_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    # Step 2: Extract the desired file from the ZIP archive
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        members = zip_ref.namelist()
        desired_file = [file for file in members
                        if file.endswith(('.csv', '.txt', '.dat', '.DAT'))]

        # If the zip archive is "normal"
        if len(desired_file) == 1:
            path_for_file = zip_ref.extract(desired_file[0], extract_to)
            os.rename(path_for_file, extract_to + "/" + new_name)

        # If the zip file is an archive of archives for some reason
        elif all(file.endswith('.zip') for file in members):
            # Evaluate every member of the archive
            for file in members:
                path_for_file = zip_ref.extract(file, extract_to)

                # if the member is the right stuff, then extract
                with zipfile.ZipFile(path_for_file, 'r') as zip_ref_two:
                    members_two = zip_ref_two.namelist()
                    desired_file_two = [file for file in members_two
                                        if file.endswith(('.csv', '.txt',
                                                          '.dat', '.DAT'))]
                    if len(desired_file_two) == 1:
                        path_for = zip_ref_two.extract(desired_file_two[0],
                                                            extract_to)
                        os.rename(path_for, extract_to + "/" + new_name)
                        break
        else:
            print("Something messed up for " + new_name)

def just_download(url_, new_name, extract_to):
    '''
    Function to just download an un-archived file
    '''
    local_zip_path = os.path.join(extract_to, new_name)
    response = requests.get(url_, stream=True, timeout=1000)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Write the ZIP file to the specified directory
    with open(local_zip_path, 'wb') as file:
        # Download in chunks
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

# %%
######################################################################
# Download files for nonfiscal/directory
######################################################################
dir_urls = {
    2023: "https://nces.ed.gov/ccd/data/zip/ccd_lea_029_2223_w_1a_083023.zip",
    2022: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_029_2122_w_1a_071722.zip",
    2021: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_029_2021_w_1a_080621.zip",
    2020: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_029_1920_w_1a_082120.zip",
    2019: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_029_1819_l_1a_091019.zip",
    2018: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_029_1718_w_1a_083118.zip",
    2017: "https://nces.ed.gov/ccd/" + \
        "data/zip/ccd_lea_029_1617_w_1a_11212017_csv.zip",
    2016: "https://nces.ed.gov/ccd/" + \
        "data/zip/ccd_lea_029_1516_w_1a_011717_csv.zip",
    2015: "https://nces.ed.gov/ccd/" + \
        "data/zip/ccd_lea_029_1415_w_0216161ar_txt.zip"
}
DIR_FOLDER = "./data/nonfiscal/directory"

for year, url in dir_urls.items():
    download_and_unzip(url, "directory_" + str(year) + ".csv", DIR_FOLDER)

# Remove the unneeded file formats.
for filename in os.listdir(DIR_FOLDER):
    filepath = os.path.join(DIR_FOLDER, filename)
    if not (filename.endswith('.csv') or filename.endswith('.txt')):
        os.remove(filepath)

# %%
######################################################################
# Download files for nonfiscal/membership
######################################################################
mem_urls = {
    2023: "https://nces.ed.gov/ccd/data/zip/ccd_lea_052_2223_l_1a_083023.zip",
    2022: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_052_2122_l_1a_071722.zip",
    2021: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_052_2021_l_1a_080621.zip",
    2020: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_052_1920_l_1a_082120.zip",
    2019: "https://nces.ed.gov/ccd/data/zip/ccd_lea_052_1819_l_1a_091019.zip",
    2018: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_052_1718_l_1a_083118.zip",
    2017: "https://nces.ed.gov/ccd/" + \
        "data/zip/ccd_lea_052_1617_l_2a_11212017.zip",
    2016: "https://nces.ed.gov/ccd/" + \
        "data/zip/ccd_lea_052_1516_w_1a_011717_csv.zip",
    2015: "https://nces.ed.gov/ccd/" + \
        "data/zip/ccd_lea_052_1415_w_0216161a_txt.zip"
}
MEM_FOLDER = "./data/nonfiscal/membership"

for year, url in mem_urls.items():
    download_and_unzip(url, "membership_" + str(year) + ".csv", MEM_FOLDER)

# Remove the unneeded file formats.
for filename in os.listdir(MEM_FOLDER):
    filepath = os.path.join(MEM_FOLDER, filename)
    if not filename.endswith('.csv'):
        os.remove(filepath)

# %%
######################################################################
# Download files for nonfiscal/staff
######################################################################
staff_urls = {
    2023: "https://nces.ed.gov/ccd/data/zip/ccd_lea_059_2223_l_1a_083023.zip",
    2022: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_059_2122_l_1a_071722.zip",
    2021: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_059_2021_l_1a_080621.zip",
    2020: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_059_1920_l_1a_082120.zip",
    2019: "https://nces.ed.gov/ccd/data/zip/ccd_lea_059_1819_l_1a_091019.zip",
    2018: "https://nces.ed.gov/ccd/Data/zip/ccd_lea_059_1718_l_1a_083118.zip",
    2017: "https://nces.ed.gov/ccd/" + \
        "data/zip/ccd_lea_059_1617_l_2a_11212017.zip",
    2016: "https://nces.ed.gov/ccd/" + \
        "data/zip/CCD_LEA_059_1516_W_1a_011717_csv.zip",
    2015: "https://nces.ed.gov/ccd/" + \
        "data/zip/ccd_lea_059_1415_w_0216161a_txt.zip"
}
STAFF_FOLDER = "./data/nonfiscal/staff"

for year, url in staff_urls.items():
    download_and_unzip(url, "staff_" + str(year) + ".csv", STAFF_FOLDER)

# Remove the unneeded file formats.
for filename in os.listdir(STAFF_FOLDER):
    filepath = os.path.join(STAFF_FOLDER, filename)
    if not (filename.endswith('.csv') or filename.endswith('.txt')):
        os.remove(filepath)

# %%
######################################################################
# Download files for nonfiscal/whole (everybody (years 2014 and earlier))
######################################################################
whole_urls_csv = {
    2014: "https://nces.ed.gov/ccd/data/zip/ag131a_supp_txt.zip",
    2013: "https://nces.ed.gov/ccd/data/zip/ag121a_supp_txt.zip",
    2012: "https://nces.ed.gov/ccd/data/zip/ag111a_txt.zip",
    2011: "https://nces.ed.gov/ccd/data/zip/ag102a_txt.zip",
    2010: "https://nces.ed.gov/ccd/data/zip/ag092a_txt.zip",
    2009: "https://nces.ed.gov/ccd/data/zip/ag081a_txt.zip",
    2008: "https://nces.ed.gov/ccd/data/zip/ag071b_txt.zip"
}
whole_urls_fwf = {
    2007: "https://nces.ed.gov/ccd/data/zip/ag061c_dat.zip",
    2006: "https://nces.ed.gov/ccd/data/zip/ag051a_dat.zip",
    2005: "https://nces.ed.gov/ccd/data/zip/ag041c_dat.zip",
    2004: "https://nces.ed.gov/ccd/data/zip/ag031b_dat.zip",
    2003: "https://nces.ed.gov/ccd/data/zip/ag021a_dat.zip",
    2002: "https://nces.ed.gov/ccd/data/zip/ag011a_dat.zip",
    2001: "https://nces.ed.gov/ccd/data/zip/ag001a_dat.zip",
    2000: "https://nces.ed.gov/ccd/data/zip/ag991b_dat.zip",
    1999: "https://nces.ed.gov/ccd/data/zip/ag981c_dat.zip",
    1998: "https://nces.ed.gov/ccd/data/zip/pau97data.zip",
    1997: "https://nces.ed.gov/ccd/data/zip/pau96data.zip",
    1996: "https://nces.ed.gov/ccd/data/zip/pau95data.zip",
    1995: "https://nces.ed.gov/ccd/data/zip/pau94datr.zip",
    1994: "https://nces.ed.gov/ccd/data/zip/pau93data.zip",
    1991: "https://nces.ed.gov/ccd/data/zip/pau90data.zip",
    1990: "https://nces.ed.gov/ccd/data/zip/pau89data.zip",
    1989: "https://nces.ed.gov/ccd/data/zip/pau88data.zip",
    1988: "https://nces.ed.gov/ccd/data/zip/pau87data.zip",
    1987: "https://nces.ed.gov/ccd/data/zip/pau86data.zip"
}
WHOLE_FOLDER = "./data/nonfiscal/whole"

for year, url in whole_urls_csv.items():
    download_and_unzip(url, "whole_" + str(year) + ".csv", WHOLE_FOLDER)

for year, url in whole_urls_fwf.items():
    download_and_unzip(url, "whole_" + str(year) + ".txt", WHOLE_FOLDER)

# For years 1993 and 1992, it wasn't worth it to figure out how to unzip
# from within Python (not worth it right now). I get an error that
# "Compression requires the (missing) lzma module" and
# "That compression method is not supported".
#
# Instead, I just manually downloaded the two files
# "https://nces.ed.gov/ccd/data/zip/pau92data.zip"
# "https://nces.ed.gov/ccd/data/zip/pau91data.zip"
# and used the terminal with command "unzip file.zip"
# and changed their names to
# whole_1993.txt
# whole_1992.txt

# Remove the unneeded file formats.
for filename in os.listdir(WHOLE_FOLDER):
    filepath = os.path.join(WHOLE_FOLDER, filename)
    if not (filename.endswith('.csv') or filename.endswith('.txt')
            or filename.endswith('.DAT') or filename.endswith('.dat')):
        os.remove(filepath)

# %%
######################################################################
# Download files for fiscal
######################################################################

###
###
### NEED TO UPDATE THESE
### why?
###

fiscal_urls = {
    2022: "https://nces.ed.gov/ccd/Data/zip/sdf22_1a.zip",
    2021: "https://nces.ed.gov/ccd/Data/zip/sdf21_1a.zip",
    2020: "https://nces.ed.gov/ccd/Data/zip/sdf20_1a.zip",
    2019: "https://nces.ed.gov/ccd/Data/zip/sdf19_2a.zip",
    2018: "https://nces.ed.gov/ccd/data/zip/sdf18_1a.zip",
    2017: "https://nces.ed.gov/ccd/data/zip/sdf17_1a.zip",
    2016: "https://nces.ed.gov/ccd/data/zip/Sdf16_1a.zip",
    2015: "https://nces.ed.gov/ccd/data/zip/sdf15_1a.zip",
    2014: "https://nces.ed.gov/ccd/data/zip/sdf14_1a_flat.zip",
    2013: "https://nces.ed.gov/ccd/data/zip/sdf13_1a.zip",
    2012: "https://nces.ed.gov/ccd/data/zip/sdf121a.zip",
    2011: "https://nces.ed.gov/ccd/data/zip/sdf11_1a_txt.zip",
    2010: "https://nces.ed.gov/ccd/data/zip/SDF101a_txt.zip",
    2009: "https://nces.ed.gov/ccd/data/zip/SDF091a_txt.zip",
    2008: "https://nces.ed.gov/ccd/data/zip/sdf081a_txt.zip",
    2007: "https://nces.ed.gov/ccd/data/zip/sdf071a.txt.zip",
    2006: "https://nces.ed.gov/ccd/data/zip/sdf061a.txt.zip",
    2005: "https://nces.ed.gov/ccd/data/zip/sdf051a.txt.zip",
    2004: "https://nces.ed.gov/ccd/data/zip/sdf041b.txt.zip",
    2003: "https://nces.ed.gov/ccd/data/zip/sdf031b.txt.zip",
    2002: "https://nces.ed.gov/ccd/data/zip/sdf021ctxt.zip",
}
FISCAL_FOLDER = "./data/fiscal"

for year, url in fiscal_urls.items():
    download_and_unzip(url, "fiscal_" + str(year) + ".csv", FISCAL_FOLDER)

# Files for 2001 and earlier are not zipped. So deal.
fiscal_urls_flat = {
    2001: "https://nces.ed.gov/ccd/data/txt/sdf011d.txt",
    2000: "https://nces.ed.gov/ccd/data/txt/sdf001d.txt",
    1999: "https://nces.ed.gov/ccd/data/txt/sdf991c.txt",
    1998: "https://nces.ed.gov/ccd/data/txt/sdf981e.txt",
    1997: "https://nces.ed.gov/ccd/data/txt/sdf97d1a.txt",
    1996: "https://nces.ed.gov/ccd/data/txt/sdf96c1b.txt",
    1995: "https://nces.ed.gov/ccd/data/txt/sdf95c1d.txt",
    1992: "https://nces.ed.gov/ccd/data/txt/sdf921a.txt",
    1990: "https://nces.ed.gov/ccd/data/txt/sdf901a.txt"
}

for year, url in fiscal_urls_flat.items():
    just_download(url, "fiscal_" + str(year) + ".csv", FISCAL_FOLDER)

# Remove the unneeded file formats.
for filename in os.listdir(FISCAL_FOLDER):
    filepath = os.path.join(FISCAL_FOLDER, filename)
    if not (filename.endswith('.csv') or filename.endswith('.txt')
            or filename.endswith('.DAT') or filename.endswith('.dat')):
        os.remove(filepath)

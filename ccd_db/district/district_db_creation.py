'''
John Sherrill
Dec 25, 2024

This script creates a sqlite3 database that contains all the nonfiscal district
level data from the CCD.

Four tables:
- membership
- staff
- directory
- state

'''

# %%

import sqlite3

# Creates a new database file if it doesn't exist
conn = sqlite3.connect('district.db')
cursor = conn.cursor()

# Create the tables
# maybe we can get rid of the encoding parameter?
with open('district_schema.sql', 'r', encoding="utf-8") as sql_file:
    sql_script = sql_file.read()

cursor.executescript(sql_script)
conn.commit()

conn.close()

# %%

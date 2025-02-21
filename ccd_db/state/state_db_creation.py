'''
John Sherrill
Feb 16, 2025

This script creates a sqlite3 database that contains all the state level data
in the CCD dataset.

Four tables:
- staff
- directory
- membership
- fiscal
'''

# %%

import sqlite3

# Creates a new database file if it doesn't exist
conn = sqlite3.connect('data/state.db')
cursor = conn.cursor()

# Create the tables
# maybe we can get rid of the encoding parameter?
with open('state_schema.sql', 'r', encoding="utf-8") as sql_file:
    sql_script = sql_file.read()

cursor.executescript(sql_script)
conn.commit()

conn.close()

# %%

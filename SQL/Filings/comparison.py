"""
This is an incomplete file
"""
import csv
import os
import openpyxl as xl

dl_from_mngmt = xl.load_workbook(os.path.join(os.environ['userprofile'], 'downloads', 'Invoice 278 Details.xlsx'))
with open(os.path.join(os.environ['userprofile'], 'downloads', 'sql_list.csv')) as f:
    sqlf = csv.reader(f)
    for row in f.readlines():
        ar = row[2]

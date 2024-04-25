import snowflake.connector as sf
import pandas as pd
from pathlib import Path
import os
import time
import logging
import openpyxl
import datetime
import csv
import re
# print(os.listdir('C:\\Users\\hyh399\\OneDrive - Sherwin-Williams\\Desktop\\repos\\audits-1'))
# Snowflake connection setup
# login, utilize the mdm_audit schema
file_name = 'file://C:\\Users\\hyh399\\OneDrive - Sherwin-Williams\\Desktop\\repos\\audits-1\\puttest.csv'
connection = sf.connect(user='hailey.y.hoyat@sherwin.com',
                        password='',
                        account='sherwin1.east-us-2.azure',
                        warehouse='DIAD_WH',
                        database='DIAD',
                        schema='LACG_REPORT',
                        role='DW_DEVELOPER_ROLE',
                        authenticator='EXTERNALBROWSER')

conn = connection.cursor()
conn.execute("USE DATABASE odsd")
conn.execute("USE SCHEMA odsd.mdm_audit") 
# conn.execute("PUT 'file://C:\\\\Users\\\\hyh399\\\\OneDrive - Sherwin-Williams\\\\Desktop\\\\repos\\\\audits-1\\\mdm_audit_detail_emea_20240425.csv' @HAILEY_TEST") 
# conn.execute("PUT 'file://C:\\\\Users\\\\hyh399\\\\OneDrive - Sherwin-Williams\\\\Desktop\\\\repos\\\\audits-1\\\mdm_audit_header_emea_20240425.csv' @HAILEY_TEST") 
# conn.execute("COPY INTO MDM_AUDIT_DETAIL FROM @HAILEY_TEST/mdm_audit_detail_emea_20240425.csv.gz ON_ERROR = CONTINUE")
conn.execute("COPY INTO MDM_AUDIT_HEADER FROM @HAILEY_TEST/mdm_audit_header_emea_20240425.csv.gz ON_ERROR = CONTINUE")
print("script completed")
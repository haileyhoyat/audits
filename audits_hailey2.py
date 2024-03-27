import snowflake.connector as sf
import pandas as pd
from pathlib import Path
import os
import logging
# from snowflake.core import Root
# from snowflake.core.database import Database

# Snowflake connection setup
# login, utilize the mdm_audit schema
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

# query a table
# try:
#     for (SW_REGION, AUDIT_CODE, FILE_HEADER, LOAD_DATE) in conn.execute("SELECT SW_REGION, AUDIT_CODE, FILE_HEADER, LOAD_DATE FROM audit_header"):
#         print('{0}, {1}, {2}, {3}'.format(SW_REGION, AUDIT_CODE, FILE_HEADER, LOAD_DATE))
# finally:
#     conn.close()

# insert data into a table
try: 
    conn.execute(
        "INSERT INTO audit_header(SW_REGION, AUDIT_CODE, FILE_HEADER, LOAD_DATE) VALUES " + 
        "('LACG', '41ZPE001', 'Error Message|Organization Code|Batch Number|Batch Status|Plan Start Date|Actual Start Date|Actual Completion Date|Item Number|Line Number|Plan Quantity|WIP Plan Quantity|Actual Quantity|Transaction Quantity|Detail UOM|Item Status|Line Source|Applied Item Template', '2024-03-26 00:00:00')"
    )
finally:
    conn.close()


print("script commpleted")
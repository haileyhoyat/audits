import snowflake.connector as sf
import pandas as pd
from pathlib import Path
import os
import logging
import openpyxl
import datetime

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

#table for audit file headers
table1 = []
#table for audit file details
table2 = []
# #path to LACG folder
path = "Z:\Program Files\MDM\MDM Global System\LACG\Audits\Reports"

#get list of all LACG audit folders
audit_folders = os.listdir(path)

#For each audit_folder in LACG:
for audit_folder in audit_folders:
    table2.clear()
    #headers for the audit code
    header_row = []
    #row details that will be inserted into table2[] 
    row_detail = []
    #data rows from an individual audit file 
    row_detail_data = ''
          
    #get all audit files in this audit folder
    audit_files = []
    specific_audit_folder_path = path+"\\" + audit_folder
    try:
        audit_folder_files = os.listdir(specific_audit_folder_path)
        for file in audit_folder_files:
            if '.xlsx' in file:
                audit_files.append(specific_audit_folder_path + '\\' + file)
    except:
        print("Could not open folder" + specific_audit_folder_path)
        
    # if audit_files is empty, no .xlsx files found in this audit_folder, continue to next audit_folder
    # else, there are audit files for this audit_folder contained in audit_files
    if not audit_files:
        continue
    else:
        
        for i in range(len(audit_files)):
    
            # print(audit_files[i])
            
            #open the audit_files[i]
            wrkbk = openpyxl.load_workbook(audit_files[i])     
            sheet = wrkbk.active 
            
            #iterate through audit_files[i] rows   
            for j, row in enumerate(sheet.values):
                
                #first row in audit_files[i] is header row
                if j == 0:
                    
                    header_row.clear()
                    header_row.append(audit_folder) 
                    
                    #iterate through each column in the header row to get the headers in audit_files[i]
                    for value in row:
                        try: 
                            header_row.append(value)
                        except TypeError:
                            header_row.append("Void value")
                       
                    #check if header for this audit code is already in table1[]
                    if len(table1) == 0:
                        table1.append(header_row)
                    else:
                        audit_code_exists = False                                    
                        for audit_code in table1:
                            if audit_code[0] == audit_folder:
                                audit_code_exists = True
                        
                        if audit_code_exists:
                            for table1_ac in table1:
                                if table1_ac[0] == audit_folder:
                                    for x in range(len(table1[1::])):
                                        if header_row[x] == table1_ac[x]:
                                            continue
                                        else:
                                            print("Discrepency in column rows for current audit_folder")
                                            print('Audit Folder: '+ audit_folder)
                                            print('Audit file path: ' + audit_files)
                                            print("table1_row: " + table1_ac)
                                            print("header_row: " + header_row)
                                                        
                        else:                            
                            table1.append(header_row[:])
                
                # the remaining rows in audit_files[i] are data rows
                # aggregate remaininng data rows into row_detail_data
                # remember, row_detail_data is a field that will hold all data from all data rows in the audit_file 
                
                #iterate through each column in the row to get the row details    
                else:
                                           
                    row_detail.clear()
                    row_detail_data = ''
                    
                    for value in row:
                        if value is None:
                            row_detail_data += 'None|'
                        else:
                            row_detail_data += str(value) + '|'
                    
                    row_detail.append('LACG') #region
                    row_detail.append(audit_folder) #audit code   
                    row_detail.append(audit_files[i].split(audit_folder)[1]) # file name
                    row_detail.append(j) # sequence file
                    row_detail.append(row_detail_data) # data from all data rows in the audit file
                    row_detail.append(str(datetime.datetime.now())) # file date
                    row_detail.append(str(datetime.datetime.now())) # upload date
                                       
                    table2.append(row_detail[:])
                    
    # insert data from this audit file into snowflake
    for i in table2:                           
        # print(tuple(i))
        record = str(tuple(i))
        conn.execute(
                "INSERT INTO audit_detail(SW_REGION, AUDIT_CODE, FILE_NAME, FILE_LINE, FILE_DETAIL, FILE_DATE, LOAD_DATE) VALUES " + 
                record
            )
    
print("script completed")
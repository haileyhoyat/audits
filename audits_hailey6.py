import snowflake.connector as sf
import pandas as pd
from pathlib import Path
import os
import time
import logging
import openpyxl
import datetime
import csv

# Snowflake connection setup
# login, utilize the mdm_audit schema
# connection = sf.connect(user='hailey.y.hoyat@sherwin.com',
#                         password='',
#                         account='sherwin1.east-us-2.azure',
#                         warehouse='DIAD_WH',
#                         database='DIAD',
#                         schema='LACG_REPORT',
#                         role='DW_DEVELOPER_ROLE',
#                         authenticator='EXTERNALBROWSER')

# conn = connection.cursor()
# conn.execute("USE DATABASE odsd")
# conn.execute("USE SCHEMA odsd.mdm_audit")

#table for headers 
table1 = []
#table for row details
table2 = []
# #path to LACG folder
path = "Z:\Program Files\MDM\MDM Global System\LACG\Audits\Reports"

#get list of all LACG audit folders
audit_folders = os.listdir(path)

#For each audit_folder in LACG:
for audit_folder in audit_folders:
    #print(audit_folder)
    # at any given time table1[] will hold headers from only a single audit folder, therefore for each iteration clear out table1[] before extracting data
    table1.clear()
    # at any given time table2[] will hold data from only a single audit folder, therefore for each iteration clear out table2[] before extracting data
    table2.clear()
    #headers for the audit code
    header_row = []
    #row details that will be inserted into table2[] 
    row_detail = []
          
    #get all audit files in this audit folder
    audit_files = []
    specific_audit_folder_path = path+"\\" + audit_folder
    try:
        audit_folder_files = os.listdir(specific_audit_folder_path)
        for file in audit_folder_files:
            if '.xlsx' in file:
                audit_files.append(specific_audit_folder_path + '\\' + file)
    except:
        print("Could not open folder " + specific_audit_folder_path)
        
    # if audit_files is empty, no .xlsx files found in this audit_folder, continue to next audit_folder
    # else, there are audit files for this audit_folder contained in audit_files
    if not audit_files:
        continue
    else:
        
        for i in range(len(audit_files)):
            #print(audit_files[i])
            #today's date
            current_date = datetime.datetime.today()
            current_date_str = datetime.datetime.strftime(current_date, '%y-%m-%d %H:%M:%S')
            yesterday_date = current_date - datetime.timedelta(days = 1)
            set_date = datetime.datetime.strptime('24-04-15', '%y-%m-%d')            
                      
            #date of the audit file
            file_date = audit_files[i].split(audit_folder)[1].split('-')[1][-7::]
            file_date_formated = str(file_date[0:2]+'-'+file_date[2:4]+'-'+file_date[4:6])
            #file time
            file_time = audit_files[i].split(audit_folder)[1].split('-')[2].split(".xlsx")[0].strip(" ")
            file_time_formated = str(file_time[0:2]+':'+file_time[2:4]+':'+file_time[4:6])
            try:                
                file_date = datetime.datetime.strptime(file_date_formated + " " + file_time_formated, '%y-%m-%d %H:%M:%S')
            except:
                continue
            
            # now compare with yesterday's date 
            # only extract data from this audit file if the file is a delta from yesterday or this morning
            # (i.e. Any files that may have been added from yesterday)
            if (file_date > set_date) or (file_date == set_date):
                # print(audit_folder)
                #open the audit_files[i]
                wrkbk = openpyxl.load_workbook(audit_files[i])     
                sheet = wrkbk.active 
                
                #iterate through audit_files[i] rows   
                for j, row in enumerate(sheet.values):
                    
                    
                    #first row in audit_files[i] is header row
                    if j == 0:
                        #continue
                        header_row.clear()
                                            
                        #iterate through each column in the header row to get the headers in audit_files[i]
                        header_row.append('LACG') #region
                        header_row.append(audit_folder) #audit code   
                        header_row.append(audit_files[i].split(audit_folder)[1][1::]) # file name
                        header_row.append(j) # file line
                        header_row.append(str(file_date)) # file date
                        
                        for value in row:
                            if value is None:
                                header_row.append("")
                            else:
                                header_row.append(str(value))
                        
                        # add values for the remaining fields that are blank. 
                        # Need 156 fields total. 
                        # Remember, last field is the upload_date
                        # zero based indexing
                        
                        #print(len(row_detail))
                        
                        while (len(header_row) < 155):
                            header_row.append("")
                        
                        header_row.append(current_date_str)
                        
                        table1.append(header_row[:])
                    
                    # the remaining rows in audit_files[i] are data rows
                    # aggregate remaininng data rows into row_detail_data
                    # remember, row_detail_data is a field that will hold all data from all data rows in the audit_file 
                    
                    #iterate through each column in the row to get the row details    
                    else:
                                    
                                
                        row_detail.clear()
                        
                        row_detail.append('LACG') #region
                        row_detail.append(audit_folder) #audit code   
                        row_detail.append(audit_files[i].split(audit_folder)[1][1::]) # file name
                        row_detail.append(j) # file line
                        row_detail.append(str(file_date)) # file date
                        
                        for value in row:
                            if value is None:
                                row_detail.append("")
                            else:
                                if isinstance(value, str):                                
                                    row_detail.append(str(value.replace("'","").replace("\"","")))
                                else:
                                    row_detail.append(value)
                        
                        # add values for the remaining fields that are blank. 
                        # Need 156 fields total. 
                        # Remember, last field is the upload_date
                        # zero based indexing
                        
                        #print(len(row_detail))
                        
                        while (len(row_detail) < 155):
                            row_detail.append("")
                        
                        row_detail.append(current_date_str)
                                    
                        table2.append(row_detail[:])
            else:
                # print("this file is not a delta")
                continue
    
    filename = 'test.csv'

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(table2)     

print("script completed")
   
    # # insert header data from this audit file into snowflake
    # for i in table1:
    #     try:                           
    #         # print(tuple(i))
    #         header = str(tuple(i))
    #         conn.execute(
    #             "INSERT INTO mdm_audit_header VALUES " + header
    #         )
    #     except:
    #         continue
    
    # count = 0       
    # # insert row detail data from this audit file into snowflake
    # for i in table2:
    #     try:                           
    #         # print(tuple(i))
    #         if count < 100:
    #             record = str(tuple(i))
    #             conn.execute(
    #                 "INSERT INTO mdm_audit_detail VALUES " + record
    #             )
    #             count += 1
    #     except:
    #         continue
    
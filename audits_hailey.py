import snowflake.connector as sf
import pandas as pd
from pathlib import Path
import os
# import glob
import logging
import openpyxl
import datetime

#Surender's idea with LACG only for now

'''
Step 1
Get xlsx files

Step 2
Convert to csv

Step 3
Create two tables:
Table 1: table containing headers for each audit code. Each Audit code will have different headers, need to keep track of them.
Table 2: table containing all details from a row/file?

Step 4:
In snowflake


'''
#table for audit file headers
table1 = []
#table for audit file details
table2 = []
#path to LACG folder
path = "Z:\Program Files\MDM\MDM Global System\LACG\Audits\Reports"

#get list of all LACG audit folders
audit_folders = os.listdir(path)

#for each audit_folder, check for audit files (will be an .xlsx file)
#put full path for each .xlsx files into audit_file_paths
audit_file_paths = []

for audit_folder in audit_folders:
    
    header_row = []            
    header_row.append(audit_folder)
    
    row_detail = []
    row_detail_data = []
    
    #get all files in this audit folder
    audit_file_paths = []
    specific_audit_folder_path = path+"\\" + audit_folder
    try:
        audit_folder_files = os.listdir(specific_audit_folder_path)
        for file in audit_folder_files:
            if '.xlsx' in file:
                audit_file_paths.append(specific_audit_folder_path + '\\' + file)
    except:
        print("Could not open folder" + specific_audit_folder_path)
        
    # if audit_file_paths is empty, no .xlsx files found in this audit_folder, continue to next audit_folder
    # else, now that you have all the files in this audit folder, now get data needed for table1 and table2
    if not audit_file_paths:
        continue
    else:
        for i in range(len(audit_file_paths)):
            
            #open the audit file
            wrkbk = openpyxl.load_workbook(audit_file_paths[i])     
            sheet = wrkbk.active         
            
            # i refers to the file in the audit folder
            # j refers to the row in a specific audit file
            
            # first file in audit_file_paths, therefore need to retrieve headers in file for table1[]
            if i == 0 :
                
                # header_row = []            
                # header_row.append(audit_folder)
                
                # row_detail = []
                # row_detail_data = []
                
                for j, row in enumerate(sheet.values):
                    #first row in file is header row
                    if j == 0: 
                        #iterate through each column in the header row to get the headers
                        for value in row:
                            try: 
                                header_row.append(value)
                            except TypeError:
                                continue
                        #add header row to table1[]
                        table1.append(header_row)
                    
                    # all following rows in the file are data rows
                    else:
                        #iterate through each column in the row to get the row details
                        row_detail.clear()
                        row_detail_data.clear()
                        for value in row:
                            try: 
                                row_detail_data.append(value)
                            except TypeError:
                                row_detail_data.append('Cannot append value from file')
                        
                        #add row_detail to table2[]
                        row_detail.append(audit_folder) #audit code
                        row_detail.append(audit_file_paths[i].split(audit_folder)[1]) # file name
                        row_detail.append(len(table2)) # sequence file
                        row_detail.append(row_detail_data) # row details
                        row_detail.append(str(datetime.datetime.now())) # date upload
                        
                        table2.append(row_detail)
                        
                        
            # not the first file in audit_file_paths, therefore need to validate header in file from table1[]                   
            else:
                #continue
                
                header_row.clear()            
                header_row.append(audit_folder)
                
                for j, row in enumerate(sheet.values):
                    
                    #first row in file is header row
                    if j == 0: 
                        
                        #iterate through each column in the header row to get the headers
                        for value in row:
                            try: 
                                header_row.append(value)
                            except TypeError:
                                continue
                        
                        #now using header_row, check if there's a record in table[1] noting down the headers for the audit code
                        for table1_row in table1:
                            if table1_row[0] == audit_folder:
                                for x in range(len(table1[1::])):
                                    if header_row[x] == table1_row[x]:
                                        continue
                                    else:
                                        print("Discrepency in column rows for current audit_folder")
                                        print('Audit Folder: '+ audit_folder)
                                        print('Audit file path: ' + audit_file_paths)
                                        print("table1_row: " + table1_row)
                                        print("header_row: " + header_row)
                        print("Found a header in table1[] that matches the audit_folder")
                    
                    # all following rows in the file are data rows
                    else:
                        #iterate through each column in the row to get the row details
                        row_detail.clear()
                        row_detail_data.clear()
                                                
                        for value in row:
                            try: 
                                row_detail_data.append(value)
                            except TypeError:
                                row_detail_data.append('Cannot append value from file')
                        
                        #add row_detail to table2[]              
                        row_detail.append(audit_folder) #audit code
                        row_detail.append(audit_file_paths[i].split(audit_folder)[1]) # file name
                        row_detail.append(len(table2)) # sequence file
                        row_detail.append(row_detail_data) # row details
                        row_detail.append(str(datetime.datetime.now())) # upload date
                        
                        table2.append(row_detail)

print("script completed")
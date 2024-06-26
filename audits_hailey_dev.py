import snowflake.connector as sf
import os
import shutil
import openpyxl
import datetime
import csv
import re

#---------Utility needs such as logging into snowflake and setting inital variables-------------------------------------------------------------#

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


#table for headers 
table1 = []
#table for row details
table2 = []
#table for mdm_audit_codes
audit_folders = []

#today's date
current_date = datetime.datetime.today()
current_date_str = datetime.datetime.strftime(current_date, '%y-%m-%d %H:%M:%S')
current_date_str_file = datetime.datetime.strftime(current_date, '%y%m%d')

#yesterday's date
yesterday_date = current_date - datetime.timedelta(days = 1)

# a set date you can choose
set_date = datetime.datetime.strptime('24-04-26', '%y-%m-%d') 

# query the MDM_AUDIT_CONFIG for each region code to create path to each audit folder
# auidt folder path format: Z:\Program Files\MDM\MDM Global System\{REGION}\Audits\Reports\{AUDIT_CODE}
try:
    for (REGION, CODE) in conn.execute("SELECT REGION, CODE FROM MDM_AUDIT_CONFIG"):
        #print('{0}, {1}'.format(REGION, CODE))        
        audit_folders.append('Z:\Program Files\MDM\MDM Global System\{0}\Audits\Reports\{1}'.format(REGION, CODE))
finally:
    conn.close()

#today's header and detail files will be named with today's date
mdm_audit_header = 'mdm_audit_header_{0}.csv'.format(current_date_str_file)
mdm_audit_detail = 'mdm_audit_detail_{0}.csv'.format(current_date_str_file)

#---------Scrape data from newly added audits-------------------------------------------------------------#

#For each audit_folder in audit_folders[]:
for audit_folder in audit_folders:    
    #print(audit_folder)
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
    
    try:
        audit_folder_files = os.listdir(audit_folder)
        for file in audit_folder_files:
            # print(audit_folder + '\\' + file)
            if '.xlsx' in file:
                #file = file.replace("")
                audit_files.append(audit_folder + '\\' + file)
    except:
        print("Could not open folder " + audit_folder)
        
    # if audit_files is empty, no .xlsx files found in this audit_folder, continue to next audit_folder
    # else, there are audit files for this audit_folder contained in audit_files
    if not audit_files:
        continue
    else:
        for i in range(len(audit_files)):                       
            
            #date of the audit file
            file_date = audit_files[i].split('.xlsx')[0][-15:-9:]
            file_date_formated = str(file_date[0:2]+'-'+file_date[2:4]+'-'+file_date[4:6])
            #file time
            file_time = audit_files[i].split('.xlsx')[0][-6::]
            file_time_formated = str(file_time[0:2]+':'+file_time[2:4]+':'+file_time[4:6])                  
            file_date = datetime.datetime.strptime(file_date_formated + " " + file_time_formated, '%y-%m-%d %H:%M:%S')

            
            # now compare with yesterday's date 
            # only extract data from this audit file if the file is a delta from yesterday or this morning
            # (i.e. Any files that may have been added from yesterday)
            if (file_date > current_date) or (file_date == current_date):
                # print(audit_folder)
                #open the audit_files[i]
                try:
                    wrkbk = openpyxl.load_workbook(audit_files[i])     
                    sheet = wrkbk.active
                    #print(audit_files[i] + " was opened")
                except:
                    continue 
                
                #iterate through audit_files[i] rows   
                for j, row in enumerate(sheet.values):
                    #first row in audit_files[i] is header row
                    if j == 0:
                        #continue
                        header_row.clear()
                                            
                        #iterate through each column in the header row to get the headers in audit_files[i]
                        header_row.append(audit_files[i].split('\\')[4]) #region
                        header_row.append(audit_folder.split('\\')[7]) #audit code   
                        header_row.append(audit_files[i].split('Reports')[1].split('\\')[2]) # file name
                        header_row.append(j) # file line
                        header_row.append(str(file_date)) # file date
                        
                        for value in row:
                            if isinstance(value, str):
                                value = re.sub(r'[^\x00-\x7F]', '', value) #remove non ASCIII characters
                                value = value.replace("'","").replace("\"","").replace(",","") #remove apostraophes and commas
                                           
                                row_detail.append(str(value))
                            else:
                                row_detail.append(value)
                        
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
                        
                        row_detail.append(audit_files[i].split('\\')[4]) #region
                        row_detail.append(audit_folder.split('\\')[7]) #audit code   
                        row_detail.append(audit_files[i].split('Reports')[1].split('\\')[2]) # file name
                        row_detail.append(j) # file line
                        row_detail.append(str(file_date)) # file date
                        
                        for value in row:
                            if value is None:
                                row_detail.append("")
                            else:
                                if isinstance(value, str):
                                    value = re.sub(r'[^\x00-\x7F]', '', value) #remove non ASCIII characters
                                    value = value.replace("'","").replace("\"","").replace(",","") #remove apostraophes and commas
                                    if len(value) > 999: # make sure value is no longer than 1000 characters
                                        value = value[0:950:]           
                                    row_detail.append(str(value))
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

#--------- Create csv files containing header and detail data for the day -------------------------------------------------------------#

    #write data into today's header and detail files
    try:           
        with open(mdm_audit_detail, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(table2)

        with open(mdm_audit_header, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(table1)
    except Exception as error:
        print(error) 

# make a copy of today's audit header and detail by renaming those files a generic name
# this is because Snowflake will expect a consistent file name to upload
shutil.copyfile(mdm_audit_header, 'mdm_audit_header.csv')
shutil.copyfile(mdm_audit_detail, 'mdm_audit_detail.csv')

#---------Load today's header and detail files into snowflake -------------------------------------------------------------#

# connect to Snowflake again becuase the connection will timeout by the time the scrape is completed. 
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

#upload today's audit data file into internal snowflake storage
#load the audit file data from internal storage into mdm_audit_header and mdm_audit_detail
#log the errors into MDM_AUDIT_HEADER_ERRORS and MDM_AUDIT_DETAIL_ERRORS
conn.execute("REMOVE @HAILEY_TEST/mdm_audit_detail.csv") 
conn.execute("REMOVE @HAILEY_TEST/mdm_audit_header.csv")
conn.execute("PUT 'file://C:\\\\Users\\\\hyh399\\\\OneDrive - Sherwin-Williams\\\\Desktop\\\\repos\\\\audits-1\\\\mdm_audit_detail.csv' @HAILEY_TEST") 
conn.execute("PUT 'file://C:\\\\Users\\\\hyh399\\\\OneDrive - Sherwin-Williams\\\\Desktop\\\\repos\\\\audits-1\\\\mdm_audit_header.csv' @HAILEY_TEST") 
conn.execute("COPY INTO MDM_AUDIT_DETAIL FROM @HAILEY_TEST/mdm_audit_detail.csv.gz ON_ERROR = CONTINUE FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' EMPTY_FIELD_AS_NULL = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '\"')")
conn.execute("CREATE OR REPLACE TABLE MDM_AUDIT_DETAIL_ERRORS AS SELECT * FROM TABLE(VALIDATE(MDM_AUDIT_DETAIL, JOB_ID=>'_last'))")
conn.execute("COPY INTO MDM_AUDIT_HEADER FROM @HAILEY_TEST/mdm_audit_header.csv.gz ON_ERROR = CONTINUE FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' EMPTY_FIELD_AS_NULL = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '\"')")
conn.execute("CREATE OR REPLACE TABLE MDM_AUDIT_HEADER_ERRORS AS SELECT * FROM TABLE(VALIDATE(MDM_AUDIT_HEADER, JOB_ID=>'_last'))")

#now delete the generic header and detail files
os.remove('mdm_audit_header.csv')
os.remove('mdm_audit_detail.csv')

print("script completed")
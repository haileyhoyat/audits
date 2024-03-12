import snowflake.connector as sf
import pandas as pd
from pathlib import Path
import os
# import glob
import logging

# adding to Github

# 20240306 - Notes:
# worked on Prod
# Note the different Snowflake connection using DIAD.LACG_REPORT.LACG_REPORT_L

# Snowflake connection setup
connection = sf.connect(user='mike.detmar@sherwin.com',
                        password='',
                        account='sherwin1.east-us-2.azure',
                        warehouse='DIAD_WH',
                        database='DIAD',
                        schema='LACG_REPORT',
                        role='DIAD_DEVELOPER_ROLE',
                        authenticator='EXTERNALBROWSER')

cur = connection.cursor()

# Setting up Logging
logging.basicConfig(filename='lacgAudit.log', level=logging.DEBUG, format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

# Set the output directory for Dell PC
# OUTPUT_DIR = Path(r'C:\\Temp\\output\\')

# Output Directory for Mac
OUTPUT_DIR = Path(r'/Users/wmd01s/OneDrive - Sherwin-Williams/temp')
logging.info(OUTPUT_DIR)

# If the Output folder doesn't exist, create it.
OUTPUT_DIR.mkdir(exist_ok=True)

names = {'error message': 'message',
         'error_message': 'message',
         'error/missing': 'message',
         'validation message': 'message',
         'erro message': 'message',
         'error description': 'message',
         'errormessage': 'message',
         'message': 'message',
         'organization code': 'organization',
         'org code': 'organization',
         'organization_code': 'organization',
         'org': 'organization',
         'organization': 'organization',
         'org_code': 'organization',
         'organization number': 'organization',
         'ov_organization number': 'organization',
         'ov_organization_number': 'organization',
         'organization code ': 'organization',
         'orgr': 'organization',
         'item': 'item_number',
         'item code': 'item_number',
         'ora_item_no': 'item_number',
         'item number': 'item_number',
         'ov_item_number': 'item_number',
         'ov_item number': 'item_number',
         'ora_item_number': 'item_number'}

# audit_list = []
# audit_input = open(r"C:\Users\wmd01s\OneDrive - Sherwin-Williams\pydata\LACG\audit_list.txt", 'r')
audit_input = open(r"/Users/wmd01s/Library/CloudStorage/OneDrive-Sherwin-Williams/pydata/LACG/audit_list.txt", 'r')

# reading the file
audit_input_data = audit_input.read()

# replacing end splitting the text
# when newline ('\n') is seen.
audit_list = audit_input_data.split(",\n")
# audit_list = audit_input_data.splitlines('\n')
audit_input.close()

logging.info('%s is the list of audits the script will process this time.', audit_list)

for audit_code in audit_list:

    print("Back at the top of the FOR loop for Audit codes. Audit Code that will be processed", audit_code)

    # For future state, this application should connect to Alex's smb share.
    # Use this tryPath instead:
    tryPath = '/Volumes/e$/Program Files/mdm/MDM Global System/LACG/Audits/Reports/' + audit_code

    # Getting the current working directory, and filter on just XLSX files.
    # tryPath = '/Users/wmd01s/OneDrive - Sherwin-Williams/pydata/LACG/' + audit_code
    logging.info('%s is the path to the current audit.', tryPath)

    df_list = []

    if not os.path.exists(tryPath):
        print("Checking to see....This is NOT a valid directory with tryPath: ", tryPath, audit_code)
        print("Since path doesn't exist. Go back to the Top - start with the NEXT folder.")
        logging.info('%s is %s', tryPath, 'Invalid directoryPath')
        print("tryPath was: ", tryPath)
        tryPath = ""
        print("tryPath should be reset here: ", tryPath)
        continue
    else:
        print("In the Else. This is a valid directory with tryPath: ", tryPath)
        print("In the Else. Audit Code being processed", audit_code)
        logging.info('%s is %s', tryPath, 'Valid directoryPath')
        print("In the Else. Now we will change to the directory using the tryPath variable. Before chdir(): ", tryPath)
        os.chdir(tryPath)
        print("In the Else. tryPath is now set after using chdir(): ", tryPath)

        # print("Setting directoryPath using Path.cwd(). It's this now: ", directoryPath)
        directoryPath = Path.cwd()
        print("Setting directoryPath using Path.cwd(). It's this after setting: ", directoryPath)
        print("This is the Audit Code being processed after cwd(): ", audit_code)

        print("Listing the Directory using os.listdir()")
        os.listdir(directoryPath)
        print("Audit Code being processed after the listdir()", audit_code, directoryPath)

        # Need to parse out the file extensions
        # localPC = []
        print("Listing the Directory to the localPC variable", audit_code, directoryPath)
        localPC = os.listdir(directoryPath)
        print("LocalPC: ", localPC)

        newLocalPC = []

    for each in localPC:
        new = os.path.splitext(each)[0]
        newLocalPC.append(new)

    df1 = pd.DataFrame(newLocalPC, columns=['AUDIT_NAME'])
    audits = df1['AUDIT_NAME'].tolist()
    print("Audits from PC: ", audits)

    print("About to query Snowflake", audit_code, directoryPath)

    cur.execute("""select distinct audit_name from DIAD.LACG_REPORT.LACG_REPORT_L where audit_code = (%s)""",
                (audit_code,))

    df2 = cur.fetch_pandas_all()
    sf = df2['AUDIT_NAME'].tolist()
    print("Audits from SF: ", sf)

    new_files_to_process = []

    for item in audits:
        if item not in sf:
            new_files_to_process.append(item)

    # NEED TO ADD FUNCTION THAT SKIPS BACK TO TOP IF THERE ARE NO ITEMS IN new_files_to_process
    # IF EMPTY, THIS WOULD MEAN THAT BOTH LISTS ARE THE SAME
    # SO, NO NEW FILES TO PROCESS.

    newFiles = pd.DataFrame(new_files_to_process, columns=['AUDIT_NAME'])
    print("These are the files to process. Are there any here? If not, this is where we need to break.", newFiles)

    if new_files_to_process == []:
        print("No new files found for this Audit - " + str(directoryPath), audit_code, newFiles)
        continue
    else:

        newLocalPCwithEXT = []
        for each in new_files_to_process:
            newLocalPCwithEXT.append(each + '.xlsx')
            
        # Need to parse any entries that contain "~". This may be just a Mac issue.
        newLocalPCwithEXT = [x for x in newLocalPCwithEXT if not x.startswith('~')]

        # I think that this is where we need to list all the files, put into a py List
        # Then compare that to the list from Snowflake
        # the resulting list is what should be the 'p' in this

        # p = Path.cwd().glob('*.xlsx')

        # path of the directory
        directoryPath = Path.cwd()
        print("Audit Code being processed", audit_code)

        # print("Directory Path is: ", directoryPath)

        # Comparing the returned list to empty list
        if os.listdir(directoryPath) == []:
            print("No files found for this Audit - " + str(directoryPath), audit_code)
        else:

            # Loop through all the XLSX files in the current directory

            for f in newLocalPCwithEXT:
                # get f(ile).name and append to data
                data = pd.read_excel(f)
                print("Audit Code being processed in the newLocalPCwithEXT loop", audit_code)

                # adding a column to the dataframe called AUDIT_NAME, and populating the value of f.stem
                # the filename without the extension.
                data['AUDIT_NAME'] = f
                # logging.info('%s is being added to the dataframe as the AUDIT_NAME.',f.stem)

                # Stripping the .xslx from the audit_name
                data['AUDIT_NAME'] = data['AUDIT_NAME'].str[:-5]
                # logging.info('%s is being added to the dataframe as the AUDIT_NAME.',f.stem)

                # here we rename the columns based on the Dictionary called names.
                # Use inplace=True - overwrites the existing columns.
                data.columns = data.columns.str.lower()
                data.rename(columns=names, inplace=True)

                # append the data to dataframe - df_list that was created earlier
                df_list.append(data)

                # This is just printing the dataframe called df_list to the screen.
                # df_list

                data = pd.concat(df_list, ignore_index=True)

                # Set the column headers to UPPERCASE
                data.columns = data.columns.str.upper()

                # Adding any missing columns if the source file doesn't provide them.
                col_ORGANIZATION = 'ORGANIZATION'
                col_MESSAGE = 'MESSAGE'
                col_ITEM_NUMBER = 'ITEM_NUMBER'

                # If the column names aren't present in the source file
                # Add the column namn
                # Fill the contents with "UNDEFINED"
                # The user can use this to address issues with the site to collect the correct data
                # Newly gathered data should overwrite.

                if col_ORGANIZATION not in data.columns:
                    data['ORGANIZATION'] = "UNDEFINED"
                    logging.info('%s was not found in this AUDIT_NAME: %s', col_ORGANIZATION, audit_code)

                if col_MESSAGE not in data.columns:
                    data['MESSAGE'] = "UNDEFINED"
                    logging.info('%s was not found in this AUDIT_NAME: %s', col_MESSAGE, audit_code)

                if col_ITEM_NUMBER not in data.columns:
                    data['ITEM_NUMBER'] = "UNDEFINED"
                    logging.info('%s was not found in this AUDIT_NAME: %s', col_ITEM_NUMBER, audit_code)

                # Add Audit Code - the variable is currently set at the top of the program -manually
                print("Audit Code being processed. Just before adding the AUDIT_CODE to the DF.", audit_code)
                data['AUDIT_CODE'] = audit_code
                # logging.info('Adding Audit Code to the dataframe: %s',audit_code)

                # This will re-organize and then drop ALL OTHER (UN-NEEDED) columns.
                # This makes the data consistent across all XLS files.
                data = data[['AUDIT_CODE', 'AUDIT_NAME', 'ORGANIZATION', 'MESSAGE', 'ITEM_NUMBER']]

                # Modified to handle the audit_code variable.
                data.to_csv(f"{OUTPUT_DIR}/" + "lacg_report.csv." + audit_code, index=False)
                logging.info('Dataframe for %s saved to CSV', audit_code)
                print("Just wrote the file to a csv. Audit Code being processed", audit_code)

# print('Done!')
logging.info('Audit Codes converted')

# itemAudit
Python code to transform XLS to CSV and then place into Snowflake

## Requirements
	1. SMB access via service account, or manual while testing
	2. Snowflake Access via service account, or manual while testing
	3. Azure File Storage Access (manual via GUI app. Future - Python library)
	4. Python -Initially coded in 3.9 on macOS. Needs to be downgraded to Python 3.6 for CentOS.

## High level overview of the Python code

	1. Use a defined list of audits/file folders documented in an external text file.
	2. Make a SMB connection to the Windows file share that hosts the audits/file folders. Saved in a Python List.
	3. Execute a Snowflake to query determine existing audits in Snowflake. Saved in a Python List.
	4. Compare the lists.
	5. Build a new Python list based on what's not in Snowflake.
	6. Process/Loop through Audit files not in Snowflake and store data in a Pandas Dataframe.
	7. Rename/Map column names based on Python Dictionary.
	8. Drop extra headers. Keeping only the required headers.
	9. Save Dataframe as CSV. Store on server running Python code. Future - save in Azure Blob Storage using Python library.
	10. Repeat, starting at step 1 until the end of the audit list has been reached.

## Detailed steps in the Python code

### Setup Environment
	* Define the Snowflake connection
	* Enable logging to a local file
	* Define the output directory for the CSV files, create if it doesn't exist
	* Define header mappings for the final CSV file.
	* Specify audit list text file that holds the audit names to gather when running the code.
	* Read the audit list into a Python List

### Start Tasks
	* Use a loop to:
		* Look at an audit name (folder) in the audit list
		* Gather the file names from the audit name (folder) on the source server
		* Write the file names to a list
		* Query the Snowflake server for all audit file names related to the specific audit
		* Write the files names to a list
		* Compare both lists
		* Create a new list with only the NEW files, those that are NOT already in Snowflake for that specific audit/folder

	* Use a nested loop to:
		* Create a Pandas dataframe
		* Read each file named in the new audit list
		* Add each line of the new XLS file to the dataframe
		* Repeat until all XLS files for the specifi audit have been added to the Dataframe.
		* Append the audit name (folder name) to the dataframe
		* Rename columns detailed in the header mapping dictionary.
		* Add, if missing the Org, MSG, and Item Number as Undefined
		* Drop all columns EXCEPT: 'AUDIT_CODE','AUDIT_NAME','ORGANIZATION','MESSAGE','ITEM_NUMBER'
		* Write the output to a CSV file in the following format: lacg_report.csv.AUDIT_CODE
		* Save in the output folder.
		* Back to Start. Repeat and cycle through the list to the end.


import csv
import os
import re
import datetime as dt 
from datetime import datetime, timezone
from os.path import exists
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from typing import Iterator, Dict, Optional, List


def preprocessing(path: str, run_log_file: str) -> Optional[List]:
	"""
	Argument: a string representing the path where CSV files to process are located.
	Returns: a numeric value signaling whether processing is needed or not.

	This function will consult a log containing a timestamp for the last time that CSV files were processed.
	If any files were dropped into the dir AFTER the last time the process ran, then only those files will be processed
	and the Combined.csv file will be updated.
	If the log file does not exist, this function will create and initialize it.
	"""
	f_name = path + '/' + run_log_file
	ts = datetime.now().timestamp()
	if not exists(f_name):
		with open(f_name, 'w') as f:
			writer = csv.writer(f)
			writer.writerow(['last_run'])
			writer.writerow([ts])
		return files_to_process(path, 0.0)
	else:
		with open(f_name, 'r') as f: 
			times = f.readlines() 
		lastRun = times[-1]
		lastRun = float(lastRun.split(',')[0])
		with open(f_name, 'a+') as f:
			writer = csv.writer(f)
			writer.writerow([ts])
		return files_to_process(path, lastRun)


def files_to_process(path: str, last_run: float) -> Optional[List]:
	"""
	Argument:  a string representing the path where CSV files to process are located.
			   a float representing the timestamp of the last time the process was run.
	Returns:  a list of CSV files found in the directory pointed to by the path string.
	"""
	if len(path) == 0:
		return None
	
	files = [path + '/' + f for f in os.listdir(path) if '.csv' in f]

	if last_run != 0.0:
		files = [file for file in files if os.path.getmtime(file) > last_run]

	return files


def read_from_csv(files: list) -> Dict:
	"""
	Argument: a list containing the path to a CSV file
	Returns: one row at a time as a dictionary
	"""
	combined_dict = {}
	# The regex pattern will match any filename starting with path to file, followed by two words separated by space (it will capture this substring),
	# followed by space and zero or more digits and terminated by '.csv' extension.
	# Example:  '/develop/data/Asia Prod 3.csv'  
	csv_file_pattern = r'[\.\w\/\s]*\/([A-Za-z]+\s[A-Za-z]+)\s*\d*\.csv'
	for file in files:
		p = re.compile(csv_file_pattern)
		m = p.match(file)
		env = ''
		if m == None:
			print('Skipping {} since it does not match CSV file pattern.'.format(file))
			continue
		else:
			env = m.group(1)

		with open(file, 'r') as f:
			### Add some clarification here for accounting for inconsistent header names
			columns = ['source_ip', 'count', 'events_per_second']
			reader = csv.DictReader(f, fieldnames=columns, delimiter=',')
			next(reader)
			for row in reader:
				if row['source_ip'] not in combined_dict.keys():
					combined_dict[row['source_ip']] = env
				else:
					continue

	return combined_dict


def sort_values(c_dict: dict) -> List:
	"""
	Arguments: a dictionary containing ip addresses as keys and corresponding environment as values
	Returns: a list of tuples ordered by ip address value 
	"""
	value_pairs = [(ip, c_dict[ip]) for ip in c_dict.keys()]
	# Sort based on the numeric value of the first integer in IP address
	value_pairs.sort(key=lambda x: int(x[0].split('.')[0]))
	return value_pairs


def write_to_csv(val_list: list, f_name: str) -> int:
	"""
	Argument: a list of tuples that will be used to write to csv file
	Returns: integer signaling exit status:
				0: Successfully created and wrote to Combined.txt
				1: Nothing to process
				2: Successfully updated Combined.txt

	This function will attempt to write the contents of val_list into CSV file f_name.
	If f_name does not already exist at the time the function is called, this function will create the file and then write to it.
	Otherwise, the function will merge any new information contained in val_list with the information already contained in f_name, giving
	priority to the new information found in val_list. 
	"""
	if len(val_list) == 0:
		return 1 

	if not exists(f_name):
		with open(f_name, 'w') as f:
			writer = csv.writer(f)
			writer.writerow(['Source Ip', 'Environment'])
			for row in val_list:
				writer.writerow(row)
		return 0
	else:
		combined_dict = {}
		for pair in val_list:
			if pair[0] not in combined_dict:
				combined_dict[pair[0]] = pair[1]

		with open(f_name, 'r') as f:
			reader = csv.DictReader(f)
			#Merge values with those already on the file. Note that new values will take precedence over those already on file. Thus updating the contents.
			for row in reader:
				if row['Source Ip'] not in combined_dict:
					combined_dict[row['Source Ip']] = row['Environment']

		updated_val_list = sort_values(combined_dict)

		with open(f_name, 'w') as f:
			writer = csv.writer(f)
			writer.writerow(['Source Ip', 'Environment'])
			for row in updated_val_list:
				writer.writerow(row)

		return 2


def combine_to_single_csv():
	data_path = '../data'
	out_file = data_path + '/Combined.csv'
	csv_files = preprocessing(path=data_path, run_log_file='process_time_log.csv')
	combined_dict = read_from_csv(csv_files)
	value_pairs = sort_values(c_dict=combined_dict)
	exit_status = write_to_csv(value_pairs, out_file)

	if exit_status == 0:
		print('File {} was successuflly created.'.format(out_file))
	elif exit_status == 1:
		print('No files to process.')
	elif exit_status == 2:
		print('File {} was updated with new CSV information.'.format(out_file))


default_args = { 'owner': 'cbeas', 
				 'start_date': dt.datetime(2021, 10, 30), 
				 'retries': 1, 
				 'retry_delay': dt.timedelta(minutes=5),
				}

with DAG('CombineToCsv', 
		  default_args=default_args,
		  schedule_interval=timedelta(days=1)
	) as dag:

	start_this = BashOperator(task_id='starting', 
								  bash_command='echo "Combine to single CSV process has started."',
								  )

	task = PythonOperator(task_id='CombineToSingleCSV', python_callable=combine_to_single_csv)

	complete = BashOperator(task_id='completing', 
								  bash_command='echo "Combine to single CSV process has completed."',
								  )

	start_this >> task >> complete


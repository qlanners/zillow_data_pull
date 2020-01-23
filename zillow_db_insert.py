from datetime import datetime
from sqlalchemy import create_engine, inspect
import pandas as pd
import os
from os import listdir, getcwd
from os.path import isfile, join
import sys


def insert_data(type, csv_name, log_name):
	engine = create_engine('{}://{}:{}@{}:{}/{}'.format(*sys.argv[1:]))

	db_log = open(log_name,"a+")

	max_city_data_id = engine.execute('SELECT MAX(id) FROM rentals_{}_data'.format(type)).fetchall()[0][0]

	if max_city_data_id is None:
		db_log.write('No records currently in rentals_{}_data\n'.format(type))
		max_city_data_id = 0

	db_log.write("Current max id in rentals_{}_data is {}\nWill begin inserting data with id #{}\n".format(type, max_city_data_id, max_city_data_id+1))

	data_df = pd.read_csv('{}/{}'.format(os.getenv('SUMMARY_FOLDER','data-cleaned'), csv_name))

	data_df.rename(columns={'{}'.format(type):'{}_id'.format(type)}, inplace=True)
	data_df['id'] += max_city_data_id

	db_cols = [d['name'] for d in inspect(engine).get_columns('rentals_{}_data'.format(type))]

	df_cols = list(data_df.columns)

	if len(db_cols) != len(df_cols):
		if len(db_cols) > len(df_cols):
			db_log.write("Database has more columns than DataFrame")
		else:
			db_log.write("DataFrame has more columns than Database")
		db_log.write("*****Aborting INSERT*****")
		exit()

	for i in df_cols:
		if i not in db_cols:
			db_log.write("Mismatch column name {}\n".format(i))
			db_log.write("*****Aborting INSERT*****\n\n")
			exit()

	data_df = data_df[db_cols]

	if list(data_df.columns) != db_cols:
		db_log.write('Unable to reformat {} dataframe to match rentals_{}_data table format\n'.format(type, type))
		db_log.write("*****Aborting INSERT*****\n\n")
		exit()

	db_log.write("Attempting to add {} rows of data to rentals_{}_data\n".format(data_df.shape[0], type))
	try:
		data_df.to_sql('rentals_{}_data'.format(type), engine, index=False, if_exists='append', chunksize=1000)
		db_log.write('Success\n\n')
	except:
		db_log.write('*****Failed to ingest*****\n\n')

	new_max_city_data_id = engine.execute('SELECT MAX(id) FROM rentals_{}_data'.format(type)).fetchall()[0][0]

	if new_max_city_data_id == max_city_data_id + data_df.shape[0]:
		db_log.write('Successfully added {} rows of data to rentals_{}_data.\n'.format(data_df.shape[0], type))
		db_log.write('New highest id in table is now {}\n\n'.format(new_max_city_data_id))
	else:
		db_log.write('*****ALERT: Data was inserted, but there was an error in the id numbering...\n')
		db_log.write('{} records where inserted, but max ID went from {} to {}\n\n'.format(data_df.shape[0], max_city_data_id, new_max_city_data_id))

	db_log.close()

cleaned_data_path = join(getcwd(), os.getenv('SUMMARY_FOLDER','data-cleaned'))
all_cleaned_data_files = [f for f in listdir(cleaned_data_path) if isfile(join(cleaned_data_path, f))]

state_csvs = [f for f in all_cleaned_data_files if os.getenv('STATE_SUMMARY_FILE','state-monthly') in f]
county_csvs = [f for f in all_cleaned_data_files if os.getenv('COUNTY_SUMMARY_FILE','county-monthly') in f]
city_csvs = [f for f in all_cleaned_data_files if os.getenv('CITY_SUMMARY_FILE','city-monthly') in f]

db_commit_log = "{}/{}/db_commit_report.txt".format(os.getenv('LOG_FOLDER','logs'), os.getenv('TODAYS_DATE',datetime.now().strftime("%Y-%m-%d")))

with open(db_commit_log,"w+") as db_log:
	db_log.write("Beginning database commits at: {}\n".format(datetime.now()))

for f in state_csvs:
	insert_data('state', f, db_commit_log)

for f in county_csvs:
	insert_data('county', f, db_commit_log)

for f in city_csvs:
	insert_data('city', f, db_commit_log)


with open(db_commit_log,"a+") as db_log:
	db_log.write("Finished database commits at: {}\n".format(datetime.now()))





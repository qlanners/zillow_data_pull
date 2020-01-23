"""
file name: rental_organizer.py
date created: 9/23/19
last edited: 9/27/19
created by: Quinn Lanners
description: This python file reformats Zillow Economic data pulled from the web using zillow_data_download.py.
The reformatted csvs are saved into the specified folder below, and transform the several csv files for
state, county, and city data, into a single csv file for each. The final csv file combines all of the monthly
stats for each location, giving each unique location and month pair its own row.
"""

import datetime
import math
import os
import pandas as pd
from sqlalchemy import create_engine

#import lists of file paths and column names to reformat
from zillow_paths import STATE_FILES, COUNTY_FILES, CITY_FILES, STATE_ABBREVS


#Use this to connect to homefinder database
engine = create_engine('postgresql://qlanners:MNMiracle18@localhost:5432/homefinder',echo=False)


def get_ids(type, state_file, city_county_file=None):
    """
    get_ids: gets the ids for the specified location type from the specified file(s)
            args:
                type: the location type (must be state, county, or city)
                state_file: file location of state ids
                city_county_file: file location of city ids
            returns:
                if type=='state':
                    dictionary of each state name with corresponding ID
                else:
                    nested dictionary with each state sbbreviation key having
                    a value corresponding to every county/city and ID pair in that state
    """
    if type.lower() not in ['state', 'county', 'city']:
        print('Type argument passed to get_ids is invalid. Must be either state, county, or city.')
        exit()
    try:
        state_db = pd.read_csv(state_file)
    except:
        print('State IDs file could not be loaded')
        exit()
    if type.lower() == 'state':
        state_ids = state_db[['State']]
        state_ids.index += 1
        return {y: x for x, y in state_ids.to_dict()['State'].items()}
    state_abbrevs = state_db[['Abbreviation']]
    state_abbrevs.index += 1
    state_dict = {x: (y, {}) for x, y in state_abbrevs.to_dict()['Abbreviation'].items()}
    try:
        other_ids = pd.read_csv(city_county_file)
    except:
        print('{} IDs file could not be loaded'.format(type.lower().capitalize()))
        exit()
    other_ids = other_ids[[type.lower().capitalize(), 'State_ID']]
    other_ids.index += 1
    #only keep top 5000 locations
    other_ids = other_ids.iloc[:5000]
    for i, r in other_ids.iterrows():
        state_dict[r['State_ID']][1][r[type.capitalize()]] = i

    return {y[0]: y[1] for x, y in state_dict.items()}


def rental_organizer(type, input_file, monthly_df, label, ids, report_file_name, get_months=None, go_back=0):
    """
    rental_organizer: reformats data from specified input file as new column in monthly_df summary dataframe.
            args:
                type: the location type (must be state, county, or city)
                input_file: file path containing data to reformat
                monthly_df: the reformatted dataframe to append the data too
                label: the column name under which this file paths data should be appended as to monthly_df
                ids: ids for the input type, retrieved us get_ids
                get_months: number of months back to pull from the input file
            returns:
                monthly_df with the appended column for the specified file.
                also adds any new location/month combos that did not already exist
                in monthly_df as a new rows.
    """
    report = open(report_file_name,"a+")

    if get_months != None and go_back > 0:
        print('Error: Cannot use both get_months and go_back arguments together. Choose one or the other to select months to pull.')
        exit()
    elif get_months == None and go_back < 1:
        print('Error: Must specify either the get_months or go_back argument. Specify one or the other to select months to pull.')
        exit()

    if type.lower() not in ['state', 'county', 'city']:
        print('Type argument passed to rental_organizer is invalid. Must be either state, county, or city.')
        exit()

    try:
        df = pd.read_csv(input_file, encoding='latin_1')

        # Check to make sure there is data for the months we are looking for
        if go_back > 0:
            months = df.columns.values[-go_back:] if go_back > 0 else df.columns.values[2:]
            months = [m for m in months if
                      datetime.datetime.strptime(m, "%Y-%m") > datetime.datetime.now() - datetime.timedelta(
                          days=31 * go_back)]
        elif get_months:
            months = [m for m in df.columns.values[2:] if m in get_months]

        #will story data in list to later add to monthly_df
        months_w_data = []

        #specifies the columns we want to keep from the input csv
        if type.lower() == 'state':
            df = df[['RegionName', *months]]
        else:
            if 'StateName' in df.columns.values:
                df.rename(columns={'StateName': 'State'}, inplace=True)
                df.replace(STATE_ABBREVS, inplace=True)
            df = df[['RegionName', 'State', *months]]


        #Pull data from csv for desired months, printing out a message each time there is an error.
        missed_regions = {m: [] for m in months}
        total_misses = 0
        for i, r in df.iterrows():
            for m in months:
                if not math.isnan(r[m]):
                    # If it's a state, we will match the region info, unless its puerto rico
                    if type.lower() == 'state' and 'puerto' not in r['RegionName'].lower():
                        months_w_data.append([ids[r['RegionName']], pd.to_datetime(m, format="%Y-%m"), m[:4], m[-2:], r[m]])
                    # If it's other region type, check to see if we want it (have an id for it)
                    else:
                        try:
                            months_w_data.append(
                                [ids[r['State']][r['RegionName']], pd.to_datetime(m, format="%Y-%m"), m[:4], m[-2:], r[m]])
                        except:
                            missed_regions[m].append(r['RegionName'])
                total_misses += len(missed_regions[m])

        #Put data into a dataframe and label columns
        organized = pd.DataFrame(months_w_data, columns=[type.lower(), 'date', 'year', 'month', label])

        #Refactor columns in both organized and monthly_df to ensure that the columns we merge on are all of type int
        organized['month'] = organized['month'].astype(int)
        organized['year'] = organized['year'].astype(int)
        monthly_df['month'] = monthly_df['month'].astype(int)
        monthly_df['year'] = monthly_df['year'].astype(int)
        cols = monthly_df.columns.tolist() + [label]

        #Merge organized and monthly_df on the year/month/location.
        final = pd.merge(organized, monthly_df, on=['year', 'month', type.lower()], how='outer')

        #Drop the duplicate date column created from the merge
        for i, r in final.iterrows():
            if pd.isnull(r['date_x']):
                final.at[i, 'date_x'] = r['date_y']
        final = final.drop(['date_y'], axis=1)
        final.rename(columns={'date_x': 'date'}, inplace=True)
        report.write("*"*min(math.ceil(total_misses/10),50))
        report.write('\nAdded data from {} under column name {} -----'.format(input_file, label))
        try:
            report.write("Misses: {}\n\n".format(total_misses))
        except:
            report.write('**********No months here**********\n\n')

        final.drop_duplicates(subset=[type, 'year', 'month'], keep='first', inplace=True)
        return final[cols]
    except:
        print('****\n***Unable to retrieve and reformat data for {}***\n****'.format(input_file))
        report.write('****\n***Unable to retrieve and reformat data for {}***\n****'.format(input_file))
        report.close()
        return monthly_df


def save_df(final, save_file, location_type):
    """
    save_df: saves final dataframe to csv, breaking up into multiple files if dataframe is very large.
            args:
                final: df to be saved to csv file(s)
                save_file: file name to save final to
            returns:
                Nothing
    """
    #Reset the index and then add one to the index to have first ID be 1 (not 0)
    final.reset_index(drop=True, inplace=True)
    final.index += 1
    #Rename index column as ID
    final.index.name = 'id'

    print('{} rows before dropping duplicates'.format(final.shape[0]))
    final.drop_duplicates(subset=[location_type, 'year', 'month'], keep='first', inplace=True)
    print('{} rows after dropping duplicates'.format(final.shape[0]))

    #Determine how many files to save the dataframe into, capping each file at 500000 rows
    breaks = math.ceil(final.shape[0] / 500000)

    #Save df to csv file(s)
    print('Creating ' + str(breaks) + ' csv files')
    for i in range(breaks):
        export = final.iloc[(500000 * i):(500000 * (i + 1))]
        csv_path = save_file + str(i) + '.csv'
        export.to_csv(csv_path)
        print('Created {}'.format(csv_path))
    print('Done saving to csv(s)')


#Create IDs and intitialize empty dataframes for each location type
state_ids = get_ids('state', state_file='ids/states.csv')
state_df = pd.DataFrame(columns=['state', 'date', 'year', 'month'])

county_ids = get_ids('county', state_file='ids/states.csv', city_county_file='ids/counties.csv')
county_df = pd.DataFrame(columns=['county', 'date', 'year', 'month'])

city_ids = get_ids('city', state_file='ids/states.csv', city_county_file='ids/cities.csv')
city_df = pd.DataFrame(columns=['city', 'date', 'year', 'month'])

#Create logging files
processed_date = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
log_folder = os.getenv('LOG_FOLDER', 'logs')
todays_date = os.getenv('TODAYS_DATE', datetime.datetime.now().strftime("%Y-%m-%d"))
state_report_file = "{}/{}/state_organizer_report.txt".format(log_folder, todays_date)
county_report_file = "{}/{}/county_organizer_report.txt".format(log_folder, todays_date)
city_report_file = "{}/{}/city_organizer_report.txt".format(log_folder, todays_date)

with open(state_report_file,"w+") as state_report:
    state_report.write("Reformatting Report for States:\nProcessed at {}\n".format(processed_date))
with open(county_report_file,"w+") as county_report:
    county_report.write("Reformatting Report for Counties:\nProcessed at {}\n".format(processed_date))
with open(city_report_file,"w+") as city_report:
    city_report.write("Reformatting Report for City:\nProcessed at {}\n".format(processed_date))    


#Get the months to scrape passed to the script
new_months = os.getenv('NEW_MONTHS','2019-10')
if new_months:
    new_months = new_months.split(',')
else:
    print('****No months specified****')
    print('Aborting Data Clean')
    exit()

#Reformat data from all of the specified files for each location type, saving results to specified file paths
for f in STATE_FILES:
    state_df = rental_organizer('state', '{}/{}'.format(os.getenv('STATE_DATA_FOLDER','state-data'), f[0]), state_df, f[1], state_ids, state_report_file, get_months=new_months)
save_df(state_df, '{}/{}.csv'.format(os.getenv('SUMMARY_FOLDER','data-cleaned'), os.getenv('STATE_SUMMARY_FILE','state-monthly')), 'state')

for f in COUNTY_FILES:
    county_df = rental_organizer('county', '{}/{}'.format(os.getenv('COUNTY_DATA_FOLDER','county-data'), f[0]), county_df, f[1], county_ids, county_report_file, get_months=new_months)
save_df(county_df, '{}/{}'.format(os.getenv('SUMMARY_FOLDER','data-cleaned'), os.getenv('COUNTY_SUMMARY_FILE','county-monthly')), 'county')

for f in CITY_FILES:
    city_df = rental_organizer('city', '{}/{}'.format(os.getenv('CITY_DATA_FOLDER','city-data'), f[0]), city_df, f[1], city_ids, city_report_file, get_months=new_months)
save_df(city_df, '{}/{}'.format(os.getenv('SUMMARY_FOLDER','data-cleaned'), os.getenv('CITY_SUMMARY_FILE','city-monthly')), 'city')

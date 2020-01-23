#!/bin/sh
# Setp environmental variables to specify the location of the data pulled using zillow_data_download.py,
# and the folder location and file names of the created summary output csvs.

# Specify the folder names where you want to save the raw data file pulls
export STATE_DATA_FOLDER='state-data'
export COUNTY_DATA_FOLDER='county-data'
export CITY_DATA_FOLDER='city-data'

# Specify the rental_organizer logging folder and get today's date
export LOG_FOLDER='logs'
export TODAYS_DATE=$(date +%Y-%m-%d)

# Specify the folder where you want to save the cleaned files, along with filenames to save each with
export SUMMARY_FOLDER='data-cleaned'
export STATE_SUMMARY_FILE='state-monthly'
export COUNTY_SUMMARY_FILE='county-monthly'
export CITY_SUMMARY_FILE='city-monthly'

# Get the database information
read -p "Enter your db type: " db_type
read -p "Enter your db username: " username
read -sp "Enter your db password: " password
echo ""
read -p "Enter db hostname: " host
read -p "Enter your db port: " port
read -p "Enter your db tablename: " table

# Remove any old summary file, and make new one
if [ -d $SUMMARY_FOLDER ]; then
    rm -rf $SUMMARY_FOLDER
fi
mkdir $SUMMARY_FOLDER

# Specify the months for which you would like to pull data, in a string format of "YYYY-MM,YYYY-MM,YYYY-MM..."
export NEW_MONTHS="2019-10,2019-11,2019-12"

# If there is old data, move to a history folder
if [ -d $STATE_DATA_FOLDER ]; then
    if [ -d "${STATE_DATA_FOLDER}-hist" ]; then
        rm -rf "${STATE_DATA_FOLDER}-hist"
    fi
    mv $STATE_DATA_FOLDER "${STATE_DATA_FOLDER}-hist"
fi
if [ -d $COUNTY_DATA_FOLDER ]; then
    if [ -d "${COUNTY_DATA_FOLDER}-hist" ]; then
        rm -rf "${COUNTY_DATA_FOLDER}-hist"
    fi
    mv $COUNTY_DATA_FOLDER "${COUNTY_DATA_FOLDER}-hist"
fi
if [ -d $CITY_DATA_FOLDER ]; then
    if [ -d "${CITY_DATA_FOLDER}-hist" ]; then
        rm -rf "${CITY_DATA_FOLDER}-hist"
    fi    
    mv $CITY_DATA_FOLDER "${CITY_DATA_FOLDER}-hist"
fi

mkdir $STATE_DATA_FOLDER
mkdir $COUNTY_DATA_FOLDER
mkdir $CITY_DATA_FOLDER

# Make log folder if doesn't exist
mkdir -p $LOG_FOLDER/$TODAYS_DATE

# Download the Zillow data and create log report
python zillow_data_download.py && echo "Completed Download"

# Clean the Zillow data, stripping out the desired months, and create reports
python zillow_cleaner.py && echo "Completed Cleaning"

# Commit the new data to the database
python zillow_db_insert.py $db_type $username $password $host $port $table && echo "Completed DB Ingestion"

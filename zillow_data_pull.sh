
# Setp environmental variables to specify the location of the data pulled using zillow_data_download.py,
# and the folder location and file names of the created summary output csvs.

# Specify the folder names where you want to save the raw data file pulls
export STATE_DATA_FOLDER='zillow-state-data'
export COUNTY_DATA_FOLDER='zillow-county-data'
export CITY_DATA_FOLDER='zillow-city-data'

# Specify the rental_organizer logging folder and get today's date
export LOG_FOLDER='organizer_logs'
export TODAYS_DATE=$(date +%Y-%m-%d)

# Specify the folder where you want to save the cleaned files, along with filenames to save each with
export SUMMARY_FOLDER='zillow-data-cleaned'
export STATE_SUMMARY_FILE='state-monthly'
export COUNTY_SUMMARY_FILE='county-monthly'
export CITY_SUMMARY_FILE='city-monthly'

# Specify the months for which you would like to pull data, in a string format of "YYYY-MM,YYYY-MM,YYYY-MM..."
export NEW_MONTHS = "2019-10,2019-11,2019-12"

mv $STATE_DATA_FOLDER "${STATE_DATA_FOLDER}-hist"
mv $COUNTY_DATA_FOLDER "${COUNTY_DATA_FOLDER}-hist"
mv $CITY_DATA_FOLDER "${CITY_DATA_FOLDER}-hist"

# Make log folder if doesn't exist
mkdir -p $LOG_FOLDER/$TODAYS_DATE

# Download the Zillow data and create log report
python zillow_data_download

# Clean the Zillow data, stripping out the desired months, and create reports
python zillow_cleaner.py

# Commit the new data to the database
python zillow_db_insert.py

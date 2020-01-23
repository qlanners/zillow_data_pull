"""
file name: zillow_data_download.py
date created: 9/18/19
last edited: 9/27/19
created by: Quinn Lanners
description: This python file downloads economic data from Zillow. The downloaded data is in the format
of csv files, and these files are saved to seperate folders for state, county, and city data. The python
file rental_organizer.py is then used to reformat these several downloaded csvs into a more compact summary file.
"""
from datetime import datetime
import os
import os.path
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
import time

#import lists of urls from which to download data from zillow
from zillow_paths import STATE_URLS, COUNTY_URLS, CITY_URLS

def create_driver(download_folder):
    """
    create_driver: creates Chrome driver which will be used to download files
            args:
                download_folder: folder to which the driver should save downloads
            returns:
                driver
    """
    option = Options()
    option.add_argument(" - incognito")
    option.add_experimental_option("prefs", {
      "download.default_directory": download_folder,
      "download.prompt_for_download": False,
      "download.directory_upgrade": True,
      "safebrowsing.enabled": True
    })

    capa = DesiredCapabilities.CHROME
    capa["pageLoadStrategy"] = "none"

    driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver', chrome_options = option, desired_capabilities = capa)

    return driver

def download_files(driver, urls, download_folder):
    """
    download_files: creates Chrome driver which will be used to download files
            args:
                driver: driver created from create_driver
                urls: list of urls to download data from
                download_folder: name of download folder
            returns:
                Nothing. Saves all downloads to specified folder and prints out summary message.
    """
    report_file_name = "{}/{}/{}.txt".format(os.getenv('LOG_FOLDER','logs'), os.getenv('TODAYS_DATE',datetime.now().strftime("%Y-%m-%d")), download_folder.split('/')[-1])
    report = open(report_file_name,"w+")

    failed = 0
    flagged = 0
    for i in range(len(urls)):
        try:
            driver.get(urls[i])
        except:
            failed += 1
            report.write('Failed to download {} from Zillow'.format(urls[i].split('/')[-1]))
            continue
        time.sleep(2)
        if (len([name for name in os.listdir(download_folder) if os.path.isfile(os.path.join(download_folder, name))]) - 1) > (i-failed):
            failed -= 1
            flagged += 1
            report.write('Data Retrieved, but driver shutdown for {} from Zillow'.format(urls[i].split('/')[-1]))
        else:
            print('.')
    report.write('Completed downloads for {}\n\tFailed Downloads: {}\n\tFlagged Downloads: {}\n\n'.format(download_folder, failed, flagged))

    report.close()


working_dir = os.getcwd()

download_sets = {
    os.getenv('STATE_DATA_FOLDER','state-data'): STATE_URLS,
    os.getenv('COUNTY_DATA_FOLDER','county-data'): COUNTY_URLS,
    os.getenv('CITY_DATA_FOLDER','city-data'): CITY_URLS
}

for k,v in download_sets.items():
    download_folder = '{}/{}'.format(working_dir,k)
    driver = create_driver(download_folder)
    download_files(driver, v, download_folder)

    #Shutdown driver between each loop
    driver.execute_script("window.stop();")
    driver.stop_client()
    driver.close()


import pandas as pd

import requests
import os
import time
from datetime import datetime
import logging

# Set up logging
log_file = "data/global_radiation_fetch_logs.txt"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


# url for fetching data
URL = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-globalstrahlung-10min/ch.meteoschweiz.messwerte-globalstrahlung-10min_en.csv"

# Combined master data file
MASTER_FILE = "data/input/master_global_radiation.csv"


def initialize_master_file():
    # Ensure the master file exists
    if not os.path.exists(MASTER_FILE):
        with open(MASTER_FILE, "w", encoding="ISO-8859-1") as file:
            file.write("")


def fetch_and_process_data():
    try:
        # get current date and time
        current_datetime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        str_current_datetime = str(current_datetime)

        # Newly fetched data file
        incoming_file = f"data/input/global_radiation_10min_{str_current_datetime}.csv"

        # Fetch data from url
        response = requests.get(URL)
        response.raise_for_status()     # Raise HTTPErrror for bad responses (4xx, 5xx)
        with open(incoming_file, "wb") as file:
            file.write(response.content)

        logging.info(f"Successfully fetched file: {incoming_file}")

        # Read and clean the incoming file
        with open(incoming_file, "r", encoding="ISO-8859-1") as file:
            lines = file.readlines()

        cleaned_file_path = incoming_file[:-4] + "_cleaned.csv"
        with open(cleaned_file_path, "w", encoding="ISO-8859-1") as file:
            file.writelines(lines[:-4])  # Remove the last 4 rows as they contain some non-tabular data
        

        # Load the cleaned data
        new_data = pd.read_csv(cleaned_file_path, delimiter=";", encoding="ISO-8859-1")

        # Load or initialize the master dataset
        if os.path.getsize(MASTER_FILE) > 0:
            master_data = pd.read_csv(MASTER_FILE, delimiter=";", encoding="ISO-8859-1")
        else:
            master_data = pd.DataFrame()

        # Append new data, remove duplicates, and save
        combined_data = pd.concat([master_data, new_data]).drop_duplicates().reset_index(drop=True)
        combined_data.to_csv(MASTER_FILE, index=False, sep=";", encoding="ISO-8859-1")

        # print(f"New data fetched and appended at {current_datetime}")
        logging.info(f"New data appended successfully to master file.")
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP request error: {e}")
    except Exception as e:
        # print(f"Error occurred: {e}")
        logging.error(f"An error occurred: {e}")


def main():
    initialize_master_file()

    # fetch data every 10 minutes
    while True:
        fetch_and_process_data()
        time.sleep(600)

if __name__== "__main__":
    main()
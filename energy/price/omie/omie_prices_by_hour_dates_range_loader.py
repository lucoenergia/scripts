##
# Script to load hourly energy prices from https://www.omie.es for a range of dates
##

import requests
import csv
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import logging
import argparse
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()

BASE_URL = "https://www.omie.es/es/file-download"
BASE_PATH = os.getenv("BASE_PATH")
FILENAME = "marginalpdbc.csv"

INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT"))
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_USER = os.getenv("INFLUXDB_USER")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")

# Get the full path to the script
script_path = __file__

# Get the base name of the script (without extension)
script_name = os.path.splitext(os.path.basename(script_path))[0]

# Logging configuration
log_file_path = f"{BASE_PATH}/logs/{script_name}.log"
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.DEBUG, format=log_format)

# Function to validate the date format
def validate_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        #logging.error(f"Invalid date format for date {date_str}. Use yyyy-mm-dd.")        
        raise argparse.ArgumentTypeError("Invalid date format. Use yyyy-mm-dd.")

# Create an ArgumentParser object
parser = argparse.ArgumentParser(description='Script to load hourly energy prices from https://www.omie.es for a range of dates')

# Define the expected arguments
parser.add_argument('start_date', type=validate_date, help='Start date in "yyyy-mm-dd" format')
parser.add_argument('end_date', type=validate_date, help='End date in "yyyy-mm-dd" format')

# Parse the command-line arguments
args = parser.parse_args()

# Access the parsed arguments
start_date = args.start_date
end_date = args.end_date

# Create an InfluxDB client
influxDbClient = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)

# Function to convert a date to "yyyymmdd" format
def convert_to_yyyymmdd(date):
    return date.strftime("%Y%m%d")

# Function to download the daily prices from OMIE
def downloadFile(dateStr):
    try:
        query_params = {
            "parents[0]": "marginalpdbc",
            "filename": f"marginalpdbc_{dateStr}.1"
        }
        response = requests.get(BASE_URL, params=query_params, stream=True)

        # If the response with ".1" is empty, we try with ".2"
        if not response.content:
            logging.info(f"Response with marginalpdbc_{dateStr}.1 is empty, trying with marginalpdbc_{dateStr}.2...")
            query_params = {
                "parents[0]": "marginalpdbc",
                "filename": f"marginalpdbc_{dateStr}.2"
            }
            response = requests.get(BASE_URL, params=query_params, stream=True)


        if response.status_code == 200:
            # Check if the content type is 'application/octet-stream'
            if response.headers.get('content-type') == 'application/octet-stream':
                # Specify the file where you want to save the binary data            

                with open(FILENAME, 'wb') as output_file:
                    for chunk in response.iter_content(chunk_size=1024):
                        # Write the binary data to the file in chunks
                        if chunk:
                            output_file.write(chunk)

                logging.info(f"Binary data saved to {FILENAME}")
            else:
                logging.error(f"The content type must be 'application/octet-stream'. Received {response.headers.get('content-type')}")
        else:
            logging.error(f"Failed to retrieve the resource. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {e}")

def readFileAndStore():

    # Read the CSV file and skip the first row (header)
    with open(FILENAME, 'r') as file:
        # Read the CSV data and skip the first row (header)
        reader = csv.reader(file, delimiter=';')
        next(reader)  # Skip the header row
        for row in reader:
            # Extract the first four fields and parse them into a datetime object
            year, month, day, hour = map(int, row[0:4])
            date = datetime(year, month, day, hour-1)

            # Extract the other two fields and store them in separate variables
            price1, price2 = map(float, row[4:6])

            logging.info(f"Datetime: {date}, Price 1: {price1}, Price 2: {price1}")

            price_hour_serie = [
                {
                    "measurement": "omie-daily-prices",
                    "time": date,
                    "fields": {
                        "price1": price1,
                        "price2": price2,
                    }
                }
            ]

            influxDbClient.write_points(price_hour_serie)

            if (hour == 24):
                break        

def removeFile():
    # Remove the file after processing
    os.remove(FILENAME)
    logging.info(f"{FILENAME} removed after processing")


# Loop through the range of dates by day
current_date = start_date
while current_date <= end_date:
    current_date_yyyymmdd = convert_to_yyyymmdd(current_date)
    logging.info(f"Current Date (yyyymmdd): {current_date_yyyymmdd}")
    
    downloadFile(current_date_yyyymmdd)

    readFileAndStore()

    removeFile()

    current_date += timedelta(days=1)

# Close the InfluxDB connection
influxDbClient.close()

logging.info("Process finished.")    
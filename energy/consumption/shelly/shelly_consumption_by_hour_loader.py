##
# Script to load hourly consumption downloading a CSV using the Shelly API and storing the results in a InfluxDB
##
from influxdb import InfluxDBClient
import csv
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import pytz
import logging
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()

SHELLY_IP = os.getenv("SHELLY_IP")
SHELLY_USERNAME=os.getenv("SHELLY_USERNAME")
SHELLY_PASSWORD=os.getenv("SHELLY_PASSWORD")
SHELLY_GET_EMETER_0_DATA = "/emeter/0/em_data.csv"
SHELLY_NAME = os.getenv("SHELLY_NAME")

# InfluxDB connection settings
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT"))
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_USER = os.getenv("INFLUXDB_USER")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")

BASE_PATH=os.getenv("BASE_PATH")

# Specify the time zone
TIMEZONE = pytz.timezone('Europe/Madrid')

# Get the full path to the script
script_path = __file__

# Get the base name of the script (without extension)
script_name = os.path.splitext(os.path.basename(script_path))[0]

# Logging configuration
log_file_path = f"{BASE_PATH}/logs/{script_name}.log"
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.DEBUG, format=log_format)

# Get the current date and time for configured time zone
current_time = datetime.now(TIMEZONE)
current_time_string = current_time.strftime("%Y-%m-%dT%H:%M")

def getCsvFilePath():
    #CSV_FILE_PATH = f"{BASE_PATH}/test/em_data_test.csv"    
    return  f"{BASE_PATH}/data/em_data_{current_time_string}.csv"

# Connect to InfluxDB
def connectToInfluxDB():
    return InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD, database=INFLUXDB_DATABASE)

# Download CSV file from Shelly API
def downloadShellyDataFile():    

    # Define URL from where we are going to download the CSV file
    api_url = "http://" + SHELLY_IP + SHELLY_GET_EMETER_0_DATA

    # Send a GET request to the API
    response = requests.get(api_url, auth=HTTPBasicAuth(SHELLY_USERNAME, SHELLY_PASSWORD))

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Define the file path where you want to save the downloaded CSV file
        file_path = getCsvFilePath()
        
        # Open the file in binary write mode and write the response content to it
        with open(file_path, 'wb') as file:
            file.write(response.content)
        
        logging.info(f'CSV file downloaded and saved as {file_path}')
    else:
        logging.info(f'Failed to download CSV file. Status code: {response.status_code}') 
    
def convertUtcToTimezone(date_utc: datetime) -> datetime:

    # Specify the timezone for Madrid
    madrid_timezone = pytz.timezone('Europe/Madrid')

    # Convert the UTC datetime to Madrid timezone
    return date_utc.replace(tzinfo=pytz.utc).astimezone(madrid_timezone)


# Read data from CSV and insert into InfluxDB
def readShellyDataAndWriteIntoDb(client):
    
    with open(getCsvFilePath(), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)

        accumulated_active_energy = 0.0

        for row in csvreader:

            date_utc_string = row["Date/time UTC"] # Date with format "2023-08-19 00:00"
            # Parse the date string into a datetime object
            date_utc = datetime.strptime(date_utc_string, "%Y-%m-%d %H:%M")

            # Convert UTC date to date with timezone of Madrid
            date = convertUtcToTimezone(date_utc)

            # Extract the minutes from the datetime object
            minutes = date.minute    

            # Read active energy and accumulate value
            active_energy = float(row["Active energy Wh"])
            accumulated_active_energy = accumulated_active_energy + active_energy
                
            # If the minute is "00", we store the accumulated energy
            if (minutes == 0):                
                json_body = [
                    {
                        "measurement": "energy_consumption_shelly_hourly",                    
                        "time": date,
                        "fields": {
                            SHELLY_NAME + "-power-hourly": accumulated_active_energy
                        }
                    }
                ]
                logging.info("Writting row with datetime: " + date.strftime("%Y-%m-%d %H:%M") + " and active energy: " + str(accumulated_active_energy))
                client.write_points(json_body)

                accumulated_active_energy = 0.0                
                

# Close the InfluxDB connection
def closeInfluxDbConnection(client):
    client.close()

# Check if the file exists before attempting to remove it
def deleteShellyDataFile():
    file_path = getCsvFilePath()
    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f'{file_path} has been removed.')
    else:
        logging.info(f'{file_path} does not exist.')


def main():

    downloadShellyDataFile()

    client = connectToInfluxDB()

    readShellyDataAndWriteIntoDb(client)

    closeInfluxDbConnection(client)

    #deleteShellyDataFile()


if __name__ == "__main__":
    main()    

logging.info("Data insertion complete.")

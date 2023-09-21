##
# Script to read Huawei inverter production hourly data through a REST API setting a dates interval
##
import requests
# import time
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import argparse
import logging
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()

BASE_URL = "https://eu5.fusionsolar.huawei.com/thirdData"
LOGIN_ENDPOINT = "/login"
DATA_ENDPOINT = "/getKpiStationHour"
USERNAME = os.getenv("HUAWEI_USERNAME")
PASSWORD = os.getenv("HUAWEI_PASSWORD")
TOKEN_EXPIRATION = timedelta(minutes=30)
STATION_CODE = os.getenv("HUAWEI_STATION_CODE")

INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT"))
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_USER = os.getenv("INFLUXDB_USER")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")

BASE_PATH=os.getenv("BASE_PATH")

# Get the full path to the script
script_path = __file__

# Get the base name of the script (without extension)
script_name = os.path.splitext(os.path.basename(script_path))[0]

# Logging configuration
log_file_path = f"{BASE_PATH}/logs/{script_name}.log"
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.DEBUG, format=log_format)

logging.info(f"Process {script_name} started.")

# Function to validate the date format
def validate_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:          
        raise argparse.ArgumentTypeError("Invalid date format. Use yyyy-mm-dd.")

# Create an ArgumentParser object
parser = argparse.ArgumentParser(description='Script to load hourly energy production from huawei inverter for a range of dates')

# Define the expected arguments
parser.add_argument('start_date', type=validate_date, help='Start date in "yyyy-mm-dd" format')
parser.add_argument('end_date', type=validate_date, help='End date in "yyyy-mm-dd" format')

# Parse the command-line arguments
args = parser.parse_args()

# Access the parsed arguments
start_date = args.start_date
end_date = args.end_date

def get_token():
    login_data = {
        "userName": USERNAME,
        "systemCode": PASSWORD
    }
    response = requests.post(BASE_URL + LOGIN_ENDPOINT, json=login_data)
    if response.status_code == 200:
        return response.headers.get("xsrf-token")
    else:
        raise Exception("Failed to retrieve token")

def get_data(token, timestamp_milliseconds):
    headers = {
        "XSRF-TOKEN": f"{token}"
    }    
    body = {
        "stationCodes": STATION_CODE,
        "collectTime": timestamp_milliseconds
    }
    response = requests.post(BASE_URL + DATA_ENDPOINT, headers=headers, json=body)
    if response.status_code == 200:
        data = response.json()

        # If access frequency is too high, raise an exception
        if data["failCode"] == 407:
            raise Exception(f"Failed to retrieve data. Reason: {data['data']}")
        return data
    else:
        raise Exception("Failed to fetch data")
    
def insert_into_influxdb(data):
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)
    json_data = data["data"]
    
    for entry in json_data:
        timestamp = entry["collectTime"] // 1000  # Convert milliseconds to seconds

        inverter_power = entry["dataItemMap"]["inverter_power"]
        if inverter_power == None:
            inverter_power = 0.0
        
        json_body = [
            {
                "measurement": "energy_production_huawei_hour",
                "time": datetime.utcfromtimestamp(timestamp),
                "fields": {
                    "inverter-power": inverter_power
                }
            }
        ]
        
        client.write_points(json_body)
        
    client.close()

def main():
    try:
        token = get_token()    

        current_date = start_date
        day_interval = timedelta(days=1)

        while current_date <= end_date:

            print("current date -> " + datetime.strftime(current_date, "%Y-%m-%d"))

            timestamp_milliseconds = int(current_date.timestamp() * 1000)        
            
            data = get_data(token, timestamp_milliseconds)            

            insert_into_influxdb(data)

            current_date += day_interval
    except Exception as e:
        logging.error("Error:", e)        

    logging.info(f"Process {script_name} finished.")

if __name__ == "__main__":
    main()

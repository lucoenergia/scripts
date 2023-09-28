##
# Script to read Huawei inverter production hourly data through a REST API for current day
##
import requests
from datetime import datetime, timedelta
import pytz
from influxdb import InfluxDBClient
import logging
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()

BASE_URL = "https://eu5.fusionsolar.huawei.com/thirdData"
LOGIN_ENDPOINT = "/login"
DATA_ENDPOINT = "/getKpiStationDay"
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

TIMEZONE='Europe/Madrid'

# Get the full path to the script
script_path = __file__

# Get the base name of the script (without extension)
script_name = os.path.splitext(os.path.basename(script_path))[0]

# Logging configuration
log_file_path = f"{BASE_PATH}/logs/{script_name}.log"
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.INFO, format=log_format)

logging.info(f"Process {script_name} started.")

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

        # Check if "inverter_power" key is present in the "dataItemMap" dictionary
        if "inverter_power" not in entry["dataItemMap"]:
            continue

        # Convert milliseconds to seconds (since Python's datetime expects seconds)
        timestamp = entry["collectTime"] / 1000  # Convert milliseconds to seconds
        # Create a datetime object in UTC
        datetime_utc = datetime.utcfromtimestamp(timestamp)
        # Convert the UTC datetime to the desired timezone
        datetime_with_timezone = datetime_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(TIMEZONE))
        
        json_body = [
            {
                "measurement": "energy_production_huawei_day2",
                "time": datetime_with_timezone,
                "fields": {
                    "installed_capacity": entry["dataItemMap"]["installed_capacity"],
                    "inverter-power": entry["dataItemMap"]["inverter_power"],
                    "power_profit": entry["dataItemMap"]["power_profit"],
                    "reduction_total_coal": entry["dataItemMap"]["reduction_total_coal"],
                    "perpower_ratio": entry["dataItemMap"]["perpower_ratio"],
                    "reduction_total_co2": entry["dataItemMap"]["reduction_total_co2"],
                }
            }
        ]
        
        client.write_points(json_body)
        
    client.close()

def getCurrentDate():
    # Get current date
    # Specify the time zone for Madrid
    madrid_timezone = pytz.timezone(TIMEZONE)
    # Get the current date and time in Madrid's time zone
    return datetime.now(madrid_timezone)      

def main():
    try:
        token = get_token()    

        current_date = getCurrentDate()
        
        current_date = current_date.replace(day=1, hour=0, minute=0, second=0)

        logging.info("Getting data for month " + datetime.strftime(current_date, "%Y-%m"))

        timestamp_milliseconds = int(current_date.timestamp() * 1000)        
        
        data = get_data(token, timestamp_milliseconds)            

        insert_into_influxdb(data)

    except Exception as e:
        logging.error("Error:", e)        

    logging.info(f"Process {script_name} finished.")

if __name__ == "__main__":
    main()

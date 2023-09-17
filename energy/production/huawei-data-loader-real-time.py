# Script to read Huawei inverter production in real time by hitting its REST API every 10 minutes and
# calculating the difference between the total production retrieved from the API call with the previsous value

import requests
import time
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import os
from dotenv import load_dotenv
import schedule
import pytz

# Load environment variables from the .env file in the current directory
load_dotenv()

BASE_URL = "https://eu5.fusionsolar.huawei.com/thirdData"
LOGIN_ENDPOINT = "/login"
DATA_ENDPOINT = "/getStationRealKpi"
USERNAME = os.getenv("HUAWEI_USERNAME")
print(USERNAME)
PASSWORD = os.getenv("HUAWEI_PASSWORD")
TOKEN_EXPIRATION = timedelta(minutes=30)
STATION_CODE = os.getenv("HUAWEI_STATION_CODE")

INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT"))
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_USER = os.getenv("INFLUXDB_USER")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")

# Define your local timezone
local_timezone = pytz.timezone('Europe/Madrid')

# Initialize a variable to store the previous float value
previous_production_value = None


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

def get_data(token):
    headers = {
        "XSRF-TOKEN": f"{token}"
    }    
    body = {
        "stationCodes": STATION_CODE
    }
    response = requests.post(BASE_URL + DATA_ENDPOINT, headers=headers, json=body)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to fetch data")
    
def get_production_difference(production_value):
    global previous_production_value
    try:

        # Calculate the difference with the previous production value
        if previous_production_value is not None:
            difference = production_value - previous_production_value
            print(f'Difference: {difference:.2f}')
            return difference
        
        # Update the previous production value
        previous_production_value = production_value
        return 0.0

    except Exception as e:
        print(f'Error: {str(e)}')
    
def insert_into_influxdb(data):
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)
    json_data = data["data"]
    
    for entry in json_data:        
        total_power = entry["dataItemMap"]["total_power"]
        print('total_power: ' + str(total_power))
        power = get_production_difference(total_power)
        print('power: ' + str(power))

        # Get the current timestamp in RFC3339 format
        # Get the current timestamp in your local timezone
        current_time = datetime.now(local_timezone).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        json_body = [
            {
                "measurement": "energy",
                "time": current_time,
                "fields": {
                    "huawei-inverter-power-10min": power
                }
            }
        ]
        print('time: ' + current_time)
        print('huawei-inverter-power-10m: ' + str(power))
        client.write_points(json_body)

def isRequestSuccessfull(data):
    if data["success"] == True:
        return True
    return False


def main():
    token = get_token()
    token_last_refreshed = datetime.now()    

    current_time = datetime.now()
    time_since_refresh = current_time - token_last_refreshed

    if time_since_refresh > TOKEN_EXPIRATION:
        print("Token expired. Refreshing...")
        token = get_token()
        token_last_refreshed = current_time

    try:
        data = get_data(token)

        print("Received data:", data)

        if (isRequestSuccessfull(data)):
            insert_into_influxdb(data)                
        
    except Exception as e:
        print("Error:", e)
    
    #time.sleep(int(5*60))  # Sleep for 5 minutes

# Schedule the job to run at specific minutes of every hour
schedule.every().hour.at(":00").do(main)
schedule.every().hour.at(":10").do(main)
schedule.every().hour.at(":20").do(main)
schedule.every().hour.at(":30").do(main)
schedule.every().hour.at(":40").do(main)
schedule.every().hour.at(":50").do(main)

# Run the job immediately
#main()

# Main loop to continuously run the scheduled job
while True:
    schedule.run_pending()
    time.sleep(1)

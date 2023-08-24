import requests
import time
from datetime import datetime, timedelta
from influxdb import InfluxDBClient

BASE_URL = "https://eu5.fusionsolar.huawei.com/thirdData"
LOGIN_ENDPOINT = "/login"
DATA_ENDPOINT = "/getKpiStationHour"
USERNAME = ""
PASSWORD = ""
TOKEN_EXPIRATION = timedelta(minutes=30)
STATION_CODE = ""

INFLUXDB_HOST = "localhost"
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = ""
INFLUXDB_USER = ""
INFLUXDB_PASSWORD = ""

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
        return response.json()
    else:
        raise Exception("Failed to fetch data")
    
def insert_into_influxdb(data):
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)
    json_data = data["data"]
    
    for entry in json_data:
        timestamp = entry["collectTime"] // 1000  # Convert milliseconds to seconds
        inverter_power = entry["dataItemMap"]["inverter_power"]
        
        json_body = [
            {
                "measurement": "production",
                "time": datetime.utcfromtimestamp(timestamp),
                "fields": {
                    "inverter-power": inverter_power
                }
            }
        ]
        
        client.write_points(json_body)    

def main():
    token = get_token()
    token_last_refreshed = datetime.now()

    start_date = datetime(year=2023, month=8, day=22)  # Replace with your desired start date
    end_date = datetime(year=2023, month=8, day=23)   # Replace with your desired end date

    current_date = start_date
    day_interval = timedelta(days=1)

    while current_date <= end_date:

        timestamp_milliseconds = int(current_date.timestamp() * 1000)
        print("Timestamp for", current_date, ":", timestamp_milliseconds)
        current_date += day_interval

        current_time = datetime.now()
        time_since_refresh = current_time - token_last_refreshed

        if time_since_refresh > TOKEN_EXPIRATION:
            print("Token expired. Refreshing...")
            token = get_token()
            token_last_refreshed = current_time

        try:
            data = get_data(token, timestamp_milliseconds)
            print("Received data:", data)
            insert_into_influxdb(data)
        except Exception as e:
            print("Error:", e)
        
        #time.sleep(int(5*60))  # Sleep for 5 minutes

if __name__ == "__main__":
    main()

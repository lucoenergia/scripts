from influxdb import InfluxDBClient
import csv
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
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

# CSV file
CSV_FILE_PATH = "em_data.csv"

# Connect to InfluxDB
client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD, database=INFLUXDB_DATABASE)

# Download CSV file from Shelly API

# Define URL from where we are going to download the CSV file
api_url = "http://" + SHELLY_IP + SHELLY_GET_EMETER_0_DATA

# Send a GET request to the API
response = requests.get(api_url, auth=HTTPBasicAuth(SHELLY_USERNAME, SHELLY_PASSWORD))

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Define the file path where you want to save the downloaded CSV file
    file_path = 'em_data.csv'
    
    # Open the file in binary write mode and write the response content to it
    with open(file_path, 'wb') as file:
        file.write(response.content)
    
    print(f'CSV file downloaded and saved as {file_path}')
else:
    print(f'Failed to download CSV file. Status code: {response.status_code}')


# Read data from CSV and insert into InfluxDB
with open(CSV_FILE_PATH, 'r') as csvfile:
    csvreader = csv.DictReader(csvfile)

    active_energy = 0.0

    for row in csvreader:

        date_string = row["Date/time UTC"] # Date with format "2023-08-19 00:00"
        # Parse the date string into a datetime object
        date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M")

        # Extract the minutes from the datetime object
        minutes = date_obj.minute
               
        if (minutes == "10" or minutes == "20" or minutes == "20" or minutes == "30" or minutes == "40" or minutes == "50"):
            active_energy = active_energy + float(row["Active energy Wh"])
        else:
            active_energy = active_energy + float(row["Active energy Wh"])
            json_body = [
                {
                    "measurement": "energy",                    
                    "time": row["Date/time UTC"],
                    "fields": {
                        "shelly-" + SHELLY_NAME + "-power-hourly": active_energy
                    }
                }
            ]
            print("Writting row with datetime: " + row["Date/time UTC"] + " and active energy: " + str(active_energy))
            client.write_points(json_body)

            active_energy = 0.0

# Close the InfluxDB connection
client.close()

# Check if the file exists before attempting to remove it
if os.path.exists(CSV_FILE_PATH):
    os.remove(CSV_FILE_PATH)
    print(f'{CSV_FILE_PATH} has been removed.')
else:
    print(f'{CSV_FILE_PATH} does not exist.')

print("Data insertion complete.")

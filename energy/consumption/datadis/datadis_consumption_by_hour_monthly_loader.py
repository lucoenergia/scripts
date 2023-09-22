##
# Script to query Datadis to retrieve consumptions for all the CUPS by month
##
import requests
import json
import random
import string
import argparse
from datetime import datetime
from influxdb import InfluxDBClient
import logging
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()

DATADIS_USERNAME = os.getenv("DATADIS_USERNAME")
DATADIS_PASSWORD = os.getenv("DATADIS_PASSWORD")
DATADIS_DISTRIBUTOR_CODE = os.getenv("DATADIS_DISTRIBUTOR_CODE")
DATADIS_MEASUREMENT_TYPE=os.getenv("DATADIS_MEASUREMENT_TYPE")
DATADIS_POINT_TYPE=os.getenv("DATADIS_POINT_TYPE")
DATADIS_BASE_URL = os.getenv("DATADIS_BASE_URL")
DATADIS_LOGIN_URL = DATADIS_BASE_URL + "/nikola-auth/tokens/login"
DATADIS_GET_CONSUMPTIONS_URL = DATADIS_BASE_URL + "/api-private/api/get-consumption-data"

INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT"))
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_USER = os.getenv("INFLUXDB_USER")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")

COMMUNITY_PARTNERS_FILE_PATH=os.getenv("COMMUNITY_PARTNERS_FILE_PATH")

# Get the absolute path of the currently executing script
script_path = os.path.abspath(__file__)

# Get the directory containing the script
script_directory = os.path.dirname(script_path)

# Get the base name of the script (without extension)
script_name = os.path.splitext(os.path.basename(script_path))[0]

# Logging configuration
log_file_path = f"{script_directory}/logs/{script_name}.log"
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.INFO, format=log_format)

# Get current script name
def get_script_name():
    # Get the absolute path of the currently executing script
    script_path = os.path.abspath(__file__)

    # Get the base name of the script (without extension)
    return os.path.splitext(os.path.basename(script_path))[0]

# Get the month passed by argument
def get_month():
    # Function to validate the date format
    def validate_date(date_str):
        try:
            return datetime.strptime(date_str, "%Y/%m")
        except ValueError:          
            raise argparse.ArgumentTypeError(f"Date {date_str} has an invalid format. Use yyyy/mm.")

    # Create an ArgumentParser object
    parser = argparse.ArgumentParser(description='Script to load hourly energy consumption from Datadis by month')

    # Define the expected arguments
    parser.add_argument('month', type=validate_date, help='Year and month in "yyyy/mm" format')

    # Parse the command-line arguments
    args = parser.parse_args()

    month = args.month.strftime("%Y/%m") 

    logging.info("Getting data for month " + month)     

    # Access the parsed arguments
    return month

# Get authorization token to be able to make calls to Datadis API
def get_token():

    json_data = {
        "username": DATADIS_USERNAME,
        "password": DATADIS_PASSWORD
    }
    
    # Generate a random boundary string
    boundary = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))
    # Create a dictionary for the headers with "Content-Type" set to "multipart/form-data" and the boundary parameter
    headers = {'Content-Type': f'multipart/form-data; boundary={boundary}'}

    # Create the request body
    body = ''
    for key, value in json_data.items():
        body += f'--{boundary}\r\n'
        body += f'Content-Disposition: form-data; name="{key}"\r\n\r\n'
        body += f'{value}\r\n'
    # Add a closing boundary
    body += f'--{boundary}--\r\n'

    response = requests.post(DATADIS_LOGIN_URL, data=body, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to retrieve token. Reason: {response.text}")
    
# Transform two strings representing a date in format "yyyy/MM/dd" and a time in format "hh:mm" 
# into a date in RFC3339 format that InfluxDB can understand
def get_influx_date(date, time):

    # "24:00" is not a valid time in the "HH:MM" format as required by the %H:%M format specifier in the strptime method.
    # In the "HH:MM" format, the hour should be between 00 and 23.
    # To represent midnight at the end of a day, we use "00:00" instead of "24:00".
    if time == "24:00":
        time = "00:00"

    # Concatenate date and time strings with a space in between
    datetime_str = f"{date} {time}"

    # Parse the concatenated string into a datetime object
    dt_obj = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M")

    # Format the datetime object as an InfluxDB-compatible timestamp string (RFC3339 format)
    return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

# Receive a list of consumptions by month for a supply and insert the data into an InfluxDB
def insert_into_influxdb(supply_consumptions):

    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)    
    
    for supply_consumption in supply_consumptions:

        time = get_influx_date(supply_consumption["date"], supply_consumption["time"])
        
        consumption_point = [
            {
                "measurement": "energy_consumption_datadis",
                "time": time,
                "fields": {
                    "consumptionKWh": supply_consumption['consumptionKWh'],
                    "obtainMethod": supply_consumption['obtainMethod'],
                    "surplusEnergyKWh": supply_consumption['surplusEnergyKWh'],
                },
                "tags": {
                    "cups": supply_consumption['cups']
                }
            }
        ]
        
        client.write_points(consumption_point)
        
    client.close()

# Loads the consumption of all the partners of the community for a month
def load_consumption(token, month):
    
    # Create headers with the "Authorization" header containing the Bearer token
    headers = {
        'Authorization': f'Bearer {token}'
    }

    query_params = {
                "cups": "",
                "distributorCode": DATADIS_DISTRIBUTOR_CODE,
                "startDate": month,
                "endDate": month,
                "measurementType": DATADIS_MEASUREMENT_TYPE,
                "pointType": DATADIS_POINT_TYPE,
                "authorizedNif": ""
            }

    # Open the JSON file for reading with all the partners with all their supplies
    with open(COMMUNITY_PARTNERS_FILE_PATH, 'r') as partners_file:
        partners = json.load(partners_file)

    # Loop the list of partners
    for partner in partners:
        dni = partner["dni"]
        supplies = partner["supplies"]    
        
        # Loop the list of supplies by partner
        for supply in supplies:
            cups = supply["cups"]
            
            logging.debug(f"Cups: {cups}")

            query_params["cups"] = cups
            query_params["authorizedNif"] = dni

            response = requests.get(DATADIS_GET_CONSUMPTIONS_URL, headers=headers, params=query_params)

            if response.status_code == 200:
                insert_into_influxdb(response.json())        
            else:
                logging.error(f"Failed to fetch data for cups {cups} of partner {partner['name']}. Error code {response.status_code} Reason: {response.text}")
    
def main():

    script_name = get_script_name()

    logging.info(f"Process {script_name} started.")

    try:

        month = get_month()

        token = get_token()            
        
        load_consumption(token, month)                    

    except Exception as e:
        logging.error("Error:", e)        

    logging.info(f"Process {script_name} finished.")

if __name__ == "__main__":
    main()
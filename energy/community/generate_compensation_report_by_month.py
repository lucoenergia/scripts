##
# Generates a report with all the surplus values hourly for all the days of a month
#
from datetime import datetime, timedelta
import json
import csv
import argparse
from influxdb import InfluxDBClient
import logging
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()

INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT"))
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_USER = os.getenv("INFLUXDB_USER")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")

COMMUNITY_PARTNERS_FILE_PATH=os.getenv("COMMUNITY_PARTNERS_FILE_PATH")

CIL = os.getenv("INFLUXDB_PASSWORD")
MEASUREMENT_TYPE = os.getenv("INFLUXDB_PASSWORD")
STATUS = os.getenv("INFLUXDB_PASSWORD")

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

##
# Get current script name
##
def get_script_name()-> str:
    # Get the absolute path of the currently executing script
    script_path = os.path.abspath(__file__)

    # Get the base name of the script (without extension)
    return os.path.splitext(os.path.basename(script_path))[0]

def create_influxdb_client():
    return InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)

def close_influxdb_client(client:InfluxDBClient):
    client.close()

##
# Get the month passed by argument with format YYYY-mm
##
def get_month_first_day()-> datetime:

    # Function to validate the date format 
    def validate_date(date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m")
        except ValueError:          
            raise argparse.ArgumentTypeError(f"Date {date_str} has an invalid format. Use yyyy-mm.")

    # Create an ArgumentParser object
    parser = argparse.ArgumentParser(description='Script to calculate surplus compensation hourly by month')

    # Define the expected arguments
    parser.add_argument('month', type=validate_date, help='Year and month in "yyyy-mm" format')

    # Parse the command-line arguments
    args = parser.parse_args()

    return args.month

def get_month_last_day(month_first_day:datetime)-> datetime:

    # Iterate by every day of the month
    month:int = month_first_day.month
    year:int = month_first_day.year
    
    # Determine the last day of the month
    if month == 12:
        last_day:datetime = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day:datetime = datetime(year, month + 1, 1) - timedelta(days=1)

    return last_day

##
# Transform a datetime into a date in RFC3339 format that InfluxDB can understand
##
def get_influx_date(month_date:datetime)-> str:

    # Format the datetime object as an InfluxDB-compatible timestamp string (RFC3339 format)
    return month_date.strftime("%Y-%m-%dT%H:%M:%SZ")

##
# Transform the result of a query to a InfluxDB into a float value
# If there is an error during the transformation, returns a 0.0
##
def get_value_as_float(result, field:str)-> float:

    try:
        # If nothing is found, return zero
        if len(result.keys()) == 0:
            return 0.0
        
        # Access the measurement name (it's the first key in the ResultSet)
        measurement = list(result.keys())[0]

        # Access the data points for that measurement
        data_points = result[measurement]

        # Iterate through the data points
        for point in data_points:
            value = point[field]            
            return value
        
    except Exception as e:
        logging.error(f"Error extracting the value {field} as float from an InfluxDB query result.", e)
        raise e
    
##
# Get aggreagate production by CUPS for a given month
##
def get_sum_value_by_interval(field:str, first_day:datetime, last_day:datetime)-> float:

    try:
        client = create_influxdb_client()

        query:str = f'SELECT SUM("{field}") AS "{field}" FROM "community_supply" WHERE "cups" = \'{cups}\' and time >= \'{get_influx_date(first_day)}\' AND time <= \'{get_influx_date(last_day)}\''
        result = client.query(query)

        close_influxdb_client(client)
            
        return get_value_as_float(result, field)
    
    except Exception as e:
        logging.error(f"Error getting aggreagate production by cups {cups} for month {first_day}.", e)
        raise e

##
# Main function
##
def main():

    script_name = get_script_name()

    logging.info(f"Process {script_name} started.")

    try:

         # Open the JSON file for reading with all the partners with all their supplies
        with open(COMMUNITY_PARTNERS_FILE_PATH, 'r') as partners_file:
            partners = json.load(partners_file)

        month_first_day:datetime = get_month_first_day()
        month_last_day:datetime = get_month_last_day(month_first_day)
        
        # File name
        csv_file_name = f"energy/community/data/supplies_report_{month_first_day.strftime('%Y-%m')}.csv"

        # Open the CSV file in write mode
        # We use newline='' to ensure consistent line endings on all platforms
        with open(csv_file_name, mode='w', newline='') as file:
            # Create a CSV writer object
            writer = csv.writer(file, delimiter=';')

            # Write header
            header = [
                "CIL",
                "Tipo de medida (AS/RC/RI)",
                "Estado (R/E)",
                "FECHA (dd/mm/aaaa)",
                "HOR1",
                "HOR2",
                "HOR3",
                "HOR4",
                "HOR5",
                "HOR6",
                "HOR7",
                "HOR8",
                "HOR9",
                "HOR10",
                "HOR11",
                "HOR12",
                "HOR13",
                "HOR14",
                "HOR15",
                "HOR16",
                "HOR17",
                "HOR18",
                "HOR19",
                "HOR20",
                "HOR21",
                "HOR22",
                "HOR23",
                "HOR24",
                "HOR25"
                ]
            writer.writerow(header)

            # Iterate through the days of the month
            current_day:datetime = month_first_day
            while current_day <= month_last_day:

                row = [CIL, MEASUREMENT_TYPE, STATUS, current_day.strftime("dd/mm/YYYY")]

                logging.info(f"Day: {current_day}.")
            
                # Iterate by every hour of the day from 0 to 23
                for current_hour in range(24):

                    logging.info(f"Hour: {current_hour}.")

                    # Loop the list of partners
                    for partner in partners:
                        supplies = partner["supplies"]
                        
                        # Loop the list of supplies by partner
                        for supply in supplies:

                            cups = supply["cups"]
                            beta = supply["beta"]

                            # Get supply final consumption
                            consumption_final = get_sum_value_by_interval("consumption_final", cups, month_first_day, month_last_day)

                            # Get supply surplus
                            surplus = get_sum_value_by_interval("surplus", cups, month_first_day, month_last_day)

                            # Get supply self consumption
                            self_consumption = get_sum_value_by_interval("self_consumption", cups, month_first_day, month_last_day)

                            # Calculate supply self consumption percentage
                            self_consumption_percentage:float = 0.0
                            if (self_consumption + consumption_final) > 0.0:
                                self_consumption_percentage = self_consumption / (self_consumption + consumption_final)

                            # Calculate utilization percentage
                            utilization_percentage:float = 0.0
                            if (self_consumption + surplus) > 0.0 :
                                utilization_percentage = self_consumption / (self_consumption + surplus)

                            row =  [cups, beta, consumption_final, surplus, self_consumption, self_consumption_percentage, utilization_percentage]

                            writer.writerow(row)

    except Exception as e:
        logging.error("Error:", e)    

    logging.info(f"Process {script_name} finished.")

if __name__ == "__main__":
    main()        
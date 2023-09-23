##
#
##
from datetime import datetime, timedelta
import argparse
import json
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

# Get the absolute path of the currently executing script
script_path = os.path.abspath(__file__)

# Get the directory containing the script
script_directory = os.path.dirname(script_path)

# Get the base name of the script (without extension)
script_name = os.path.splitext(os.path.basename(script_path))[0]

# Logging configuration
log_file_path = f"{script_directory}/logs/{script_name}.log"
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.DEBUG, format=log_format)

# Get current script name
def get_script_name()-> str:
    # Get the absolute path of the currently executing script
    script_path = os.path.abspath(__file__)

    # Get the base name of the script (without extension)
    return os.path.splitext(os.path.basename(script_path))[0]

# Get the month passed by argument
def get_month()-> datetime:
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

# Transform a datetime into a date in RFC3339 format that InfluxDB can understand
def get_influx_date(month_date:datetime)-> str:
    # Format the datetime object as an InfluxDB-compatible timestamp string (RFC3339 format)
    return month_date.strftime("%Y-%m-%dT%H:%M:%SZ")

def get_value_as_float(result, field:str)-> float:
    # Access the measurement name (it's the first key in the ResultSet)
    measurement = list(result.keys())[0]

    # Access the data points for that measurement
    data_points = result[measurement]

    # Iterate through the data points
    for point in data_points:
        timestamp = point['time']
        value = point[field]
        print(f'Timestamp: {timestamp}, {field}: {value}')
        
        return value

def get_production_by_hour(datetime_hour:datetime)-> float:
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)

    query:str = f'SELECT "inverter-power" FROM energy_production_huawei_hour WHERE time = \'{get_influx_date(datetime_hour)}\''
    result = client.query(query)

    client.close()
        
    return get_value_as_float(result, 'inverter-power')

def get_supply_surplus_by_hour(datetime_hour:datetime, cups:str)-> float:
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)

    query:str = f'SELECT surplusEnergyKWh FROM energy_consumption_datadis WHERE cups = \'{cups}\' and time = \'{get_influx_date(datetime_hour)}\''
    result = client.query(query)

    client.close()

    return get_value_as_float(result, 'surplusEnergyKWh')

def get_supply_final_consumption_by_hour(datetime_hour:datetime, cups:str)-> float:
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)

    query:str = f'SELECT consumptionKWh FROM energy_consumption_datadis WHERE cups = \'{cups}\' and time = \'{get_influx_date(datetime_hour)}\''
    result = client.query(query)

    client.close()

    return get_value_as_float(result, 'consumptionKWh')

def get_price_by_hour(datetime_hour:datetime)-> float:
    client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)

    query:str = f'SELECT price1 FROM "omie-daily-prices" WHERE time = \'{get_influx_date(datetime_hour)}\''
    result = client.query(query)

    client.close()

    price_mWh:float = get_value_as_float(result, 'price1')

    return price_mWh / 1000

def main():

    script_name = get_script_name()

    logging.info(f"Process {script_name} started.")

    # Initialize supplies data dictionary
    supplies_data:dict = {}

    try:

        month_first_day:datetime = get_month()

        # Iterate by every day of the month
        month:int = month_first_day.month
        year:int = month_first_day.year
        
        # Determine the last day of the month
        if month == 12:
            last_day:datetime = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day:datetime = datetime(year, month + 1, 1) - timedelta(days=1)

        compensation_by_month:float = 0.0
        
        # Iterate through the days of the month
        current_day:datetime = month_first_day
        while current_day <= last_day:

            logging.debug(f"Day: {current_day}.")

            compensation_by_day:float = 0.0
            
            # Iterate by every hour of the day from 0 to 23
            for hour in range(24):

                logging.debug(f"Hour: {hour}.")

                datetime_hour = current_day + timedelta(hours=hour)

                # Get total production
                production_total:float = get_production_by_hour(datetime_hour)

                # Get the price for that hour
                price:float = get_price_by_hour(datetime_hour)

                # Initialize surplus total
                surplus_total:float = 0.0                

                 # Open the JSON file for reading with all the partners with all their supplies
                with open(COMMUNITY_PARTNERS_FILE_PATH, 'r') as partners_file:
                    partners = json.load(partners_file)

                # Loop the list of partners
                for partner in partners:                    
                    supplies = partner["supplies"]    
                    
                    # Loop the list of supplies by partner
                    for supply in supplies:
                        cups = supply["cups"]
                        beta = supply["beta"]
                        
                        logging.debug(f"Cups: {cups} with beta {beta}.")

                        # Apply beta to production to get supply production
                        production_supply:float = production_total * beta                        

                        logging.debug(f"Supply production: {production_supply}.")
                            
                        # Get supply surplus
                        surplus_supply:float = get_supply_surplus_by_hour(datetime_hour, cups)

                        logging.debug(f"Supply surplus: {surplus_supply}.")

                        # Get supply surplus
                        consumption_final_supply:float = get_supply_final_consumption_by_hour(datetime_hour, cups)

                        logging.debug(f"Supply final consumption: {consumption_final_supply}.")

                        # Aggregate supply surplus to total
                        surplus_total += surplus_supply

                        # Get supply self consumption
                        self_consumption_supply:float = production_supply - surplus_supply

                        logging.debug(f"Supply self consumption: {self_consumption_supply}.")

                        # Get supply self consumption percentage
                        self_consumption_percentage_supply:float = 0.0
                        if (self_consumption_supply + consumption_final_supply) > 0.0:
                            self_consumption_percentage_supply = self_consumption_supply / (self_consumption_supply + consumption_final_supply)

                        logging.debug(f"Supply self consumption percentage: {self_consumption_percentage_supply}.")

                        # Get utilization percentage
                        utilization_percentage:float = 0.0
                        if (self_consumption_supply + surplus_supply) > 0.0 :
                            utilization_percentage = self_consumption_supply / (self_consumption_supply + surplus_supply)

                        logging.debug(f"Supply utilization percentaje: {utilization_percentage}.")

                # Multiply the calculated consumption by the price
                compensation_by_hour:float = surplus_total * price

                # Aggregate hour compensation to day compensation
                compensation_by_day += compensation_by_hour

                logging.debug(f"Supply utilization percentaje: {utilization_percentage}.")

            current_day += timedelta(days=1)

            logging.debug(f"Compensation by day: {compensation_by_day}.")

            compensation_by_month += compensation_by_day

            logging.debug(f"Compensation by month: {compensation_by_month}.")

            logging.debug(supplies_data)            

    except Exception as e:
        logging.error("Error:", e)    

    logging.info(f"Process {script_name} finished.")

if __name__ == "__main__":
    main()    
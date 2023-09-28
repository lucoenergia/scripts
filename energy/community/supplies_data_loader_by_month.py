##
# Script to load energy data for all supplies registered in the community.
# This energy data inclues:
# - production
# - surplus
# - final consumption
# - self consumption
# - self consumption percentage
# - utilization percentage
##
from datetime import datetime, timedelta
import pytz
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

# Specify the time zone
TIMEZONE = pytz.timezone('Europe/Madrid')

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
# Get the production for an hour
##
def get_production_by_hour(datetime_hour:datetime)-> float:

    try:
        client = create_influxdb_client()

        query:str = f'SELECT "inverter-power" FROM energy_production_huawei_hour WHERE time = \'{get_influx_date(datetime_hour)}\''
        result = client.query(query)

        close_influxdb_client(client)
            
        return get_value_as_float(result, 'inverter-power')
    
    except Exception as e:
        logging.error(f"Error getting the production for hour {datetime_hour}.", e)
        raise e

##
# Get the surplus for a supply, given a CUPS, for an hour
##
def get_supply_surplus_by_hour(datetime_hour:datetime, cups:str)-> float:

    try:
        client = create_influxdb_client()

        query:str = f'SELECT surplusEnergyKWh FROM energy_consumption_datadis WHERE cups = \'{cups}\' and time = \'{get_influx_date(datetime_hour)}\''
        result = client.query(query)

        close_influxdb_client(client)

        return get_value_as_float(result, 'surplusEnergyKWh')
    
    except Exception as e:
        logging.error(f"Error getting surplus for cups {cups} and hour {datetime_hour}.", e)
        raise e

##
# Get the final consumption for a supply, given a CUPS, for an hour
##
def get_supply_final_consumption_by_hour(datetime_hour:datetime, cups:str)-> float:

    try:
        client = create_influxdb_client()

        query:str = f'SELECT consumptionKWh FROM energy_consumption_datadis WHERE cups = \'{cups}\' and time = \'{get_influx_date(datetime_hour)}\''
        result = client.query(query)

        close_influxdb_client(client)

        return get_value_as_float(result, 'consumptionKWh')
    
    except Exception as e:
        logging.error(f"Error getting final consumption for cups {cups} and hour {datetime_hour}.", e)
        raise e

##
# Get the price of the energy in €kWh for an hour
##
def get_price_by_hour(datetime_hour:datetime)-> float:

    try:
        client = create_influxdb_client()

        query:str = f'SELECT price1 FROM "omie-daily-prices" WHERE time = \'{get_influx_date(datetime_hour)}\''
        result = client.query(query)

        close_influxdb_client(client)

        price_mWh:float = get_value_as_float(result, 'price1')

        return price_mWh / 1000
    
    except Exception as e:
        logging.error(f"Error getting energy price for hour {datetime_hour}.", e)
        raise e

##
# Insert collected data for a supply in a InfluxDB measurement
##
def insert_supply_data(time, supply_data):

    client = create_influxdb_client()

    time = get_influx_date(time)
    
    supply_point = [
        {
            "measurement": "community_supply",
            "time": time,
            "fields": {
                "production": supply_data["production"],
                "surplus": supply_data["surplus"],
                "consumption_final": supply_data["consumption_final"],
                "self_consumption": supply_data["self_consumption"],
                "self_consumption_percentage": supply_data["self_consumption_percentage"],
                "utilization_percentage": supply_data["utilization_percentage"],
                "compensation": supply_data["compensation"],
            },
            "tags": {
                "cups": supply_data['cups'],
                "beta": supply_data["beta"],
            }
        }
    ]
    
    client.write_points(supply_point)
        
    close_influxdb_client(client)


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

        month_first_day:datetime = get_month()

        # Iterate by every day of the month
        month:int = month_first_day.month
        year:int = month_first_day.year
        
        # Determine the last day of the month
        if month == 12:
            last_day:datetime = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day:datetime = datetime(year, month + 1, 1) - timedelta(days=1)
        
        # Iterate through the days of the month
        current_day:datetime = month_first_day
        while current_day <= last_day:

            logging.info(f"Day: {current_day}.")
            
            # Iterate by every hour of the day from 0 to 23
            for current_hour in range(24):

                logging.info(f"Hour: {current_hour}.")

                datetime_hour = current_day + timedelta(hours=current_hour)

                # Set Madrid timezone to date
                localized_datetime_hour:datetime = TIMEZONE.localize(datetime_hour)

                # Convert localized date to UTC
                utc_datetime_hour:datetime = localized_datetime_hour.astimezone(pytz.utc)

                # Get total production
                production_total:float = get_production_by_hour(utc_datetime_hour)
                logging.debug(f"Production total: {production_total}.")

                # Get the price for that hour
                price:float = get_price_by_hour(utc_datetime_hour)
                logging.debug(f"Price: {price}.")                

                # Loop the list of partners
                for partner in partners:
                    supplies = partner["supplies"]
                    
                    # Loop the list of supplies by partner
                    for supply in supplies:

                        supply_data = {}

                        cups = supply["cups"]
                        supply_data["cups"] = cups                        

                        beta = supply["beta"]
                        supply_data["beta"] = beta                                                                    

                        # Calculate production by multiplying beta to production
                        production_supply:float = production_total * beta
                        supply_data["production"] = production_supply
                            
                        # Get supply surplus
                        surplus_supply:float = get_supply_surplus_by_hour(utc_datetime_hour, cups)
                        supply_data["surplus"] = surplus_supply

                        # Calculate compensation by multiplying surplus by price
                        compensation:float = surplus_supply * price
                        supply_data["compensation"] = compensation

                        # Get supply final consumption
                        consumption_final_supply:float = get_supply_final_consumption_by_hour(utc_datetime_hour, cups)
                        supply_data["consumption_final"] = consumption_final_supply

                        # Get supply self consumption
                        self_consumption_supply:float = production_supply - surplus_supply
                        supply_data["self_consumption"] = self_consumption_supply

                        # Calculate supply self consumption percentage
                        self_consumption_percentage_supply:float = 0.0
                        if (self_consumption_supply + consumption_final_supply) > 0.0:
                            self_consumption_percentage_supply = self_consumption_supply / (self_consumption_supply + consumption_final_supply)
                        supply_data["self_consumption_percentage"] = self_consumption_percentage_supply

                        # Calculate utilization percentage
                        utilization_percentage:float = 0.0
                        if (self_consumption_supply + surplus_supply) > 0.0 :
                            utilization_percentage = self_consumption_supply / (self_consumption_supply + surplus_supply)
                        supply_data["utilization_percentage"] = utilization_percentage

                        logging.info(f"Supply data {supply_data}.")

                        insert_supply_data(utc_datetime_hour, supply_data)
                
            current_day += timedelta(days=1)

    except Exception as e:
        logging.error("Error:", e)    

    logging.info(f"Process {script_name} finished.")

if __name__ == "__main__":
    main()    
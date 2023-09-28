##
# Reads data from InfluxDB measurement, get the time, set correct timezone and then change to UTC to insert data into a new measurement
##
from datetime import datetime, timedelta
from influxdb import InfluxDBClient
import pytz
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()


INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT"))
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_USER = os.getenv("INFLUXDB_USER")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")


client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD)    

# Query data from original measurement
query = 'SELECT * FROM "energy_consumption_datadis"'
result = client.query(query)

# Process and modify the data
for point in result.get_points():

    timestamp = point['time']

    consumptionKWh = point['consumptionKWh']
    obtainMethod = point['obtainMethod']
    surplusEnergyKWh = point['surplusEnergyKWh']
    cups = point['cups']

    # Set Madrid timezone to date
    localized_datetime:datetime = pytz.timezone('Europe/Madrid').localize(datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ'))

    # Convert localized date to UTC
    utc_datetime = localized_datetime.astimezone(pytz.utc)

    # Create a new data point with the modified timestamp
    new_data_point = {
        "measurement": "energy_consumption_datadis2",
        "time": utc_datetime,
        "fields": {
            "consumptionKWh": consumptionKWh,
            "obtainMethod": obtainMethod,
            "surplusEnergyKWh": surplusEnergyKWh,
        },
        "tags": {
            "cups": cups
        }
    }

    # Write the new data point to InfluxDB
    client.write_points([new_data_point])

# Close the InfluxDB connection
client.close()
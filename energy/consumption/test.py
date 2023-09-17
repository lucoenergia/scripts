from influxdb import InfluxDBClient
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()

# InfluxDB connection settings
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT"))
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_USER = os.getenv("INFLUXDB_USER")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")

# Connect to InfluxDB
client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USER, password=INFLUXDB_PASSWORD, database=INFLUXDB_DATABASE)

timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

for i in range(2):
    json_body = [
                    {
                        "measurement": "foo",                    
                        "time": timestamp,
                        "fields": {
                            "test": "bar"
                        }
                    }
                ]
    print("Point " + str(i))
    client.write_points(json_body)

# Close the InfluxDB connection
client.close()

print("Program finished.")
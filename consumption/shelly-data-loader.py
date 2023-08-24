from influxdb import InfluxDBClient
import csv

# InfluxDB connection settings
HOST = "localhost"  # Update with your InfluxDB host
PORT = 8086         # Update with your InfluxDB port
USERNAME = ""  # Update with your InfluxDB username
PASSWORD = ""  # Update with your InfluxDB password
DATABASE = ""  # Update with your InfluxDB database name
MEASUREMENT_NAME = ""   # Measurement name in InfluxDB

# CSV file settings
CSV_FILE_PATH = "/path/to/em_data.csv"  # Update with your CSV file path


# Connect to InfluxDB
client = InfluxDBClient(host=HOST, port=PORT, username=USERNAME, password=PASSWORD, database=DATABASE)

# Read data from CSV and insert into InfluxDB
with open(CSV_FILE_PATH, 'r') as csvfile:
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:
        json_body = [
            {
                "measurement": MEASUREMENT_NAME,
                "tags": {},
                "time": row["Date/time UTC"],  # Assuming CSV has a "Date/time UTC" column
                "fields": {
                    "active-energy-wh": float(row["Active energy Wh"]),  # Assuming CSV has a "Active energy Wh" column
                    "returned-energy-wh": float(row["Returned energy Wh"]),     # Assuming CSV has a "Returned energy Wh" column
                    "min-v": float(row["Min V"]),     # Assuming CSV has a "Min V" column
                    "max-v": float(row["Max V"])     # Assuming CSV has a "Max V" column
                    # Add more fields if needed
                }
            }
        ]
        print("Writting row with datetime." + row["Date/time UTC"])
        client.write_points(json_body)

# Close the InfluxDB connection
client.close()

print("Data insertion complete.")
# Script to read data from a Shelly device using MQTT messages and then store the value in a InfluxDB timeseries database.


import paho.mqtt.client as mqtt
#from influxdb import InfluxDBClient
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the current directory
load_dotenv()

# MQTT Configuration
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST")
MQTT_BROKER_PORT = 1883
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC = "shellies/70c590f9f395fbae/#" # Add your MQTT topics here

# InfluxDB Configuration
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_MEASUREMENT = ""



# Create InfluxDB client
#influxdb_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, database=INFLUXDB_DATABASE)

# MQTT on_connect callback
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe(MQTT_TOPIC)

# MQTT on_message callback
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        # Create InfluxDB data point
        data_point = {
            "measurement": INFLUXDB_MEASUREMENT,
            "time": timestamp,
            "tags": {
                "topic": msg.topic
            },
            "fields": payload
        }
        
        # Write the data point to InfluxDB
        #influxdb_client.write_points([data_point])
        print(f"Message to be stored on InfluxDB: {data_point}")
        print(f"Received and saved message from topic: {msg.topic}")
    except Exception as e:
        print(f"Error processing message: {e}")

# Create MQTT client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to MQTT broker
mqtt_client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)

# Start the MQTT client loop
mqtt_client.loop_start()

try:
    while True:
        time.sleep(5)
except KeyboardInterrupt:
    print("Exiting...")
    mqtt_client.disconnect()
    #influxdb_client.close()

from kafka import KafkaConsumer
import csv
from datetime import datetime
import os
import json

if os.path.isfile('weather-data.csv'):
	print("weather-data.csv already exists...\ndeleting...")
	os.remove('weather-data.csv')

consumer = KafkaConsumer('weather-stream',
                            auto_offset_reset='earliest',
                            bootstrap_servers=['localhost'],
                            api_version=(0, 10, 0),
                            value_deserializer = json.loads,
                            consumer_timeout_ms=1000)

with open('weather-data.csv', 'a') as f:
	fWriter = csv.writer(f)
	fWriter.writerow(['timestamp','temp','feels_like','pressure','humidity','dew_point','clouds','visibility','wind_speed','wind_deg','weather','pop'])
	print("CSV initialised")
	for message in consumer:
		msg = message.value
		fWriter.writerow([msg['dt'], msg['temp'], msg['feels_like'], msg['pressure'], msg['humidity'], msg['dew_point'], msg['clouds'], msg['visibility'], msg['wind_speed'], msg['wind_deg'], msg['weather'], msg['pop']])

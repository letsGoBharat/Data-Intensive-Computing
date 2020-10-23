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
	print("CSV initialised")
	for message in consumer:
		msg = str(message.value)
		fWriter.writerow([msg])

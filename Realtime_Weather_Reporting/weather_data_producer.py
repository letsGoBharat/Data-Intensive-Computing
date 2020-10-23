from kafka import KafkaProducer
from json import dumps
import time
import json
import requests
import uuid
from pprint import pprint

def get_api_weather_data():
    #personal API key
    API_KEY = 'fb68dc4a03f52b61ef70e6b9612e2aff'                    
    city = 'Stockholm'
    lat = 59.3326
    lon = 18.0649

    request = requests.get(f'https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={API_KEY}')
    json_data = json.loads(request.text)
    return json_data
    #print(len(json_data['hourly']))

def kafka_producer(ip, json_data):
    producer = KafkaProducer(bootstrap_servers=[ip+':9092'],
                            value_serializer=lambda x:
                            dumps(x).encode('utf-8'))

    for i, data in enumerate(json_data['hourly']):
        producer.send('weather-stream', data)
        time.sleep(2)

json_data = get_api_weather_data()
kafka_producer('localhost', json_data)
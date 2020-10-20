from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

def main():
    #Create Spark context and streaming context
    sc = SparkContext(appName = "WeatherForecastStream")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('ssc_checkpoint')

    #Connect to Kafka
    ip = 'localhost'
    ### auto.offset.reset - largest takes the latest messages and smallest takes from where the execution stopped last time
    kafkaParams = {"metadata.broker.list": ip+':9092', "auto.offset.reset": 'smallest'} 
    myStream = KafkaUtils.createDirectStream(ssc, ['weather-stream'], kafkaParams)

    counts = myStream.pprint()
    
    #Starting Spark context
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
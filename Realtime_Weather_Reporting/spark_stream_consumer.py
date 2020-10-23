from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import col
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StructType
import json
import csv
import os
from pprint import pprint
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, MapType, ArrayType, LongType
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

def save_as_csv(rdd):
    ss = SparkSession(rdd.context)
    if rdd.isEmpty():
        return
    df = ss.createDataFrame(rdd)
    df2 = df.withColumn('weather', col('weather').cast('string'))
    df2.write.options(header='True').mode('append').csv("Weather_Data_CSV")

def main():
    #Create Spark context and streaming context
    sc = SparkContext(appName = "WeatherForecastStream")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('ssc_checkpoint')

    sqlContext = SQLContext(sc)

    #Connect to Kafka
    ip = 'localhost'
    ### auto.offset.reset - largest takes the latest messages and smallest takes from where the execution stopped last time
    kafkaParams = {"metadata.broker.list": ip+':9092', "auto.offset.reset": 'smallest'} 
    myStream = KafkaUtils.createDirectStream(ssc, ['weather-stream'], kafkaParams)
    weatherStream = myStream.map(lambda x: json.loads(x[1]))

    weatherStream.foreachRDD(lambda x: save_as_csv(x))

    weatherStream.pprint()
    
    #Starting Spark context
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
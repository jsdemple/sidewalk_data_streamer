import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.port=4040 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 pyspark-shell'
import time

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
conf = SparkConf() \
    .setAppName("Streaming test") \
    .setMaster("local[2]") \
    .set("spark.cassandra.connection.host", "127.0.0.1")
sc = SparkContext(conf=conf) 
sqlContext=SQLContext(sc)
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import argparse
import csv
import time
import json

from utils import parse_line_into_schema

from fastavro import writer, reader, parse_schema
import avro_schemas

debug = True

# PARSE ARGS AND SET PATHS, TOPICS
KEYSPACE = 'raw_instrument'
DATA_OUT_DIR = '/data/'
DMI_OUT_FILEPATH = DATA_OUT_DIR + 'dmi.csv'
IMU_OUT_FILEPATH = DATA_OUT_DIR + 'imu.csv'
LIDAR_OUT_FILEPATH = DATA_OUT_DIR + 'lidar.csv'
GPS_OUT_FILEPATH = DATA_OUT_DIR + 'gps.csv'
INSTRUMENTS = ['imu', 'dmi', 'lidar', 'gps']

if debug:
    instrument = 'gps'
else:
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--instrument', help='which instrument data to save to disk {0}'.format(INSTRUMENTS))
    args = parser.parse_args()
    instrument = args.instrument.lower()
    
if 'dmi' in instrument:
    filepath = DMI_OUT_FILEPATH
    schema = avro_schemas.dmi
    field_names = [d['name'] for d in schema['fields']]
    TOPIC = 'dmi'
elif 'imu' in instrument:
    filepath = IMU_OUT_FILEPATH
    schema = avro_schemas.imu
    field_names = [d['name'] for d in schema['fields']]
    TOPIC = 'imu'
elif 'lidar' in instrument:
    filepath = LIDAR_OUT_FILEPATH
    schema = avro_schemas.lidar
    field_names = [d['name'] for d in schema['fields']]
    TOPIC = 'lidar'
elif 'gps' in instrument:
    filepath = GPS_OUT_FILEPATH
    schema = avro_schemas.gps
    field_names = [d['name'] for d in schema['fields']]
    TOPIC = 'gps'
else:
    print('ERROR: ARGUMENT {0} NOT IN INSTRUMENTS {1}'.format(args.instrument, INSTRUMENTS))
    assert False

parsed_schema = parse_schema(schema)


def saveToCassandra(rows):
    if not rows.isEmpty(): 
        sqlContext.createDataFrame(rows).write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=TOPIC, keyspace=KEYSPACE)\
        .save()
        
        
ssc = StreamingContext(sc, 5)
kvs = KafkaUtils.createStream(ssc, "127.0.0.1:2181", "spark-streaming-consumer", {TOPIC: 1})
#data = kvs.map(lambda x: x[1])
data = kvs.map(lambda x: json.loads(x[1]))
# rows= data.map(lambda x:Row(time_sent=x,time_received=time.strftime("%Y-%m-%d %H:%M:%S")))
# rows = data.map(lambda x: x.split(',')).map(lambda x: Row(field_names, x))
# rows = data.map(lambda x: x.split(','))
rows = data.map(lambda x: x)
#rows = data.map(lambda x: parse_line_into_schema(x, schema))
#rows= data.map(lambda x:Row(x))

rows.foreachRDD(saveToCassandra)
rows.pprint()



ssc.start()

# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.port=4040 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 pyspark-shell'
# import time

# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SQLContext, Row
# conf = SparkConf() \
#     .setAppName("Streaming test") \
#     .setMaster("local[2]") \
#     .set("spark.cassandra.connection.host", "127.0.0.1")
# sc = SparkContext(conf=conf) 
# sqlContext=SQLContext(sc)
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils

# import argparse
# import csv
# import time
# import json

# from utils import parse_line_into_schema

# from fastavro import writer, reader, parse_schema
# import avro_schemas


# # PARSE ARGS AND SET PATHS, TOPICS
# KEYSPACE = 'raw_instrument'
# DATA_OUT_DIR = '/data/'
# DMI_OUT_FILEPATH = DATA_OUT_DIR + 'dmi.csv'
# IMU_OUT_FILEPATH = DATA_OUT_DIR + 'imu.csv'
# LIDAR_OUT_FILEPATH = DATA_OUT_DIR + 'lidar.csv'
# GPS_OUT_FILEPATH = DATA_OUT_DIR + 'gps.csv'
# INSTRUMENTS = ['imu', 'dmi', 'lidar', 'gps']

# parser = argparse.ArgumentParser()
# parser.add_argument('-i', '--instrument', help='which instrument data to save to disk {0}'.format(INSTRUMENTS))
# args = parser.parse_args()
# instrument = args.instrument.lower()
    
# if 'dmi' in instrument:
#     filepath = DMI_OUT_FILEPATH
#     schema = avro_schemas.dmi
#     field_names = [d['name'] for d in schema['fields']]
#     TOPIC = 'dmi'
# elif 'imu' in instrument:
#     filepath = IMU_OUT_FILEPATH
#     schema = avro_schemas.imu
#     field_names = [d['name'] for d in schema['fields']]
#     TOPIC = 'imu'
# elif 'lidar' in instrument:
#     filepath = LIDAR_OUT_FILEPATH
#     schema = avro_schemas.lidar
#     field_names = [d['name'] for d in schema['fields']]
#     TOPIC = 'lidar'
# elif 'gps' in instrument:
#     filepath = GPS_OUT_FILEPATH
#     schema = avro_schemas.gps
#     field_names = [d['name'] for d in schema['fields']]
#     TOPIC = 'gps'
# else:
#     print('ERROR: ARGUMENT {0} NOT IN INSTRUMENTS {1}'.format(args.instrument, INSTRUMENTS))
#     assert False

# parsed_schema = parse_schema(schema)


# def saveToCassandra(rows):
#     if not rows.isEmpty(): 
#         sqlContext.createDataFrame(rows).write\
#         .format("org.apache.spark.sql.cassandra")\
#         .mode('append')\
#         .options(table=TOPIC, keyspace=KEYSPACE)\
#         .save()

# print('TOPIC: {0}'.format(TOPIC))
        
# ssc = StreamingContext(sc, 5)
# kvs = KafkaUtils.createStream(ssc, "127.0.0.1:2181", "spark-streaming-consumer", {TOPIC: 1})
# data = kvs.map(lambda x: json.loads(x[1]))
# rows = data.map(lambda x: x)
# rows.foreachRDD(saveToCassandra)
# rows.pprint()


# if __name__ == "__main__":
#     ssc.start()
#     #ssc.awaitTermination()

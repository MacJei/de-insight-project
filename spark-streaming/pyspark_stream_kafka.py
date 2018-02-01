from __future__ import print_function
import sys
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
import json
from datetime import datetime, timedelta

"""
Run Spark streaming job using the following command:
 nohup spark-submit --master spark://ip-10-0-0-13:7077 \
 --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.2.1 \
 /home/ubuntu/python-consumer/pyspark_stream.py web_parser2 web_traffic https://kinesis.us-east-1.amazonaws.com us-east-1 &
"""

# Start Spark session in order to be able to use sparksql and dataframes with pyspark dstreams.
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(time, rdd):
	spark = getSparkSessionInstance(rdd.context.getConf())
	try:	
		df = spark.read.json(rdd)
		df.createOrReplaceTempView("raw_logs")	
	
		# Spark SQL query to partiition the data for S3 and pre sort data.
		batchDF = spark.sql("""SELECT
					cast(UNIX_TIMESTAMP(timestamp, 'dd-MM-yyyy H:m:s') as TIMESTAMP) timestamp,
                	to_date(from_utc_timestamp(cast(UNIX_TIMESTAMP(current_timestamp,'dd-MM-yyyy') as TIMESTAMP),'EST')) upload_date, # Partition field
                	date_format(from_utc_timestamp(cast(UNIX_TIMESTAMP(current_timestamp,'dd-MM-yyyy H:m:s') as TIMESTAMP),'EST'), 'H') upload_hour, # Partition field
                	cast(date_format(cast(UNIX_TIMESTAMP(current_timestamp,'dd-MM-yyyy H:m:s') as TIMESTAMP), 'm')/5 as integer) upload_interval, # Partition field
					ip, 
					user_id, 
					user_agent, 
					event_type, 
					product_id 
					from raw_logs
					order by 1 desc""")

		# Spark SQL query to aggregate data, and to transform the data. Also partitions the data
		agg_events = spark.sql("""SELECT        
                        to_date(cast(UNIX_TIMESTAMP(timestamp,'dd-MM-yyyy') as TIMESTAMP)) event_date,
                        date_format(cast(UNIX_TIMESTAMP(timestamp,'dd-MM-yyyy H:m:s') as TIMESTAMP), 'h') hour,
                        date_format(cast(UNIX_TIMESTAMP(timestamp,'dd-MM-yyyy H:m:s') as TIMESTAMP), 'm') minute,
               			to_date(from_utc_timestamp(cast(UNIX_TIMESTAMP(current_timestamp,'dd-MM-yyyy') as TIMESTAMP),'EST')) upload_date, # Partition field
               			date_format(from_utc_timestamp(cast(UNIX_TIMESTAMP(current_timestamp,'dd-MM-yyyy H:m:s') as TIMESTAMP),'EST'), 'H') upload_hour, # Partifion field
               			cast(date_format(cast(UNIX_TIMESTAMP(current_timestamp,'dd-MM-yyyy H:m:s') as TIMESTAMP), 'm')/5 as integer) upload_interval, # Partition field
                        split(user_agent,"/")[0] browser,
                        case when split(user_agent,'\\\\(')[1] like '%Linux%' then 'Linux'
                        when split(user_agent,'\\\\(')[1] like '%Windows%' then 'Windows'
                        when split(user_agent,'\\\\(')[1] like '%Mac%' then 'iOS'
                        else 'Other' end os,
                        product_id,
                        count(case when event_type = 'pageView' then 1 end) page_views,
                        count(case when event_type = 'click' then 1 end) clicks,
                        count(case when event_type = 'purchase' then 1 end) purchases,
                        count(case when event_type = 'addToCart' then 1 end) add_to_cart
                        from raw_logs
                        group by 1,2,3,4,5,6,7,8,9
                        order by 1,2,3 """)


		batchDF.coalesce(1).write.partitionBy('upload_date','upload_hour','upload_interval').mode('append').csv("s3n://insight-spark-stream-files/event_logs",sep='|')
		agg_events.coalesce(1).write.partitionBy('upload_date','upload_hour','upload_interval').mode('append').csv("s3n://insight-spark-stream-files/event_aggs",sep='|')
		
		batchDF.show()


	except:
		pass

if __name__=="__main__":
        if len(sys.argv) != 3:
            print(
                "Usage: pyspark_stream_kafka.py <zkQuorum> <1>",
                file=sys.stderr)
            sys.exit(-1)

	sc = SparkContext(appName="Spark Streaming App")
	ssc = StreamingContext(sc,60)
        zkQuorum, topic = sys.argv[1:]
        kvs = KafkaUtils.createStream(
            ssc, zkQuorum, "spark-streaming-consumer", {topic: 6})
	lines = kvs.map(lambda x: x[1])
	#lines.pprint()
	#json_lines = lines.map(lambda x: json.loads(x))
	#json_lines.pprint()
	#windowed_lines = lines.window(60).flatMap(lambda x: x.split("\n"))
	# Split the spark context lines by the newline delimiter
	sc_lines = lines.flatMap(lambda x: x.split('\n'))
	sc_lines.pprint(1)
	# For each dstream RDD, apply the processing
	lines.foreachRDD(process)

	ssc.start()
	ssc.awaitTermination()


'''Read the streaming data from a Kafka topic and aggregates it for the country and the time window.
The result will be stored in a Kafka topic in a JSON fomrat.
Arg - Nothing required - All are configured in the application properties file
'''

from pyspark.sql.functions import from_utc_timestamp, to_timestamp, lit, concat, desc, from_json, to_json, to_timestamp, window, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from datetime import datetime
from time import sleep
from util.applogger import getAppLogger
from util.appconfigreader import AppConfigReader


'''Read the streaming data and shows the results in JSON format
Args - Spark Session, streamingSchema, json location, checkpoint directory, processing time interval, max wait count'''

def streamingStatus(streamingHandler):
    '''Returns the current status of the streaming DF'''
    return streamingHandler.isActive

def stopStreaming(streamingHandler):
    '''Check the progress of the stream passed and stops it if required'''
    try:
        stopFlag = False
        logger.info(f"Stopping the stream {streamingHandler.id}")
        streamingHandler.stop()
        sleep(eventInterval*60)
        while streamingStatus(streamingHandler):
            logger.info(f"Streaming {streamingHandler.id} has not stopped hence going to stop again and wait for {eventInterval*60} seconds")
            streamingHandler.stop()
            sleep(eventInterval*60)
        else:
            logger.info(f"Streaming {streamingHandler.id} has stopped now")
            stopFlag = True
    except Exception as error:
        logger.exception(f"Failed to stop the streaming {error}")
        return True
    else:
        return stopFlag

def start_streaming(spark, streamingSchema, checkpointDir, maxStopCount, streamingOutputMode, eventInterval, kafkaHost, kafkaPort, consumeTopic, produceTopic):
    '''Starts the streaming and returns the handler'''
    try:
        # tweetStreaming = spark.readStream.schema(streamingSchema).json(jsonDir)
        # self.tweetStreamingQuery = tweetStreaming.select("*").writeStream.trigger(processingTime=f'{processingDuration} seconds').queryName(
        # "tweetsQuery").format("memory").outputMode("append").start()

        #Read data from source kafka topic and convert the value into columns, the value is in JSON format

        logger.info(f"Create streaming with the details configured Source-{consumeTopic}, target-{produceTopic}, Window time-{eventInterval} minutes")
        tweetStreaming = spark.readStream.format("kafka").option("kafka.bootstrap.servers",kafkaHost+':'+kafkaPort)\
        .option("subscribe",consumeTopic).option("startingOffsets","latest").load()\
        .withColumn("j_value",from_json(col("value").cast("string"),streamingSchema))\
        .select("j_value.user","j_value.place",to_timestamp(col("j_value.creationTime"),'EEE MMM dd hh:mm:ss +0000 yyyy').alias('creationTime'),"partition","offset","j_value.msg")

        #Start the streaming, write the result into kafka topic with value as JSON format
        tweetStreamingQuery = tweetStreaming.withWatermark("creationTime", "1 minutes").where("place != ''")\
        .groupBy(window(col("creationTime"),str(eventInterval)+" minutes"),"place").count()\
        .withColumn("value",concat(lit('{ "country":"'),col('place'),lit('", "count":'), col('count'), lit(', "time_period":'),to_json(col('window')),lit('}')))\
        .select(lit('country_count').alias('key'),"value").writeStream.queryName("tweetsQuery")\
        .format("kafka").option("kafka.bootstrap.servers",kafkaHost+':'+kafkaPort).option("topic",produceTopic)\
        .option("checkpointLocation",checkpointDir).outputMode(streamingOutputMode).start()

        # Required in production scenario so that driver program keeps running until streaming queries completes
        #tweetStreamingQuery.awaitTermination() #timeout=self.eventInterval*60*3

    except Exception as error:
        logger.exception(f"Failed to start the streaming {error}")
        raise
    else:
        logger.info(f"Streaming ID-{tweetStreamingQuery.id} and name-{tweetStreamingQuery.name} has started successfully")
        return tweetStreamingQuery

if __name__ == '__main__':
    try:
        logger = getAppLogger(__name__)

        # Read application CFG file
        appConfigReader = AppConfigReader()
        if 'APP' in appConfigReader.config:
            appCfg = appConfigReader.config['APP']
            checkpointDir = appCfg['CHECKPOINT_DIR']
            maxStopCount = int(appCfg['MAX_STOP_RETRY'])
            streamingOutputMode = appCfg['STREAMING_OUTPUT_MODE']
            eventInterval = int(appCfg['EVENT_INTERVAL'])
            kafkaHost = appCfg['KAFKA_SERVER_NAME']
            kafkaPort = appCfg['KAFKA_PORT']
            consumeTopic = appCfg['CONSUME_TOPIC']
            consumerTimeout = int(appCfg['CONSUMER_TIMEOUT'])
            produceTopic = appCfg['TARGET_TOPIC']
        else:
            logger.error("Application details are missed out to configure")
            raise SystemExit(1)

        stopCount = 1

        #Create a spark session
        spark = SparkSession.builder.appName("TwitterStreaming").getOrCreate()
        if spark:
            streamingSchema = StructType([StructField("user", StringType(), False),
                                          StructField("place", StringType(), True),
                                          StructField("msg", StringType(), True),
                                          StructField("creationTime",StringType(),True)])

            #Start the streaming
            streamingHandler = start_streaming(spark, streamingSchema, checkpointDir, maxStopCount, streamingOutputMode,
             eventInterval, kafkaHost, kafkaPort, consumeTopic, produceTopic)

            #streamingHandler.awaitTermination()

            logger.info(f"Status of the streaming {streamingHandler.id} is {streamingStatus(streamingHandler)}")

            while stopCount <= maxStopCount:
                sleep(eventInterval*60)
                logger.info(f"Check it is required to stop the streaming now which status is {streamingStatus(streamingHandler)} currently")
                logger.info(f"Number of rows processed by the last batch is {streamingHandler.lastProgress['numInputRows']}")
                if streamingHandler.lastProgress["numInputRows"] != None and int(streamingHandler.lastProgress["numInputRows"]) == 0:
                    stopCount += 1
                else:
                    stopCount = 1
            else:
                logger.info("There is no new feed in the stream to process, stop the active streaming")
                stopFlag = stopStreaming(streamingHandler)

    except Exception as error:
        logger.exception(f"Failed to start the streaming {error}")
        raise
    else:
        logger.info("Streaming has completed processing the data")
    finally:
        spark.stop()

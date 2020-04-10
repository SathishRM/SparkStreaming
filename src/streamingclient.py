from pyspark.sql import SparkSession
from pyspark.sql.functions import from_utc_timestamp, to_timestamp, lit, concat, desc
from pyspark.sql.types import StructField, StructType, StringType
import argparse
from time import sleep
from util.appconfigreader import AppConfigReader
from util.applogger import getAppLogger
from streamingdata import StreamingData


if __name__ == '__main__':
    try:
        logger = getAppLogger(__name__)

        # Read application CFG file
        appConfigReader = AppConfigReader()
        if 'APP' in appConfigReader.config:
            appCfg = appConfigReader.config['APP']
            jsonDir = appCfg['JSON_DIR']
            checkpointDir = appCfg['CHECKPOINT_DIR']
            maxFileTrigger = int(appCfg['MAX_FILE_TRIGGER'])
            processingDuration = int(appCfg['PROCESSING_DURATION'])
            maxStopCount = int(appCfg['MAX_STOP_RETRY'])
        else:
            logger.error("Application details are missed out to configure")
            raise SystemExit(1)
        print(f"{jsonDir} {checkpointDir} {checkpointDir} {processingDuration}")
        # Parse the argument passed
        argParser = argparse.ArgumentParser()
        argParser.add_argument(
            "process", help="Should be one of these processes Start")
        args = argParser.parse_args()
        if args.process.lower() == 'start':
            spark = SparkSession.builder.appName("TwitterStreaming").getOrCreate()
            if spark:
                logger.info("Spark session has created")
                streamingSchema = StructType([StructField("userName", StringType(), False),
                                              StructField("userLocation", StringType(), True),
                                              StructField("msg", StringType(), True)])
                streamingDF = StreamingData(
                    spark, streamingSchema, jsonDir, checkpointDir, processingDuration, maxStopCount)

                stopCount = 1

                while streamingDF.streamingStatus():
                    # Get 10 locations with most tweetStreaming
                    logger.info("List of 10 countries with the most number of tweets")
                    topCountries = streamingDF.top10Countries()
                    if topCountries:
                        for country in topCountries:
                            logger.info(f"{country}")
                    else:
                        logger.info("No new feeds to update")

                    # Count the number of tweets from a countryCount
                    country = "INDIA"
                    logger.info(f"Count the number of tweets from {country}")
                    countTweet = streamingDF.countryCount(country)
                    if countTweet:
                        logger.info(f"{countTweet}")
                    else:
                        logger.info("No new feeds to update")

                    # Get the country with the maximum number of tweets
                    logger.info("Get the country which tweets the most")
                    maxTweetCountry = streamingDF.topCountry()
                    if maxTweetCountry:
                        logger.info(f"{maxTweetCountry}")
                    else:
                        logger.info("No new feeds to update")

                    # Get the user with the maximum number of tweets
                    logger.info("Get the user who tweeted the most")
                    user = streamingDF.topTweetsByUser()
                    if user:
                        logger.info(f"{user}")
                    else:
                        logger.info("No new feeds to update")

                    # Check if need to stop the streaming
                    stopFlag = streamingDF.stopStreaming()
                    if stopFlag:
                        break

                    sleep(processingDuration)
        else:
            logger.error(
                f"Incorrect parameter {args.process}, please checkt the script help for the list of allowed arguments")
            raise SystemExit(2)
    except Exception as error:
        logger.exception(f"Something went wrong here {error}")
    else:
        logger.info("Streaming has completed processing the feeds")
    finally:
        spark.stop()

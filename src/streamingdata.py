'''Read the streaming data and shows the results in JSON format'''

from pyspark.sql.functions import from_utc_timestamp, to_timestamp, lit, concat, desc
from datetime import datetime
from time import sleep
from util.applogger import getAppLogger

logger = getAppLogger(__name__)


class StreamingData:

    def __init__(self, spark, streamingSchema, jsonDir, checkpointDir, processingDuration, maxStopCount):
        '''Create streaming
        Args - Spark Session, streamingSchema, json location'''
        try:
            self.spark = spark
            tweetStreaming = spark.readStream.schema(streamingSchema).json(jsonDir)
            self.tweetStreamingQuery = tweetStreaming.select("*").writeStream.trigger(processingTime=f'{processingDuration} seconds').queryName(
                "tweetsQuery").format("memory").outputMode("append").start()

            self.processingDuration = processingDuration
            self.maxStopCount = maxStopCount
            # Required in production scenario so that driver program keeps running until streaming queries completes
            self.tweetStreamingQuery.awaitTermination(timeout=self.processingDuration*2)
            self.stopCount = 1
        except Exception as error:
            logger.exception(f"Failed to start the streaming {error}")
            raise
        else:
            logger.info("Streaming has started successfully")

    def streamingStatus(self):
        return self.tweetStreamingQuery.isActive

    def top10Countries(self):
        '''Returns the list of top 10 tweeting countries'''
        if self.streamingStatus():
            # if int(streamingQuery.lastProgress["numInputRows"]):
                # if lastProcessingTime(streamingQuery.lastProgress["timestamp"]) < 300:
            result = self.spark.sql(
                "select userLocation,count(*) as count from tweetsQuery where userLocation != '' group by userLocation order by count desc limit 10")
            return result.select("UserLocation").toJSON().collect()
        return None

    def topCountry(self):
        '''Returns the country which has most number of tweets'''
        if self.streamingStatus():
            # if int(streamingQuery.lastProgress["numInputRows"]):
                # if lastProcessingTime(streamingQuery.lastProgress["timestamp"]) < 300:
            result = self.spark.sql(
                "select userLocation as country,count(*) as count from tweetsQuery where userLocation != '' group by userLocation order by count desc limit 1")
            return result.toJSON().collect()
        return None

    def countryCount(self, country):
        '''Return the tweet count of the country
        Arg - Name of a country'''
        if self.streamingStatus():
            # if int(streamingQuery.lastProgress["numInputRows"]):
                # if lastProcessingTime(streamingQuery.lastProgress["timestamp"]) < 300:
            tweetCount = self.spark.sql(
                f"select count(userName) as tweetCount from tweetsQuery where lower(userLocation) like '%{country.lower()}%'")
            countryTweetCount = tweetCount.withColumn(
                "country", lit(country)).select("country", "tweetCount")
            return countryTweetCount.toJSON().collect()
        return None

    def topTweetsByUser(self):
        '''Returns the user with maximum number of tweets'''
        if self.streamingStatus():
            # if int(streamingQuery.lastProgress["numInputRows"]):
                # if lastProcessingTime(streamingQuery.lastProgress["timestamp"]) < 300:
            result = self.spark.sql(
                "select userName,count(*) as count from tweetsQuery group by userName order by count desc limit 1")
            return result.toJSON().collect()
        return None

    def lastProcessingTime(self, lastProcessTime):
        '''Returns the seconds difference between current time and last processed streaming time
        Arg - Last processing UTC datetime in string format YYYY-MM-DDThh:mm:ss.SSSZ'''
        if lastProcessTime:
            lastProcessTime = datetime.strptime(
                lastProcessTime.replace("T", " ")[:-5], "%Y-%m-%d %H:%M:%S")
            return (datetime.utcnow() - lastProcessTime).total_seconds()
        return 0

    def stopStreaming(self):
        '''Stops the active streaming'''
        try:
            stopFlag = False
            if not int(self.tweetStreamingQuery.lastProgress["numInputRows"]):
                if self.stopCount >= self.maxStopCount:
                    logger.info(
                        "There is no new feed in the stream to process, stop the active streaming")
                    self.tweetStreamingQuery.stop()
                    sleep(self.processingDuration)
                    if self.streamingStatus():
                        self.tweetStreamingQuery.stop()
                        sleep(self.processingDuration)
                    stopFlag = True
                self.stopCount += 1
        except Exception as error:
            logger.exception(f"Failed to stop the streaming {error}")
            return True
        else:
            return stopFlag

'''
Fetch the messages from the Kafka topic for a specific country which is passed as a argument
Arg (-c) - Name of a country (Optional)
If no country name is passed, script will look into the application properties file for the default country
'''

import argparse
from time import sleep
from kafka import KafkaConsumer
from json import loads
from util.appconfigreader import AppConfigReader
from util.applogger import getAppLogger


if __name__ == '__main__':
    try:
        logger = getAppLogger(__name__)

        # Read application CFG file
        appConfigReader = AppConfigReader()
        if 'APP' in appConfigReader.config:
            appCfg = appConfigReader.config['APP']
            kafkaHost = appCfg['KAFKA_SERVER_NAME']
            kafkaPort = appCfg['KAFKA_PORT']
            consumerTimeout = int(appCfg['CONSUMER_TIMEOUT'])
            produceTopic = appCfg['TARGET_TOPIC']
            defaultCountry = appCfg['DEFAULT_COUNTRY']
        else:
            logger.error("Application details are missed out to configure")
            raise SystemExit(1)

        # Get the country to look for the aggregation
        argParser = argparse.ArgumentParser("Get tweets count for a country")
        argParser.add_argument("-c","--country", help="Name of the country to check", default=defaultCountry)
        args = argParser.parse_args()

        logger.info(f"Read the streaming data of the country {args.country} from the topic {produceTopic}")
        consumer = KafkaConsumer(bootstrap_servers=kafkaHost+':'+kafkaPort, auto_offset_reset='latest',
         consumer_timeout_ms=consumerTimeout, value_deserializer=lambda x: loads(x.decode('utf-8')))
        consumer.subscribe(topics=produceTopic)

        #Read data from the subscribed topics for the specific country
        logger.info("Reading the messages...")
        for message in consumer:

            if message.key.decode() == 'country_count' and message.value['country'].lower() == args.country.lower():
                logger.info(f"Partition: {message.partition}, Offset: {message.offset}, Value: {message.value}")
        else:
            logger.info("Completed reading messages from the topic")

    except Exception as error:
        logger.exception(f"Something went wrong here {error}")
    else:
        logger.info("Streaming has completed processing the feeds")

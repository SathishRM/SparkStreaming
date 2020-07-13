from configparser import ConfigParser
import os


class AppConfigReader():
    '''Loads the config file values'''

    def __init__(self):
        self.config = ConfigParser()
        # Get the config file path from environmental variable PY_APP_CONFIG
        cfgDir = os.environ.get('SPARK_STREAMING_CFG_DIR')
        if cfgDir:
            cfgFile = cfgDir + "\\twitterstreaming.properties"
        else:
            cfgFile = "E:\\Spark\\github\\SparkStreaming\\conf\\twitterstreaming.properties"
        print(cfgFile)
        # Load the CFG file
        self.config.read(cfgFile)

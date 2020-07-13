### Process tweets saved in a Kafka topic continuously and write the results into a Kafka topic in JSON format which can be shown on any dashboard 

Look at the file **requirements.txt** for any external library required by the script. Make sure those are installed first.

Flow is to read tweets from a topic in Kafka which contains the details like user, place, actual message and the time with date. Process the tweets to aggregate the number for the countries for the time window specified in the config file (*EVENT_INTERVAL*). The final result is a JSON format text which is also stored in a Kafka topic which will be consumed by the downstream applications.

The streaming process will be stopped with the customized logic instead of using awaitTermination.
When there is no data for a certain amount of time (*MAX_STOP_RETRY\*EVENT_INTERVAL minutes*) in the input stream, this will be stoped gracefully.

#### Sample result: ####
{ "country":"Japan", "count":3, "time_period":{"start":"2020-07-13T03:45:00.000+08:00","end":"2020-07-13T03:50:00.000+08:00"}}
{ "country":"Paris", "count":1, "time_period":{"start":"2020-07-13T03:45:00.000+08:00","end":"2020-07-13T03:50:00.000+08:00"}}


Use spark-submit to run the script, provided adding the pyspark installation directory to PATH variable and have the soure code path in the variable PYTHON_PATH

*Command to start the streaming:*
**streamingdata.py**

*Command to run the client to consume the data:*
**streamingclient.py -c [country]**

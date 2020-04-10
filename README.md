### Process JSON files dropped in the input directory continuously and shows the results in JSON format  

Look at the file **requirements.txt** for any external library required by the script. Make sure those are installed first.

Use spark-submit to run the script, provided adding the pyspark installation directory to PATH variable and have the soure code path in the variable PYTHON_PATH

#### Arguments allowed: ####
  * -h, --help     show this help message and exit

  * status [start] - To begin the streaming process

*Command to run the script:*
**streamingclient.py start**

**Update the config file with the settings as per the environment.**

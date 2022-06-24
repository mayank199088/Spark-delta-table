# Spark-delta-table
#setup instructions :

First install hadoop, java , pyspark packages and create enviroments. you can install packages by entering following in notebook

!apt-get install openjdk-11-jdk-headless -qq > /dev/null

!wget -q https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz

!tar xf spark-3.1.2-bin-hadoop3.2.tgz

!pip -q install findspark

Create enviroments in "edit edviroment variables for your account" on your system

## Import necessary packages
import os
import findspark
### start new spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('session_name').getOrCreate()
#### read csv file into dataframe
df=spark.read.format('csv').load('file path',header=True)
##### write dataframe as delta table
df.write.format("delta").saveAsTable("Table_name")

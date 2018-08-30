#############################################################
#
# Cleanup project data folder 
#
#############################################################
!rm ~/data/*.*
!rmdir ~/data

#############################################################
#
# Cleanup HDFS 
#
#############################################################

!hdfs dfs -rm -r flightdelay/weather
!hdfs dfs -rm -r flightdelay/flights
!hdfs dfs -rm -r flightdelay

#############################################################
#
# Cleanup Hive tables 
#
#############################################################

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FlightDelay-Cleanup").getOrCreate()
spark.sql('drop table if exists flightDelay.flights_weatherhourly')
spark.sql('drop database flightDelay')

# Weather and flight delay
Created by Jessie Lin (jlin@cloudera.com)

## Status: In Progress
## Use Case: 
This project attempts to use historical flight and weather to predict departure delay.
So it could be used to predict delay as soon as weather forecast is known and could be improved as departure day come closure as weather forecast gets more accurate.

## Steps:
1. In Terminal run setup.sh to download weather data and copy to HDFS. It downloads dataset for 10 years and can take up to 30 minutes. 
If Hive has flightDelay.flights_OriginWeather table ready, you can skip setup.sh and ingest.scala 
2. Launch Scala Engine (at least 2 core 8GB memory) and use Ingest.scala to ingest data from HDFS to Hive flightDelay.flights_OriginWeather
3. In Python Engine and run cleanup.py

## Recommended Session Sizes: 
Scala for Ingestion: 2 cores 16GB memory

## Recommended Jobs/Pipeline:


## Notes:

## Estimated Runtime:
1. Ingest.scala

## Demo Script
Ingest.scala

## Related Content
1. Aki wrote a [blog](http://blog.cloudera.com/blog/2017/02/analyzing-us-flight-data-on-amazon-s3-with-sparklyr-and-apache-spark-2-0/)  on predicting arrival delay using vairables such as departure delay 
The model very accurate, but it may only be used to predict after a flight is in the air.

1. Variable Descriptions of flight data at [Statistical Computing](http://stat-computing.org/dataexpo/2009/the-data.html) website
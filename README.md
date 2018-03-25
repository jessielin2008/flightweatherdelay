# Weather and flight delay
Created by Jessie Lin (jlin@cloudera.com)

## Status: In Progress
## Use Case: 
To combine weather and flight data to predict flight departure delay

## Steps:
1. use setup.sh to download weather data and copy to HDFS
2. use setupFlights.sh to download flight data and copy to HDFS
3. use Ingest.scala to ingest data from HDFS to Hive


## Recommended Session Sizes: 
Scala for Ingestion: 1 core 8GB memory

## Recommended Jobs/Pipeline:


## Notes:

## Estimated Runtime:
1. Ingest.scala

## Demo Script
Ingest.scala

## Related Content
Aki wrote a [blog](http://blog.cloudera.com/blog/2017/02/analyzing-us-flight-data-on-amazon-s3-with-sparklyr-and-apache-spark-2-0/)  on predicting arrival delay using vairables such as departure delay 
It's very accurate, but it can be used after a flight is in the air.
And departure delay and distance can come up a pretty good model for arrival delay.
If we know the delay on departure, it largely depends on how much the it could catch up during the distance of the flight before it lands.
The rest is things like waiting for taxing.


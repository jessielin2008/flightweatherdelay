# Weather and flight delay
Created by Jessie Lin (jlin@cloudera.com)

## Status: In Progress
## Use Case: 
This project attempts to use historical flight and weather to predict departure delay.
So it could be used to predict delay as soon as weather forecast is known and could be improved as departure day come closure as weather forecast gets more accurate.

## Steps:
1. In Terminal run setup.sh to download flight and daily weather data and copy to HDFS. It downloads dataset for 4 years(2004-2007) and can take up to 10 minutes. 
   Then run setup_isd.sh to download hourly weather day. This script takes overnight. Don't run it if you don't need to.
   If Hive has flightDelay.flights_weatherhourly table ready, you can skip setup.sh and ingest.scala 
2. Launch Scala Engine (at least 2 core 8GB memory) and use Ingest-hourly.scala to ingest data from HDFS to Hive flightDelay.flights_weatherhourly
3. When completed demo, in Python Engine and run cleanup.py

## Recommended Session Sizes: 
1 core 16GB memory

## Recommended Jobs/Pipeline:

## Notes:
[Presentation](https://cloudera.box.com/s/247b9y6e6btc0rer24yfjfqhwgbxzk0t)
[Recording](https://cloudera.box.com/s/v8q7j5g7cb4lw2y7peozfeax50d37rxu)

## Estimated Runtime:
0. Setup.sh 10 minutes / Setup_isd.sh overnight
1. Ingest-hourly.scala - 30 minutes
2. analysis-hourly.py - 5 minutes
3. ml_binaryclass_interactive.py - 15 minutes
4. ml_regression_interactive.py - 30 minutes
5. ml_rfc_experiment.py - 2 minutes
6. predict_weatherdelay.py

## Demo Script
1. ingest-hourly.scala Ingest pipeline
2. analysis-hourly.py 
3. predict_weatherdelay.py 

## Related Content
1. Aki wrote a [blog](http://blog.cloudera.com/blog/2017/02/analyzing-us-flight-data-on-amazon-s3-with-sparklyr-and-apache-spark-2-0/)  on predicting arrival delay using vairables such as departure delay 

2. Variable Descriptions of flight data at [Statistical Computing](http://stat-computing.org/dataexpo/2009/the-data.html) website

3. [Flight Delay notebook on Kaggle](https://www.kaggle.com/fabiendaniel/predicting-flight-delays-tutorial)

4.[Daily weather data(GSOD)](https://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.ncdc:C00516)
  [Description](https://www1.ncdc.noaa.gov/pub/data/gsod/readme.txt)
   
5.[Hourly weather data(ISD)](https://www.ncdc.noaa.gov/isd)
  [Description](ftp://ftp.ncdc.noaa.gov/pub/data/noaa/ish-format-document.pdf)
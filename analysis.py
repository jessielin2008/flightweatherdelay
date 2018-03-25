from pyspark.sql import SparkSession
#  Correlation between flight delay and weather conditions
#  ********************************************************************************

# resultDF.groupBy("Year","Month").count.sort($"count".desc).show

spark = SparkSession.builder.appName("FlightDelayAnalysis").getOrCreate()
correlDF = spark.sql("""select year, month, count(*) as totalNumFlights, sum(delay), sum(wdelay),
            round(sum(fogDelay)/sum(delay)*100) as pfogDelay, 
            round(sum(snowDelay)/sum(delay)*100) as psnowDelay, 
            round(sum(hailDelay)/sum(delay)*100) as phailDelay, 
            round(sum(thunderDelay)/sum(delay)*100) as pthunderDelay, 
            round(sum(tornadoDelay)/sum(delay)*100) as ptornadoDelay
            from ( select year, month, day, FlightNum,  
                case when DepDelay > 0 and Cancelled = 0 then 1 else 0 end as delay, 
                case when WeatherDelay > 0 and Cancelled = 0 then 1 else 0 end as wdelay, 
                case when DepDelay > 0 and Cancelled = 0 and fog='1' then 1 else 0 end as fogDelay, 
                case when DepDelay > 0 and Cancelled = 0 and snow='1' then 1 else 0 end as snowDelay, 
                case when DepDelay > 0 and Cancelled = 0 and hail='1' then 1 else 0 end as hailDelay, 
                case when DepDelay > 0 and Cancelled = 0 and thunder='1' then 1 else 0 end as thunderDelay, 
                case when DepDelay > 0 and Cancelled = 0 and tornado='1' then 1 else 0 end as tornadoDelay 
                from flightdelay.flights_OriginWeather 
            ) as b group by year, month 
""")

correlDF.show()

correlDF2 = spark.sql("""select year, month, count(*) as totalNumFlights, sum(delay), sum(wdelay),
            round(sum(fogDelay)/sum(fog)*100) as pfogDelay, 
            round(sum(snowDelay)/sum(snow)*100) as psnowDelay, 
            round(sum(hailDelay)/sum(hail)*100) as phailDelay, 
            round(sum(thunderDelay)/sum(thunder)*100) as pthunderDelay, 
            round(sum(tornadoDelay)/sum(tornado)*100) as ptornadoDelay
            from ( select year, month, day, FlightNum,  
                case when DepDelay > 0 and Cancelled = 0 then 1 else 0 end as delay, 
                case when WeatherDelay > 0 and Cancelled = 0 then 1 else 0 end as wdelay, 
                case when fog='1' then 1 else 0 end as fog, 
                case when snow='1' then 1 else 0 end as snow, 
                case when hail='1' then 1 else 0 end as hail, 
                case when thunder='1' then 1 else 0 end as thunder, 
                case when tornado='1' then 1 else 0 end as tornado,
                case when DepDelay > 0 and Cancelled = 0 and fog='1' then 1 else 0 end as fogDelay, 
                case when DepDelay > 0 and Cancelled = 0 and snow='1' then 1 else 0 end as snowDelay, 
                case when DepDelay > 0 and Cancelled = 0 and hail='1' then 1 else 0 end as hailDelay, 
                case when DepDelay > 0 and Cancelled = 0 and thunder='1' then 1 else 0 end as thunderDelay, 
                case when DepDelay > 0 and Cancelled = 0 and tornado='1' then 1 else 0 end as tornadoDelay 
                from flightdelay.flights_OriginWeather 
            ) as b group by year, month 
          order by sum(wdelay) desc
""")

correlDF2.show()
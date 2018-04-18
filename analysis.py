from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

#  ********************************************************************************

# verify pandas version
pd.__version__

spark = SparkSession.builder.appName("FlightDelayVisualization").getOrCreate()

# table format
delayDF = spark.sql("select * from flightdelay.flights_OriginWeather limit 10000")
delayDF.head()

plt.style.use('ggplot')

# # Flight Depature Delay distribution
# Shows that majority of flights departure earlier than planned!
# Method 1 using spark for heavy lifting
depDelayDF = spark.sql("select DepDelay from flightdelay.flights_OriginWeather limit 10000")
hists = depDelayDF.select("DepDelay").rdd.flatMap(
    lambda row: row
).histogram(30)


data = {
    'bins': hists[0][:-1],
    'freq': hists[1]
}
plt.bar(data['bins'], data['freq'], width = 8)


weatherDelayDF = spark.sql("select WeatherDelay from flightdelay.flights_OriginWeather where WeatherDelay > 0 limit 10000")
hists = weatherDelayDF.select("WeatherDelay").rdd.flatMap(
    lambda row: row
).histogram(30)
data = {
    'bins': hists[0][:-1],
    'freq': hists[1]
}
plt.bar(data['bins'], data['freq'], width = 8)

# Scatter plot Departure Delay and Weather Delay
delayP = spark.sql("select coalesce(WeatherDelay,0) as weatherDelay, coalesce(DepDelay,0) as depDelay  \
                   from flightdelay.flights_OriginWeather where WeatherDelay >0 limit 10000").toPandas()
delayP.describe()
ax = delayP.plot.scatter(x=0, y=1)
#ax.set_aspect('equal')

## box plot between weather Delay and weather conditions
wdDetailsDF =spark.sql("select case when Rain ='1' then 1 else 0 end as RainDelay,  \
                          case when snow ='1' then weatherDelay else 0 end as snowDelay, \
                          case when hail ='1' then weatherDelay else 0 end as HailDelay, \
                          case when thunder ='1' then weatherDelay else 0 end as ThunderDelay , \
                          case when tornado ='1' then weatherDelay else 0 end as TornadoDelay \
                      from flightdelay.flights_OriginWeather where weatherDelay > 0 limit 100000").toPandas()
#plt.xLabel="Minutes"
#wdDetailsDF.boxplot(ax=ax )
#axy = wdDetailsDF.boxplot()
#axy.set_ylabel="Minutes"
#wdDetailsDF.boxplot(ax=axy )
#wdDetailsDF.index.name = "test"
wdDetailsDF.boxplot()

# seaborn violinplot shows more information than boxplot. will try next time

# ## Snow Delay and SnowDepth
snowDelayDF =spark.sql("select snowDepth, snowDelay from (select SnowDepth, \
                          case when snow ='1' then weatherDelay else 0 end as snowDelay  \
                      from flightdelay.flights_OriginWeather ) where snowDelay > 0 limit 100000").toPandas()
snowDelayDF.plot.scatter(x=0, y=1)

# ## Snow Delay and SnowDepth
rainDelayDF =spark.sql("select Precipitation, rainDelay from (select Precipitation, \
                          case when rain ='1' and fog='0' and snow='0' and hail='0' and thunder='0' then weatherDelay else 0 end as rainDelay  \
                      from flightdelay.flights_OriginWeather ) where rainDelay > 0 ").toPandas()
rainDelayDF.plot.scatter(x=0, y=1)

# Correlation between 
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


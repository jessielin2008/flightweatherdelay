from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

#  ********************************************************************************
# # verify pandas version
pd.__version__

spark = SparkSession.builder.appName("FlightDelayVisualization").getOrCreate()

# # WeatherDelay by year
weatherDF = spark.sql("""
select year, sum(nullval), sum(notnullval), count(*) from (
select year, case when weatherDelay is null then 1 else 0 end as nullval, 
case when weatherDelay is not null then 1 else 0 end as notnullval
from flightdelay.flights_OriginWeather
) tmp group by year order by year""")
weatherDF.show()

# # Tune sample percentage to 1% = 400K flights between 2003 and 2008
# table format. sample without replacement
delayDF = spark.sql("select * from flightdelay.flights_OriginWeather where year >= 2003").sample(False, 0.01) 
delayDF.count()
delayDF.head()

plt.style.use('ggplot')

# # Depature Delay distribution
# Shows that majority of flights departure earlier than planned!
# This histogram method uses spark for heavy lifting
hists = delayDF.select("DepDelay").rdd.flatMap(
    lambda row: row
).histogram(30)
data = {
    'bins': hists[0][:-1],
    'freq': hists[1]
}
plt.bar(data['bins'], data['freq'], width = 8)

# # Weather Delay distribution
# It shows weather delay looks more like a Poisson distribution
hists = delayDF.select("WeatherDelay").where(delayDF.WeatherDelay>0).rdd.flatMap(
    lambda row: row
).histogram(30)
data = {
    'bins': hists[0][:-1],
    'freq': hists[1]
}
plt.bar(data['bins'], data['freq'], width = 8)

# # Scatter plot Departure Delay and Weather Delay
# It shows some linear relationship between these two variables, but there are other factors
# The majority of weather delay are within 240 minutes ~ 4 hours
delayP = spark.sql("select coalesce(WeatherDelay,0) as weatherDelay, coalesce(DepDelay,0) as depDelay  \
                   from flightdelay.flights_OriginWeather where WeatherDelay >0 and year >= 2003").sample(False, 0.01).toPandas()
delayP.describe()
ax = delayP.plot.scatter(x=0, y=1)

# #Show category
delayDF = spark.sql("""select delayCat, count(*) from (
                          select case when weatherDelay < 0 then '0.Early Arrival'
                          when weatherDelay> 0 and weatherDelay <= 15 then '1.Less than 15 Minutes' 
                          when weatherDelay> 15 and weatherDelay <= 60 then  '2.Between 15 Minutes and 1 Hour' 
                          when weatherDelay> 60 and weatherDelay <= 120 then '3.Between 1 Hour and 2 Hours' 
                          when weatherDelay > 120 and weatherDelay <= 240 then '4.Between 2 Hours and 4 Hours' 
                          when weatherDelay > 240 then '5.Beyond 4 Hours' end as delayCat 
                          from flightdelay.flights_OriginWeather 
                          where weatherDelay > 0 and year >= 2003
                     ) tmp
                     group by delayCat order by delayCat""")
delayDF.show()             
#delayDF.plot.bar(color='r',title='Frequency of Delay')

# # Box plot between weather Delay and weather conditions
wdDetailsDF =spark.sql("""select case when Rain ='1' then 1 else 0 end as rainDelay,  
                          case when snow ='1' then weatherDelay else 0 end as snowDelay, 
                          case when hail ='1' then weatherDelay else 0 end as hailDelay, 
                          case when thunder ='1' then weatherDelay else 0 end as thunderDelay, 
                          case when tornado ='1' then weatherDelay else 0 end as tornadoDelay, 
                          case when weatherDelay < 0 then '0.Early Arrival'
                            when weatherDelay> 0 and weatherDelay <= 15 then '1.Less than 15 Minutes' 
                            when weatherDelay> 15 and weatherDelay <= 60 then  '2.Between 15 Minutes and 1 Hour' 
                            when weatherDelay> 60 and weatherDelay <= 120 then '3.Between 1 Hour and 2 Hours' 
                            when weatherDelay > 120 and weatherDelay <= 240 then '4.Between 2 Hours and 4 Hours' 
                            when weatherDelay > 240 then '5.Beyond 4 Hours' 
                          end as delayCat
                      from flightdelay.flights_OriginWeather where weatherDelay > 0 and year >= 2003""").sample(False, 0.01).toPandas()
#plt.xLabel="Minutes"
#wdDetailsDF.boxplot(ax=ax)
axy = wdDetailsDF.boxplot()
#axy.set_ylabel="Minutes"
#wdDetailsDF.boxplot(ax=axy)
#wdDetailsDF.index.name = "test"
wdDetailsDF.boxplot()

# #Numberic values pairplot with weatherDelay
wdDetailsDF =spark.sql("""select case when weatherDelay < 0 then '0.Early Arrival'
                            when weatherDelay> 0 and weatherDelay <= 15 then '1.Less than 15 Minutes' 
                            when weatherDelay> 15 and weatherDelay <= 60 then  '2.Between 15 Minutes and 1 Hour' 
                            when weatherDelay> 60 and weatherDelay <= 120 then '3.Between 1 Hour and 2 Hours' 
                            when weatherDelay > 120 and weatherDelay <= 240 then '4.Between 2 Hours and 4 Hours' 
                            when weatherDelay > 240 then '5.Beyond 4 Hours' 
                          end as delayCat,
                          Temp,Visibility,WindSpeed,MaxWindSpeed,Precipitation,SnowDepth
                          from flightdelay.flights_OriginWeather 
                          where weatherDelay > 0 and year >= 2003 
                          """).sample(False, 0.01).toPandas()
#                          and Precipitation < 99 and SnowDepth < 999
#                          and visibility < 999 and windspeed < 999 and maxwindspeed < 999
wdDetailsDF_selected = wdDetailsDF[["Temp","Visibility","WindSpeed","MaxWindSpeed","Precipitation","SnowDepth","delayCat"]]
#sb.pairplot(wdDetailsDF_selected)
sb.pairplot(wdDetailsDF_selected, hue="delayCat")

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


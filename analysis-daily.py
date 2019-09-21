from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb
import numpy as np

#  ********************************************************************************
# ### verify pandas version
pd.__version__
sb.__version__

spark = SparkSession.builder.appName("FlightDelayVisualization").getOrCreate()

# # WeatherDelay summary by year
weatherDF = spark.sql("""
select year, sum(weatherDelay), sum(NOweatherDelay), count(*) as total from (
select year,
case when weatherDelay > 0 then 1 else 0 end weatherDelay,
case when weatherDelay = 0 then 1 else 0 end NOweatherDelay
from flightdelay.flights_weather
) tmp group by year order by year
""").toPandas()
print(weatherDF)

# # Tune sample percentage to 1% = 260K flights
# table format. sample without replacement
delayDF = spark.sql("select depDelay, weatherDelay from flightdelay.flights_weather").sample(False, 0.01)
delayDF.count()

# Example distribution for Departure Delay, Weather Delay using Spark for heavy lifting
def histgram(df, column):
  hists = df.select(column).rdd.flatMap(
      lambda row: row
  ).histogram(30)
  data = {
      'bins': hists[0][:-1],
      'freq': hists[1]
  }
  plt.bar(data['bins'], data['freq'], width = 8)
#Most flights depart earlier  
histgram(delayDF, "depDelay")
#Most flights have no or very little weather delay
histgram(delayDF, "weatherDelay")


# #Example distribution for 10% of weatherDelay > 0 using Pandas
delayDF = spark.sql("""select depDelay, weatherDelay, coalesce(log10(WeatherDelay),0) as logWeatherDelay
          from flightdelay.flights_weather where weatherDelay > 0""").sample(False, 0.1).toPandas() 
delayDF.count()
sb.distplot(delayDF["depDelay"])
sb.distplot(delayDF["weatherDelay"])
sb.distplot(delayDF["logWeatherDelay"])

# # Scatter plot Departure Delay and Weather Delay
# It shows some linear relationship between these two variables, but there are other factors
# The majority of weather delay are within 240 minutes ~ 4 hours
delayP = spark.sql("select coalesce(WeatherDelay,0) as weatherDelay, coalesce(DepDelay,0) as depDelay  \
                   from flightdelay.flights_weather where WeatherDelay >0").sample(False, 0.01).toPandas()
delayP.describe()
ax = delayP.plot.scatter(x=0, y=1)

#######################################################################################################
# # clustering of weather delay
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
### Test Clustering 2-10
def testClustering(dataset):
  assembler = VectorAssembler(
    inputCols=["logWeatherDelay"],
    outputCol="features")
  costK = pd.DataFrame()
  for k in range(2,10):
    [wssse,centers] = clusteringByK(assembler.transform(dataset), k, False)
    costK = costK.append({'ClusterSize': k, 'Error': wssse}, ignore_index=True)
  print costK
  return costK

### clustering by K
def clusteringByK(dataset, k, showCenter):
  kmeans = KMeans().setK(k).setSeed(1)
  # Trains a k-means model.
  model = kmeans.fit(dataset)
  # Evaluate clustering by computing Within Set Sum of Squared Errors.
  wssse = model.computeCost(dataset)
  print("Within Set Sum of Squared Errors = " + str(wssse))
  # Shows the result.
  if (showCenter):
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
  return [wssse, model.clusterCenters()]

### Plot
def plotClustering(cost):
  #plt.figure(figsize=(16, 28*4))
  #ax = plt.subplot()
  #ax.set_xlabel('Number of Clusters')
  #ax.set_ylabel('Within Set Sum of Squared Errors')
  #ax.set_title('K-Means Cluster Costs (to determine K)', loc='left')
  sb.lineplot(x="ClusterSize", y="Error",data=cost)
  #plt.show()
  
# log10 based clustering weatherDelay
dataset = spark.sql("select coalesce(log10(weatherDelay),0) as logWeatherDelay from flightDelay.flights_weather").sample(False, 0.1)
costK = testClustering(dataset)
plotClustering(costK)
### elbow is 4
assembler = VectorAssembler(
    inputCols=["logWeatherDelay"],
    outputCol="features")
[wssse, centers] = clusteringByK(assembler.transform(dataset), 4, True)
for center in centers:
        print(pow(10, center))
[wssse, centers] = clusteringByK(assembler.transform(dataset), 5, True)
for center in centers:
        print(pow(10, center))
# centroids are 1, 9, 30, 99 minutes
    
## #Show category
#delayDF = spark.sql("""select delayCat, count(*) from (
#                          select case when weatherDelay < 0 then '0.Early Arrival'
#                          when weatherDelay> 0 and weatherDelay <= 15 then '1.Less than 15 Minutes' 
#                          when weatherDelay> 15 and weatherDelay <= 60 then  '2.Between 15 Minutes and 1 Hour' 
#                          when weatherDelay> 60 and weatherDelay <= 120 then '3.Between 1 Hour and 2 Hours' 
#                          when weatherDelay > 120 and weatherDelay <= 240 then '4.Between 2 Hours and 4 Hours' 
#                          when weatherDelay > 240 then '5.Beyond 4 Hours' end as delayCat 
#                          from flightdelay.flights_weather 
#                          where weatherDelay > 0 and year >= 2003
#                     ) tmp
#                     group by delayCat order by delayCat""")
#delayDF.show()             
##delayDF.plot.bar(color='r',title='Frequency of Delay')

# # Box plot between categorial variables and Log10 of weather Delay
wdDetailsDF =spark.sql("""select 
                          case when OriginFog ='1' then 1 else 0 end as OFog,
                          case when OriginRain ='1' then 1 else 0 end as ORain,  
                          case when OriginSnow ='1' then 1 else 0 end as OSnow, 
                          case when OriginHail ='1' then 1 else 0 end as OHail, 
                          case when OriginThunder ='1' then 1 else 0 end as OThunder, 
                          case when OriginTornado ='1' then 1 else 0 end as OTornado,
                          case when DestFog ='1' then 1 else 0 end as DFog,
                          case when DestRain ='1' then 1 else 0 end as DRain,  
                          case when DestSnow ='1' then 1 else 0 end as DSnow, 
                          case when DestHail ='1' then 1 else 0 end as DHail, 
                          case when DestThunder ='1' then 1 else 0 end as DThunder, 
                          case when DestTornado ='1' then 1 else 0 end as DTornado,
                          log10(weatherDelay) as logWeatherDelay
                      from flightdelay.flights_weather""").sample(False, 0.1).toPandas()
wdDetailsDF.describe()
features = wdDetailsDF.iloc[:, 0:12].columns
plt.figure(figsize=(16, 28*4))
for i, col in enumerate(wdDetailsDF[features]):
  ax = plt.subplot()
  sb.boxplot(x= wdDetailsDF[col], y=wdDetailsDF["logWeatherDelay"])
  ax.set_ylabel('Log10 of Weather Delay in Minutes')
  ax.set_title('Boxplot of Weather by feature:' + str(col))
  plt.show()
# seaborn violinplot shows more information than boxplot. will try next time

# # Scatter plot Numeric variables and Weather Delay
# It shows some linear relationship between these two variables, but there are other factors
# The majority of weather delay are within 240 minutes ~ 4 hours
delayDF = spark.sql("""select log10(weatherDelay) as logWeatherDelay, OriginVisibility, OriginWindSpeed,OriginMaxWindSpeed,OriginPrecipitation,OriginSnowDepth,
                    DestVisibility, DestWindSpeed,DestMaxWindSpeed,DestPrecipitation,DestSnowDepth
                   from flightdelay.flights_weather where WeatherDelay >0""").sample(False, 0.01).toPandas()
features = delayDF.iloc[:,1:10].columns
plt.figure(figsize=(16,28*4))
for i, col in enumerate(delayDF[features]):
    ax = plt.subplot()
    sb.regplot(x=delayDF[col], y=delayDF["logWeatherDelay"], color='b')
    ax.set_ylabel('Log10 of Weather Delay in Minutes')
    ax.set_title('Scatterplot of Feature: ' + str(col))
    plt.show()
   

 ## Snow Delay and SnowDepth
snowDelayDF =spark.sql("select OriginVisibility, snowDelay from (select OriginSnowDepth, \
                          case when OriginSnow ='1' then weatherDelay else 0 end as snowDelay  \
                      from flightdelay.flights_weather ) where SnowDelay > 0 limit 100000").toPandas()
snowDelayDF.plot.scatter(x=0, y=1)

# ## Precipitation Delay and SnowDepth
rainDelayDF =spark.sql("select OriginPrecipitation, rainDelay from (select Precipitation, \
                          case when rain ='1' and fog='0' and snow='0' and hail='0' and thunder='0' then weatherDelay else 0 end as rainDelay  \
                      from flightdelay.flights_weather ) where rainDelay > 0 ").toPandas()
rainDelayDF.plot.scatter(x=0, y=1)

# Correlation between 
correlDF = spark.sql("""select year, month, count(*) as totalNumFlights, sum(delay), sum(wdelay),
            round(sum(OriginFogDelay)/sum(delay)*100) as pfogDelay, 
            round(sum(OriginSnowDelay)/sum(delay)*100) as psnowDelay, 
            round(sum(OriginHailDelay)/sum(delay)*100) as phailDelay, 
            round(sum(OriginThunderDelay)/sum(delay)*100) as pthunderDelay, 
            round(sum(OriginTornadoDelay)/sum(delay)*100) as ptornadoDelay
            from ( select year, month, day, FlightNum,  
                case when DepDelay > 0 and Cancelled = 0 then 1 else 0 end as delay, 
                case when WeatherDelay > 0 and Cancelled = 0 then 1 else 0 end as wdelay, 
                case when DepDelay > 0 and Cancelled = 0 and OriginFog='1' then 1 else 0 end as OriginFogDelay, 
                case when DepDelay > 0 and Cancelled = 0 and OriginSnow='1' then 1 else 0 end as OriginSnowDelay, 
                case when DepDelay > 0 and Cancelled = 0 and OriginHail='1' then 1 else 0 end as OriginHailDelay, 
                case when DepDelay > 0 and Cancelled = 0 and OriginThunder='1' then 1 else 0 end as OriginThunderDelay, 
                case when DepDelay > 0 and Cancelled = 0 and OriginTornado='1' then 1 else 0 end as OriginTornadoDelay 
                from flightdelay.flights_weather 
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
                from flightdelay.flights_weather 
            ) as b group by year, month 
          order by sum(wdelay) desc
""")
correlDF2.show()

# #######################################################################################################
# # clustering of weather delay
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
### Test Clustering 2-10
def testClustering(dataset):
  assembler = VectorAssembler(
    inputCols=["logWeatherDelay"],
    outputCol="features")
  costK = pd.DataFrame()
  for k in range(2,10):
    [wssse,centers] = clusteringByK(assembler.transform(dataset), k, False)
    costK = costK.append({'ClusterSize': k, 'Error': wssse}, ignore_index=True)
  print costK
  return costK

### clustering by K
def clusteringByK(dataset, k, showCenter):
  kmeans = KMeans().setK(k).setSeed(1)
  # Trains a k-means model.
  model = kmeans.fit(dataset)
  # Evaluate clustering by computing Within Set Sum of Squared Errors.
  wssse = model.computeCost(dataset)
  print("Within Set Sum of Squared Errors = " + str(wssse))
  # Shows the result.
  if (showCenter):
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
  return [wssse, model.clusterCenters()]

### Plot
def plotClustering(cost):
  #plt.figure(figsize=(16, 28*4))
  #ax = plt.subplot()
  #ax.set_xlabel('Number of Clusters')
  #ax.set_ylabel('Within Set Sum of Squared Errors')
  #ax.set_title('K-Means Cluster Costs (to determine K)', loc='left')
  sb.lineplot(x="ClusterSize", y="Error",data=cost)
  #plt.show()

# log10 based clustering weatherDelay
dataset = spark.sql("select coalesce(log10(weatherDelay),0) as logWeatherDelay from flightDelay.flights_weatherhourly").sample(False, 0.1)
costK = testClustering(dataset)
plotClustering(costK)
### elbow is 4
assembler = VectorAssembler(
    inputCols=["logWeatherDelay"],
    outputCol="features")
[wssse, centers] = clusteringByK(assembler.transform(dataset), 4, True)
for center in centers:
        print(pow(10, center))
[wssse, centers] = clusteringByK(assembler.transform(dataset), 5, True)
for center in centers:
        print(pow(10, center))

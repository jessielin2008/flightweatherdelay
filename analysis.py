from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb
import numpy as np

# ### verify pandas version
pd.__version__
sb.__version__

spark = SparkSession.builder.appName("FlightDelayVisualization") \
      .config("spark.executor.cores","2") \
      .config("spark.executor.memory", "8g") \
      .getOrCreate()

#  ********************************************************************************
# # WeatherDelay summary by year
weatherDF = spark.sql("""
select year, sum(weatherDelay), sum(NOweatherDelay), count(*) as total from (
select year,
case when weatherDelay > 0 then 1 else 0 end weatherDelay,
case when weatherDelay = 0 then 1 else 0 end NOweatherDelay
from flightdelay.flights_weatherhourly
) tmp group by year order by year
""").toPandas()
print(weatherDF)

# # Tune sample percentage to 1% = 260K flights
# table format. sample without replacement
delayDF = spark.sql("select depDelay, weatherDelay from flightdelay.flights_weatherhourly").sample(False, 0.01)
delayDF.count()

# ## Example distribution for Departure Delay, Weather Delay using Spark for heavy lifting
def histgram(df, column):
  hists = df.select(column).rdd.flatMap(
      lambda row: row
  ).histogram(30)
  data = {
      'bins': hists[0][:-1],
      'freq': hists[1]
  }
  plt.bar(data['bins'], data['freq'], width = 8)
# ## Most flights depart earlier  
histgram(delayDF, "depDelay")
# ## Most flights have no or very little weather delay
histgram(delayDF, "weatherDelay")

# ## flights with or without weather delay
weatherDelay = spark.sql("""select sum(NoweatherDelay), sum(WeatherDelay) from (
                      select case when WeatherDelay = 0 then 1 end as NoWeatherDelay
                           , case when weatherDelay > 0 then 1 end as WeatherDelay
                      from flightdelay.flights_weatherhourly) as a
                   """).toPandas()
print(weatherDelay)


# # Scatter plot Departure Delay and Weather Delay
# It shows some linear relationship between these two variables, but there are other factors
# The majority of weather delay are within 240 minutes ~ 4 hours
delayP = spark.sql("select coalesce(WeatherDelay,0) as weatherDelay, coalesce(DepDelay,0) as depDelay  \
                   from flightdelay.flights_weatherhourly where WeatherDelay >0").sample(False, 0.01).toPandas()
delayP.describe()
ax = delayP.plot.scatter(x=0, y=1)
sb.distplot(delayP["weatherDelay"])


# ## Example distribution for 10% of weatherDelay > 0 using Pandas
delayDF = spark.sql("""select log10(WeatherDelay) as logWeatherDelay
          from flightdelay.flights_weatherhourly where weatherDelay > 0""").toPandas() 
delayDF.count()
# ### log of Weather Delay looks like normal distribution
sb.distplot(delayDF["logWeatherDelay"])
df = delayDF.describe()
df["WeatherDelay"] = pow(10, df["logWeatherDelay"])
print(df)

# ## Mean 25 minutes
# ## 1 std (68%) 8-75 minutes 
# ## 2 std(95%) 2.5 - 243 minutes
# ## 3 Standard Deviation(99.7%) 0.8 - 770 minutes

# percentile 12 minutes, 25 mintues, 55 minutes
# # Show percentil
delayDF = spark.sql("""select delayCat, count(*) from (
                          select case when weatherDelay = 0 then '0.On Time Arrival'
                          when weatherDelay> 0 and weatherDelay <= 12 then '1.Less than 12 Minutes' 
                          when weatherDelay> 12 and weatherDelay <= 25 then  '2.Between 12 Minutes and 25 Minutes' 
                          when weatherDelay> 25 and weatherDelay <= 55 then '3.Between 25 Minutes and 55 Minutes' 
                          when weatherDelay > 55 then '5.Beyond 55 minutes' end as delayCat 
                          from flightdelay.flights_weatherhourly
                     ) tmp
                     group by delayCat order by delayCat""").toPandas()
print(delayDF)           
#delayDF.plot.bar(color='r',title='Frequency of Delay')


#######################################################################################################
# # Examine relationship between independent vairables and dependent variable
# # [Fed Aviation](https://www.faa.gov/nextgen/programs/weather/faq/#faq3) Shows variables to delay
# # Box plot between variables and weather Delay
# ## Winter 75% caused by low C&V, 25% by rain/thunderstorm, 
wdDetailsDF =spark.sql("""select case when weatherDelay > 0 then 1 else 0 end as weatherDelay,
                          MaxWindSpeed,
                          MinCeilingHeight,
                          MinVisibility,
                          MaxPressure,
                          MaxPrecipitation48
                          from flightdelay.flights_weatherhourly 
                          where ((month >=10 and month <12) or (month >=1 and month <=3))
                         and MaxWindSpeed < 9999
                         and MinCeilingHeight < 99999
                         and MinVisibility < 999999
                         and MaxPressure < 99999
                         and MinPressure < 99999
                         and MaxPrecipitation48 < 9999
                      """).sample(False, 0.01).toPandas()
wdDetailsDF.describe()
features = wdDetailsDF.iloc[:, 1:].columns
plt.figure(figsize=(16, 28*4))
for i, col in enumerate(wdDetailsDF[features]):
  ax = plt.subplot()
  sb.boxplot(x=wdDetailsDF["weatherDelay"], y=wdDetailsDF[col])
  ax.set_ylabel(col)
  ax.set_title("Boxplot of Weather by feature:" + str(col))
  plt.show()

# ## Summer 40% due to Rain/Thunderstorm, Low C&V conditions (~30%), airport winds (~20%)
wdDetailsDF =spark.sql("""select case when weatherDelay > 0 then 1 else 0 end as weatherDelay,
                          MaxWindSpeed,
                          MinCeilingHeight,
                          MinVisibility,
                          MaxPressure, 
                          MaxPrecipitation48
                          from flightdelay.flights_weatherhourly 
                          where (month >=4 and month <=9)  
                         and MaxWindSpeed < 9999
                         and MinCeilingHeight < 99999
                         and MinVisibility < 999999
                         and MaxPressure < 99999
                         and MaxPrecipitation48 < 9999
                      """).sample(False, 0.01).toPandas()
wdDetailsDF.describe()
features = wdDetailsDF.iloc[:, 1:].columns
plt.figure(figsize=(16, 28*4))
for i, col in enumerate(wdDetailsDF[features]):
  ax = plt.subplot()
  sb.boxplot(x=wdDetailsDF["weatherDelay"], y=wdDetailsDF[col])
  ax.set_ylabel(col)
  ax.set_title("Boxplot of Weather by feature:" + str(col))
  plt.show()
  
# seaborn violinplot shows more information than boxplot. will try next time

# # Scatter plot Numeric variables and Weather Delay
# It shows some linear relationship between these two variables, but there are other factors
# The majority of weather delay are within 240 minutes - 4 hours
delayDF = spark.sql("""select weatherDelay as WeatherDelay, 
                          MaxWindSpeed,
                          MinCeilingHeight,
                          MinVisibility,
                          MaxPressure, 
                          MaxPrecipitation48
                   from flightdelay.flights_weatherhourly
                   where weatherDelay > 0
                         and MaxWindSpeed < 9999
                         and MinCeilingHeight < 99999
                         and MinVisibility < 999999
                         and MaxPressure < 99999
                         and MaxPrecipitation48 < 9999
                    """).sample(False, 0.01).toPandas()
features = delayDF.iloc[:,1:].columns
plt.figure(figsize=(16,28*4))
for i, col in enumerate(delayDF[features]):
    ax = plt.subplot()
    sb.regplot(x=delayDF[col], y=delayDF["WeatherDelay"])
    ax.set_ylabel("Weather Delay in Minutes")
    ax.set_title("Scatterplot of Feature: " + str(col))
    plt.show()


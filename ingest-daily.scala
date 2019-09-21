import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//********************************************************************************
// Ingest worldwide airport station data in csv
//********************************************************************************
val stationColumnList= List( 
     StructField("USAF", IntegerType),
     StructField("WBAN", IntegerType),
     StructField("StationN", StringType),
     StructField("Country", StringType),
     StructField("State", StringType),
     StructField("ICAO", StringType),
     StructField("Latitude", DoubleType),
     StructField("Longtitude", DoubleType),
     StructField("Elevation", DoubleType),
     StructField("Begin", StringType),
     StructField("End", StringType))

val stationSchema=StructType(stationColumnList)
val stationDF=spark.read.option("header","true").
     schema(stationSchema).
     csv("flightdelay/weather/isd-history.csv")
//Total number of weather stations = 30046
stationDF.count
stationDF.show


//********************************************************************************
// Filter US airport with valid station number and airport code
//********************************************************************************
val airportDF = stationDF.where("Country = 'US'").
     where("USAF != '999999'").
     where("ICAO is not null").
     select($"USAF", ($"ICAO".substr(2,4)).alias("AirportCode")).distinct.
     groupBy("AirportCode").max("USAF")

//val airportDF1=airportDF.select($"AirportCode", $"max(USAF)".alias("StationNumber"))
// Airportcode count = 2485
airportDF.count
airportDF.show

//********************************************************************************
// ingest daily weather data - fixed length text file
//********************************************************************************
case class Weather(StationNumber: Int, 
                   WBAN: String, 
                   Year:Int, 
                   Month:Int,
                   Day:Int,
                   Temp:Float,
                   Visibility:Float,
                   WindSpeed:Float,
                   MaxWindSpeed:Float,
                   Precipitation:Float,
                   SnowDepth:Float,
                   Fog:Int,
                   Rain:Int, //Rain or Drizzle
                   Snow:Int, //Snow or Ice Pellets
                   Hail:Int,
                   Thunder:Int,
                   Tornado:Int //Tornado or Funnel Cloud
                  )

// Notice in map function, skipped columns not in interest
val weatherDF =spark.read.textFile("flightdelay/weather/gsod_*.txt").
  map(l => (l.substring(0, 6).trim(), 
             l.substring(7, 12).trim(), 
             l.substring(14,18).trim(), 
             l.substring(18,20).trim(),
             l.substring(20,22).trim(),
             l.substring(24,30).trim(),
             l.substring(68,73).trim(),
             l.substring(78,83).trim(),
             l.substring(88,93).trim(),
             l.substring(118,123).trim(),
             l.substring(125,130).trim(),
             l.substring(132,133).trim(),
             l.substring(133,134).trim(),
             l.substring(134,135).trim(),
             l.substring(135,136).trim(),
             l.substring(136,137).trim(),    
             l.substring(137,138).trim())).
  map({ case (st, wban, y, m, d, t, v, ws, mws, p, sd, f, r, s, h, th, to) => 
        Weather(st.toInt, wban, y.toInt, m.toInt, d.toInt,
                t.toFloat, v.toFloat, ws.toFloat, mws.toFloat,
                p.toFloat, sd.toFloat, f.toInt, r.toInt, s.toInt,h.toInt, th.toInt, to.toInt)}).
  where("StationNumber != '999999'").
  toDF
// number of weather data in the year = 324419
weatherDF.count
weatherDF.show

//weatherDF.write.saveAsTable("flightdelay.weather")
//Validate the year has all 12 months
weatherDF.groupBy("Year").count.show 

//********************************************************************************
// join DFs and get daily weather only for US airports
//********************************************************************************
val originWtherDF=weatherDF.join(airportDF, weatherDF("StationNumber")===airportDF("max(USAF)")).
  select("AirportCode", "Year", "Month","Day", 
         "Temp","Visibility","WindSpeed","MaxWindSpeed",
         "Precipitation","SnowDepth","Fog","Rain","Snow","Hail","Thunder","Tornado")
//number of airport weather data = 2744251
originWtherDF.count
originWtherDF.show
val destWtherDF = originWtherDF

//********************************************************************************
// Join with flight on origin airport and YMD
//********************************************************************************
val columnList= List( StructField("Year", IntegerType),
     StructField("Month", IntegerType),
     StructField("DayOfMonth", IntegerType),
     StructField("DayOfWeek", IntegerType),
     StructField("DepTime", IntegerType),
     StructField("CRSDepTime", IntegerType),
     StructField("ArrTime", IntegerType),
     StructField("CRSArrTime", IntegerType),
     StructField("UniqueCarrier", StringType),
     StructField("FlightNum", StringType),
     StructField("TailNum", StringType), 
     StructField("ActualElaspedTime", IntegerType),
     StructField("CRSElapsedTime", IntegerType),
     StructField("AirTime", IntegerType),
     StructField("ArrDelay", IntegerType),
     StructField("DepDelay", IntegerType),
     StructField("Origin", StringType),
     StructField("Dest", StringType),
     StructField("Distance", IntegerType),
     StructField("TaxiIn", IntegerType),
     StructField("TaxiOut", IntegerType),
     StructField("Cancelled", IntegerType),
     StructField("CancellationCode", StringType),
     StructField("Diverted", IntegerType),
     StructField("CarrierDelay", IntegerType),
     StructField("WeatherDelay", IntegerType),
     StructField("NASDelay", IntegerType),
     StructField("SecurityDelay", IntegerType),
     StructField("LateAircraftDelay", IntegerType))

val flightSchema=StructType(columnList)

val flightDF=spark.read.option("header","true").
     option("nullValue","NA").option("nanValue","NA").
     option("quote", null).option("mode","DROPMALFORMED").
     schema(flightSchema).
     csv("flightdelay/flights/*.csv")
//US has 6-7 million flights a year
flightDF.count 
flightDF.groupBy("Year").count.show
//flightDF.write.mode("overwrite").saveAsTable("flightdelay.flights")

val flight1DF = flightDF.join(originWtherDF, flightDF("Origin")===originWtherDF("AirportCode") and 
                            flightDF("Year")===originWtherDF("Year") and
                            flightDF("Month")===originWtherDF("Month") and
                            flightDF("DayOfMonth")===originWtherDF("Day")).
    select(flightDF("Year"), flightDF("Month"), flightDF("DayOfMonth").alias("Day"),
          flightDF("DayOfWeek"), flightDF("Origin"), flightDF("Dest"),flightDF("DepTime"), flightDF("CRSDepTime")
          , flightDF("ArrTime"), flightDF("CRSArrTime"), flightDF("UniqueCarrier")
          , flightDF("FlightNum"), flightDF("DepDelay"), flightDF("Origin")
          , flightDF("TaxiOut"), flightDF("Cancelled"), flightDF("CancellationCode")
          , flightDF("CarrierDelay"), flightDF("WeatherDelay"),flightDF("NASDelay"), flightDF("SecurityDelay")
          , flightDF("LateAircraftDelay"),originWtherDF("Temp").alias("OriginTemp"),originWtherDF("Visibility").alias("OriginVisibility"),originWtherDF("WindSpeed").alias("OriginWindSpeed")
          ,originWtherDF("MaxWindSpeed").alias("OriginMaxWindSpeed"),originWtherDF("Precipitation").alias("OriginPrecipitation")
          ,originWtherDF("SnowDepth").alias("OriginSnowDepth"),originWtherDF("Fog").alias("OriginFog"),originWtherDF("Rain").alias("OriginRain")
          ,originWtherDF("Snow").alias("OriginSnow"),originWtherDF("Hail").alias("OriginHail"),originWtherDF("Thunder").alias("OriginThunder"),originWtherDF("Tornado").alias("OriginTornado"))

val resultDF = flight1DF.join(destWtherDF,flight1DF("Dest")===destWtherDF("AirportCode") and 
                            flight1DF("Year")===destWtherDF("Year") and
                            flight1DF("Month")===destWtherDF("Month") and
                            flight1DF("Day")===destWtherDF("Day")).
    select(flight1DF("Year"), flight1DF("Month"), flight1DF("Day"),
          flight1DF("DayOfWeek"), flight1DF("DepTime"), flight1DF("CRSDepTime")
          , flight1DF("ArrTime"), flight1DF("CRSArrTime"), flight1DF("UniqueCarrier")
          , flight1DF("FlightNum"), flight1DF("DepDelay"), flight1DF("Origin"), flight1DF("Dest")
          , flight1DF("TaxiOut"), flight1DF("Cancelled"), flight1DF("CancellationCode")
          , flight1DF("CarrierDelay"), flight1DF("WeatherDelay"),flight1DF("NASDelay"), flight1DF("SecurityDelay")
          , flight1DF("LateAircraftDelay"),flight1DF("OriginTemp"),flight1DF("OriginVisibility"),flight1DF("OriginWindSpeed")
          ,flight1DF("OriginMaxWindSpeed"),flight1DF("OriginPrecipitation")
          ,flight1DF("OriginSnowDepth"),flight1DF("OriginFog"),flight1DF("OriginRain")
          ,flight1DF("OriginSnow"),flight1DF("OriginHail"),flight1DF("OriginThunder"),flight1DF("OriginTornado")
          ,destWtherDF("Temp").alias("DestTemp"),destWtherDF("Visibility").alias("DestVisibility"),destWtherDF("WindSpeed").alias("DestWindSpeed")
          ,destWtherDF("MaxWindSpeed").alias("DestMaxWindSpeed"),destWtherDF("Precipitation").alias("DestPrecipitation")
          ,destWtherDF("SnowDepth").alias("DestSnowDepth"),destWtherDF("Fog").alias("DestFog"),destWtherDF("Rain").alias("DestRain")
          ,destWtherDF("Snow").alias("DestSnow"),destWtherDF("Hail").alias("DestHail"),destWtherDF("Thunder").alias("DestThunder"),destWtherDF("Tornado").alias("DestTornado"))

//flights with origin airport weather - 26MM for 2004-2007
resultDF.count 
resultDF.printSchema

//********************************************************************************
// Save dataframe to parquet file for later analytics
//********************************************************************************

val spark = SparkSession.builder.getOrCreate()
spark.sql("create database if not exists flightdelay")
resultDF.write.mode("overwrite").partitionBy("Year","Month").saveAsTable("flightdelay.flights_dailyweather")
resultDF.groupBy("Year","Month").count.sort($"count".desc).show

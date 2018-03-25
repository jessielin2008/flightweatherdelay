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
     csv("weather/isd-history.csv")
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
                   Fog:String,
                   Rain:String, //Rain or Drizzle
                   Snow:String, //Snow or Ice Pellets
                   Hail:String,
                   Thunder:String,
                   Tornado:String //Tornado or Funnel Cloud
                  )

// Notice in map function, skipped columns not in interest
val weatherDF =spark.read.textFile("weather/gsod_1988.txt").
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
                p.toFloat, sd.toFloat, f, r, s,h, th, to)}).
  where("StationNumber != '999999'").
  toDF
// number of weather data in the year = 324419
weatherDF.count
weatherDF.show

//weatherDF.write.saveAsTable("flights.weather")
//Validate the year has all 12 months
weatherDF.groupBy("Month").count.show 

//********************************************************************************
// join DFs and gether daily weather only for US airports
//********************************************************************************
val aptWtherDF=weatherDF.join(airportDF, weatherDF("StationNumber")===airportDF("max(USAF)")).
  select("AirportCode", "Year", "Month","Day", 
         "Temp","Visibility","WindSpeed","MaxWindSpeed",
         "Precipitation","SnowDepth","Fog","Rain","Snow","Hail","Thunder","Tornado")
//number of airport weather data = 324419
aptWtherDF.count
aptWtherDF.show

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
     StructField("TailNum", IntegerType), 
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
     csv("flights/1988.csv")
//US has 5 million flights a year : 1988 - 5,202,096
flightDF.count 
flightDF.show
flightDF.write.mode("overwrite").saveAsTable("flights.flights")

//********************************************************************************
// Save dataframe to parquet file for later analytics
//********************************************************************************
val resultDF = flightDF.join(aptWtherDF, flightDF("Origin")===aptWtherDF("AirportCode") and 
                            flightDF("Year")===aptWtherDF("Year") and
                            flightDF("Month")===aptWtherDF("Month") and
                            flightDF("DayOfMonth")===aptWtherDF("Day")).
    select(flightDF("Year"), flightDF("Month"), aptWtherDF("Day"),
          flightDF("DayOfWeek"), flightDF("DepTime"), flightDF("CRSDepTime")
          , flightDF("ArrTime"), flightDF("CRSArrTime"), flightDF("UniqueCarrier")
          , flightDF("FlightNum"), flightDF("DepDelay"), flightDF("Origin")
          , flightDF("TaxiOut"), flightDF("Cancelled"), flightDF("CancellationCode")
          , flightDF("CarrierDelay"), flightDF("WeatherDelay"),flightDF("NASDelay"), flightDF("SecurityDelay")
          , flightDF("LateAircraftDelay"),aptWtherDF("Temp"),aptWtherDF("Visibility"),aptWtherDF("WindSpeed")
          ,aptWtherDF("MaxWindSpeed"),aptWtherDF("Precipitation"),aptWtherDF("SnowDepth"),aptWtherDF("Fog"),aptWtherDF("Rain")
          ,aptWtherDF("Snow"),aptWtherDF("Hail"),aptWtherDF("Thunder"),aptWtherDF("Tornado"))
//flights with origin airport weather - 4,119,538 for 1988
resultDF.count 
resultDF.printSchema
resultDF.write.mode("overwrite").partitionBy("Year","Month").saveAsTable("flights.flights_originweather")


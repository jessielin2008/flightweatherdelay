import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
//********************************************************************************
// Ingest worldwide weather station data in csv
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
// Filter weather station with with valid number and US airport code
//********************************************************************************
val airportDF = stationDF.where("Country = 'US'").
     where("USAF != '999999'").
     where("ICAO is not null").
     select($"USAF", ($"ICAO".substr(2,4)).alias("AirportCode")).distinct.
     groupBy("AirportCode").max("USAF")

//val airportDF1=airportDF.select($"AirportCode", $"max(USAF)".alias("StationNumber"))
// Airportcode count = 2461
airportDF.count
airportDF.show

//********************************************************************************
// ingest hourly weather data - fixed length text file
//********************************************************************************
case class Weather(StationNumber: Int, 
                   WBAN: String, 
                   Year:Int, 
                   Month:Int,
                   Day:Int,
                   Hour:Int,
                   WindSpeed:Int,
                   CeilingHeight:Int,
                   Visibility:Int,
                   Temp:Int,
                   Pressure:Int,
                   PrecipitationHour:Int,
                   Precipitation:Int,
                   Precipitation48:Float,
                   PresentWeatherCode:String
                   //AA1 precipitation hourly, AJ1 snow depth-4,AN1 snow accumulation 3-4, AU1 Present Weather Code 2
              )

def parseISD(l:String): Weather = {
    var precipitationHour = 0
    var precipitation = 0
    var presentWeatherCode = "0"
    var precipitation48:Float = 0.0f
    if(l.indexOf("AA1") != -1){
        //Additional Info sectuib starts with "AA1"
        val additionalInfo: String =l.substring(l.indexOf("AA1") + 3 ).trim()
        try {
            precipitation = additionalInfo.substring(2,6).trim().toInt
        }catch {
              case ex: NumberFormatException => {
                precipitation = 9999
              }
        }
        try {
            precipitationHour = additionalInfo.substring(0,2).trim().toInt
            if (precipitationHour == 99 || precipitation == 9999) {
              precipitation48 = 9999.0f
            } else if (precipitationHour == 0 && precipitation == 0 ){
              precipitation48 = 0.0f          
            } else if (precipitationHour == 0 && precipitation != 0 ){
              //treat it as hourly
              precipitation48 = precipitation* 48f * 0.0393700787f
            } else {  
              precipitation48 = precipitation/precipitationHour * 48f * 0.0393700787f
            }
        }catch {
              case ex: NumberFormatException => {
                precipitationHour = 99
                precipitation48 = 9999
              }
        }
    }
    if(l.indexOf("AT1") > 0) {
      val additionalInfo: String =l.substring(l.indexOf("AT1") + 3).trim()
      if (additionalInfo.length() >=4 ) {
        presentWeatherCode = additionalInfo.substring(4)
        if(additionalInfo.length() >= 8) {
          presentWeatherCode = additionalInfo.substring(4,8)
        }
      }
    }
    return Weather(
          l.substring(4, 10).trim().toInt, 
          l.substring(10, 15).trim(),
          l.substring(15,19).trim().toInt, 
          l.substring(19,21).trim().toInt,
          l.substring(21,23).trim().toInt,
          l.substring(23,25).trim().toInt,
          l.substring(65,69).trim().toInt,
          l.substring(70,75).trim().toInt,
          l.substring(78,84).trim().toInt,
          l.substring(87,92).trim().toInt,
          l.substring(99,104).trim().toInt, 
          precipitationHour,
          precipitation,
          precipitation48,
          presentWeatherCode
        )
}

// Notice in map function, skipped columns not in interest
val isdDF =spark.read.textFile("flightdelay/weatherhourly/isd-*.txt").
  map(l => parseISD(l)).
  filter("WBAN <> '99999'").
  toDF
isdDF.registerTempTable("isd_weather")
//isdDF.write.mode("overwrite").saveAsTable("flightdelay.isd_weather")

// dedupe WBAN, and replace missing value with null
val weatherDF = spark.sql("""
  select StationNumber,WBAN, year, month, day, hour, 
      case when MaxWindSpeed = -1 then 9999 else MaxWindSpeed end as MaxWindSpeed,
      MinWindSpeed,
      DistinctWindSpeed,
      case when MaxCeilingHeight = -1 then 99999 else MaxCeilingHeight end as MaxCeilingHeight,
      MinCeilingHeight,
      DistinctCeilingHeight,
      case when MaxVisibility = -1 then 999999 else MaxVisibility end as MaxVisibility,
      MinVisibility,
      DistinctVisibility,
      case when MaxTemp = -9999 then 9999 else MaxTemp end as MaxTemp,
      MinTemp,
      DistinctTemp,
      case when MaxPressure = -1 then 99999 else MaxPressure end as MaxPressure,
      MinPressure,
      DistinctPressure,
      case when MaxPrecipitation48 = -1 then 9999 else MaxPrecipitation48 end as MaxPrecipitation48,
      MinPrecipitation48,
      DistinctPrecipitation48,
      MaxPresentWeatherCode,
      NumOfRec from (
        select StationNumber,WBAN, year, month, day, hour, 
        max(case when WindSpeed = 9999 then -1 else WindSpeed end) as MaxWindSpeed,
        min(WindSpeed) as MinWindSpeed,
        count(distinct WindSpeed) as DistinctWindSpeed,
        max(case when CeilingHeight = 99999 then -1 else CeilingHeight end) as MaxCeilingHeight,
        min(CeilingHeight) as MinCeilingHeight,
        count(distinct CeilingHeight) as DistinctCeilingHeight,
        max(case when visibility = 999999 then -1 else visibility end) as MaxVisibility,
        min(visibility) as MinVisibility,
        count(distinct visibility) as DistinctVisibility,
        max(case when temp = 9999 then -9999 else temp end) as MaxTemp,
        min(temp) as MinTemp,
        count(distinct temp) as DistinctTemp,
        max(case when pressure = 99999 then -1 else pressure end) as MaxPressure,
        min(pressure) as MinPressure,
        count(distinct pressure) as DistinctPressure,
        max(case when precipitation48 = 9999 then -1 else precipitation48 end) as MaxPrecipitation48,
        min(precipitation48) as MinPrecipitation48,
        count(distinct Precipitation48) as DistinctPrecipitation48,
        max(presentWeatherCode) as MaxPresentWeatherCode,
        count(*) as NumOfRec
        from flightdelay.isd_weather
        group by StationNumber,WBAN, year, month, day,hour
      ) as tmp
    """)
//weatherDF.registerTempTable("weather")
weatherDF.printSchema
// number of weather data in the year after dedupe ~ 5MM 5579396
//weatherDF.count
//weatherDF.show
//Validate the year has all 12 months  5MM for 2004
//weatherDF.groupBy("Year").count.show 
//weatherDF.write.saveAsTable("flightdelay.weather")

//********************************************************************************
// join DFs and get weather only for US airports
//********************************************************************************
val originWtherDF=weatherDF.join(airportDF, weatherDF("StationNumber")===airportDF("max(USAF)")).
  select("AirportCode", "Year", "Month","Day", "Hour"
         ,"MaxWindSpeed","MinWindSpeed","DistinctWindSpeed"
         ,"MaxCeilingHeight","MinCeilingHeight","DistinctCeilingHeight"
         ,"MaxVisibility", "MinVisibility","DistinctVisibility"
         ,"MaxTemp","MinTemp","DistinctTemp"
         ,"MaxPressure","MinPressure","DistinctPressure"
         ,"MaxPrecipitation48","MinPrecipitation48","DistinctPrecipitation48"
         , "MaxPresentWeatherCode","NumOfRec"
        )
//number of airport weather data = 4.8MM for 2004
originWtherDF.groupBy("Year", "Month").count.show
//originWtherDF.show
//val destWtherDF = originWtherDF
//originWtherDF.write.mode("overwrite").saveAsTable("flightdelay.weatherhourly")

//********************************************************************************
// Read flight on origin airport and YMD
//********************************************************************************
val columnList= List( StructField("Year", IntegerType, nullable = false),
     StructField("Month", IntegerType, nullable = false),
     StructField("DayOfMonth", IntegerType, nullable = false),
     StructField("DayOfWeek", IntegerType),
     StructField("DepTime", IntegerType),
     StructField("CRSDepTime", IntegerType, nullable = false),
     StructField("ArrTime", IntegerType),
     StructField("CRSArrTime", IntegerType),
     StructField("UniqueCarrier", StringType),
     StructField("FlightNum", StringType, nullable = false),
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

val flDF=spark.read.option("header","true").
     option("nullValue","NA").option("nanValue","NA").
     option("quote", null).option("mode","DROPMALFORMED").
     schema(flightSchema).
     csv("flightdelay/flights/*.csv")
//US has over 7 million flights a year 2004-2007
//flDF.groupBy("Year").count.show
val CRSDepHour: (Column) => Column = (x) => { (x/100).cast(IntegerType) } 
val flightDF = flDF.withColumn("CRSDepHour", CRSDepHour(col("CRSDepTime")))
flightDF.printSchema()
//flightDF.head
//flightDF.write.mode("overwrite").saveAsTable("flightdelay.flights")

val flight1DF = flightDF.join(originWtherDF, flightDF("Origin")===originWtherDF("AirportCode") and 
                            flightDF("Year")===originWtherDF("Year") and
                            flightDF("Month")===originWtherDF("Month") and
                            flightDF("DayOfMonth")===originWtherDF("Day") and
                            flightDF("CRSDepHour")===originWtherDF("Hour"), "inner").
    select(flightDF("Year"), flightDF("Month"), flightDF("DayOfMonth").alias("Day"),flightDF("CRSDepHour")
          , flightDF("DayOfWeek"), flightDF("Origin"), flightDF("Dest"),flightDF("DepTime"), flightDF("CRSDepTime")
          , flightDF("ArrTime"), flightDF("CRSArrTime"), flightDF("UniqueCarrier")
          , flightDF("FlightNum"), flightDF("DepDelay")
          , flightDF("TaxiOut"), flightDF("Cancelled"), flightDF("CancellationCode")
          , flightDF("CarrierDelay"), flightDF("WeatherDelay"),flightDF("NASDelay"), flightDF("SecurityDelay")
          , flightDF("LateAircraftDelay")
          , originWtherDF("MaxWindSpeed"), originWtherDF("MinWindSpeed"), originWtherDF("DistinctWindSpeed")
           ,originWtherDF("MaxCeilingHeight"),originWtherDF("MinCeilingHeight"),originWtherDF("DistinctCeilingHeight")
           ,originWtherDF("MaxVisibility"), originWtherDF("MinVisibility"),originWtherDF("DistinctVisibility")
           ,originWtherDF("MaxTemp"),originWtherDF("MinTemp"),originWtherDF("DistinctTemp")
           ,originWtherDF("MaxPressure"),originWtherDF("MinPressure"),originWtherDF("DistinctPressure")
           ,originWtherDF("MaxPrecipitation48"),originWtherDF("MinPrecipitation48"),originWtherDF("DistinctPrecipitation48")
           ,originWtherDF("MaxPresentWeatherCode"), originWtherDF("NumOfRec"))
//flight1DF.groupBy("Year").count.show


//********************************************************************************
// Save dataframe to parquet file for later analytics
//********************************************************************************

spark.sql("create database if not exists flightdelay")
flight1DF.write.mode("overwrite").partitionBy("Year","Month").saveAsTable("flightdelay.flights_weatherhourly")
flight1DF.groupBy("Year","Month").count.sort($"count".desc).show

//spark.sql("drop table if exists flightdelay.isd_weather")
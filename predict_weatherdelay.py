from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

spark = SparkSession.builder \
      .appName("FlightDelayMachineLearning") \
      .master("local[*]") \
      .getOrCreate()

model1 = PipelineModel.load("flightdelay/lrwModel") 
model2 = PipelineModel.load("flightdelay/rfrModel") 

features = ["Month","Day", "DayOfWeek", "CRSDepHour","UniqueCarrier","Origin", "Dest"
            ,"MaxWindSpeed","MinCeilingHeight","MinVisibility","MaxPressure"
            ,"MaxPrecipitation48","MaxPresentWeatherCode"]

def predict(args):
  flight=args["feature"].split(",")
  if (len(flight) == 7):
    #fill in missing weather
    flight = flight + [9999,99999,999999,99999,9999,0]
    
  feature = spark.createDataFrame([map(float,flight[:4]) + flight[4:7] 
                                  + map(float,flight[7:12]) + [flight[12]]], features)
  result=model1.transform(feature).collect()[0].prediction 

  if (result == 1):
    #predict how long if there will be a weather delay
    result=pow(10, model2.transform(feature).collect()[0].prediction)
  return {"result" : result}

predict(
  {"feature": "9,1,6,6,AA,SFO,JFK"}
)

predict(
  {"feature": "9,1,6,6,AA,SFO,JFK,10,1000,1200,10250,0,FG"}
)

predict(
  {"feature": "12,24,1,22,UA,JFK,SFO"}
) 

predict(
  {"feature": "12,24,1,21,UA,JFK,SFO,70,500,400,10100,30,RA"}
)
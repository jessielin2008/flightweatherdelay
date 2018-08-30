
# Imports
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import (StandardScaler, StringIndexer, VectorAssembler, IndexToString, OneHotEncoderEstimator)
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import cdsw

spark = SparkSession.builder.appName("FlightDelayML_Experiment") \
      .config("spark.executor.memory", "32g") \
      .config("spark.executor.cores", "2") \
      .config("spark.yarn.executor.memoryOverhead", "1g") \
      .getOrCreate()
print(spark.version)

param_numTrees=int(sys.argv[1])
param_maxDepth=int(sys.argv[2])
#param_numTrees=10
#param_maxDepth=10

#####################################################################################
## Random Forest Classifier
# # Deal with imbalanced dataset
#df = spark.sql("""select Year, Month, Day,DayOfWeek,CRSDepHour,UniqueCarrier,Origin,Dest
#               , case when MaxWindSpeed == null then 9999 else MaxWindSpeed end  as MaxWindSpeed
#               , case when MinCeilingHeight == null then 99999 else MinCeilingHeight end as MinCeilingHeight
#               , case when MinVisibility == null then 999999 else MinVisibility end as MinVisibility
#               , case when MaxPressure == null then 99999 else MaxPressure end as MaxPressure
#               , case when MaxPrecipitation48 == null then 9999 else MaxPrecipitation48 end as MaxPrecipitation48
#               , MaxPresentWeatherCode
#               , case when weatherDelay> 0 and weatherDelay <= 15 then '1.Less than 15 Minutes' 
#                    when weatherDelay> 15 and weatherDelay <= 60 then  '2.Between 15 Minutes and 1 Hour' 
#                    when weatherDelay> 60 and weatherDelay <= 120 then '3.Between 1 Hour and 2 Hours' 
#                    when weatherDelay > 120 and weatherDelay <= 240 then '4.Between 2 Hours and 4 Hours' 
#                    when weatherDelay > 240 then '5.Beyond 4 Hours' 
#               end as delayCat 
#               from flightdelay.flights_weatherhourly
#               where weatherDelay > 0
#                """)
df = spark.sql("""select Year, Month, Day,DayOfWeek,CRSDepHour,UniqueCarrier,Origin,Dest
               , case when MaxWindSpeed == null then 9999 else MaxWindSpeed end  as MaxWindSpeed
               , case when MinCeilingHeight == null then 99999 else MinCeilingHeight end as MinCeilingHeight
               , case when MinVisibility == null then 999999 else MinVisibility end as MinVisibility
               , case when MaxPressure == null then 99999 else MaxPressure end as MaxPressure
               , case when MaxPrecipitation48 == null then 9999 else MaxPrecipitation48 end as MaxPrecipitation48
               , MaxPresentWeatherCode
               , case when weatherDelay> 0 and weatherDelay <= 12 then '1.Less than 12 Minutes' 
                    when weatherDelay> 12 and weatherDelay <= 25 then  '2.Between 12 Minutes and 25 Minutes' 
                    when weatherDelay> 25 and weatherDelay <= 55 then '3.Between 25 Minutes and 55 Minutes' 
                    else '5.Beyond 55 minutes' 
               end as delayCat 
               from flightdelay.flights_weatherhourly
               where weatherDelay > 0
               """)
df.count()


# With Hourly weather Classification 36%
ucInd = StringIndexer(inputCol="UniqueCarrier", outputCol="UniqueCarrierInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct UniqueCarrier from flightdelay.flights_weatherhourly"))
oInd = StringIndexer(inputCol="Origin", outputCol="OriginInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct Origin from flightdelay.flights_weatherhourly"))
dInd = StringIndexer(inputCol="Dest", outputCol="DestInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct Dest from flightdelay.flights_weatherhourly"))
wInd = StringIndexer(inputCol="MaxPresentWeatherCode", outputCol="WeatherCodeInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct MaxPresentWeatherCode from flightdelay.flights_weatherhourly"))
labelInd = StringIndexer(inputCol="delayCat", outputCol="delayCatInd").fit(df)
encoder = OneHotEncoderEstimator(inputCols=["UniqueCarrierInd", "OriginInd", "DestInd","WeatherCodeInd"],
                                 outputCols=["UniqueCarrierOHE", "OriginOHE","DestOHE","WeatherCodeOHE"])
assembler = VectorAssembler(
    inputCols=["Month","Day","DayOfWeek","CRSDepHour","UniqueCarrierOHE","OriginOHE","DestOHE"
              ,"MaxWindSpeed","MinCeilingHeight","MinVisibility","MaxPressure","MaxPrecipitation48"
              ,"WeatherCodeOHE"
              ],
    outputCol="features")
classifier = RandomForestClassifier(labelCol = 'delayCatInd', featuresCol = 'features',
                                    numTrees= param_numTrees, maxDepth =param_maxDepth, maxBins=500, predictionCol="prediction")
labelConv = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelInd.labels)

pipeline = Pipeline(stages=[ucInd, oInd, dInd,wInd,labelInd,encoder, assembler,classifier, labelConv])
(train, test) = df.randomSplit([0.7, 0.3])
model = pipeline.fit(train)
predictions = model.transform(test)
predictions.head()

# ## Model Evalution
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="delayCatInd", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages[-1]
print(rfModel)  # summary only

cdsw.track_metric("accuracy", accuracy)

model.write().overwrite().save("flightdelay/rfcModel")

!rm -r -f /home/cdsw/models/spark/rfcModel
!rm -r -f /home/cdsw/models/spark_rfc.tar
!hdfs dfs -get models/spark/rfcModel /home/cdsw/models/spark/rfcModel
!tar -cvf spark_rfc.tar /home/cdsw/models/spark

cdsw.track_file("/home/cdsw/models/spark_rfc.tar")

spark.stop()
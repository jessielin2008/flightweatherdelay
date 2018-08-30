
# Imports
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import (StandardScaler, StringIndexer, VectorAssembler, IndexToString, OneHotEncoderEstimator)
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row

spark = SparkSession.builder.appName("FlightDelayMachineLearning") \
      .config("spark.executor.memory", "58g") \
      .config("spark.executor.cores", "2") \
      .config("spark.yarn.executor.memoryOverhead", "1g") \
      .getOrCreate()

print(spark.version)
#####################################################################################
# # Prep for LinearRegression 
df = spark.sql("""select Year, Month, Day,DayOfWeek,CRSDepHour,UniqueCarrier,Origin,Dest
               , case when MaxWindSpeed == null then 9999 else MaxWindSpeed end  as MaxWindSpeed
               , case when MinCeilingHeight == null then 99999 else MinCeilingHeight end as MinCeilingHeight
               , case when MinVisibility == null then 999999 else MinVisibility end as MinVisibility
               , case when MaxPressure == null then 99999 else MaxPressure end as MaxPressure
               , case when MaxPrecipitation48 == null then 9999 else MaxPrecipitation48 end as MaxPrecipitation48
               , MaxPresentWeatherCode
               , Log10(WeatherDelay) as LogWeatherDelay
               , WeatherDelay
               from flightdelay.flights_weatherhourly
               where weatherDelay > 0
               and MaxPresentWeatherCode is not null
               """)
df.count()
df.head()

# ## transform features
ucInd = StringIndexer(inputCol="UniqueCarrier", outputCol="UniqueCarrierInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct UniqueCarrier from flightdelay.flights_weatherhourly"))
oInd = StringIndexer(inputCol="Origin", outputCol="OriginInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct Origin from flightdelay.flights_weatherhourly"))
dInd = StringIndexer(inputCol="Dest", outputCol="DestInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct Dest from flightdelay.flights_weatherhourly"))
wInd = StringIndexer(inputCol="MaxPresentWeatherCode", outputCol="WeatherCodeInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct MaxPresentWeatherCode from flightdelay.flights_weatherhourly"))
encoder = OneHotEncoderEstimator(inputCols=["UniqueCarrierInd", "OriginInd", "DestInd","WeatherCodeInd"],
                                 outputCols=["UniqueCarrierOHE", "OriginOHE","DestOHE","WeatherCodeOHE"])
assembler = VectorAssembler(
    inputCols=["Month","Day", "DayOfWeek", "CRSDepHour","UniqueCarrierOHE","OriginOHE", "DestOHE"
               , "MaxWindSpeed","MinCeilingHeight", "MinVisibility","MaxPressure", "MaxPrecipitation48"
               , "WeatherCodeOHE"],
    outputCol="features")
(training, test) = df.randomSplit([0.8,0.2], seed = 10)
#####################################################################################
# # Base Line Linear Regression 

# ## feed transformed features into linear regression
lr = LinearRegression(featuresCol="features", labelCol="LogWeatherDelay")
pipeline = Pipeline(stages=[ucInd,oInd,dInd,wInd,encoder,assembler, lr])
lrModel = pipeline.fit(training)

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.stages[-1].summary
#print("numIterations: %d" % trainingSummary.totalIterations)
#print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
#trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

predictions = lrModel.transform(test)
predictions.select("LogWeatherDelay","prediction").show()

evaluator = RegressionEvaluator(
    labelCol="WeatherDelay", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# get Weather Delay from logWeatherDelay
predictions.select("prediction", "WeatherDelay").registerTempTable("predictionLR")
predictionsLR = spark.sql("select pow(10, prediction) as prediction, \
                          WeatherDelay from predictionLR")
predictionsLR.select("WeatherDelay","prediction").show()
evaluator = RegressionEvaluator(
    labelCol="WeatherDelay", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictionsLR)

print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
lrModel.write().overwrite().save("flightdelay/lrModel")

#####################################################################################
# # Random Forest Regression
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.sql.functions import *
# Train a RandomForest model.
rfr = RandomForestRegressor(featuresCol="features", labelCol="LogWeatherDelay")

pipeline = Pipeline(stages=[ucInd,oInd,dInd,wInd,encoder,assembler,rfr])
rfrModel = pipeline.fit(training)

# Make predictions.
predictions = rfrModel.transform(test)

# Select (prediction, true label) and compute test error
predictions.select("prediction", "WeatherDelay").registerTempTable("predictionRFR")
predictionsRFR = spark.sql("select pow(10, prediction) as prediction, \
                           WeatherDelay from predictionRFR")
evaluator = RegressionEvaluator(
    labelCol="WeatherDelay", predictionCol="prediction", metricName="rmse")
predictionsRFR.select("prediction", "WeatherDelay").show(5)
rmse = evaluator.evaluate(predictionsRFR)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

#metrics = RegressionMetrics(labelCol="WeatherDelay", predictionCol="prediction")
## Squared Error
#print("MSE = %s" % metrics.meanSquaredError)
#print("RMSE = %s" % metrics.rootMeanSquaredError)
#
## R-squared
#print("R-squared = %s" % metrics.r2)

rfModel = rfrModel.stages[-1]
print(rfModel)  # summary only

##############################################
rfrModel.write().overwrite().save("flightdelay/rfrModel")

!rm -r -f models/spark/rfrModel
!rm -r -f models/spark_rfr.tar
!hdfs dfs -get flightdelay/rfrModel models/spark/rfrModel
!tar -cvf models/spark_rfr.tar models/spark



# Imports
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import (StandardScaler, StringIndexer, VectorAssembler)
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row

spark = SparkSession.builder.appName("FlightDelayMachineLearning").getOrCreate()
print(spark.version)
# Base Line Linear Regression
df = spark.sql("select Day, DayOfWeek, DepTime,UniqueCarrier,FlightNum,Origin, \
               Temp,Visibility,WindSpeed,MaxWindSpeed,Precipitation,SnowDepth, \
               Fog,Rain,Snow,Hail, Thunder,Tornado,Year,Month from flightdelay.flights_OriginWeather where cancelled = 0 limit 10000")
df.head()

# ## transform features
# #indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(df) for column in list(set(df.columns)-set(['date'])) ]

ucInd = StringIndexer(inputCol="UniqueCarrier", 
                      outputCol="UniqueCarrierInd")
flInd = StringIndexer(inputCol="FlightNum", 
                      outputCol="FlightNumInd")
oInd = StringIndexer(inputCol="Origin", 
                      outputCol="OriginInd")

assembler = VectorAssembler(
    inputCols=["Day", "DayOfWeek", "DepTime","UniqueCarrierInd","FlightNumInd","OriginInd",
               "Temp","Visibility","WindSpeed","MaxWindSpeed","Precipitation","SnowDepth",
               "Fog","Rain","Snow","Hail", "Thunder","Tornado","Year","Month"],
    outputCol="features")
featureTransformer = Pipeline(stages=[ucInd,flInd,oInd,assembler])
dataModel = featureTransformer.fit(df)
transformedDF = dataModel.transform(df)
# Need to cache it otherwise StringIndexer throws Label Unseen error on random labels
transformedDF.cache()

# ## feed transformed features into linear regression
lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, 
                      featuresCol="features", labelCol="DepDelay")
(training, test) = transformedDF.randomSplit([0.8,0.2], seed = 10)
lrModel = lr.fit(training)
prediction = lrModel.evaluate(test)

# Print the coefficients and intercept for linear regression
results = lrModel.evaluate(test)
results.predictions.select("DepDelay","prediction").show()
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)




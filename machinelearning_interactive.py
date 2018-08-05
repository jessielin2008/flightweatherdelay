
# Imports
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import (StandardScaler, StringIndexer, VectorAssembler, IndexToString)
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row

spark = SparkSession.builder.appName("FlightDelayMachineLearning").getOrCreate()
print(spark.version)
#####################################################################################
# Base Line Linear Regression 
# # 400K weather delay >0 across four years
df = spark.sql("select * from flightdelay.flights_weather where weatherDelay > 0")
df.count()
df.show()
df = spark.sql("""select Year, Month, Day, DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier
                ,FlightNum,Origin,Dest,
               OriginTemp,OriginVisibility,OriginWindSpeed,OriginPrecipitation,OriginSnowDepth,
               OriginFog,OriginRain,OriginSnow,OriginHail, OriginThunder,OriginTornado,
               DestTemp,DestVisibility,DestWindSpeed,DestPrecipitation,DestSnowDepth,
               DestFog,DestRain,DestSnow,DestHail, DestThunder,DestTornado,
               Log10(WeatherDelay) as WeatherDelay
               from flightdelay.flights_weather
               where weatherDelay > 0
               """)
df.head()

# ## transform features
# #indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").
#                          fit(df) for column in list(set(df.columns)-set(['date'])) ]
ucInd = StringIndexer(inputCol="UniqueCarrier", outputCol="UniqueCarrierInd").fit(df)
flInd = StringIndexer(inputCol="FlightNum", outputCol="FlightNumInd").fit(df)
oInd = StringIndexer(inputCol="Origin", outputCol="OriginInd").fit(df)
oInd = StringIndexer(inputCol="Dest", outputCol="DestInd").fit(df)
assembler = VectorAssembler(
    inputCols=["Year","Month","Day", "DayOfWeek", "DepTime","UniqueCarrierInd","FlightNumInd","OriginInd",
               "OriginTemp","OriginVisibility","OriginWindSpeed","OriginPrecipitation","OriginSnowDepth",
               "OriginFog","OriginRain","OriginSnow","OriginHail", "OriginThunder","OriginTornado"],
    outputCol="features")
lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, 
                      featuresCol="features", labelCol="WeatherDelay")
pipeline = Pipeline(stages=[ucInd,flInd,oInd,assembler,lr])

dataModel = featureTransformer.fit(df)
transformedDF = dataModel.transform(df)
# Need to cache it otherwise StringIndexer throws Label Unseen error on random labels
transformedDF.cache()

# ## feed transformed features into linear regression
(training, test) = transformedDF.randomSplit([0.8,0.2], seed = 10)
lrModel = lr.fit(training)
prediction = lrModel.evaluate(test)

# Print the coefficients and intercept for linear regression
results = lrModel.evaluate(test)
results.predictions.select("WeatherDelay","prediction").show()
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

#####################################################################################
## Random Forest Classifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
df2 = spark.sql("""select Year,Month,Day, DayOfWeek,DepTime,UniqueCarrier,FlightNum,Origin,
               Temp,Visibility,WindSpeed,Precipitation,SnowDepth,
               Fog,Rain,Snow,Hail, Thunder,Tornado,
               case when weatherDelay < 0 then '0.Early Arrival'
                    when weatherDelay> 0 and weatherDelay <= 15 then '1.Less than 15 Minutes' 
                    when weatherDelay> 15 and weatherDelay <= 60 then  '2.Between 15 Minutes and 1 Hour' 
                    when weatherDelay> 60 and weatherDelay <= 120 then '3.Between 1 Hour and 2 Hours' 
                    when weatherDelay > 120 and weatherDelay <= 240 then '4.Between 2 Hours and 4 Hours' 
                    when weatherDelay > 240 then '5.Beyond 4 Hours' 
               end as delayCat
               from flightdelay.flights_weather
               where weatherDelay > 0 and year >= 2003 
               """)
#1, 9, 30, 99
df2 = spark.sql("""select Year,Month,Day, DayOfWeek,DepTime,UniqueCarrier,FlightNum,Origin,Dest,
               OriginTemp,OriginVisibility,OriginWindSpeed,OriginPrecipitation,OriginSnowDepth,
               OriginFog,OriginRain,OriginSnow,OriginHail, OriginThunder,OriginTornado,
               DestTemp,DestVisibility,DestWindSpeed,DestPrecipitation,DestSnowDepth,
               DestFog,DestRain,DestSnow,DestHail, DestThunder,DestTornado,
               case when weatherDelay < 0 then '0.Early Arrival'
                    when weatherDelay> 0 and weatherDelay <= 5 then '1.Less than 15 Minutes' 
                    when weatherDelay> 5 and weatherDelay <= 20 then  '2.Between 15 Minutes and 1 Hour' 
                    when weatherDelay>20 and weatherDelay <= 60 then '3.Between 1 Hour and 2 Hours' 
                    when weatherDelay > 60 then '5.Beyond 4 Hours' 
               end as delayCat
               from flightdelay.flights_weather
               where weatherDelay > 0 and year >= 2003 
               """)
ucInd = StringIndexer(inputCol="UniqueCarrier",outputCol="UniqueCarrierInd").fit(df2)
flInd = StringIndexer(inputCol="FlightNum", outputCol="FlightNumInd").fit(df2)
oInd = StringIndexer(inputCol="Origin",outputCol="OriginInd").fit(df2)
dInd = StringIndexer(inputCol="Dest",outputCol="DestInd").fit(df2)
labelInd = StringIndexer(inputCol="delayCat", outputCol="delayCatInd").fit(df2)
assembler = VectorAssembler(
    inputCols=["Year","Month", "Day", "DayOfWeek", "DepTime","UniqueCarrierInd","FlightNumInd","OriginInd", "DestInd",
               "OriginTemp","OriginVisibility","OriginWindSpeed","OriginPrecipitation","OriginSnowDepth",
               "OriginFog","OriginRain","OriginSnow","OriginHail", "OriginThunder","OriginTornado",
               "DestTemp","DestVisibility","DestWindSpeed","DestPrecipitation","DestSnowDepth",
               "DestFog","DestRain","DestSnow","DestHail", "DestThunder","DestTornado"],
    outputCol="features")
classifier = RandomForestClassifier(labelCol = 'delayCatInd', featuresCol = 'features', numTrees= 15, maxDepth =15, predictionCol="prediction", maxBins=8000)
labelConv = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelInd.labels)

pipeline = Pipeline(stages=[ucInd, flInd, oInd,dInd, labelInd, assembler,classifier, labelConv])
(train, test) = df2.randomSplit([0.7, 0.3])
model = pipeline.fit(train)
predictions = model.transform(train)
predictions.head()

# ## Model Evalution
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="delayCatInd", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages[2]
print(rfModel)  # summary only


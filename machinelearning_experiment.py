
# Imports
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import (StandardScaler, StringIndexer, VectorAssembler, IndexToString)
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import cdsw

spark = SparkSession.builder.appName("FlightDelayMachineLearning").getOrCreate()
print(spark.version)

param_numTrees=int(sys.argv[1])
param_maxDepth=int(sys.argv[2])

#####################################################################################
## Random Forest Classifier
df2 = spark.sql("""select Year,Month,Day, DayOfWeek,DepTime,UniqueCarrier,FlightNum,Origin,
               Temp,Visibility,WindSpeed,Precipitation,SnowDepth,
               Fog,Rain,Snow,Hail, Thunder,Tornado,
               case when weatherDelay> 0 and weatherDelay <= 15 then '1.Less than 15 Minutes' 
                    when weatherDelay> 15 and weatherDelay <= 120 then  '2.Between 15 Minutes and 2 Hour' 
                    when weatherDelay > 120 then '3.Beyond 2 Hours'
               end as delayCat
               from flightdelay.flights_OriginWeather
               where weatherDelay > 0 and year >= 2003 
               """)

ucInd = StringIndexer(inputCol="UniqueCarrier",outputCol="UniqueCarrierInd").fit(df2)
flInd = StringIndexer(inputCol="FlightNum", outputCol="FlightNumInd").fit(df2)
oInd = StringIndexer(inputCol="Origin",outputCol="OriginInd").fit(df2)
labelInd = StringIndexer(inputCol="delayCat", outputCol="delayCatInd").fit(df2)
assembler = VectorAssembler(
    inputCols=["Year","Month", "Day", "DayOfWeek", "DepTime","UniqueCarrierInd","FlightNumInd","OriginInd",
               "Temp","Visibility","WindSpeed","Precipitation","SnowDepth",
               "Fog","Rain","Snow","Hail", "Thunder","Tornado"],
    outputCol="features")
classifier = RandomForestClassifier(labelCol = 'delayCatInd', featuresCol = 'features', 
              numTrees= param_numTrees, maxDepth = param_maxDepth, predictionCol="prediction", maxBins=8000)
cdsw.track_metric("numTrees",param_numTrees)
cdsw.track_metric("maxDepth",param_maxDepth)

labelConv = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelInd.labels)

pipeline = Pipeline(stages=[ucInd, flInd, oInd, labelInd, assembler,classifier, labelConv])
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

cdsw.track_metric("accuracy", accuracy)

rfModel = model.stages[2]
print(rfModel)  # summary only

spark.stop()
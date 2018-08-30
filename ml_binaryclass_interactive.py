
# Imports
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import (StandardScaler, StringIndexer, VectorAssembler, IndexToString, OneHotEncoderEstimator)
from pyspark.ml import Pipeline
from pyspark.sql import Row

spark = SparkSession.builder.appName("FlightDelayMachineLearning") \
      .config("spark.executor.memory", "32g") \
      .config("spark.yarn.executor.memoryOverhead", "1g") \
      .getOrCreate()
      
print(spark.version)
#####################################################################################
#  # Prep for data
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# ## Deal with imbalanced dataset
df = spark.sql("""select Year, Month, Day,DayOfWeek,CRSDepHour,UniqueCarrier,Origin,Dest
               , MaxWindSpeed,MinCeilingHeight, MinVisibility,MaxPressure, MaxPrecipitation48
               , MaxPresentWeatherCode
               , 1 as label 
               from flightdelay.flights_weatherhourly
               where weatherDelay > 0
               and MaxWindSpeed is not null
               and MinCeilingHeight is not null
               and MinVisibility is not null
               and MaxPressure is not null
               and MaxPresentWeatherCode is not null
               """)
df.count()
df1 = spark.sql("""select Year, Month, Day,DayOfWeek,CRSDepHour,UniqueCarrier,Origin,Dest
               , MaxWindSpeed,MinCeilingHeight, MinVisibility,MaxPressure,MaxPrecipitation48
               , MaxPresentWeatherCode
               , 0 as label
               from flightdelay.flights_weatherhourly
               where weatherDelay = 0
               and MaxWindSpeed is not null
               and MinCeilingHeight is not null
               and MinVisibility is not null
               and MaxPressure is not null
               and MaxPresentWeatherCode is not null
          """).sample(False, 0.02)
df1.count()
df2= df.unionAll(df1)
df2.count()
(training, test) = df2.randomSplit([0.7, 0.3])
# ## Index string features
ucInd = StringIndexer(inputCol="UniqueCarrier", outputCol="UniqueCarrierInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct UniqueCarrier from flightdelay.flights_weatherhourly"))
oInd = StringIndexer(inputCol="Origin", outputCol="OriginInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct Origin from flightdelay.flights_weatherhourly"))
dInd = StringIndexer(inputCol="Dest", outputCol="DestInd", handleInvalid="keep") \
        .fit(spark.sql("select distinct Dest from flightdelay.flights_weatherhourly"))
###############################################################
# ## With flight info alone using Random Forest 63%
encoder = OneHotEncoderEstimator(inputCols=["UniqueCarrierInd", "OriginInd", "DestInd"],
                                 outputCols=["UniqueCarrierOHE", "OriginOHE","DestOHE"])

assembler = VectorAssembler(
    inputCols=["Month","Day","DayOfWeek","CRSDepHour","UniqueCarrierOHE","OriginOHE","DestOHE"
              ],
    outputCol="features")
classifier = RandomForestClassifier(predictionCol="prediction")
pipeline = Pipeline(stages=[ucInd, oInd, destInd,encoder, assembler,classifier])
model = pipeline.fit(training)
predictions = model.transform(test)
predictions.head()

# ## Model Evalution
# Select (prediction, true label) and compute test error
evaluator = BinaryClassificationEvaluator()
auROC = evaluator.evaluate(predictions)
print("Area Under ROC = %g" % auROC)

rfModel = model.stages[-1]
print(rfModel)  # summary only

###############################################################
# # With Hourly weather using Random Forest 67%
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
    inputCols=["Month","Day","DayOfWeek","CRSDepHour","UniqueCarrierOHE","OriginOHE","DestOHE"
              ,"MaxWindSpeed","MinCeilingHeight","MinVisibility","MaxPressure","MaxPrecipitation48"
              ,"WeatherCodeOHE"
              ],
    outputCol="features")
classifier = RandomForestClassifier(predictionCol="prediction")

pipeline = Pipeline(stages=[ucInd, oInd, dInd,wInd,encoder, assembler,classifier])
model = pipeline.fit(training)
predictions = model.transform(test)
predictions.head()

# ## Model Evalution
# Select (prediction, true label) and compute test error
evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="prediction")
auROC = evaluator.evaluate(predictions)
print("Area Under ROC = %g" % auROC)

rfModel = model.stages[-1]
print(rfModel)  # summary only


#####################################################################################
#  # Logistic regression 77%
from pyspark.ml.classification import LogisticRegression

encoder = OneHotEncoderEstimator(inputCols=["UniqueCarrierInd", "OriginInd", "DestInd"],
                                 outputCols=["UniqueCarrierOHE", "OriginOHE","DestOHE"])
assembler = VectorAssembler(
    inputCols=["Month","Day","DayOfWeek","CRSDepHour","UniqueCarrierOHE","OriginOHE","DestOHE"],
    outputCol="features")
lr = LogisticRegression(maxIter=10)
pipeline = Pipeline(stages=[ucInd, oInd, destInd,encoder, assembler,lr])

# Fit the model
lrModel = pipeline.fit(training)
trainingSummary = lrModel.stages[-1].summary

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
roc_auc = trainingSummary.areaUnderROC
print("areaUnderROC: " + str(roc_auc))

#lrModel.write().overwrite().save("flightdelay/lrfModel")
#
#!rm -r -f models/spark/lrfModel
#!rm -r -f models/spark_lrf.tar
#!hdfs dfs -get flightdelay/lrfModel models/spark/lrfModel
#!tar -cvf models/spark_lrf.tar models/spark/lrfModel

###############################################################
# # With weather info 79%
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
    inputCols=["Month","Day","DayOfWeek","CRSDepHour","UniqueCarrierOHE","OriginOHE","DestOHE"
              ,"MaxWindSpeed","MinCeilingHeight","MinVisibility","MaxPressure","MaxPrecipitation48"
              ,"WeatherCodeOHE"
              ],
    outputCol="features")
lr = LogisticRegression(maxIter=10)
pipeline = Pipeline(stages=[ucInd, oInd, dInd,wInd, encoder, assembler,lr])

# Fit the model
lrModel = pipeline.fit(training)
trainingSummary = lrModel.stages[-1].summary

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
#trainingSummary.roc.show()
roc_auc = trainingSummary.areaUnderROC
print("areaUnderROC: " + str(roc_auc))
# plot ROC
def plot_roc(roc):
  import matplotlib.pyplot as plt
  plt.title('Receiver Operating Characteristic')
  plt.plot(roc.FPR, roc.TPR, 'b',label='AUC = %0.5f'% roc_auc)
  plt.legend(loc='lower right')
  plt.plot([0,1],[0,1],'r--')
  plt.xlim([-0.1,1.0])
  plt.ylim([-0.1,1.01])
  plt.ylabel('True Positive Rate')
  plt.xlabel('False Positive Rate')
  plt.show()

plot_roc(trainingSummary.roc.toPandas())

###############################################################
# # Logistic Regression Grid search 79%
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.001,0.01, 0.1]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

evaluator = BinaryClassificationEvaluator()
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=3)  # use 3+ folds in practice

# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(training)

# Evaluate model
predictions = cvModel.transform(test)   
evaluator.evaluate(predictions)
predictions.head()

trainingSummary = cvModel.bestModel.stages[-1].summary
# Print the coefficients and intercept for logistic regression
#print("Coefficients: " + str(lrModel.stages[-1].coefficients))
#print("Intercept: " + str(lrModel.stages[-1].intercept))

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
trainingSummary.roc.show()
roc_auc = trainingSummary.areaUnderROC
print("areaUnderROC: " + str(roc_auc))

#############################################################
# # Save model

cvModel.bestModel.write().overwrite().save("flightdelay/lrwModel")

!rm -r -f models/spark/lrwModel
!rm -r -f models/spark_lrw.tar
!hdfs dfs -get flightdelay/lrwModel models/spark/lrwModel
!tar -cvf models/spark_lrw.tar models/spark/lrwModel

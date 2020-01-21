import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel, Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(inputs, model_file):
    
    # get the data
    test_tmax = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = test_tmax.randomSplit([0.75, 0.25])

    # with yesterday feature, the code is as following:
    sql_query = 'SELECT today.latitude as latitude, today.longitude as longitude, today.elevation as elevation, dayofyear(today.date) as dayofyear, today.tmax as tmax, yesterday.tmax AS y_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station'
    sql_transformer = SQLTransformer(statement=sql_query)
  
  
    assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'dayofyear', 'y_tmax'], outputCol='features')
    classifier = GBTRegressor(featuresCol='features', labelCol='tmax')
    pipelineModel = Pipeline(stages=[sql_transformer, assembler, classifier])


#     # without yesterday feature, the code is as following:
#     sql_query = 'SELECT today.latitude as latitude, today.longitude as longitude, today.elevation as elevation, dayofyear(today.date) as dayofyear, today.tmax as tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station'
#  
#     sql_transformer = SQLTransformer(statement=sql_query)
#     assembler = VectorAssembler(inputCols=['latitude', 'longitude', 'elevation', 'dayofyear'], outputCol='features')
#  
#     classifier = GBTRegressor(featuresCol='features', labelCol='tmax')
#     pipelineModel = Pipeline(stages=[sql_transformer, assembler, classifier])


    # load the model
    model = pipelineModel.fit(train)
   # model = PipelineModel.load(train)
    
    # use the model to make predictions
    predictions = model.transform(validation)
    
    #predictions.show()    
    
    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
   
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    print('r2 =', r2)
    print('rmse =', rmse)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    #print(model.stages[-1].featureImportances)

    model.write().overwrite().save(model_file)
    

if __name__ == '__main__':
    model_file = sys.argv[2]
    inputs = sys.argv[1]
    main(inputs, model_file)

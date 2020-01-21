import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import datetime

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


def main(model_file):
    
    t_model = PipelineModel.load(model_file)
    sfu_tmrw = [("sfu", datetime.date(2019, 11, 8), 49.2771, -122.9146, 330.0, 12.0), ("sfu", datetime.date(2019, 11, 9), 49.2771, -122.9146, 330.0, 12.0)]
    sfu_tmrw_df = spark.createDataFrame(sfu_tmrw, schema=tmax_schema)
    sfu_tmrw_df.show()
    
    prediction = t_model.transform(sfu_tmrw_df)
    #prediction.show()
    
    prediction = prediction.collect()[0]['prediction']
     
    print('Predicted tmax tomorrow:', prediction)

if __name__ == '__main__':
    model_file = sys.argv[1]
    main(model_file)
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    
    # TODO: create a pipeline to predict RGB colours -> word
    
    rgb_assembler =  VectorAssembler(inputCols=['R', 'G', 'B'], outputCol = 'features')                     
    word_indexer = StringIndexer(inputCol='word', outputCol='new_word')
    classifier = MultilayerPerceptronClassifier(labelCol="new_word",layers=[3, 30, 11])
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)
    
    # TODO: create an evaluator and score the validation data
    
    rgb_validation = rgb_model.transform(validation)
    # rgb_validation.show()
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    vali_evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol='new_word')
    score = vali_evaluator.evaluate(rgb_validation)
    print('Validation score for RGB model: %g' % (score, ))
    
    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.

    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sql_transformer = SQLTransformer(statement = rgb_to_lab_query)
  
    new_assembler = VectorAssembler(inputCols=['labL', 'labA', 'labB'], outputCol='features')
    new_pipeline = Pipeline(stages = [sql_transformer, new_assembler, word_indexer, classifier])
    new_training = sql_transformer.transform(train)
    new_model = new_pipeline.fit(new_training)
    new_validation = new_model.transform(validation)
    
    #new_validation.show()
    
    new_vali_evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='new_word')
    new_score = new_vali_evaluator.evaluate(new_validation)
    print('Validation score for LAB model:', new_score)
    print('Validation score for LAB model:', new_score)
    print('Validation score for LAB model:', new_score)

    plot_predictions(new_model, 'LAB', labelCol="word")

    
if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main(inputs):
    
    # main logic starts here

    messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092').option('subscribe', inputs).load()
    values = messages.select(messages['value'].cast('string'))
    values_split = functions.split(values.value, ' ')
    
    values = values.withColumn('x', values_split[0])
    values = values.withColumn('y', values_split[1])
    
    xy = values_split[0] * values_split[1]
    x2 = values_split[0] * values_split[0]
    
    values = values.withColumn('xy', xy)
    values = values.withColumn('x2', x2)
  
#     print(values)
#     DataFrame[value: string, x: string, y: string, xy: double, x2: double]

    sum_df = values.agg(functions.sum('x').alias('x_sum'),functions.sum('y').alias('y_sum'),functions.sum('xy').alias('xy_sum'),functions.sum('x2').alias('x2_sum'),functions.count('x').alias('n_count'))
    
    b_beta = sum_df.withColumn('b_beta', (sum_df.xy_sum- ((sum_df.x_sum * sum_df.y_sum)/sum_df.n_count))/((sum_df.x2_sum)- ((sum_df.x_sum*sum_df.x_sum)/sum_df.n_count)))
    
    a_alpha = b_beta.withColumn('a_alpha',((sum_df.y_sum/sum_df.n_count) - (b_beta.b_beta*(sum_df.x_sum)/sum_df.n_count)))
    
    final_stream = a_alpha.select('b_beta', 'a_alpha')
    
    stream = final_stream.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(600)


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
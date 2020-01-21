from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
spark = SparkSession.builder.appName('wikipedia_popular_df').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+

    
def path_to_hour(path):
    
    split_word = path.split('/')
    filename = split_word[len(split_word)-1]
    hour = filename[11:22]
    return hour


def main(inputs, output):
    
    pathfunction = functions.udf(path_to_hour, returnType=types.StringType())

    comments_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('views', types.LongType()),
        types.StructField('bytes', types.LongType()),

    ])

    wikipage = spark.read.csv(inputs, schema = comments_schema, sep=' ').withColumn('hour', pathfunction(functions.input_file_name()))
    filtered_page = wikipage.filter((wikipage.language=='en') & (wikipage.title != 'Main Page') & (~ wikipage.title.startswith('Special:'))).cache()

    max_view = filtered_page.groupBy(wikipage.hour).agg(functions.max(wikipage.views).alias('total_views'))
    conditions = [filtered_page.views == max_view.total_views, filtered_page.hour == max_view.hour]
    
    # regular join: join_page = filtered_page.join(functions.broadcast(max_view), conditions).select(filtered_page.hour, 'title', 'views')
    # broadcast join as following:
    join_page = filtered_page.join(functions.broadcast(max_view), conditions).select(filtered_page.hour, 'title', 'views')
    join_page.sort('hour', 'title').write.json(output, mode = 'overwrite')
    join_page.explain() 

if __name__ == '__main__':

    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
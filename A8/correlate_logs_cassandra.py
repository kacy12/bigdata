import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import math
from pyspark.sql import SparkSession, functions, types
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('example code').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext



def main(keyspace, table):
    # main logic starts here
    
    match_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    match_df.createOrReplaceTempView('match_df')
    match_df = spark.sql("SELECT *, 1 as count from match_df")
    match_df.show()
    
    calculation_method = [functions.sum(match_df['count']).alias('count_requests'),functions.sum(match_df['bytes']).alias('sum_request_bytes')]
    xy_df = match_df.groupBy('hostname').agg(*calculation_method)
   
                                       
    r_df = xy_df.withColumn('x2', xy_df.count_requests**2).withColumn('y2', xy_df.sum_request_bytes**2).withColumn('xy', xy_df.count_requests*xy_df.sum_request_bytes).cache()
    n_count = r_df.count()
    
    sum_df = r_df.groupby().sum()
  
    print('n_counttttt', n_count)
    
    sum_df = sum_df.collect()
    x_i = sum_df[0][0]
    y_i = sum_df[0][1]
    x_i_2 = sum_df[0][2]
    y_i_2 = sum_df[0][3]
    xy_i = sum_df[0][4] 
    
    x_2 = x_i**2
    y_2 = y_i**2 
    sqrt_xi_2 = math.sqrt((n_count*x_i_2)-x_2)
    sqrt_yi_2 = math.sqrt((n_count*y_i_2)-y_2)
    
    
    r = (n_count*xy_i-(x_i*y_i))/(sqrt_xi_2*sqrt_yi_2)
    r2 = r**2
    r_corr = xy_df.corr('count_requests', 'sum_request_bytes')
    print('r issssssssssss', r)
    print('r_corr function issssssssssss', r_corr)
    print('r2 issssssssssss', r2)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    table = sys.argv[2]
    main(keyspace, table)
    
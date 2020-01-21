
from pyspark.sql import SparkSession, functions, types

from pyspark import SparkConf, SparkContext
import sys
import re
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
conf = SparkConf().setAppName('correlate logs')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('correlate logs').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+


line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')


NASA_schema = types.StructType([
        types.StructField('hostname', types.StringType()),
        types.StructField('count', types.IntegerType()),
        types.StructField('bytes', types.IntegerType()),
        
        ])

def match_line(line):
    
    match = re.search(line_re, line)
    
    if match:
        
        match_line = re.split(line_re, line)
        
        hostname = match_line[1]
        #requests = match_line[3]
        bytes = int(match_line[4])
        return (hostname, 1, bytes)
    

def main(inputs):
    text = sc.textFile(inputs)
    match1 = text.map(match_line)
    match2 = match1.filter(lambda x: x is not None)
    
    match_df = spark.createDataFrame(match2, schema = NASA_schema)
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
    inputs = sys.argv[1]
    main(inputs)

from pyspark.sql import SparkSession, functions, types
from pyspark import SparkConf, SparkContext
import math
from cassandra.cluster import Cluster
import cassandra.query
import re, gzip
import sys, os
from uuid import uuid1
import datetime
import org.apache.spark.sql.cassandra

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
conf = SparkConf().setAppName('cassandra')
sc = SparkContext(conf=conf)

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

assert spark.version >= '2.4' # make sure we have Spark 2.4+


line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')


NASA_schema = types.StructType([
        types.StructField('hostname', types.StringType()),
        types.StructField('uuid_1', types.StringType()),
        types.StructField('bytes', types.IntegerType()),
        types.StructField('date_new', types.TimestampType()),
        types.StructField('path', types.StringType()),

        
        ])


def match_line(line):
    
    
    match = re.search(line_re, line)
    
    if match:

        
        match_line = re.split(line_re, line)
        hostname = match_line[1]
        date_new = datetime.datetime.strptime(match_line[2], '%d/%b/%Y:%H:%M:%S')          
        path = match_line[3]
        bytes = int(match_line[4])
        uuid_1 = uuid1()
        
        return (hostname, str(uuid_1), bytes, date_new, path)
                    
    
        
def main(inputs, keyspace, tablename):
    
    text = sc.textFile(inputs)
    match1 = text.map(match_line)
    
    match2 = match1.filter(lambda x: x is not None)

    match_df = spark.createDataFrame(match2, schema = NASA_schema).repartition(100)
    
    match_df.write.format("org.apache.spark.sql.cassandra").options(table=tablename, keyspace=keyspace).save()

   
   
if __name__ == '__main__':
    
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    tablename = sys.argv[3]
    
    main(inputs, keyspace, tablename)
    
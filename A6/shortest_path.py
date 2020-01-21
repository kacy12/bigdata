from pyspark.sql import SparkSession, functions, types
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import sys

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
conf = SparkConf().setAppName('shortest_path')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('shortest_path').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
sqlContext = SQLContext(sc)


edge_schema = types.StructType([
    types.StructField('start', types.StringType()),
    types.StructField('target', types.StringType()),
    
    ])

path_schema = types.StructType([
   
    types.StructField('destination', types.StringType()),
    types.StructField('previous', types.StringType()),
    types.StructField('distance', types.IntegerType()),

    ])


node_schema = types.StructType([
   
    types.StructField('node', types.StringType()),

    ])

def graph_edge(graph):
    path = graph.split(':')
      
    start = path[0]
  
    all_targets = path[1].split(" ")[1:]
    
    l = []
    for target in all_targets:
        l.append((start, target))
    return l


def main(inputs, output, start, end):
    
    rdd = sc.textFile(inputs + '/links-simple-sorted.txt')
    edges = rdd.flatMap(graph_edge)
     
    
 
    edges_df = spark.createDataFrame(edges, edge_schema).cache()
    edges_df.show()
     
    known_paths = [[start, 'dummy',0]]
    known_paths = sqlContext.createDataFrame(known_paths, path_schema).cache()
    
     
    current_node = [[start]]
    current_node = sqlContext.createDataFrame(current_node, node_schema).cache()
     
    for i in range(6):
        current_neighbors = edges_df.join(current_node, edges_df.start == current_node.node).drop('node').cache()
    
        new_paths = known_paths.join(current_neighbors, known_paths.destination == current_neighbors.start).select(current_neighbors.target, current_neighbors.start, known_paths.distance)
        new_paths = new_paths.withColumn('updated_distance', new_paths.distance + 1).drop(new_paths.distance)
 
        known_paths = known_paths.unionAll(new_paths).dropDuplicates()
        minimum_p = known_paths.groupBy(known_paths.destination).agg(functions.min(known_paths.distance).alias('minimum_dist')).withColumnRenamed('destination','minimum_dest')
        known_paths = minimum_p.join(known_paths, (minimum_p.minimum_dest == known_paths.destination) & (minimum_p.minimum_dist == known_paths.distance)).drop('minimum_dist', 'minimum_dest').cache()
        known_paths.write.csv(output + '/iter-' + str(i), mode = 'overwrite')
    
        current_node = current_neighbors.select('target').withColumnRenamed('target', 'node')
        
        if current_node.where(col("target") == end).count() > 0:
            break
         
    
         
    last_node = end
    output_list = [last_node]
         
    while(last_node != start):
              
        value = known_paths.where(known_paths.destination == last_node).collect()
              
        last_node = value[0][1]
        output_list.append(last_node)
              
    output_list = list(reversed(output_list))
    final_path = sc.parallelize(output_list)
    final_path.saveAsTextFile(output+ "/path") 
            
             
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    start = sys.argv[3]
    end = sys.argv[4]
    main(inputs, output, start, end)
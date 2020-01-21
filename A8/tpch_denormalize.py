import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('example code').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary


'''

CREATE TABLE orders_parts (
    
  orderkey int,
  custkey int,
  orderstatus text,
  totalprice decimal,
  orderdate date,
  order_priority text,
  clerk text,
  ship_priority int,
  comment text,
  part_names set<text>,
  PRIMARY KEY (orderkey)
);

'''


def main(keyspace1, keyspace2):
    
    order_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace1).load()
    order_df.createOrReplaceTempView('orders')
    
    part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace1).load()
    part_df.createOrReplaceTempView('part')

    lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace1).load()
    lineitem_df.createOrReplaceTempView('lineitem')
    
    summary_table = spark.sql('''
    
    SELECT o.orderkey, o.custkey, o.orderstatus, o.totalprice, o.orderdate, o.order_priority, o.clerk, o.ship_priority, o.comment, p.name
    FROM orders AS o, lineitem AS l, part AS p
    WHERE o.orderkey = l.orderkey AND l.partkey = p.partkey 
    ORDER BY o.orderkey, p.name
    ''')
    
    group_table = summary_table.groupBy('orderkey', 'custkey', 'orderstatus', 'totalprice','orderdate', 'order_priority', 'clerk', 'ship_priority','comment').agg(functions.collect_set('name').alias('part_names'))
    group_table.write.format('org.apache.spark.sql.cassandra').options(table = 'orders_parts', keyspace = keyspace2).save(mode='append')
    


if __name__ == '__main__':
    keyspace1 = sys.argv[1]
    keyspace2 = sys.argv[2]
    main(keyspace1, keyspace2)
    
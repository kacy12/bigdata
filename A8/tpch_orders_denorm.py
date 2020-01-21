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
  part_names text
  PRIMARY KEY (orderkey)
);

'''

def output_line(row):
    orderkey = row[0] 
    price = row[1]
    names = row[2]
    
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(keyspace, outdir, orderkeys):
    
    
    
    order_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=keyspace).load()
    order_df.createOrReplaceTempView('orders_parts')
    
         
    summary_table = spark.sql(('''
    SELECT o.orderkey, o.totalprice, o.part_names
    FROM orders_parts AS o
    WHERE o.orderkey IN %s
    ORDER BY o.orderkey
    ''' % orderkeys).replace('[', '(').replace(']', ')'))
                 
    #summary_table.show(5, False)

    order_rdd = summary_table.rdd.map(output_line)
    order_rdd.saveAsTextFile(outdir)


if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    orderkeys = [int(k) for k in orderkeys]
    main(keyspace, outdir, orderkeys)
    
    
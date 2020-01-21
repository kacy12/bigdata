import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('example code').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext



def output_line(row):
    
    orderkey = row[0] 
    price = row[1]
    names = row[2]
    
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(keyspace, outdir, orderkeys):
    
    # main logic starts here

    order_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace).load()
    order_df.createOrReplaceTempView('orders')
    
    
    part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace).load()
    part_df.createOrReplaceTempView('part')
    

    lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace).load()
    lineitem_df.createOrReplaceTempView('lineitem')

         
    summary_table = spark.sql(('''
    SELECT o.orderkey, o.totalprice, p.name
    FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey JOIN part p ON p.partkey = l.partkey
    WHERE o.orderkey IN %s
    ORDER BY o.orderkey, p.name
    ''' % orderkeys).replace('[', '(').replace(']', ')'))
                 


    group_table = summary_table.groupBy('orderkey', 'totalprice').agg(functions.collect_set('name'))
    
    group_table = group_table.orderBy(group_table.orderkey)
    group_table.explain()
    
    order_rdd = group_table.rdd.map(output_line)
    order_rdd.saveAsTextFile(outdir)
    


if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    orderkeys = [int(k) for k in orderkeys]
    main(keyspace, outdir, orderkeys)
    

from pyspark import SparkConf, SparkContext
import sys
import random
import operator
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

    
def euler(partitions):
    total_iterations = 0

    for i in range(partitions):
        sum = 0.0
        while (sum <1.0):
            
            rnumber = random.random()
            sum += rnumber
            total_iterations += 1
        
    return total_iterations
    

def main(inputs):
    group = 10
    s = [inputs // group] * group
    RDDrange=sc.parallelize(s, numSlices=15)
    newRDD = RDDrange.map(euler) 
    iterations = newRDD.reduce(operator.add)
    finaleuler = float (iterations) / inputs
        
    print ("Euler is ", finaleuler)


if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(int(inputs))

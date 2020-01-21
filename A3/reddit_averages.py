from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    
def parse_json(line):
    parsed_json = json.loads(line)
    key = parsed_json["subreddit"]
    score = parsed_json["score"]
    count=1
    return(key,(count, score))

def add_pairs(a, b):
    addcount = (a[0]+b[0])
    addscore = (a[1]+b[1])
    return (addcount, addscore)
    
def get_key (kv):
    return kv[0]

def averagescore(kv):
    totalcount = kv[1][0]
    totalscore =kv[1][1]
    finalscore = totalscore/totalcount
    return (kv[0],finalscore)

def main(inputs, output):
    text = sc.textFile(inputs) 
    alldata = text.map(parse_json)
    Jreduce = alldata.reduceByKey(add_pairs)
    average = Jreduce.map(averagescore)
    outdata = average.sortBy(get_key).map(json.dumps)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit Average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

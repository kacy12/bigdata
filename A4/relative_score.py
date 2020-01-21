from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    
def parse_json(line):
    key = line["subreddit"]
    score = line["score"]
    count = 1
    value = (count, score)
    return (key,value)

def subredit_pair(a, b):
    result = (a[0]+b[0], a[1]+b[1])
    return (result)

def positive_data(value):
    if value [1]>0:
        return True
    else:
        return False
    
def averagescore(kv):
    k, v = kv[0], kv[1]
    totalcount, totalscore = v[0], v[1]
    averagescore = totalscore/totalcount
    return (k, averagescore)
    
def best_score(kv):
    average = kv[1][0]
    score = int(kv[1][1]['score'])
    author = kv[1][1]['author']
    best_scores = float(score / average)
    return (best_scores, author)

    
def main(inputs, output):

    text = sc.textFile(inputs).map(json.loads).cache() 
    averages = text.map(parse_json).reduceByKey(subredit_pair).map(averagescore).filter(positive_data)
    commentbysub = text.map(lambda c: (c['subreddit'], c))
    joindata = averages.join(commentbysub)
    best_scores = joindata.map(best_score)
    sortdata = best_scores.sortBy(lambda kv: kv[0], ascending=False)
    outdata = sortdata.saveAsTextFile(output)
   

if __name__ == '__main__':
    conf = SparkConf().setAppName('Relative Score')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
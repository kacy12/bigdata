from pyspark import SparkConf, SparkContext
import sys
import json
import timeit
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    
def parse_json(line):
    parsed_json = json.loads(line)
    key = parsed_json["subreddit"]
    score = parsed_json["score"]
    author = parsed_json["author"]
    return(key,score,author)


def e_data(value):
    if 'e' in value[0]:
        return True
    
    else:
        return False
    
def positive_reddit(value):
    if value[1] > 0:
        return True
    
    else:
        return False

def negative_reddit(value):
    if value [1] <0:
        return True
    
    else:
        return False
    

def main(inputs, output):
    start = timeit.default_timer()
    text = sc.textFile(inputs).map(parse_json) 
    filtered_subreddit = text.filter(e_data).cache()
    p_reddit = filtered_subreddit.filter(positive_reddit)
    n_reddit = filtered_subreddit.filter(negative_reddit)
    p_reddit_output = p_reddit.map(json.dumps).saveAsTextFile(output + '/positive')
    n_reddit_output = n_reddit.map(json.dumps).saveAsTextFile(output + '/negative')
    stop = timeit.default_timer()
    print ('Running Time is: ', stop-start, 'seconds')
    
   
if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit ETL')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
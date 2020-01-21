from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


# split and return tuple of 5 things
def mapper(line):
    l = line.split()
    
    return (l[0], l[1], l[2], l[3], l[4])

    
# filter method, only need language: English and filter out Main and Special
def filter_method(line): 
     
    l = list(line)   
    if l[1] != "en" or l[2] == "Main_Page" or l[2].startswith("Special:"):
        
        return False
    
    else:
        return line

# group page name and view count together            
def tuplemap(line):
    
    line = list(line)
    line [3] = int(line[3])

    return (line [0], (line[3],line[2]))
               
# key value definition    
def get_key (kv):
    return kv[0]

#output format
def tab_seperated(kv):
  
    return "%s\t%s" % (kv[0], kv[1])



text=sc.textFile(inputs) #read text file
mappage=text.map(mapper) # map
filter_page=mappage.filter(filter_method) #filter
title = filter_page.map(tuplemap)#Map
max_count=title.reduceByKey(max) #reduce
outdata=max_count.sortBy(get_key).map(tab_seperated) #sorting
outdata.saveAsTextFile(output)  #output method 
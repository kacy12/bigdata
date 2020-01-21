import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temp_range_sql').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+

observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])

def main(inputs, output):

    weather = spark.read.csv(inputs, schema=observation_schema)
    weather.createOrReplaceTempView('weather')
    
    
    filter_weather = spark.sql("SELECT date, station, observation, value FROM weather WHERE qflag IS NULL")
    filter_weather.createOrReplaceTempView('filter_weather')
    
    max_weather = spark.sql("SELECT * FROM filter_weather WHERE observation = 'TMAX' ")
    max_weather.createOrReplaceTempView('max_weather')
   
    min_weather = spark.sql("SELECT * FROM filter_weather WHERE observation = 'TMIN' ")
    min_weather.createOrReplaceTempView('min_weather')
    
    range = spark.sql("SELECT max_weather.date, max_weather.station, ((max_weather.value - min_weather.value)/10) AS range FROM max_weather, min_weather WHERE max_weather.date = min_weather.date AND max_weather.station = min_weather.station")
    range.createOrReplaceTempView('range')

    max_range = spark.sql("SELECT date, MAX(range) AS Max_range FROM range GROUP BY date")
    max_range.createOrReplaceTempView('max_range')
    
    max_station = spark.sql("SELECT range.date, range.station, range.range FROM range, max_range WHERE range.date = max_range.date AND range.range == max_range.Max_range ORDER BY date, station" ) 
    max_station.write.csv(output, mode = 'overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
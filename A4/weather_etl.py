import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
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
    filter_weather = weather.filter(weather.qflag.isNull()).filter(weather.station.startswith('CA')).filter(weather.observation == ('TMAX'))
    divide_temperature = filter_weather.withColumn('tmax', weather.value/10)
    select_weather = divide_temperature.select('station','date','tmax')
    select_weather.show()
    select_weather.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
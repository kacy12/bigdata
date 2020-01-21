import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temp_range_dataframe').getOrCreate()
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
    filter_weather = weather.filter(weather.qflag.isNull()).cache()

    Max_weather = filter_weather.filter(weather.observation == ('TMAX')).withColumn('tmax', weather.value)
    Max_temperature = Max_weather.select('station','date','tmax')
    Max_temperature.show(10)

    Min_weather = filter_weather.filter(weather.observation == ('TMIN')).withColumn('tmin', weather.value)
    Min_temperature = Min_weather.select('station','date','tmin')
    
    # join
    join_MaxMin = Max_weather.join(Min_weather, [Max_temperature.station == Min_temperature.station, Max_temperature.date == Min_temperature.date])
    select_MaxMin = join_MaxMin.select(Max_temperature.station, Min_temperature.date, 'tmax','tmin')
    select_MaxMin.show(10)

    range = select_MaxMin.select(select_MaxMin.station, select_MaxMin.date, ((select_MaxMin.tmax - select_MaxMin.tmin)/10).alias('range'))
    Max_range = range.groupby(range.date).agg(functions.max(range.range).alias('Max_Range')).withColumnRenamed('date','Max_date')
    Max_station = range.join(Max_range, [range.date == Max_range.Max_date, range.range == Max_range.Max_Range]).select('date', 'station','range')
 
    range_station = Max_station.orderBy('date', 'station')
    range_station.explain()
    range_station.write.csv(output, mode = 'overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

# MODE = 'max'
MODE = 'min'

sc = SparkContext(appName="lab1-e1")
sqlContext = SQLContext(sc)

rdd = sc.textFile("BDA/input/temperature-readings.csv")

parts = rdd.map(lambda l: l.split(";"))

temp_readings = parts.map(lambda p: Row(station=int(p[0]), year=int(p[1][0:4]), temp=float(p[3])))

schema_temp_readings = sqlContext.createDataFrame(temp_readings)

schema_temp_readings = schema_temp_readings.filter((schema_temp_readings['year'] >= 1950) & (schema_temp_readings['year'] <= 2014))

if MODE == 'max':
    schema_temp_readings_max = schema_temp_readings.groupBy('year').agg(F.max('temp').alias('temp'))

    schema_temp_result_max = schema_temp_readings_max.join(schema_temp_readings, ['year', 'temp'], 'inner')

    schema_temp_result_max = schema_temp_result_max.select('station', 'year', 'temp').distinct().orderBy(F.desc('temp'))

    schema_temp_result_max.rdd.saveAsTextFile("BDA/output")

elif MODE == 'min':
    schema_temp_readings_min = schema_temp_readings.groupBy('year').agg(F.min('temp').alias('temp'))

    schema_temp_result_min = schema_temp_readings_min.join(schema_temp_readings, ['year', 'temp'], 'inner')

    schema_temp_result_min = schema_temp_result_min.select('station', 'year', 'temp').distinct().orderBy(F.desc('temp'))

    schema_temp_result_min.rdd.saveAsTextFile("BDA/output")
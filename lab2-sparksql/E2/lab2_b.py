from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="lab1-e1")
sqlContext = SQLContext(sc)

rdd = sc.textFile("BDA/input/temperature-readings.csv")

parts = rdd.map(lambda l: l.split(";"))

temp_readings = parts.map(lambda p: Row(station=int(p[0]), year=int(p[1][0:4]), month=int(p[1][5:7]),temp=float(p[3])))

schema_temp_readings = sqlContext.createDataFrame(temp_readings)

schema_temp_readings = schema_temp_readings.filter((schema_temp_readings['year'] >= 1950) & (schema_temp_readings['year'] <= 2014))

schema_temp_readings = schema_temp_readings.filter(schema_temp_readings['temp'] > 10)

schema_temp_readings_count = schema_temp_readings.select('station', 'year', 'month').distinct()

schema_temp_readings_result = schema_temp_readings_count.groupBy('year', 'month').agg(F.count('station').alias('count')).orderBy('count', ascending=False)

schema_temp_readings_result.rdd.saveAsTextFile("BDA/output")


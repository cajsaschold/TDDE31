from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="lab1-e1")
sqlContext = SQLContext(sc)

rdd = sc.textFile("BDA/input/temperature-readings.csv")

parts = rdd.map(lambda l: l.split(";"))

temp_readings = parts.map(lambda p: Row(station=int(p[0]), year=int(p[1][0:4]), month=int(p[1][5:7]),temp=float(p[3])))

schema_temp_readings = sqlContext.createDataFrame(temp_readings)

schema_temp_readings = schema_temp_readings.filter((schema_temp_readings['year'] >= 1960) & (schema_temp_readings['year'] <= 2014))

schema_temp_readings = schema_temp_readings.groupBy(['year', 'month', 'station']).agg(F.avg('temp').alias('avgMonthlyTemperature'))

schema_temp_readings = schema_temp_readings.select('year', 'month', 'station', 'avgMonthlyTemperature').orderBy(F.desc('avgMonthlyTemperature'))

schema_temp_readings.rdd.saveAsTextFile("BDA/output")


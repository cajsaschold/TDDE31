from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="lab1-e1")
sqlContext = SQLContext(sc)

rdd1 = sc.textFile("BDA/input/stations-Ostergotland.csv")
parts1 = rdd1.map(lambda l: l.split(";"))

rdd2 = sc.textFile("BDA/input/precipitation-readings.csv")
parts2 = rdd2.map(lambda l: l.split(";"))

perc_readings = parts2.map(lambda p: Row(station=int(p[0]), year=int(p[1][0:4]), month=int(p[1][5:7]), perc=float(p[3])))
stations_readings = parts1.map(lambda p: Row(station=int(p[0])))

schema_perc_readings = sqlContext.createDataFrame(perc_readings)
schema_stations_readings = sqlContext.createDataFrame(stations_readings)

schema_temp_result = schema_perc_readings.filter((schema_perc_readings['year'] >= 1993) & (schema_perc_readings['year'] <= 2016))

nr_lines = schema_stations_readings.count()

joined_station_perc = schema_temp_result.join(schema_stations_readings, ['station'], 'inner')

joined_station_perc = joined_station_perc.select('year', 'month', 'perc')

schema_temp_result_sum = joined_station_perc.groupBy(['year', 'month']).agg((F.avg('perc')).alias('avgPerc'))

result = schema_temp_result_sum.select('year', 'month', 'avgPerc').orderBy(F.desc('year'), F.desc('month'))

result.rdd.saveAsTextFile("BDA/output")




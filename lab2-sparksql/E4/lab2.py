from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName="lab1-e1")
sqlContext = SQLContext(sc)

rdd1 = sc.textFile("BDA/input/temperature-readings.csv")
parts1 = rdd1.map(lambda l: l.split(";"))

rdd2 = sc.textFile("BDA/input/precipitation-readings.csv")
parts2 = rdd2.map(lambda l: l.split(";"))

temp_readings = parts1.map(lambda p: Row(station=int(p[0]), date=str(p[1]), temp=float(p[3])))
perc_readings = parts2.map(lambda p: Row(station=int(p[0]), date=str(p[1]), perc=float(p[3])))

schema_temp_readings = sqlContext.createDataFrame(temp_readings)
schema_perc_readings = sqlContext.createDataFrame(perc_readings)

temp_readings = schema_temp_readings.groupBy('station').agg(F.max('temp').alias('temp'))
filtered_temp_readings = temp_readings.filter((temp_readings['temp'] >= 25) & (temp_readings['temp'] <= 30))

sum_perc_readings = schema_perc_readings.groupBy('station', 'date').agg(F.sum('perc').alias('sumPerc'))
max_sum_perc_readings = sum_perc_readings.groupBy('station').agg(F.max('sumPerc').alias('maxPerc'))

filtered_perc_readings = max_sum_perc_readings.filter((max_sum_perc_readings['maxPerc'] >= 100) & (max_sum_perc_readings['maxPerc'] <= 200))

result = filtered_temp_readings.join(filtered_perc_readings, 'station')

result = result.select('station', 'temp', 'maxPerc').orderBy(F.desc('station'))

result.write.csv("BDA/output")

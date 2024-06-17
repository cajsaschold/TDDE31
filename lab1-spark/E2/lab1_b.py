#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "lab1-e1")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# ((stationID, year, month), degree)
filtered_columns = lines.map(lambda x: ((int(x[0]), int(x[1][0:4]), int(x[1][5:7])), float(x[3])))

filtered_lines_year = filtered_columns.filter(lambda x: x[0][1]>= 1950 and x[0][1]<=2014)

filtered_lines_degrees = filtered_lines_year.filter(lambda x: x[1] > 10)

distinct_map = filtered_lines_degrees.map(lambda x : ((x[0][0], x[0][1], x[0][2]), 1))

distinct_lines = distinct_map.distinct()

# ((year, month), count)
updated_map = distinct_lines.map(lambda x: ((x[0][1], x[0][2]), 1))

result = updated_map.reduceByKey(lambda x, y: x + y)

temperatures_sorted = result.sortBy(ascending = False, keyfunc=lambda x: x[0][1])

temperatures_sorted.saveAsTextFile("BDA/output")

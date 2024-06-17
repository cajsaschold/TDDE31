#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "lab1-e1")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

filtered_columns = lines.map(lambda x: (int(x[1][0:4]), float(x[3])))

filtered_lines = filtered_columns.filter(lambda x: x[0]>= 1950 and x[0]<=2014)

max_temperatures = filtered_lines.reduceByKey(lambda x, y: x if x >= y else y)

min_temperatures = filtered_lines.reduceByKey(lambda x, y: x if x <= y else y)

temperatures = max_temperatures.join(min_temperatures)

temperatures_sorted = temperatures.sortBy(ascending = False, keyfunc=lambda x: x[1])

temperatures_sorted.saveAsTextFile("BDA/output")

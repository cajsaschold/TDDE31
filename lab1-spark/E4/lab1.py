#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "lab1-e1")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
percipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")

tempetature_lines = temperature_file.map(lambda line: line.split(";"))
percipitation_lines = percipitation_file.map(lambda line: line.split(";"))

tempetature_columns = tempetature_lines.map(lambda x: (int(x[0]), float(x[3])))
percipitation_columns = percipitation_lines.map(lambda x: ((int(x[0]), str(x[1])), float(x[3])))

aggregated_percipitation = percipitation_columns.reduceByKey(lambda x, y: x + y)
summed_percipitation = aggregated_percipitation.map(lambda x: (int(x[0][0]), float(x[1])))

max_temperatures = tempetature_columns.reduceByKey(lambda x, y: x if x >= y else y)
max_percipitation = summed_percipitation.reduceByKey(lambda x, y: x if x >= y else y)

filtered_temperatures = max_temperatures.filter(lambda x: float(x[1]) <= 30 and float(x[1]) >= 25)
filtered_percipitation = max_percipitation.filter(lambda x: float(x[1]) <= 200 and float(x[1]) >= 100)

result = filtered_temperatures.join(filtered_percipitation)

sorted_result = result.sortBy(ascending = False, keyfunc=lambda x: x[0])

sorted_result.saveAsTextFile("BDA/output")

#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "lab1-e1")

stations_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
percipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")

station_lines = stations_file.map(lambda line: line.split(";"))
percipitation_lines = percipitation_file.map(lambda line: line.split(";"))

stations = station_lines.map(lambda x: int(x[0]))
collected_stations = stations.collect()

percipitation_columns = percipitation_lines.map(lambda x: ((int(x[0]), str(x[1][0:7])), float(x[3])))

filtered_percep_stations = percipitation_columns.filter(lambda x: x[0][0] in collected_stations)

filtered_percipitation = filtered_percep_stations.filter(lambda x: int(x[0][1][0:4]) <= 2016 and int(x[0][1][0:4]) >= 1993)

aggregated_percipitation = filtered_percipitation.reduceByKey(lambda x, y: x + y)

percep_map = aggregated_percipitation.map(lambda x: (x[0][1], (x[1], 1)))

percep_count = percep_map.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

percep_avg = percep_count.mapValues(lambda x: (x[0]/x[1], 0))

results = percep_avg.map(lambda x: (x[0], x[1][0]))

sorted_result = results.sortBy(ascending = False, keyfunc=lambda x: x[0][0])
sorted_result.saveAsTextFile("BDA/output")
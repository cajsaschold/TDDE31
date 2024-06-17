#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "lab1-e1")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

filtered_columns = lines.map(lambda x: ((int(x[1][0:4]), int(x[1][5:7]), int(x[1][8:]), int(x[0])), float(x[3])))

filtered_lines = filtered_columns.filter(lambda x: x[0][0]>= 1960 and x[0][0]<=2014)

max_temperatures = filtered_lines.reduceByKey(lambda x, y: x if x >= y else y)

min_temperatures = filtered_lines.reduceByKey(lambda x, y: x if x <= y else y)

temperatures = max_temperatures.join(min_temperatures)

# En min/max tuple per dag
sum_day_max_min = temperatures.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

year_month_station = sum_day_max_min.map(lambda x: ((int(x[0][0]), int(x[0][1]), int(x[0][3])), x[1]))

# En min/max tuple per månad
sum_month_max_min = year_month_station.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

result = sum_month_max_min.map(lambda x: ((int(x[0][0]), int(x[0][1]), int(x[0][2])), float((x[1][0] + x[1][1]) / 62)))

sorted_result = result.sortBy(ascending = False, keyfunc=lambda x: x[0][0])

sorted_result.saveAsTextFile("BDA/output")

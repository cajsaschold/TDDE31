from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from pyspark import SparkContext

sc = SparkContext(appName="lab_kernel")
stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")

date = "2013-07-04" 
h_distance = 150
h_date = 7 
h_time = 150 
lon = 58.4274 
lat = 14.826

times = ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
         "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]

days_in_month = 31
minutes_per_hour = 60

def haversine(lon1, lat1, lon2, lat2):
    """ 
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

# kernel for physical distance
def compute_dist_kern(c_lon, c_lat):
    eukl_dist_km = haversine(lon, lat, c_lon, c_lat)
    kernel = exp(-(eukl_dist_km/h_distance)**2)
    return kernel

# kernel for day distance
def compute_day_kern(c_month, c_day):
    month = int(date[5:7])
    day = int(date[8:10])
    elapsed_days_in_year = month * days_in_month + day
    c_elapsed_days_in_year = c_month * days_in_month + c_day
    day_delta = abs(elapsed_days_in_year - c_elapsed_days_in_year)
    kernel = exp(-(day_delta/h_date)**2)
    return kernel

# kernel for minute distance
def compute_time_kern(time, c_time):
    hour, c_hour = int(time[0:2]), int(c_time[0:2])
    minute, c_minute = int(time[3:5]), int(c_time[3:5])
    elapsed_minutes_in_day = hour * minutes_per_hour + minute
    c_elapsed_minutes_in_day = c_hour * minutes_per_hour + c_minute
    minute_delta = abs(elapsed_minutes_in_day - c_elapsed_minutes_in_day)
    kernel = exp(-(minute_delta/h_time)**2)
    return kernel

# create and broadcast a RDD containing station data to all nodes
station_lines = stations.map(lambda line: line.split(";"))
# (station, (lon, lat))
subset_station_lines = station_lines.map(lambda x: (int(x[0]), (float(x[3]), float(x[4]))))
station_data = subset_station_lines.collectAsMap()
b_station_data = sc.broadcast(station_data)

temp_lines = temps.map(lambda line: line.split(";"))
# ((year, month, day, time), (temp, lon + lat))
subset_temp_lines = temp_lines.map(lambda x: ((int(x[1][0:4]), int(x[1][5:7]), int(x[1][8:10]), x[2]), 
                                              (float(x[3]), b_station_data.value.get(int(x[0])))))
# Filter out posterior years, month and days
f_temp_lines = subset_temp_lines.filter(lambda x: x[0][0] <= int(date[0:4]) and x[0][1] <= int(date[5:7]) and x[0][2] < int(date[8:10]))

# cache the temperature data so that it does not have to be read again
f_temp_lines.cache()

sum_kernel_predictions = []
mult_kernel_predictions = []

for timestamp in times:
    
    # Isolate the hour from the timestamp
    timestamp_hour = int(timestamp[0:2])

    # Filter out posterior hours
    filtered_temp_lines = f_temp_lines.filter(lambda x: int(x[0][3][0:2]) < timestamp_hour)

    #((year, month, day, time), ((distance_kernel, day_kernel, hour_kernel), temp))
    combined_kernel = filtered_temp_lines.map(lambda x: (x[0], ((compute_dist_kern(x[1][1][0], x[1][1][1]), compute_day_kern(x[0][1], x[0][2]), compute_time_kern(timestamp, x[0][3])), x[1][0])))

    # (1, (sum(u)*y, sum(u), mult(u)*y, mult(u)))
    kernels = combined_kernel.map(lambda x: (1, ((x[1][0][0] + x[1][0][1] + x[1][0][2]) * x[1][1], x[1][0][0] + x[1][0][1] + x[1][0][2], (x[1][0][0] * x[1][0][1] * x[1][0][2]) * x[1][1], x[1][0][0] * x[1][0][1] * x[1][0][2])))

    # aggregating the values over all the training points
    kernels = kernels.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3]))
    # compute y(x), (agg_sum(u)*y / agg_sum(u), agg_mult(u)*y / agg_mult(u)) 
    kernel_values = kernels.mapValues(lambda x: (x[0]/x[1], x[2]/x[3]))

    kernel_result = kernel_values.collectAsMap().get(1)

    sum_kernel = kernel_result[0]
    mult_kernel = kernel_result[1]
    sum_kernel_predictions.append((timestamp_hour, sum_kernel))
    mult_kernel_predictions.append((timestamp_hour, mult_kernel))

print(sum_kernel_predictions)
print(mult_kernel_predictions)

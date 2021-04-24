from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext


sc = SparkContext(appName = "kernel test")

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km
h_distance = 1000
h_date = 20
h_time = 4
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2013-11-05"# Up to you
#function to change to timestamp(hours)
def change_to_time(x):
    time_form = datetime.strptime(x, "%H:%M:%S")
    return time_form
#function to change to timestamp(days)
def change_to_datetime(x):
    splitter = x.split('-',3)
    days = datetime(int(splitter[0]),int(splitter[1]),int(splitter[2]))
    return days

#loading the files

temp_file =  sc.textFile("BDA/input/temperature-readings.csv",128)\
                .map(lambda line: line.split(";"))\
                .map(lambda x:(x[0],x[1],x[2],x[3]))

stations_file = sc.textFile("BDA/input/stations.csv",128)\
                   .map(lambda line: line.split(";"))\
                   .map(lambda x:((x[0]),(x[3],x[4])))


#broadcasting the smaller file
station_bc = sc.broadcast(stations_file.collectAsMap())

# Merging stations and temperature rdd
joint_rdd = temp_file.map(lambda x: x + station_bc.value[x[0]][:])\
                     .filter(lambda x:x[1] < date)

#date kenel calculation
date_kernel = joint_rdd.map(lambda x:(x[1]))\
                        .map(lambda x:change_to_datetime(x))\
                        .map(lambda x:change_to_datetime(date)-x)\
                        .map(lambda x:min(365-x.days%365,x.days%365))\
                        .map(lambda x:exp(-1*(x/h_date)**2))

#distance kernel calculation
distance_kernel = joint_rdd.map(lambda x:(x[4],x[5]))\
                                   .map(lambda x:abs(haversine(float(x[1]),float(x[0]),b,a)))\
                                   .map(lambda x:exp(-1*(x/h_distance)**2))
#create a rdd containing temperature
temp_rdd  = joint_rdd.map(lambda x:(x[3]))
#create list to hold the prediction results
pred_results_sum  = []
pred_results_prod =[]

for time in ["00:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    time_est = joint_rdd.map(lambda x:(x[2]))\
                        .map(lambda x:change_to_time(time)-change_to_time(x))\
                        .map(lambda x: min(x.seconds/3600.0, 24-x.seconds/3600))
#time kernel calculation
    time_kernel_2 = time_est.map(lambda x: exp(-1 * (x/h_time)**2))
#zipped values of al the three kernels
    zipped_rdd = date_kernel.zip(distance_kernel.zip(time_kernel_2))
#kernel sum
    kernel_sum = zipped_rdd.map(lambda x:x[0]+x[1][0]+x[1][1])
#kernel product
    kernel_prod = zipped_rdd.map(lambda x:x[0]*x[1][0]*x[1][1])
#temperature prediction using kernel sum
    temp_pred_sum = kernel_sum.zip(temp_rdd)\
                            .map(lambda x:((x[0]*float(x[1])),x[0]))\
                            .reduce(lambda x,y: (x[0]+y[0], x[1]+y[1]))
    est_temp_add = (temp_pred_sum[0]/temp_pred_sum[1])

#temperature prediction using kernel product
    temp_pred_prod = kernel_prod.zip(temp_rdd)\
                            .map(lambda x:((x[0]*float(x[1])),x[0]))\
                            .reduce(lambda x,y: (x[0]+y[0], x[1]+y[1]))
    est_temp_prod = (temp_pred_prod[0]/temp_pred_prod[1])

#collect results for each time-point in a list
    pred_results_sum.append(est_temp_add)
    pred_results_prod.append(est_temp_prod)
#kernel addition results
pred_add = sc.parallelize(pred_results_sum)
pred_add.coalesce(1).saveAsTextFile("BDA/output/sum")
#kernel multiplication results
pred_prod = sc.parallelize(pred_results_prod)
pred_prod.coalesce(1).saveAsTextFile("BDA/output/prod")

# coding=utf-8
#Generting sparkcontext to read from HDFS
from pyspark import SparkContext
sc = SparkContext(appName = "exercise test")




temperature_file = sc.textFile("BDA/input/temperature-readings.csv")

# transform data by splitting each line
lines = temperature_file.map(lambda line: line.split(";"))
"""
Question 1) What are thelowest and highest temperatures measured each year for the period 1950-2014.
Provide the lists sorted in the descending order with respect to the maximum temperature. In
this exercise you will use the temperature-readings.csv file
"""


#Extracting year and temperature as a tuple for the year 1950-2014
year_temperature = lines.map(lambda x:(int(x[1][0:4]),float(x[3])))\
                        .filter(lambda x: int(x[0])>1950 or int(x[0]<2014))
#Extracting the maximum temperature
max_temps = year_temperature.reduceByKey(max)
#Extracting the minimum temperature
min_temps = year_temperature.reduceByKey(min)
#combining rdd of max temperature and min temperature
combine_rdd = max_temps.union(min_temps)\
                        .sortBy(lambda x:x[1],ascending=False)
combine_rdd.coalesce(1).saveAsTextFile("BDA/output/Q1result")

"""
Question 2:Count the number of readings for each month in the period of 1950-2014 which are higher
than 10 degrees. Repeat the exercise, this time taking only distinct readings from each station.
That is, if a station reported a reading above 10 degrees in some month, then it appears only
once in the count for that month.
In this exercise you will use the temperature-readings.csv file.
The output should contain the following information:
Year, month, count
"""


monthly_reading = lines.map(lambda x:(x[1][0:7],float(x[3])))\
                        .filter(lambda x: int(x[0][0:4])>1950 or int(x[0][0:4]<2014))\
                        .filter(lambda x: (x[1] >= 10))\
                        .map(lambda x: (x[0],1))\
                        .reduceByKey(lambda x,y: x+y)\
                        .map(lambda x: (int(x[0][0:4]),int(x[0][5:7]),x[1]))


monthly_reading.coalesce(1).saveAsTextFile("BDA/output/Q2result")


"""
Question 3:  Find the average monthly temperature for each available station in Sweden. Your result
should include average temperature for each station for each month in the period of 1960-
2014. Bear in mind that not every station has the readings for each month in this timeframe.
In this exercise you will use the temperature-readings.csv file.
The output should contain the following information:
Year, month, station number, average monthly temperature.
"""
year_temperature = lines.map(lambda x:(int(x[0]),int(x[1][0:4]),int(x[1][5:7]),float(x[3])))\
                        .filter(lambda x:(x[1]>1960 or x[1]<2014))\
                        .map(lambda x:((str(x[0]) +","+ str(x[1]) +","+ str(x[2])),float(x[3])))\
                        .reduceByKey(lambda x,y: (x + y)/2)\
                        .map(lambda x:(x[0].split(","),float(x[1])))
#                        .map(lambda x:(str(x[0][0:6]),str(x[0][7:11]),str(x[0][12:]),float(x[1])))

year_temperature.coalesce(1).saveAsTextFile("BDA/output/Q3result")


"""
Question 4 :Provide a list of stations with their associated maximum measured temperatures and
maximum measured daily precipitation. Show only those stations where the maximum
temperature is between 25 and 30 degrees and maximum daily precipitation is between 100
mm and 200 mm.
In this exercise you will use the temperature-readings.csv and precipitation-readings.csv
files.
The output should contain the following information:
Station number, maximum measured temperature, maximum daily precipitation
"""


lines = temperature_file.map(lambda line: line.split(";"))
temp_2 = lines.map(lambda x: ((x[0] +","+x[1]),float(x[3])))\
                .filter(lambda x: (x[1] > 25 and x[1] < 30))\
                .reduceByKey(max)
preci_file = sc.textFile("BDA/input/precipitation-readings.csv")
preci_1 = preci_file.map(lambda line: line.split(";"))\
                    .map(lambda x: ((x[0] + "," +  x[1]),float(x[3])))\
                    .filter(lambda x: (x[1] > 100 and x[1] < 200))\
                    .reduceByKey(max)
combine_rdd_2 = temp_2.union(preci_1)\
                    .reduceByKey(lambda x,y: (x,y))\
                    .map(lambda x:(x[0].split(","),float(x[1])))
#                    .map(lambda x:(x[0][0:6],x[0][7:17],x[1]))
combine_rdd_2.coalesce(1).saveAsTextFile("BDA/output/Q4result")

"""
Question 5 :Calculate the average monthly precipitation for the Östergotland region (list of stations is
provided in the separate file) for the period 1993-2016. In order to do this, you will first need to
calculate the total monthly precipitation for each station before calculating the monthly
average (by averaging over stations).
In this exercise you will use the precipitation-readings.csv and stations-Ostergotland.csv
files. HINT (not for the SparkSQL lab): Avoid using joins here! stations-Ostergotland.csv is
small and if distributed will cause a number of unnecessary shuffles when joined with
precipitation RDD. If you distribute precipitation-readings.csv then either repartition your
stations RDD to 1 partition or make use of the collect to acquire a python list and broadcast
function to broadcast the list to all nodes.
The output should contain the following information:
Year, month, average monthly precipitation
"""

station_number = sc.textFile("BDA/input/stations-Ostergotland.csv")\
                     .map(lambda line: line.split(";"))\
                    .map(lambda x: x[0])
station_bc  = sc.broadcast(station_number.collect())

preci_2 = preci_file.map(lambda line: line.split(";"))\
                    .filter(lambda x:x[0] in station_bc.value)\
                    .map(lambda x: ((int(x[0]),x[1][0:7]),float(x[3])))\
                    .reduceByKey(lambda x,y:(x+y)/2)\
                    .map(lambda x:(x[0][0],x[0][1],x[1]))
preci_2.coalesce(1).saveAsTextFile("BDA/output/Q5result")


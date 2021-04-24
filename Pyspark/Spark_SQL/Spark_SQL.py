# coding=utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark_session = SparkSession\
                .builder\
                .appName("myr")\
                .getOrCreate()


##Creation of schema and the initializing the datatypes
schema = StructType([\
    StructField("Station_number", IntegerType(), True),\
    StructField("date", StringType(), True),\
    StructField("time", StringType(), True),\
    StructField("temperature", FloatType(), True),\
    StructField("UK", StringType(), True)])

#creation of data_frame
data_frame = spark_session.read\
    .schema(schema)\
    .option("delimiter", ";")\
    .csv('BDA/input/temperature-readings.csv')\
    .cache()

#Converting to timestamp from string
data_frame = data_frame.select('Station_number',
    to_date(data_frame["date"],"yyyy-MM-dd").alias("date"),"time",'temperature','UK'
)

"""
Question 1 : What are the lowest and highest temperatures measured each year for the period 1950-2014.
Provide the lists sorted in the descending order with respect to the maximum temperature. In
 this exercise you will use the temperature-readings.csv file.
"""

#Sorting max temperaturein descending order
df_max = data_frame.filter((data_frame.date>'1950')&(data_frame.date<'2014'))\
                .select('Station_number','date','temperature')\
                .withColumn("year",year("date"))\
                .orderBy("temperature")\
                .groupBy("year","station_number")\
                .agg({'temperature': 'max'})\
                .sort(desc("max(temperature)"))
#Soring the minimum temperature in descending orderBy
df_min = data_frame.filter((data_frame.date>'1950')&(data_frame.date<'2014'))\
                .select('station_number','date','temperature')\
                .withColumn("year",year("date"))\
                .orderBy("temperature")\
                .groupBy("year","station_number")\
                .agg({'temperature': 'min'})\
                .sort(desc("min(temperature)"))
df_max.rdd.coalesce(1).saveAsTextFile("BDA/output/Q1_maxresult")
df_min.rdd.coalesce(1).saveAsTextFile("BDA/output/Q1_minresult")


"""
Question 2: Count the number of readings for each month in the period of 1950-2014 which are higher
than 10 degrees. Repeat the exercise, this time taking only distinct readings from each station.
That is, if a station reported a reading above 10 degrees in some month, then it appears only
once in the count for that month.
In this exercise you will use the temperature-readings.csv file.
The output should contain the following information:
Year, month, count
"""


df2 = data_frame.filter((data_frame.date>'1950')&(data_frame.date<'2014'))\
                .filter(data_frame['temperature']>10)\
                .select('date','temperature')\
                .withColumn("year",year("date"))\
                .withColumn("month",month("date"))\
                .groupBy('year','month').count()\
                .sort(desc("count"))
df2_2 = data_frame.filter((data_frame.date>'1950')&(data_frame.date<'2014'))\
                .filter(data_frame['temperature']>10)\
                .select('date','temperature')\
                .withColumn("year",year("date"))\
                .withColumn("month",month("date"))\
                .groupBy('year','month','temperature').count()\
                .sort(desc("count"))\
                .groupBy('year','month').count()\
                .sort(desc("count"))
df2.rdd.coalesce(1).saveAsTextFile("BDA/output/Q2_result-1")
df2_2.rdd.coalesce(1).saveAsTextFile("BDA/output/Q2_result-2")



"""
Question 3: year, month, station, avgMonthlyTemperature ORDER BY avgMonthlyTemperature DESC
Find the average monthly temperature for each available station in Sweden. Your result
should include average temperature for each station for each month in the period of 1960-
2014. Bear in mind that not every station has the readings for each month in this timeframe.
In this exercise you will use the temperature-readings.csv file.
The output should contain the following information:
Year, month, station number, average monthly temperature
"""

df3 = data_frame.filter((data_frame.date>'1950')&(data_frame.date<'2014'))\
                .filter(data_frame['temperature']>10)\
                .select("station_number",'date','temperature')\
                .withColumn("year",year("date"))\
                .withColumn("month",month("date"))\
                .groupBy('year','month',"station_number")\
                .agg({'temperature': 'mean'})

df3.rdd.coalesce(1).saveAsTextFile("BDA/output/Q3result")


"""
Question 4: station, maxTemp, maxDailyPrecipitation ORDER BY station DESC
Provide a list of stations with their associated maximum measured temperatures and
maximum measured daily precipitation. Show only those stations where the maximum
temperature is between 25 and 30 degrees and maximum daily precipitation is between 100
mm and 200 mm.
In this exercise you will use the temperature-readings.csv and precipitation-readings.csv
files.
The output should contain the following information:
Station number, maximum measured temperature, maximum daily precipitation
"""
# Precipitation data_frame
prec_df = spark_session.read\
                        .option("delimiter", ";")\
                        .csv('BDA/input/precipitation-readings.csv',header=True, inferSchema=True).toDF("Station_number","date","time","Precipitation","Quality")
#converting to timestamp
prec_df = prec_df.select('Station_number',to_date(prec_df["date"],"yyyy-MM-dd").alias("date"),"time","Precipitation","Quality")

# Temperature data_frame
df_4 = data_frame.filter((data_frame['temperature']>25) & (data_frame['temperature']<30)) \
                .select("Station_number",'date','temperature')\
                .groupBy("Station_number","date")\
                .agg({'temperature': 'max'})



prec_df1 = prec_df.select("Station_number",'date','Precipitation')\
                    .groupBy("Station_number","date")\
                    .agg({'Precipitation': 'max'})
##Inner join of the two dataframes with station number as the common join
df4_2 = df_4.join(prec_df1,(df_4["station_number"]==prec_df1["Station_number"]) & (df_4["date"]==prec_df1["date"]) ,'inner')\
             .select(prec_df1["Station_number"],df_4["max(temperature)"],prec_df1["max(Precipitation)"])

df4_2.rdd.coalesce(1).saveAsTextFile("BDA/output/resultQ4")


"""
Question 5: Calculate the average monthly precipitation for the Östergotland region (list of stations is
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
year, month, avgMonthlyPrecipitation ORDER BY year DESC, month DESC
"""

#stations file
station_df = spark_session.read\
                        .option("delimiter", ";")\
                        .csv('BDA/input/stations-Ostergotland.csv',header=False, inferSchema=True)\
                        .toDF("Station_number","Station_name","Measurement_height", "Latitude", "Longitude" ,"Readings_from","Readings_to","Elevation")\
                        .cache()
prec_df_q5 = prec_df.select("Station_number",'date','Precipitation')\
                    .withColumn("year",year("date"))\
                    .withColumn("month",month("date"))\
                    .groupBy("Station_number","year","month")\
                    .agg({'Precipitation': 'mean'})

df5 = station_df.join(prec_df_q5,station_df["Station_number"]==prec_df_q5["Station_number"])\
                .select(station_df.Station_number,prec_df_q5.year,prec_df_q5.month,prec_df_q5["avg(Precipitation)"])

df5.rdd.coalesce(1).saveAsTextFile("BDA/output/resultQ5")


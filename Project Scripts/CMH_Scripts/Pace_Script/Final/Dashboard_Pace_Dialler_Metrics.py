from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

import os
from pyspark.sql.functions import *
from pyspark.sql import Window
import datetime,time


def Business_day_calculator(today_date):
	business_day = (today_date-datetime.timedelta(days= 1)).strftime("%Y-%m-%d")
	
	return business_day

business_day = Business_day_calculator(today_date=datetime.datetime.today())

#business_day = Business_day_calculator(today_date=datetime.datetime.strptime("2020-02-01","%Y-%m-%d"))


# Reading Pace Dialler Data table from Master table in S3 process-data bucket
pace_dialler_complete=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_pace_dialler/pace_dialler.csv",header =True,inferSchema=True)

pace_dialler_complete=pace_dialler_complete.withColumn('business_date',lit(business_day))

# Formatting the date in date StartTime column
pace_dialler_complete=pace_dialler_complete.withColumn('StartTime',to_date('StartTime',"yyyy-MM-dd").cast('date'))

## Applying filter for current month 
pace_dialler_current_month=pace_dialler_complete.filter('StartTime >= date_add(last_day(business_date - interval 01 months),1) and StartTime<=(last_day(business_date))')

## Creating a new column Day and Month .
pace_dialler_current_month =pace_dialler_current_month.withColumn('Day', dayofmonth('StartTime')).withColumn('Month',month('StartTime'))

## Formatting the data
pace_dialler_current_month=pace_dialler_current_month.withColumn('TalkDuration',substring('TalkDuration',4,5))
pace_dialler_current_month=pace_dialler_current_month.withColumn('TalkDuration',split('TalkDuration',':'))
pace_dialler_current_month=pace_dialler_current_month.withColumn('Talk_duration_Minutes',pace_dialler_current_month['TalkDuration'].getItem(0).cast('Integer'))
pace_dialler_current_month=pace_dialler_current_month.withColumn('Talk_duration_Seconds',pace_dialler_current_month['TalkDuration'].getItem(1).cast('Integer'))
pace_dialler_current_month=pace_dialler_current_month.withColumn('Time_in_secs',(pace_dialler_current_month.Talk_duration_Minutes*60+pace_dialler_current_month.Talk_duration_Seconds))
pace_dialler_current_month.createOrReplaceTempView('pace')

# Selecting Max day
max_day=spark.sql('select dayofmonth (max(StartTime)) as max_day from pace').collect()
max_day=max_day[0].max_day

pace_dialler_current_month=pace_dialler_current_month.withColumn('max_day',lit(max_day))
pace_dialler_current_month.createOrReplaceTempView('pace')

## Genrating PACE Dashboard - Daywise Metrics sheet

pace_daywise=spark.sql('Select Agent,Day, count(Agent) as Attempts, count(case when ContactedStatus="Contacted" then Agent end) as Contacts,round(Avg(case when ContactedStatus="Contacted" then Time_in_secs end),2) as Avg_Call_time_in_secs, round((sum(case when ContactedStatus="Contacted" then Time_in_secs end)/60),2) as Total_Talk_Time_in_Mins,min(StartTime) as Date from pace group by Agent,Day order by Agent,Day')

pace_daywise=pace_daywise.withColumn('run_date',lit(current_date()))
pace_daywise=pace_daywise.withColumn('run_date',pace_daywise.run_date.cast('string'))

pace_daywise=pace_daywise.na.fill(0)

pace_daywise.createOrReplaceTempView('pdw')

pace_daywise_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_daywise/Pace_daywise.csv",header =True,inferSchema=True)
pace_daywise_old=pace_daywise_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
pace_daywise_old=pace_daywise_old.withColumn('run_date',pace_daywise_old.run_date.cast('string'))

pace_daywise_old.createOrReplaceTempView('po')

pace_daywise_final=spark.sql('select * from po union all select * from pdw')

pace_daywise_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_daywise",mode="append",header="true")


## Genrating PACE Dashboard - Effort Metrics sheet


pace_effort_metrics=spark.sql('Select Agent, count(Agent) as No_of_calls,count(case when ContactedStatus="Contacted" then Agent end) as No_of_calls_handled,count(case when ContactedStatus="Contacted" and Time_in_secs >= 60 then Agent end) as Handled_Calls_1_mins,count(distinct(Day)) as No_of_working_days,round(count(Agent)/count(distinct(Day)),2) as No_of_calls_per_Day,round(count(case when ContactedStatus="Contacted" then Agent end)/count(distinct(Day)),2) as No_of_calls_Handled_per_day,round(Avg(case when ContactedStatus="Contacted" then Time_in_secs end),2) as Avg_handled_call_duration,round(((sum(case when ContactedStatus="Contacted" then Time_in_secs end)/count(distinct(Day)))/60),2) as Daily_call_Duration_in_mins,min(StartTime) as Min_Date,max(StartTime) as Max_Date from pace group by Agent order by Agent')


pace_summ= spark.sql('select sum(Time_in_secs) as Sum_time_in_secs, avg(Time_in_secs) as Avg_Time_in_secs from pace where ContactedStatus="Contacted"')

Sum_time_in_secs = pace_summ.collect()[0]['Sum_time_in_secs']

Avg_Time_in_secs= pace_summ.collect()[0]['Avg_Time_in_secs']



pace_effort_metrics=pace_effort_metrics.withColumn('Sum_time_in_secs',lit(Sum_time_in_secs)).withColumn('Avg_Time_in_secs',lit(Avg_Time_in_secs)).withColumn('run_date',lit(current_date()))

pace_effort_metrics=pace_effort_metrics.withColumn('run_date',pace_effort_metrics.run_date.cast('string'))

pace_effort_metrics=pace_effort_metrics.na.fill(0)

pace_effort_metrics.createOrReplaceTempView('pem')

pace_effort_metrics_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_effort_metrics/Pace_effort_metrics.csv",header =True,inferSchema=True)
pace_effort_metrics_old=pace_effort_metrics_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

pace_effort_metrics_old=pace_effort_metrics_old.withColumn('run_date',pace_effort_metrics_old.run_date.cast('string'))

pace_effort_metrics_old.createOrReplaceTempView('peo')

pace_effort_metrics_final=spark.sql('select * from peo union all select * from pem')

pace_effort_metrics_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_effort_metrics",mode="append",header="true")

## Genrating PACE Dashboard - Daily Report Card

pace_daily_report=spark.sql('Select Agent, round(count(case when Day= max_day then Agent end),2) as As_on_day_attempt,round(count(Agent)/count(distinct(Day)),2) as Avg_attempts,count(case when Day= max_day and ContactedStatus="Contacted" then Agent end) as As_on_day_contact,round((count(case when ContactedStatus="Contacted" then Agent end))/count(distinct(Day)),2) as Avg_call_contacted,round((Avg(case when ContactedStatus="Contacted" and Day= max_day then Time_in_secs end)),2) as As_on_call_time_in_sec,round((Avg(case when ContactedStatus="Contacted" then Time_in_secs end)),2) as Avg_call_Time_secs,round((sum(case when ContactedStatus="Contacted" and Day= max_day then Time_in_secs end))/60,2) as Total_Time_mins,round(((sum(case when ContactedStatus="Contacted" then Time_in_secs end)/count(distinct(Day)))/60),2) as Avg_Total_talktime_mins,min(StartTime) as Min_Date,max(StartTime) as Max_Date from pace group by Agent order by Agent')


pace_daily_report=pace_daily_report.withColumn('run_date',lit(current_date()))

pace_daily_report=pace_daily_report.withColumn('run_date',pace_daily_report.run_date.cast('string'))

pace_daily_report=pace_daily_report.na.fill(0)

pace_daily_report.createOrReplaceTempView('pdr')

pace_daily_report_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_daily_report/Pace_daily_report.csv",header =True,inferSchema=True)
pace_daily_report_old=pace_daily_report_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

pace_daily_report_old=pace_daily_report_old.withColumn('run_date',pace_daily_report_old.run_date.cast('string'))

pace_daily_report_old.createOrReplaceTempView('pdro')

pace_daily_report_final=spark.sql('select * from pdro union all select * from pdr')

pace_daily_report_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_daily_report",mode="append",header="true")



master_table_Pace = spark.sql('select "pace_dialler" as Table_name, max(StartTime) as last_modified, current_date() as run_date from pace')

master_table_Pace=master_table_Pace.withColumn('last_modified',master_table_Pace.last_modified.cast('string')).withColumn('run_date',master_table_Pace.run_date.cast('string'))
master_table_Pace.createOrReplaceTempView('pt')

master_table_Pace_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/Pace_dialler/Table_details/table_details.csv",header =True,inferSchema=True)

master_table_Pace_old=master_table_Pace_old.withColumn('last_modified',to_date('last_modified','yyyy-MM-dd')).withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

master_table_Pace_old=master_table_Pace_old.withColumn('last_modified',master_table_Pace_old.last_modified.cast('string')).withColumn('run_date',master_table_Pace_old.run_date.cast('string'))
master_table_Pace_old.createOrReplaceTempView('pto')

master_table_Pace_final = spark.sql('select * from pto union all select * from pt')

master_table_Pace_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Pace_dialler/Table_details",mode="append",header="true")

## Importing the System libraries
time.sleep(30)
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
time.sleep(10)

# Deleting the previously present intermediate file
cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_daywise/Pace_daywise.csv"
os.system(cmd1)
time.sleep(10)
cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_effort_metrics/Pace_effort_metrics.csv"
os.system(cmd2)
time.sleep(10)
cmd3="sudo aws s3 rm s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_daily_report/Pace_daily_report.csv"
os.system(cmd3)
time.sleep(10)
cmd4="sudo aws s3 rm s3://cmh-process-data/intermediate_files/Pace_dialler/Table_details/table_details.csv"
os.system(cmd4)
time.sleep(10)


# Setting up S3 process-data Bucket path
fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
time.sleep(10)

## setting up the file path for the new file generated.

file_path_pace_daywise = "s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_daywise/"
time.sleep(10)
file_path_pace_effort_metrics= "s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_effort_metrics/"
time.sleep(10)
file_path_pace_daily_report = "s3://cmh-process-data/intermediate_files/Pace_dialler/Pace_daily_report/"
time.sleep(10)
file_path_Table_details = "s3://cmh-process-data/intermediate_files/Pace_dialler/Table_details/"
time.sleep(10)
created_file_path_pace_daywise = fs.globStatus(Path(file_path_pace_daywise + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_pace_effort_metrics = fs.globStatus(Path(file_path_pace_effort_metrics + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_pace_daily_report = fs.globStatus(Path(file_path_pace_daily_report + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)


fs.rename(created_file_path_pace_daywise,Path(file_path_pace_daywise + "Pace_daywise.csv"))
time.sleep(10)
fs.rename(created_file_path_pace_effort_metrics,Path(file_path_pace_effort_metrics + "Pace_effort_metrics.csv"))
time.sleep(10)
fs.rename(created_file_path_pace_daily_report,Path(file_path_pace_daily_report + "Pace_daily_report.csv"))
time.sleep(10)
fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))








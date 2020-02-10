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


# Reading NHM Dialler Data table from Master table in S3 process-data bucket
nhm_dialler_complete=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_nhm_dialler/nhm_dialler.csv",header =True,inferSchema=True)

nhm_dialler_complete=nhm_dialler_complete.withColumn('business_date',lit(business_day))


nhm_dialler_current_month=nhm_dialler_complete.filter('Date >= date_add(last_day(business_date - interval 01 months),1) and Date<=(last_day(business_date))')
nhm_dialler_current_month.createOrReplaceTempView('nhm')

max_day=spark.sql('select dayofmonth (max(Date)) as max_day from nhm').collect()
max_day=max_day[0].max_day

nhm_dialler_current_month=nhm_dialler_current_month.withColumn('max_day',lit(max_day))

nhm_dialler_current_month.createOrReplaceTempView('nhm')

### nhm Daywise ###


nhm_daywise=spark.sql('Select Agent,Day, count(Agent) as Attempted, count(case when TerminationStatus="Contacted" then Agent end) as Contacted,round(Avg(case when TerminationStatus="Contacted" then Time_in_Seconds end),2) as Avg_Call_Time_in_Seconds, round((sum(case when TerminationStatus="Contacted" then Time_in_Seconds end)/60),2) as Total_Talk_Time_in_Mins,min(Date) as Date from nhm group by Agent,Day order by Agent,Day')

nhm_daywise=nhm_daywise.withColumn('run_date',lit(current_date()))
#nhm_daywise=nhm_daywise.withColumn('run_date',nhm_daywise.run_date.cast('string'))

nhm_daywise.createOrReplaceTempView('pdw')

nhm_daywise_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_daywise/nhm_daywise.csv",header =True,inferSchema=True)

nhm_daywise_old=nhm_daywise_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

#nhm_daywise_old=nhm_daywise_old.withColumn('run_date',nhm_daywise_old.run_date.cast('string'))
nhm_daywise_old.createOrReplaceTempView('po')

nhm_daywise_final=spark.sql('select * from po union all select * from pdw')

nhm_daywise_final=nhm_daywise_final.withColumn('run_date',nhm_daywise_final.run_date.cast('string'))


nhm_daywise_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_daywise",mode="append",header="true")


### nhm -Effort Metrics

nhm_effort_metrics=spark.sql('Select Agent, count(Agent) as No_of_calls,count(case when TerminationStatus="Contacted" then Agent end) as No_of_calls_handled,count(case when TerminationStatus="Contacted" and Time_in_Seconds >= 60 then Agent end) as Handled_Calls_1_mins,count(distinct(Day)) as No_of_working_days,round(count(Agent)/count(distinct(Day)),2) as No_of_calls_per_Day,round(count(case when TerminationStatus="Contacted" then Agent end)/count(distinct(Day)),2) as No_of_calls_Handled_per_day,round(Avg(case when TerminationStatus="Contacted" then Time_in_Seconds end),2) as Avg_handled_call_duration,round(((sum(case when TerminationStatus="Contacted" then Time_in_Seconds end)/count(distinct(Day)))/60),2) as Daily_call_Duration_in_mins,min(Date) as Min_Date,max(Date) as Max_Date from nhm group by Agent order by Agent')

nhm_summ= spark.sql('select sum(Time_in_Seconds) as Sum_Time_in_Seconds, avg(Time_in_Seconds) as Avg_Time_in_Seconds from nhm where TerminationStatus="Contacted"')

Sum_Time_in_Seconds = nhm_summ.collect()[0]['Sum_Time_in_Seconds']

Avg_Time_in_Seconds= nhm_summ.collect()[0]['Avg_Time_in_Seconds']


nhm_effort_metrics=nhm_effort_metrics.withColumn('Sum_Time_in_Seconds',lit(Sum_Time_in_Seconds)).withColumn('Avg_Time_in_Seconds',lit(Avg_Time_in_Seconds)).withColumn('run_date',lit(current_date()))

#nhm_effort_metrics=nhm_effort_metrics.withColumn('run_date',nhm_effort_metrics.run_date.cast('string'))

nhm_effort_metrics.createOrReplaceTempView('pem')

nhm_effort_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_effort_metrics/nhm_effort_metrics.csv",header =True,inferSchema=True)


#nhm_effort_old=nhm_effort_old.withColumn('run_date',to_date('run_date','dd-MM-yyyy'))
nhm_effort_old=nhm_effort_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

#nhm_effort_old=nhm_effort_old.withColumn('run_date',nhm_effort_old.run_date.cast('string'))

nhm_effort_old.createOrReplaceTempView('peo')

nhm_effort_metrics_final=spark.sql('select * from peo union all select * from pem')

nhm_effort_metrics_final=nhm_effort_metrics_final.withColumn('run_date',nhm_effort_metrics_final.run_date.cast('string'))

nhm_effort_metrics_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_effort_metrics",mode="append",header="true")

#### nhm Daily Report Card ##

nhm_daily_report=spark.sql('Select Agent, round(count(case when Day= max_day then Agent end),2) as As_on_day_attempt,round(count(Agent)/count(distinct(Day)),2) as Avg_attempts,count(case when Day= max_day and TerminationStatus="Contacted" then Agent end) as As_on_day_contact,round((count(case when TerminationStatus="Contacted" then Agent end))/count(distinct(Day)),2) as Avg_call_contacted,round((Avg(case when TerminationStatus="Contacted" and Day= max_day then Time_in_Seconds end)),2) as As_on_call_time_in_sec,round((Avg(case when TerminationStatus="Contacted" then Time_in_Seconds end)),2) as Avg_call_Time_secs,round((sum(case when TerminationStatus="Contacted" and Day= max_day then Time_in_Seconds end))/60,2) as Total_Time_mins,round(((sum(case when TerminationStatus="Contacted" then Time_in_Seconds end)/count(distinct(Day)))/60),2) as Avg_Total_talktime_mins,min(Date) as Min_Date,max(Date) as Max_Date from nhm group by Agent order by Agent')

nhm_daily_report=nhm_daily_report.withColumn('run_date',lit(current_date()))

#nhm_daily_report=nhm_daily_report.withColumn('run_date',nhm_daily_report.run_date.cast('string'))
nhm_daily_report.createOrReplaceTempView('pdr')

nhm_daily_report_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_daily_report/nhm_daily_report.csv",header =True,inferSchema=True)

nhm_daily_report_old=nhm_daily_report_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

#nhm_daily_report_old=nhm_daily_report_old.withColumn('run_date',nhm_daily_report_old.run_date.cast('string'))


nhm_daily_report_old.createOrReplaceTempView('pdro')

nhm_daily_report_final=spark.sql('select * from pdro union all select * from pdr')

nhm_daily_report_final=nhm_daily_report_final.withColumn('run_date',nhm_daily_report_final.run_date.cast('string'))

nhm_daily_report_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_daily_report",mode="append",header="true")



nhm_table = spark.sql('select "nhm_dialler" as Table_name, max(to_date(Date,"yyyy-MM-dd")) as last_modified, current_date() as run_date from nhm')
nhm_table.createOrReplaceTempView('pt')

nhm_table_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/NHM_dialler/Table_details/table_details.csv",header =True,inferSchema=True)


#nhm_table_old=nhm_table_old.withColumn('last_modified',to_date('last_modified','dd-MM-yyyy')).withColumn('run_date',to_date('run_date','dd-MM-yyyy'))
nhm_table_old=nhm_table_old.withColumn('last_modified',to_date('last_modified','yyyy-MM-dd')).withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
#nhm_table_old=nhm_table_old.withColumn('last_modified',nhm_table_old.last_modified.cast('string')).withColumn('run_date',nhm_table_old.run_date.cast('string'))

nhm_table_old.createOrReplaceTempView('pto')

nhm_table_final = spark.sql('select * from pto union all select * from pt')
nhm_table_final=nhm_table_final.withColumn('last_modified',nhm_table_final.last_modified.cast('string')).withColumn('run_date',nhm_table_final.run_date.cast('string'))

nhm_table_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/NHM_dialler/Table_details",mode="append",header="true")

time.sleep(30)

# Importing the System Libraries
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)

# Deleting the Previously present Files
cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_daywise/nhm_daywise.csv"
os.system(cmd1)
time.sleep(10)
cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_effort_metrics/nhm_effort_metrics.csv"
os.system(cmd2)
time.sleep(10)
cmd3="sudo aws s3 rm s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_daily_report/nhm_daily_report.csv"
os.system(cmd3)
time.sleep(10)
cmd4="sudo aws s3 rm s3://cmh-process-data/intermediate_files/NHM_dialler/Table_details/table_details.csv"
os.system(cmd4)
time.sleep(10)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
time.sleep(10)
file_path_nhm_daywise = "s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_daywise/"
time.sleep(10)
file_path_nhm_effort_metrics= "s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_effort_metrics/"
time.sleep(10)
file_path_nhm_daily_report = "s3://cmh-process-data/intermediate_files/NHM_dialler/nhm_daily_report/"
time.sleep(10)
file_path_Table_details = "s3://cmh-process-data/intermediate_files/NHM_dialler/Table_details/"
time.sleep(10)


created_file_path_nhm_daywise = fs.globStatus(Path(file_path_nhm_daywise + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_nhm_effort_metrics = fs.globStatus(Path(file_path_nhm_effort_metrics + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_nhm_daily_report = fs.globStatus(Path(file_path_nhm_daily_report + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)

# Renaming the File Names

fs.rename(created_file_path_nhm_daywise,Path(file_path_nhm_daywise + "nhm_daywise.csv"))
time.sleep(10)
fs.rename(created_file_path_nhm_effort_metrics,Path(file_path_nhm_effort_metrics + "nhm_effort_metrics.csv"))
time.sleep(10)
fs.rename(created_file_path_nhm_daily_report,Path(file_path_nhm_daily_report + "nhm_daily_report.csv"))
time.sleep(10)
fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))

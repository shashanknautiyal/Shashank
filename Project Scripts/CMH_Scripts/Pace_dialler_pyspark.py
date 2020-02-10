
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window


# Latest

pace=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/pace-dialer/pace-dialer_Till 22072019.csv",header =True,inferSchema=True)

pace=pace.withColumn('StartTime',to_date('StartTime',"yyyy-MM-dd").cast('date'))

pace =pace.withColumn('Day', dayofmonth('StartTime')).withColumn('Month',month('StartTime'))


pace=pace.withColumn('TalkDuration',substring('TalkDuration',1,5))
pace=pace.withColumn('TalkDuration',split('TalkDuration',':'))

pace=pace.withColumn('Talk_duration_Minutes',pace['TalkDuration'].getItem(0).cast('Integer'))

pace=pace.withColumn('Talk_duration_Seconds',pace['TalkDuration'].getItem(1).cast('Integer'))


pace=pace.withColumn('Time_in_secs',(pace.Talk_duration_Minutes*60+pace.Talk_duration_Seconds))


#pace=pace.drop('TalkDuration')
pace.createOrReplaceTempView('pace')

#pace.repartition(1).write.csv("D:\\final_data\pace_cleaned.csv",mode="append",header="true")

### Pace Daywise ###


pace_daywise=spark.sql('Select Agent,Day, count(Agent) as Attempts, count(case when ContactedStatus="Contacted" then Agent end) as Contacts,round(Avg(case when ContactedStatus="Contacted" then Time_in_secs end),2) as Avg_Call_time_in_secs, round((sum(case when ContactedStatus="Contacted" then Time_in_secs end)/60),2) as Total_Talk_Time_in_Mins,min(StartTime) as Date from pace group by Agent,Day order by Agent,Day')


pace_daywise.repartition(1).write.csv("D:\\final_data\pace_daywise.csv",mode="append",header="true")



### Pace -Effort Metrics


pace_effort_metrics=spark.sql('Select Agent, count(Agent) as No_of_calls,count(case when ContactedStatus="Contacted" then Agent end) as No_of_calls_handled,count(case when ContactedStatus="Contacted" and Time_in_secs >= 60 then Agent end) as Handled_Calls_1_mins,count(distinct(Day)) as No_of_working_days,round(count(Agent)/count(distinct(Day)),2) as No_of_calls_per_Day,round(count(case when ContactedStatus="Contacted" then Agent end)/count(distinct(Day)),2) as No_of_calls_Handled_per_day,round(Avg(case when ContactedStatus="Contacted" then Time_in_secs end),2) as Avg_handled_call_duration,round(((sum(case when ContactedStatus="Contacted" then Time_in_secs end)/count(distinct(Day)))/60),2) as Daily_call_Duration_in_mins,min(StartTime) as Min_Date,max(StartTime) as Max_Date from pace group by Agent order by Agent')


pace_summ= spark.sql('select sum(Time_in_secs) as Sum_time_in_secs, avg(Time_in_secs) as Avg_Time_in_secs from pace where ContactedStatus="Contacted"')

Sum_time_in_secs = pace_summ.collect()[0]['Sum_time_in_secs']

Avg_Time_in_secs= pace_summ.collect()[0]['Avg_Time_in_secs']


pace_effort_metrics=pace_effort_metrics.withColumn('Sum_time_in_secs',lit(Sum_time_in_secs)).withColumn('Avg_Time_in_secs',lit(Avg_Time_in_secs))

pace_effort_metrics.repartition(1).write.csv("D:\\final_data\pace_effort_metrics.csv",mode="append",header="true")




#### pace Daily Report Card ##

pace_daily_report=spark.sql('Select Agent, round(count(case when Day= 28 then Agent end),2) as As_on_day_attempt,round(count(Agent)/count(distinct(Day)),2) as Avg_attempts,count(case when Day= 28 and ContactedStatus="Contacted" then Agent end) as As_on_day_contact,round((count(case when ContactedStatus="Contacted" then Agent end))/count(distinct(Day)),2) as Avg_call_contacted,round((Avg(case when ContactedStatus="Contacted" and Day= 28 then Time_in_secs end)),2) as As_on_call_time_in_sec,round((Avg(case when ContactedStatus="Contacted" then Time_in_secs end)),2) as Avg_call_Time_secs,round((sum(case when ContactedStatus="Contacted" and Day= 28 then Time_in_secs end))/60,2) as Total_Time_mins,round(((sum(case when ContactedStatus="Contacted" then Time_in_secs end)/count(distinct(Day)))/60),2) as Avg_Total_talktime_mins,min(StartTime) as Min_Date,max(StartTime) as Max_Date from pace group by Agent order by Agent')

pace_daily_report.repartition(1).write.csv("D:\\final_data\pace_daily_report.csv",mode="append",header="true")


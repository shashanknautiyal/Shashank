from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

import os
from pyspark.sql.functions import *
from pyspark.sql import Window
import datetime,time


#system_date_minus1 = (datetime.datetime.today()-datetime.timedelta(days= 1)).strftime("%Y-%m-%d")

sc = SparkContext()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

branch_full =spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_branch_dialler/branch_dialler.csv",header=True,inferSchema=True)

for col in branch_full.columns:
	branch_full=branch_full.withColumnRenamed(col,col.lower())

branch_full=branch_full.withColumn('dateofcalling',to_date('dateofcalling','yyyy-MM-dd')).withColumn('contract_id',branch_full.contract_id.cast('Integer'))

#branch= branch_full.filter('DateofCalling>= date_add(last_day(now() - interval 02 month),1) and DateofCalling <= last_day(now() - interval 01 month)')

branch= branch_full.filter('DateofCalling>= date_add(last_day(now() - interval 01 month),1) and DateofCalling <= last_day(now())')

branch_1=branch.select('contract_id','dateofcalling','contactstatus','control_location')
branch_1=branch_1.withColumn('control_location',lower(branch_1.control_location))

branch_1=branch_1.withColumn('control_location',when(branch_1.control_location =="delhi pitampura","delhi - pitampura").otherwise(branch_1.control_location))
branch_1=branch_1.withColumn('control_location',regexp_replace('control_location',' ',''))

branch_1=branch_1.withColumn('day',dayofmonth('dateofcalling')).withColumn('contactstatus',trim(branch_1.contactstatus))
branch_1.createOrReplaceTempView('b')



## Field Visit

field_visit_full =spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_field_visit/field_visit.csv",header=True,inferSchema=True)

#for col in field_visit_full.columns:
	#field_visit_full=field_visit_full.withColumnRenamed(col,col.lower())

field_visit=field_visit_full.withColumn('InteractionDate',to_date('InteractionDate','yyyy-MM-dd')).withColumn('MemberID',field_visit_full.MemberID.cast('Integer'))

field_visit= field_visit_full.filter('InteractionDate>= date_add(last_day(now() - interval 01 month),1) and InteractionDate <= last_day(now())')

field1 = field_visit.withColumn('Executive',substring('ExecutiveName',-2,2))
field1 = field1.withColumn("Executive_Type",when(field1.Executive == "PE","PE").when(field1.Executive == "CE","CE").otherwise("-NA"))
field1 = field1.withColumn("Executive_App",when(field1.Executive_Type == "-NA","-NA").otherwise(""))

field1 = field1.withColumn('ExecutiveName2',concat(field1.ExecutiveName,lit(''),field1.Executive_App))  
field1 = field1.withColumn('name',split('ExecutiveName2','-'))
field1 = field1.withColumn('Exe_Name',field1['name'].getItem(0))
field1 = field1.withColumn('branch',field1['name'].getItem(1))
field1 = field1.withColumn('branch1',field1['name'].getItem(2))
field1 = field1.drop('name')
field1.createOrReplaceTempView('field1')
field2 = sqlContext.sql('select *,case when branch1 in ("CE","PE","NA") then " " else branch1 end as branch2 from field1')
field1.createOrReplaceTempView('field1')
field3 = field2.withColumn('branchfinal',concat(field2.branch,lit('_'),field2.branch2))

field3.createOrReplaceTempView('f3')

mapping = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal in ("LCKNW_ ","LCKN_ ") then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal = "PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from f3')

mapping_select=mapping.select('MemberID','Exe_Name','InteractionDate','ControlLocation')

mapping_select=mapping_select.withColumn('ControlLocation',lower(mapping_select.ControlLocation)).withColumn('Exe_Name',lower(mapping_select.Exe_Name))

mapping_select=mapping_select.withColumn('ControlLocation',regexp_replace('ControlLocation',' ','')).withColumn('InteractionDate',to_date('InteractionDate','yyyy-MM-dd'))

mapping_select.createOrReplaceTempView('ms')

## Exec lookup

emi_dialler = spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_Emi_dialler/emi_dialler.csv",header =True,inferSchema=True)

## T & E : 26-12-2019

emi_dialler = emi_dialler.withColumn('last_modified_date',to_date('last_modified_date','yyyy-MM-dd'))

emi_dialler = emi_dialler.filter('last_modified_date >= date_add(last_day(now() - interval 01 months),1) and last_modified_date <= (last_day(now()))') 

##

emi_dialler = emi_dialler.select('AgentName','BranchLocation')

emi_dialler = emi_dialler.dropDuplicates()

exec_lookup = emi_dialler

exec_lookup = exec_lookup.withColumnRenamed('AgentName','Executive').withColumnRenamed('BranchLocation','Location')

exec_lookup = exec_lookup.withColumn('Location',rtrim(exec_lookup.Location))
exec_lookup = exec_lookup.withColumn('Executive',rtrim(exec_lookup.Executive))
exec_lookup = exec_lookup.withColumn('Location',ltrim(exec_lookup.Location))
exec_lookup = exec_lookup.withColumn('Executive',rtrim(exec_lookup.Executive))
exec_lookup = exec_lookup.withColumn("Executive",lower(exec_lookup.Executive)).withColumn("Location",lower(exec_lookup.Location))

exec_lookup = exec_lookup.withColumn('Executive', regexp_replace('Executive', '  ', ' '))

exec_lookup = exec_lookup.withColumn('Location',split('Location','-'))

exec_lookup = exec_lookup.withColumn('Location1',exec_lookup['Location'].getItem(0))

exec_lookup = exec_lookup.drop('Location')

exec_lookup = exec_lookup.withColumnRenamed('Location1','Location')

exec_lookup.createOrReplaceTempView('exe')

exec_lookup = spark.sql('select *, case when Location in ("vashi","andheri") then "mumbai" else Location end as Location2 from exe')

exec_lookup = exec_lookup.drop('Location')

exec_lookup = exec_lookup.withColumnRenamed('Location2','Location')

look_up_data = spark.sql('select ms.*,exe.Location as location_via_executive from ms left join exe on ms.Exe_Name=exe.Executive')
look_up_data= look_up_data.withColumn('Day', dayofmonth('InteractionDate'))
look_up_data.createOrReplaceTempView('lud')
max_day=spark.sql('select dayofmonth (max(InteractionDate)) as max_day from lud').collect()
max_day=max_day[0].max_day

look_up_data=look_up_data.withColumn('max_day',lit(max_day))

look_up_data.createOrReplaceTempView('lud')

# test 

#test= spark.sql('select *, case when ControlLocation="delhi-pitampura" then "delhi-pitampura" when ControlLocation="mumbai-vashi" then "mumbai-vashi" end as location_via_executive_test from lud')

#test.createOrReplaceTempView('test')

# change location_via_executive_test to location_via_executive and temp table will lud

lookup_summ = spark.sql('select location_via_executive, count(case when Day = max_day and ControlLocation = location_via_executive then MemberID end) as no_of_visits_today,count(case when ControlLocation = location_via_executive then MemberID end) as no_of_visits_mtd from lud group by location_via_executive')


lookup_summ.createOrReplaceTempView('ls')

## Branch Benchmarks
## Field App required for 


branch_benchmarks = spark.sql('select control_location, round((count(case when contactstatus= "contactable" then contract_id end)/count(contract_id))*100,2) as mtd from b group by control_location')

branch_benchmarks.createOrReplaceTempView('bb')


# change location_via_executive_test to location_via_executive in live

branch_benchmarks_final=spark.sql('select bb.control_location,bb.mtd,ls.no_of_visits_today,ls.no_of_visits_mtd from bb left join ls on bb.control_location = ls.location_via_executive')

branch_benchmarks_final=branch_benchmarks_final.na.fill(0)

#branch_benchmarks_final=branch_benchmarks_final.withColumn('run_date',lit("2019-09-16"))

branch_benchmarks_final=branch_benchmarks_final.withColumn('run_date',lit(current_date()))

#branch_benchmarks_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Branch_benchmark",mode="append",header="true")

branch_benchmarks_final.createOrReplaceTempView('bbf')

branch_benchmarks_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Branch_benchmark/branch_benchmark.csv",header =True,inferSchema=True)

branch_benchmarks_old.createOrReplaceTempView('bbo')

branch_benchmarks_final_2=spark.sql('select * from bbo union all select * from bbf')

branch_benchmarks_final_2.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Branch_benchmark",mode="append",header="true")



## Pace Dialler


pace_full=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_pace_dialler/pace_dialler.csv",header =True,inferSchema=True)
pace_full=pace_full.withColumn('StartTime',to_date('StartTime',"yyyy-MM-dd").cast('date'))
#pace=pace_full.filter('Date >= date_add(last_day(now() - interval 03 months),1) and Date<=(last_day(now()-interval 02 months))')


pace=pace_full.filter('StartTime >= date_add(last_day(now() - interval 01 months),1) and StartTime<=(last_day(now()))')

pace =pace.withColumn('Day', dayofmonth('StartTime')).withColumn('Month',month('StartTime'))
pace=pace.withColumn('TalkDuration',substring('TalkDuration',4,5))
pace=pace.withColumn('TalkDuration',split('TalkDuration',':'))
pace=pace.withColumn('Talk_duration_Minutes',pace['TalkDuration'].getItem(0).cast('Integer'))
pace=pace.withColumn('Talk_duration_Seconds',pace['TalkDuration'].getItem(1).cast('Integer'))
pace=pace.withColumn('Time_in_secs',(pace.Talk_duration_Minutes*60+pace.Talk_duration_Seconds))
pace=pace.withColumn('Time_in_mins',round((pace.Time_in_secs/60),2))

pace.createOrReplaceTempView('pace')
max_day=spark.sql('select dayofmonth (max(StartTime)) as max_day from pace').collect()
max_day=max_day[0].max_day

pace=pace.withColumn('max_day',lit(max_day))
pace.createOrReplaceTempView('pace')


## gets the daily report Status

pace_daily_report=spark.sql('Select Agent, round(count(case when Day= max_day then Agent end),2) as As_on_day_attempt,round(count(Agent)/count(distinct(Day)),2) as Avg_attempts,count(case when Day= max_day and ContactedStatus="Contacted" then Agent end) as As_on_day_contact,round((count(case when ContactedStatus="Contacted" then Agent end))/count(distinct(Day)),2) as Avg_call_contacted,round((Avg(case when ContactedStatus="Contacted" and Day= max_day then Time_in_secs end)),2) as As_on_call_time_in_sec,round((Avg(case when ContactedStatus="Contacted" then Time_in_secs end)),2) as Avg_call_Time_secs,round((sum(case when ContactedStatus="Contacted" and Day= max_day then Time_in_secs end))/60,2) as Total_Time_mins,round(((sum(case when ContactedStatus="Contacted" then Time_in_secs end)/count(distinct(Day)))/60),2) as Avg_Total_talktime_mins,sum(case when Day= max_day then Time_in_secs end) as talk_duration_per_exec_secs,round(sum(case when Day= max_day then Time_in_mins end),2) as talk_duration_per_exec_mins,min(StartTime) as Min_Date,max(StartTime) as Max_Date from pace group by Agent order by Agent')

pace_daily_report.createOrReplaceTempView('pdr')


## pace MTD

pace_daily_mtd=spark.sql('Select Agent, round(count (Agent),2) as attempt_mtd, round(count(case when ContactedStatus="Contacted" then Agent end),2) as contact_mtd,round(sum(Time_in_secs),2) as talk_duration_per_exec_secs,round(sum(Time_in_mins),2) as talk_duration_per_exec_mins,count(distinct(Day)) as no_of_working_days, min(StartTime) as Min_Date,max(StartTime) as Max_Date from pace group by Agent order by Agent')

pace_daily_mtd=pace_daily_mtd.withColumn('mtd_talktime_mins',round((pace_daily_mtd.talk_duration_per_exec_mins/pace_daily_mtd.no_of_working_days),2))

pace_daily_mtd.createOrReplaceTempView('pdm')

	

## Take sum of  from daily report card

pace_integrated_1 = spark.sql('select "pace" as Channel,round(sum(As_on_day_attempt)/count(distinct(Agent)),2) as daily_attempt_per_executive_today,round(sum(As_on_day_contact)/count(distinct(Agent)),2) as daily_contacted_per_executive_today,round(sum(talk_duration_per_exec_secs)/sum(As_on_day_contact),2) as daily_average_call_time_today,round((sum(talk_duration_per_exec_secs)/count(distinct(Agent)))/60,2) as daily_talktime_per_executive_today,round(count(case when talk_duration_per_exec_mins < 75 then Agent end ),2) as executives_less_than_75mins ,max(Max_Date) as today_date from pdr')
 
pace_integrated_1.createOrReplaceTempView('pi1')

pace_integrated_2 = spark.sql('select "pace" as Channel,round(sum(attempt_mtd)/sum(no_of_working_days),2) as mtd_attempt,round(sum(contact_mtd)/sum(no_of_working_days),2) as mtd_contacted,round(sum(talk_duration_per_exec_secs)/sum(contact_mtd),2) as mtd_average_call_time,round((sum(talk_duration_per_exec_secs)/sum(no_of_working_days))/60,2) mtd_talktime_mins,round(count(case when talk_duration_per_exec_mins < 75 then Agent end ),2) as mtd_executives_less_than_75mins from pdm')

pace_integrated_2.createOrReplaceTempView('pi2')


pace_integrated_final = spark.sql('select pi1.*, pi2.mtd_attempt,pi2.mtd_contacted,pi2.mtd_average_call_time,pi2.mtd_talktime_mins,pi2.mtd_executives_less_than_75mins from pi1 left join pi2 on pi1.Channel = pi2.Channel')

pace_integrated_final.createOrReplaceTempView('pif')



## Reading NHM
nhm=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_nhm_dialler/nhm_dialler.csv",header =True,inferSchema=True)

#nhm_current=nhm.filter('Date >= date_add(last_day(now() - interval 03 months),1) and Date<=(last_day(now() - interval 02 months))')
nhm_current=nhm.filter('Date >= date_add(last_day(now() - interval 01 months),1) and Date<=(last_day(now()))')

nhm_current=nhm_current.withColumn('Time_in_mins',round((nhm_current.Time_in_Seconds/60),2)).withColumn('Date',to_date('Date','yyyy-MM-dd'))
nhm_current.createOrReplaceTempView('nhm')

max_day=spark.sql('select dayofmonth (max(Date)) as max_day from nhm').collect()
max_day=max_day[0].max_day

nhm_current=nhm_current.withColumn('max_day',lit(max_day))

nhm_current.createOrReplaceTempView('nhm')

nhm_daily_report=spark.sql('Select Agent, round(count(case when Day= max_day then Agent end),2) as As_on_day_attempt,round(count(Agent)/count(distinct(Day)),2) as Avg_attempts,count(case when Day= max_day and TerminationStatus="Contacted" then Agent end) as As_on_day_contact,round((count(case when TerminationStatus="Contacted" then Agent end))/count(distinct(Day)),2) as Avg_call_contacted,round((Avg(case when TerminationStatus="Contacted" and Day= max_day then Time_in_Seconds end)),2) as As_on_call_time_in_sec,round((Avg(case when TerminationStatus="Contacted" then Time_in_Seconds end)),2) as Avg_call_Time_secs,round((sum(case when TerminationStatus="Contacted" and Day= max_day then Time_in_Seconds end))/60,2) as Total_Time_mins,round(((sum(case when TerminationStatus="Contacted" then Time_in_Seconds end)/count(distinct(Day)))/60),2) as Avg_Total_talktime_mins,sum(case when Day= max_day then Time_in_Seconds end) as talk_duration_per_exec_secs,round(sum(case when Day= max_day then Time_in_mins end),2) as talk_duration_per_exec_mins,min(Date) as Min_Date,max(Date) as Max_Date from nhm group by Agent order by Agent')


nhm_daily_report.createOrReplaceTempView('ndr')

## NHM mtd


nhm_mtd=spark.sql('Select Agent, round(count (Agent),2) as attempt_mtd, round(count(case when TerminationStatus="Contacted" then Agent end),2) as contact_mtd,round(sum(Time_in_Seconds),2) as talk_duration_per_exec_secs,round(sum(Time_in_mins),2) as talk_duration_per_exec_mins,count(distinct(Day)) as no_of_working_days, min(Date) as Min_Date,max(Date) as Max_Date from nhm group by Agent order by Agent')

nhm_mtd=nhm_mtd.withColumn('mtd_talktime_mins',round((nhm_mtd.talk_duration_per_exec_mins/nhm_mtd.no_of_working_days),2))

nhm_mtd.createOrReplaceTempView('ndm')

## Take sum of  from daily report card

nhm_integrated_1 = spark.sql('select "nhm" as Channel,round(sum(As_on_day_attempt)/count(distinct(Agent)),2) as daily_attempt_per_executive_today,round(sum(As_on_day_contact)/count(distinct(Agent)),2) as daily_contacted_per_executive_today,round(sum(talk_duration_per_exec_secs)/sum(As_on_day_contact),2) as daily_average_call_time_today,round((sum(talk_duration_per_exec_secs)/count(distinct(Agent)))/60,2) as daily_talktime_per_executive_today,round(count(case when talk_duration_per_exec_mins < 75 then Agent end ),2) as executives_less_than_75mins,max(Max_Date) as today_date from ndr')
 
nhm_integrated_1.createOrReplaceTempView('ni1')

nhm_integrated_2 = spark.sql('select "nhm" as Channel,round(sum(attempt_mtd)/sum(no_of_working_days),2) as mtd_attempt,round(sum(contact_mtd)/sum(no_of_working_days),2) as mtd_contacted,round(sum(talk_duration_per_exec_secs)/sum(contact_mtd),2) as mtd_average_call_time,round((sum(talk_duration_per_exec_secs)/sum(no_of_working_days))/60,2) mtd_talktime_mins,round(count(case when talk_duration_per_exec_mins < 75 then Agent end ),2) as mtd_executives_less_than_75mins from ndm')

nhm_integrated_2.createOrReplaceTempView('ni2')


nhm_integrated_final = spark.sql('select ni1.*, ni2.mtd_attempt,ni2.mtd_contacted,ni2.mtd_average_call_time,ni2.mtd_talktime_mins,ni2.mtd_executives_less_than_75mins from ni1 left join ni2 on ni1.Channel = ni2.Channel')

nhm_integrated_final.createOrReplaceTempView('nif')

pace_nhm = pace_integrated_final.unionAll(nhm_integrated_final)



## Chennai Dialler


chennai=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_chennai_dialler/chennai_dialler.csv",header =True,inferSchema=True)

chennai=chennai.withColumn('StartTime',to_date('StartTime','MM/dd/yyyy')).withColumn('AgentStartTime',to_date('AgentStartTime','MM/dd/yyyy'))

chennai_current=chennai.filter('StartTime >= date_add(last_day(now() - interval 01 months),1) and StartTime<=(last_day(now()))')

chennai_current =chennai_current.withColumn('Day', dayofmonth('StartTime'))
chennai_current=chennai_current.withColumn('TalkTime',substring('TalkTime',4,5))
chennai_current=chennai_current.withColumn('TalkTime',split('TalkTime',':'))
chennai_current=chennai_current.withColumn('Talk_duration_Minutes',chennai_current['TalkTime'].getItem(0).cast('Integer'))
chennai_current=chennai_current.withColumn('Talk_duration_Seconds',chennai_current['TalkTime'].getItem(1).cast('Integer'))
chennai_current=chennai_current.withColumn('Time_in_secs',(chennai_current.Talk_duration_Minutes*60+chennai_current.Talk_duration_Seconds))

chennai_current=chennai_current.withColumn('Time_in_mins',round((chennai_current.Time_in_secs/60),2))
chennai_current.createOrReplaceTempView('cc')
max_day=spark.sql('select dayofmonth (max(StartTime)) as max_day from cc').collect()
max_day=max_day[0].max_day

chennai_current=chennai_current.withColumn('max_day',lit(max_day))

chennai_current.createOrReplaceTempView('cc1')

## daily report
chennai_daily_report=spark.sql('Select Agent, round(count(case when Day= max_day then Agent end),2) as As_on_day_attempt,round(count(Agent)/count(distinct(Day)),2) as Avg_attempts,count(case when Day= max_day and TerminationStatus="Handled" then Agent end) as As_on_day_contact,round((count(case when TerminationStatus="Handled" then Agent end))/count(distinct(Day)),2) as Avg_call_contacted,round((Avg(case when TerminationStatus="Handled" and Day= max_day then Time_in_secs end)),2) as As_on_call_time_in_sec,round((Avg(case when TerminationStatus="Handled" then Time_in_secs end)),2) as Avg_call_Time_secs,round((sum(case when TerminationStatus="Handled" and Day= max_day then Time_in_secs end))/60,2) as Total_Time_mins,round(((sum(case when TerminationStatus="Handled" then Time_in_secs end)/count(distinct(Day)))/60),2) as Avg_Total_talktime_mins,sum(case when Day= max_day then Time_in_secs end) as talk_duration_per_exec_secs,round(sum(case when Day= max_day then Time_in_mins end),2) as talk_duration_per_exec_mins,min(StartTime) as Min_Date,max(StartTime) as Max_Date from cc1 group by Agent order by Agent')

chennai_daily_report.createOrReplaceTempView('cdr')


## mtd report

chennai_mtd=spark.sql('Select Agent, round(count (Agent),2) as attempt_mtd, round(count(case when TerminationStatus="Handled" then Agent end),2) as contact_mtd,round(sum(Time_in_secs),2) as talk_duration_per_exec_secs,round(sum(Time_in_mins),2) as talk_duration_per_exec_mins,count(distinct(Day)) as no_of_working_days, min(StartTime) as Min_Date,max(StartTime) as Max_Date from cc1 group by Agent order by Agent')

chennai_mtd=chennai_mtd.withColumn('mtd_talktime_mins',round((chennai_mtd.talk_duration_per_exec_mins/chennai_mtd.no_of_working_days),2))

chennai_mtd.createOrReplaceTempView('cdm')

chennai_integrated_1 = spark.sql('select "Chennai ASF" as Channel,round(sum(As_on_day_attempt)/count(distinct(Agent)),2) as daily_attempt_per_executive_today,round(sum(As_on_day_contact)/count(distinct(Agent)),2) as daily_contacted_per_executive_today,round(sum(talk_duration_per_exec_secs)/sum(As_on_day_contact),2) as daily_average_call_time_today,round((sum(talk_duration_per_exec_secs)/count(distinct(Agent)))/60,2) as daily_talktime_per_executive_today,round(count(case when talk_duration_per_exec_mins < 75 then Agent end ),2) as executives_less_than_75mins,max(Max_Date) as today_date from cdr')
chennai_integrated_1.createOrReplaceTempView('ci1')


chennai_integrated_2 = spark.sql('select "pace" as Channel,round(sum(attempt_mtd)/sum(no_of_working_days),2) as mtd_attempt,round(sum(contact_mtd)/sum(no_of_working_days),2) as mtd_contacted,round(sum(talk_duration_per_exec_secs)/sum(contact_mtd),2) as mtd_average_call_time,round((sum(talk_duration_per_exec_secs)/sum(no_of_working_days))/60,2) mtd_talktime_mins,round(count(case when talk_duration_per_exec_mins < 75 then Agent end ),2) as mtd_executives_less_than_75mins from cdm')

chennai_integrated_2.createOrReplaceTempView('ci2')

chennai_integrated_final = spark.sql('select ci1.*, ci2.mtd_attempt,ci2.mtd_contacted,ci2.mtd_average_call_time,ci2.mtd_talktime_mins,ci2.mtd_executives_less_than_75mins from ci1 left join ci2 on ci1.Channel = ci2.Channel')


pace_nhm_chennai = chennai_integrated_final.unionAll(pace_nhm)

#pace_nhm_chennai=pace_nhm_chennai.withColumn('run_date',lit('2019-09-15'))
#pace_nhm_chennai.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/ASF_daillers",mode="append",header="true")
pace_nhm_chennai=pace_nhm_chennai.withColumn('run_date',lit(current_date()))
pace_nhm_chennai=pace_nhm_chennai.na.fill(0)

#pace_nhm_chennai.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/ASF_daillers",mode="append",header="true")

pace_nhm_chennai.createOrReplaceTempView('pnc')

pace_nhm_chennai_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/ASF_daillers/asf_daillers.csv",header =True,inferSchema=True)

pace_nhm_chennai_old.createOrReplaceTempView('pnco')

pace_nhm_chennai_final=spark.sql('select * from pnco union all select * from pnc')

pace_nhm_chennai_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/ASF_daillers",mode="append",header="true")



table_1 = spark.sql('select "Branch_Dailler_Table" as Table_name, max(dateofcalling) as last_modified, current_date() as run_date from b')

table_2 = spark.sql('select "Field_visit_Table" as Table_name, max(InteractionDate) as last_modified, current_date() as run_date from ms')

table_A = table_1.unionAll(table_2)

table_3 = spark.sql('select "Pace_Dailler_Table" as Table_name, max(StartTime) as last_modified, current_date() as run_date from pace')

table_B = table_A.unionAll(table_3)

table_4 = spark.sql('select "NHM_Dailler_Table" as Table_name, max(Date) as last_modified, current_date() as run_date from nhm')

table_C = table_B.unionAll(table_4)

table_5 = spark.sql('select "Chennai_Dailler_Table" as Table_name, max(StartTime) as last_modified, current_date() as run_date from cc1')

table_D = table_C.unionAll(table_5)

table_D.createOrReplaceTempView('tf')

#table_D.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details",mode="append",header="true")

table_details_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details/table_details.csv",header =True,inferSchema=True)

table_details_old.createOrReplaceTempView('nto')

table_details_final= spark.sql('select * from nto union all select * from tf')

table_details_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details",mode="append",header="true")

time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)


cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/ASF_daillers/asf_daillers.csv"
os.system(cmd1)
time.sleep(10)

cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Branch_benchmark/branch_benchmark.csv"
os.system(cmd2)
time.sleep(10)

cmd3="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details/table_details.csv"
os.system(cmd3)
time.sleep(10)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())

file_path_asf_dailler = "s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/ASF_daillers/"
time.sleep(10)

file_path_branch_benchmark = "s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Branch_benchmark/"
time.sleep(10)

file_path_Table_details = "s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details/"
time.sleep(10)

created_file_path_asf_dailler = fs.globStatus(Path(file_path_asf_dailler + "part*.csv"))[0].getPath()
time.sleep(10)

created_file_path_branch_benchmark = fs.globStatus(Path(file_path_branch_benchmark + "part*.csv"))[0].getPath()
time.sleep(10)

created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)

fs.rename(created_file_path_asf_dailler,Path(file_path_asf_dailler + "asf_daillers.csv"))
time.sleep(10)

fs.rename(created_file_path_branch_benchmark,Path(file_path_branch_benchmark + "branch_benchmark.csv"))
time.sleep(10)

fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))
time.sleep(10)







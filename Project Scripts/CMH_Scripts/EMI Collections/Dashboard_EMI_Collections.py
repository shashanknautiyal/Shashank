# Importing the Required Libraries

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

# Reading Final_Base File from Master Tables from S3 Process data Bucket. 

base_complete=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_final_base/final_base.csv",header=True,inferSchema=True)

# Formating the date Column
base_complete = base_complete.withColumn('date', to_date('month_year','yyyy-MM'))

base_complete=base_complete.withColumn("ODBucketMax",regexp_replace("ODBucketMax", "_", "-"))

# Filtering the data for Current month
base_current_month=base_complete.filter('date>= date_add(last_day(now() - interval 01 month),1) and date <= last_day(now())')
base_current_month.createOrReplaceTempView('base')


## Filling the spaces in columns names with underscore.
for col in base_current_month.columns:
	base_current_month=base_current_month.withColumnRenamed(col,col.replace(" ", "_"))

# Renaming the Column Name
base_current_month=base_current_month.withColumnRenamed('Contract_No.','Contract_No')

#selcting the required columns
base_current_month=base_current_month.select('Contract_No','New_Branch','Zone','ODBucketMax')

base_current_month.createOrReplaceTempView('b1')

# Mapping Sub Branches to Main Branch.
base_current_month=spark.sql('select *, case when New_Branch in ("bangalore koramangala","bangalore millers road","zest_bangalore") then "bangalore" when New_Branch in ("delhi bikaji","delhi jasola","delhi pitampura") then "delhi" when New_Branch in ("mumbai andheri","mumbai vashi") then "mumbai" when New_Branch in ("chennai", "zest_chennai") then "chennai" else New_Branch end as New_Branch_1 from b1')

base_current_month=base_current_month.dropDuplicates(['Contract_No'])

base_current_month=base_current_month.withColumn('flag',lit("Base"))

base_current_month.createOrReplaceTempView('b')

# Reading Final Inflow File from Master Tables from S3 Process data Bucket. 

inflow_complete=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_final_inflow/final_inflow.csv",header=True,inferSchema=True)

# Formating the date Column
inflow_complete = inflow_complete.withColumn('date', to_date('month_year','yyyy-MM'))

inflow_complete=inflow_complete.withColumn("ODBucketMax",regexp_replace("ODBucketMax", "_", "-"))

# Filtering the data for Current month
inflow_current_month=inflow_complete.filter('date>= date_add(last_day(now() - interval 01 month),1) and date <= last_day(now())')
inflow_current_month.createOrReplaceTempView('inflow')

## Filling the spaces in columns names with underscore.
for col in inflow_current_month.columns:
	inflow_current_month=inflow_current_month.withColumnRenamed(col,col.replace(" ", "_"))

# Renaming the Column Name
inflow_current_month=inflow_current_month.withColumnRenamed('Contract_No.','Contract_No')

# Selecting the required Columns
inflow_selected_columns=inflow_current_month.select('Contract_No','New_Branch','Zone','ODBucketMax')

inflow_selected_columns.createOrReplaceTempView('if1')


# Replacing the categories in New_Branch Column
inflow=spark.sql('select *, case when New_Branch in ("bangalore koramangala","bangalore millers road","zest_bangalore") then "bangalore" when New_Branch in ("delhi bikaji","delhi jasola","delhi pitampura") then "delhi" when New_Branch in ("mumbai andheri","mumbai vashi") then "mumbai" when New_Branch in ("chennai", "zest_chennai") then "chennai" else New_Branch end as New_Branch_1 from if1')

# Dropping the duplicate Contract No.
inflow=inflow.dropDuplicates(['Contract_No'])

inflow=inflow.withColumn('flag',lit("Inflow"))

inflow.createOrReplaceTempView("if")

# Calculating the opening base at Branch/ControlLocation Level
base_summ=spark.sql("Select New_Branch_1 as New_Branch, Count(*) as opening_base from b group by New_Branch_1 order by New_Branch_1")

base_summ.createOrReplaceTempView('bs')

# Calculating the Inflow at Branch/ControlLocation Level
inflow_summ=spark.sql("Select New_Branch_1 as New_Branch, Count(*) as inflow from if group by New_Branch_1 order by New_Branch_1")

inflow_summ.createOrReplaceTempView('ifs')

## Mapping the opening_base and Inflow columns 
base_inflow=spark.sql("Select bs.*,ifs.inflow from bs left join ifs on bs.New_Branch=ifs.New_Branch")

base_inflow.createOrReplaceTempView('bi')


# Reading Target File from Master Tables from S3 Process data Bucket. 
target_Complete=spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_target_sheets_emi/emi_target.csv",header=True,inferSchema=True)

# Formating the date column
target_Complete=target_Complete.withColumn("date",to_date("Target_Month","MMM-yy"))

# Applying the Current Month Filter
target_current_month=target_Complete.filter('date>= date_add(last_day(now() - interval 01 month),1) and date <= last_day(now())')

# Cleaing and structuring the data
target_current_month=target_current_month.withColumnRenamed('Closing Base Target','Closing_base_target')
target_current_month=target_current_month.withColumn('Closing_base_target',regexp_replace('Closing_base_target',',',''))
target_current_month=target_current_month.withColumn('Closing_base_target',target_current_month.Closing_base_target.cast('Integer'))
target_current_month=target_current_month.withColumn('Branch',trim(target_current_month.Branch))
target_current_month=target_current_month.withColumn('Branch',lower(target_current_month.Branch))
target_current_month.createOrReplaceTempView('t')


## Calculating the assumed Inflow.
base_inflow_1=spark.sql("Select New_Branch,opening_base,inflow,round((opening_base/(select sum(opening_base) from bi))*(select ((sum(opening_base)-(select sum(Closing_base_target) from t))+6000) from  bi)) as assumed_inflow from bi")
base_inflow_1.createOrReplaceTempView('bi1')

## Mapping the Closing_base_target column.
base_inflow_2=spark.sql("select bi1.*,t.Closing_base_target from bi1 left join t on bi1.New_Branch=t.Branch")
base_inflow_2.createOrReplaceTempView('bi2')

## storing the current day of month.
day=spark.sql('select dayofmonth(now()) as day').collect()
day=day[0].day

## Creating Recovery_Target Column as per the business logic
base_inflow_3=spark.sql('select New_Branch,opening_base,inflow as MTD_inflow,Closing_base_target,assumed_inflow,opening_base - Closing_base_target + case when  dayofmonth(now()) <= 7  then greatest(assumed_inflow,inflow) else inflow end as Recovery_Target from bi2')
base_inflow_3.createOrReplaceTempView('bi3')




## # Reading ZFI_065_Daily file from Master Tables from S3 Process data Bucket.
zfi_65_28_feb =spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_065_daily/ZFI_065_Daily.csv",header=True,inferSchema=True)

# Formatting the date column
zfi_65_28_feb=zfi_65_28_feb.withColumn('dw_lastupdated',to_date('dw_lastupdated','yyyy-MM-dd'))

# Lower casing the columns
for col in zfi_65_28_feb.columns:
	zfi_65_28_feb=zfi_65_28_feb.withColumnRenamed(col,col.lower())
	
# Renaming,Cleaning and structuring the Data.
zfi_65_28_feb = zfi_65_28_feb.withColumnRenamed('odbucketmax','OD_Bucket_max').withColumnRenamed('mortgageid','Mortgage_ID').withColumnRenamed('total_od_emi_dp','Total_OD').withColumnRenamed('emibanked','EMI_Banked').withColumnRenamed('dpoverdueamount','DP_Overdue_amount').withColumnRenamed('realizedpercentcontract','Realized_per_con').withColumnRenamed('contractno','Contract_No').withColumnRenamed('mortgagecheck','Mortgage_Check')

zfi_65_28_feb = zfi_65_28_feb.withColumn("OD_Bucket_max",regexp_replace("OD_Bucket_max", "[^a-zA-Z0-9_-]", ""))

zfi_65_28_feb=zfi_65_28_feb.withColumn('OD_Bucket_max',trim(zfi_65_28_feb.OD_Bucket_max))

zfi_65_28_feb.createOrReplaceTempView('zfeb')

# selecting the required columns and applying the filters.

zfi_65_summ=spark.sql('Select Contract_No,Realized_per_con,status,Mortgage_Check,Total_OD,OD_Bucket_max,EMI_Banked,DP_Overdue_amount from zfeb where status="Active" and OD_Bucket_max = "" and Mortgage_Check=1 and Total_OD<=0 and EMI_Banked=0 and DP_Overdue_amount=0')

zfi_65_summ.createOrReplaceTempView('zsum')

### Applying filters to Zfeb table to generate od_contracts.

od_contracts = zfi_65_28_feb.filter('OD_Bucket_max <> ""')

od_contracts=od_contracts.select('Contract_No')
od_contracts=od_contracts.withColumn('od_flag', lit(1))
od_contracts.createOrReplaceTempView('od')

zfi_65_summ_1 = spark.sql('select zsum.*,od.od_flag from zsum left join od on zsum.Contract_No=od.Contract_No')
zfi_65_summ_1=zfi_65_summ_1.fillna(0,subset=['od_flag'])
zfi_65_summ_1 = zfi_65_summ_1.filter('od_flag ==0')
zfi_65_summ_1=zfi_65_summ_1.select('Contract_No','Realized_per_con')
zfi_65_summ_1.createOrReplaceTempView('zsum1')

## base inflow append

base_od=base_current_month.select('Contract_No','flag','New_Branch_1','ODBucketMax')
base_od.createOrReplaceTempView('bo')

inflow_od=inflow.select('Contract_No','flag','New_Branch_1','ODBucketMax')
inflow_od.createOrReplaceTempView('io')


# Appending base and Inflow
od_base_inflow=spark.sql('select Contract_No, flag,New_Branch_1 as New_Branch,ODBucketMax from bo union all select Contract_No,flag,New_Branch_1 as New_Branch,ODBucketMax from io')
od_base_inflow.createOrReplaceTempView('obi')

#	

zfi_od_base_inflow=od_base_inflow.join(zfi_65_summ_1,od_base_inflow.Contract_No==zfi_65_summ_1.Contract_No, how ='inner').drop(zfi_65_summ_1.Contract_No)

zfi_od_base_inflow=zfi_od_base_inflow.dropDuplicates(['Contract_No'])

## Drop duplicate Contract_No

zfi_od_base_inflow.createOrReplaceTempView('zobi')

rez_collection=spark.sql("select New_Branch,count(Contract_No) as Total_realised from zobi group by New_Branch order by New_Branch")

rez_collection.createOrReplaceTempView('rc1')

base_inflow_4=spark.sql("select bi3.*,rc1.Total_realised from bi3 left join rc1 on bi3.New_Branch=rc1.New_Branch")

base_inflow_4=base_inflow_4.fillna(0,subset=['MTD_inflow'])

# make Dynamic
base_inflow_4=base_inflow_4.withColumn('Day',lit(day))
base_inflow_4=base_inflow_4.withColumn('MTD_inflow', when(base_inflow_4.Day<=7,base_inflow_4.assumed_inflow).otherwise(base_inflow_4.MTD_inflow))

base_inflow_4.createOrReplaceTempView('bi4')

base_inflow_5=spark.sql('Select *,case when Day<=7 then 0.15* Closing_base_target when (Day>=8 and Day<=15) then 0.45*Closing_base_target when (Day>=16 and Day<=21) then 0.75* Closing_base_target else 1*Closing_base_target end as Milestone_target from bi4')

base_inflow_5=base_inflow_5.withColumn('Milestone_target_Achieved_Perc',round((base_inflow_5.Total_realised/base_inflow_5.Milestone_target)*100))

day_name=spark.sql('select date_format(now(),"E") as day_name').collect()
day_name=day_name[0].day_name

# keep thursday value and make dynamic

base_inflow_5=base_inflow_5.withColumn('day_name',lit(day_name))

base_inflow_5=base_inflow_5.withColumn('day_value', when(base_inflow_5.day_name=='Mon',6).when(base_inflow_5.day_name=='Tue',5).when(base_inflow_5.day_name=='Wed',4).when(base_inflow_5.day_name=='Thu',3).when(base_inflow_5.day_name=='Fri',2).otherwise(1))

base_inflow_5=base_inflow_5.withColumn('Required_DRR',round((base_inflow_5.Milestone_target-base_inflow_5.Total_realised)/base_inflow_5.day_value))


base_inflow_5.createOrReplaceTempView('bi5')

rec_bucket=spark.sql('select zobi.* from zobi left join obi on zobi.Contract_No=obi.Contract_No')
rec_bucket=rec_bucket.dropDuplicates(['Contract_No'])
rec_bucket.createOrReplaceTempView('rb')

rec_bucket_1=spark.sql('select New_Branch,ODBucketMax,count(*) as Count from rb group by New_Branch,ODBucketMax')

rec_bucket_1.createOrReplaceTempView('rb1')

rec_bucket_summ=spark.sql('select New_Branch, sum(case when ODBucketMax = "00-01" then count end) as X_00_01,sum(case when ODBucketMax = "02-03" then count end) as X_02_03,sum(case when ODBucketMax = "04-06" then count end) as X_04_06, sum(case when ODBucketMax = "07-09" then count end) as X_07_09,sum(case when ODBucketMax = "10-12" then count end) as X_10_12,sum(case when ODBucketMax = "13-15" then count end) as X_13_15,sum(case when ODBucketMax = "16-18" then count end) as X_16_18,sum(case when ODBucketMax = "19-21" then count end) as X_19_21,sum(case when ODBucketMax = "22-24" then count end) as X_22_24,sum(case when ODBucketMax = "25-27" then count end) as X_25_27,sum(case when ODBucketMax = "28-30" or ODBucketMax = "31-33" or ODBucketMax = "34-36" or ODBucketMax = "37-39" or ODBucketMax = "40-42" or ODBucketMax = "43-45" or ODBucketMax = "46-48" or ODBucketMax = "49-MO" then count end) as X_27plus from rb1 group by New_Branch')

rec_bucket_summ=rec_bucket_summ.na.fill(0)

rec_bucket_summ=rec_bucket_summ.withColumn('X_07_12', (rec_bucket_summ.X_07_09 + rec_bucket_summ.X_10_12)).withColumn('X_13_18',(rec_bucket_summ.X_13_15 + rec_bucket_summ.X_16_18))

rec_bucket_summ.createOrReplaceTempView('rbs')

base_inflow_6=spark.sql("Select bi5.*,rbs.X_00_01, rbs.X_02_03, rbs.X_04_06, rbs.X_07_12, rbs.X_13_18, rbs.X_19_21, rbs.X_22_24, rbs.X_25_27, rbs.X_27plus from bi5 left join rbs on bi5.New_Branch=rbs.New_Branch")

emi=base_inflow_6

emi=emi.na.fill(0)

emi.createOrReplaceTempView('emi')

base_inflow_7=emi
base_inflow_7.createOrReplaceTempView('bi7')

## Reading EMI Dialler Table from Master Tables from S3 Process data Bucket.

dialler_data= spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_Emi_dialler/emi_dialler.csv",header =True,inferSchema=True)

dialler_data=dialler_data.withColumn('last_modified_date',to_date('last_modified_date','yyyy-MM-dd'))

dialler_data=dialler_data.filter(dialler_data.CallStatus == 'Connected')
dialler_data.createOrReplaceTempView('dd')


## Reading Dialler recording log Table Table from Master Tables from S3 Process data Bucket.
dialler_rec_log=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_dialler_recording_log/dialler_recording_log.csv",header =True,inferSchema=True)

# Formating the date of Start_time and dwLastUpdatedOn columns
dialler_rec_log=dialler_rec_log.withColumn('Start_time',to_date('Start_time','yyyy-MM-dd')).withColumn('dwLastUpdatedOn',to_date('dwLastUpdatedOn','yyyy-MM-dd'))


# Applying the current month Filter.
dialler_rec_log_1=dialler_rec_log.filter('Start_time >= date_add(last_day(now() - interval 01 month),1) and Start_time<= last_day(now())')

dialler_rec_log_1=dialler_rec_log_1.withColumn('day_month',dayofmonth(dialler_rec_log_1.Start_time))

dialler_rec_log_1.createOrReplaceTempView('drl')

## Mapping the EMI Dialler table with Dailler Recording log table and taking the selecting the required columns.

emi_dialler=spark.sql("Select drl.Lead_id,drl.Campaign_name, drl.Start_time, drl.Length_in_sec,dd.AgentName,dd.MemberName,dd.ContractId,dd.CallStatus from drl left join dd on drl.Lead_id=dd.ID")
emi_dialler=emi_dialler.dropDuplicates()
emi_dialler.createOrReplaceTempView('ed')

## Reading Agent Mapping Table from Master Tables from S3 Process data Bucket.

agent_role_full= spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_agent_mapping/agent_mapping.csv",header =True,inferSchema=True)

## Formating the date of dw_last_modified_date column.
agent_role_full=agent_role_full.withColumn('dw_last_modified_date',to_date('dw_last_modified_date','dd/MM/yyyy'))


## Applying the Filter for current month.
agent_role=agent_role_full.filter('dw_last_modified_date >= date_add(last_day(now() - interval 01 month),1) and dw_last_modified_date<= last_day(now())')

# Cleaing and structuring the data
agent_role=agent_role.withColumnRenamed('Agent name', 'Agent_Name').withColumnRenamed('Agent Type','Agent_type')
agent_role=agent_role.withColumn('Branch',trim(agent_role.Branch))
agent_role=agent_role.withColumn('Branch',lower(agent_role.Branch))
agent_role.createOrReplaceTempView('ar')


## Mapping the Table with Agent role mapping table.
emi_dialler_join=spark.sql("select ed.*,ar.Agent_type from ed left join ar on ed.AgentName=ar.Agent_Name")
emi_dialler_join=emi_dialler_join.na.fill(0)
emi_dialler_join.createOrReplaceTempView('edj')

## Mapping the Table with base_inflow table and selecting the required columns.
emi_master=spark.sql("select obi.Contract_No,obi.New_Branch,edj.Lead_id,edj.Campaign_name,edj.Start_time,edj.Length_in_sec,edj.AgentName,edj.MemberName,edj.CallStatus,edj.Agent_type from obi left join edj on edj.ContractId=obi.Contract_No")

#emi_master=emi_master.dropDuplicates()

emi_master.createOrReplaceTempView('em')

# Summarising the table on ControlLocation/Branch Level.
emi_master_summ=spark.sql('select New_Branch , count(Contract_No) as total_mtd_contacts, count(distinct Contract_No) as total_unique_mtd_contacts from em group by New_Branch')

emi_master_summ.createOrReplaceTempView('ems')

emi_master=emi_master.withColumn('day', dayofmonth('Start_time'))
emi_master_tele_caller=emi_master.filter(emi_master.Agent_type=='Tele Caller')
emi_master_tele_caller=emi_master_tele_caller.na.fill(0)
emi_master_tele_caller.createOrReplaceTempView('emtc')

day_wise_summ=spark.sql("select New_Branch,day,count(New_Branch) as Total_calls, count(distinct AgentName) as Total_agents from emtc group by New_Branch,day")

# Calculating the avg_contacts_per_tc_day KPI.
day_wise_summ=day_wise_summ.withColumn('avg_contacts_per_tc_day',(day_wise_summ.Total_calls/day_wise_summ.Total_agents))

# Creating a prev_day column to filter the data.
prev_day =spark.sql("select max(day_month) as prev_day from drl where day_month < (select max(day_month) from drl)").collect()

prev_day=prev_day[0].prev_day
day_wise_summ=day_wise_summ.withColumn('prev_day',lit(prev_day))

day_wise_summ.createOrReplaceTempView('dws')

# Applying the Business Logics and generating the required Contracts KPI
agg_day_wise_dialer=spark.sql("Select New_Branch,sum(avg_contacts_per_tc_day) as total_contacts_per_tc_day, count(distinct day) as unique_day from dws group by New_Branch")

agg_day_wise_dialer=agg_day_wise_dialer.withColumn('mtd_avg_contacts_per_tc_per_day',(agg_day_wise_dialer.total_contacts_per_tc_day/agg_day_wise_dialer.unique_day))

agg_day_wise_dialer=agg_day_wise_dialer.select('New_Branch','mtd_avg_contacts_per_tc_per_day')

agg_day_wise_dialer.createOrReplaceTempView('adws')

##


# Make Dynamic
yesterday_contact= day_wise_summ.filter('day =prev_day')
yesterday_contact=yesterday_contact.na.fill(0)
yesterday_contact=yesterday_contact.withColumn('yesterday_contact_per_tc',yesterday_contact.Total_calls/yesterday_contact.Total_agents)

yesterday_contact=yesterday_contact.select('New_Branch','yesterday_contact_per_tc')

yesterday_contact.createOrReplaceTempView('yc')

dialler_master_summ_final=spark.sql("Select ems.*,adws.mtd_avg_contacts_per_tc_per_day,yc.yesterday_contact_per_tc from ems left join adws on ems.New_Branch=adws.New_Branch left join yc on ems.New_Branch=yc.New_Branch")

dialler_master_summ_final.createOrReplaceTempView('dmsf')

base_inflow_8= spark.sql("Select bi7.*,dmsf.total_mtd_contacts ,dmsf.total_unique_mtd_contacts,dmsf.mtd_avg_contacts_per_tc_per_day,dmsf.yesterday_contact_per_tc from bi7 left join dmsf on bi7.New_Branch=dmsf.New_Branch")

base_inflow_8.createOrReplaceTempView('bi8')

field_app= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Emi_app/emi_app.csv",header =True,inferSchema=True)

field_app =field_app.withColumn('Rundate',to_date('Rundate','yyyy-MM-dd'))

# add a filter for current month on Rundate
field_app=field_app.filter('Rundate >= date_add(last_day(now() - interval 01 month),1) and Rundate<= last_day(now())')

field_app=field_app.withColumnRenamed('Branch','ControlLocation')

field_app_1=field_app.select('ControlLocation','Total_unique_members_met','Yesterday_Visit_count','of_visits_per_day_ce','Rundate')
field_app_1=field_app_1.withColumn('ControlLocation',lower(field_app_1.ControlLocation))

field_app_1=field_app_1.withColumn('ControlLocation',regexp_replace('ControlLocation', ' - ',' '))
field_app_1=field_app_1.withColumn('ControlLocation',regexp_replace('ControlLocation', '-',' '))
field_app_1.createOrReplaceTempView('fa')

field_app_1=spark.sql('select *, case when ControlLocation in ("bangalore koramangala","bangalore millers road") then "bangalore" when ControlLocation in ("mumbai andheri", "mumbai vashi") then "mumbai" when ControlLocation in ("delhi pitampura" ,"delhi jasola") then "delhi" else ControlLocation end as New_Branch  from fa')

field_app_1.createOrReplaceTempView('fa')

field_app_final=spark.sql('select New_Branch, sum(Total_unique_members_met) as Total_MTD_visits,round(sum(Yesterday_Visit_count),2) as yesterday_visits,round(sum(of_visits_per_day_ce),2) as MTD_visits_per_CE_day from fa group by New_Branch')

field_app_final.createOrReplaceTempView('faf')

base_inflow_9=spark.sql('select bi8.*,faf.Total_MTD_visits,faf.yesterday_visits,faf.MTD_visits_per_CE_day from bi8 left join faf on bi8.New_Branch=faf.New_Branch')

base_inflow_9.createOrReplaceTempView('bi9')

# Reading ZFI_PDI Table from Master Tables from S3 Process data Bucket.

con_pay=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_PDI/ZFI_PDI.csv",header =True,inferSchema=True)

#con_pay=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/zfi-pdi/zfi-pdi_16122019.csv",header =True,inferSchema=True)

#con_pay_1= con_pay.select('reference_document','pdi_returned_date','')


##
#con_pay=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/incremental-load/zfi-pdi/zfi-pdi_16122019.csv",header =True,inferSchema=True)


## spark.sql("select sales_document,pdi_returned_date from  cp where sales_document = 3451 order by pdi_returned_date desc ")

## Lower casing the column names
for col in con_pay.columns:
	con_pay=con_pay.withColumnRenamed(col,col.lower())


## Selecting the required Columns
con_pay_select=con_pay.select('reference_document','sales_document','pdi_type','pdi_status','pdi_realization_date','installment_date','pdi_received_date','pdi_returned_date','dw_last_modified_date')


# Formating the date in the columns
con_pay_select=con_pay_select.withColumn('pdi_returned_date',to_date('pdi_returned_date',"MM/dd/yyyy")).withColumn('pdi_received_date', to_date('pdi_received_date', "MM/dd/yyyy")).withColumn('installment_date', to_date('installment_date', "MM/dd/yyyy")).withColumn('pdi_status',con_pay_select.pdi_status.cast('Integer')).withColumn('dw_last_modified_date',to_date('dw_last_modified_date','yyyy-MM-dd'))

con_pay_select.createOrReplaceTempView('cp')


con_pay_1=spark.sql(" select reference_document,count(*) as pdi_count from cp where pdi_status= 4 group by reference_document")
con_pay_1.createOrReplaceTempView('cp1')
fut_pdi_df = spark.sql("Select zobi.*, cp1.pdi_count from zobi left join cp1 on zobi.Contract_No=cp1.reference_document")
fut_pdi_df=fut_pdi_df.na.fill(0)
fut_pdi_df.createOrReplaceTempView("fpd")
fut_pdi_df=spark.sql("Select Contract_No,New_Branch,pdi_count, case when (pdi_count>0 and pdi_count=1) then 'PDI_1' when (pdi_count>1 and pdi_count<=3) then 'PDI_2to3' when (pdi_count>3 and pdi_count<=12) then 'PDI_4to12' when pdi_count>12 then 'PDI_12_more' else 'PDI_0' end as pdi_setup_bucket from fpd")
fut_pdi_df.createOrReplaceTempView('fpd')

foreclosed_data = zfi_65_28_feb.select('Contract_No','Realized_per_con')
foreclosed_data.createOrReplaceTempView("fd")
foreclosed_data =  spark.sql("select Contract_No, min(Realized_per_con) as Realized_per_con from fd group by Contract_No")
foreclosed_data=foreclosed_data.filter('Realized_per_con>=100')
foreclosed_data=foreclosed_data.withColumn('foreclosed_flag',lit(1))
foreclosed_data =foreclosed_data.dropDuplicates()
foreclosed_data.createOrReplaceTempView('fcd')

fut_pdi_summ=spark.sql("select fpd.*,fcd.Realized_per_con,fcd.foreclosed_flag from fpd left join fcd on fpd.Contract_No=fcd.Contract_No")
fut_pdi_summ.createOrReplaceTempView('fps')
fut_pdi_summ=spark.sql("select *, case when foreclosed_flag=1 then 'Foreclosed' else pdi_setup_bucket end as pdi_setup_bucket_new from fps")
fut_pdi_summ.createOrReplaceTempView('fps')
fut_pdi_summ_final=spark.sql("Select New_Branch,pdi_setup_bucket_new,count(*) as count from fps group by New_Branch,pdi_setup_bucket_new")
fut_pdi_summ_final.createOrReplaceTempView('fpsf')

ct_fut_pdi=spark.sql('select New_Branch, sum(case when pdi_setup_bucket_new = "PDI_0" then count end) as PDI_0,sum(case when pdi_setup_bucket_new = "PDI_1" then count end) as PDI_1,sum(case when pdi_setup_bucket_new = "PDI_2to3" then count end) as PDI_2to3,sum(case when pdi_setup_bucket_new = "PDI_4to12" then count end) as PDI_4to12,sum(case when pdi_setup_bucket_new = "PDI_12_more" then count end) as PDI_12_more,sum(case when pdi_setup_bucket_new = "Foreclosed" then count end) as Foreclosed from fpsf group by New_Branch')

ct_fut_pdi.createOrReplaceTempView('cfp')

#ct_fut_pdi= fut_pdi_summ_final.groupBy('New_Branch').pivot('pdi_setup_bucket_new').sum('count')


base_inflow_10=spark.sql("Select bi9.*,cfp.Foreclosed,cfp.PDI_0,cfp.PDI_1,cfp.PDI_2to3,cfp.PDI_4to12,cfp.PDI_12_more from bi9 left join cfp on bi9.New_Branch=cfp.New_Branch")
base_inflow_10.createOrReplaceTempView('bi10')


contract_pay=con_pay_select
contract_pay.createOrReplaceTempView('c_p')


## Logic Change 
# select contracts whose pdi returned date is in last 3 months and map it with only recovery.
ro_contract_pay_1=spark.sql('Select distinct reference_document as ContractNo from c_p where pdi_returned_date >= date_add(last_day(now() - interval 04 months),1) and pdi_returned_date <=last_day(now() - interval 01 months)')
ro_contract_pay_1.createOrReplaceTempView('rcp1')


#ro_contract_pay_1.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/emi_collection_intermediate_file",mode="append",header="true")
#inflow_1 = inflow_current_month
#inflow_1.createOrReplaceTempView('inf')

## reading recovery of current month

recovery=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_Recovery/recovery.csv",header =True,inferSchema=True)

recovery=recovery.withColumnRenamed('Contract No.','Contract_No')

recovery=recovery.withColumn('rec_date',to_date('recovered_month_year','yyyy-MM'))

recovery_filter= recovery.filter('rec_date >= date_add(last_day(now() - interval 1 months),1) and rec_date <= last_day(now())')

recovery_filter.createOrReplaceTempView('rf')

ro_contracts_recovered=spark.sql('select rcp1.* from rcp1 inner join rf on rcp1.ContractNo=rf.Contract_No')



#inflow_recovery= spark.sql('select inf.Contract_No from inf inner join rf on rf.Contract_No=inf.Contract_No')

#inflow_recovery.createOrReplaceTempView('i_r')

#inflow_recovery.coalesce(1).write.csv("s3://cmh-process-data/test-1/ro_contract_pay_1",mode="append",header="true")

## ro inner join with inflow_recovery

#ro_contracts_future=spark.sql('select rcp1.* from rcp1 inner join i_r on rcp1.ContractNo=i_r.Contract_No')

#ro_contracts_recovered=ro_contracts_recovered.withColumn('ro_status',lit('pdi_available_but_likely_future_ro'))
ro_contracts_recovered.createOrReplaceTempView('rcf')


## 
ro_contracts_future_final=spark.sql('select rcf.ContractNo,obi.New_Branch from rcf inner join obi on obi.Contract_No=rcf.ContractNo')
ro_contracts_future_final.createOrReplaceTempView('rcff')

#ro_contracts_future_final_summ = spark.sql('select New_Branch, count(ro_status) as pdi_available_but_likely_future_ro from rcff group by New_Branch')

#ro_contracts_future_final_summ.createOrReplaceTempView('rcffs')

## 
# change Repalce the date with now() later

pdi_modified=spark.sql('Select distinct reference_document as ContractNo from c_p where pdi_received_date between date_add(last_day(now() - interval 01 month),1) and last_day(now()) and installment_date>= date_add(last_day(now()),1)')

#pdi_modified.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/emi_collection_intermediate_file",mode="append",header="true")

pdi_modified.createOrReplaceTempView('pm')

ro_contracts_present=spark.sql('select rcff.ContractNo from rcff inner join pm on rcff.ContractNo=pm.ContractNo')

ro_contracts_present=ro_contracts_present.withColumn('ro_status',lit('contracts_with_fresh_pdi_setup'))

ro_contracts_present.createOrReplaceTempView('rcp')

## New Logic

ro_contracts_present_summ = spark.sql('select rcff.ContractNo, rcff.New_Branch,rcp.ro_status from rcff left join rcp on rcff.ContractNo = rcp.ContractNo')

ro_contracts_present_summ=ro_contracts_present_summ.na.fill('pdi_available_but_likely_future_ro')

ro_contracts_present_summ.createOrReplaceTempView('rcc')

ro_contracts_future_present_Summ= spark.sql('select New_Branch, count(case when ro_status ="contracts_with_fresh_pdi_setup" then ContractNo end) as contracts_with_fresh_pdi_setup,count(case when ro_status ="pdi_available_but_likely_future_ro" then ContractNo end) as pdi_available_but_likely_future_ro  from rcc group by New_Branch')

ro_contracts_future_present_Summ.createOrReplaceTempView('rcps')




#ro_contracts_present_summ = spark.sql('select New_Branch, count(ro_status) as contracts_with_fresh_pdi_setup from rcp group by New_Branch ')

#ro_contracts_present_summ.createOrReplaceTempView('rcps')


base_inflow_11=spark.sql('select bi10.*,rcps.pdi_available_but_likely_future_ro,rcps.contracts_with_fresh_pdi_setup from bi10 left join rcps on bi10.New_Branch=rcps.New_Branch')

base_inflow_11.createOrReplaceTempView('bi11')

#base_inflow_12=spark.sql('select bi11.*,rcps.contracts_with_fresh_pdi_setup from bi11 left join rcps on bi11.New_Branch=rcps.New_Branch')

base_inflow_11=base_inflow_11.withColumn('run_date', lit(current_date()))

base_inflow_11 = base_inflow_11.withColumn('run_date', base_inflow_11.run_date.cast('string'))

base_inflow_11=base_inflow_11.na.fill(0)
base_inflow_11.createOrReplaceTempView('bi12')

#base_inflow_11.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/emi_collection_intermediate_file",mode="append",header="true")

base_inflow_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/emi_collection_intermediate_file/emi_collections.csv",header =True,inferSchema=True)

#base_inflow_old=base_inflow_old.withColumn('run_date',to_date('run_date','dd-MM-yyyy'))
base_inflow_old=base_inflow_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
base_inflow_old = base_inflow_old.withColumn('run_date',base_inflow_old.run_date.cast('string'))
base_inflow_old.createOrReplaceTempView('bio')

base_inflow_12_final= spark.sql('select * from bio union all select * from bi12')


## output-2

base_inflow_12_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/emi_collection_intermediate_file",mode="append",header="true")


## Taking the max dates for below tables as last_modified dates

table_1 = spark.sql('select "Base_Table" as Table_name, max(date) as last_modified, current_date() as run_date from base')

table_2 = spark.sql('select "Inflow_Table" as Table_name, max(date) as last_modified, current_date() as run_date from inflow')

table_A = table_1.unionAll(table_2)

table_3 = spark.sql('select "EMI_Target_Table" as Table_name, max(date) as last_modified, current_date() as run_date from t')

table_B = table_A.unionAll(table_3)

table_4 = spark.sql('select "ZFI_065_Table" as Table_name, max(dw_lastupdated) as last_modified, current_date() as run_date from zfeb')

table_C = table_B.unionAll(table_4)

table_5 = spark.sql('select "EMI_Dialler_table" as Table_name, max(last_modified_date) as last_modified, current_date() as run_date from dd')

table_D = table_C.unionAll(table_5)

table_6 = spark.sql('select "Dialler_rec_log_Table" as Table_name, max(Start_time) as last_modified, current_date() as run_date from drl')

table_E = table_D.unionAll(table_6)

table_7 = spark.sql('select "Agent_Role_Mapping_Table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from ar')

table_F = table_E.unionAll(table_7)

table_8 = spark.sql('select "ZFI_PDI_Table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from cp')

table_G = table_F.unionAll(table_8)

table_9 = spark.sql('select "Recovery_Table" as Table_name, max(rec_date) as last_modified, current_date() as run_date from rf')

table_H = table_G.unionAll(table_9)

table_10 = spark.sql('select "Field_App_Intermediate_Table" as Table_name, max(Rundate) as last_modified, current_date() as run_date from fa')
table_I = table_H.unionAll(table_10)

table_I=table_I.withColumn('run_date',table_I.run_date.cast('string')).withColumn('last_modified',table_I.last_modified.cast('string'))

table_I.createOrReplaceTempView('tf')

# Reading the old intermediate file
emi_table_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/Table_details/table_details.csv",header =True,inferSchema=True)


## Formating the dates of last_modified and run_date column. 

emi_table_old=emi_table_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd')).withColumn('last_modified',to_date('last_modified','yyyy-MM-dd'))

emi_table_old=emi_table_old.withColumn('run_date',emi_table_old.run_date.cast('string')).withColumn('last_modified',emi_table_old.last_modified.cast('string'))

emi_table_old.createOrReplaceTempView('nto')

emi_table_final= spark.sql('select * from nto union all select * from tf')


## output-2
## Writing the final intermediate file to S3 process-data
emi_table_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/Table_details",mode="append",header="true")


# Reading the Required System Libraries
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/emi_collection_intermediate_file/emi_collections.csv"
os.system(cmd1)
time.sleep(10)

cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/Table_details/table_details.csv"
os.system(cmd2)
time.sleep(10)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
time.sleep(10)


file_path_emi_collection = "s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/emi_collection_intermediate_file/"
time.sleep(10)
file_path_Table_details = "s3://cmh-process-data/intermediate_files/Emi_Collection_Dashboard/Table_details/"
time.sleep(10)

created_file_path_emi_collection = fs.globStatus(Path(file_path_emi_collection + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)

fs.rename(created_file_path_emi_collection,Path(file_path_emi_collection + "emi_collections.csv"))
time.sleep(10)
fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))




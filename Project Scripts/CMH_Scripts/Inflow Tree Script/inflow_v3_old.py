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

inflow=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_final_inflow_previous_month/final_inflow.csv",header=True,inferSchema=True)

inflow = inflow.withColumn('date',to_date('month_year','yyyy-MM'))

inflow=inflow.withColumnRenamed('Contract No.','Contract_No').withColumnRenamed('New Branch','New_Branch')
inflow.createOrReplaceTempView('inflow')

inflow=spark.sql("select distinct Contract_No,New_Branch from inflow")
inflow.createOrReplaceTempView('if')


zfi_65_1_feb=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_065_monthly/ZFI_065_monthly_file.csv",header=True,inferSchema=True)

#zfi_65_1_feb=zfi_65_1_feb.withColumn('dw_lastupdated',to_date('dw_lastupdated','dd/MM/yyyy')).withColumn('SaleDate',to_date('SaleDate','yyyy-MM-dd'))

zfi_65_1_feb=zfi_65_1_feb.withColumn('dw_lastupdated',to_date('dw_lastupdated','yyyy-MM-dd')).withColumn('SaleDate',to_date('SaleDate','yyyy-MM-dd'))

zfi_65_1_feb.createOrReplaceTempView('z')

zfi_65_1_feb_1=zfi_65_1_feb.select('ContractNo','EMIRealizedCount','SaleDate')

zfi_65_1_feb_1.createOrReplaceTempView('zfi1')

inflow_1=spark.sql('select if.Contract_No,if.New_Branch, zfi1.EMIRealizedCount,zfi1.SaleDate from if left join zfi1 on if.Contract_No=zfi1.ContractNo')
inflow_1.createOrReplaceTempView('if1')

inflow_1=spark.sql('select Contract_No,min(EMIRealizedCount) as EMI_Realized_count,SaleDate from if1 group by Contract_No,SaleDate')
inflow_1.createOrReplaceTempView('if1')

## change inflow will be calculated using inflow sheet on S3##

# S3 path

cancel_req_1= spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_cancellation_request/cancellation_request.csv",header=True,inferSchema=True)
cancel_req_1=cancel_req_1.withColumn('dw_last_modified_date',to_date('dw_last_modified_date','yyyy-MM-dd'))
cancel_req_1.createOrReplaceTempView('c1')

cancel_req_1=cancel_req_1.select('contractID','cancellation_trigger','StatusOfRequest')
cancel_req_1.createOrReplaceTempView('cr1')

cancel_req=spark.sql('select if.*, cr1.cancellation_trigger,cr1.StatusOfRequest from if inner join cr1 on if.Contract_No=cr1.contractID')

cancel_req=cancel_req.withColumn('Cancel',when(cancel_req.cancellation_trigger =='Rescission Cancellation Request','Rescission_Cancellation_Request').otherwise('General_Cancellation')).withColumn('status_request',when(cancel_req.StatusOfRequest=='Resolved','Resolved').otherwise('Pending'))

cancel_req=cancel_req.withColumn('trigger_value',when(cancel_req.Cancel =='Rescission_Cancellation_Request',10).otherwise(1)).withColumn('status_value',when(cancel_req.status_request=='Resolved',1).otherwise(2))

cancel_req=cancel_req.withColumn('total_value',(cancel_req.trigger_value*cancel_req.status_value))

cancel_req=cancel_req.drop('cancellation_trigger').drop('StatusOfRequest')

cancel_req.createOrReplaceTempView('cr')

cancel_req_priority=spark.sql('select s.Contract_No,s.Cancel,s.status_request,s.total_value from (select Contract_No,Cancel,status_request,total_value,row_number() over(partition by Contract_No order by total_value desc) as rownum1 from cr) s where rownum1 =1')

cancel_req_priority.createOrReplaceTempView('crp')

inflow_can=spark.sql("select if1.*,crp.Cancel,crp.status_request,crp.total_value from if1 left join crp on if1.Contract_No=crp.Contract_No")

inflow_can.createOrReplaceTempView('ic')

inflow_can=inflow_can.na.fill("Active_with_no_cancellation",subset=['Cancel'])

inflow_can=inflow_can.withColumn('run_date',lit(current_date()))

#inflow_can=inflow_can.withColumn('run_date',lit('2019-10-01'))

inflow_can.createOrReplaceTempView('ic')

inflow_can_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/inflow_can/Inflow_cancel.csv",header=True,inferSchema=True)

inflow_can_old=inflow_can_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

#inflow_can_old.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/inflow_can",mode="append",header="true")

## check run date format

inflow_can_old.createOrReplaceTempView('ico')

inflow_can_final = spark.sql('select * from ico union all select * from ic')
inflow_can_final=inflow_can_final.withColumn('run_date',inflow_can_final.run_date.cast('string'))

# Output-1
inflow_can_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/inflow_can",mode="append",header="true")

cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/inflow_can/Inflow_cancel.csv"
os.system(cmd1)
time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)


fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())

file_path_inflow = "s3://cmh-process-data/intermediate_files/EMI_Inflow/inflow_can/"
created_file_path_inflow = fs.globStatus(Path(file_path_inflow + "part*.csv"))[0].getPath()
time.sleep(10)

fs.rename(created_file_path_inflow,Path(file_path_inflow + "Inflow_cancel.csv"))


### EMI RZ COUNT ##

active_with_nc= inflow_can.filter("Cancel=='Active_with_no_cancellation'")

active_with_nc=active_with_nc.withColumn('emi_rz_cat', when(active_with_nc.EMI_Realized_count== 0,'emi_rz_0').when((active_with_nc.EMI_Realized_count>0) & (active_with_nc.EMI_Realized_count<=5),'emi_rz_1_5').otherwise('emi_rz_>5'))
active_with_nc.createOrReplaceTempView('awn')

active_with_emi_0= active_with_nc.filter("emi_rz_cat=='emi_rz_0'")
active_with_emi_0.createOrReplaceTempView('awe')

# change Dynamic i.e running in aug sale date between should be in may and june

active_with_emi0_rz=spark.sql('select Contract_No,case when SaleDate >= date_add(last_day(now() - interval 04 months),1) and  SaleDate <= last_day(now() - Interval 2 months) then "FIF" else "upgrade/ppc" end as Flag from awe')
active_with_emi0_rz.createOrReplaceTempView('aw0r')


active_with_emi_1_5_rz = active_with_nc.filter("emi_rz_cat=='emi_rz_1_5'")
active_with_emi_1_5_rz=active_with_emi_1_5_rz.select('Contract_No')
active_with_emi_1_5_rz.createOrReplaceTempView('awe1_5')


recovery_file_1=spark.read.option("delimiter",",").csv(r"s3://cmh-process-data/master-tables/master_table_Recovery/recovery.csv",header=True,inferSchema=True)
recovery_file_1 = recovery_file_1.withColumn('rec_date',to_date('recovered_month_year','yyyy-MM'))
recovery_file_1=recovery_file_1.withColumnRenamed('Contract No.','Contract_No')
recovery_file_1.createOrReplaceTempView('rf')

# change Dynamic for august dashboard  last 09 month from july month is last month and script will run in month of sept. 1 nov 2018 to 31st july 19 
# check once

recovery_file_09=spark.sql('select *, date_add(last_day(now() - interval 11 months),1) as offset from rf')

recovery_file_last_9_mo = recovery_file_09.filter("rec_date>=offset")
recovery_file_last_9_mo=recovery_file_last_9_mo.dropDuplicates(['Contract_No'])
recovery_file_last_9_mo.createOrReplaceTempView('rfl9m')

recovery_bucket_1_5=spark.sql('select rfl9m.*, case when rec_date>= date_add(last_day(now() - interval 05 months),1) and rec_date<=last_day(now() - interval 02 months) then "normalised_in_last_3_months" when rec_date>= date_add(last_day(now()- interval 08 months),1) and rec_date<=last_day(now() - interval 05 months) then "normalised_4_6_months_ago" when rec_date>= date_add(last_day(now()- interval 11 months),1) and rec_date<=last_day(now() - interval 08 months) then "normalised_7_9_months_ago" end as flag_normalised from rfl9m')

recovery_bucket_1_5=recovery_bucket_1_5.filter('flag_normalised is not null')
recovery_bucket_1_5.createOrReplaceTempView('nbo')


active_with_emi_1_5_rz=spark.sql("select awe1_5.*,nbo.flag_normalised from awe1_5 left join nbo on awe1_5.Contract_No=nbo.Contract_No")

active_with_emi_1_5_rz=active_with_emi_1_5_rz.na.fill('First_time_inflow',subset=['flag_normalised'])
active_with_emi_1_5_rz.createOrReplaceTempView('a15r')


###### active Contracts with >5 emi's realised #######

active_with_morethan_5_emi_rz= active_with_nc.filter("emi_rz_cat=='emi_rz_>5'")

active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.select('Contract_No')

active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5')


# change Dynamic

# change check in recovery_file_last_9_mo if yes normalised but back in od and first time inflow

active_with_morethan_5_emi_rz=spark.sql("select awm5.*,nbo.flag_normalised from awm5 left join nbo on awm5.Contract_No=nbo.Contract_No")

active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.na.fill("First_time_inflow",subset=['flag_normalised'])

active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5r')

###### PDI Not Available #####

zsd_excluded=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_pdinone_prevmonth/pdinone_contracts.csv",header=True,inferSchema=True)

#zsd_excluded=zsd_excluded.withColumn('date',to_date('month_year','yyyy-MM'))
zsd_excluded.createOrReplaceTempView('pnb')


###### FIF PDI Not Available Contracts ########

active_with_emi0_rz =spark.sql('select aw0r.*, pnb.upcoming_pdi_type from aw0r left join pnb on aw0r.Contract_No=pnb.CONTRACT_NUMBER')

active_with_emi0_rz=active_with_emi0_rz.withColumn('pdi_contracts_status',when(active_with_emi0_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

active_with_emi0_rz=active_with_emi0_rz.withColumn('run_date',lit(current_date()))
active_with_emi0_rz.createOrReplaceTempView('awe0r')


active_with_emi0_rz_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Active0/active_zero.csv",header=True,inferSchema=True)

active_with_emi0_rz_old=active_with_emi0_rz_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

#active_with_emi0_rz_old.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Active0",mode="append",header="true")

active_with_emi0_rz_old.createOrReplaceTempView('awe0z0')

active_with_emi0_rz_final=spark.sql('select * from awe0r union all select * from awe0z0')

active_with_emi0_rz_final=active_with_emi0_rz_final.withColumn('run_date',active_with_emi0_rz_final.run_date.cast('string'))

# output-2

active_with_emi0_rz_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Active0",mode="append",header="true")


cmd2="aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/Active0/active_zero.csv"
os.system(cmd2)
time.sleep(10)

file_path_active_zero = "s3://cmh-process-data/intermediate_files/EMI_Inflow/Active0/"

created_file_path_active_zero = fs.globStatus(Path(file_path_active_zero + "part*.csv"))[0].getPath()

time.sleep(10)
fs.rename(created_file_path_active_zero,Path(file_path_active_zero + "active_zero.csv"))

#### 1-5 EMI RZ with PDI Not Available Contracts #######

active_with_emi_1_5_rz =spark.sql('select a15r.*, pnb.upcoming_pdi_type from a15r left join pnb on a15r.Contract_No=pnb.CONTRACT_NUMBER')

active_with_emi_1_5_rz=active_with_emi_1_5_rz.withColumn('pdi_contracts_status',when(active_with_emi_1_5_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))
active_with_emi_1_5_rz=active_with_emi_1_5_rz.withColumn('run_date',lit(current_date()))
active_with_emi_1_5_rz.createOrReplaceTempView('awe15r')

active_with_emi_1_5_rz_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_1_5/active_1_5.csv",header=True,inferSchema=True)


active_with_emi_1_5_rz_old=active_with_emi_1_5_rz_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

#active_with_emi_1_5_rz_old.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_1_5",mode="append",header="true")

active_with_emi_1_5_rz_old.createOrReplaceTempView('awe15ro')

active_with_emi_1_5_rz_final=spark.sql('select * from awe15r union all select * from awe15ro ')

active_with_emi_1_5_rz_final=spark.sql('select * from awe15r union all select * from awe15ro ')

active_with_emi_1_5_rz_final=active_with_emi_1_5_rz_final.withColumn('run_date', active_with_emi_1_5_rz_final.run_date.cast('string'))

## output-3

active_with_emi_1_5_rz_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_1_5",mode="append",header="true")

cmd3="aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/active_1_5/active_1_5.csv"
os.system(cmd3)
time.sleep(10)

file_path_active_1_5 = "s3://cmh-process-data/intermediate_files/EMI_Inflow/active_1_5/"
created_file_path_active_1_5 = fs.globStatus(Path(file_path_active_1_5 + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path_active_1_5,Path(file_path_active_1_5 + "active_1_5.csv"))

#### More than 5 EMI RZ with PDI Not Available Contracts #######

active_with_morethan_5_emi_rz=spark.sql('select awm5r.*,pnb.upcoming_pdi_type from awm5r left join pnb on awm5r.Contract_No=pnb.CONTRACT_NUMBER')

active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.withColumn('pdi_contracts_status',when(active_with_morethan_5_emi_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

active_with_morethan_5_emi_rz = active_with_morethan_5_emi_rz.withColumn('run_date',lit(current_date()))

active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5r1')

active_with_morethan_5_emi_rz_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_more_5/active_more_5.csv",header=True,inferSchema=True)

active_with_morethan_5_emi_rz_old=active_with_morethan_5_emi_rz_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
active_with_morethan_5_emi_rz_old.createOrReplaceTempView('awm5ro')
#active_with_morethan_5_emi_rz_old.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_more_5",mode="append",header="true")
active_with_morethan_5_emi_rz_final=spark.sql('select * from awm5r1 union all select * from awm5ro ')

active_with_morethan_5_emi_rz_final=active_with_morethan_5_emi_rz_final.withColumn('run_date',active_with_morethan_5_emi_rz_final.run_date.cast('string'))


## output-4
active_with_morethan_5_emi_rz_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_more_5",mode="append",header="true")

cmd4="aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/active_more_5/active_more_5.csv"
os.system(cmd4)
time.sleep(10)

file_path_active_more_5 = "s3://cmh-process-data/intermediate_files/EMI_Inflow/active_more_5/"
created_file_path_active_more_5 = fs.globStatus(Path(file_path_active_more_5 + "part*.csv"))[0].getPath()

time.sleep(10)
fs.rename(created_file_path_active_more_5,Path(file_path_active_more_5 + "active_more_5.csv"))	


table_1 = spark.sql('select "Inflow_table" as Table_name, max(date) as last_modified, current_date() as run_date from inflow')

table_2 = spark.sql('select "ZFI065_table" as Table_name, max(dw_lastupdated) as last_modified, current_date() as run_date from z')

table_A = table_1.unionAll(table_2)

table_3 = spark.sql('select "Cancellation_request_table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from c1')

table_B = table_A.unionAll(table_3)

table_4 = spark.sql('select "Recovery_table" as Table_name, max(rec_date) as last_modified, current_date() as run_date from rf')

table_C = table_B.unionAll(table_4)

table_5 = spark.sql('select "PDI_None_table" as Table_name, max(date) as last_modified, current_date() as run_date from pnb')


#table_5 = spark.sql('select "PDI_None_table" as Table_name, "2019-09-01" as last_modified, current_date() as run_date')
table_D = table_C.unionAll(table_5)

table_D.createOrReplaceTempView('tf')

#table_D.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details",mode="append",header="true")


inflow_table_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details/table_details.csv",header =True,inferSchema=True)

# inflow_table_old=inflow_table_old.withColumn('last_modified',to_date('last_modified','dd-MM-yyyy')).withColumn('run_date',to_date('run_date','dd-MM-yyyy'))

inflow_table_old.createOrReplaceTempView('nto')

inflow_table_final= spark.sql('select * from nto union all select * from tf')

inflow_table_final=inflow_table_final.withColumn('last_modified',inflow_table_final.last_modified.cast('string')).withColumn('run_date',inflow_table_final.run_date.cast('string'))

inflow_table_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details",mode="append",header="true")

cmd5="sudo aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details/table_details.csv"
os.system(cmd5)
time.sleep(10)

file_path_Table_details = "s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details/"
created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()

time.sleep(10)
fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))







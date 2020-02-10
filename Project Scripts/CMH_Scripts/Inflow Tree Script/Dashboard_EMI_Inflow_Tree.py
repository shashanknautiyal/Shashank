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

#CR dated on 21-11-2019 Take Current Month Inflow file from S3 Master tables.

# Reading Inflow File from Master Tables from S3 Process data Bucket.



def Business_day_calculator(today_date):
	business_day = (today_date-datetime.timedelta(days= 1)).strftime("%Y-%m-%d")
	
	return business_day

business_day = Business_day_calculator(today_date=datetime.datetime.today())

#business_day = Business_day_calculator(today_date=datetime.datetime.strptime("2020-02-01","%Y-%m-%d"))

inflow_file=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_final_inflow/final_inflow.csv",header=True,inferSchema=True)

# Formatting the date in date column
inflow_file = inflow_file.withColumn('date',to_date('month_year','yyyy-MM'))

# Renaming the Columns.
inflow_file=inflow_file.withColumnRenamed('Contract No.','Contract_No').withColumnRenamed('New Branch','New_Branch')

## please note createOrReplaceTempView function is used to create a temporary view to perform sql Queries and will be used serval times in the below code.
inflow_file.createOrReplaceTempView('inflow')


#selecting the Distinct Contract_No from inflow_file

inflow=spark.sql("select distinct Contract_No,New_Branch from inflow")
inflow.createOrReplaceTempView('if')

## Read ZOD File 

ZOD_file=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_zod/zod_inflow.csv",header=True,inferSchema=True)

ZOD_file=ZOD_file.withColumn('dw_lastupdated',to_date('dw_lastupdated','yyyy-MM-dd'))

ZOD_file.createOrReplaceTempView('zod_file')

#CR -1 dated on 02-12-2019 Take ZFI065_table Daily File.
#CR -2 dated 06-12-2019 Take ZFI_065_monthly_file
 

# Reading ZFI_065 Monthly file from Master Tables from S3 Process data Bucket.

zfi_65_Monthly_table=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_065_monthly/ZFI_065_monthly_file.csv",header=True,inferSchema=True)

# Formatting the Date of dw_lastupdated and SaleDate Columns.

zfi_65_Monthly_table=zfi_65_Monthly_table.withColumn('dw_lastupdated',to_date('dw_lastupdated','yyyy-MM-dd')).withColumn('SaleDate',to_date('SaleDate','yyyy-MM-dd'))

zfi_65_Monthly_table.createOrReplaceTempView('z')

# Selecting the required columns.
zfi_65_select_columns=zfi_65_Monthly_table.select('ContractNo','EMIRealizedCount','SaleDate')

zfi_65_select_columns.createOrReplaceTempView('zfi1')

# Doing a left join on infow and zfi_65_select_columns and selecting the reuqired columns
inflow_Zfi_65=spark.sql('select if.Contract_No,if.New_Branch, zfi1.EMIRealizedCount,zfi1.SaleDate from if left join zfi1 on if.Contract_No=zfi1.ContractNo')
inflow_Zfi_65.createOrReplaceTempView('if1')

# summarising on Contract_No and SaleDate and taking minimum of EMIRealizedCount.

## Logic Update 10-12-2019 Have to Take Maximum EMIRealizedCount
inflow_Zfi_65_summary=spark.sql('select Contract_No,max(EMIRealizedCount) as EMI_Realized_count,SaleDate from if1 group by Contract_No,SaleDate')
inflow_Zfi_65_summary.createOrReplaceTempView('if1')

## change inflow will be calculated using inflow sheet on S3##

# Reading Cancellation request table from S3 process-data.
cancellation_request_table= spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_cancellation_request/cancellation_request.csv",header=True,inferSchema=True)

## Formatting the date of dw_last_modified_date column.
cancellation_request_table=cancellation_request_table.withColumn('dw_last_modified_date',to_date('dw_last_modified_date','yyyy-MM-dd'))
cancellation_request_table.createOrReplaceTempView('c1')

# Selecting required columns from cancell_request_table
cancellation_request_select_columns=cancellation_request_table.select('contractID','cancellation_trigger','StatusOfRequest')
cancellation_request_select_columns.createOrReplaceTempView('cr1')

# Applying inner join on inflow table and cancellation_request_select_columns
Inflow_cancel_request=spark.sql('select if.*, cr1.cancellation_trigger,cr1.StatusOfRequest from if inner join cr1 on if.Contract_No=cr1.contractID')

## Converting the categories in cancel column to Rescission_Cancellation_Request and General_Cancellation and categories in status_request column to Resolved and pending
 
Inflow_cancel_request=Inflow_cancel_request.withColumn('Cancel',when(Inflow_cancel_request.cancellation_trigger =='Rescission Cancellation Request','Rescission_Cancellation_Request').otherwise('General_Cancellation')).withColumn('status_request',when(Inflow_cancel_request.StatusOfRequest=='Resolved','Resolved').otherwise('Pending'))

# Assigning the weights to categories in cancel and status_request columns and created two new columns trigger_value and status_value to store the weighted values.

Inflow_cancel_request=Inflow_cancel_request.withColumn('trigger_value',when(Inflow_cancel_request.Cancel =='Rescission_Cancellation_Request',10).otherwise(1)).withColumn('status_value',when(Inflow_cancel_request.status_request=='Resolved',1).otherwise(2))

# Created new column to store the product of trigger_value and status_value.
Inflow_cancel_request=Inflow_cancel_request.withColumn('total_value',(Inflow_cancel_request.trigger_value*Inflow_cancel_request.status_value))

# Dropping the unwanted columns
Inflow_cancel_request=Inflow_cancel_request.drop('cancellation_trigger').drop('StatusOfRequest')
Inflow_cancel_request.createOrReplaceTempView('cr')


# selecting unique Contract_No with having highest value.
Inflow_cancel_request_priority=spark.sql('select s.Contract_No,s.Cancel,s.status_request,s.total_value from (select Contract_No,Cancel,status_request,total_value,row_number() over(partition by Contract_No order by total_value desc) as rownum1 from cr) s where rownum1 =1')
Inflow_cancel_request_priority.createOrReplaceTempView('crp')

# Selecting Cancel, status_request,and total_value column from above table and left joining it with inflow_Zfi_65_summary table.
inflow_cancellation_priortized=spark.sql("select if1.*,crp.Cancel,crp.status_request,crp.total_value from if1 left join crp on if1.Contract_No=crp.Contract_No")

inflow_cancellation_priortized.createOrReplaceTempView('ic')

### CR dated 21-11-2019

# Reading Cancellation Interaction table from S3 process-data
cancellation_interaction = spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_cancellation_interaction/cancellation_interaction.csv",header=True,inferSchema=True) 
cancellation_interaction.createOrReplaceTempView('cirt')

# selecting interaction_state_value from cancellation_interaction table and left joining it on inflow_cancellation_priortized table
inflow_cancellation_priortized= spark.sql('select ic.*,cirt.interaction_state_value, cirt.created, cirt.acontractid from ic left join cirt on ic.Contract_No = cirt.acontractid')

inflow_cancellation_priortized=inflow_cancellation_priortized.dropDuplicates(['Contract_No'])
inflow_cancellation_priortized.createOrReplaceTempView('ic')

# checking if interaction_state_value column value is not null then category in cancel column changes to Cancellation interactions.
inflow_cancellation_priortized = inflow_cancellation_priortized.withColumn('Cancel',when(inflow_cancellation_priortized.interaction_state_value.isNotNull(),"Cancellation interactions").otherwise(inflow_cancellation_priortized.Cancel))

###

# Filling all the null values in cancel column with new category "Active_with_no_cancellation".
inflow_cancellation_priortized=inflow_cancellation_priortized.na.fill("Active_with_no_cancellation",subset=['Cancel'])

## Adding a new column run_date containing the current day timestamp
inflow_cancellation_priortized=inflow_cancellation_priortized.withColumn('run_date',lit(current_date()))
inflow_cancellation_priortized.createOrReplaceTempView('ic')


## Reading the old Intermediate File from S3 process-data
inflow_cancellation_priortized_old_table = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/inflow_can/Inflow_cancel.csv",header=True,inferSchema=True)

## Formating the run_date column to date format
inflow_cancellation_priortized_old_table=inflow_cancellation_priortized_old_table.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
inflow_cancellation_priortized_old_table.createOrReplaceTempView('ico')

## Appending the old intermediate files with new intermediate file.
inflow_cancellation_priortized_final = spark.sql('select * from ico union all select * from ic')

# Converting the run_date to String
inflow_cancellation_priortized_final=inflow_cancellation_priortized_final.withColumn('run_date',inflow_cancellation_priortized_final.run_date.cast('string'))

# Writing the Output file to S3 process-data
inflow_cancellation_priortized_final.repartition(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/inflow_can",mode="append",header="true")

time.sleep(20)

#Deleting the old intermediate File 
cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/inflow_can/Inflow_cancel.csv"
os.system(cmd1)
time.sleep(10)

# Reading the Required System Libraries
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
time.sleep(10)

# Setting up the path for S3 process-data bucket 
fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())

file_path_inflow = "s3://cmh-process-data/intermediate_files/EMI_Inflow/inflow_can/"
created_file_path_inflow = fs.globStatus(Path(file_path_inflow + "part*.csv"))[0].getPath()
time.sleep(10)

# Renaming the Existing file 
fs.rename(created_file_path_inflow,Path(file_path_inflow + "Inflow_cancel.csv"))


### EMI RZ COUNT ##

## Applying Filter on inflow_cancellation_priortized and selecting all the contracts with cancel status Active_with_no_cancellation.

active_contracts_with_no_cancellation= inflow_cancellation_priortized.filter("Cancel=='Active_with_no_cancellation'")

## Creating a new column emi_rz_cat by appying case condition on EMI_Realized_count column. 
active_contracts_with_no_cancellation=active_contracts_with_no_cancellation.withColumn('emi_rz_cat', when(active_contracts_with_no_cancellation.EMI_Realized_count== 0,'emi_rz_0').when((active_contracts_with_no_cancellation.EMI_Realized_count>0) & (active_contracts_with_no_cancellation.EMI_Realized_count<=5),'emi_rz_1_5').otherwise('emi_rz_>5'))


active_contracts_with_no_cancellation=active_contracts_with_no_cancellation.withColumn('business_date',lit(business_day))

active_contracts_with_no_cancellation.createOrReplaceTempView('awn')

## Applying Filter on active_contracts_with_no_cancellation table and selecting all the contracts with emi_rz_cat as "emi_rz_0".

active_with_emi_0= active_contracts_with_no_cancellation.filter("emi_rz_cat=='emi_rz_0'")
active_with_emi_0.createOrReplaceTempView('awe')


# Categorizing the Contracts into FIF and upgrade/ppc based on the SaleDate.
## For Example if dashboard is running for December 2019 the sale date will from 1st sep 2019 to 31st oct 2019. 
active_with_emi0_rz=spark.sql('select Contract_No,case when SaleDate >= date_add(last_day(business_date - interval 03 months),1) and  SaleDate <= last_day(business_date - Interval 02 months) then "FIF" else "upgrade/ppc" end as Flag from awe')
active_with_emi0_rz.createOrReplaceTempView('aw0r')



## Applying Filter on active_contracts_with_no_cancellation table and selecting all the contracts with emi_rz_cat as "emi_rz_1_5".

active_with_emi_1_5_rz = active_contracts_with_no_cancellation.filter("emi_rz_cat=='emi_rz_1_5'")
active_with_emi_1_5_rz=active_with_emi_1_5_rz.select('Contract_No')
active_with_emi_1_5_rz.createOrReplaceTempView('awe1_5')


## Reading the recovery file from S3 process-data.
recovery_file=spark.read.option("delimiter",",").csv(r"s3://cmh-process-data/master-tables/master_table_Recovery/recovery.csv",header=True,inferSchema=True)

# Formatting the Date of recovered_month_year column
recovery_file = recovery_file.withColumn('rec_date',to_date('recovered_month_year','yyyy-MM'))

recovery_file=recovery_file.withColumn('business_date',lit(business_day))

# Renaming the columns
recovery_file=recovery_file.withColumnRenamed('Contract No.','Contract_No')
recovery_file.createOrReplaceTempView('rf')

# setting an offset date to filter the last 09 months Contracts in recovery fileLast 09 Months. Last nine Month leave Nov if the Dashboard is running for December Month.

## New Logic 11-12-2019 - Take Last Nine Months that is if December Month Dashboard is running take from Nov Month.
recovery_file=spark.sql('select *, date_add(last_day(business_date - interval 10 months),1) as offset from rf')

## Filtering the data for only last 09 months from recovered file
recovery_file_last_9_mo = recovery_file.filter("rec_date>=offset")
recovery_file_last_9_mo.createOrReplaceTempView('rf9')

recovery_file_last_9_months= spark.sql('select * from rf9 where rec_date between date_add(last_day(business_date- interval 10 months),1) and last_day(business_date - interval 01 months)')

recovery_file_last_9_months.createOrReplaceTempView('rfl')

recovery_file_last_9_Summ=spark.sql("select s.Contract_No,s.recovered_month_year,s.rec_date,s.offset,s.business_date from (select Contract_No, recovered_month_year, rec_date, offset,business_date, row_number() over(partition by Contract_No order by rec_date desc) as rownum1 from rfl) s where rownum1 = 1")

#recovery_file_last_9_mo=recovery_file_last_9_mo.dropDuplicates(['Contract_No'])
recovery_file_last_9_Summ.createOrReplaceTempView('rfl9m')

# Creating a new column flag_normalised and segregating the contracts based on the recovery dates as normalised_in_last_3_months,normalised_4_6_months_ago & normalised_7_9_months_ago.

recovery_file_bucket=spark.sql('select rfl9m.*, case when rec_date>= date_add(last_day(business_date - interval 04 months),1) and rec_date<=last_day(business_date - interval 01 months) then "normalised_in_last_3_months" when rec_date>= date_add(last_day(business_date- interval 07 months),1) and rec_date<=last_day(business_date - interval 04 months) then "normalised_4_6_months_ago" when rec_date>= date_add(last_day(business_date- interval 10 months),1) and rec_date<=last_day(business_date - interval 07 months) then "normalised_7_9_months_ago" end as flag_normalised from rfl9m')



#recovery_file_bucket=spark.sql('select rfl9m.*, case when rec_date>= date_add(last_day(now() - interval 05 months),1) and rec_date<=last_day(now() - interval 02 months) then "normalised_in_last_3_months" when rec_date>= date_add(last_day(now()- interval 08 months),1) and rec_date<=last_day(now() - interval 05 months) then "normalised_4_6_months_ago" when rec_date>= date_add(last_day(now()- interval 11 months),1) and rec_date<=last_day(now() - interval 08 months) then "normalised_7_9_months_ago" end as flag_normalised from rfl9m')


# Filtering the data where flag_normalised is not null 
recovery_file_bucket=recovery_file_bucket.filter('flag_normalised is not null')
recovery_file_bucket.createOrReplaceTempView('nbo')

# Joining the recovered contracts with active_with_emi_1_5_rz table. 
active_with_emi_1_5_rz=spark.sql("select awe1_5.*,nbo.flag_normalised from awe1_5 left join nbo on awe1_5.Contract_No=nbo.Contract_No")

## replacing the null Values in flag_normalised columns as First_time_inflow.
active_with_emi_1_5_rz=active_with_emi_1_5_rz.na.fill('First_time_inflow',subset=['flag_normalised'])
active_with_emi_1_5_rz.createOrReplaceTempView('a15r')


## Applying Filter on active_contracts_with_no_cancellation table and selecting all the contracts with emi_rz_cat as "emi_rz_>5".

active_with_morethan_5_emi_rz= active_contracts_with_no_cancellation.filter("emi_rz_cat=='emi_rz_>5'")
active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.select('Contract_No')
active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5')

# Joining the recovered contracts with active_with_morethan_5_emi_rz table.
active_with_morethan_5_emi_rz=spark.sql("select awm5.*,nbo.flag_normalised from awm5 left join nbo on awm5.Contract_No=nbo.Contract_No")

## replacing the null Values in flag_normalised columns as First_time_inflow.
active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.na.fill("First_time_inflow",subset=['flag_normalised'])
active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5r')

## Reading PDI None file from S3 process-data

#pdi_none_contracts=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_pdi_none/pdi_none.csv",header=True,inferSchema=True)

pdi_none_contracts=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_final_zsd/master_table_final_zsd",header=True,inferSchema=True)
pdi_none_contracts=pdi_none_contracts.withColumnRenamed('Contract No.','CONTRACT_NUMBER')

# All the Contract in this table are null_pdi hence creating a new column upcoming_pdi_type with null_pdi as category.
pdi_none_contracts=pdi_none_contracts.withColumn('upcoming_pdi_type',lit('null_pdi'))
pdi_none_contracts.createOrReplaceTempView('pnb')


# Joining the pdi_none_contracts with active_with_emi0_rz table.
# This is done to map PDI_not_Available and PDI_Available_but_RO contract.
active_with_emi0_rz =spark.sql('select aw0r.*, pnb.upcoming_pdi_type from aw0r left join pnb on aw0r.Contract_No=pnb.CONTRACT_NUMBER')

# Categorizing the Contracts into PDI_not_Available and PDI_Available_but_RO
active_with_emi0_rz=active_with_emi0_rz.withColumn('pdi_contracts_status',when(active_with_emi0_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

## Adding a new column run_date containing the current day timestamp
active_with_emi0_rz=active_with_emi0_rz.withColumn('run_date',lit(current_date()))
active_with_emi0_rz.createOrReplaceTempView('awe0r')


## Reading the old intermediate file 
active_with_emi0_rz_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Active0/active_zero.csv",header=True,inferSchema=True)

#Formatting the date of run_date column 
active_with_emi0_rz_old=active_with_emi0_rz_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

active_with_emi0_rz_old.createOrReplaceTempView('awe0z0')

## Appending the old intermediate file with the new intermediate file
active_with_emi0_rz_final=spark.sql('select * from awe0r union all select * from awe0z0')

active_with_emi0_rz_final=active_with_emi0_rz_final.withColumn('run_date',active_with_emi0_rz_final.run_date.cast('string'))


# output-2

## Writing the final intermediate file to S3 process-data
active_with_emi0_rz_final.repartition(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Active0",mode="append",header="true")

time.sleep(20)

## Deleting the old file present in S3
cmd2="aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/Active0/active_zero.csv"
os.system(cmd2)
time.sleep(10)

# Setting up the path to the new file 
file_path_active_zero = "s3://cmh-process-data/intermediate_files/EMI_Inflow/Active0/"

created_file_path_active_zero = fs.globStatus(Path(file_path_active_zero + "part*.csv"))[0].getPath()
time.sleep(10)

# renaming the file to its original name
fs.rename(created_file_path_active_zero,Path(file_path_active_zero + "active_zero.csv"))


# Joining the pdi_none_contracts with active_with_emi_1_5_rz table.
active_with_emi_1_5_rz =spark.sql('select a15r.*, pnb.upcoming_pdi_type from a15r left join pnb on a15r.Contract_No=pnb.CONTRACT_NUMBER')

# Categorizing the Contracts into PDI_not_Available and PDI_Available_but_RO
active_with_emi_1_5_rz=active_with_emi_1_5_rz.withColumn('pdi_contracts_status',when(active_with_emi_1_5_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))
## Adding a new column run_date containing the current day timestamp
active_with_emi_1_5_rz=active_with_emi_1_5_rz.withColumn('run_date',lit(current_date()))
active_with_emi_1_5_rz.createOrReplaceTempView('awe15r')

## Reading the old intermediate file 
active_with_emi_1_5_rz_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_1_5/active_1_5.csv",header=True,inferSchema=True)

#Formatting the date of run_date column 
active_with_emi_1_5_rz_old=active_with_emi_1_5_rz_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
active_with_emi_1_5_rz_old.createOrReplaceTempView('awe15ro')


##Appending the old intermediate file with the new intermediate file
active_with_emi_1_5_rz_final=spark.sql('select * from awe15r union all select * from awe15ro ')

active_with_emi_1_5_rz_final=active_with_emi_1_5_rz_final.withColumn('run_date', active_with_emi_1_5_rz_final.run_date.cast('string'))

## output-3
## Writing the final intermediate file to S3 process-data
active_with_emi_1_5_rz_final.repartition(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_1_5",mode="append",header="true")

time.sleep(20)
## Deleting the old file present in S3
cmd3="aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/active_1_5/active_1_5.csv"
os.system(cmd3)
time.sleep(10)

## Setting up the path to the new file 
file_path_active_1_5 = "s3://cmh-process-data/intermediate_files/EMI_Inflow/active_1_5/"
created_file_path_active_1_5 = fs.globStatus(Path(file_path_active_1_5 + "part*.csv"))[0].getPath()

time.sleep(20)
## Renaming the file to its original name
fs.rename(created_file_path_active_1_5,Path(file_path_active_1_5 + "active_1_5.csv"))

## Joining the pdi_none_contracts with active_with_morethan_5_emi_rz table.
active_with_morethan_5_emi_rz=spark.sql('select awm5r.*,pnb.upcoming_pdi_type from awm5r left join pnb on awm5r.Contract_No=pnb.CONTRACT_NUMBER')

## Categorizing the Contracts into PDI_not_Available and PDI_Available_but_RO
active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.withColumn('pdi_contracts_status',when(active_with_morethan_5_emi_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

##Adding a new column run_date containing the current day timestamp
active_with_morethan_5_emi_rz = active_with_morethan_5_emi_rz.withColumn('run_date',lit(current_date()))
active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5r1')

## Reading the old intermediate file 
active_with_morethan_5_emi_rz_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_more_5/active_more_5.csv",header=True,inferSchema=True)

## Formatting date of run_date column
active_with_morethan_5_emi_rz_old=active_with_morethan_5_emi_rz_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
active_with_morethan_5_emi_rz_old.createOrReplaceTempView('awm5ro')

## Appending the old intermediate file with the new intermediate file
active_with_morethan_5_emi_rz_final=spark.sql('select * from awm5r1 union all select * from awm5ro ')

active_with_morethan_5_emi_rz_final=active_with_morethan_5_emi_rz_final.withColumn('run_date',active_with_morethan_5_emi_rz_final.run_date.cast('string'))


## output-4
## Writing the final intermediate file to S3 process-data
active_with_morethan_5_emi_rz_final.repartition(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/active_more_5",mode="append",header="true")

time.sleep(20)
## Deleting the old file present in S3
cmd4="aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/active_more_5/active_more_5.csv"
os.system(cmd4)
time.sleep(20)


## setting up the file path for new file
file_path_active_more_5 = "s3://cmh-process-data/intermediate_files/EMI_Inflow/active_more_5/"
created_file_path_active_more_5 = fs.globStatus(Path(file_path_active_more_5 + "part*.csv"))[0].getPath()

time.sleep(20)

## Renaming the file the its original  name
fs.rename(created_file_path_active_more_5,Path(file_path_active_more_5 + "active_more_5.csv"))	

time.sleep(20)

# Taking the max dates for below tables as last_modified dates
table_1 = spark.sql('select "Inflow_table" as Table_name, max(dw_lastupdated) as last_modified, current_date() as run_date from zod_file')

table_2 = spark.sql('select "ZFI065_table" as Table_name, max(dw_lastupdated) as last_modified, current_date() as run_date from z')

table_A = table_1.unionAll(table_2)

table_3 = spark.sql('select "Cancellation_request_table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from c1')

table_B = table_A.unionAll(table_3)

table_4 = spark.sql('select "Recovery_table" as Table_name, max(rec_date) as last_modified, current_date() as run_date from rf')

table_C = table_B.unionAll(table_4)

table_5 = spark.sql('select "Cancellation_Interaction_table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from cirt')

master_table_summary = table_C.unionAll(table_5)
master_table_summary=master_table_summary.withColumn('last_modified',to_date('last_modified','yyyy-MM-dd')).withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
master_table_summary.createOrReplaceTempView('tf')

## Reading the old intermediate file
master_table_summary_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details/table_details.csv",header =True,inferSchema=True)

#master_table_summary_old=master_table_summary_old.withColumn('last_modified',to_date('last_modified','dd-MM-yyyy')).withColumn('run_date',to_date('run_date','dd-MM-yyyy'))

#master_table_summary_old=master_table_summary_old.withColumn('last_modified',master_table_summary_old.last_modified.cast('string')).withColumn('run_date',master_table_summary_old.run_date.cast('string'))

#master_table_summary_old.repartition(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details",mode="append",header="true")

## Formating the dates of last_modified and run_date column. 
master_table_summary_old=master_table_summary_old.withColumn('last_modified',to_date('last_modified','yyyy-MM-dd')).withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
master_table_summary_old.createOrReplaceTempView('nto')

## Appending the old intermediate file and new intermediate file.
master_table_summary_final= spark.sql('select * from nto union all select * from tf')

master_table_summary_final=master_table_summary_final.withColumn('last_modified',master_table_summary_final.last_modified.cast('string')).withColumn('run_date',master_table_summary_final.run_date.cast('string'))

time.sleep(20)
## Output 5
## Writing the output to S3 process-data.
master_table_summary_final.repartition(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details",mode="append",header="true")

## Deleting the old file from S3.
cmd5="sudo aws s3 rm s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details/table_details.csv"
os.system(cmd5)
time.sleep(10)

## setting up the file path for the new file generated.
file_path_Table_details = "s3://cmh-process-data/intermediate_files/EMI_Inflow/Table_details/"
created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()

time.sleep(20)
## Renaming the new file to its original name.
fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))

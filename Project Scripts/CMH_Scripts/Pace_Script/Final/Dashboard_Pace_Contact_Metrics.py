### Importing the libraries.

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

# Reading Customer Contact table from master Table in S3 process-data Bucket
customer_contact=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_customer_contact/customer_contact.csv",header =True,inferSchema=True)

## Casting the aContactID columns to integer data type.
customer_contact= customer_contact.withColumn('aContactID',customer_contact['aContactID'].cast('Integer'))

# Formatting the date in dLastUpdated column
customer_contact=customer_contact.withColumn('dLastUpdated',to_date('dLastUpdated'))

## please note createOrReplaceTempView function is used to create a temporary view/table to perform sql Queries and will be used serval times in the below code.

customer_contact.createOrReplaceTempView("cc")


# Reading Contract data table from master Table in S3 process-data bucket

contract_data=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_Contract_data/Contract_data.csv",header =True,inferSchema=True)

## Casting the aContactID and nMember columns to integer data type
contract_data = contract_data.withColumn('aContractID',contract_data['aContractID'].cast('Integer')).withColumn('nMember',contract_data['nMember'].cast('Integer'))

# Formatting the date in dLastUpdated column
contract_data=contract_data.withColumn('dtLastUpdated',to_date('dtLastUpdated'))
contract_data.createOrReplaceTempView("cd")

# Reading Allocation table from master Table in S3 process-data Bucket

allocation=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_allocation/allocation.csv",header =True,inferSchema=True)

allocation=allocation.withColumn('business_date',lit(business_day))


## Formatting the date in date column.
allocation=allocation.withColumn('Date',to_date('Date','yyyy-MM'))

# Filtering the data for the current month in Allocation table
allocation_current_month=allocation.filter('Date >= date_add(last_day( business_date - interval 01 month),1) and Date<= last_day(business_date)')

allocation_current_month=allocation_current_month.withColumn('Allocation',lower(allocation_current_month.Allocation))
allocation_current_month.createOrReplaceTempView("al")

allocation_current_month=spark.sql('select *, case when Allocation like "pace%" then "pace" when Allocation like "nhm%" then "nhm" when Allocation like "asf%" then "asf" when Allocation like "branch%" then "branch" else Allocation end as Allocation_New from al')

allocation_current_month = allocation_current_month.drop('Allocation')

allocation_current_month=allocation_current_month.withColumnRenamed('Allocation_New','Allocation')



allocation_current_month.createOrReplaceTempView("al")


## Preparing the member data considering the allocation Table as the base table and left joining other tables with it.

Member_data=spark.sql("select distinct al.Contract_ID, al.Control_Location as ControlLocation, cd.dtSalesPosted, right(cc.tMobile, 10) as Mobile_No,right(cc.tResPhone1,10) as tResPhone1, right(cc.tResPhone2,10) as tResPhone2, al.category, month(cd.dtSalesPosted) as MonthofCharge,al.Allocation as Team_Allocation, al.HWC_bucket as Rating,al.Due as Due from al left join cd on al.Contract_ID = cd.aContractID left join cc on cd.nMember = cc.aContactID where al.Allocation = 'pace'")
Member_data.createOrReplaceTempView("md")


# Reading ZFI PDI table from master Table in S3 process-data Bucket.
zfi_pdi=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_PDI/ZFI_PDI.csv",header =True,inferSchema=True)

zfi_pdi=zfi_pdi.withColumn('business_date',lit(business_day))


## Formatting the date in PDI_REALIZATION_DATE and dw_last_modified_date column.
## And Casting the PDI_STATUS column to integer.
zfi_pdi=zfi_pdi.withColumn('PDI_REALIZATION_DATE',to_date('PDI_REALIZATION_DATE','yyyy-MM-dd')).withColumn('dw_last_modified_date',to_date('dw_last_modified_date')).withColumn('PDI_STATUS',zfi_pdi.PDI_STATUS.cast('Integer'))
zfi_pdi.createOrReplaceTempView("cpd")

## Selecting the required columns from zfi_pdi Table
zfi_pdi_select_columns=spark.sql("select reference_document,PDI_DATE,PDI_TYPE,PDI_STATUS,PDI_AMOUNT,PDI_REALIZATION_DATE, Contract_Flag,business_date from cpd")

## Renaming the columns
zfi_pdi_select_columns=zfi_pdi_select_columns.withColumnRenamed('reference_document','Contract_ID')
zfi_pdi_select_columns.createOrReplaceTempView("filter")

# summarizing the zfi_pdi_select_columns by taking taking sum of PDI_AMOUNT and grouping it on Contract_ID level.
zfi_pdi_summarize=spark.sql("select distinct Contract_ID, sum(PDI_AMOUNT) as PDI_Amount from filter where (PDI_REALIZATION_DATE>=date_add(last_day(business_date - interval 01 month),1) and PDI_REALIZATION_DATE<= last_day(business_date)) and PDI_STATUS = 9 and Contract_Flag ='A'and PDI_TYPE !='ADJ' group by Contract_ID")
zfi_pdi_summarize.createOrReplaceTempView("fl")

## Preparing the Member_data_2 by taking the summarized columns from zfi_pdi_summarize table and left joining it with Member_data table.
Member_data_2=spark.sql("select md.Contract_ID,md.ControlLocation,md.dtSalesPosted,md.Mobile_No,md.tResPhone1,md.tResPhone2,md.category,md.MonthofCharge,md.Team_Allocation,md.Rating,md.Due,fl.PDI_Amount from md left join fl on md.Contract_ID=fl.Contract_ID")

Member_data_2.createOrReplaceTempView('fm')


# Reading Pace Dialler Data table from Master table in S3 process-data bucket

pace_dialler=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_pace_dialler/pace_dialler.csv",header =True,inferSchema=True)

# Formatting the date in date StartTime column
pace_dialler=pace_dialler.withColumn('StartTime',to_date('StartTime',"yyyy-MM-dd").cast('date'))

pace_dialler=pace_dialler.withColumn('business_date',lit(business_day))

## Applying filter for current month 
pace_dialler_current_month=pace_dialler.filter('StartTime >= date_add(last_day(business_date - interval 01 months),1) and StartTime<=(last_day(business_date))')
pace_dialler_current_month.createOrReplaceTempView('new')

# Selecting the 10 digit from rigt from CLi column
pace_dailler_select_columns=spark.sql("Select right(CLI,10) as Cli,Contactedstatus from new")

## Applying Crosstab on Cli column and Contactedstatus column to get the count to contacted and Not_Contacted contacts 
pace_dailler_mobile_data=pace_dailler_select_columns.crosstab('Cli','Contactedstatus')
## Renaming the Columns
pace_dailler_mobile_data=pace_dailler_mobile_data.withColumnRenamed('Contacted','Contacted_m').withColumnRenamed('Not Contacted','Not_Contacted_m')
pace_dailler_mobile_data.createOrReplaceTempView('ps')

## Applying Crosstab on Cli column and Contactedstatus column to get the count to contacted and Not_Contacted contacts 
pace_dailler_tres1_data=pace_dailler_select_columns.crosstab('Cli','Contactedstatus')
pace_dailler_tres1_data=pace_dailler_tres1_data.withColumnRenamed('Contacted','Contacted_t1').withColumnRenamed('Not Contacted','Not_Contacted_t1')
pace_dailler_tres1_data.createOrReplaceTempView('ps1')

## Applying Crosstab on Cli column and Contactedstatus column to get the count to contacted and Not_Contacted contacts 
pace_dailler_tres2_data=pace_dailler_select_columns.crosstab('Cli','Contactedstatus')
pace_dailler_tres2_data=pace_dailler_tres2_data.withColumnRenamed('Contacted','Contacted_t2').withColumnRenamed('Not Contacted','Not_Contacted_t2')
pace_dailler_tres2_data.createOrReplaceTempView('ps2')


# Mapping Mobile_No column from Member_data_2 with Contacted_m and Not_Contacted_m from pace_dailler_mobile_data.

mobile_no_mapped=spark.sql("select fm.*, ps.Contacted_m, ps.Not_Contacted_m from fm left join ps on fm.Mobile_No=ps.Cli_Contactedstatus")
mobile_no_mapped.createOrReplaceTempView('temp')

# Mapping tres1 column from Member_data_2 with Contacted_t1 and Not_Contacted_t1 from pace_dailler_tres1_data.

tres1_no_mapped=spark.sql("select temp.*,ps1.Contacted_t1,ps1.Not_Contacted_t1 from temp left join ps1 on temp.tResPhone1=ps1.Cli_Contactedstatus")
tres1_no_mapped.createOrReplaceTempView('temp2')

# Mapping tres2 column from Member_data_2 with Contacted_t1 and Not_Contacted_t1 from pace_dailler_tres2_data.

tres2_no_mapped=spark.sql("select temp2.*,ps2.Contacted_t2,ps2.Not_Contacted_t2 from temp2 left join ps2 on temp2.tResPhone2=ps2.Cli_Contactedstatus")

# Filling Null Values with 0.
Member_data_3=tres2_no_mapped.na.fill(0)

# Taking the Greatest value column from  Contacted_m,Contacted_t1,Contacted_t2 as Contacted and Greatest value column from Not_Contacted_m, Not_Contacted_t1,Not_Contacted_t2 as Not_Contacted column

Member_data_3=Member_data_3.withColumn('Contacted',greatest(Member_data_3.Contacted_m , Member_data_3.Contacted_t1, Member_data_3.Contacted_t2)).withColumn('Not_Contacted',greatest(Member_data_3.Not_Contacted_m,Member_data_3.Not_Contacted_t1,Member_data_3.Not_Contacted_t2))

## Adding a run_date column with current date timestamp
Final_Member_Data=Member_data_3.withColumn('run_date',lit(current_date()))

## Adding a new column total_attempted as the sum of Contacted and Not_Contacted columns.
Final_Member_Data=Final_Member_Data.withColumn('total_attempted',(Final_Member_Data.Contacted+Final_Member_Data.Not_Contacted))

Final_Member_Data.createOrReplaceTempView("ttr")

## Creating new column total_attempted with two categories as yes - when total_attempted is more than zero and No - when total_attempted is <=0.

## Creating new column contacted_dialer with two categories as yes - when contacted >0 and No - when contacted is <=0.

## Creating new column realized with two categories as yes - when PDI_AMOUNT >0 and No - when PDI_AMOUNT is <=0.
 
Final_Member_Data=spark.sql("select t.*,case when total_attempted >0 then 'yes' else 'no' end as attempted_dialer,case when contacted>0 then 'yes' else 'no' end as contacted_dialer,case when PDI_Amount>0 then 1 else 0 end as realized from ttr t")

## Lower casing the Rating columns
Final_Member_Data=Final_Member_Data.withColumn('Rating',lower(Final_Member_Data.Rating))

Final_Member_Data.createOrReplaceTempView("mem")


# Applying the Business Logic on the Final_Member_Data to generate the pace_contact_metrics_dashboard metrics.

pace_contact_metrics_dashboard=spark.sql("select m.Category,sum(case when m.Rating='hot' then 1 else 0 end) as base_hot,sum(case when m.Rating='warm' then 1 else 0 end) as base_warm,sum(case when m.Rating='cold' then 1 else 0 end) as base_cold,sum(case when m.Rating is null then 1 else 0 end) as base_unknown,count(*) as base_total,sum(case when m.Rating='hot' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_hot,sum(case when m.Rating='warm' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_warm,sum(case when m.Rating='cold' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_cold,sum(case when m.Rating is null and m.attempted_dialer='yes' then 1 else 0 end) as attempted_unknown,sum(case when m.attempted_dialer='yes' then 1 else 0 end) as attempted_total,sum(case when m.Rating='hot' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_hot,sum(case when m.Rating='warm' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_warm,sum(case when m.Rating='cold' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_cold,sum(case when m.Rating is null  and m.contacted_dialer='yes' then 1 else 0 end) as contacted_unknown,sum(case when m.contacted_dialer='yes' then 1 else 0 end) as contacted_total,sum(case when m.Rating='hot' and m.realized=1 then 1 else 0 end) as realized_hot,sum(case when m.Rating='warm' and m.realized=1 then 1 else 0 end) as realized_warm,sum(case when m.Rating='cold' and m.realized=1 then 1 else 0 end) as realized_cold,sum(case when m.Rating is null and m.realized=1 then 1 else 0 end) as realized_unknown,sum(case when m.realized=1 then 1 else 0 end) as realized_total,sum(case when m.Rating='hot' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_hot,sum(case when m.Rating='warm' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_warm,sum(case when m.Rating='cold' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_cold,sum(case when m.Rating is null and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_unknown,sum(case when m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_total,sum(case when m.Rating='hot' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_hot,sum(case when m.Rating='warm' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_warm,sum(case when m.Rating='cold' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_cold,sum(case when m.Rating is null and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_unknown,sum(case when m.realized=1 then PDI_Amount else 0 end) as Amt_realized_total from mem m where m.Category is not null group by m.Category")

pace_contact_metrics_dashboard.createOrReplaceTempView("tests")

# Applying the Business Logic on the Final_Member_Data to generate the pace_contact_metrics_dashboard metrics.

pace_contact_metrics_dashboard_1=spark.sql("select t.*,((attempted_hot/base_hot)*100) as perc_attempted_hot,((attempted_warm/base_warm)*100) as perc_attempted_warm,((attempted_cold/base_cold)*100) as perc_attempted_cold,((attempted_total/base_total)*100) as perc_total,((contacted_hot/base_hot)*100) as perc_con_allocated_hot,((contacted_warm/base_warm)*100) as perc_con_allocated_warm,((contacted_cold/base_cold)*100) as perc_con_allocated_cold,((contacted_total/base_total)*100) as perc_con_allocated_total,((contacted_hot/attempted_hot)*100) as perc_con_outofattm_hot,((contacted_warm/attempted_warm)*100) as perc_con_outofattm_warm,((contacted_cold/attempted_cold)*100) as perc_con_outofattm_cold,((contacted_total/attempted_total)*100) as perc_con_outofattm_total,((no_of_calls_hot/attempted_hot)) as churn_hot,((no_of_calls_warm/attempted_warm)) as churn_warm,((no_of_calls_cold/attempted_cold)) as churn_cold,((no_of_calls_unknown/attempted_total)) as churn_unknown,((no_of_calls_total/attempted_total)) as churn_total,no_of_calls_hot/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_hot,no_of_calls_warm/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_warm,no_of_calls_cold/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_cold,no_of_calls_total/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_total,((realized_hot/base_hot)*100) as perc_conversion_allo_hot,((realized_warm/base_warm)*100) as perc_conversion_allo_warm,((realized_cold/base_cold)*100) as perc_conversion_allo_cold,((realized_total/base_total)*100) as perc_conversion_allo_total,((realized_hot/attempted_hot)*100) as perc_conversion_attem_hot,((realized_warm/attempted_warm)*100) as perc_conversion_attem_warm,((realized_cold/attempted_cold)*100) as perc_conversion_attem_cold,((realized_total/attempted_total)*100) as perc_conversion_attem_total from tests t")

## Adding run_date as current date timestamp
pace_contact_metrics_dashboard_1=pace_contact_metrics_dashboard_1.withColumn('run_date',lit(current_date()))

pace_contact_metrics_dashboard_1 = pace_contact_metrics_dashboard_1.withColumn('run_date',pace_contact_metrics_dashboard_1.run_date.cast('string'))
pace_contact_metrics_dashboard_1.createOrReplaceTempView('t7')

# Reading the old file present in intermediate folder from processed data in S3.
pace_contact_metrics_dashboard_old_file= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/Pace_Contact_metrics/pace_contact_metrics/Contact_Metrics.csv",header =True,inferSchema=True)

## Formatting the date in run_date date column
pace_contact_metrics_dashboard_old_file=pace_contact_metrics_dashboard_old_file.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

pace_contact_metrics_dashboard_old_file=pace_contact_metrics_dashboard_old_file.withColumn('run_date',pace_contact_metrics_dashboard_old_file.run_date.cast('string'))

pace_contact_metrics_dashboard_old_file.createOrReplaceTempView('to')

# Appending the Old intermediate table and present intermediate table
pace_contact_metrics_dashboard_final= spark.sql('select * from to union all select * from t7')

# Output 1
# Writing the file in S3 process-data Bucket.
pace_contact_metrics_dashboard_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Pace_Contact_metrics/pace_contact_metrics",mode="append",header="true")

## Taking the max dates for below tables as last_modified dates
table_1 = spark.sql('select "customer_contact_table" as Table_name, max(dLastUpdated) as last_modified, current_date() as run_date from cc')

table_2 = spark.sql('select "contract_table" as Table_name, max(dtLastUpdated) as last_modified, current_date() as run_date from cd')

table_A = table_1.unionAll(table_2)

table_3 = spark.sql('select "Allocation_table" as Table_name, max(date) as last_modified, current_date() as run_date from al')

table_B = table_A.unionAll(table_3)

table_4 = spark.sql('select "ZFI_PDI_table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from cpd')

table_C = table_B.unionAll(table_4)

table_5 = spark.sql('select "pace_dialler_table" as Table_name, max(StartTime) as last_modified, current_date() as run_date from new')

master_table_summary = table_C.unionAll(table_5)

master_table_summary.createOrReplaceTempView('tf')

master_table_summary=master_table_summary.withColumn('last_modified',master_table_summary.last_modified.cast('string')).withColumn('run_date',master_table_summary.run_date.cast('string'))

## Reading the Old Master Table Summary file
master_table_summary_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/Pace_Contact_metrics/Table_details/table_details.csv",header =True,inferSchema=True)

master_table_summary_old=master_table_summary_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd')).withColumn('last_modified',to_date('last_modified','yyyy-MM-dd'))

master_table_summary_old=master_table_summary_old.withColumn('last_modified',master_table_summary_old.last_modified.cast('string')).withColumn('run_date',master_table_summary_old.run_date.cast('string'))

master_table_summary_old.createOrReplaceTempView('nto')

## Appending the old intermediate file and new intermediate file.
master_table_summary_final= spark.sql('select * from nto union all select * from tf')

## Output 2
## Writing the output to S3 process-data.
master_table_summary_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/Pace_Contact_metrics/Table_details",mode="append",header="true")

time.sleep(30)

## Importing the System libraries
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)

# Deleting the previously present intermediate file
cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/Pace_Contact_metrics/pace_contact_metrics/Contact_Metrics.csv"
os.system(cmd1)
time.sleep(10)

## Deleting the previosuly present table Detials file
cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/Pace_Contact_metrics/Table_details/table_details.csv"
os.system(cmd2)
time.sleep(10)

# Setting up S3 process-data Bucket path
fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())

## setting up the file path for the new file generated.
file_path_pace_contact = "s3://cmh-process-data/intermediate_files/Pace_Contact_metrics/pace_contact_metrics/"
time.sleep(10)

## setting up the file path for the new file generated.
file_path_Table_details = "s3://cmh-process-data/intermediate_files/Pace_Contact_metrics/Table_details/"
time.sleep(10)


##setting up the file path for the new file generated.
created_file_path_pace_contact = fs.globStatus(Path(file_path_pace_contact + "part*.csv"))[0].getPath()
time.sleep(10)

## setting up the file path for the new file generated.
created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)

## Renaming the new file to its original name.
fs.rename(created_file_path_pace_contact,Path(file_path_pace_contact + "Contact_Metrics.csv"))
time.sleep(10)
## Renaming the new file to its original name.
fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))
































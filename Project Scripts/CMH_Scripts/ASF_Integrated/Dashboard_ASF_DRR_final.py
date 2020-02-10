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


## For Manual Run
#business_day = Business_day_calculator(today_date=datetime.datetime.strptime("2020-02-01","%Y-%m-%d"))


## Reading allocation S3 file.
allocation=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_allocation/allocation.csv",header =True,inferSchema=True)

allocation=allocation.withColumn('business_date',lit(business_day))

allocation=allocation.withColumn('Date',to_date('Date','yyyy-MM')).withColumn('Contract_ID',allocation.Contract_ID.cast('Integer'))


allocation_filter=allocation.filter('Date >= date_add(last_day(business_date - interval 01 month),1) and Date<= last_day(business_date)')
allocation_filter=allocation_filter.withColumn('Control_Location',regexp_replace('Control_Location',' - ','-'))

allocation_filter=allocation_filter.withColumn('Allocation',lower(allocation_filter.Allocation))
allocation_filter.createOrReplaceTempView("al")


Member_data = spark.sql('select distinct Contract_ID, Allocation as Team_Allocation, category, Control_Location as ControlLocation , Due, HWC_bucket as rating from al')
Member_data.createOrReplaceTempView("md")



## Reading ZFI_PDI File
zfi=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_PDI/ZFI_PDI.csv",header =True,inferSchema=True)

zfi=zfi.withColumn('business_date',lit(business_day))


zfi=zfi.withColumn('PDI_REALIZATION_DATE',to_date('PDI_REALIZATION_DATE','yyyy-MM-dd')).withColumn('dw_last_modified_date',to_date('dw_last_modified_date','yyyy-MM-dd')).withColumn('PDI_STATUS',zfi.PDI_STATUS.cast('Integer'))
zfi.createOrReplaceTempView("cpd")

filter_df=spark.sql("select reference_document,PDI_DATE,PDI_TYPE,PDI_STATUS,PDI_AMOUNT,PDI_REALIZATION_DATE,Contract_Flag,business_date from cpd")

filter_df=filter_df.withColumnRenamed('reference_document','Contract_ID')
filter_df=filter_df.withColumn('month_day',dayofmonth('PDI_REALIZATION_DATE'))
filter_df.createOrReplaceTempView("filter")

## Applying Filter as per BRD
filter_df_2=spark.sql("select distinct Contract_ID, sum(PDI_AMOUNT) as PDI_Amount, month_day from filter where (PDI_REALIZATION_DATE>=date_add(last_day(business_date - interval 01 month),1) and PDI_REALIZATION_DATE<= last_day(business_date)) and PDI_STATUS= 9 and Contract_Flag ='A'and PDI_TYPE !='ADJ' group by Contract_ID,month_day")
filter_df_2.createOrReplaceTempView("fl")


Final_Member_data=spark.sql("select md.Contract_ID,md.ControlLocation,md.category,md.Team_Allocation,md.Due,md.rating,fl.PDI_Amount,fl.month_day from md left join fl on md.Contract_ID=fl.Contract_ID")

Final_Member_data.createOrReplaceTempView('fm1')


Final_Member_data=spark.sql('select *, case when Team_Allocation is null then "unknown_allocation" else Team_Allocation end as Team_Allocation_1,case when category is null then "unknown_category" else category end as category_1 from fm1')

Final_Member_data=Final_Member_data.drop('Team_Allocation','category')

Final_Member_data=Final_Member_data.withColumnRenamed('Team_Allocation_1','Team_Allocation').withColumnRenamed('category_1','category')

Final_Member_data=Final_Member_data.withColumn('Team_Allocation',lower(Final_Member_data.Team_Allocation)).withColumn('ControlLocation',lower(Final_Member_data.ControlLocation))

Final_Member_data=spark.sql('select *, case when Team_Allocation like "pace%" then "pace" when Team_Allocation like "nhm%" then "nhm" when Team_Allocation like "asf%" then "asf" when Team_Allocation like "branch%" then "branch" else Team_Allocation end as Team_Allocation_2 from fm1')


Final_Member_data=Final_Member_data.drop('Team_Allocation')

Final_Member_data=Final_Member_data.withColumnRenamed('Team_Allocation_2','Team_Allocation')


Final_Member_data.createOrReplaceTempView("fm")


# DRR -Channel Wise - channel Sheet

drr_channel = spark.sql('select Team_Allocation,month_day,sum(PDI_Amount) as pdi_amount from fm group by Team_Allocation, month_day')
drr_channel=drr_channel.withColumnRenamed('Team_Allocation_1','Team_Allocation')
drr_channel=drr_channel.withColumn('run_date',lit(current_date()))
drr_channel=drr_channel.na.fill(0)
drr_channel.createOrReplaceTempView('dt')

## Reading Old File
drr_channel_old = spark.read.option("header", "true").option("delimiter", ",").option("inferSchema","true").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_channelwise/DRR_channelwise.csv")

drr_channel_old=drr_channel_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

drr_channel_old.createOrReplaceTempView('dto')

drr_channel_final = spark.sql('select * from dto union all select * from dt')

drr_channel_final=drr_channel_final.withColumn('run_date',drr_channel_final.run_date.cast('string'))

drr_channel_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_channelwise",mode="append",header="true")

time.sleep(10)


# DRR -Channel Wise -Control location Sheet


Final_Member_data_Branch = Final_Member_data.filter('Team_Allocation == "branch"')

Final_Member_data_Branch.createOrReplaceTempView('fmb')

drr_controllocation = spark.sql('select ControlLocation,month_day,sum(PDI_Amount) as pdi_amount from fmb group by ControlLocation, month_day')

drr_controllocation=drr_controllocation.withColumn('run_date',lit(current_date()))
drr_controllocation=drr_controllocation.na.fill(0)
drr_controllocation.createOrReplaceTempView('dc')

#drr_controllocation.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_channelwise_locations",mode="append",header="true")

drr_controllocation_old = spark.read.option("header", "true").option("delimiter", ",").option("inferSchema","true").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_channelwise_locations/DRR_channelwise_locations.csv")

drr_controllocation_old=drr_controllocation_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

drr_controllocation_old.createOrReplaceTempView('dco')


drr_controllocation_final = spark.sql('select * from dco union all select * from dc')

drr_controllocation_final=drr_controllocation_final.withColumn('run_date',drr_controllocation_final.run_date.cast('string'))

drr_controllocation_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_channelwise_locations",mode="append",header="true")


# DRR categorywise


drr_categorywise = spark.sql('select category_1,month_day,count(case when pdi_amount>0 then Contract_ID end) as count,sum(PDI_Amount) as pdi_amount from fm group by category_1, month_day')

drr_categorywise=drr_categorywise.withColumnRenamed('category_1','category')

drr_categorywise=drr_categorywise.withColumn('run_date',lit(current_date()))
drr_categorywise=drr_categorywise.na.fill(0)

#drr_categorywise.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_categorywise",mode="append",header="true")

drr_categorywise.createOrReplaceTempView('dcw')

drr_categorywise_old = spark.read.option("header", "true").option("delimiter", ",").option("inferSchema","true").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_categorywise/DRR_categorywise.csv")

drr_categorywise_old=drr_categorywise_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

drr_categorywise_old.createOrReplaceTempView('dcwo')

drr_categorywise_final = spark.sql('select * from dcwo union all select * from dcw')

drr_categorywise_final=drr_categorywise_final.withColumn('run_date',drr_categorywise_final.run_date.cast('string'))

drr_categorywise_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_categorywise",mode="append",header="true")



table_1 = spark.sql('select "Allocation_table" as Table_name, max(date) as last_modified, current_date() as run_date from al')

table_2 = spark.sql('select "ZFI_PDI_table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from cpd')

table_A = table_1.unionAll(table_2)

table_A.createOrReplaceTempView('tf')

#table_A.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Table_details",mode="append",header="true")

asf_integrated_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Table_details/table_details.csv",header =True,inferSchema=True)

asf_integrated_old=asf_integrated_old.withColumn('last_modified',to_date('last_modified','yyyy-MM-dd')).withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

asf_integrated_old.createOrReplaceTempView('nto')

asf_integrated_final= spark.sql('select * from nto union all select * from tf')

asf_integrated_final=asf_integrated_final.withColumn('last_modified',asf_integrated_final.last_modified.cast('string')).withColumn('run_date',asf_integrated_final.run_date.cast('string'))

asf_integrated_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Table_details",mode="append",header="true")


time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)


cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_channelwise/DRR_channelwise.csv"
os.system(cmd1)
time.sleep(10)
cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_channelwise_locations/DRR_channelwise_locations.csv"
os.system(cmd2)
time.sleep(10)
cmd3="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_categorywise/DRR_categorywise.csv"
os.system(cmd3)
time.sleep(10)
cmd4="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/Table_details/table_details.csv"
os.system(cmd4)
time.sleep(10)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())

file_path_Drr_channel = "s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_channelwise/"
time.sleep(10)

file_path_Drr_channel_location = "s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_channelwise_locations/"
time.sleep(10)

file_path_Drr_categorywise = "s3://cmh-process-data/intermediate_files/ASF_Integrated/DRR_categorywise/"
time.sleep(10)

file_path_Table_details = "s3://cmh-process-data/intermediate_files/ASF_Integrated/Table_details/"
time.sleep(10)

created_file_path_Drr_channel = fs.globStatus(Path(file_path_Drr_channel + "part*.csv"))[0].getPath()
time.sleep(10)

created_file_path_Drr_channel_location = fs.globStatus(Path(file_path_Drr_channel_location + "part*.csv"))[0].getPath()
time.sleep(10)

created_file_path_Drr_categorywise = fs.globStatus(Path(file_path_Drr_categorywise + "part*.csv"))[0].getPath()
time.sleep(10)

created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)

fs.rename(created_file_path_Drr_channel,Path(file_path_Drr_channel + "DRR_channelwise.csv"))
time.sleep(10)

fs.rename(created_file_path_Drr_channel_location,Path(file_path_Drr_channel_location + "DRR_channelwise_locations.csv"))
time.sleep(10)

fs.rename(created_file_path_Drr_categorywise,Path(file_path_Drr_categorywise + "DRR_categorywise.csv"))
time.sleep(10)

fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))
time.sleep(10)







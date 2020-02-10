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

## Read the Allocation file from S3.

allocation=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_allocation/allocation.csv",header =True,inferSchema=True)

allocation=allocation.withColumn('business_date',lit(business_day))


allocation=allocation.withColumn('date',to_date('date','yyyy-MM')).withColumn('Contract_ID',allocation.Contract_ID.cast('Integer'))

# Applyinh Filter for Current Month
allocation_filter=allocation.filter('date >= date_add(last_day(business_date - interval 01 month),1) and date<= last_day(business_date)')
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

Final_Member_data.createOrReplaceTempView("fm")


# Reading ASF Targets File from S3 master tables.

asf_targets=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_asf_targets/target_sheet_asf.csv",header =True,inferSchema=True)

asf_targets=asf_targets.withColumn('business_date',lit(business_day))

asf_targets=asf_targets.withColumnRenamed('0-30 days','x_0_30_days')

for col in asf_targets.columns:
	asf_targets=asf_targets.withColumnRenamed(col,col.lower())
	
	
asf_targets = asf_targets.withColumn("x_0_30_days",regexp_replace(asf_targets.x_0_30_days,",","")).withColumn("current_yr",regexp_replace(asf_targets.current_yr,",","")).withColumn("current_1yr",regexp_replace(asf_targets.current_1yr,",","")).withColumn("current_2yr",regexp_replace(asf_targets.current_2yr,",","")).withColumn("current_3yr",regexp_replace(asf_targets.current_3yr,",","")).withColumn("current_4yr",regexp_replace(asf_targets.current_4yr,",","")).withColumn("total_target",regexp_replace(asf_targets.total_target,",",""))


asf_targets = asf_targets.withColumn("x_0_30_days",asf_targets.x_0_30_days.cast('Integer')).withColumn("current_yr",asf_targets.current_yr.cast('Integer')).withColumn("current_1yr",asf_targets.current_1yr.cast('Integer')).withColumn("current_2yr",asf_targets.current_2yr.cast('Integer')).withColumn("current_3yr",asf_targets.current_3yr.cast('Integer')).withColumn("current_4yr",asf_targets.current_4yr.cast('Integer')).withColumn("total_target",asf_targets.total_target.cast('Integer'))

asf_targets=asf_targets.withColumn('location',lower(asf_targets.location)).withColumn('target_month',to_date('target_month','yyyy-MM'))

asf_targets=asf_targets.withColumn('location',when(asf_targets.location=="chennai asf team","asf").when(asf_targets.location.like("branch%"),"branch").otherwise(asf_targets.location))
asf_targets=asf_targets.withColumn('location',regexp_replace('location',' - ','-'))

asf_targets=asf_targets.withColumnRenamed('x_0_30_days','0-30 days')

asf_targets_filter_current_month= asf_targets.filter('target_month >= date_add(last_day(business_date - interval 01 month),1) and target_month <= last_day(business_date)')

asf_targets_filter_current_month=asf_targets_filter_current_month.filter("location in ('asf','pace','nhm','branch')")
asf_targets_filter_current_month.createOrReplaceTempView('atf')


asf_targets_file= asf_targets.drop('business_date')



## Output -1 Writing 
asf_targets_file.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/ASF_targets",mode="append",header="true")




# Realisation and Targets of ASF

asf_realisation_summ= spark.sql('select Team_Allocation,category,sum(pdi_amount) as realisation_amount from fm group by Team_Allocation,category')

asf_realisation_summ.createOrReplaceTempView('ars')

asf_real_target = spark.sql('select ars.Team_Allocation,ars.category,ars.realisation_amount/10000000 as realisation_amount,atf.total_target/10000000 as total_target from ars left join atf on ars.Team_Allocation = atf.location')

asf_real_target=asf_real_target.na.fill(0)

asf_real_target=asf_real_target.withColumn('run_date',lit(current_date()))
#asf_real_target.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/asf_realisation_target",mode="append",header="true")

asf_real_target.createOrReplaceTempView('art')

asf_real_target_old = spark.read.option("header", "true").option("delimiter", ",").option("inferSchema","true").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/asf_realisation_target/asf_realisation_target.csv")

asf_real_target_old=asf_real_target_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

asf_real_target_old.createOrReplaceTempView('arto')

asf_real_target_final = spark.sql('select * from arto union all select * from art')

asf_real_target_final=asf_real_target_final.withColumn('run_date',asf_real_target_final.run_date.cast('string'))

## Output-2 Realisation Vs Target sheet

asf_real_target_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/asf_realisation_target",mode="append",header="true")



# Branch Targets

branch_target_filter = asf_targets_filter_1.filter("location not in ('chennai_team','pace','nhm','branches','overall target')")

branch_target_filter.createOrReplaceTempView('btf')

branch_realisation_filter = Final_Member_data.filter('Team_Allocation == "branch"')
branch_realisation_filter.createOrReplaceTempView('brf')

branch_realisation_summ= spark.sql('select ControlLocation,category,sum(pdi_amount) as realisation_amount from brf group by ControlLocation,category')

branch_realisation_summ.createOrReplaceTempView('brs')

branch_real_target = spark.sql('select brs.ControlLocation,brs.realisation_amount/10000000 as realisation_amount,btf.total_target/10000000 as total_target from brs left join btf on brs.ControlLocation = btf.location')

branch_real_target=branch_real_target.na.fill(0)

branch_real_target=branch_real_target.withColumn('run_date',lit(current_date()))

#branch_real_target.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/branch_realisation_target",mode="append",header="true")

branch_real_target.createOrReplaceTempView('brt')

branch_real_target_old = spark.read.option("header", "true").option("delimiter", ",").option("inferSchema","true").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/branch_realisation_target/branch_realisation_target.csv")

branch_real_target_old=branch_real_target_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

branch_real_target_old.createOrReplaceTempView('brto')

branch_real_target_final = spark.sql('select * from brto union all select * from brt')

branch_real_target_final=branch_real_target_final.withColumn('run_date',branch_real_target_final.run_date.cast('string')))


## Output 3 ControlLocation wise Realsaiton and Target sheet
branch_real_target_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/branch_realisation_target",mode="append",header="true")


##


table_1 = spark.sql('select "Allocation_table" as Table_name, max(date) as last_modified, current_date() as run_date from al')

table_2 = spark.sql('select "ZFI_PDI_table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from cpd')

table_A = table_1.unionAll(table_2)

table_3 = spark.sql('select "ASF_Targets_Table" as Table_name, max(target_month) as last_modified, current_date() as run_date from atf')
#table_3 = spark.sql('select "ASF_Targets_Table" as Table_name, "2019-10-11" as last_modified, current_date() as run_date')

table_B = table_A.unionAll(table_3)

table_B.createOrReplaceTempView('tf')

#table_B.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details_2",mode="append",header="true")

Table_details_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details_2/table_details.csv",header =True,inferSchema=True)

Table_details_old=Table_details_old.withColumn('last_modified',to_date('last_modified','yyyy-MM-dd')).withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

Table_details_old.createOrReplaceTempView('nto')

Table_details_final= spark.sql('select * from nto union all select * from tf')

Table_details_final=Table_details_final.withColumn('last_modified',Table_details_final.last_modified.cast('string')).withColumn('run_date',Table_details_final.run_date.cast('string'))

## Output-4 Master tables

Table_details_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details_2",mode="append",header="true")

time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)


cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/asf_realisation_target/asf_realisation_target.csv"
os.system(cmd1)
time.sleep(10)
cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/branch_realisation_target/branch_realisation_target.csv"
os.system(cmd2)
time.sleep(10)

cmd3="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details_2/table_details.csv"
os.system(cmd3)
time.sleep(10)

cmd4="sudo aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/ASF_targets/asf_targets.csv"
os.system(cmd4)
time.sleep(10)






fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())

file_path_asf_real = "s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/asf_realisation_target/"
time.sleep(10)

file_path_branch_real = "s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/branch_realisation_target/"
time.sleep(10)

file_path_Table_details = "s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/Table_details_2/"
time.sleep(10)

file_path_asf_targets = "s3://cmh-process-data/intermediate_files/ASF_Integrated/ASF_Integrated_daily/ASF_targets/"
time.sleep(10)


created_file_path_asf_real = fs.globStatus(Path(file_path_asf_real + "part*.csv"))[0].getPath()
time.sleep(10)

created_file_path_branch_real = fs.globStatus(Path(file_path_branch_real + "part*.csv"))[0].getPath()
time.sleep(10)

created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)

created_file_path_asf_targets = fs.globStatus(Path(file_path_asf_targets + "part*.csv"))[0].getPath()
time.sleep(10)



fs.rename(created_file_path_asf_real,Path(file_path_asf_real + "asf_realisation_target.csv"))
time.sleep(10)

fs.rename(created_file_path_branch_real,Path(file_path_branch_real + "branch_realisation_target.csv"))
time.sleep(10)

fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))
time.sleep(10)

fs.rename(created_file_path_asf_targets,Path(file_path_asf_targets + "asf_targets.csv"))
time.sleep(10)





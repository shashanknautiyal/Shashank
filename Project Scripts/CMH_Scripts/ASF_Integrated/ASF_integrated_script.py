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

allocation=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_allocation/allocation.csv",header =True,inferSchema=True)

for col in allocation.columns:
	allocation=allocation.withColumnRenamed(col,col.replace(" ", "_"))

allocation=allocation.withColumn('date',to_date('date','yyyy-MM')).withColumn('Contract_ID',allocation.Contract_ID.cast('Integer'))

allocation_filter=allocation.filter('date >= date_add(last_day(now() - interval 01 month),1) and date<= last_day(now())')

allocation_filter.createOrReplaceTempView("al")

Member_data = spark.sql('select distinct al.Contract_ID, al.Oct_Allocation as Team_Allocation , al.category , al.Control_Location , al.Due as Due from al')

Member_data.createOrReplaceTempView("md")

zfi=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_PDI/ZFI_PDI.csv",header =True,inferSchema=True)

zfi=zfi.withColumn('PDI_REALIZATION_DATE',to_date('PDI_REALIZATION_DATE','yyyy-MM-dd')).withColumn('dw_last_modified_date',to_date('dw_last_modified_date','yyyy-MM-dd')).withColumn('PDI_STATUS',zfi.PDI_STATUS.cast('Integer'))
zfi.createOrReplaceTempView("cpd")

filter_df=spark.sql("select reference_document,PDI_DATE,PDI_TYPE,PDI_STATUS,PDI_AMOUNT,PDI_REALIZATION_DATE, Contract_Flag from cpd")

filter_df=filter_df.withColumnRenamed('reference_document','Contract_ID')
filter_df.createOrReplaceTempView("filter")

filter_df_2=spark.sql("select distinct Contract_ID, sum(PDI_AMOUNT) as PDI_Amount, dayofmonth(PDI_REALIZATION_DATE) as month_day from filter where (PDI_REALIZATION_DATE >= date_add(last_day(now() - interval 01 month),1) and PDI_REALIZATION_DATE <= last_day(now())) and PDI_STATUS= 9 and Contract_Flag ='A'and PDI_TYPE !='ADJ' group by Contract_ID,month_day")

filter_df_2.createOrReplaceTempView("fl")

Final_Member_data=spark.sql("select md.Contract_ID,md.Control_Location as ControlLocation , md.category,md.Team_Allocation,md.Due,fl.PDI_Amount,fl.month_day from md left join fl on md.Contract_ID=fl.Contract_ID")

Final_Member_data.createOrReplaceTempView('fm')

Final_Member_data = spark.sql('select *,case when category = "0-30 days" then "0-30 days" when category = "Current Year" then "Current Yr" when category = "1 YEAR TO 2 YEARS" then "Current +1Yr" when category = "2 YEARS TO 3 YEARS" then "Current +2Yr" when category = "3 YEARS TO 4 YEARS" then "Current +3Yr" when category = "4 YEARS +" then "Current +4Yr" end as Category2 from fm ')

Final_Member_data = Final_Member_data.drop('category')

Final_Member_data = Final_Member_data.withColumnRenamed("Category2","category")

Final_Member_data.createOrReplaceTempView('fm')

### code for 5th sheet "Branch - Category Split"

Branch_category_split = sqlContext.sql('select ControlLocation, count(case when category is null and PDI_Amount > 0 then Contract_ID end) as unknown_count, sum(case when category is null then PDI_Amount else 0 end) as unknown_Amount, count(case when category = "0-30 days" and PDI_Amount > 0 then Contract_ID end) as 0_30_days_count, sum(case when category="0-30 days" then PDI_Amount else 0 end) as 0_30_days_Amount, count(case when category = "Current Yr" and PDI_Amount > 0 then Contract_ID end) as Current_Yr_count, sum(case when category="Current Yr" then PDI_Amount else 0 end) as Current_Yr_Amount, count(case when category = "Current +1Yr" and PDI_Amount > 0 then Contract_ID end) as Current_1_Yr_count, sum(case when category="Current +1Yr" then PDI_Amount else 0 end) as Current_1_Yr_Amount , count(case when category = "Current +2Yr" and PDI_Amount > 0 then Contract_ID end) as Current_2_Yr_count, sum(case when category="Current +2Yr" then PDI_Amount else 0 end) as Current_2_Yr_Amount, count(case when category = "Current +3Yr" and PDI_Amount > 0 then Contract_ID end) as Current_3_Yr_count, sum(case when category="Current +3Yr" then PDI_Amount else 0 end) as Current_3_Yr_Amount, count(case when category = "Current +4Yr" and PDI_Amount > 0 then Contract_ID end) as Current_4_Yr_count, sum(case when category="Current +4Yr" then PDI_Amount else 0 end) as Current_4_Yr_Amount, current_date() as Rundate from fm group by ControlLocation')

Branch_category_split.createOrReplaceTempView('bcs')

#Branch_category_split.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Branch_category_split",mode="append",header=True)

# above statement to be performed for first manual run

Branch_category_split_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Branch_category_split/Branch_category_split.csv",inferSchema=True,header=True)

Branch_category_split_old = Branch_category_split_old.withColumn('Rundate',to_date('Rundate','yyyy-MM-dd')) 

Branch_category_split_old.createOrReplaceTempView('bcso')

Branch_category_split_final=spark.sql('select * from bcso union all select * from bcs')

Branch_category_split_final = Branch_category_split_final.withColumn('Rundate',to_date('Rundate','yyyy-MM-dd')) 

Branch_category_split_final = Branch_category_split_final.withColumn('Rundate',Branch_category_split_final['Rundate'].cast('String'))

Branch_category_split_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Branch_category_split",mode="append",header=True)

cmd2 = "aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Branch_category_split/Branch_category_split.csv"

os.system(cmd2)

time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem2 = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)

fs = FileSystem2.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Branch_category_split/"
created_file_path2 = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path2,Path(file_path + "Branch_category_split.csv"))

#### Channel - Category Split Sheet Code 

Channel_category_split = sqlContext.sql('select Team_Allocation, count(case when category = "0-30 days" and Due > 0 then Contract_ID else 0 end) as 0_30_days_collectable_count, sum(case when category = "0-30 days" and Due > 0 then Due else 0 end) as 0_30_days_collectable_Amount, count(case when category = "0-30 days" and PDI_AMOUNT > 0 then Contract_ID else 0 end) as 0_30_days_collected_count, sum(case when category = "0-30 days" then PDI_AMOUNT else 0 end) as 0_30_days_collected_Amount, count(case when category = "Current Yr" and Due > 0 then Contract_ID else 0 end) as Current_Yr_count_collectable_count, sum(case when category = "Current Yr" and Due > 0 then Due else 0 end) as Current_Yr_count_collectable_Amount, count(case when category = "Current Yr" and PDI_AMOUNT > 0 then Contract_ID else 0 end) as Current_Yr_count_collected_count, sum(case when category = "Current Yr" then PDI_AMOUNT else 0 end) as Current_Yr_count_collected_Amount, count(case when category = "Current +1Yr" and Due > 0 then Contract_ID else 0 end) as Current_1_Yr_count_collectable_count, sum(case when category = "Current +1Yr" and Due > 0 then Due else 0 end) as Current_1_Yr_count_collectable_Amount, count(case when category = "Current +1Yr" and PDI_AMOUNT > 0 then Contract_ID else 0 end) as Current_1_Yr_count_collected_count, sum(case when category = "Current +1Yr" then PDI_AMOUNT else 0 end) as Current_1_Yr_count_collected_Amount, count(case when category = "Current +2Yr" and Due > 0 then Contract_ID else 0 end) as Current_2_Yr_count_collectable_count, sum(case when category = "Current +2Yr" and Due > 0 then Due else 0 end) as Current_2_Yr_count_collectable_Amount, count(case when category = "Current +2Yr" and PDI_AMOUNT > 0 then Contract_ID else 0 end) as Current_2_Yr_count_collected_count, sum(case when category = "Current +2Yr" then PDI_AMOUNT else 0 end) as Current_2_Yr_count_collected_Amount, count(case when category = "Current +3Yr" and Due > 0 then Contract_ID else 0 end) as Current_3_Yr_count_collectable_count, sum(case when category = "Current +3Yr" and Due > 0 then Due else 0 end) as Current_3_Yr_count_collectable_Amount, count(case when category = "Current +3Yr" and PDI_AMOUNT > 0 then Contract_ID else 0 end) as Current_3_Yr_count_collected_count, sum(case when category = "Current +3Yr" then PDI_AMOUNT else 0 end) as Current_3_Yr_count_collected_Amount, count(case when category = "Current +4Yr" and Due > 0 then Contract_ID else 0 end) as Current_4_Yr_count_collectable_count, sum(case when category = "Current +4Yr" and Due > 0 then Due else 0 end) as Current_4_Yr_count_collectable_Amount, count(case when category = "Current +4Yr" and PDI_AMOUNT > 0 then Contract_ID else 0 end) as Current_4_Yr_count_collected_count, sum(case when category = "Current +4Yr" then PDI_AMOUNT else 0 end) as Current_4_Yr_count_collected_Amount, current_date() as Rundate from fm group by Team_Allocation') 

## Channel_category_split.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Channel_category_split/Channel_category_split",mode="append",header=True)

Channel_category_split.createOrReplaceTempView('ccs')

Channel_category_split_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Channel_category_split/Channel_category_split/Channel_category_split.csv",inferSchema=True,header=True)

Channel_category_split_old = Channel_category_split_old.withColumn('Rundate',to_date('Rundate','yyyy-MM-dd'))

Channel_category_split_old.createOrReplaceTempView('ccso')

Channel_category_split_final=spark.sql('select * from ccso union all select * from ccs')

Channel_category_split_final = Channel_category_split_final.withColumn('Rundate',to_date('Rundate','yyyy-MM-dd'))

Channel_category_split_final = Channel_category_split_final.withColumn('Rundate',Channel_category_split_final['Rundate'].cast('String'))

Channel_category_split_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Channel_category_split/Channel_category_split",mode="append",header=True)

cmd4 = "aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/Channel_category_split/Channel_category_split/Channel_category_split.csv"

os.system(cmd4)

time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem4 = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)

fs = FileSystem4.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/intermediate_files/ASF_Integrated/Channel_category_split/Channel_category_split/"
created_file_path4 = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path4,Path(file_path + "Channel_category_split.csv"))

### table details information

table_1 = sqlContext.sql('select "Allocation Table" as Table_name, max(date) as last_modified_date, current_date() as rundate from al ')

table_2 = sqlContext.sql('select "ZFI-PDI Table" as Table_name, max(dw_last_modified_date) as last_modified_date, current_date() as rundate from cpd ')

table_A = table_1.unionAll(table_2)

table_A.createOrReplaceTempView('tablea')

# table_A.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Table_Details",mode="append",header=True)

table_A_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Table_Details/Table_Details.csv",inferSchema=True,header=True)

table_A_old = table_A_old.withColumn('rundate',to_date('rundate','yyyy-MM-dd'))

table_A_old.createOrReplaceTempView('tao')

table_A_final=spark.sql('select * from tao union all select * from tablea')

table_A_final = table_A_final.withColumn('rundate',to_date('rundate','yyyy-MM-dd'))

table_A_final = table_A_final.withColumn('rundate',table_A_final['rundate'].cast('String'))

table_A_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Table_Details",mode="append",header=True)

cmd3="aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Table_Details/Table_Details.csv"
os.system(cmd3)

time.sleep(15)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem3 = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(15)

fs = FileSystem3.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/intermediate_files/Asf_Integrated_Dashboard/Branch_category_split/Table_Details/"
created_file_path3 = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(15)
fs.rename(created_file_path3,Path(file_path + "Table_Details.csv"))

## table details for Channel Category Sheet
######
######

table_11 = sqlContext.sql('select "Allocation Table" as Table_name, max(date) as last_modified_date, current_date() as rundate from al ')

table_22 = sqlContext.sql('select "ZFI-PDI Table" as Table_name, max(dw_last_modified_date) as last_modified_date, current_date() as rundate from cpd ')

table_AA = table_11.unionAll(table_22)

#table_AA.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Channel_category_split/Table_Details",mode="append",header=True)

table_AA.createOrReplaceTempView('tableaa')

table_AA_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Branch_category_split/Table_Details/Table_Details.csv",inferSchema=True,header=True)

table_AA_old = table_AA_old.withColumn('rundate',to_date('rundate','yyyy-MM-dd'))

table_AA_old.createOrReplaceTempView('taao')

table_AA_final = spark.sql('select * from taao union all select * from tableaa')

table_AA_final = table_AA_final.withColumn('rundate',to_date('rundate','yyyy-MM-dd'))

table_AA_final = table_AA_final.withColumn('rundate',table_AA_final['rundate'].cast('String'))

table_AA_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/ASF_Integrated/Channel_category_split/Table_Details",mode="append",header=True)

cmd4="aws s3 rm s3://cmh-process-data/intermediate_files/ASF_Integrated/Channel_category_split/Table_Details/Table_Details.csv"
os.system(cmd4)

time.sleep(15)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem4 = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(15)

fs = FileSystem4.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/intermediate_files/ASF_Integrated/Channel_category_split/Table_Details/"
created_file_path4 = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(15)
fs.rename(created_file_path4,Path(file_path + "Table_Details.csv"))

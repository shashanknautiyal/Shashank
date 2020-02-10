# Importing the required Libraries.

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

dt=datetime.datetime.now().strftime ("%m%Y")
#dt=datetime.datetime.now().strftime ("%d%m%Y")
complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-process-data/master-tables/master_table_allocation/allocation.csv")

for col in complete_dump.columns:
	complete_dump=complete_dump.withColumnRenamed(col,col.replace(" ",""))

complete_dump_Selected_columns=complete_dump.select('Contract_ID','Member_ID','Membership_Name','Control_Location','Due','category','ASFoverdueCount','HolidayStartDate','Month_of_Charge','BillingType','Season','Sale_Posted_Date','HWC_bucket','Allocation','Date')

#ASF_Charged,ASF_Realized

complete_dump_Selected_columns.createOrReplaceTempView("complete_dump")

incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/allocation-file/Allocation_OD_"+dt+".csv")

#incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/allocation-file/Allocation_"+dt+".csv")

for col in incremental_dump.columns:
	incremental_dump=incremental_dump.withColumnRenamed(col,col.replace(" ",""))

#incremental_dump=incremental_dump.withColumn('date',substring('date',1,7))

incremental_dump_selected_columns=incremental_dump.select('Contract_ID','Member_ID','Membership_Name','Control_Location','Due','category','ASFoverdueCount','HolidayStartDate','Month_of_Charge','BillingType','Season','Sale_Posted_Date','HWC_bucket','Allocation','Date')

#incremental_dump_selected_columns=incremental_dump_selected_columns.withColumn('Date',when(incremental_dump_selected_columns.Date== "2020-1","2020-01").otherwise(incremental_dump_selected_columns.Date))
incremental_dump_selected_columns.createOrReplaceTempView("incremental_dump")


data=sqlContext.sql("select * from complete_dump union all select * from incremental_dump")
data.createOrReplaceTempView("data")

data.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_allocation",mode="append",header="true")
time.sleep(10)

cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_allocation/allocation.csv"
os.system(cmd)

time.sleep(10)
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_allocation/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path,Path(file_path + "allocation.csv"))



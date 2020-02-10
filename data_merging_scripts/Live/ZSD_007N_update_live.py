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

dt=datetime.datetime.now().strftime ("%d%m%Y")

complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-process-data/master-tables/master_table_zsd_007n/ZSD_007N.csv")

complete_dump=complete_dump.withColumn("unique_key", concat("CONTRACT_NUMBER",lit("-"),"INSTALLMENT_NUMBER",lit("-"),"PDI_STATUS",lit("-"),"CUSTOMER_ACCOUNT_NO"))
complete_dump.createOrReplaceTempView("complete_dump")


incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/zsd007n/zsd007n_"+dt+".csv")

incremental_dump=incremental_dump.withColumn("unique_key", concat("CONTRACT_NUMBER",lit("-"),"INSTALLMENT_NUMBER",lit("-"),"PDI_STATUS",lit("-"),"CUSTOMER_ACCOUNT_NO"))
incremental_dump.createOrReplaceTempView("incremental_dump")

data=sqlContext.sql("select * from complete_dump union all select * from incremental_dump")
data.createOrReplaceTempView("data")


final_data =spark.sql("select s.CONTRACT_NUMBER, s.INSTALLMENT_DATE, s.PDI_STATUS,s.PDI_TYPE, s.INSTALLMENT_NUMBER,s.DOWNPAYMENT_DATE,s.REGISTRATION_STATUS,s.CUSTOMER_ACCOUNT_NO,s.VAULTED_DATE,s.PDI_RECEIVED_DATE,s.MEMBERSHIP_ID,s.dw_last_modified_date,s.unique_key from (select *, row_number() over(partition by unique_key order by dw_last_modified_date desc) as rownum1 from data) s where rownum1 =1 ")

final_data.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_zsd_007n",mode="append",header="true")

cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_zsd_007n/ZSD_007N.csv"
os.system(cmd)

time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_zsd_007n/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()

time.sleep(10)
fs.rename(created_file_path,Path(file_path + "ZSD_007N.csv"))






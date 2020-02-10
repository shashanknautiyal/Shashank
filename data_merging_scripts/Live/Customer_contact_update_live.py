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

complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-process-data/master-tables/master_table_customer_contact/customer_contact.csv")
complete_dump=complete_dump.withColumn('dLastUpdated',to_date('dLastUpdated','yyyy-MM-dd'))
complete_dump.createOrReplaceTempView("complete_dump")

incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/customer-contact/customer-contact_"+dt+".csv")
incremental_dump=incremental_dump.withColumn('dLastUpdated',to_date('dLastUpdated','yyyy-MM-dd'))
incremental_dump.createOrReplaceTempView("incremental_dump")

data=spark.sql("select * from complete_dump union all select * from incremental_dump")

data.createOrReplaceTempView("data")

final_data =spark.sql("select s.aContactID, s.tMobile, s.tResPhone1,s.tResPhone2, s.tRescity,s.tResState,s.tResPinCode, s.dLastUpdated  from (select *,row_number() over(partition by aContactID order by dLastUpdated desc) as rownum1 from data) s where rownum1 =1")
final_data.createOrReplaceTempView("final_data")



final_data.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_customer_contact",mode="append",header="true")


cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_customer_contact/customer_contact.csv"
os.system(cmd)
time.sleep(10)



sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)
fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_customer_contact/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path,Path(file_path + "customer_contact.csv"))
   
   
   

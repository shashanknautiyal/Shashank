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

# pyspark

complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/complete-dump/sales-hirarchy/sales-hirarchy_Till 23072019.csv")

complete_dump.createOrReplaceTempView("complete_dump")

incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/sales-hirarchy/*")

incremental_dump.createOrReplaceTempView("incremental_dump")

data=sqlContext.sql("select * from complete_dump union all select * from incremental_dump")

data.createOrReplaceTempView("data")

final_data =spark.sql("select s.nSalesHirarchyID, s.tName, s.tReportingManager,s.dw_last_modified_date from (select *, row_number() over(partition by nSalesHirarchyID order by dw_last_modified_date desc) as rownum1 from data) s where rownum1 =1 ")

final_data.coalesce(1).write.option("delimiter", "|").csv("s3://cmh-process-data/master-tables/master_table_hierarchy",mode="append",header="true")


cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_hierarchy/Sales_hierarchy.csv"
os.system(cmd)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_hierarchy/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
fs.rename(created_file_path,Path(file_path + "Sales_hierarchy.csv"))


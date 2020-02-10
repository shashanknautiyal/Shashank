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

incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/Zod inflow/Zod_inflow_"+dt+".csv")


incremental_dump.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_zod",mode="append",header="true")


cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_zod/zod_inflow.csv"
os.system(cmd)
time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)
fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_zod/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path,Path(file_path + "zod_inflow.csv"))





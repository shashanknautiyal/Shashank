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

complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-process-data/master-tables/master_table_dialler_recording_log/dialler_recording_log.csv")

complete_dump.createOrReplaceTempView("complete_dump")


incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/Dialler recording log/Dialler_recording_log_"+dt+".csv")

incremental_dump.createOrReplaceTempView("incremental_dump")

data=sqlContext.sql("select * from complete_dump union all select * from incremental_dump")

data.createOrReplaceTempView("data")


data.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_dialler_recording_log",mode="append",header="true")

cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_dialler_recording_log/dialler_recording_log.csv"
os.system(cmd)

time.sleep(10)
dt=datetime.datetime.now().strftime ("%d%m%Y")
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
time.sleep(10)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_dialler_recording_log/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path,Path(file_path + "dialler_recording_log.csv"))




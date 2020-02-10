from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

import os
from pyspark.sql.functions import *
from pyspark.sql import Window
import datetime,time

dt=datetime.datetime.now().strftime ("%d%m%Y")

complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-process-data/master-tables/master_table_Emi_dialler/emi_dialler.csv")

complete_dump.createOrReplaceTempView("complete_dump")

incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/EMI Dialer/EMI_Dialer_"+dt+".csv")
incremental_dump.createOrReplaceTempView("incremental_dump")

data=sqlContext.sql("select * from complete_dump union all select * from incremental_dump")
data.createOrReplaceTempView("data")


data.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_Emi_dialler",mode="append",header="true")

time.sleep(10)

cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_Emi_dialler/emi_dialler.csv"
os.system(cmd)
time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
time.sleep(10)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_Emi_dialler/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path,Path(file_path + "emi_dialler.csv"))



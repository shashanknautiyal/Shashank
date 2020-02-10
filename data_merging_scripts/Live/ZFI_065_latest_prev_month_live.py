from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem


import os
from pyspark.sql.functions import *
from pyspark.sql import Window
import datetime,time
from dateutil.relativedelta import *

day=datetime.datetime.now().strftime ("%d")




if day == '01':

	one_month_ago = datetime.datetime.now() - relativedelta(months=1)
	dt=one_month_ago.strftime ("%d%m%Y")
	
	incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/zfi065n/ZFI065N_OUTPUT_"+dt+".csv")
	
	incremental_dump.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_zfi065n_previous_month",mode="append",header="true")
	
	
	cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_zfi065n_previous_month/ZFI_065_monthly_file.csv"
	os.system(cmd)
	
	time.sleep(10)

	fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
	file_path = "s3://cmh-process-data/master-tables/master_table_zfi065n_previous_month/"
	created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
	
	time.sleep(10)
	fs.rename(created_file_path,Path(file_path + "ZFI_065_monthly_file.csv"))

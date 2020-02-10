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
import datetime

dt=datetime.datetime.now().strftime ("%d")



sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/complete-dump/zfi065n/ZFI065N_OUTPUT_from 1st july 19 to 22 july 19.csv")
complete_dump.createOrReplaceTempView("complete_dump")


incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/zfi065n/*")
incremental_dump.createOrReplaceTempView("incremental_dump")

data=sqlContext.sql("select * from complete_dump union all select * from incremental_dump")
data.createOrReplaceTempView("data")

final_data =spark.sql("select s.ContractNo, s.MemberID, s.MortgageID, s.MortgageCheck, s.Status, s.Name, s.ODBucketMax, s.SourceSystem, s.StatusDate, s.SaleDate, s.ProductPrice, s.EMIAmount, s.EMIOverDueCount, s.EMIRealizedAmount, s.NoDue_EMI_DP, s.EMIBanked, s.EMIOverdueAmount, s.Total_OD_EMI_DP, s.DPRealizedAmount, s.DPOverdueAmount, s.RunDate, s.RealignmentDate, s.EMIRealizedCount, s.RealizedPercentContract, s.DownPaymentAmount, s.dw_lastupdated from (select *, row_number() over(partition by MortgageID order by dw_lastupdated desc) as rownum1 from data) s where rownum1 =1 ")

final_data.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_065_daily",mode="append",header="true")


cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_ZFI_065_daily/ZFI_065_Daily.csv"
os.system(cmd)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_ZFI_065_daily/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
fs.rename(created_file_path,Path(file_path + "ZFI_065_Daily.csv"))



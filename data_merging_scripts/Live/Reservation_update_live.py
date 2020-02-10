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

# Pyspark

complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-process-data/master-tables/master_table_reservation_done/reservation_done.csv")
complete_dump.createOrReplaceTempView("complete_dump")

incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/reservation done/reservation_done_"+dt+".csv*")
incremental_dump.createOrReplaceTempView("incremental_dump")

data=spark.sql("select * from complete_dump union all select * from incremental_dump")
data.createOrReplaceTempView("data")


final_data =spark.sql("select s.ContractID, s.ReservationNo, s.RoomNights,s.SeasonHolidayed, s.nProductID,s.CheckIn,s.CheckOut, s.dBookingDate,s.ReservationStatusID,s.dwLastUpdatedOn from (select *,row_number() over(partition by ReservationNo order by dwLastUpdatedOn desc) as rownum1 from data) s where rownum1 =1")
final_data.createOrReplaceTempView("final_data")


final_data.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_reservation_done",mode="append",header="true")

cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_reservation_done/reservation_done.csv"
os.system(cmd)

time.sleep(10)
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem


fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_reservation_done/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path,Path(file_path + "reservation_done.csv"))

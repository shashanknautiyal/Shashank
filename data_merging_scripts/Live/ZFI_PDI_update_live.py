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
#dt='16122019'
complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-process-data/master-tables/master_table_ZFI_PDI/ZFI_PDI.csv")
complete_dump=complete_dump.withColumn('dw_last_modified_date',to_date('dw_last_modified_date','yyyy-MM-dd'))
complete_dump.createOrReplaceTempView("complete_dump")

complete_dump=spark.sql("select *, case when reference_document is null then sales_document else reference_document end as reference_doc_final from complete_dump")
complete_dump=complete_dump.withColumn("unique_key", concat("reference_doc_final",lit("-"),"Mortgage_ID",lit("-"),"installment_number",lit("-"),"pdi_id"))
complete_dump.createOrReplaceTempView("complete_dump")

incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/zfi-pdi/zfi-pdi_"+dt+".csv")

#incremental_dump.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_PDI",mode="append",header="true")

#incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/zfi-pdi/zfi-pdi_09102019.csv")

incremental_dump=incremental_dump.withColumn('dw_last_modified_date',to_date('dw_last_modified_date','yyyy-MM-dd'))
incremental_dump.createOrReplaceTempView("incremental_dump")
incremental_dump=spark.sql("select *, case when reference_document is null then sales_document else reference_document end as reference_doc_final from incremental_dump")
incremental_dump=incremental_dump.withColumn("unique_key", concat("reference_doc_final",lit("-"),"Mortgage_ID",lit("-"),"installment_number",lit("-"),"pdi_id"))
incremental_dump.createOrReplaceTempView("incremental_dump")

data=spark.sql("select * from complete_dump union all select * from incremental_dump")
data.createOrReplaceTempView("data")

final_data =spark.sql("select s.reference_document, s.sales_document, s.Mortgage_ID, s.pdi_id, s.PDI_NUMBER, s.PDI_TYPE, s.installment_number, s.Installment_Date, s.PDI_STATUS, s.EMI_AMOUNT, s.PDI_AMOUNT, s.PDI_DATE, s.PDI_RECEIVED_DATE, s.unique_identification_no, s.waiver_date, s.at_branch_date, s.BANKED_DATE, s.VAULTED_DATE, s.PDI_REALIZATION_DATE, s.PDI_RETURNED_DATE, s.reversed_date, s.INSTRUMENT_EXPIRED_DATE, s.TOTAL_REALIZED, s.Contract_Flag, s.card_expiry_date, s.dw_last_modified_date from (select *,row_number() over(partition by unique_key order by dw_last_modified_date desc) as rownum1 from data) s where rownum1 =1")
final_data.createOrReplaceTempView("final_data")

final_data.coalesce(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_PDI",mode="append",header="true")

cmd="aws s3 rm s3://cmh-process-data/master-tables/master_table_ZFI_PDI/ZFI_PDI.csv"
os.system(cmd)

time.sleep(10)

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
file_path = "s3://cmh-process-data/master-tables/master_table_ZFI_PDI/"
created_file_path = fs.globStatus(Path(file_path + "part*.csv"))[0].getPath()
time.sleep(10)
fs.rename(created_file_path,Path(file_path + "ZFI_PDI.csv"))



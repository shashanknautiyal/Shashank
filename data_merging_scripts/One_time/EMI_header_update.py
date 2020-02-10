from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window


## Pyspark

complete_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/complete-dump/EMI header/emi header_TiIl 31072019.csv")

complete_dump=complete_dump.withColumn('dtCreated',complete_dump.dtCreated.cast('date'))

complete_dump=complete_dump.withColumn("unique_key", concat("contractID",lit("-"),"cancellation_trigger",lit("-"),"StatusOfRequest",lit("-"),"dtCreated"))

complete_dump.createOrReplaceTempView("complete_dump")


incremental_dump = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema","true").csv("s3://cmh-raw-data/incremental-load/cancellation-request/*")

incremental_dump=incremental_dump.withColumn('dtCreated',incremental_dump.dtCreated.cast('date'))

incremental_dump=incremental_dump.withColumn("unique_key", concat("contractID",lit("-"),"cancellation_trigger",lit("-"),"StatusOfRequest",lit("-"),"dtCreated"))

incremental_dump.createOrReplaceTempView("incremental_dump")

data=sqlContext.sql("select * from complete_dump union all select * from incremental_dump")

data.createOrReplaceTempView("data")


final_data =spark.sql("select s.contractID, s.Memberid, s.cancellation_trigger,s.StatusOfRequest, s.dtCreated,s.dtLastModified,s.dw_last_modified_date,s.unique_key from (select *, row_number() over(partition by unique_key order by dw_last_modified_date desc) as rownum1 from data) s where rownum1 =1 ")

final_data.repartition(1).write.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_cancellation_request",mode="append",header="true")






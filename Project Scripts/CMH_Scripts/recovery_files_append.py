
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window

# recovery files before jan 2019

recovery_file= spark.read.option("delimiter",",").csv(r"s3://cmh-process-data/test-1/Recovery/Recoveries and Inflow 20190322.csv",header=True,inferSchema=True)

recovery_file=recovery_file.withColumn('Mon_year',concat(recovery_file.Month,lit('-'),recovery_file.Year))

recovery_file=recovery_file.withColumn('rec_date',to_date('Mon_year','MM-yyyy'))

recovery_file=recovery_file.withColumnRenamed('Contract No.','Contract_No')

recovery_file.createOrReplaceTempView('rf1')

# recovery jan

recovery_jan= spark.read.option("delimiter",",").csv("s3://cmh-process-data/test-1/Recovery/Jan Recovery.csv",header=True,inferSchema=True)

recovery_jan=recovery_jan.withColumn('Mon_year',lit('01-2019'))
recovery_jan=recovery_jan.withColumn('rec_date',to_date('Mon_year','MM-yyyy'))

recovery_jan=recovery_jan.withColumnRenamed('Contract_id','Contract_No')

recovery_jan.createOrReplaceTempView('rj')

recovery_file_jan=spark.sql('select Contract_No, rec_date from rf1 union all select Contract_No, rec_date from rj')

recovery_file_jan.createOrReplaceTempView('rfj')


# recovery_feb
recovery_feb= spark.read.option("delimiter",",").csv("s3://cmh-process-data/test-1/Recovery/Feb_2019_OD_Recovery.csv",header=True,inferSchema=True)

recovery_feb=recovery_feb.withColumnRenamed('Contract No.','Contract_No').withColumnRenamed('Date on Data Given','Date_on_Data_Given')

recovery_feb=recovery_feb.withColumn('Mon_year',lit('02-2019'))

recovery_feb=recovery_feb.withColumn('rec_date',to_date('Mon_year','MM-yyyy'))

recovery_feb.createOrReplaceTempView('rf')

recovery_jan_feb=spark.sql('select Contract_No, rec_date from rfj union all select Contract_No, rec_date from rf')

recovery_jan_feb.createOrReplaceTempView('rjf')

# recovery mar_july

recovery_mar_july= spark.read.option("delimiter","|").csv("s3://cmh-raw-data/incremental-load/inflow-sheet-emi/inflow sheet emi from Mar to Jul.csv",header=True,inferSchema=True)

recovery_mar_july=recovery_mar_july.withColumnRenamed('Contract No.','Contract_No')

recovery_mar_july=recovery_mar_july.withColumn('rec_date',to_date('Month','MMM_yyyy'))

recovery_mar_july.createOrReplaceTempView('rm')

recovery_final=spark.sql('select Contract_No, rec_date from rjf union all select Contract_No, rec_date from rm')

recovery_final=recovery_final.withColumn('Mon_year', concat(month('rec_date'),lit('-'),year('rec_date')))


recovery_final.repartition(1).write.csv("s3://cmh-process-data/master-tables/master_table_Recovery",mode="append",header="true")



from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window

customer=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/customer-contact/Customer Contact_11042019.csv",header =True,inferSchema=True)
customer.createOrReplaceTempView("cc")

contract=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/contract-data/Contract data_11042019.csv",header =True,inferSchema=True)
contract.createOrReplaceTempView("cd")

prop=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/propensity-scores/Propensity Scores_11042019.csv",header =True,inferSchema=True)
prop.createOrReplaceTempView("ps")

Allocation=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/allocation-file/ASF_OD_Feb_19.csv",header =True,inferSchema=True)
Allocation.createOrReplaceTempView("al")


Asf_summary=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/asfsummary/ASFSummary.csv",header =True,inferSchema=True)

Asf_summary=Asf_summary.withColumn('Contractno',Asf_summary['Contractno'].cast('Integer'))
Asf_summary.createOrReplaceTempView("as")

Member_data=spark.sql("select distinct al.Contract_ID, cc.nMemberID as Member_ID, cd.ControlLocation, ps.Prob_Sore,cd.dtSalesPosted, ps.Contact_ID, right(cc.tMobile, 10) as Mobile_No,right(cc.tResPhone1,10) as tResPhone1, right(cc.tResPhone2,10) as tResPhone2, ps.category, substring(cd.dtSalesPosted,4,2) as MonthofCharge,al.Allocation as Team_Allocation, ps.Bucket as Rating,as.Due as Due from al left join ps on al.Contract_ID = ps.Contract_ID left join cd on al.Contract_ID = cd.aContractID left join cc on cd.nMember = cc.aContactID left join as on al.Contract_ID=as.Contractno where al.Allocation = 'PACE'")
Member_data.createOrReplaceTempView("md")


#zfi=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\ZFI pdi\ZFI PDI_12042019.csv",header =True,inferSchema=True)

zfi=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/zfi-pdi/ZFI PDI_12042019.csv",header =True,inferSchema=True)

zfi.createOrReplaceTempView("cpd")

filter_df=spark.sql("select reference_document,pdi_date,pdi_type,pdi_status,pdi_amount,pdi_realization_date, year(pdi_realization_date) as year,month(pdi_realization_date) as month, contract_flag from cpd")
filter_df=filter_df.withColumnRenamed('reference_document','Contract_ID')
filter_df.createOrReplaceTempView("filter")

filter_df_2=spark.sql("select distinct Contract_ID, sum(pdi_amount) as PDI_Amount from filter where year ='2019'and month ='02'and pdi_status= '09'and contract_flag ='A'and pdi_type !='ADJ' group by Contract_ID")

filter_df_2.createOrReplaceTempView("fl")


Final_Member_data=spark.sql("select md.*,fl.PDI_Amount from md left join fl on md.Contract_ID=fl.Contract_ID")


Final_Member_data.createOrReplaceTempView('fm')

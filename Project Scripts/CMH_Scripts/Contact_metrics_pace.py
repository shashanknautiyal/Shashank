from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window

#customer=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\Customer Contact\Customer Contact_11042019.csv",header =True,inferSchema=True)

customer=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/customer-contact/Customer Contact_11042019.csv",header =True,inferSchema=True)

customer.createOrReplaceTempView("cc")

#contract=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\Contract data\Contract data_11042019.csv",header =True,inferSchema=True)

contract=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/contract-data/Contract data_11042019.csv",header =True,inferSchema=True)

contract.createOrReplaceTempView("cd")

#prop=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\Propensity Scores\Propensity Scores_11042019.csv",header =True,inferSchema=True)

prop=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/propensity-scores/Propensity Scores_11042019.csv",header =True,inferSchema=True)

prop.createOrReplaceTempView("ps")

#Allocation=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\Allocation data\ASF_OD_Feb_19.csv",header =True,inferSchema=True)

Allocation=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/allocation-file/ASF_OD_Feb_19.csv",header =True,inferSchema=True)
Allocation.createOrReplaceTempView("al")

#Asf_summary=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\ASF Summary\ASFSummary.csv",header =True,inferSchema=True)

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

#pd=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\Pace dialler\PACE dialer_feb 19.csv",header =True,inferSchema=True)

pd=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/pace-dialer/PACE dialer_feb 19.csv",header =True,inferSchema=True)
pd.createOrReplaceTempView('new')

pd=spark.sql("Select right(Cli,10) as Cli,Contactedstatus from new")


pd_mobile=pd.crosstab('Cli','Contactedstatus')
pd_mobile=pd_mobile.withColumnRenamed('Contacted','Contacted_m').withColumnRenamed('Not Contacted','Not_Contacted_m')
pd_mobile.createOrReplaceTempView('ps')


pd_tres1=pd.crosstab('Cli','Contactedstatus')
pd_tres1=pd_tres1.withColumnRenamed('Contacted','Contacted_t1').withColumnRenamed('Not Contacted','Not_Contacted_t1')
pd_tres1.createOrReplaceTempView('ps1')


pd_tres2=pd.crosstab('Cli','Contactedstatus')
pd_tres2=pd_tres2.withColumnRenamed('Contacted','Contacted_t2').withColumnRenamed('Not Contacted','Not_Contacted_t2')
pd_tres2.createOrReplaceTempView('ps2')


t1=spark.sql("select fm.*, ps.Contacted_m, ps.Not_Contacted_m from fm left join ps on fm.Mobile_No=ps.Cli_Contactedstatus")

t1.createOrReplaceTempView('temp')

t2=spark.sql("select temp.*,ps1.Contacted_t1,ps1.Not_Contacted_t1 from temp left join ps1 on temp.tResPhone1=ps1.Cli_Contactedstatus")

t2.createOrReplaceTempView('temp2')

t3=spark.sql("select temp2.*,ps2.Contacted_t2,ps2.Not_Contacted_t2 from temp2 left join ps2 on temp2.tResPhone2=ps2.Cli_Contactedstatus")

t3=t3.na.fill(0)


t3=t3.withColumn('Contacted',greatest(t3.Contacted_m , t3.Contacted_t1, t3.Contacted_t2)).withColumn('Not_Contacted',greatest(t3.Not_Contacted_m,t3.Not_Contacted_t1,t3.Not_Contacted_t2))

#t3.repartition(1).write.csv("D:\\final_data\final_memdata.csv",mode="append",header="true")

t3.repartition(1).write.csv("s3://cmh-process-data/test-1/Pace_Member_Data",mode="append",header="true")

#test=spark.read.option("delimiter",",").csv("D:\\final_data\Member_data_new.csv",header =True,inferSchema=True)

test=t3

test=test.withColumn('total_attempted',(test.Contacted+test.Not_Contacted))

test.createOrReplaceTempView("ttr")

test5=spark.sql("select t.*,case when total_attempted >0 then 'yes' else 'no' end as attempted_dialer,case when contacted>0 then 'yes' else 'no' end as contacted_dialer,case when PDI_Amount>0 then 1 else 0 end as realized from ttr t")

test5.createOrReplaceTempView("mem")

test6=spark.sql("select m.Category,sum(case when m.Rating='Hot' then 1 else 0 end) as base_hot,sum(case when m.Rating='Warm' then 1 else 0 end) as base_warm,sum(case when m.Rating='Cold' then 1 else 0 end) as base_cold,count(*) as base_total,sum(case when m.Rating='Hot' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_hot,sum(case when m.Rating='Warm' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_warm,sum(case when m.Rating='Cold' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_cold,sum(case when m.attempted_dialer='yes' then 1 else 0 end) as attempted_total,sum(case when m.Rating='Hot' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_hot,sum(case when m.Rating='Warm' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_warm,sum(case when m.Rating='Cold' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_cold,sum(case when m.contacted_dialer='yes' then 1 else 0 end) as contacted_total,sum(case when m.Rating='Hot' and m.realized=1 then 1 else 0 end) as realized_hot,sum(case when m.Rating='Warm' and m.realized=1 then 1 else 0 end) as realized_warm,sum(case when m.Rating='Cold' and m.realized=1 then 1 else 0 end) as realized_cold,sum(case when m.realized=1 then 1 else 0 end) as realized_total,sum(case when m.Rating='Hot' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_hot,sum(case when m.Rating='Warm' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_warm,sum(case when m.Rating='Cold' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_cold,sum(case when m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_total,sum(case when m.Rating='Hot' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_hot,sum(case when m.Rating='Warm' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_warm,sum(case when m.Rating='Cold' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_cold,sum(case when m.realized=1 then PDI_Amount else 0 end) as Amt_realized_total from mem m where m.Category is not null group by m.Category")

test6.createOrReplaceTempView("tests")

test7=spark.sql("select t.*,((attempted_hot/base_hot)*100) as perc_attempted_hot,((attempted_warm/base_warm)*100) as perc_attempted_warm,((attempted_cold/base_cold)*100) as perc_attempted_cold,((attempted_total/base_total)*100) as perc_total,((contacted_hot/base_hot)*100) as perc_con_allocated_hot,((contacted_warm/base_warm)*100) as perc_con_allocated_warm,((contacted_cold/base_cold)*100) as perc_con_allocated_cold,((contacted_total/base_total)*100) as perc_con_allocated_total,((contacted_hot/attempted_hot)*100) as perc_con_outofattm_hot,((contacted_warm/attempted_warm)*100) as perc_con_outofattm_warm,((contacted_cold/attempted_cold)*100) as perc_con_outofattm_cold,((contacted_total/attempted_total)*100) as perc_con_outofattm_total,((no_of_calls_hot/attempted_hot)*100) as churn_hot,((no_of_calls_warm/attempted_warm)*100) as churn_warm,((no_of_calls_cold/attempted_cold)*100) as churn_cold,((no_of_calls_total/attempted_total)*100) as churn_total,no_of_calls_hot/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_hot,no_of_calls_warm/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_warm,no_of_calls_cold/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_cold,no_of_calls_total/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_total,((realized_hot/base_hot)*100) as perc_conversion_allo_hot,((realized_warm/base_warm)*100) as perc_conversion_allo_warm,((realized_cold/base_cold)*100) as perc_conversion_allo_cold,((realized_total/base_total)*100) as perc_conversion_allo_total,((realized_hot/attempted_hot)*100) as perc_conversion_attem_hot,((realized_warm/attempted_warm)*100) as perc_conversion_attem_warm,((realized_cold/attempted_cold)*100) as perc_conversion_attem_cold,((realized_total/attempted_total)*100) as perc_conversion_attem_total from tests t")


test7.repartition(1).write.csv("s3://cmh-process-data/test-1/Pace_Member_Data/Pace_Contract_Metrics",mode="append",header="true")



































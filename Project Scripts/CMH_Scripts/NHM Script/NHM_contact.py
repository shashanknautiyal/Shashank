from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import DateType

customer=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/customer-contact/customer-contact_Till 22072019.csv",header =True,inferSchema=True)

customer= customer.withColumn('aContactID',customer['aContactID'].cast('Integer'))
customer.createOrReplaceTempView("cc")

contract=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/contract-data/contract-data_Till 23072019.csv",header =True,inferSchema=True)

contract = contract.withColumn('aContractID',contract['aContractID'].cast('Integer')).withColumn('nMember',contract['nMember'].cast('Integer'))
contract.createOrReplaceTempView("cd")

prop=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/propensity-scores/propensity_scores_Till 22072019.csv",header =True,inferSchema=True)
prop.createOrReplaceTempView("ps")

allocation=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/incremental-load/allocation-file/July 2019 ASF OD Base.csv",header =True,inferSchema=True)
allocation.createOrReplaceTempView("al")

asf_summary=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/asfsummary/asfsummary_Till 22072019.csv",header =True,inferSchema=True)

asf_summary=asf_summary.withColumn('Contractno',asf_summary['Contractno'].cast('Integer'))
asf_summary.createOrReplaceTempView("as")

nhm_Member_data=spark.sql("select distinct al.Contract_ID, cd.ControlLocation, ps.Prob_Score,cd.dtSalesPosted, ps.Contact_ID, right(cc.tMobile, 10) as Mobile_No,right(cc.tResPhone1,10) as tResPhone1, right(cc.tResPhone2,10) as tResPhone2, ps.category, month(cd.dtSalesPosted) as MonthofCharge,al.Allocation as Team_Allocation, ps.Bucket as Rating,as.Due as Due from al left join ps on al.Contract_ID = ps.Contract_ID left join cd on al.Contract_ID = cd.aContractID left join cc on cd.nMember = cc.aContactID left join as on al.Contract_ID=as.Contractno where al.Allocation = 'NHM'")

nhm_Member_data.createOrReplaceTempView("md")

zfi=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/zfi-pdi/zfi-pdi_Till 23072019.csv",header =True,inferSchema=True)

zfi=zfi.withColumn('PDI_REALIZATION_DATE',to_date('PDI_REALIZATION_DATE','yyyy-MM-dd'))

zfi.createOrReplaceTempView("cpd")
filter_df=spark.sql("select reference_document,PDI_DATE,PDI_TYPE,PDI_STATUS,PDI_AMOUNT,PDI_REALIZATION_DATE, Contract_Flag from cpd")

filter_df=filter_df.withColumnRenamed('reference_document','Contract_ID')
filter_df.createOrReplaceTempView("filter")

filter_df_2=spark.sql("select distinct Contract_ID, sum(PDI_AMOUNT) as PDI_Amount from filter where (PDI_REALIZATION_DATE>=date_add(last_day(now() - interval 01 month),1) and PDI_REALIZATION_DATE<= last_day(now())) and PDI_STATUS= '09'and Contract_Flag ='A'and PDI_TYPE !='ADJ' group by Contract_ID")
filter_df_2.createOrReplaceTempView("fl")


Final_Member_data=spark.sql("select md.Contract_ID,md.ControlLocation,md.Prob_Score,md.dtSalesPosted,md.Contact_ID,md.Mobile_No,md.tResPhone1,md.tResPhone2,md.category,md.MonthofCharge,md.Team_Allocation,md.Rating,md.Due,fl.PDI_Amount from md left join fl on md.Contract_ID=fl.Contract_ID")
Final_Member_data.createOrReplaceTempView('fm')

pd=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/nhm-dialer/nhm-dialer_from Mar to Jul.csv",header =True,inferSchema=True)

pd_current= pd.filter('Date >= date_add(last_day(now() - interval 01 months),1) and Date<=(last_day(now()))')
pd_current.createOrReplaceTempView('new')

pd=spark.sql("Select right(ContactNo,10) as ContactNo,TerminationStatus from new")
pd_mobile=pd.crosstab('ContactNo','TerminationStatus')
pd_mobile=pd_mobile.withColumnRenamed('Contacted','Contacted_m').withColumnRenamed('Not Contacted','Not_Contacted_m')
pd_mobile.createOrReplaceTempView('ps')
pd_tres1=pd.crosstab('ContactNo','TerminationStatus')
pd_tres1=pd_tres1.withColumnRenamed('Contacted','Contacted_t1').withColumnRenamed('Not Contacted','Not_Contacted_t1')
pd_tres1.createOrReplaceTempView('ps1')
pd_tres2=pd.crosstab('ContactNo','TerminationStatus')
pd_tres2=pd_tres2.withColumnRenamed('Contacted','Contacted_t2').withColumnRenamed('Not Contacted','Not_Contacted_t2')
pd_tres2.createOrReplaceTempView('ps2')


t1=spark.sql("select fm.*, ps.Contacted_m, ps.Not_Contacted_m from fm left join ps on fm.Mobile_No=ps.ContactNo_TerminationStatus")
t1.createOrReplaceTempView('temp')

t2=spark.sql("select temp.*,ps1.Contacted_t1,ps1.Not_Contacted_t1 from temp left join ps1 on temp.tResPhone1=ps1.ContactNo_TerminationStatus")
t2.createOrReplaceTempView('temp2')

t3=spark.sql("select temp2.*,ps2.Contacted_t2,ps2.Not_Contacted_t2 from temp2 left join ps2 on temp2.tResPhone2=ps2.ContactNo_TerminationStatus")

t3=t3.na.fill(0)


t3=t3.withColumn('Contacted',greatest(t3.Contacted_m , t3.Contacted_t1, t3.Contacted_t2)).withColumn('Not_Contacted',greatest(t3.Not_Contacted_m,t3.Not_Contacted_t1,t3.Not_Contacted_t2))

t4=t3.withColumn('total_attempted',(t3.Contacted+t3.Not_Contacted))

t4.createOrReplaceTempView("ttr")

test5=spark.sql("select t.*,case when total_attempted >0 then 'yes' else 'no' end as attempted_dialer,case when contacted>0 then 'yes' else 'no' end as contacted_dialer,case when PDI_Amount>0 then 1 else 0 end as realized from ttr t")
test5.createOrReplaceTempView("mem")

test5=test5.withColumn('run_date',lit(current_date()))

test5.repartition(1).write.csv("s3://cmh-process-data/test-1/NHM_Contact_Metrics/Member_data",mode="append",header="true")



test6=spark.sql("select m.Category,sum(case when m.Rating='Hot' then 1 else 0 end) as base_hot,sum(case when m.Rating='Warm' then 1 else 0 end) as base_warm,sum(case when m.Rating='Cold' then 1 else 0 end) as base_cold,count(*) as base_total,sum(case when m.Rating='Hot' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_hot,sum(case when m.Rating='Warm' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_warm,sum(case when m.Rating='Cold' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_cold,sum(case when m.attempted_dialer='yes' then 1 else 0 end) as attempted_total,sum(case when m.Rating='Hot' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_hot,sum(case when m.Rating='Warm' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_warm,sum(case when m.Rating='Cold' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_cold,sum(case when m.contacted_dialer='yes' then 1 else 0 end) as contacted_total,sum(case when m.Rating='Hot' and m.realized=1 then 1 else 0 end) as realized_hot,sum(case when m.Rating='Warm' and m.realized=1 then 1 else 0 end) as realized_warm,sum(case when m.Rating='Cold' and m.realized=1 then 1 else 0 end) as realized_cold,sum(case when m.realized=1 then 1 else 0 end) as realized_total,sum(case when m.Rating='Hot' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_hot,sum(case when m.Rating='Warm' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_warm,sum(case when m.Rating='Cold' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_cold,sum(case when m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_total,sum(case when m.Rating='Hot' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_hot,sum(case when m.Rating='Warm' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_warm,sum(case when m.Rating='Cold' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_cold,sum(case when m.realized=1 then PDI_Amount else 0 end) as Amt_realized_total from mem m where m.Category is not null group by m.Category")

test6.createOrReplaceTempView("tests")

test7=spark.sql("select t.*,((attempted_hot/base_hot)*100) as perc_attempted_hot,((attempted_warm/base_warm)*100) as perc_attempted_warm,((attempted_cold/base_cold)*100) as perc_attempted_cold,((attempted_total/base_total)*100) as perc_total,((contacted_hot/base_hot)*100) as perc_con_allocated_hot,((contacted_warm/base_warm)*100) as perc_con_allocated_warm,((contacted_cold/base_cold)*100) as perc_con_allocated_cold,((contacted_total/base_total)*100) as perc_con_allocated_total,((contacted_hot/attempted_hot)*100) as perc_con_outofattm_hot,((contacted_warm/attempted_warm)*100) as perc_con_outofattm_warm,((contacted_cold/attempted_cold)*100) as perc_con_outofattm_cold,((contacted_total/attempted_total)*100) as perc_con_outofattm_total,((no_of_calls_hot/attempted_hot)) as churn_hot,((no_of_calls_warm/attempted_warm)) as churn_warm,((no_of_calls_cold/attempted_cold)) as churn_cold,((no_of_calls_total/attempted_total)) as churn_total,no_of_calls_hot/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_hot,no_of_calls_warm/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_warm,no_of_calls_cold/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_cold,no_of_calls_total/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_total,((realized_hot/base_hot)*100) as perc_conversion_allo_hot,((realized_warm/base_warm)*100) as perc_conversion_allo_warm,((realized_cold/base_cold)*100) as perc_conversion_allo_cold,((realized_total/base_total)*100) as perc_conversion_allo_total,((realized_hot/attempted_hot)*100) as perc_conversion_attem_hot,((realized_warm/attempted_warm)*100) as perc_conversion_attem_warm,((realized_cold/attempted_cold)*100) as perc_conversion_attem_cold,((realized_total/attempted_total)*100) as perc_conversion_attem_total from tests t")

test7=test7.withColumn('run_date',lit(current_date()))


test7.repartition(1).write.csv("s3://cmh-process-data/test-1/NHM_Contact_Metrics/NHM_contact_metrics",mode="append",header="true")


test_dubai=spark.sql('select * from ttr where ControlLocation = "Dubai"')

test_dubai.createOrReplaceTempView('ttr1')

test_dubai5=spark.sql("select t.*,case when total_attempted >0 then 'yes' else 'no' end as attempted_dialer,case when contacted>0 then 'yes' else 'no' end as contacted_dialer,case when PDI_Amount>0 then 1 else 0 end as realized from ttr1 t")

test_dubai5.createOrReplaceTempView("mem1")

test_dubai6 =spark.sql("select m.Category,sum(case when m.Rating='Hot' then 1 else 0 end) as base_hot,sum(case when m.Rating='Warm' then 1 else 0 end) as base_warm,sum(case when m.Rating='Cold' then 1 else 0 end) as base_cold,count(*) as base_total,sum(case when m.Rating='Hot' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_hot,sum(case when m.Rating='Warm' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_warm,sum(case when m.Rating='Cold' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_cold,sum(case when m.attempted_dialer='yes' then 1 else 0 end) as attempted_total,sum(case when m.Rating='Hot' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_hot,sum(case when m.Rating='Warm' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_warm,sum(case when m.Rating='Cold' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_cold,sum(case when m.contacted_dialer='yes' then 1 else 0 end) as contacted_total,sum(case when m.Rating='Hot' and m.realized=1 then 1 else 0 end) as realized_hot,sum(case when m.Rating='Warm' and m.realized=1 then 1 else 0 end) as realized_warm,sum(case when m.Rating='Cold' and m.realized=1 then 1 else 0 end) as realized_cold,sum(case when m.realized=1 then 1 else 0 end) as realized_total,sum(case when m.Rating='Hot' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_hot,sum(case when m.Rating='Warm' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_warm,sum(case when m.Rating='Cold' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_cold,sum(case when m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_total,sum(case when m.Rating='Hot' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_hot,sum(case when m.Rating='Warm' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_warm,sum(case when m.Rating='Cold' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_cold,sum(case when m.realized=1 then PDI_Amount else 0 end) as Amt_realized_total from mem1 m where m.Category is not null group by m.Category")

test_dubai6.createOrReplaceTempView("tests1")

test_dubai7=spark.sql("select t.*,((attempted_hot/base_hot)*100) as perc_attempted_hot,((attempted_warm/base_warm)*100) as perc_attempted_warm,((attempted_cold/base_cold)*100) as perc_attempted_cold,((attempted_total/base_total)*100) as perc_total,((contacted_hot/base_hot)*100) as perc_con_allocated_hot,((contacted_warm/base_warm)*100) as perc_con_allocated_warm,((contacted_cold/base_cold)*100) as perc_con_allocated_cold,((contacted_total/base_total)*100) as perc_con_allocated_total,((contacted_hot/attempted_hot)*100) as perc_con_outofattm_hot,((contacted_warm/attempted_warm)*100) as perc_con_outofattm_warm,((contacted_cold/attempted_cold)*100) as perc_con_outofattm_cold,((contacted_total/attempted_total)*100) as perc_con_outofattm_total,((no_of_calls_hot/attempted_hot)) as churn_hot,((no_of_calls_warm/attempted_warm)) as churn_warm,((no_of_calls_cold/attempted_cold)) as churn_cold,((no_of_calls_total/attempted_total)) as churn_total,no_of_calls_hot/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_hot,no_of_calls_warm/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_warm,no_of_calls_cold/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_cold,no_of_calls_total/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_total,((realized_hot/base_hot)*100) as perc_conversion_allo_hot,((realized_warm/base_warm)*100) as perc_conversion_allo_warm,((realized_cold/base_cold)*100) as perc_conversion_allo_cold,((realized_total/base_total)*100) as perc_conversion_allo_total,((realized_hot/attempted_hot)*100) as perc_conversion_attem_hot,((realized_warm/attempted_warm)*100) as perc_conversion_attem_warm,((realized_cold/attempted_cold)*100) as perc_conversion_attem_cold,((realized_total/attempted_total)*100) as perc_conversion_attem_total from tests1 t")


test_dubai7=test_dubai7.withColumn('run_date',lit(current_date()))

test_dubai7.repartition(1).write.csv("s3://cmh-process-data/test-1/NHM_Contact_Metrics/dubai_contact_metrics",mode="append",header="true")

################# Normal Base script start #################

normal_base=spark.sql('select * from ttr where ControlLocation <> "Dubai"')

normal_base.createOrReplaceTempView('ttr2')

normal_base2=spark.sql("select t.*,case when total_attempted >0 then 'yes' else 'no' end as attempted_dialer,case when contacted>0 then 'yes' else 'no' end as contacted_dialer,case when PDI_Amount>0 then 1 else 0 end as realized from ttr2 t")

normal_base2.createOrReplaceTempView("mem2")

normal_base3=spark.sql("select m.Category,sum(case when m.Rating='Hot' then 1 else 0 end) as base_hot,sum(case when m.Rating='Warm' then 1 else 0 end) as base_warm,sum(case when m.Rating='Cold' then 1 else 0 end) as base_cold,count(*) as base_total,sum(case when m.Rating='Hot' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_hot,sum(case when m.Rating='Warm' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_warm,sum(case when m.Rating='Cold' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_cold,sum(case when m.attempted_dialer='yes' then 1 else 0 end) as attempted_total,sum(case when m.Rating='Hot' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_hot,sum(case when m.Rating='Warm' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_warm,sum(case when m.Rating='Cold' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_cold,sum(case when m.contacted_dialer='yes' then 1 else 0 end) as contacted_total,sum(case when m.Rating='Hot' and m.realized=1 then 1 else 0 end) as realized_hot,sum(case when m.Rating='Warm' and m.realized=1 then 1 else 0 end) as realized_warm,sum(case when m.Rating='Cold' and m.realized=1 then 1 else 0 end) as realized_cold,sum(case when m.realized=1 then 1 else 0 end) as realized_total,sum(case when m.Rating='Hot' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_hot,sum(case when m.Rating='Warm' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_warm,sum(case when m.Rating='Cold' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_cold,sum(case when m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_total,sum(case when m.Rating='Hot' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_hot,sum(case when m.Rating='Warm' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_warm,sum(case when m.Rating='Cold' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_cold,sum(case when m.realized=1 then PDI_Amount else 0 end) as Amt_realized_total from mem2 m where m.Category is not null group by m.Category")

normal_base3.createOrReplaceTempView("tests2")

normal_base4=spark.sql("select t.*,((attempted_hot/base_hot)*100) as perc_attempted_hot,((attempted_warm/base_warm)*100) as perc_attempted_warm,((attempted_cold/base_cold)*100) as perc_attempted_cold,((attempted_total/base_total)*100) as perc_total,((contacted_hot/base_hot)*100) as perc_con_allocated_hot,((contacted_warm/base_warm)*100) as perc_con_allocated_warm,((contacted_cold/base_cold)*100) as perc_con_allocated_cold,((contacted_total/base_total)*100) as perc_con_allocated_total,((contacted_hot/attempted_hot)*100) as perc_con_outofattm_hot,((contacted_warm/attempted_warm)*100) as perc_con_outofattm_warm,((contacted_cold/attempted_cold)*100) as perc_con_outofattm_cold,((contacted_total/attempted_total)*100) as perc_con_outofattm_total,((no_of_calls_hot/attempted_hot)) as churn_hot,((no_of_calls_warm/attempted_warm)) as churn_warm,((no_of_calls_cold/attempted_cold)) as churn_cold,((no_of_calls_total/attempted_total)) as churn_total,no_of_calls_hot/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_hot,no_of_calls_warm/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_warm,no_of_calls_cold/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_cold,no_of_calls_total/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_total,((realized_hot/base_hot)*100) as perc_conversion_allo_hot,((realized_warm/base_warm)*100) as perc_conversion_allo_warm,((realized_cold/base_cold)*100) as perc_conversion_allo_cold,((realized_total/base_total)*100) as perc_conversion_allo_total,((realized_hot/attempted_hot)*100) as perc_conversion_attem_hot,((realized_warm/attempted_warm)*100) as perc_conversion_attem_warm,((realized_cold/attempted_cold)*100) as perc_conversion_attem_cold,((realized_total/attempted_total)*100) as perc_conversion_attem_total from tests2 t")

normal_base4=normal_base4.withColumn('run_date',lit(current_date()))

normal_base4.repartition(1).write.csv("s3://cmh-process-data/test-1/NHM_Contact_Metrics/Normal_base_contact_metrics",mode="append",header="true")


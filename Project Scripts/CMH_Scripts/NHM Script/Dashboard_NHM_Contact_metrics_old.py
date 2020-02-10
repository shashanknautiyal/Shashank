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
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import DateType
import datetime,time



customer=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_customer_contact/customer_contact.csv",header =True,inferSchema=True)

customer= customer.withColumn('aContactID',customer['aContactID'].cast('Integer'))
customer=customer.withColumn('dLastUpdated',to_date('dLastUpdated'))
customer.createOrReplaceTempView("cc")

contract=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_Contract_data/Contract_data.csv",header =True,inferSchema=True)

contract = contract.withColumn('aContractID',contract['aContractID'].cast('Integer')).withColumn('nMember',contract['nMember'].cast('Integer'))
contract=contract.withColumn('dtLastUpdated',to_date('dtLastUpdated'))
contract.createOrReplaceTempView("cd")


allocation=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_allocation/allocation.csv",header =True,inferSchema=True)

#allocation=allocation.withColumnRenamed('Oct Allocation','Allocation')
allocation=allocation.withColumn('date',to_date('date','yyyy-MM'))

allocation_filter=allocation.filter('date >= date_add(last_day(now() - interval 01 month),1) and date<= last_day(now())')
allocation_filter.createOrReplaceTempView("al")



nhm_Member_data=spark.sql("select distinct al.Contract_ID, al.Control_Location as ControlLocation, cd.dtSalesPosted, right(cc.tMobile, 10) as Mobile_No,right(cc.tResPhone1,10) as tResPhone1, right(cc.tResPhone2,10) as tResPhone2, al.category, month(cd.dtSalesPosted) as MonthofCharge,al.Allocation as Team_Allocation, al.HWC_bucket as Rating,al.Due as Due from al left join cd on al.Contract_ID = cd.aContractID left join cc on cd.nMember = cc.aContactID where al.Allocation = 'NHM'")
nhm_Member_data.createOrReplaceTempView("md")


zfi=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_PDI/ZFI_PDI.csv",header =True,inferSchema=True)

zfi=zfi.withColumn('PDI_REALIZATION_DATE',to_date('PDI_REALIZATION_DATE','yyyy-MM-dd')).withColumn('dw_last_modified_date',to_date('dw_last_modified_date')).withColumn('PDI_STATUS',zfi.PDI_STATUS.cast('Integer'))

zfi.createOrReplaceTempView("cpd")
filter_df=spark.sql("select reference_document,PDI_DATE,PDI_TYPE,PDI_STATUS,PDI_AMOUNT,PDI_REALIZATION_DATE, Contract_Flag from cpd")

filter_df=filter_df.withColumnRenamed('reference_document','Contract_ID')
filter_df.createOrReplaceTempView("filter")

filter_df_2=spark.sql("select distinct Contract_ID, sum(PDI_AMOUNT) as PDI_Amount from filter where (PDI_REALIZATION_DATE>=date_add(last_day(now() - interval 01 month),1) and PDI_REALIZATION_DATE<= last_day(now())) and PDI_STATUS = 9 and Contract_Flag ='A'and PDI_TYPE !='ADJ' group by Contract_ID")
filter_df_2.createOrReplaceTempView("fl")


Final_Member_data=spark.sql("select md.Contract_ID,md.ControlLocation,md.dtSalesPosted,md.Mobile_No,md.tResPhone1,md.tResPhone2,md.category,md.MonthofCharge,md.Team_Allocation,md.Rating,md.Due,fl.PDI_Amount from md left join fl on md.Contract_ID=fl.Contract_ID")

Final_Member_data.createOrReplaceTempView('finalmem')

Final_Member_data=spark.sql('select finalmem.*, case when category is null then "unknown_category" else category end as category_1 from finalmem')

Final_Member_data=Final_Member_data.drop('category')
Final_Member_data=Final_Member_data.withColumnRenamed('category_1','category')

Final_Member_data.createOrReplaceTempView('fm')

pd=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_nhm_dialler/nhm_dialler.csv",header =True,inferSchema=True)

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
t4=t4.withColumn('Rating',lower(t4.Rating))

t4.createOrReplaceTempView("ttr")

test5=spark.sql("select t.*,case when total_attempted >0 then 'yes' else 'no' end as attempted_dialer,case when contacted>0 then 'yes' else 'no' end as contacted_dialer,case when PDI_Amount>0 then 1 else 0 end as realized from ttr t")

#test5=test5.withColumn('Rating',lower(test5.Rating))

test5.createOrReplaceTempView("mem")

#test5=test5.withColumn('run_date',lit(current_date()))

test6=spark.sql("select m.Category,sum(case when m.Rating='hot' then 1 else 0 end) as base_hot,sum(case when m.Rating='warm' then 1 else 0 end) as base_warm,sum(case when m.Rating='cold' then 1 else 0 end) as base_cold,sum(case when m.Rating is null then 1 else 0 end) as base_unknown,count(*) as base_total,sum(case when m.Rating='hot' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_hot,sum(case when m.Rating='warm' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_warm,sum(case when m.Rating='cold' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_cold,sum(case when m.Rating is null and m.attempted_dialer='yes' then 1 else 0 end) as attempted_unknown,sum(case when m.attempted_dialer='yes' then 1 else 0 end) as attempted_total,sum(case when m.Rating='hot' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_hot,sum(case when m.Rating='warm' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_warm,sum(case when m.Rating='cold' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_cold,sum(case when m.Rating is null and m.contacted_dialer='yes' then 1 else 0 end) as contacted_unknown,sum(case when m.contacted_dialer='yes' then 1 else 0 end) as contacted_total,sum(case when m.Rating='hot' and m.realized=1 then 1 else 0 end) as realized_hot,sum(case when m.Rating='warm' and m.realized=1 then 1 else 0 end) as realized_warm,sum(case when m.Rating='cold' and m.realized=1 then 1 else 0 end) as realized_cold,sum(case when m.Rating is null and m.realized=1 then 1 else 0 end) as realized_unknown,sum(case when m.realized=1 then 1 else 0 end) as realized_total,sum(case when m.Rating='hot' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_hot,sum(case when m.Rating='warm' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_warm,sum(case when m.Rating='cold' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_cold,sum(case when m.Rating is null and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_unknown,sum(case when m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_total,sum(case when m.Rating='hot' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_hot,sum(case when m.Rating='warm' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_warm,sum(case when m.Rating='cold' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_cold,sum(case when m.Rating is null and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_unknown,sum(case when m.realized=1 then PDI_Amount else 0 end) as Amt_realized_total from mem m where m.Category is not null group by m.Category")

test6.createOrReplaceTempView("tests")

test7=spark.sql("select t.*,((attempted_hot/base_hot)*100) as perc_attempted_hot,((attempted_warm/base_warm)*100) as perc_attempted_warm,((attempted_cold/base_cold)*100) as perc_attempted_cold,((attempted_total/base_total)*100) as perc_total,((contacted_hot/base_hot)*100) as perc_con_allocated_hot,((contacted_warm/base_warm)*100) as perc_con_allocated_warm,((contacted_cold/base_cold)*100) as perc_con_allocated_cold,((contacted_total/base_total)*100) as perc_con_allocated_total,((contacted_hot/attempted_hot)*100) as perc_con_outofattm_hot,((contacted_warm/attempted_warm)*100) as perc_con_outofattm_warm,((contacted_cold/attempted_cold)*100) as perc_con_outofattm_cold,((contacted_total/attempted_total)*100) as perc_con_outofattm_total,((no_of_calls_hot/attempted_hot)) as churn_hot,((no_of_calls_warm/attempted_warm)) as churn_warm,((no_of_calls_cold/attempted_cold)) as churn_cold,((no_of_calls_unknown/attempted_total)) as churn_unknown,((no_of_calls_total/attempted_total)) as churn_total,no_of_calls_hot/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_hot,no_of_calls_warm/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_warm,no_of_calls_cold/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_cold,no_of_calls_total/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_total,((realized_hot/base_hot)*100) as perc_conversion_allo_hot,((realized_warm/base_warm)*100) as perc_conversion_allo_warm,((realized_cold/base_cold)*100) as perc_conversion_allo_cold,((realized_total/base_total)*100) as perc_conversion_allo_total,((realized_hot/attempted_hot)*100) as perc_conversion_attem_hot,((realized_warm/attempted_warm)*100) as perc_conversion_attem_warm,((realized_cold/attempted_cold)*100) as perc_conversion_attem_cold,((realized_total/attempted_total)*100) as perc_conversion_attem_total from tests t")

test7=test7.withColumn('run_date',lit(current_date()))

test7=test7.withColumn('run_date',test7.run_date.cast('string'))

test7.createOrReplaceTempView('t7')

test7_old=spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/NHM_contact_metrics/NHM_Contact_Metrics.csv",header =True,inferSchema=True)

test7_old=test7_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

test7_old=test7_old.withColumn('run_date',test7_old.run_date.cast('string'))
test7_old.createOrReplaceTempView('t7o')

test7_final=spark.sql('select * from t7 union all select * from t7o')


test7_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/NHM_contact_metrics",mode="append",header="true")


test_dubai=spark.sql('select * from ttr where ControlLocation = "Dubai"')
test_dubai.createOrReplaceTempView('ttr1')

test_dubai5=spark.sql("select t.*,case when total_attempted >0 then 'yes' else 'no' end as attempted_dialer,case when contacted>0 then 'yes' else 'no' end as contacted_dialer,case when PDI_Amount>0 then 1 else 0 end as realized from ttr1 t")
test_dubai5.createOrReplaceTempView("mem1")

test_dubai6 =spark.sql("select m.Category,sum(case when m.Rating='hot' then 1 else 0 end) as base_hot,sum(case when m.Rating='warm' then 1 else 0 end) as base_warm,sum(case when m.Rating='cold' then 1 else 0 end) as base_cold,sum(case when m.Rating is null then 1 else 0 end) as base_unknown,count(*) as base_total,sum(case when m.Rating='hot' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_hot,sum(case when m.Rating='warm' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_warm,sum(case when m.Rating='cold' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_cold,sum(case when m.Rating is null and m.attempted_dialer='yes' then 1 else 0 end) as attempted_unknown,sum(case when m.attempted_dialer='yes' then 1 else 0 end) as attempted_total,sum(case when m.Rating='hot' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_hot,sum(case when m.Rating='warm' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_warm,sum(case when m.Rating='cold' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_cold,sum(case when m.Rating is null and m.contacted_dialer='yes' then 1 else 0 end) as contacted_unknown,sum(case when m.contacted_dialer='yes' then 1 else 0 end) as contacted_total,sum(case when m.Rating='hot' and m.realized=1 then 1 else 0 end) as realized_hot,sum(case when m.Rating='warm' and m.realized=1 then 1 else 0 end) as realized_warm,sum(case when m.Rating='cold' and m.realized=1 then 1 else 0 end) as realized_cold,sum(case when m.Rating is null and m.realized=1 then 1 else 0 end) as realized_unknown,sum(case when m.realized=1 then 1 else 0 end) as realized_total,sum(case when m.Rating='hot' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_hot,sum(case when m.Rating='warm' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_warm,sum(case when m.Rating='cold' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_cold,sum(case when m.Rating is null and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_unknown,sum(case when m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_total,sum(case when m.Rating='hot' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_hot,sum(case when m.Rating='warm' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_warm,sum(case when m.Rating='cold' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_cold,sum(case when m.Rating is null and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_unknown,sum(case when m.realized=1 then PDI_Amount else 0 end) as Amt_realized_total from mem1 m where m.Category is not null group by m.Category")
test_dubai6.createOrReplaceTempView("tests1")

test_dubai7=spark.sql("select t.*,((attempted_hot/base_hot)*100) as perc_attempted_hot,((attempted_warm/base_warm)*100) as perc_attempted_warm,((attempted_cold/base_cold)*100) as perc_attempted_cold,((attempted_total/base_total)*100) as perc_total,((contacted_hot/base_hot)*100) as perc_con_allocated_hot,((contacted_warm/base_warm)*100) as perc_con_allocated_warm,((contacted_cold/base_cold)*100) as perc_con_allocated_cold,((contacted_total/base_total)*100) as perc_con_allocated_total,((contacted_hot/attempted_hot)*100) as perc_con_outofattm_hot,((contacted_warm/attempted_warm)*100) as perc_con_outofattm_warm,((contacted_cold/attempted_cold)*100) as perc_con_outofattm_cold,((contacted_total/attempted_total)*100) as perc_con_outofattm_total,((no_of_calls_hot/attempted_hot)) as churn_hot,((no_of_calls_warm/attempted_warm)) as churn_warm,((no_of_calls_cold/attempted_cold)) as churn_cold,((no_of_calls_unknown/attempted_total)) as churn_unknown,((no_of_calls_total/attempted_total)) as churn_total,no_of_calls_hot/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_hot,no_of_calls_warm/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_warm,no_of_calls_cold/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_cold,no_of_calls_total/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_total,((realized_hot/base_hot)*100) as perc_conversion_allo_hot,((realized_warm/base_warm)*100) as perc_conversion_allo_warm,((realized_cold/base_cold)*100) as perc_conversion_allo_cold,((realized_total/base_total)*100) as perc_conversion_allo_total,((realized_hot/attempted_hot)*100) as perc_conversion_attem_hot,((realized_warm/attempted_warm)*100) as perc_conversion_attem_warm,((realized_cold/attempted_cold)*100) as perc_conversion_attem_cold,((realized_total/attempted_total)*100) as perc_conversion_attem_total from tests1 t")


test_dubai7=test_dubai7.withColumn('run_date',lit(current_date()))

test_dubai7=test_dubai7.withColumn('run_date',test_dubai7.run_date.cast('string'))

test_dubai7.createOrReplaceTempView('td7')

test_dubai7_old=spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/dubai_contact_metrics/NHM_dubai_base.csv",header =True,inferSchema=True)

test_dubai7_old=test_dubai7_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

test_dubai7_old=test_dubai7_old.withColumn('run_date',test_dubai7_old.run_date.cast('string'))


test_dubai7_old.createOrReplaceTempView('td7o')

test_dubai7_final=spark.sql('select * from td7 union all select * from td7o')

test_dubai7_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/dubai_contact_metrics",mode="append",header="true")

################# Normal Base script start #################

normal_base=spark.sql('select * from ttr where ControlLocation <> "Dubai"')

normal_base.createOrReplaceTempView('ttr2')

normal_base2=spark.sql("select t.*,case when total_attempted >0 then 'yes' else 'no' end as attempted_dialer,case when contacted>0 then 'yes' else 'no' end as contacted_dialer,case when PDI_Amount>0 then 1 else 0 end as realized from ttr2 t")

normal_base2.createOrReplaceTempView("mem2")

normal_base3=spark.sql("select m.Category,sum(case when m.Rating='hot' then 1 else 0 end) as base_hot,sum(case when m.Rating='warm' then 1 else 0 end) as base_warm,sum(case when m.Rating='cold' then 1 else 0 end) as base_cold,sum(case when m.Rating is null then 1 else 0 end) as base_unknown,count(*) as base_total,sum(case when m.Rating='hot' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_hot,sum(case when m.Rating='warm' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_warm,sum(case when m.Rating='cold' and m.attempted_dialer='yes' then 1 else 0 end) as attempted_cold,sum(case when m.Rating is null and m.attempted_dialer='yes' then 1 else 0 end) as attempted_unknown,sum(case when m.attempted_dialer='yes' then 1 else 0 end) as attempted_total,sum(case when m.Rating='hot' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_hot,sum(case when m.Rating='warm' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_warm,sum(case when m.Rating='cold' and m.contacted_dialer='yes' then 1 else 0 end) as contacted_cold,sum(case when m.Rating is null and m.contacted_dialer='yes' then 1 else 0 end) as contacted_unknown,sum(case when m.contacted_dialer='yes' then 1 else 0 end) as contacted_total,sum(case when m.Rating='hot' and m.realized=1 then 1 else 0 end) as realized_hot,sum(case when m.Rating='warm' and m.realized=1 then 1 else 0 end) as realized_warm,sum(case when m.Rating='cold' and m.realized=1 then 1 else 0 end) as realized_cold,sum(case when m.Rating is null and m.realized=1 then 1 else 0 end) as realized_unknown,sum(case when m.realized=1 then 1 else 0 end) as realized_total,sum(case when m.Rating='hot' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_hot,sum(case when m.Rating='warm' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_warm,sum(case when m.Rating='cold' and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_cold,sum(case when m.Rating is null and m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_unknown,sum(case when m.attempted_dialer='yes' then total_attempted else 0 end) as no_of_calls_total,sum(case when m.Rating='hot' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_hot,sum(case when m.Rating='warm' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_warm,sum(case when m.Rating='cold' and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_cold,sum(case when m.Rating is null and m.realized=1 then PDI_Amount else 0 end) as Amt_realized_unknown,sum(case when m.realized=1 then PDI_Amount else 0 end) as Amt_realized_total from mem2 m where m.Category is not null group by m.Category")

normal_base3.createOrReplaceTempView("tests2")

normal_base4=spark.sql("select t.*,((attempted_hot/base_hot)*100) as perc_attempted_hot,((attempted_warm/base_warm)*100) as perc_attempted_warm,((attempted_cold/base_cold)*100) as perc_attempted_cold,((attempted_total/base_total)*100) as perc_total,((contacted_hot/base_hot)*100) as perc_con_allocated_hot,((contacted_warm/base_warm)*100) as perc_con_allocated_warm,((contacted_cold/base_cold)*100) as perc_con_allocated_cold,((contacted_total/base_total)*100) as perc_con_allocated_total,((contacted_hot/attempted_hot)*100) as perc_con_outofattm_hot,((contacted_warm/attempted_warm)*100) as perc_con_outofattm_warm,((contacted_cold/attempted_cold)*100) as perc_con_outofattm_cold,((contacted_total/attempted_total)*100) as perc_con_outofattm_total,((no_of_calls_hot/attempted_hot)) as churn_hot,((no_of_calls_warm/attempted_warm)) as churn_warm,((no_of_calls_cold/attempted_cold)) as churn_cold,((no_of_calls_unknown/attempted_total)) as churn_unknown,((no_of_calls_total/attempted_total)) as churn_total,no_of_calls_hot/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_hot,no_of_calls_warm/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_warm,no_of_calls_cold/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_cold,no_of_calls_total/(select sum(no_of_calls_total) from tests)*100 as perc_total_call_total,((realized_hot/base_hot)*100) as perc_conversion_allo_hot,((realized_warm/base_warm)*100) as perc_conversion_allo_warm,((realized_cold/base_cold)*100) as perc_conversion_allo_cold,((realized_total/base_total)*100) as perc_conversion_allo_total,((realized_hot/attempted_hot)*100) as perc_conversion_attem_hot,((realized_warm/attempted_warm)*100) as perc_conversion_attem_warm,((realized_cold/attempted_cold)*100) as perc_conversion_attem_cold,((realized_total/attempted_total)*100) as perc_conversion_attem_total from tests2 t")

normal_base4=normal_base4.withColumn('run_date',lit(current_date()))

normal_base4=normal_base4.withColumn('run_date',normal_base4.run_date.cast('string'))

normal_base4.createOrReplaceTempView('nb4')


normal_base4_old=spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/Normal_base_contact_metrics/NHM_normal_base.csv",header =True,inferSchema=True)

normal_base4_old=normal_base4_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))
normal_base4_old=normal_base4_old.withColumn('run_date',normal_base4_old.run_date.cast('string'))


normal_base4_old.createOrReplaceTempView('nb4o')

normal_base4_final=spark.sql('select * from nb4 union all select * from nb4o')

normal_base4_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/Normal_base_contact_metrics",mode="append",header="true")

table_1 = spark.sql('select "customer_contact_table" as Table_name, max(dLastUpdated) as last_modified, current_date() as run_date from cc')

table_2 = spark.sql('select "contract_table" as Table_name, max(dtLastUpdated) as last_modified, current_date() as run_date from cd')

table_A = table_1.unionAll(table_2)

table_3 = spark.sql('select "Allocation_table" as Table_name, max(date) as last_modified, current_date() as run_date from al')

table_B = table_A.unionAll(table_3)

table_4 = spark.sql('select "ZFI_PDI_table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from cpd')

table_C = table_B.unionAll(table_4)

table_5 = spark.sql('select "nhm_dialler_table" as Table_name, max(Date) as last_modified, current_date() as run_date from new')

table_D = table_C.unionAll(table_5)

table_D=table_D.withColumn('last_modified',table_D.last_modified.cast('string')).withColumn('run_date',table_D.run_date.cast('string'))


table_D.createOrReplaceTempView('tf')


nhm_table_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/Table_details/table_details.csv",header =True,inferSchema=True)

nhm_table_old=nhm_table_old.withColumn('run_date',to_date('run_date','yyyy-MM-dd')).withColumn('last_modified',to_date('last_modified','yyyy-MM-dd'))

nhm_table_old=nhm_table_old.withColumn('last_modified',nhm_table_old.last_modified.cast('string')).withColumn('run_date',nhm_table_old.run_date.cast('string'))

nhm_table_old.createOrReplaceTempView('nto')

nhm_table_final= spark.sql('select * from nto union all select * from tf')

nhm_table_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/Table_details",mode="append",header="true")


time.sleep(10)
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

time.sleep(10)
cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/NHM_contact_metrics/NHM_Contact_Metrics.csv"
os.system(cmd1)
time.sleep(10)
cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/dubai_contact_metrics/NHM_dubai_base.csv"
os.system(cmd2)
time.sleep(10)
cmd3="sudo aws s3 rm s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/Normal_base_contact_metrics/NHM_normal_base.csv"
os.system(cmd3)
time.sleep(10)
cmd4="sudo aws s3 rm s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/Table_details/table_details.csv"
os.system(cmd4)




fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
time.sleep(10)
file_path_nhm_contact = "s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/NHM_contact_metrics/"
time.sleep(10)
file_path_nhm_dubai= "s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/dubai_contact_metrics/"
time.sleep(10)
file_path_nhm_normal_base = "s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/Normal_base_contact_metrics/"
time.sleep(10)
file_path_Table_details = "s3://cmh-process-data/intermediate_files/NHM_Contact_Metrics/Table_details/"
time.sleep(10)

created_file_path_nhm_contact = fs.globStatus(Path(file_path_nhm_contact + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_nhm_dubai = fs.globStatus(Path(file_path_nhm_dubai + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_nhm_normal_base = fs.globStatus(Path(file_path_nhm_normal_base + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)


fs.rename(created_file_path_nhm_contact,Path(file_path_nhm_contact + "NHM_Contact_Metrics.csv"))
time.sleep(10)
fs.rename(created_file_path_nhm_dubai,Path(file_path_nhm_dubai + "NHM_dubai_base.csv"))
time.sleep(10)
fs.rename(created_file_path_nhm_normal_base,Path(file_path_nhm_normal_base + "NHM_normal_base.csv"))
time.sleep(10)
fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))



from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window

inflow=spark.read.option("delimiter",",").csv("s3://cmh-process-data/test-1/Base_inflow_july/final_inflow.csv",header=True,inferSchema=True)

inflow=inflow.withColumnRenamed('Contract No.','Contract_No').withColumnRenamed('New Branch','New_Branch')
inflow.createOrReplaceTempView('if')

inflow=spark.sql("select distinct Contract_No,New_Branch from if")
inflow.createOrReplaceTempView('if')


# take monthy file for now i have taken daily zfi_65 file.
zfi_65_1_feb=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_ZFI_065_daily/ZFI_065_Daily.csv",header=True,inferSchema=True)


zfi_65_1_feb=zfi_65_1_feb.select('ContractNo','EMIRealizedCount','SaleDate')

zfi_65_1_feb.createOrReplaceTempView('zfi1')

inflow_1=spark.sql('select if.Contract_No,if.New_Branch, zfi1.EMIRealizedCount,zfi1.SaleDate from if left join zfi1 on if.Contract_No=zfi1.ContractNo')

inflow_1.createOrReplaceTempView('if1')

inflow_1=spark.sql('select Contract_No,min(EMIRealizedCount) as EMI_Realized_count,SaleDate from if1 group by Contract_No,SaleDate')
inflow_1.createOrReplaceTempView('if1')

## change inflow will be calculated using inflow sheet on S3##

# S3 path

cancel_req_1= spark.read.option("delimiter",",").csv(r"s3://cmh-process-data/master-tables/master_table_cancellation_request/cancellation_request.csv",header=True,inferSchema=True)

cancel_req_1=cancel_req_1.select('contractID','cancellation_trigger','StatusOfRequest')

cancel_req_1.createOrReplaceTempView('cr1')

cancel_req=spark.sql('select if.*, cr1.cancellation_trigger,cr1.StatusOfRequest from if inner join cr1 on if.Contract_No=cr1.contractID')

cancel_req=cancel_req.withColumn('Cancel',when(cancel_req.cancellation_trigger =='Rescission Cancellation Request','Rescission_Cancellation_Request').otherwise('General_Cancellation')).withColumn('status_request',when(cancel_req.StatusOfRequest=='Resolved','Resolved').otherwise('Pending'))

cancel_req=cancel_req.withColumn('trigger_value',when(cancel_req.Cancel =='Rescission_Cancellation_Request',10).otherwise(1)).withColumn('status_value',when(cancel_req.status_request=='Resolved',1).otherwise(2))

cancel_req=cancel_req.withColumn('total_value',(cancel_req.trigger_value*cancel_req.status_value))

cancel_req=cancel_req.drop('cancellation_trigger').drop('StatusOfRequest')

cancel_req.createOrReplaceTempView('cr')

cancel_req_priority=spark.sql('select s.Contract_No,s.Cancel,s.status_request,s.total_value from (select Contract_No,Cancel,status_request,total_value,row_number() over(partition by Contract_No order by total_value desc) as rownum1 from cr) s where rownum1 =1')

cancel_req_priority.createOrReplaceTempView('crp')

inflow_can=spark.sql("select if1.*,crp.Cancel,crp.status_request,crp.total_value from if1 left join crp on if1.Contract_No=crp.Contract_No")

inflow_can.createOrReplaceTempView('ic')

inflow_can=inflow_can.na.fill("Active_with_no_cancellation",subset=['Cancel'])

# Output-1
inflow_can.repartition(1).write.csv("s3://cmh-process-data/test-1/EMI_Inflow/inflow_can",mode="append",header="true")

### EMI RZ COUNT ##

active_with_nc= inflow_can.filter("Cancel=='Active_with_no_cancellation'")

active_with_nc=active_with_nc.withColumn('emi_rz_cat', when(active_with_nc.EMI_Realized_count== 0,'emi_rz_0').when((active_with_nc.EMI_Realized_count>0) & (active_with_nc.EMI_Realized_count<=5),'emi_rz_1_5').otherwise('emi_rz_>5'))
active_with_nc.createOrReplaceTempView('awn')

active_with_emi_0= active_with_nc.filter("emi_rz_cat=='emi_rz_0'")
active_with_emi_0.createOrReplaceTempView('awe')

# change Dynamic i.e for july dashboard june month is last month and script will run in the month of august.

active_with_emi0_rz=spark.sql('select Contract_No,case when SaleDate >= date_add(last_day(now() - interval 03 months),1) and  SaleDate <= last_day(now() - Interval 1 months) then "FIF" else "upgrade/ppc" end as Flag from awe')
active_with_emi0_rz.createOrReplaceTempView('aw0r')


active_with_emi_1_5_rz = active_with_nc.filter("emi_rz_cat=='emi_rz_1_5'")
active_with_emi_1_5_rz=active_with_emi_1_5_rz.select('Contract_No')
active_with_emi_1_5_rz.createOrReplaceTempView('awe1_5')


recovery_file_1=spark.read.option("delimiter",",").csv(r"s3://cmh-process-data/master-tables/master_table_Recovery/recovery.csv",header=True,inferSchema=True)

recovery_file_1.createOrReplaceTempView('rf')

# change Dynamic for july dashboard  last 09 month from june month is last month and script will month in the month of august. 1 oct 18 to 30 jun 19 
# check once

recovery_file_09=spark.sql('select *, date_add(last_day(now() - interval 10 months),1) as offset from rf')

recovery_file_last_9_mo = recovery_file_09.filter("rec_date>=offset")

recovery_file_last_9_mo=recovery_file_last_9_mo.dropDuplicates(['Contract_No'])

recovery_file_last_9_mo.createOrReplaceTempView('rfl9m')

recovery_bucket_1_5=spark.sql('select rfl9m.*, case when rec_date>= date_add(last_day(now() - interval 04 months),1) and rec_date<=last_day(now() - interval 01 months) then "normalised_in_last_3_months" when rec_date>= date_add(last_day(now()- interval 07 months),1) and rec_date<=last_day(now() - interval 04 months) then "normalised_4_6_months_ago" else "normalised_7_9_months_ago" end as flag_normalised from rfl9m')
recovery_bucket_1_5.createOrReplaceTempView('nbo')


active_with_emi_1_5_rz=spark.sql("select awe1_5.*,nbo.flag_normalised from awe1_5 left join nbo on awe1_5.Contract_No=nbo.Contract_No")

active_with_emi_1_5_rz=active_with_emi_1_5_rz.na.fill('First_time_inflow',subset=['flag_normalised'])
active_with_emi_1_5_rz.createOrReplaceTempView('a15r')


###### active Contracts with >5 emi's realised #######

active_with_morethan_5_emi_rz= active_with_nc.filter("emi_rz_cat=='emi_rz_>5'")

active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.select('Contract_No')

active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5')


# change Dynamic

# change check in recovery_file_last_9_mo if yes normalised but back in od and first time inflow

active_with_morethan_5_emi_rz=spark.sql("select awm5.*,nbo.flag_normalised from awm5 left join nbo on awm5.Contract_No=nbo.Contract_No")

active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.na.fill("First_time_inflow",subset=['flag_normalised'])

active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5r')

###### PDI Not Available #####

zsd=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_zsd_007n/ZSD_007N.csv",header=True,inferSchema=True)

zsd=zsd.withColumn('INSTALLMENT_DATE',to_date('INSTALLMENT_DATE','MM/dd/yyyy')).withColumn('dw_last_modified_date',zsd.dw_last_modified_date.cast('date')).withColumn('CONTRACT_NUMBER',zsd.CONTRACT_NUMBER.cast('Integer'))

zsd.createOrReplaceTempView('zsd')


# for 1st march dashboard
# change dynamic

zsd_filter= spark.sql('select * from zsd where INSTALLMENT_DATE >= "2019-08-01" and INSTALLMENT_DATE<="2019-08-31" and PDI_STATUS="01"')

zsd_filter=zsd_filter.dropDuplicates(['CONTRACT_NUMBER'])
zsd_filter.createOrReplaceTempView('zf')
zsd_filter.repartition(1).write.csv("s3://cmh-process-data/test-1/ZSD",mode="append",header="true")


# read 1t march or 31st july zfi_65 for March dashboard

zfi_65_1_feb=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_ZFI_065_daily/ZFI_065_Daily.csv",header=True,inferSchema=True)


for col in zfi_65_1_feb.columns:
	zfi_65_1_feb=zfi_65_1_feb.withColumnRenamed(col,col.lower())
		
for col in zfi_65_1_feb.columns:
	zfi_65_1_feb=zfi_65_1_feb.withColumnRenamed(col,col.replace(" ", "_"))

zfi_65_1_feb.createOrReplaceTempView('zfeb')

zfi_65_summ=spark.sql("Select contractno from zfeb where status='Active' and odbucketmax in ('43-45','00-03','04-06','07-09','28-30','25-27','10-12','22-24','13-15','34-36','16-18','19-21','40-42','46-48','37-39','31-33','49-MO') and mortgagecheck=1")
zfi_65_summ=zfi_65_summ.withColumn('flag',lit(1))
zfi_65_summ.createOrReplaceTempView('z65')


zfi_zsd=spark.sql('Select zf.CONTRACT_NUMBER,z65.flag from zf left join z65 on zf.CONTRACT_NUMBER=z65.contractno')

zsd_excluded=zfi_zsd.filter('flag is null')
zsd_excluded.createOrReplaceTempView('ze')
zsd_excluded=spark.sql('select *, case when flag is null then "null_pdi" end as upcoming_pdi_type from ze')
zsd_excluded.createOrReplaceTempView('pnb')

zsd_excluded.repartition(1).write.csv("s3://cmh-process-data/master-tables/master_table_pdi_none",mode="append",header="true")


###### FIF PDI Not Available Contracts ########

active_with_emi0_rz =spark.sql('select aw0r.*, pnb.upcoming_pdi_type from aw0r left join pnb on aw0r.Contract_No=pnb.CONTRACT_NUMBER')

active_with_emi0_rz=active_with_emi0_rz.withColumn('pdi_contracts_status',when(active_with_emi0_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

# output-2

active_with_emi0_rz.repartition(1).write.csv("s3://cmh-process-data/test-1/EMI_Inflow/Active0",mode="append",header="true")

#### 1-5 EMI RZ with PDI Not Available Contracts #######

active_with_emi_1_5_rz =spark.sql('select a15r.*, pnb.upcoming_pdi_type from a15r left join pnb on a15r.Contract_No=pnb.CONTRACT_NUMBER')

active_with_emi_1_5_rz=active_with_emi_1_5_rz.withColumn('pdi_contracts_status',when(active_with_emi_1_5_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

## output-3

active_with_emi_1_5_rz.repartition(1).write.csv("s3://cmh-process-data/test-1/EMI_Inflow/active_1_5",mode="append",header="true")

#### More than 5 EMI RZ with PDI Not Available Contracts #######

active_with_morethan_5_emi_rz=spark.sql('select awm5r.*,pnb.upcoming_pdi_type from awm5r left join pnb on awm5r.Contract_No=pnb.CONTRACT_NUMBER')

active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.withColumn('pdi_contracts_status',when(active_with_morethan_5_emi_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

## output-4
active_with_morethan_5_emi_rz.repartition(1).write.csv("s3://cmh-process-data/test-1/EMI_Inflow/active_more_5",mode="append",header="true")	


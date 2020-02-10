from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window

#### opening base Box 1 ####

#inflow=spark.read.option("delimiter","|").csv(r"D:\Shashank\Club Mahindra\S3 data\inflow_sheet_emi\inflow Feb 19.csv",header=True,inferSchema=True)

# S3 path

inflow=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/inflow-sheet-emi/inflow Feb 19.csv",header=True,inferSchema=True)

inflow=inflow.withColumnRenamed('Contract No.','Contract_No').withColumnRenamed('New Branch','New_Branch')
inflow.createOrReplaceTempView('if')

inflow=spark.sql("select distinct Contract_No,New_Branch from if")
inflow.createOrReplaceTempView('if')


#zfi_65_1_feb =spark.read.option("delimiter","|").csv(r"D:\Shashank\Club Mahindra\S3 data\Zfi065\ZFI065N_OUTPUT_28.02.2019_28.02.2019_08_30_04.csv",header=True,inferSchema=True)


# S3 path

zfi_65_1_feb=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/zfi065n/ZFI065N_OUTPUT_28.02.2019_28.02.2019_08_30_04.csv",header=True,inferSchema=True)


#zfi_65_1_feb.repartition(1).write.csv("s3://cmh-process-data/test-1/zfi_65.csv",mode="append",header="true")

zfi_65_1_feb=zfi_65_1_feb.withColumnRenamed('Contract No.','Contract_No').withColumnRenamed('Mortgage ID','Mortgage_ID').withColumnRenamed('OD Bucket Max','OD_Bucket_Max').withColumnRenamed('Mortgage Check','Mortgage_Check').withColumnRenamed('EMI Realized count','EMI_Realized_count').withColumnRenamed('Sale date', 'Sale_date')


zfi_65_1_feb=zfi_65_1_feb.withColumn('Sale_date',to_date('Sale_date','dd.MM.yyyy'))

zfi_65_1_feb=zfi_65_1_feb.select('Contract_No','EMI_Realized_count','Sale_date')


#zfi_65_1_feb=zfi_65_1_feb.select('Contract_No','Mortgage_ID','Status','OD_Bucket_Max','Mortgage_Check','Name','EMI_Realized_count','Sale_date')

zfi_65_1_feb.createOrReplaceTempView('zfi1')

#zfi_65_1_feb.repartition(1).write.csv("s3://cmh-process-data/test-1/EMI_Inflow",mode="append",header="true")

inflow_1=spark.sql('select if.Contract_No,if.New_Branch, zfi1.EMI_Realized_count,zfi1.Sale_date from if left join zfi1 on if.Contract_No=zfi1.Contract_No')

#inflow_1.repartition(1).write.csv("s3://cmh-process-data/test-1/inflow_1.csv",mode="append",header="true")

#inflow_1=inflow_1.dropDuplicates(['Contract_No'])

inflow_1.createOrReplaceTempView('if1')


inflow_1=spark.sql('select Contract_No,min(EMI_Realized_count) as EMI_Realized_count,sale_date from if1 group by Contract_No,Sale_date')

inflow_1.createOrReplaceTempView('if1')

## change inflow will be calculated using inflow sheet on S3##

# S3 path

cancel_req_1= spark.read.option("delimiter","|").csv(r"s3://cmh-raw-data/complete-dump/cancellation-request/Cancellation request_11042019.csv",header=True,inferSchema=True)

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

active_with_nc=active_with_nc.withColumn('Sale_date', to_date('Sale_date','dd.MM.yyyy'))

active_with_nc=active_with_nc.withColumn('year',year('Sale_date')).withColumn('month',month('Sale_date'))

active_with_nc.createOrReplaceTempView('awn')

active_with_nc=active_with_nc.withColumn('emi_rz_cat', when(active_with_nc.EMI_Realized_count== 0,'emi_rz_0').when((active_with_nc.EMI_Realized_count>0) & (active_with_nc.EMI_Realized_count<=5),'emi_rz_1_5').otherwise('emi_rz_>5'))

active_with_nc.createOrReplaceTempView('awn')

active_with_emi_0= active_with_nc.filter("emi_rz_cat=='emi_rz_0'")

active_with_emi_0.createOrReplaceTempView('awe')

#active_with_emi_0.repartition(1).write.csv("s3://cmh-process-data/test-1/active_zero",mode="append",header="true")

# change Dynamic

active_with_emi0_rz=spark.sql('select Contract_No,case when Sale_date between date_add(last_day("2019-02-15" - interval 03 months),1) and last_day("2019-02-15" - Interval 1 months) then "FIF" else "upgrade/ppc" end as Flag from awe')

active_with_emi0_rz.createOrReplaceTempView('aw0r')


## output

#active_with_emi0_rz.repartition(1).write.csv("D:\\final_data\inflow_tree\active_with_emi_Zero_rz.csv",mode="append",header="true")

###### active contracts with 1-5 emi's realised and further processed to normalised and back in od #####

active_with_emi_1_5_rz = active_with_nc.filter("emi_rz_cat=='emi_rz_1_5'")

active_with_emi_1_5_rz=active_with_emi_1_5_rz.select('Contract_No')

active_with_emi_1_5_rz.createOrReplaceTempView('awe1_5')


### recovery and back in od ###


#recovery_jan= spark.read.option("delimiter",",").csv(r"D:\Shashank\Club Mahindra\S3 data\inflowTree\Jan Recovery.csv",header=True,inferSchema=True)

# S3 path

recovery_jan= spark.read.option("delimiter",",").csv("s3://cmh-process-data/test-1/Recovery/Jan Recovery.csv",header=True,inferSchema=True)

recovery_jan=recovery_jan.withColumn('Mon_year',lit('01-2019'))
recovery_jan=recovery_jan.withColumn('rec_date',to_date('Mon_year','MM-yyyy'))

recovery_jan=recovery_jan.withColumnRenamed('Contract_id','Contract_No')

recovery_jan.createOrReplaceTempView('rj')

#recovery_file= spark.read.option("delimiter",",").csv(r"D:\\Shashank\Club Mahindra\S3 data\inflowTree\Recoveries and Inflow 20190322.csv",header=True,inferSchema=True)

# S3 path

recovery_file= spark.read.option("delimiter",",").csv(r"s3://cmh-process-data/test-1/Recovery/Recoveries and Inflow 20190322.csv",header=True,inferSchema=True)

recovery_file=recovery_file.withColumn('Mon_year',concat(recovery_file.Month,lit('-'),recovery_file.Year))

recovery_file=recovery_file.withColumn('rec_date',to_date('Mon_year','MM-yyyy'))

recovery_file=recovery_file.withColumnRenamed('Contract No.','Contract_No')

recovery_file.createOrReplaceTempView('rf1')


recovery_file_1=spark.sql('select Contract_No,Mon_year,rec_date from rj union all select Contract_No, Mon_year, rec_date from rf1')

recovery_file_1.createOrReplaceTempView('rf')


# change Dynamic

recovery_file_09=spark.sql('select *, date_add(last_day("2019-02-14"- interval 10 months),1) as offset from rf')

recovery_file_last_9_mo = recovery_file_09.filter("rec_date>=offset")

recovery_file_last_9_mo=recovery_file_last_9_mo.dropDuplicates(['Contract_No'])

recovery_file_last_9_mo.createOrReplaceTempView('rfl9m')

#normalised_but_od= recovery_file_last_9_mo.select('Contract_No')

##normalised_but_od=normalised_but_od.withColumn('flag_normalised',lit('Normalised_but_back_in_OD'))

recovery_bucket_1_5=spark.sql('select rfl9m.*, case when rec_date>= date_add(last_day("2019-02-15"- interval 04 months),1) and rec_date<=last_day("2019-02-15"- interval 01 months) then "normalised_in_last_3_months" when rec_date>= date_add(last_day("2019-02-15"- interval 07 months),1) and rec_date<=last_day("2019-02-15"- interval 04 months) then "normalised_4_6_months_ago" else "normalised_7_9_months_ago" end as flag_normalised from rfl9m')

#recovery_bucket_1_5=spark.sql('select rfl9m.*, case when rec_date>= date_add(last_day("2019-02-15"- interval 04 months),1) and rec_date<=last_day("2019-02-15"- interval 01 months) then "last_three" when rec_date>= date_add(last_day("2019-02-15"- interval 07 months),1) and rec_date<=last_day("2019-02-15"- interval 04 months) then "four_to_six" else "seven_to_nine" end as flag from rfl9m')


#recovery_bucket_1_5=spark.sql('select rfl9m.*, case when rec_date between date_add(last_day("2019-02-15"- interval 04 months),1) and last_day("2019-02-15"- interval 01 months) then "normalised_in_last_3_months" when rec_date between date_add(last_day("2019-02-15"- interval 07 months),1) and last_day("2019-02-15"- interval 04 months) then "normalised_4_6_months_ago" else "normalised_7_9_months_ago" end as flag from rfl9m')

recovery_bucket_1_5.createOrReplaceTempView('nbo')

#recovery_bucket_1_5.repartition(1).write.csv("s3://cmh-process-data/test-1/recovery_bucket_1_5",mode="append",header="true")

active_with_emi_1_5_rz=spark.sql("select awe1_5.*,nbo.flag_normalised from awe1_5 left join nbo on awe1_5.Contract_No=nbo.Contract_No")

active_with_emi_1_5_rz=active_with_emi_1_5_rz.na.fill('First_time_inflow',subset=['flag_normalised'])

#active_with_emi_1_5_rz=active_with_emi_1_5_rz.withColumn('flag_',when(active_with_emi_1_5_rz.flag_normalised=='Normalised_but_back_in_OD','Normalised_but_back_in_OD').otherwise('First_time_inflow'))

active_with_emi_1_5_rz.createOrReplaceTempView('a15r')


###### active Contracts with >5 emi's realised #######

active_with_morethan_5_emi_rz= active_with_nc.filter("emi_rz_cat=='emi_rz_>5'")

active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.select('Contract_No')

active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5')


# change Dynamic

# change check in recovery_file_last_9_mo if yes normalised but back in od and first time inflow


#recovery_file_last_9_mo=spark.sql('select rfl9m.*, case when rec_date>= date_add(last_day("2019-02-15"- interval 02 months),1) and rec_date<=last_day("2019-02-15"- interval 01 months) then "normalised_last_month" else "normalised_more_than_1_months_ago" end as flag_normalised from rfl9m')

#recovery_file_last_9_mo.createOrReplaceTempView('rfl9')

active_with_morethan_5_emi_rz=spark.sql("select awm5.*,nbo.flag_normalised from awm5 left join nbo on awm5.Contract_No=nbo.Contract_No")

active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.na.fill("First_time_inflow",subset=['flag_normalised'])

active_with_morethan_5_emi_rz.createOrReplaceTempView('awm5r')

###### PDI Not Available #####

# S3 path 

zfi=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/zfi065n/ZFI065N_OUTPUT_28.02.2019_28.02.2019_08_30_04.csv",header =True,inferSchema=True)


for col in zfi.columns:
	zfi=zfi.withColumnRenamed(col,col.lower())
		
for col in zfi.columns:
	zfi=zfi.withColumnRenamed(col,col.replace(" ", "_"))


zfi=zfi.withColumnRenamed('contract_no.','contract_no')

zfi=zfi.withColumn("mortgage_id",regexp_replace("mortgage_id","\'",""))

zfi=zfi.withColumn('mortgage_id',zfi['mortgage_id'].cast('Integer')).withColumn('member_id',zfi['member_id'].cast('Integer')).withColumn('sale_date',to_date('sale_date','dd.MM.yyyy'))

zfi.createOrReplaceTempView('z')


# S3 Path

pdi=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/zfi-pdi/ZFI PDI_12042019.csv",header =True,inferSchema=True)


pdi=pdi.withColumn('Mortgage_ID',pdi['Mortgage_ID'].cast('Integer')).withColumn('sales_document',pdi['sales_document'].cast('Integer')).withColumn('reference_document',pdi['reference_document'].cast('Integer'))

pdi=pdi.select("sales_document", "Mortgage_ID", "reference_document", "pdi_type", "card_expiry_date", "installment_number","installment_date", "emi_amount", "pdi_amount", "pdi_date", "pdi_received_date", "pdi_realization_date", "pdi_returned_date","reversed_date", "contract_flag", "pdi_status", "unique_identification_no", "waiver_date")


for col in pdi.columns:
	pdi=pdi.withColumnRenamed(col,col.lower())
		
for col in pdi.columns:
	pdi=pdi.withColumnRenamed(col,col.replace(" ", "_"))

pdi=pdi.filter('contract_flag <>"A"')


pdi=pdi.withColumn('installment_date',to_date('installment_date','MM/dd/yyyy')).withColumn('pdi_date',to_date('pdi_date','dd/MM/yyyy')).withColumn('pdi_returned_date',to_date('pdi_returned_date','MM/dd/yyyy')).withColumn('card_expiry_date',to_date('card_expiry_date','MM/dd/yyyy')).withColumn('pdi_received_date',to_date('pdi_received_date','MM/dd/yyyy')).withColumn('waiver_date',to_date('waiver_date','MM/dd/yyyy'))

pdi=pdi.withColumn('pdi_type', when(pdi.pdi_type == "NULL",'null_pdi').when(pdi.pdi_type== " ",'other').otherwise(pdi.pdi_type)).withColumn('pdi_status',when(pdi.pdi_status== " ",'100').otherwise(pdi.pdi_status).cast('Integer'))


# Check dates filter
pdi_1 = pdi.filter('installment_date>="2012-04-01" or waiver_date>= "2012-04-01" or pdi_realization_date >="2012-04-01"')
pdi_1.createOrReplaceTempView('p1')

## upcoming


pdi_2=spark.sql('select *, case when pdi_date <= installment_date and pdi_realization_date is not null and pdi_type !="null_pdi" then 1 else 0 end as Condition_1,case when pdi_date <= installment_date and reversed_date is not null and pdi_type !="null_pdi" and pdi_type in ("CASH","DCAS","TRFR") then 1 else 0 end as Condition_2,case when pdi_date <= installment_date and pdi_returned_date>=installment_date and pdi_type !="null_pdi" and pdi_type not in ("CASH","DCAS","TRFR") then 1 else 0 end as Condition_3,case when pdi_date <= installment_date and reversed_date>=installment_date and pdi_type !="null_pdi" and pdi_type not in ("CASH", "DCAS","TRFR") then 1 else 0 end as Condition_4,case when pdi_date> installment_date then 1 else 0 end as Condition_5 from p1')
pdi_2.createOrReplaceTempView('p2')
pdi_2=spark.sql('select *, case when Condition_1= 0 and Condition_2==0 and Condition_3==0 and Condition_4==0 and Condition_5==0 then 1 else 0 end as Condition_6 from p2')


## Ranking

pdi_2=pdi_2.withColumn('rank', when(pdi_2.Condition_1==1,10).when(pdi_2.Condition_2==1,8).when(pdi_2.Condition_3==1,6).when(pdi_2.Condition_4==1,4).when(pdi_2.Condition_5==1,2).otherwise(1))

## upcoming_installments

total_pdi = pdi_2.select('pdi_type','reference_document','mortgage_id','installment_date','installment_number','pdi_returned_date','pdi_date','pdi_realization_date','card_expiry_date','pdi_received_date','unique_identification_no','Condition_1','Condition_2','Condition_3','Condition_4','Condition_5','Condition_6','rank')


## change if running on 1st march filter 1st march to 31st march
total_upcoming_pdi= total_pdi.filter((total_pdi.installment_date >= "2019-03-01") & (total_pdi.installment_date <= "2019-03-31")).sort('installment_number')

total_upcoming_pdi.createOrReplaceTempView('tup')

upcoming_installments=spark.sql('select mortgage_id, max(installment_number) as installment_no from tup group by mortgage_id')


upcoming_installments.createOrReplaceTempView('ui')


upcoming_installments_1=spark.sql('select tup.*,ui.installment_no from ui left join tup on ui.mortgage_id=tup.mortgage_id and ui.installment_no=tup.installment_number order by rank desc')


upcoming_installments_ranked=upcoming_installments_1

upcoming_installments_ranked.createOrReplaceTempView('uir')



upcoming_installments_pdi_type= spark.sql('select mortgage_id,first(pdi_type) as pdi_type,first(reference_document) as reference_document,first(installment_number) as installment_number,first(rank) as rank,first(card_expiry_date) as card_expiry_date,first(pdi_received_date) as pdi_received_date,first(installment_date) as installment_date,first(unique_identification_no) as unique_identification_no from uir group by mortgage_id order by mortgage_id,pdi_type,reference_document,installment_number,rank,card_expiry_date,pdi_received_date,installment_date,unique_identification_no')


upcoming_installments_pdi_type.createOrReplaceTempView('uipt')

upcoming_installments_pdi_type_1=spark.sql('select mortgage_id,case when (rank==1 or rank ==2) then "null_pdi" else pdi_type end as upcoming_pdi_type,reference_document,installment_number as upcoming_installment_number,rank as conditional_rank, card_expiry_date,pdi_received_date,installment_date as installment_date,unique_identification_no as upcoming_pdi_uin from uipt')


upcoming_installments_pdi_type_1=upcoming_installments_pdi_type_1.withColumn('card_expiry_date_diff_with_upcoming_installment_date',ceil(((datediff(upcoming_installments_pdi_type_1.card_expiry_date,upcoming_installments_pdi_type_1.installment_date))/30.5))).withColumn('time_between_pdi_recieve_and_upcoming_pdi',ceil(((datediff(upcoming_installments_pdi_type_1.installment_date,upcoming_installments_pdi_type_1.pdi_received_date))/30.5)))

upcoming_installments_pdi_type_1.createOrReplaceTempView('uipt1')


# Join with Zfi


pdi_none_base=spark.sql('select z.contract_no,uipt1.* from z left join uipt1 on z.mortgage_id=uipt1.mortgage_id')
pdi_none_base_select=pdi_none_base.select('contract_no', 'mortgage_id', 'upcoming_pdi_type')
pdi_none_base_select=pdi_none_base_select.dropDuplicates(['contract_no'])
pdi_none_base_select.createOrReplaceTempView('pnb')

#pdi_none_base_select.repartition(1).write.csv("s3://cmh-process-data/test-1/EMI_Inflow",mode="append",header="true")	

#result.repartition(1).write.csv("D:\\result_pdi.csv",mode="append",header="true")

###### FIF PDI Not Available Contracts ########

active_with_emi0_rz =spark.sql('select aw0r.*, pnb.upcoming_pdi_type from aw0r left join pnb on aw0r.Contract_No=pnb.contract_no')


active_with_emi0_rz=active_with_emi0_rz.withColumn('pdi_contracts_status',when(active_with_emi0_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

# output-2

active_with_emi0_rz.repartition(1).write.csv("s3://cmh-process-data/test-1/EMI_Inflow/Active0",mode="append",header="true")



#### 1-5 EMI RZ with PDI Not Available Contracts #######


active_with_emi_1_5_rz =spark.sql('select a15r.*, pnb.upcoming_pdi_type from a15r left join pnb on a15r.Contract_No=pnb.contract_no')


active_with_emi_1_5_rz=active_with_emi_1_5_rz.withColumn('pdi_contracts_status',when(active_with_emi_1_5_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

## output-3

active_with_emi_1_5_rz.repartition(1).write.csv("s3://cmh-process-data/test-1/EMI_Inflow/active_1_5",mode="append",header="true")

#### More than 5 EMI RZ with PDI Not Available Contracts #######


active_with_morethan_5_emi_rz=spark.sql('select awm5r.*,pnb.upcoming_pdi_type from awm5r left join pnb on awm5r.Contract_No=pnb.Contract_No')

active_with_morethan_5_emi_rz=active_with_morethan_5_emi_rz.withColumn('pdi_contracts_status',when(active_with_morethan_5_emi_rz.upcoming_pdi_type=='null_pdi','PDI_not_Available').otherwise('PDI_Available_but_RO'))

## output-4
active_with_morethan_5_emi_rz.repartition(1).write.csv("s3://cmh-process-data/test-1/EMI_Inflow/active_more_5",mode="append",header="true")	


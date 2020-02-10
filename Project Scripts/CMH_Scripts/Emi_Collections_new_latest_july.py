from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window

# S3 path

base=spark.read.option("delimiter",",").csv("s3://cmh-process-data/test-1/Base_inflow_july/final_base.csv",header=True,inferSchema=True)


for col in base.columns:
	base=base.withColumnRenamed(col,col.replace(" ", "_"))

base=base.withColumnRenamed('Contract_No.','Contract_No')

base_1=base.select('Contract_No','New_Branch','Zone','OD_Bucket_Max')

base_1.createOrReplaceTempView('b1')

base_1=spark.sql('select *, case when New_Branch in ("bangalore koramangala","bangalore millers road","zest_bangalore") then "bangalore" when New_Branch in ("delhi bikaji","delhi jasola","delhi pitampura") then "delhi" when New_Branch in ("mumbai andheri","mumbai vashi") then "mumbai" when New_Branch in ("chennai", "zest_chennai") then "chennai" else New_Branch end as New_Branch_1 from b1')

base_1=base_1.dropDuplicates(['Contract_No'])

base_1=base_1.withColumn('flag',lit("Base"))

base_1.createOrReplaceTempView('b')

#inflow 

# S3 path

inflow=spark.read.option("delimiter",",").csv("s3://cmh-process-data/test-1/Base_inflow_july/final_inflow.csv",header=True,inferSchema=True)



for col in inflow.columns:
	inflow=inflow.withColumnRenamed(col,col.replace(" ", "_"))

inflow=inflow.withColumnRenamed('Contract_No.','Contract_No')

inflow_1=inflow.select('Contract_No','New_Branch','Zone','OD_Bucket_Max')

inflow.createOrReplaceTempView('if1')

inflow=spark.sql('select *, case when New_Branch in ("bangalore koramangala","bangalore millers road","zest_bangalore") then "bangalore" when New_Branch in ("delhi bikaji","delhi jasola","delhi pitampura") then "delhi" when New_Branch in ("mumbai andheri","mumbai vashi") then "mumbai" when New_Branch in ("chennai", "zest_chennai") then "chennai" else New_Branch end as New_Branch_1 from if1')

inflow=inflow.dropDuplicates(['Contract_No'])

inflow=inflow.withColumn('flag',lit("Inflow"))

inflow.createOrReplaceTempView("if")

base_summ=spark.sql("Select New_Branch_1 as New_Branch, Count(*) as opening_base from b group by New_Branch_1 order by New_Branch_1")

base_summ.createOrReplaceTempView('bs')

inflow_summ=spark.sql("Select New_Branch_1 as New_Branch, Count(*) as inflow from if group by New_Branch_1 order by New_Branch_1")

inflow_summ.createOrReplaceTempView('ifs')

base_inflow=spark.sql("Select bs.*,ifs.inflow from bs left join ifs on bs.New_Branch=ifs.New_Branch")

base_inflow.createOrReplaceTempView('bi')


target=spark.read.option("delimiter","|").csv(r"s3://cmh-raw-data/complete-dump/Target-sheets-emi/emi target jul.csv",header=True,inferSchema=True)

target=target.withColumnRenamed('Closing Base Target','Closing_base_target')
target=target.withColumn('Branch',trim(target.Branch))
target=target.withColumn('Branch',lower(target.Branch))
target.createOrReplaceTempView('t')

base_inflow_1=spark.sql("Select New_Branch,opening_base,inflow,round((opening_base/(select sum(opening_base) from bi))*(select ((sum(opening_base)-(select sum(Closing_base_target) from t))+6000) from  bi)) as assumed_inflow from bi")
base_inflow_1.createOrReplaceTempView('bi1')

base_inflow_2=spark.sql("select bi1.*,t.Closing_base_target from bi1 left join t on bi1.New_Branch=t.Branch")
base_inflow_2.createOrReplaceTempView('bi2')

### recovery target #####

day=spark.sql('select dayofmonth(last_day(now() - interval 01 month)) as day').collect()
day=day[0].day


base_inflow_3=spark.sql('select New_Branch,opening_base,inflow as MTD_inflow,Closing_base_target,assumed_inflow,opening_base - Closing_base_target + case when  dayofmonth(last_day(now() - interval 01 month)) <= 7  then greatest(assumed_inflow,inflow) else inflow end as Recovery_Target from bi2')
base_inflow_3.createOrReplaceTempView('bi3')

####  Collected & Realized####

zfi_65_28_feb =spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_ZFI_065_daily/ZFI_065_Daily_2.csv",header=True,inferSchema=True)

for col in zfi_65_28_feb.columns:
	zfi_65_28_feb=zfi_65_28_feb.withColumnRenamed(col,col.lower())
	

zfi_65_28_feb = zfi_65_28_feb.withColumnRenamed('odbucketmax','OD_Bucket_max').withColumnRenamed('Mortgage ID','Mortgage_ID').withColumnRenamed('total_od_emi_dp','Total_OD').withColumnRenamed('emibanked','EMI_Banked').withColumnRenamed('dpoverdueamount','DP_Overdue_amount').withColumnRenamed('realizedpercentcontract','Realized_per_con').withColumnRenamed('contractno','Contract_No').withColumnRenamed('mortgagecheck','Mortgage_Check')

zfi_65_28_feb.createOrReplaceTempView('zfeb')

# check total_od column
zfi_65_summ=spark.sql("Select Contract_No,Realized_per_con,status,Mortgage_Check,Total_OD,OD_Bucket_max,EMI_Banked,DP_Overdue_amount from zfeb where status='Active' and OD_Bucket_max not in ('43-45','00-03','04-06','07-09','28-30','25-27','10-12','22-24','13-15','34-36','16-18','19-21','40-42','46-48','37-39','31-33','49-MO') and Mortgage_Check=1 and Total_OD<=0 and EMI_Banked=0 and DP_Overdue_amount=0")

zfi_65_summ.createOrReplaceTempView('zsum')

### Change

od_contracts = zfi_65_28_feb.filter("OD_Bucket_Max in ('43-45','00-03','04-06','07-09','28-30','25-27','10-12','22-24','13-15','34-36','16-18','19-21','40-42','46-48','37-39','31-33','49-MO')")

od_contracts=od_contracts.select('Contract_No')
od_contracts=od_contracts.withColumn('od_flag', lit(1))
od_contracts.createOrReplaceTempView('od')

zfi_65_summ_1 = spark.sql('select zsum.*,od.od_flag from zsum left join od on zsum.Contract_No=od.Contract_No')
zfi_65_summ_1=zfi_65_summ_1.fillna(0,subset=['od_flag'])
zfi_65_summ_1=zfi_65_summ_1.select('Contract_No','Realized_per_con')
zfi_65_summ_1.createOrReplaceTempView('zsum1')

## base inflow append

base_od=base_1.select('Contract_No','flag','New_Branch_1','OD_Bucket_Max')

base_od.createOrReplaceTempView('bo')

inflow_od=inflow.select('Contract_No','flag','New_Branch_1','OD_Bucket_Max')
inflow_od.createOrReplaceTempView('io')

od_base_inflow=spark.sql('select Contract_No, flag,New_Branch_1 as New_Branch,OD_Bucket_Max from bo union all select Contract_No,flag,New_Branch_1 as New_Branch,OD_Bucket_Max from io')
od_base_inflow.createOrReplaceTempView('obi')

#	

zfi_od_base_inflow=od_base_inflow.join(zfi_65_summ_1,od_base_inflow.Contract_No==zfi_65_summ_1.Contract_No, how ='inner').drop(zfi_65_summ_1.Contract_No)
zfi_od_base_inflow.createOrReplaceTempView('zobi')

rez_collection=spark.sql("select New_Branch,count(Contract_No) as Total_realised from zobi group by New_Branch order by New_Branch")

rez_collection.createOrReplaceTempView('rc1')

base_inflow_4=spark.sql("select bi3.*,rc1.Total_realised from bi3 left join rc1 on bi3.New_Branch=rc1.New_Branch")

base_inflow_4=base_inflow_4.fillna(0,subset=['MTD_inflow'])


# change if daily just take day
day=spark.sql('select dayofmonth(last_day(now() - interval 01 month)) as day').collect()
day=day[0].day

# make Dynamic
base_inflow_4=base_inflow_4.withColumn('Day',lit(day))
base_inflow_4=base_inflow_4.withColumn('MTD_inflow', when(base_inflow_4.Day<=7,base_inflow_4.assumed_inflow).otherwise(base_inflow_4.MTD_inflow))

base_inflow_4.createOrReplaceTempView('bi4')

base_inflow_5=spark.sql('Select *,case when Day<=7 then 0.15* Closing_base_target when (Day>=8 and Day<=15) then 0.45*Closing_base_target when (Day>=16 and Day<=21) then 0.75* Closing_base_target else 1*Closing_base_target end as Milestone_target from bi4')

base_inflow_5=base_inflow_5.withColumn('Milestone_target_Achieved_Perc',round((base_inflow_5.Total_realised/base_inflow_5.Milestone_target)*100))

day_name=spark.sql('select date_format(last_day(now() - interval 01 month),"E") as day_name').collect()
day_name=day_name[0].day_name

# keep thursday value and make dynamic

base_inflow_5=base_inflow_5.withColumn('day_name',lit(day_name))

base_inflow_5=base_inflow_5.withColumn('day_value', when(base_inflow_5.day_name=='Mon',6).when(base_inflow_5.day_name=='Tue',5).when(base_inflow_5.day_name=='Wed',4).when(base_inflow_5.day_name=='Thu',3).when(base_inflow_5.day_name=='Fri',2).otherwise(1))

base_inflow_5=base_inflow_5.withColumn('Required_DRR',round((base_inflow_5.Milestone_target-base_inflow_5.Total_realised)/base_inflow_5.day_value))


base_inflow_5.createOrReplaceTempView('bi5')

rec_bucket=spark.sql('select zobi.* from zobi left join obi on zobi.Contract_No=obi.Contract_No')
rec_bucket.createOrReplaceTempView('rb')

rec_bucket_1=spark.sql('select New_Branch,OD_Bucket_Max,count(*) as Count from rb group by New_Branch,OD_Bucket_Max')
rec_bucket_1.createOrReplaceTempView('rb1')

rec_bucket_summ=spark.sql('select New_Branch, sum(case when OD_Bucket_Max = "00-01" then count end) as X_00_01,sum(case when OD_Bucket_Max = "02-03" then count end) as X_02_03,sum(case when OD_Bucket_Max = "04-06" then count end) as X_04_06, sum(case when OD_Bucket_Max = "07-09" then count end) as X_07_09,sum(case when OD_Bucket_Max = "10-12" then count end) as X_10_12,sum(case when OD_Bucket_Max = "13-15" then count end) as X_13_15,sum(case when OD_Bucket_Max = "16-18" then count end) as X_16_18,sum(case when OD_Bucket_Max = "19-21" then count end) as X_19_21,sum(case when OD_Bucket_Max = "22-24" then count end) as X_22_24,sum(case when OD_Bucket_Max = "25-27" then count end) as X_25_27,sum(case when OD_Bucket_Max = "28-30" or OD_Bucket_Max = "31-33" or OD_Bucket_Max = "34-36" or OD_Bucket_Max = "37-39" or OD_Bucket_Max = "40-42" or OD_Bucket_Max = "43-45" or OD_Bucket_Max = "46-48" or OD_Bucket_Max = "49-MO" then count end) as X_27_more from rb1 group by New_Branch')

rec_bucket_summ=rec_bucket_summ.na.fill(0)

rec_bucket_summ=rec_bucket_summ.withColumn('X_07_12', (rec_bucket_summ.X_07_09 + rec_bucket_summ.X_10_12)).withColumn('X_13_18',(rec_bucket_summ.X_13_15 + rec_bucket_summ.X_16_18))

rec_bucket_summ.createOrReplaceTempView('rbs')

base_inflow_6=spark.sql("Select bi5.*,rbs.X_00_01, rbs.X_02_03, rbs.X_04_06, rbs.X_07_12, rbs.X_13_18, rbs.X_19_21, rbs.X_22_24, rbs.X_25_27, rbs.X_27_more from bi5 left join rbs on bi5.New_Branch=rbs.New_Branch")

emi=base_inflow_6

#for col in emi.columns:
	#emi= emi.withColumnRenamed(col,col.replace("-", "_"))
	
emi=emi.na.fill(0)

emi.createOrReplaceTempView('emi')

#emi_1=emi.withColumn('X_07_12',(emi.X_07_09 + emi.X_10_12)).withColumn('X_13_18',(emi.X_13_15 + emi.X_16_18)).withColumn('X_27+',(emi.X_28_30 + emi.X_31_33 + emi.X_34_36 + emi.X_37_39 + emi.X_40_42 + emi.X_43_45 + emi.X_46_48 + emi.X_49_MO))

#emi_1=emi.withColumn('X_07_12',(emi.X_07_09 + emi.X_10_12)).withColumn('X_13_18',(emi.X_13_15 + emi.X_16_18)).withColumn('X_27+',(emi.X_28_30 + emi.X_31_33 + emi.X_34_36 + emi.X_37_39 + emi.X_40_42 + emi.X_43_45 + emi.X_46_48 + emi.X_49_MO))

#emi_1=emi_1.withColumnRenamed('X_00_03','X_02_03')

#emi_1=emi_1.drop('X_07_09').drop('X_10_12').drop('X_13_15').drop('X_16_18').drop('X_28_30').drop('X_31_33').drop('X_34_36').drop('X_37_39').drop('X_40_42').drop('X_43_45').drop('X_46_48').drop('X_49_MO').drop('X_New_Branch')

base_inflow_7=emi

base_inflow_7.createOrReplaceTempView('bi7')

## Tele Caller

#dialler_data= spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\Dialler Data\Dialer_data EMI_12042019.csv",header =True,inferSchema=True)

# S3 path

dialler_data= spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_Emi_dialler/emi_dialler.csv",header =True,inferSchema=True)

#dialler_data = dialler_data.select('ID','AgentName','MemberName','ContractId','CallStatus')
#dialler_data =dialler_data.withColumn('ID',dialler_data.ID.cast("Integer"))

dialler_data=dialler_data.filter(dialler_data.CallStatus == 'Connected')

dialler_data.createOrReplaceTempView('dd')


# S3 path
dialler_rec_log=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_dialler_recording_log/dialler_recording_log.csv",header =True,inferSchema=True)


#dialler_rec_log=dialler_rec_log.select('Lead_id','Campaign_name', 'Start_time','Length_in_sec')

#dialler_rec_log=dialler_rec_log.withColumn('year',year('Start_time')).withColumn('month',month('Start_time'))

dialler_rec_log_1=dialler_rec_log.filter('Start_time between date_add(last_day(now() - interval 02 month),1)and last_day(now() - interval 01 month)')

dialler_rec_log_1.createOrReplaceTempView('drl')

emi_dialler=spark.sql("Select drl.Lead_id,drl.Campaign_name, drl.Start_time, drl.Length_in_sec,dd.AgentName,dd.MemberName,dd.ContractId,dd.CallStatus from drl left join dd on drl.Lead_id=dd.ID")
emi_dialler=emi_dialler.dropDuplicates()
emi_dialler.createOrReplaceTempView('ed')

# S3 path

agent_role= spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/agent mapping/agent mapping jul.csv",header =True,inferSchema=True)

agent_role=agent_role.withColumnRenamed('Agent name', 'Agent_Name').withColumnRenamed('Agent Type','Agent_type')
agent_role=agent_role.withColumn('Branch',trim(agent_role.Branch))
agent_role=agent_role.withColumn('Branch',lower(agent_role.Branch))
agent_role.createOrReplaceTempView('ar')

emi_dialler_join=spark.sql("select ed.*,ar.Agent_type from ed left join ar on ed.AgentName=ar.Agent_Name")
emi_dialler_join=emi_dialler_join.na.fill(0)
emi_dialler_join.createOrReplaceTempView('edj')


#emi_master=spark.sql("select edj.*, obi.New_Branch from obi left join edj on edj.ContractId=obi.Contract_No")

emi_master=spark.sql("select obi.Contract_No,obi.New_Branch,edj.Lead_id,edj.Campaign_name,edj.Start_time,edj.Length_in_sec,edj.AgentName,edj.MemberName,edj.CallStatus,edj.Agent_type from obi left join edj on edj.ContractId=obi.Contract_No")

#emi_master=emi_master.dropDuplicates()

emi_master.createOrReplaceTempView('em')

emi_master_summ=spark.sql('select New_Branch , count(New_Branch) as total_mtd_contacts, count(distinct MemberName) as total_unique_mtd_contacts from em group by New_Branch')

emi_master_summ.createOrReplaceTempView('ems')

emi_master=emi_master.withColumn('day', dayofmonth('Start_time'))
emi_master_tele_caller=emi_master.filter(emi_master.Agent_type=='Tele Caller')
emi_master_tele_caller=emi_master_tele_caller.na.fill(0)
emi_master_tele_caller.createOrReplaceTempView('emtc')

day_wise_summ=spark.sql("select New_Branch,day,count(New_Branch) as Total_calls, count(distinct AgentName) as Total_agents from emtc group by New_Branch,day")

day_wise_summ=day_wise_summ.withColumn('avg_contacts_per_tc_day',(day_wise_summ.Total_calls/day_wise_summ.Total_agents))

day_wise_summ.createOrReplaceTempView('dws')

agg_day_wise_dialer=spark.sql("Select New_Branch,sum(avg_contacts_per_tc_day) as total_contacts_per_tc_day, count(distinct day) as unique_day from dws group by New_Branch")

agg_day_wise_dialer=agg_day_wise_dialer.withColumn('mtd_avg_contacts_per_tc_per_day',(agg_day_wise_dialer.total_contacts_per_tc_day/agg_day_wise_dialer.unique_day))

agg_day_wise_dialer=agg_day_wise_dialer.select('New_Branch','mtd_avg_contacts_per_tc_per_day')

agg_day_wise_dialer.createOrReplaceTempView('adws')

##
#prev_day = spark.sql("select dayofmonth(max(Start_time) - interval 02 day) as prev_day from drl").collect()

#prev_day =spark.sql("select s.prev_day from (select dayofmonth(max(Start_time) as prev_day,row_number() over(order by  desc) as rownum1 from drl) s where rownum1 =2")
#prev_day=prev_day[0].prev_day

# Make Dynamic
yesterday_contact= day_wise_summ.filter('day == 28')
yesterday_contact=yesterday_contact.na.fill(0)
yesterday_contact=yesterday_contact.withColumn('yesterday_contact_per_tc',yesterday_contact.Total_calls/yesterday_contact.Total_agents)

yesterday_contact=yesterday_contact.select('New_Branch','yesterday_contact_per_tc')

yesterday_contact.createOrReplaceTempView('yc')

dialler_master_summ_final=spark.sql("Select ems.*,adws.mtd_avg_contacts_per_tc_per_day,yc.yesterday_contact_per_tc from ems left join adws on ems.New_Branch=adws.New_Branch left join yc on ems.New_Branch=yc.New_Branch")

dialler_master_summ_final.createOrReplaceTempView('dmsf')

base_inflow_8= spark.sql("Select bi7.*,dmsf.total_mtd_contacts ,dmsf.total_unique_mtd_contacts,dmsf.mtd_avg_contacts_per_tc_per_day,dmsf.yesterday_contact_per_tc from bi7 left join dmsf on bi7.New_Branch=dmsf.New_Branch")

base_inflow_8.createOrReplaceTempView('bi8')

field_app= spark.read.option("delimiter",",").csv("s3://cmh-process-data/test-1/EMI_APP_DATA_DASHBOARD/01_08_EMIAPP/EMI_APP_01_08.csv",header =True,inferSchema=True)

field_app_1=field_app.select('Branch','Total_unique_members_met','Yesterday_Visit_count','of_visits_per_day_ce')
field_app_1=field_app_1.withColumn('Branch',lower(field_app_1.Branch))

field_app_1=field_app_1.withColumn('Branch',regexp_replace('Branch', ' - ',' '))
field_app_1=field_app_1.withColumn('Branch',regexp_replace('Branch', '-',' '))
field_app_1.createOrReplaceTempView('fa')

field_app_1=spark.sql('select *, case when Branch in ("bangalore koramangala","bangalore millers road") then "bangalore" when Branch in ("mumbai andheri", "mumbai vashi") then "mumbai" when Branch in ("delhi pitampura" ,"delhi jasola") then "delhi" else Branch end as New_Branch  from fa')

field_app_1.createOrReplaceTempView('fa')

field_app_final=spark.sql('select New_Branch, sum(Total_unique_members_met) as Total_MTD_visits,round(sum(Yesterday_Visit_count),2) as yesterday_visits,round(sum(of_visits_per_day_ce),2) as MTD_visits_per_CE_day from fa group by New_Branch')

field_app_final.createOrReplaceTempView('faf')

base_inflow_9=spark.sql('select bi8.*,faf.Total_MTD_visits,faf.yesterday_visits,faf.MTD_visits_per_CE_day from bi8 left join faf on bi8.New_Branch=faf.New_Branch')

base_inflow_9.createOrReplaceTempView('bi9')


## Future PDI


#con_pay=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\ZFI pdi\ZFI PDI_12042019.csv",header =True,inferSchema=True)


#S3 path

con_pay=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_ZFI_PDI/ZFI_PDI.csv",header =True,inferSchema=True)

for col in con_pay.columns:
	con_pay=con_pay.withColumnRenamed(col,col.lower())

con_pay_select=con_pay.select('reference_document','pdi_type','pdi_status','pdi_realization_date','installment_date','pdi_received_date','pdi_returned_date','pdi_realization_date')

con_pay_select=con_pay_select.withColumn('pdi_returned_date',to_date('pdi_returned_date',"MM/dd/yyyy")).withColumn('pdi_received_date', to_date('pdi_received_date', "MM/dd/yyyy")).withColumn('installment_date', to_date('installment_date', "MM/dd/yyyy"))

con_pay_select.createOrReplaceTempView('cp')
con_pay_1=spark.sql(" select reference_document,count(*) as pdi_count from cp where pdi_status= 04 group by reference_document")
con_pay_1.createOrReplaceTempView('cp1')
fut_pdi_df = spark.sql("Select zobi.*, cp1.pdi_count from zobi left join cp1 on zobi.Contract_No=cp1.reference_document")
fut_pdi_df=fut_pdi_df.na.fill(0)
fut_pdi_df.createOrReplaceTempView("fpd")
fut_pdi_df=spark.sql("Select Contract_No,New_Branch,pdi_count, case when (pdi_count>0 and pdi_count=1) then 'PDI_1' when (pdi_count>1 and pdi_count<=3) then 'PDI_2to3' when (pdi_count>3 and pdi_count<=12) then 'PDI_4to12' when pdi_count>12 then 'PDI_12_more' else 'PDI_0' end as pdi_setup_bucket from fpd")
fut_pdi_df.createOrReplaceTempView('fpd')
foreclosed_data = zfi_65_28_feb.select('Contract_No','Realized_per_con')
foreclosed_data=foreclosed_data.filter('Realized_per_con>=100')
foreclosed_data=foreclosed_data.withColumn('foreclosed_flag',lit(1))
foreclosed_data =foreclosed_data.dropDuplicates()
foreclosed_data.createOrReplaceTempView('fcd')

fut_pdi_summ=spark.sql("select fpd.*,fcd.Realized_per_con,fcd.foreclosed_flag from fpd left join fcd on fpd.Contract_No=fcd.Contract_No")
fut_pdi_summ.createOrReplaceTempView('fps')
fut_pdi_summ=spark.sql("select *, case when foreclosed_flag=1 then 'Foreclosed' else pdi_setup_bucket end as pdi_setup_bucket_new from fps")
fut_pdi_summ.createOrReplaceTempView('fps')
fut_pdi_summ_final=spark.sql("Select New_Branch,pdi_setup_bucket_new,count(*) as count from fps group by New_Branch,pdi_setup_bucket_new")
ct_fut_pdi= fut_pdi_summ_final.groupBy('New_Branch').pivot('pdi_setup_bucket_new').sum('count')
ct_fut_pdi.createOrReplaceTempView('cfp')

base_inflow_10=spark.sql("Select bi9.*,cfp.Foreclosed,cfp.PDI_0,cfp.PDI_1,cfp.PDI_2to3,cfp.PDI_4to12,cfp.PDI_12_more from bi9 left join cfp on bi9.New_Branch=cfp.New_Branch")
base_inflow_10.createOrReplaceTempView('bi10')


contract_pay=con_pay_select
contract_pay.createOrReplaceTempView('c_p')

contract_pay=contract_pay.withColumn('pdi_returned_date',to_date('pdi_returned_date',"MM/dd/yyyy")).withColumn('pdi_received_date', to_date('pdi_received_date', "MM/dd/yyyy")).withColumn('installment_date', to_date('installment_date', "MM/dd/yyyy"))



# select contracts whose pdi returned date is in last 3 months

# Change july date to Now() to make dynamic

ro_contract_pay_1=spark.sql('Select distinct reference_document as ContractNo from c_p where pdi_returned_date >= date_add(last_day("2019-07-14" - interval 04 months),1) and pdi_returned_date <=last_day("2019-07-14" - interval 01 months)')
ro_contract_pay_1.createOrReplaceTempView('rcp1')

# reading current month inflow and recovery files and inner join

inflow_1 = inflow
inflow_1.createOrReplaceTempView('inf')

## reading recovery of current month

recovery_feb=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_Recovery/recovery.csv",header =True,inferSchema=True)

recovery_july_filter= recovery_feb.filter('rec_date >= date_add(last_day("2019-07-13" - interval 1 months),1) and rec_date <= last_day("2019-07-13")')

recovery_july_filter.createOrReplaceTempView('rf')

inflow_recovery= spark.sql('select inf.Contract_No from inf inner join rf on rf.Contract_No=inf.Contract_No')

inflow_recovery.createOrReplaceTempView('i_r')

## ro inner join with inflow_recovery

ro_contracts_future=spark.sql('select rcp1.* from rcp1 inner join i_r on rcp1.ContractNo=i_r.Contract_No')

ro_contracts_future=ro_contracts_future.withColumn('ro_status',lit('pdi_available_but_likely_future_ro'))
ro_contracts_future.createOrReplaceTempView('rcf')


## 
ro_contracts_future_final=spark.sql('select rcf.ContractNo,rcf.ro_status,obi.New_Branch from rcf inner join obi on obi.Contract_No=rcf.ContractNo')
ro_contracts_future_final.createOrReplaceTempView('rcff')

ro_contracts_future_final_summ = spark.sql('select New_Branch, count(ro_status) as pdi_available_but_likely_future_ro from rcff group by New_Branch')

ro_contracts_future_final_summ.createOrReplaceTempView('rcffs')

## 
# change Repalce the date with now() later

pdi_modified=spark.sql('Select distinct reference_document as ContractNo from c_p where pdi_received_date between date_add(last_day("2019-07-14" - interval 01 month),1) and last_day("2019-07-14") and installment_date>= date_add(last_day("2019-07-14"),1)')

pdi_modified.createOrReplaceTempView('pm')

ro_contracts_present=spark.sql('select rcff.ContractNo,rcff.New_Branch from rcff inner join pm on rcff.ContractNo=pm.ContractNo')

ro_contracts_present=ro_contracts_present.withColumn('ro_status',lit('contracts_with_fresh_pdi_setup'))

ro_contracts_present.createOrReplaceTempView('rcp')

ro_contracts_present_summ = spark.sql('select New_Branch, count(ro_status) as contracts_with_fresh_pdi_setup from rcp group by New_Branch ')

ro_contracts_present_summ.createOrReplaceTempView('rcps')


base_inflow_11=spark.sql('select bi10.*,rcffs.pdi_available_but_likely_future_ro from bi10 left join rcffs on bi10.New_Branch=rcffs.New_Branch')

base_inflow_11.createOrReplaceTempView('bi11')

base_inflow_12=spark.sql('select bi11.*,rcps.contracts_with_fresh_pdi_setup from bi11 left join rcps on bi11.New_Branch=rcps.New_Branch')

base_inflow_12=base_inflow_12.withColumn('run_date', lit(current_date()))

base_inflow_12.repartition(1).write.csv("s3://cmh-process-data/test-1/emi_collections_intermediate",mode="append",header="true")


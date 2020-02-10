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


# Reading ZFI_065_Daily File from S3 master Tables

zfi_65_Daily =spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_ZFI_065_daily/ZFI_065_Daily.csv",header=True,inferSchema=True)


zfi_65_Daily = zfi_65_Daily.withColumn('dw_lastupdated',to_date('dw_lastupdated','yyyy-MM-dd')).withColumn('saledate',to_date('saledate','yyyy-MM-dd'))

zfi_65_Daily.createOrReplaceTempView('zfi')

for col in zfi_65_Daily.columns:
	zfi_65_Daily=zfi_65_Daily.withColumnRenamed(col,col.lower())


zfi_65_Daily = zfi_65_Daily.withColumnRenamed('contractno','Contract_No').withColumnRenamed('memberid','Member_ID').withColumnRenamed('mortgageid','Mortgage_ID').withColumnRenamed('status','Status').withColumnRenamed('odbucketmax','OD_Bucket_max').withColumnRenamed('mortgagecheck','Mortgage_Check').withColumnRenamed('name','Name').withColumnRenamed('emirealizedcount','EMI_Realized_count').withColumnRenamed('saledate','Sale_date').withColumnRenamed('realizedpercentcontract','Realized_percent_con')

zfi_65_Daily = zfi_65_Daily.withColumn("OD_Bucket_max",regexp_replace("OD_Bucket_max", "[^a-zA-Z0-9_-]", ''))

zfi_65_Daily=zfi_65_Daily.withColumn("OD_Bucket_max",trim(zfi_65_Daily.OD_Bucket_max))

zfi_65_Daily=zfi_65_Daily.withColumn('Branch',zfi_65_Daily.Name)

zfi_65_Daily=zfi_65_Daily.select('Contract_No','Member_ID','Mortgage_ID','Status','OD_Bucket_Max','Mortgage_Check','Name','Branch','EMI_Realized_count','Sale_date','Realized_percent_con')

## Applying the Filters i.e Mortgage_Check= 1 and OD_Bucket_Max <>""

zfi_filter= zfi_65_Daily.filter('Mortgage_Check==1 and Status =="Active" and OD_Bucket_Max <>""')
zfi_filter.createOrReplaceTempView('zfilter')


zfi_filter=spark.sql('select *,(datediff(date_add(last_day(current_date() - interval 01 month),1),Sale_date))/30.5 as Sale_vintage from zfilter')
zfi_filter=zfi_filter.withColumn('Sale_vintage',ceil('Sale_vintage'))

## Creating Buckets based on the EMI Realised Count
zfi_filter=zfi_filter.withColumn('EMI_Realization_Bucket',when(zfi_filter.EMI_Realized_count==0,'0').when(zfi_filter.EMI_Realized_count >5,'>5').when((zfi_filter.EMI_Realized_count >0) & (zfi_filter.EMI_Realized_count <=5) ,'1_5'))

## Creating Buckets based on the sale_Vintage
zfi_filter=zfi_filter.withColumn('Sale_vintage_Bucket',when((zfi_filter.Sale_vintage>= 0) & (zfi_filter.Sale_vintage<=3),'0-3M').when((zfi_filter.Sale_vintage>= 4) & (zfi_filter.Sale_vintage<=12),'4-12M').when((zfi_filter.Sale_vintage>12),'>12'))


Zfi_filter=zfi_filter.withColumn('RZ_Bucket',when( (zfi_filter.Realized_percent_con<=10),'0-10%').when((zfi_filter.Realized_percent_con>10) & (zfi_filter.Realized_percent_con<=20),'10-20%').when((zfi_filter.Realized_percent_con>20) & (zfi_filter.Realized_percent_con<=30),'20-30%').when((zfi_filter.Realized_percent_con>30) & (zfi_filter.Realized_percent_con<=40),'30-40%').when((zfi_filter.Realized_percent_con>40) & (zfi_filter.Realized_percent_con<=50),'40-50%').when((zfi_filter.Realized_percent_con>50) & (zfi_filter.Realized_percent_con<=60),'50-60%').when((zfi_filter.Realized_percent_con>60) & (zfi_filter.Realized_percent_con<=70),'60-70%').when((zfi_filter.Realized_percent_con>70) & (zfi_filter.Realized_percent_con<=80),'70-80%').when((zfi_filter.Realized_percent_con>80) & (zfi_filter.Realized_percent_con<=90),'80-90%').when((zfi_filter.Realized_percent_con>90),'90-100%'))

zfi_filter.createOrReplaceTempView('zf')


## Holiday###

# reading the Reservation table from S3 Master Tables

reservation_table =spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_reservation_done/reservation_done.csv",header=True,inferSchema=True)
reservation_table=reservation_table.withColumn('dwLastUpdatedOn',to_date('dwLastUpdatedOn','yyyy-MM-dd'))
reservation_table.createOrReplaceTempView('reservation')

## take latest seasoned Holiday

reservation_table=reservation_table.withColumn('ReservationStatusID',reservation_table.ReservationStatusID.cast('integer'))

## Remark - Cast ReservationStatusID to Integer


# change Dynamic check between taking outer values
reservation_filter=reservation_table.filter('dBookingDate < date_add(last_day(current_date() - interval 01 months),1) and CheckIn < date_add(last_day(current_date() - interval 01 months),1) and ReservationStatusID between 21 and 24')

reservation_filter.createOrReplaceTempView('rf')

# Applying left join on ZFI and Reservation Table.
zfi_reservation=spark.sql('select zf.*,rf.* from zf left join rf on zf.Contract_No=rf.ContractID')

zfi_reservation.createOrReplaceTempView('zr')

zfi_reservation_summary=spark.sql('select Contract_No,count(ReservationNo) as count_HD, sum(case when RoomNights>=1 then RoomNights else 0 end) as HD_Nights,max(CheckIn) as Last_HD from zr group by Contract_No')

zfi_reservation_summary.createOrReplaceTempView('zrs')
zfi_reservation_new =zfi_reservation.dropDuplicates(['Contract_No'])

zfi_reservation_new.createOrReplaceTempView('zr1')


zfi_reservation_final=spark.sql('select zr1.*,zrs.count_HD, zrs.HD_Nights, zrs.Last_HD from zr1 left join zrs on zr1.Contract_No=zrs.Contract_No')

zfi_reservation_final=zfi_reservation_final.withColumn('Holiday_bucket',when(zfi_reservation_final.HD_Nights==0,'0_HD').when(zfi_reservation_final.HD_Nights==1,'1_HD').when(zfi_reservation_final.HD_Nights==2,'2_HD').when(zfi_reservation_final.HD_Nights==3,'3_HD').when(zfi_reservation_final.HD_Nights==4,'4_HD').when(zfi_reservation_final.HD_Nights==5,'5_HD').when(zfi_reservation_final.HD_Nights==6,'6_HD').when(zfi_reservation_final.HD_Nights>=7,'7_HD_+'))
zfi_reservation_final.createOrReplaceTempView('zrf_filter')

# Change Dynamic

zfi_reservation_final=spark.sql('select *, round((datediff(date_add(last_day(current_date() - interval 01 month),1),Last_HD))/30.5,2) as Time_since_last_holiday from zrf_filter')

zfi_reservation_final=zfi_reservation_final.withColumn('Time_since_last_holiday_bucket', when(zfi_reservation_final.Time_since_last_holiday<6,'<_6').when((zfi_reservation_final.Time_since_last_holiday>=6 )&(zfi_reservation_final.Time_since_last_holiday<=12),'6_12').when((zfi_reservation_final.Time_since_last_holiday>12)&(zfi_reservation_final.Time_since_last_holiday<18),'13_18').when((zfi_reservation_final.Time_since_last_holiday>=18)&(zfi_reservation_final.Time_since_last_holiday<24),'18_24').when(zfi_reservation_final.Time_since_last_holiday>24,'24>').otherwise('NO_HD'))

zfi_reservation_final.createOrReplaceTempView('zrf')


##

# Reading Base File From S3 master tables

base_Complete=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_final_base/final_base.csv",header=True,inferSchema=True)

base_Complete = base_Complete.withColumn('date', to_date('month_year','yyyy-MM'))

base_current_month=base_Complete.filter('date>= date_add(last_day(now() - interval 01 month),1) and date <= last_day(now())')

for col in base_current_month.columns:
	base_current_month=base_current_month.withColumnRenamed(col,col.replace(" ", "_"))

base_current_month=base_current_month.withColumnRenamed('Contract_No.','Contract_No').withColumnRenamed('New_Branch','ControlLocation')

base_current_month=base_current_month.withColumn('flag',lit("Base"))
base_current_month.createOrReplaceTempView('b1')



# Reading Inflow File From S3 master tables

inflow_complete=spark.read.option("delimiter",",").csv("s3://cmh-process-data/master-tables/master_table_final_inflow/final_inflow.csv",header=True,inferSchema=True)

inflow_complete = inflow_complete.withColumn('date', to_date('month_year','yyyy-MM'))

inflow_current_month=inflow_complete.filter('date>= date_add(last_day(now() - interval 01 month),1) and date <= last_day(now())')

for col in inflow_current_month.columns:
	inflow_current_month=inflow_current_month.withColumnRenamed(col,col.replace(" ", "_"))

inflow_current_month=inflow_current_month.withColumnRenamed('Contract_No.','Contract_No').withColumnRenamed('New_Branch','ControlLocation')

inflow_current_month=inflow_current_month.withColumn('flag',lit("Inflow"))

inflow_current_month.createOrReplaceTempView("if")

## Reading ZOD Inflow File from S3 master Table

zod_inflow=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_zod/zod_inflow.csv",header=True,inferSchema=True)

zod_inflow=zod_inflow.withColumn('dw_lastupdated',to_date('dw_lastupdated','yyyy-MM-dd'))

zod_inflow.createOrReplaceTempView('zod')


# Appending Base and Inflow 
base_inflow=spark.sql('select distinct Contract_No, flag,2 as weight from b1 union all select distinct Contract_No,flag,1 as weight from if')
base_inflow.createOrReplaceTempView("bi")

## selecting the Contracts with Flag =2.
base_inflow_final=spark.sql('select k.Contract_No,k.flag from (select Contract_No,flag,weight,row_number() over(partition by Contract_No order by weight desc) as rownum1 from bi) k where rownum1 =1')
base_inflow_final.createOrReplaceTempView("bif")


## 
zfi_reservation_final_Table_1= spark.sql('select zrf.*,bif.flag as Base_Inflow_Type from zrf left join bif on zrf.Contract_No=bif.Contract_No')

zfi_reservation_final_Table_1.createOrReplaceTempView("zrf1")




# Reading Contract data file from S3 master tables

contract=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_Contract_data/Contract_data.csv",header =True,inferSchema=True)

contract=contract.withColumn('dtLastUpdated',to_date('dtLastUpdated','yyyy-MM-dd'))

contract_dp = contract.select('aContractID','tFinanceInstitute','nPaymentTenure','Zone','dtLastUpdated')

contract_dp.createOrReplaceTempView('cd')

dpp=spark.sql("select aContractID,Zone, Case when tFinanceInstitute = 'Bliss-50 DP' then 50 when tFinanceInstitute = 'GoZest-15 DP' then 15            when tFinanceInstitute = 'GoZest-30 DP' then 30 when tFinanceInstitute = 'GoZest-50 DP' then  50 when tFinanceInstitute = 'MHRIL-10 DP' then   10 when tFinanceInstitute = 'MHRIL-20 DP' then   20 when tFinanceInstitute = 'MHRIL-30 DP' then   30 when tFinanceInstitute = 'MHRIL-50 DP' then   50 when tFinanceInstitute = 'MHRIL-50 DP (Onsite)' then   50 when tFinanceInstitute = 'MHRIL-Cafe Plus'    then   15 when tFinanceInstitute = 'MHRIL-Cafe Plus (Dubai)'  then   10 when tFinanceInstitute = 'MHRIL-Cafe Plus (Kuwait)' then 15 when tFinanceInstitute = 'MHRIL-Employee' then 15 when tFinanceInstitute = 'MHRIL-Purple' then 15 when tFinanceInstitute = 'UpZest' then 15 when tFinanceInstitute = 'UpZest-10 DP' then 10 when tFinanceInstitute = 'UpZest-30 DP' then 30 when tFinanceInstitute = 'UpZest-50 DP' then 50 when tFinanceInstitute = 'HDFC' then   100 when tFinanceInstitute = 'HDFC' then   100 when tFinanceInstitute = 'MHRIL- Full payment(No Discount)' then 100 when nPaymentTenure = 0 then 100 when tFinanceInstitute = 'MHRIL-Corporate' then 15 when tFinanceInstitute = 'ICICI' then 15 when tFinanceInstitute = 'MHRIL-Cafe Plus (White and Blue)' then 15 when tFinanceInstitute = 'MHRIL-50 DP (Mumbai)' then 50 when tFinanceInstitute = 'MHRIL-20 DP Special' then 20 when tFinanceInstitute = 'MHRIL-50 DP Special' then 50 when tFinanceInstitute = 'MHRIL-30 DP Special' then 30 when tFinanceInstitute is null then 15 when tFinanceInstitute = 'MHRIL- OLD Contract' then 15 end as DPPlan from cd")


dpp=dpp.withColumn('DP_Bucket',when( (dpp.DPPlan<=10),'0-10%').when((dpp.DPPlan>10) & (dpp.DPPlan<=20),'10-20%').when((dpp.DPPlan>20) & (dpp.DPPlan<=30),'20-30%').when((dpp.DPPlan>30) & (dpp.DPPlan<=40),'30-40%').when((dpp.DPPlan>40) & (dpp.DPPlan<=50),'40-50%').when((dpp.DPPlan>50) & (dpp.DPPlan<=60),'50-60%').when((dpp.DPPlan>60) & (dpp.DPPlan<=70),'60-70%').when((dpp.DPPlan>70) & (dpp.DPPlan<=80),'70-80%').when((dpp.DPPlan>80) & (dpp.DPPlan<=90),'80-90%').when((dpp.DPPlan>90),'90-100%'))

dpp.createOrReplaceTempView('dp')

zfi_reservation_final_Table_2=spark.sql('select zrf1.*,dp.DPPlan,dp.DP_Bucket,dp.Zone from zrf1 left join dp on zrf1.Contract_No=dp.aContractID')

zfi_reservation_final_Table_2.createOrReplaceTempView("zrf2")



# reading Product master file from S3 master tables.

product_master=spark.read.option("delimiter","|").csv("s3://cmh-process-data/master-tables/master_table_product_master/product_master.csv",header =True,inferSchema=True)

product_master=product_master.withColumn('dwLastUpdate',to_date('dwLastUpdate','yyyy-MM-dd'))

product_master.createOrReplaceTempView('p')

zfi_reservation_final_Table_3=spark.sql('select zrf2.*,p.tProductName from zrf2 left join p on zrf2.nProductID=p.aProductMasterID')

zfi_reservation_final_Table_3=zfi_reservation_final_Table_3.withColumn('Member_ID',zfi_reservation_final_Table_3.Member_ID.cast('integer'))

zfi_reservation_final_Table_3.createOrReplaceTempView("zrf3")


# Reading Field App Complete Dump


field_app_complete =spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_field_visit/field_visit.csv",header=True,inferSchema=True)

field_app_complete=field_app_complete.withColumnRenamed('MemberID','Member_ID').withColumnRenamed('InteractionDate','Interaction_Date').withColumnRenamed('VOCLevel1','VOC_level_1').withColumnRenamed('VOCLevel2','VOC_level_2')


field_app_complete=field_app_complete.withColumn('Interaction_Date',to_date('Interaction_Date','yyyy-MM-dd')).withColumn('Member_ID',field_app_complete.Member_ID.cast('Integer'))


#### Apply filter for last 3 months data for example dashboard in running for Jan 2020 the filtered data should be Oct,Nov,Dec 2019.

field_app_last_3_months= field_app_complete.filter('Interaction_Date>= date_add(last_day(now() - interval 04 month),1) and Interaction_Date <= last_day(now() - interval 01 month)')

field_app_select_columns=field_app_last_3_months.select('Member_ID','Interaction_Date','VOC_level_1','VOC_level_2')

field_app_select_columns.createOrReplaceTempView('f')

# Reading Branch Dialer Complete Dump

branch_dialer_complete =spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_branch_dialler/branch_dialler.csv",header=True,inferSchema=True)

branch_dialer_complete=branch_dialer_complete.withColumn('DateofCalling',to_date('DateofCalling','yyyy-MM-dd')).withColumn('Member_ID',branch_dialer_complete.Member_ID.cast('Integer'))

#### Apply filter for last 3 months data for example dashboard in running for Jan 2020 the filtered data should be Oct,Nov,Dec 2019.
branch_dailer_last_3_months= branch_dialer_complete.filter('DateofCalling>= date_add(last_day(now() - interval 04 month),1) and DateofCalling <= last_day(now() - interval 01 month)')

branch_select_columns=branch_dailer_last_3_months.select('Member_ID','DateofCalling','VoC1_Customer_Reason_for_ASFDefault','VoC2_SubReason')

branch_select_columns.createOrReplaceTempView('b')

#branch.repartition(1).write.csv("s3://cmh-process-data/test-1/VOC",mode="append",header="true")


zfi_field_branch=spark.sql("select zrf3.*, f.Interaction_Date,f.VOC_level_1,f.VOC_level_2,b.DateofCalling,b.VoC1_Customer_Reason_for_ASFDefault,b.VoC2_SubReason from zrf3 left join f on zrf3.Member_ID=f.Member_ID left join b on zrf3.Member_ID=b.Member_ID")

zfi_field_branch.createOrReplaceTempView('zfb')


# Mapping the Additional generated columns to main ZFI Table
zfi_reservation_final_Table_4=spark.sql("select s.Contract_No, s.Member_ID, s.Mortgage_ID, s.Status, s.OD_Bucket_Max, s.Mortgage_Check, s.Name,s.Branch, s.EMI_Realized_count, s.Sale_date,  s.Realized_percent_con, s.Sale_vintage, s.EMI_Realization_Bucket, s.Sale_vintage_Bucket, s.ReservationNo, s.RoomNights, s.CheckIn, s.dBookingDate, s.SeasonHolidayed, s.ReservationStatusID, s.nProductID, s.count_HD, s.HD_Nights, s.Last_HD, s.Holiday_bucket, s.Time_since_last_holiday, s.Time_since_last_holiday_bucket, s.Base_Inflow_Type, s.DPPlan, s.DP_Bucket, s.Zone, s.tProductName, s.Interaction_Date, s.VOC_level_1, s.VOC_level_2, s.DateofCalling, s.VoC1_Customer_Reason_for_ASFDefault, s.VoC2_SubReason from (select Contract_No, Member_ID, Mortgage_ID, Status, OD_Bucket_Max, Mortgage_Check, Name,Branch, EMI_Realized_count, Sale_date, Realized_percent_con, Sale_vintage, EMI_Realization_Bucket, Sale_vintage_Bucket, ReservationNo, RoomNights, CheckIn, dBookingDate, SeasonHolidayed, ReservationStatusID, nProductID, count_HD, HD_Nights, Last_HD, Holiday_bucket, Time_since_last_holiday, Time_since_last_holiday_bucket, Base_Inflow_Type, DPPlan, DP_Bucket, Zone, tProductName, Interaction_Date, VOC_level_1, VOC_level_2, DateofCalling, VoC1_Customer_Reason_for_ASFDefault, VoC2_SubReason,row_number() over(partition by Contract_No order by Interaction_Date desc,DateofCalling desc) as rownum1 from zfb) s where rownum1 =1")

zfi_reservation_final_Table_4.createOrReplaceTempView('zrf4')

zfi_reservation_final_Table_4=spark.sql('select zrf4.*, case when Interaction_Date>= DateofCalling or DateofCalling is null then VOC_level_1 else VoC1_Customer_Reason_for_ASFDefault end as final_VOC1, case when Interaction_Date>=DateofCalling or DateofCalling is null then VOC_level_2 else VoC2_SubReason end as final_VOC2 from zrf4')

zfi_reservation_final_Table_4.createOrReplaceTempView('zrf4')

# Reading Cancellation Request Table from S3 master table

cancellation_request=spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_cancellation_request/cancellation_request.csv",header=True,inferSchema=True)

cancellation_request=cancellation_request.withColumn('dw_last_modified_date',to_date('dw_last_modified_date','yyyy-MM-dd'))

cancellation_request.createOrReplaceTempView('creq')

cancellation_request_select_columns=cancellation_request.select('contractID','Memberid','cancellation_trigger','StatusOfRequest')

cancellation_request_select_columns=cancellation_request_select_columns.withColumn('cancellation_trigger',when(cancellation_request_select_columns.cancellation_trigger =='Rescission Cancellation Request','Rescission_Cancellation_Request').otherwise('General_Cancellation')).withColumn('StatusOfRequest',when(cancellation_request_select_columns.StatusOfRequest=='Resolved','Resolved').otherwise('Pending'))

cancellation_request_select_columns.createOrReplaceTempView('cr1')


cancellation_interaction=spark.read.option("delimiter","|").csv(r"s3://cmh-process-data/master-tables/master_table_cancellation_interaction/cancellation_interaction.csv",header=True,inferSchema=True)

cancellation_interaction=cancellation_interaction.withColumn('dw_last_modified_date',to_date('dw_last_modified_date','yyyy-MM-dd'))

cancellation_interaction.createOrReplaceTempView('cinter')

#can_inter_1= can_inter.filter('acontractid != "NULL"')
#

cancellation_interaction_select_columns=cancellation_interaction.select('acontractid','memberid','interaction_state_value')

cancellation_interaction_select_columns=cancellation_interaction_select_columns.withColumn('acontractid',cancellation_interaction_select_columns.acontractid.cast('Integer'))

cancellation_interaction_select_columns=cancellation_interaction_select_columns.withColumnRenamed('acontractid','contractID').withColumnRenamed('memberid','Memberid').withColumnRenamed('interaction_state_value','StatusOfRequest')

cancellation_interaction_select_columns=cancellation_interaction_select_columns.withColumn('cancellation_trigger',lit('Cancellation_Interaction'))

cancellation_interaction_select_columns.createOrReplaceTempView('ci1')

cancellation_interaction_request_join = spark.sql('select contractID,Memberid,cancellation_trigger,StatusOfRequest from cr1 union all select contractID,Memberid,cancellation_trigger,StatusOfRequest from ci1')

cancellation_interaction_request_join=cancellation_interaction_request_join.withColumn('ct_value', when(cancellation_interaction_request_join.cancellation_trigger=='Rescission_Cancellation_Request',20).when(cancellation_interaction_request_join.cancellation_trigger=='General_Cancellation',5).otherwise(1)).withColumn('status_value',when(cancellation_interaction_request_join.StatusOfRequest=='Pending',2).otherwise(1))

cancellation_interaction_request_join=cancellation_interaction_request_join.withColumn('total_value',(cancellation_interaction_request_join.ct_value*cancellation_interaction_request_join.status_value))

cancellation_interaction_request_join.createOrReplaceTempView('cri')

zfi_reservation_final_Table_5=spark.sql('select zrf4.*, cri.cancellation_trigger,cri.StatusOfRequest,cri.total_value from zrf4 left join cri on zrf4.Contract_No=cri.contractID')

zfi_reservation_final_Table_5.createOrReplaceTempView('zrf5')


zfi_reservation_final_Table_6=spark.sql("select s.Contract_No, s.Member_ID, s.Mortgage_ID, s.Status, s.OD_Bucket_Max, s.Mortgage_Check, s.Name,s.Branch, s.EMI_Realized_count, s.Sale_date, s.Realized_percent_con, s.Sale_vintage, s.EMI_Realization_Bucket, s.Sale_vintage_Bucket, s.ReservationNo, s.RoomNights, s.CheckIn, s.dBookingDate, s.SeasonHolidayed, s.ReservationStatusID, s.nProductID, s.count_HD, s.HD_Nights, s.Last_HD, s.Holiday_bucket, s.Time_since_last_holiday, s.Time_since_last_holiday_bucket, s.Base_Inflow_Type, s.DPPlan, s.DP_Bucket, s.Zone, s.tProductName, s.Interaction_Date, s.VOC_level_1, s.VOC_level_2, s.DateofCalling, s.VoC1_Customer_Reason_for_ASFDefault, s.VoC2_SubReason,s.final_VOC1,s.final_VOC2,s.cancellation_trigger,s.StatusOfRequest,s.total_value from (select Contract_No, Member_ID, Mortgage_ID, Status, OD_Bucket_Max, Mortgage_Check, Name,Branch, EMI_Realized_count, Sale_date, Realized_percent_con, Sale_vintage, EMI_Realization_Bucket, Sale_vintage_Bucket, ReservationNo, RoomNights, CheckIn, dBookingDate, SeasonHolidayed, ReservationStatusID, nProductID, count_HD, HD_Nights, Last_HD, Holiday_bucket, Time_since_last_holiday, Time_since_last_holiday_bucket, Base_Inflow_Type, DPPlan, DP_Bucket, Zone, tProductName, Interaction_Date, VOC_level_1, VOC_level_2, DateofCalling, VoC1_Customer_Reason_for_ASFDefault, VoC2_SubReason,final_VOC1,final_VOC2,cancellation_trigger,StatusOfRequest,total_value,row_number() over(partition by Contract_No order by total_value desc) as rownum1 from zrf5) s where rownum1 =1")

zfi_reservation_final_Table_6.createOrReplaceTempView('zrf6')


## Reading Recovery File from S3 master Tables

recovery_file_complete=spark.read.option("delimiter",",").csv(r"s3://cmh-process-data/master-tables/master_table_Recovery/recovery.csv",header=True,inferSchema=True)

recovery_file_complete = recovery_file_complete.withColumn('rec_date',to_date('recovered_month_year','yyyy-MM'))
recovery_file_complete=recovery_file_complete.withColumnRenamed('Contract No.','Contract_No')
recovery_file_complete.createOrReplaceTempView('rf1')

recovery_file_add_offset=spark.sql('select *, date_add(last_day(now()- interval 10 months),1) as offset from rf1')

recovery_file_last_9_month_filter = recovery_file_add_offset.filter("rec_date>=offset")

recovery_file_last_9_month_filter=recovery_file_last_9_month_filter.dropDuplicates(['Contract_No'])

recovery_file_last_9_month_filter=recovery_file_last_9_month_filter.withColumn('flag',lit('last_09_months'))
recovery_file_last_9_month_filter.createOrReplaceTempView('rfl9m')

zfi_reservation_final_Table_7=spark.sql('select zrf6.*,rfl9m.flag from zrf6 left join rfl9m on zrf6.Contract_No=rfl9m.Contract_No')

zfi_reservation_final_Table_7=zfi_reservation_final_Table_7.na.fill("Not_in_9_Months",subset=['flag'])

zfi_reservation_final_Table_7.createOrReplaceTempView('zrf7')


zfi_reservation_final_Table_8=spark.sql('select zrf7.*,case when EMI_Realization_Bucket="0" and Sale_date between date_add(last_day(now()- interval 02 months),1) and last_day(now() - interval 01 month) then "FIF_Failure" when EMI_Realization_Bucket="1_5" and flag = "Not_in_9_Months" then "First_Time_Inflow" when flag="last_09_months" then "Normalised_and_back_in_OD"  else "NA" end as Inflow_type from zrf7')

zfi_reservation_final_Table_8=zfi_reservation_final_Table_8.withColumn('run_date',lit(current_date()))

# add a column Branch logic said by vinod

zfi_reservation_final_Table_8=zfi_reservation_final_Table_8.withColumnRenamed('RoomNights','NoOfNights')

zfi_reservation_final_Table_8.createOrReplaceTempView('zrf8')


# Reading the old intermediate file from S3 
zfi_reservation_old_intermediate_file=spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/VOC_Dashboard/voc_intermediate/voc.csv",header =True,inferSchema=True)

zfi_reservation_old_intermediate_file=zfi_reservation_old_intermediate_file.withColumn('run_date',to_date('run_date','yyyy-MM-dd'))


#zfi_reservation_final_8_old.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/VOC_Dashboard/voc_intermediate",mode="append",header="true")
zfi_reservation_old_intermediate_file.createOrReplaceTempView('zrf8o')

zfi_reservation_final=spark.sql('select * from zrf8o union all select * from zrf8')

zfi_reservation_final=zfi_reservation_final.withColumn('run_date',zfi_reservation_final.run_date.cast('string'))

zfi_reservation_final.repartition(1).write.csv("s3://cmh-process-data/intermediate_files/VOC_Dashboard/voc_intermediate",mode="append",header="true")

table_1 = spark.sql('select "ZFI_065_Table" as Table_name, max(dw_lastupdated) as last_modified, current_date() as run_date from zfi')

table_2 = spark.sql('select "Reservation_Table" as Table_name, max(dwLastUpdatedOn) as last_modified, current_date() as run_date from reservation')

table_A = table_1.unionAll(table_2)

table_3 = spark.sql('select "Base_Table" as Table_name, max(date) as last_modified, current_date() as run_date from b1')

table_B = table_A.unionAll(table_3)

table_4 = spark.sql('select "Inflow_Table" as Table_name, max(dw_lastupdated) as last_modified, current_date() as run_date from zod')

table_C = table_B.unionAll(table_4)

table_5 = spark.sql('select "Contract_table" as Table_name, max(dtLastUpdated) as last_modified, current_date() as run_date from cd')

table_D = table_C.unionAll(table_5)

table_6 = spark.sql('select "Product_Table" as Table_name, max(dwLastUpdate) as last_modified, current_date() as run_date from p')

table_E = table_D.unionAll(table_6)

table_7 = spark.sql('select "Field_App_Table" as Table_name, max(Interaction_Date) as last_modified, current_date() as run_date from f')

table_F = table_E.unionAll(table_7)

table_8 = spark.sql('select "Branch_Dailler_Table" as Table_name, max(DateofCalling) as last_modified, current_date() as run_date from b')

table_G = table_F.unionAll(table_8)

table_9 = spark.sql('select "Cancellation_Request_Table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from creq')

table_H = table_G.unionAll(table_9)

table_10 = spark.sql('select "Cancellation_Interaction_Table" as Table_name, max(dw_last_modified_date) as last_modified, current_date() as run_date from cinter')

table_I = table_H.unionAll(table_10)

table_11 = spark.sql('select "Recovery_Table" as Table_name, max(rec_date) as last_modified, current_date() as run_date from rf1')

table_J = table_I.unionAll(table_11)

table_J.createOrReplaceTempView('tf')

#table_J.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/VOC_Dashboard/Table_details",mode="append",header="true")


voc_table_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/VOC_Dashboard/Table_details/table_details.csv",header =True,inferSchema=True)

voc_table_old=voc_table_old.withColumn('last_modified',to_date('last_modified','yyyy-MM-dd')).withColumn('run_date',to_date('run_date','yyyy-MM-dd'))

voc_table_old.createOrReplaceTempView('nto')

voc_table_final= spark.sql('select * from nto union all select * from tf')

voc_table_final=voc_table_final.withColumn('last_modified',voc_table_final.last_modified.cast('string')).withColumn('run_date',voc_table_final.run_date.cast('string'))

voc_table_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/VOC_Dashboard/Table_details",mode="append",header="true")

time.sleep(30)
sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/VOC_Dashboard/voc_intermediate/voc.csv"
os.system(cmd1)
time.sleep(10)
cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/VOC_Dashboard/Table_details/table_details.csv"
os.system(cmd2)
time.sleep(10)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
time.sleep(10)


file_path_voc = "s3://cmh-process-data/intermediate_files/VOC_Dashboard/voc_intermediate/"
time.sleep(10)
file_path_Table_details = "s3://cmh-process-data/intermediate_files/VOC_Dashboard/Table_details/"
time.sleep(10)

created_file_path_voc = fs.globStatus(Path(file_path_voc + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)

fs.rename(created_file_path_voc,Path(file_path_voc + "voc.csv"))
time.sleep(10)
fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))




from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
conf = SparkConf()

from pyspark.sql.functions import *
from pyspark.sql import Window


#### Zfi_65  ####

#zfi_65_1_feb =spark.read.option("delimiter","|").csv(r"D:\Shashank\Club Mahindra\S3 data\Zfi065\ZFI065N_OUTPUT_28.02.2019_28.02.2019_08_30_04.csv",header=True,inferSchema=True)


# S3 path

zfi_65_1_feb =spark.read.option("delimiter","|").csv(r"s3://cmh-raw-data/complete-dump/zfi065n/ZFI065N_OUTPUT_28.02.2019_28.02.2019_08_30_04.csv",header=True,inferSchema=True)


zfi_65_1_feb=zfi_65_1_feb.withColumnRenamed('Contract No.','Contract_No').withColumnRenamed('Mortgage ID','Mortgage_ID').withColumnRenamed('OD Bucket Max','OD_Bucket_Max').withColumnRenamed('Mortgage Check','Mortgage_Check').withColumnRenamed('EMI Realized count','EMI_Realized_count').withColumnRenamed('Sale date','Sale_date').withColumnRenamed('Member ID','Member_ID').withColumnRenamed('Realized Percent Con','Realized_percent_con')

zfi_65_1_feb=zfi_65_1_feb.withColumn('Sale_date',to_date('Sale_date','dd.MM.yyyy'))

zfi_65_1_feb=zfi_65_1_feb.select('Contract_No','Member_ID','Mortgage_ID','Status','OD_Bucket_Max','Mortgage_Check','Name','EMI_Realized_count','Sale_date','Branch','Realized_percent_con')


zfi_filter= zfi_65_1_feb.filter("Mortgage_Check==1 and Status =='Active' and OD_Bucket_Max is not null")



zfi_filter=zfi_filter.withColumn('Sale_vintage',(datediff(lit('2019-02-01'),zfi_filter.Sale_date))/30.5)
zfi_filter=zfi_filter.withColumn('Sale_vintage',ceil('Sale_vintage'))


zfi_filter=zfi_filter.withColumn('EMI_Realization_Bucket',when(zfi_filter.EMI_Realized_count==0,'0').when(zfi_filter.EMI_Realized_count >5,'>5').otherwise('1_5'))

zfi_filter=zfi_filter.withColumn('Sale_vintage_Bucket',when((zfi_filter.Sale_vintage>= 0) & (zfi_filter.Sale_vintage<=3),'0-3M').when((zfi_filter.Sale_vintage>= 4) & (zfi_filter.Sale_vintage<=12),'4-12M').otherwise('>12'))


Zfi_filter=zfi_filter.withColumn('RZ_Bucket',when( (zfi_filter.Realized_percent_con<=10),'0-10%').when((zfi_filter.Realized_percent_con>10) & (zfi_filter.Realized_percent_con<=20),'10-20%').when((zfi_filter.Realized_percent_con>20) & (zfi_filter.Realized_percent_con<=30),'20-30%').when((zfi_filter.Realized_percent_con>30) & (zfi_filter.Realized_percent_con<=40),'30-40%').when((zfi_filter.Realized_percent_con>40) & (zfi_filter.Realized_percent_con<=50),'40-50%').when((zfi_filter.Realized_percent_con>50) & (zfi_filter.Realized_percent_con<=60),'50-60%').when((zfi_filter.Realized_percent_con>60) & (zfi_filter.Realized_percent_con<=70),'60-70%').when((zfi_filter.Realized_percent_con>70) & (zfi_filter.Realized_percent_con<=80),'70-80%').when((zfi_filter.Realized_percent_con>80) & (zfi_filter.Realized_percent_con<=90),'80-90%').when((zfi_filter.Realized_percent_con>90),'90-100%'))

zfi_filter.createOrReplaceTempView('zf')


## Holiday###


#reservation =spark.read.option("delimiter","|").csv(r"D:\Shashank\Club Mahindra\S3 data\Reservation\Reservation Done.csv",header=True,inferSchema=True)

# S3 path

reservation =spark.read.option("delimiter","|").csv(r"s3://cmh-raw-data/complete-dump/reservation done/reservation done_22062019.csv",header=True,inferSchema=True)

## take latest seasoned Holiday
reservation_sel=reservation.select('ReservationNo','nContractID','NoOfNights','CheckIn','dBookingDate','SeasonHolidayed','ReservationStatusID','nProductID')

# change Dynamic check between taking outer values
reservation_filter=reservation_sel.filter('dBookingDate < date_add(last_day("2019-02-15"- interval 01 months),1) and CheckIn<date_add(last_day("2019-02-15"- interval 01 months),1) and ReservationStatusID between 21 and 24')

reservation_filter.createOrReplaceTempView('rf')

zfi_reservation=spark.sql('select zf.*,rf.* from zf left join rf on zf.Contract_No=rf.nContractID')

zfi_reservation.createOrReplaceTempView('zr')

zfi_reservation_summ=spark.sql('select Contract_No,count(ReservationNo) as count_HD, sum(case when NoOfNights>=1 then NoOfNights else 0 end) as HD_Nights,max(CheckIn) as Last_HD from zr group by Contract_No order by Contract_No')

zfi_reservation_summ.createOrReplaceTempView('zrs')


zfi_reservation_1 =zfi_reservation.dropDuplicates(['Contract_No'])

zfi_reservation_1.createOrReplaceTempView('zr1')


zfi_reservation_final=spark.sql('select zr1.*,zrs.count_HD, zrs.HD_Nights, zrs.Last_HD from zr1 left join zrs on zr1.Contract_No=zrs.Contract_No')

zfi_reservation_final=zfi_reservation_final.withColumn('Holiday_bucket',when(zfi_reservation_final.HD_Nights==0,'0_HD').when(zfi_reservation_final.HD_Nights==1,'1_HD').when(zfi_reservation_final.HD_Nights==2,'2_HD').when(zfi_reservation_final.HD_Nights==3,'3_HD').when(zfi_reservation_final.HD_Nights==4,'4_HD').when(zfi_reservation_final.HD_Nights==5,'5_HD').when(zfi_reservation_final.HD_Nights==6,'6_HD').when(zfi_reservation_final.HD_Nights>=7,'7_HD_+'))


# Change Dynamic
zfi_reservation_final=zfi_reservation_final.withColumn('Time_since_last_holiday',round((datediff(lit("2019-02-01"),zfi_reservation_final.Last_HD))/30.5,2))

zfi_reservation_final=zfi_reservation_final.withColumn('Time_since_last_holiday_bucket', when(zfi_reservation_final.Time_since_last_holiday<6,'<_6').when((zfi_reservation_final.Time_since_last_holiday>=6 )&(zfi_reservation_final.Time_since_last_holiday<=12),'6_12').when((zfi_reservation_final.Time_since_last_holiday>12)&(zfi_reservation_final.Time_since_last_holiday<18),'13_18').when((zfi_reservation_final.Time_since_last_holiday>=18)&(zfi_reservation_final.Time_since_last_holiday<24),'18_24').when(zfi_reservation_final.Time_since_last_holiday>24,'24>').otherwise('NO_HD'))



zfi_reservation_final.createOrReplaceTempView('zrf')


##

#base=spark.read.option("delimiter",",").csv(r"D:\Shashank\Club Mahindra\S3 data\inflow_sheet_emi\Feb_2019_OD_Base.csv",header=True,inferSchema=True)

# S3 Path

base=spark.read.option("delimiter",",").csv(r"s3://cmh-process-data/test-1/Feb_2019_OD_Base.csv",header=True,inferSchema=True)


for col in base.columns:
	base=base.withColumnRenamed(col,col.replace(" ", "_"))

base=base.withColumnRenamed('Contract_No.','Contract_No').withColumnRenamed('New_Branch','ControlLocation')

base=base.withColumn('flag',lit("Base"))

base.createOrReplaceTempView('b')



#inflow=spark.read.option("delimiter","|").csv(r"D:\Shashank\Club Mahindra\S3 data\inflow_sheet_emi\inflow Feb 19.csv",header=True,inferSchema=True)


# S3 path

inflow=spark.read.option("delimiter","|").csv(r"s3://cmh-raw-data/complete-dump/inflow-sheet-emi/inflow Feb 19.csv",header=True,inferSchema=True)

for col in inflow.columns:
	inflow=inflow.withColumnRenamed(col,col.replace(" ", "_"))

inflow=inflow.withColumnRenamed('Contract_No.','Contract_No').withColumnRenamed('New_Branch','ControlLocation')

inflow=inflow.withColumn('flag',lit("Inflow"))

inflow.createOrReplaceTempView("if")

base_inflow=spark.sql('select distinct Contract_No, flag,2 as weight from b union all select distinct Contract_No,flag,1 as weight from if')


base_inflow.createOrReplaceTempView("bi")


base_inflow_final=spark.sql('select k.Contract_No,k.flag from (select Contract_No,flag,weight,row_number() over(partition by Contract_No order by weight desc) as rownum1 from bi) k where rownum1 =1')

base_inflow_final.createOrReplaceTempView("bif")



zfi_reservation_final_1= spark.sql('select zrf.*,bif.flag as Base_Inflow_Type from zrf left join bif on zrf.Contract_No=bif.Contract_No')

zfi_reservation_final_1.createOrReplaceTempView("zrf1")

## DPP ### ## Product ID and zone ## 

#contract=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\Contract data\Contract data_11042019.csv",header =True,inferSchema=True)


#S3 path

contract=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/contract-data/Contract data_11042019.csv",header =True,inferSchema=True)

contract_dp = contract.select('aContractID','tFinanceInstitute','nPaymentTenure','Zone')

contract_dp.createOrReplaceTempView('cd')

dpp=spark.sql("select aContractID,Zone, Case when tFinanceInstitute = 'Bliss-50 DP' then 50 when tFinanceInstitute = 'GoZest-15 DP' then 15            when tFinanceInstitute = 'GoZest-30 DP' then 30 when tFinanceInstitute = 'GoZest-50 DP' then  50 when tFinanceInstitute = 'MHRIL-10 DP' then   10 when tFinanceInstitute = 'MHRIL-20 DP' then   20 when tFinanceInstitute = 'MHRIL-30 DP' then   30 when tFinanceInstitute = 'MHRIL-50 DP' then   50 when tFinanceInstitute = 'MHRIL-50 DP (Onsite)' then   50 when tFinanceInstitute = 'MHRIL-Cafe Plus'    then   15 when tFinanceInstitute = 'MHRIL-Cafe Plus (Dubai)'  then   10 when tFinanceInstitute = 'MHRIL-Cafe Plus (Kuwait)' then 15 when tFinanceInstitute = 'MHRIL-Employee' then 15 when tFinanceInstitute = 'MHRIL-Purple' then 15 when tFinanceInstitute = 'UpZest' then 15 when tFinanceInstitute = 'UpZest-10 DP' then 10 when tFinanceInstitute = 'UpZest-30 DP' then 30 when tFinanceInstitute = 'UpZest-50 DP' then 50 when tFinanceInstitute = 'HDFC' then   100 when tFinanceInstitute = 'HDFC' then   100 when tFinanceInstitute = 'MHRIL- Full payment(No Discount)' then 100 when nPaymentTenure = 0 then 100 when tFinanceInstitute = 'MHRIL-Corporate' then 15 when tFinanceInstitute = 'ICICI' then 15 when tFinanceInstitute = 'MHRIL-Cafe Plus (White and Blue)' then 15 when tFinanceInstitute = 'MHRIL-50 DP (Mumbai)' then 50 when tFinanceInstitute = 'MHRIL-20 DP Special' then 20 when tFinanceInstitute = 'MHRIL-50 DP Special' then 50 when tFinanceInstitute = 'MHRIL-30 DP Special' then 30 when tFinanceInstitute is null then 15 when tFinanceInstitute = 'MHRIL- OLD Contract' then 15 end as DPPlan from cd")


dpp=dpp.withColumn('DP_Bucket',when( (dpp.DPPlan<=10),'0-10%').when((dpp.DPPlan>10) & (dpp.DPPlan<=20),'10-20%').when((dpp.DPPlan>20) & (dpp.DPPlan<=30),'20-30%').when((dpp.DPPlan>30) & (dpp.DPPlan<=40),'30-40%').when((dpp.DPPlan>40) & (dpp.DPPlan<=50),'40-50%').when((dpp.DPPlan>50) & (dpp.DPPlan<=60),'50-60%').when((dpp.DPPlan>60) & (dpp.DPPlan<=70),'60-70%').when((dpp.DPPlan>70) & (dpp.DPPlan<=80),'70-80%').when((dpp.DPPlan>80) & (dpp.DPPlan<=90),'80-90%').when((dpp.DPPlan>90),'90-100%'))

dpp.createOrReplaceTempView('dp')

zfi_reservation_final_2=spark.sql('select zrf1.*,dp.DPPlan,dp.DP_Bucket,dp.Zone from zrf1 left join dp on zrf1.Contract_No=dp.aContractID')

zfi_reservation_final_2.createOrReplaceTempView("zrf2")



#product=spark.read.option("delimiter","|").csv("D:\Shashank\Club Mahindra\S3 data\Product Master\Product_master_08042019.csv",header =True,inferSchema=True)

# S3 Path

product=spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/ProductMaster/Product_master_08042019.csv",header =True,inferSchema=True)

product.createOrReplaceTempView('p')

zfi_reservation_final_3=spark.sql('select zrf2.*,p.tProductName from zrf2 left join p on zrf2.nProductID=p.aProductMasterID')

zfi_reservation_final_3=zfi_reservation_final_3.withColumn('Member_ID',zfi_reservation_final_3.Member_ID.cast('integer'))

zfi_reservation_final_3.createOrReplaceTempView("zrf3")


### Voc Dump##

#field_app =spark.read.option("delimiter","|").csv(r"D:\Shashank\Club Mahindra\S3 data\Field_App\Field visit App_16042019.csv",header=True,inferSchema=True)


# S3 path

field_app =spark.read.option("delimiter","|").csv(r"s3://cmh-raw-data/complete-dump/field-visit-app/Field visit App_16042019.csv",header=True,inferSchema=True)

field_app=field_app.withColumnRenamed('Member ID','Member_ID').withColumnRenamed('VOC level 1','VOC_level_1').withColumnRenamed('VOC level 2','VOC_level_2').withColumnRenamed('Interaction Date','Interaction_Date')

#field_app.repartition(1).write.csv("s3://cmh-process-data/test-1/VOC",mode="append",header="true")


field_app=field_app.withColumn('Interaction_Date',to_date('Interaction_Date','dd/MM/yyyy')).withColumn('Member_ID',field_app.Member_ID.cast('Integer'))

field_app=field_app.select('Member_ID','Interaction_Date','VOC_level_1','VOC_level_2')

field_app.createOrReplaceTempView('f')

#branch =spark.read.option("delimiter","|").csv(r"D:\Shashank\Club Mahindra\S3 data\branch dialler data\Branch dialer_12042019.csv",header=True,inferSchema=True)

# S3 path

branch =spark.read.option("delimiter","|").csv(r"s3://cmh-raw-data/complete-dump/branch-dialer/Branch dialer_12042019.csv",header=True,inferSchema=True)


branch=branch.withColumn('DateofCalling',to_date('DateofCalling','dd/MM/yyyy')).withColumn('Member_ID',branch.Member_ID.cast('Integer'))

branch=branch.select('Member_ID','DateofCalling','VoC1_Customer_Reason_for_ASFDefault','VoC2_SubReason')

branch.createOrReplaceTempView('b')

#branch.repartition(1).write.csv("s3://cmh-process-data/test-1/VOC",mode="append",header="true")


zfi_field_branch=spark.sql("select zrf3.*, f.Interaction_Date,f.VOC_level_1,f.VOC_level_2,b.DateofCalling,b.VoC1_Customer_Reason_for_ASFDefault,b.VoC2_SubReason from zrf3 left join f on zrf3.Member_ID=f.Member_ID left join b on zrf3.Member_ID=b.Member_ID")

zfi_field_branch.createOrReplaceTempView('zfb')


# change check patition MemberID and ContractID
zfi_reservation_final_4=spark.sql("select s.Contract_No, s.Member_ID, s.Mortgage_ID, s.Status, s.OD_Bucket_Max, s.Mortgage_Check, s.Name, s.EMI_Realized_count, s.Sale_date, s.Branch, s.Realized_percent_con, s.Sale_vintage, s.EMI_Realization_Bucket, s.Sale_vintage_Bucket, s.ReservationNo, s.nContractID, s.NoOfNights, s.CheckIn, s.dBookingDate, s.SeasonHolidayed, s.ReservationStatusID, s.nProductID, s.count_HD, s.HD_Nights, s.Last_HD, s.Holiday_bucket, s.Time_since_last_holiday, s.Time_since_last_holiday_bucket, s.Base_Inflow_Type, s.DPPlan, s.DP_Bucket, s.Zone, s.tProductName, s.Interaction_Date, s.VOC_level_1, s.VOC_level_2, s.DateofCalling, s.VoC1_Customer_Reason_for_ASFDefault, s.VoC2_SubReason from (select Contract_No, Member_ID, Mortgage_ID, Status, OD_Bucket_Max, Mortgage_Check, Name, EMI_Realized_count, Sale_date, Branch, Realized_percent_con, Sale_vintage, EMI_Realization_Bucket, Sale_vintage_Bucket, ReservationNo, nContractID, NoOfNights, CheckIn, dBookingDate, SeasonHolidayed, ReservationStatusID, nProductID, count_HD, HD_Nights, Last_HD, Holiday_bucket, Time_since_last_holiday, Time_since_last_holiday_bucket, Base_Inflow_Type, DPPlan, DP_Bucket, Zone, tProductName, Interaction_Date, VOC_level_1, VOC_level_2, DateofCalling, VoC1_Customer_Reason_for_ASFDefault, VoC2_SubReason,row_number() over(partition by Contract_No order by Interaction_Date desc,DateofCalling desc) as rownum1 from zfb) s where rownum1 =1")

zfi_reservation_final_4.createOrReplaceTempView('zrf4')

zfi_reservation_final_4=spark.sql('select zrf4.*, case when Interaction_Date>= DateofCalling or DateofCalling is null then VOC_level_1 else VoC1_Customer_Reason_for_ASFDefault end as final_VOC1, case when Interaction_Date>=DateofCalling or DateofCalling is null then VOC_level_2 else VoC2_SubReason end as final_VOC2 from zrf4')

zfi_reservation_final_4.createOrReplaceTempView('zrf4')

zfi_reservation_final_4.repartition(1).write.csv("s3://cmh-process-data/test-1/VOC",mode="append",header="true")


# cancellation Interaction and cancellation Request

can_req=spark.read.option("delimiter","|").csv(r"s3://cmh-raw-data/complete-dump/cancellation-request/Cancellation request_11042019.csv",header=True,inferSchema=True)


can_req_1=can_req.select('contractID','Memberid','cancellation_trigger','StatusOfRequest')

can_req_1=can_req_1.withColumn('cancellation_trigger',when(can_req_1.cancellation_trigger =='Rescission Cancellation Request','Rescission_Cancellation_Request').otherwise('General_Cancellation')).withColumn('StatusOfRequest',when(can_req_1.StatusOfRequest=='Resolved','Resolved').otherwise('Pending'))

can_req_1.createOrReplaceTempView('cr1')


can_inter=spark.read.option("delimiter","|").csv(r"s3://cmh-raw-data/complete-dump/cancellation interaction/Cancellation Interaction.csv",header=True,inferSchema=True)

can_inter_1= can_inter.filter(can_inter.acontractid <>"NULL")

can_inter_1=can_inter_1.select('acontractid','memberid','interaction_state_value')

can_inter_1=can_inter_1.withColumn('acontractid',can_inter_1.acontractid.cast('Integer'))

can_inter_1=can_inter_1.withColumnRenamed('acontractid','contractID').withColumnRenamed('memberid','Memberid').withColumnRenamed('interaction_state_value','StatusOfRequest')

can_inter_1=can_inter_1.withColumn('cancellation_trigger',lit('Cancellation_Interaction'))

can_inter_1.createOrReplaceTempView('ci1')

can_req_inter = spark.sql('select contractID,Memberid,cancellation_trigger,StatusOfRequest from cr1 union all select contractID,Memberid,cancellation_trigger,StatusOfRequest from ci1')

can_req_inter=can_req_inter.withColumn('ct_value', when(can_req_inter.cancellation_trigger=='Rescission_Cancellation_Request',20).when(can_req_inter.cancellation_trigger=='General_Cancellation',5).otherwise(1)).withColumn('status_value',when(can_req_inter.StatusOfRequest=='Pending',2).otherwise(1))

can_req_inter=can_req_inter.withColumn('total_value',(can_req_inter.ct_value*can_req_inter.status_value))

can_req_inter.createOrReplaceTempView('cri')

zfi_reservation_final_5=spark.sql('select zrf4.*, cri.cancellation_trigger,cri.StatusOfRequest,cri.total_value from zrf4 left join cri on zrf4.Contract_No=cri.contractID')

zfi_reservation_final_5.createOrReplaceTempView('zrf5')


zfi_reservation_final_6=spark.sql("select s.Contract_No, s.Member_ID, s.Mortgage_ID, s.Status, s.OD_Bucket_Max, s.Mortgage_Check, s.Name, s.EMI_Realized_count, s.Sale_date, s.Branch, s.Realized_percent_con, s.Sale_vintage, s.EMI_Realization_Bucket, s.Sale_vintage_Bucket, s.ReservationNo, s.NoOfNights, s.CheckIn, s.dBookingDate, s.SeasonHolidayed, s.ReservationStatusID, s.nProductID, s.count_HD, s.HD_Nights, s.Last_HD, s.Holiday_bucket, s.Time_since_last_holiday, s.Time_since_last_holiday_bucket, s.Base_Inflow_Type, s.DPPlan, s.DP_Bucket, s.Zone, s.tProductName, s.Interaction_Date, s.VOC_level_1, s.VOC_level_2, s.DateofCalling, s.VoC1_Customer_Reason_for_ASFDefault, s.VoC2_SubReason,s.final_VOC1,s.final_VOC2,s.cancellation_trigger,s.StatusOfRequest,s.total_value from (select Contract_No, Member_ID, Mortgage_ID, Status, OD_Bucket_Max, Mortgage_Check, Name, EMI_Realized_count, Sale_date, Branch, Realized_percent_con, Sale_vintage, EMI_Realization_Bucket, Sale_vintage_Bucket, ReservationNo, NoOfNights, CheckIn, dBookingDate, SeasonHolidayed, ReservationStatusID, nProductID, count_HD, HD_Nights, Last_HD, Holiday_bucket, Time_since_last_holiday, Time_since_last_holiday_bucket, Base_Inflow_Type, DPPlan, DP_Bucket, Zone, tProductName, Interaction_Date, VOC_level_1, VOC_level_2, DateofCalling, VoC1_Customer_Reason_for_ASFDefault, VoC2_SubReason,final_VOC1,final_VOC2,cancellation_trigger,StatusOfRequest,total_value,row_number() over(partition by Contract_No order by total_value desc) as rownum1 from zrf5) s where rownum1 =1")

zfi_reservation_final_6.createOrReplaceTempView('zrf6')


## Inflow Type

recovery_jan= spark.read.option("delimiter",",").csv("s3://cmh-process-data/test-1/Recovery/Jan Recovery.csv",header=True,inferSchema=True)

recovery_jan=recovery_jan.withColumn('Mon_year',lit('01-2019'))
recovery_jan=recovery_jan.withColumn('rec_date',to_date('Mon_year','MM-yyyy'))

recovery_jan=recovery_jan.withColumnRenamed('Contract_id','Contract_No')

recovery_jan.createOrReplaceTempView('rj')


recovery_file= spark.read.option("delimiter",",").csv(r"s3://cmh-process-data/test-1/Recovery/Recoveries and Inflow 20190322.csv",header=True,inferSchema=True)

recovery_file=recovery_file.withColumn('Mon_year',concat(recovery_file.Month,lit('-'),recovery_file.Year))

recovery_file=recovery_file.withColumn('rec_date',to_date('Mon_year','MM-yyyy'))

recovery_file=recovery_file.withColumnRenamed('Contract No.','Contract_No')

recovery_file.createOrReplaceTempView('rf1')


recovery_file_1=spark.sql('select Contract_No,Mon_year,rec_date from rj union all select Contract_No, Mon_year, rec_date from rf1')

recovery_file_1.createOrReplaceTempView('rf')

recovery_file_09=spark.sql('select *, date_add(last_day("2019-02-14"- interval 10 months),1) as offset from rf')

recovery_file_last_9_mo = recovery_file_09.filter("rec_date>=offset")

recovery_file_last_9_mo=recovery_file_last_9_mo.dropDuplicates(['Contract_No'])

recovery_file_last_9_mo=recovery_file_last_9_mo.withColumn('flag',lit('last_09_months'))


recovery_file_last_9_mo.createOrReplaceTempView('rfl9m')

zfi_reservation_final_7=spark.sql('select zrf6.*,rfl9m.flag from zrf6 left join rfl9m on zrf6.Contract_No=rfl9m.Contract_No')

zfi_reservation_final_7=zfi_reservation_final_7.na.fill("No_in_9_Months",subset=['flag'])

zfi_reservation_final_7.createOrReplaceTempView('zrf7')


zfi_reservation_final_7=spark.sql('select zrf7.*,case when EMI_Realization_Bucket="0" and Sale_date between date_add(last_day("2019-02-15"- interval 02 months),1) and last_day("2019-02-15" - interval 01 month) then "FIF_Failure" when EMI_Realization_Bucket="1_5" and flag = "No_in_9_Months" then "First_Time_Inflow" when flag="last_09_months" then "Normalised_and_back_in_OD"  else "NA" end as Inflow_type from zrf7')


zfi_reservation_final_7.repartition(1).write.csv("s3://cmh-process-data/test-1/VOC",mode="append",header="true")



from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)
spark = SparkSession.builder.appName('Club_Mahindra_DashBoard').getOrCreate()

from pyspark.sql.functions import *
from pyspark.sql import functions as sf
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import IntegerType
import datetime,time

field = spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/field-visit-app/field visit app_Till 31072019.csv",inferSchema=True,header=True)

#change convert InteractionDate to date from timestamp
	
#for col in field.columns:
#    field = field.withColumnRenamed(col,col.replace(" ","_"))

field = field.withColumn("Visit_Date",to_date("InteractionDate","yyyy-MM-dd")).withColumn("InteractionDate",to_date("InteractionDate","yyyy-MM-dd")).withColumn('dw_lastupdated',to_date('dw_lastupdated','yyyy-MM-dd'))
	 	 
# field = field.filter('Visit_Date >= date_add(last_day(now() - interval 02 months),1) and Visit_Date <= last_day(now() - interval 01 month)')

field.createOrReplaceTempView('fi')	 
field = field.filter('Visit_Date >= date_add(last_day(now() - interval 01 months),1) and Visit_Date <= last_day(now())')	 
	
field = field.filter('InteractionDate is not null')

field1 = field.withColumn('Executive',substring('ExecutiveName',-2,2))

field1 = field1.withColumn("Executive_Type",when(field1.Executive == "PE","PE").when(field1.Executive == "CE","CE").otherwise("-NA"))

field1 = field1.withColumn("Executive_App",when(field1.Executive_Type == "-NA","-NA").otherwise(""))

field1 = field1.withColumn('ExecutiveName2',sf.concat(sf.col('ExecutiveName'),sf.lit(''),sf.col('Executive_App')))  

field1 = field1.withColumn('name',split('ExecutiveName2','-'))

field1 = field1.withColumn('Exe_Name',field1['name'].getItem(0))

field1 = field1.withColumn('branch',field1['name'].getItem(1))

field1 = field1.withColumn('branch1',field1['name'].getItem(2))

field1 = field1.drop('name')

field1.createOrReplaceTempView('field1')

field2 = sqlContext.sql('select *,case when branch1 in ("CE","PE","NA") then " " else branch1 end as branch2 from field1')

field1.createOrReplaceTempView('field1')

field3 = field2.withColumn('branchfinal',sf.concat(sf.col('branch'),sf.lit('_'),sf.col('branch2')))

field3 = field3.select('ExecutiveName','MemberName','MemberID','NoofEMIs','dw_lastupdated','Visit_Date','Executive','Executive_Type','Executive_App','ExecutiveName2','Exe_Name','branch','branch1','branch2','branchfinal')

field3 = field3.withColumn("Member_Key",concat("Exe_Name",lit("-"),"MemberID",lit("-"),"Visit_Date"))

field3 = field3.withColumn("NoofEMIs",field3["NoofEMIs"].cast(IntegerType()))

field3.createOrReplaceTempView('field3')

# earlier query was wrong #field4 = sqlContext.sql('select branchfinal,Visit_Date, count(Distinct Visit_Date) as visitdate from field3 group by branchfinal,Visit_Date')

field4 = sqlContext.sql('select branchfinal, count(Distinct Visit_Date) as visitdate from field3 group by branchfinal')

field4.createOrReplaceTempView('field4')

#field5 = sqlContext.sql('select branchfinal,count(branchfinal) as visitdate from field4 where branchfinal is not null group by branchfinal order by branchfinal')

field5 = sqlContext.sql('select branchfinal, visitdate from field4 where branchfinal is not null order by branchfinal')

field5.createOrReplaceTempView('field5')

#mapping = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal="LCKNW_ " then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal = "PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from field5')

#change for lucknow

mapping = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal in ("LCKNW_ ","LCKN_ ") then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal = "PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from field5')

mapping = mapping.select('ControlLocation','Visitdate')

mapping.createOrReplaceTempView('mapp')

mapping = sqlContext.sql('select ControlLocation,sum(Visitdate) as Visitdate from mapp group by ControlLocation order by ControlLocation')

mapping.createOrReplaceTempView('mapp')

# loading manforce sheet file
# append and make it dynamic

map = spark.read.option("delimiter","|").csv("s3://cmh-raw-data/complete-dump/manforce-sheet/manforce -sheet Jul .csv",inferSchema=True,header=True)

map=map.withColumn('Date',to_date('Date','MMM-yy'))

for col in map.columns:
     map = map.withColumnRenamed(col,col.replace(" ","_"))

map = map.withColumnRenamed("No._of_PEs_","No_of_PEs_").withColumnRenamed("No._of_CEs_","No_of_CEs_")

#map = map.withColumn("Branch",regexp_replace(col("Branch"),"�",""))

map.createOrReplaceTempView('map')

emimap = sqlContext.sql('select mapp.ControlLocation,mapp.visitdate,map.No_of_PEs_, map.No_of_CEs_ from mapp left join map on mapp.ControlLocation = map.Branch')

emimap.createOrReplaceTempView('emi')

emipece = sqlContext.sql('select branchfinal,count(Distinct (case when Executive_Type="PE" then Exe_name end)) as No_of_PEs_using_the_app, count(Distinct(case when Executive_Type="CE" then Exe_name end)) as No_of_CEs_using_the_app, count(Distinct(case when Executive_Type="-NA" then Exe_name end)) as Executive_Type_Unknown from field3 where branchfinal is not null group by branchfinal order by branchfinal')

emipece.createOrReplaceTempView('emipece')

#######

mappingemi = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal in ("LCKNW_ ","LCKN_ ") then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal="PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from emipece')

mappingemi = mappingemi.select('ControlLocation','No_of_PEs_using_the_app','No_of_CEs_using_the_app','Executive_Type_Unknown')

mappingemi.createOrReplaceTempView('mappemi')

mappingemi = sqlContext.sql('select ControlLocation,sum(No_of_PEs_using_the_app) as No_of_PEs_using_the_app, sum(No_of_CEs_using_the_app) as No_of_CEs_using_the_app, sum(Executive_Type_Unknown) as Executive_Type_Unknown from mappemi group by ControlLocation')

mappingemi.createOrReplaceTempView('mappemi')

emimap.createOrReplaceTempView('emi')

emi1 = sqlContext.sql('select emi.*,mappemi.No_of_PEs_using_the_app,mappemi.No_of_CEs_using_the_app,mappemi.Executive_Type_Unknown from emi left join mappemi on emi.ControlLocation=mappemi.ControlLocation')

emi1.createOrReplaceTempView('emi1')

emi1 = sqlContext.sql('select *, round((100*(No_of_PEs_using_the_app+No_of_CEs_using_the_app)/(No_of_PEs_+No_of_CEs_)),2) as App_Adoption_per from emi1 ')

emi1.createOrReplaceTempView('emi1')

# NEW LOGIC for deriving the PE & CE columns

# NEW CODE FOR DERIVING THE LAST 9 COLUMNS

# 1 of visit column for the day

# dynamic => code per = sqlContext.sql('select branchfinal , Executive_Type, count(distinct Member_Key) as visits from field3 where Visit_Date = Now() - interval 01 day group by branchfinal,Executive_Type')

per = sqlContext.sql('select branchfinal , Executive_Type, count(distinct Member_Key) as visits from field3 where Visit_Date = "2019-07-09" group by branchfinal,Executive_Type')

per.createOrReplaceTempView('per')

## mapping of branchfinal column of per dataframe

permap = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal in ("LCKNW_ ","LCKN_ ") then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal="PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from per')

permap = permap.groupBy("ControlLocation").pivot("Executive_Type").sum("visits")

permap = permap.na.fill(0)

permap = permap.withColumnRenamed("-NA","NA")

permap.createOrReplaceTempView('permap')

# grouping of similar controllocation

permap = sqlContext.sql('select ControlLocation, sum(NA) as NA, sum(PE) as PE, sum(CE) as CE  from permap group by ControlLocation order by ControlLocation')

permap.createOrReplaceTempView('permap')

emi1 = sqlContext.sql('select emi1.*, permap.PE as Visits_PE_day, permap.CE as Visits_CE_day from emi1 left join permap on emi1.ControlLocation = permap.ControlLocation')

emi1 = emi1.na.fill(0)

emi1.createOrReplaceTempView('emi1')

## deriving the of visits column for the day

emi1 = sqlContext.sql('select emi1.*,(Visits_PE_day/No_of_PEs_using_the_app) as of_visits_per_day_pe, (Visits_CE_day/No_of_CEs_using_the_app) as of_visits_per_day_ce from emi1')

emi1 = emi1.na.fill(0)

emi1.createOrReplaceTempView('emi1')


#### 2 deriving the collections columns for the day

# dynamic code ==> collectionday = sqlContext.sql('select branchfinal , Executive_Type, count(distinct Member_Key) as visits from field3 where Visit_Date = Now() - interval 01 day and NoofEMIs > 0 group by branchfinal,Executive_Type')

collectionday = sqlContext.sql('select branchfinal , Executive_Type, count(distinct Member_Key) as visits from field3 where Visit_Date = "2019-07-09" and NoofEMIs > 0 group by branchfinal,Executive_Type')

collectionday.createOrReplaceTempView('cold')

# collectionday mapping

collmap = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal in ("LCKNW_ ","LCKN_ ") then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal="PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from cold')

collmap = collmap.groupBy("ControlLocation").pivot("Executive_Type").sum("visits")

collmap = collmap.na.fill(0)

collmap = collmap.withColumnRenamed("-NA","NA")

collmap.createOrReplaceTempView('collmap')

collmap = sqlContext.sql('select ControlLocation, sum(PE) as PE, sum(CE) as CE from collmap group by ControlLocation order by ControlLocation')

collmap.createOrReplaceTempView('collmap')

emi1 = sqlContext.sql('select emi1.*, collmap.PE as collection_PE_day, collmap.CE as collection_CE_day from emi1 left join collmap on emi1.ControlLocation = collmap.ControlLocation')

emi1 = emi1.na.fill(0)

emi1.createOrReplaceTempView('emi1')

# collection columns derivation

emi1 = sqlContext.sql('select emi1.*,(collection_PE_day/No_of_PEs_using_the_app) as of_collections_per_day_pe, (collection_CE_day/No_of_CEs_using_the_app) as of_collections_per_day_ce from emi1')

# dropping extra columns 

emi1 = emi1.drop('Visits_PE_day','Visits_CE_day','collection_PE_day','collection_CE_day')

emi1 = emi1.na.fill(0)

emi1.createOrReplaceTempView('emi1')

#### 3 deriving the of visits columns for the month

# the data is already filtered for the  current month at the start of the code so no need to filter for current month at this line

permonth = sqlContext.sql('select branchfinal , Executive_Type, count(distinct Member_Key) as visits from field3 group by branchfinal,Executive_Type')

permonth.createOrReplaceTempView('permonth')

## mapping of branchfinal column of per dataframe

permonthmap = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal in ("LCKNW_ ","LCKN_ ") then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal="PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from permonth')

permonthmap = permonthmap.groupBy("ControlLocation").pivot("Executive_Type").sum("visits")

permonthmap = permonthmap.filter('ControlLocation is not null')

permonthmap = permonthmap.na.fill(0)

permonthmap.createOrReplaceTempView('permonthmap')

permonthmap = sqlContext.sql('select ControlLocation, sum(PE) as PE, sum(CE) as CE  from permonthmap group by ControlLocation order by ControlLocation')

permonthmap.createOrReplaceTempView('permonthmap')

emi1 = sqlContext.sql('select emi1.*,permonthmap.PE as Visits_PE_month , permonthmap.CE as Visits_CE_month from emi1 left join permonthmap on emi1.ControlLocation = permonthmap.ControlLocation')

emi1 = emi1.na.fill(0)

emi1.createOrReplaceTempView('emi1')

## deriving the of visits column for the month

emi1 = sqlContext.sql('select emi1.*,(Visits_PE_month/No_of_PEs_using_the_app) as of_visit_per_month_pe, (Visits_CE_month/No_of_CEs_using_the_app) as of_visit_per_month_ce from emi1')

emi1 = emi1.na.fill(0)

# dropping useless columns

emi1 = emi1.drop('Visits_PE_month','Visits_CE_month')

emi1.createOrReplaceTempView('emi1')

## 4 deriving the collections columns for the month

collectionmonth = sqlContext.sql('select branchfinal , Executive_Type, count(distinct Member_Key) as visits from field3 where NoofEMIs > 0 group by branchfinal,Executive_Type')

collectionmonth.createOrReplaceTempView('colm')

# collectionday mapping

collectionmap = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal in ("LCKNW_ ","LCKN_ ") then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal="PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from colm')

collectionmap = collectionmap.groupBy("ControlLocation").pivot("Executive_Type").sum("visits")

collectionmap = collectionmap.na.fill(0)

collectionmap.createOrReplaceTempView('collectionmap')

collectionmap = sqlContext.sql('select ControlLocation, sum(PE) as PE, sum(CE) as CE  from collectionmap group by ControlLocation order by ControlLocation')

collectionmap.createOrReplaceTempView('collectionmap')

emi1 = sqlContext.sql('select emi1.*, collectionmap.PE as collection_PE_month, collectionmap.CE as collection_CE_month from emi1 left join collectionmap on emi1.ControlLocation = collectionmap.ControlLocation')

emi1 = emi1.na.fill(0)

emi1.createOrReplaceTempView('emi1')


# derivation of the columns of collections for the month

emi1 = sqlContext.sql('select emi1.*,(collection_PE_month/No_of_PEs_using_the_app) as of_collections_per_month_pe, (collection_CE_month/No_of_CEs_using_the_app) as of_collections_per_month_ce from emi1')

emi1 = emi1.na.fill(0)

# dropping useless columns 

emi1 = emi1.drop('collection_PE_month','collection_CE_month')

emi1.createOrReplaceTempView('emi1')

### 5 deriving the Total_unique_members_met and Yesterday_Visit_count column 

# dynamic ==> Yesterday_Visit_count = sqlContext.sql('select branchfinal, count(distinct Member_Key) as Yesterday_Visit_count from field3 where Visit_Date = Now() - interval 02 day group by branchfinal')

Yesterday_Visit_count = sqlContext.sql('select branchfinal, count(distinct Member_Key) as Yesterday_Visit_count from field3 where Visit_Date = "2019-07-08" group by branchfinal')

Yesterday_Visit_count.createOrReplaceTempView('Yesterday_Visit_count')

Yesterday_Visit_count = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal in ("LCKNW_ ","LCKN_ ") then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal="PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from Yesterday_Visit_count')

Yesterday_Visit_count.createOrReplaceTempView('Yesterday_Visit_count')

# do grouping of similar controllocation

Yesterday_Visit_count = sqlContext.sql('select ControlLocation, sum(Yesterday_Visit_count) as Yesterday_Visit_count from Yesterday_Visit_count group by ControlLocation order by ControlLocation')

Yesterday_Visit_count.createOrReplaceTempView('Yesterday_Visit_count')

emi1 = sqlContext.sql('select emi1.*,Yesterday_Visit_count.Yesterday_Visit_count from emi1 left join Yesterday_Visit_count on emi1.ControlLocation = Yesterday_Visit_count.ControlLocation ')

emi1.createOrReplaceTempView('emi1')


## Total_unique_members_met

Total_unique_members_met = sqlContext.sql('select branchfinal, count(distinct Member_Key) as Total_unique_members_met from field3 group by branchfinal')

Total_unique_members_met.createOrReplaceTempView('Total_unique_members_met')

Total_unique_members_met = sqlContext.sql('select *,case when branchfinal="AHM_ " then "Ahmedabad" when branchfinal="BANG_KOR" then "Bangalore - Koramangala" when branchfinal="BANG_MIL" then "Bangalore - Millers Road" when branchfinal="BHPL_ " then "Bhopal" when branchfinal="CHDG_ " then "Chandigarh" when branchfinal in ("CHEN_ ","N_CHEN") then "Chennai" when branchfinal="DELHI_J" then "Delhi - Jasola" when branchfinal="DELHI_P" then "Delhi - Pitampura" when branchfinal="HYD_ " then "Hyderabad" when branchfinal="JAIP_ " then "Jaipur" when branchfinal="KOCHI_ " then "Kochi" when branchfinal="KOLK_ " then "Kolkata" when branchfinal in ("LCKNW_ ","LCKN_ ") then "Lucknow" when branchfinal="MUM_A" then "Mumbai-Andheri" when branchfinal="MUM_V" then "Mumbai-Vashi" when branchfinal="PATNA_ " then "Patna" when branchfinal="PUNE_ " then "Pune" else branchfinal end as ControlLocation from Total_unique_members_met')

Total_unique_members_met.createOrReplaceTempView('Total_unique_members_met')

Total_unique_members_met = sqlContext.sql('select ControlLocation, sum(Total_unique_members_met) as Total_unique_members_met from Total_unique_members_met group by ControlLocation order by ControlLocation')

Total_unique_members_met.createOrReplaceTempView('Total_unique_members_met')

emi1 = sqlContext.sql('select emi1.*,Total_unique_members_met.Total_unique_members_met from emi1 left join Total_unique_members_met on emi1.ControlLocation = Total_unique_members_met.ControlLocation ')

emi1 = emi1.na.fill(0)

emi1.createOrReplaceTempView('emi1')

finalemi = sqlContext.sql('select ControlLocation,visitdate,No_of_PEs_,No_of_CEs_,No_of_PEs_using_the_app,No_of_CEs_using_the_app,Executive_Type_Unknown,App_Adoption_per, of_visits_per_day_pe, of_visits_per_day_ce, of_collections_per_day_pe, of_collections_per_day_ce, of_visit_per_month_pe, of_visit_per_month_ce, of_collections_per_month_pe, of_collections_per_month_ce, Total_unique_members_met, Yesterday_Visit_count, now() as Rundate from emi1 ')
finalemi=finalemi.withColumn('run_date',lit(current_date()))
finalemi.createOrReplaceTempView('pdr')

finalemi_old = spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Emi_app/emi_app.csv",header =True,inferSchema=True)
finalemi_old.createOrReplaceTempView('pdro')

finalemi_append = spark.sql('select * from pdro union all select * from pdr')
# write Statement
finalemi.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Emi_app",mode="append",header=True)
time.sleep(10)

table_1 = spark.sql('select "Field_Visit_Table" as Table_name, max(dw_lastupdated) as last_modified, current_date() as run_date from fi')

table_2 = spark.sql('select "ManForce_Table" as Table_name, max(date) as last_modified, current_date() as run_date from map')

table_A = table_1.unionAll(table_2)
table_A.createOrReplaceTempView('tf')

#table_A.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Table_details",mode="append",header="true")


emi_app_table_old= spark.read.option("delimiter",",").csv("s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Table_details/table_details.csv",header =True,inferSchema=True)

emi_app_table_old.createOrReplaceTempView('nto')

emi_app_final= spark.sql('select * from nto union all select * from tf')

emi_app_final.coalesce(1).write.csv("s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Table_details",mode="append",header="true")

sc._jsc.hadoopConfiguration().set("mapred.output.committer.class","org.apache.hadoop.mapred.FileOutputCommitter")
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

cmd1="sudo aws s3 rm s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Emi_app/emi_app.csv"
os.system(cmd1)
time.sleep(10)
cmd2="sudo aws s3 rm s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Table_details/table_details.csv"
os.system(cmd2)
time.sleep(10)

fs = FileSystem.get(URI("s3://cmh-process-data"), sc._jsc.hadoopConfiguration())
time.sleep(10)


file_path_emi_app = "s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Emi_app/"
time.sleep(10)
file_path_Table_details = "s3://cmh-process-data/intermediate_files/EMI_APP_dashboard/Table_details/"
time.sleep(10)

created_file_path_emi_app = fs.globStatus(Path(file_path_emi_app + "part*.csv"))[0].getPath()
time.sleep(10)
created_file_path_Table_details = fs.globStatus(Path(file_path_Table_details + "part*.csv"))[0].getPath()
time.sleep(10)

fs.rename(created_file_path_emi_app,Path(file_path_emi_app + "emi_app.csv"))
time.sleep(10)
fs.rename(created_file_path_Table_details,Path(file_path_Table_details + "table_details.csv"))









### End of the Lines 
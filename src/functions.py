# Databricks notebook source
### import required functions
from pyspark.sql.functions import sequence, to_date, explode, col, substring, when, collect_list, lit, ntile, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

### function to read and append multiple source parquet files, then output to lake location
def prepare_source_parquets(root_path, schema_path, file_prefix, out_path, min_date, max_date):

    # read one parquet to retrieve schema for empty data frame
    df_temp = spark.read.parquet(schema_path)

    # create empty data frame and apply schema
    df_pld = spark.createDataFrame(data = [], schema = df_temp.schema)

    # define sql query to list months to retrieve data for
    sql_query = "SELECT sequence(to_date('" + str(min_date) + "'), to_date('" + str(max_date) + "'), interval 1 month) as date"

    # prepare table of months to retrieve data for
    df_months = spark.sql(sql_query).withColumn("date", explode(col("date")))
    df_months = df_months.withColumn("year", substring("date",1,4)).withColumn("month", substring("date",6,2))
    df_loop = df_months.toPandas()

    # define loop to run through each element and build data
    for index, row in df_loop.iterrows():   
        
        # define year for current loop     
        path_year = row[1]    
        
        # define month for current loop     
        path_month = row[2]    
        
        # define path for current loop   
        file_name = root_path + path_year + "/" + path_month + "/" + file_prefix + path_year + path_month + "_00000.parquet"        
        
        # read current path as dataset    
        df_pld_in = spark.read.parquet(file_name)    
        
        # append to output file    
        df_pld = df_pld.union(df_pld_in)

    # write data
    df_pld.write.mode('overwrite').parquet(out_path)

# COMMAND ----------

## function to read patient level data
def read_pld(min_date, max_date, out_path):

    df_pld = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/MESH/Tobacco/TobaccoDependenceRecord/Published/1/")

    df_pld = df_pld.filter("Activity_Date > '" + min_date + "'")
    df_pld = df_pld.filter("Activity_Date < '" + max_date + "'")

    df_pld.write.mode('overwrite').parquet(out_path)

# COMMAND ----------

### function to clean patient level data
def clean_patient(raw_path, out_path, min_date, max_date): 
    
    ### load data 
    # patient level filtered for activity dates
    df_pld = spark.read.parquet(raw_path)
    
    df_pld = df_pld.filter("Activity_Date > '" + min_date + "'")
    df_pld = df_pld.filter("Activity_Date < '" + max_date + "'")


    ### prepare subicb reference data
    # read subicb reference data
    df_ref_subicb = spark.read.parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/reference/Internal/Reference/Sub_ICBs_2022_23/Published/1/Internal_Reference_Sub_ICBs_2022_23_00000.parquet")

    # create temp tables to call in sql
    df_pld.createOrReplaceTempView("df_pld");

    df_ref_subicb.createOrReplaceTempView("df_ref_subicb");

    # define subicb assignment sql
    sql_query = """select RecordID \
                ,case when b.Sub_ICB_Location_ODS_Code is not null then b.Sub_ICB_Location_ODS_Code 
                    when c.Sub_ICB_Location_ODS_Code is not null then c.Sub_ICB_Location_ODS_Code 
                    when d.Sub_ICB_Location_ODS_Code is not null then d.Sub_ICB_Location_ODS_Code 
                    else null end as SubICB_Code 
        from df_pld a 
        left join (select * from  df_ref_subicb) b on a.CCG_OF_REGISTRATION = b.Sub_ICB_Location_ODS_Code 
        left join (select * from  df_ref_subicb) c on a.CCG_OF_RESIDENCE = c.Sub_ICB_Location_ODS_Code 
        left join (select * from  df_ref_subicb) d on a.Der_Postcode_CCG_Code = d.Sub_ICB_Location_ODS_Code"""

    # execute sql, return subicb assignment per record id
    df_subicb = spark.sql(sql_query)

    # create temp table
    df_subicb.createOrReplaceTempView("df_subicb");


    ### check 1 - duplicate records

    # define sql to check for duplicate records
    sql_query = """select RecordID 
            ,case when Max_AuditID = AuditId or Max_AuditID is null then 0 else 1 end as DQ_Duplicates
            ,case when Max_AuditID = AuditId or Max_AuditID is null then null else '[Duplicate Record]' end as DQ_Duplicates_Description
        from df_pld a 
        left join (select MAX(AuditID) as Max_AuditID
                    ,pseudo_nhs_number_ncdr as nhs_number_dup
                    ,HOSPITAL_SPELL_ID as spell_id_dup
                    ,ACTIVITY_DATE as activity_date_dup
            from df_pld
            group by pseudo_nhs_number_ncdr
                    ,HOSPITAL_SPELL_ID
                    ,ACTIVITY_DATE) b on a.ACTIVITY_DATE = b.activity_date_dup
                                    and a.HOSPITAL_SPELL_ID = b.spell_id_dup
                                    and a.pseudo_nhs_number_ncdr = b.nhs_number_dup"""

    # execute sql
    df_duplicates = spark.sql(sql_query)

    # create temp table
    df_duplicates.createOrReplaceTempView("df_duplicates");


    ### check 2 - dummy records  

    # define sql to check for dummy records
    sql_query = """select RecordID
                ,case when b.ODS_CODE is not null then 1 else 0 end as DQ_Dummy
                ,case when b.ODS_CODE is not null then '[Dummy Record]' else null end as DQ_Dummy_Description
        from df_pld a
        left join (select * from (
                    select ODS_Code
                            ,Reporting_Period
                            ,count(*) as count
                    from df_pld
                    group by ODS_CODE
                                ,REPORTING_PERIOD
                    ) a 
                    where count = 1) b
                on a.ODS_CODE = b.ODS_CODE
    and a.REPORTING_PERIOD = b.REPORTING_PERIOD"""

    # execute sql
    df_dummy = spark.sql(sql_query)

    # create temp table
    df_dummy.createOrReplaceTempView("df_dummy");


    ### check 3 - activity date

    # define sql to check for invalid activity dates
    sql_query = """select RecordID
                ,case when ACTIVITY_DATE is not null then 0 else 1 end as DQ_ActivityDate
                ,case when ACTIVITY_DATE is not null then null else '[Invalid Activity Date]' end as DQ_ActivityDate_Description
    from df_pld"""

    # execute sql
    df_date = spark.sql(sql_query)

    # create temp table
    df_date.createOrReplaceTempView("df_date");


    ### check 4 - setting type 

    # define sql to check for valid setting types
    sql_query = """select RecordID
                ,case when SETTING_TYPE is not null then 0 else 1 end as DQ_Setting
                ,case when SETTING_TYPE is not null then null else '[Invalid Setting Type]' end as DQ_Setting_Description
        from df_pld"""

    # execute sql 
    df_setting = spark.sql(sql_query)

    # create temp tables to call
    df_setting.createOrReplaceTempView("df_setting");


    ### create dq table 

    # define sql to prepare dq table 
    sql_query = "select record.RecordID \
                ,duplicates.DQ_Duplicates \
                ,dummy.DQ_Dummy \
                ,activitydate.DQ_ActivityDate \
                ,setting.DQ_Setting \
                ,case when DQ_Duplicates + DQ_Dummy + DQ_ActivityDate + DQ_Setting > 0 then 1 else 0 end as DQ_Flag \
                ,case when DQ_Duplicates + DQ_Dummy + DQ_ActivityDate + DQ_Setting > 0 then concat(DQ_Duplicates_Description, DQ_Dummy_Description, DQ_ActivityDate_Description, DQ_Setting_Description) \
                else 'No DQ Issue Identified' end as DQ_Description \
        from df_pld record \
        left join df_duplicates duplicates on record.RecordID = duplicates.RecordID \
        left join df_dummy dummy on record.RecordID = dummy.RecordID \
        left join df_date activitydate on record.RecordID = activitydate.RecordID \
        left join df_setting setting on record.RecordID = setting.RecordID"

    # execute sql
    df_dq = spark.sql(sql_query)

    # create temp tables to call
    df_dq.createOrReplaceTempView("df_dq");


    ### output cleaned dataset 

    # define sql to prepare cleaned dataset 
    sql_query = "	select a.* \
                ,SubICB_Code \
        from df_pld a  \
        inner join (select * from df_dq where DQ_Flag = 0) b on a.RecordID = b.RecordID \
        left join df_subicb c on a.RecordID = c.RecordID"

    # execute sql
    df_clean = spark.sql(sql_query)

    # write data
    df_clean.write.mode('overwrite').parquet(out_path)

# COMMAND ----------

### function to generate provider reference data 
def provider_ref(clean_path, out_path): 

    # read clean tobacco patient level data
    df_pld = spark.read.parquet(clean_path)

    # read provider reference data (ODS)
    df_ref_prov = spark.read.parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/reference/UKHD/ODS/All_Providers_SCD/Published/1/UKHD_ODS_All_Providers_SCD_00000.parquet")

    # filter to only retain current configuration of providers
    df_ref_prov = df_ref_prov.filter("Effective_To is null")

    # read icb reference data
    df_ref_icb = spark.read.table("tobacco_ref_icb")

    # read region reference data 
    df_ref_region = spark.read.table("tobacco_ref_region")

    # create temp tables to call
    df_pld.createOrReplaceTempView("df_pld");

    df_ref_prov.createOrReplaceTempView("df_ref_prov");

    df_ref_icb.createOrReplaceTempView("df_ref_icb");

    df_ref_region.createOrReplaceTempView("df_ref_region");

    # define sql to generate provider reference data
    sql_query = """select distinct ODS_Code
                    ,ODS_NAME
                    ,b.High_Level_Health_Authority_Code as ICB_Code
                    ,d.ICB_Name
                    ,CONCAT(b.High_Level_Health_Authority_Code, ': ', d.ICB_Name) as ICB_Code_Name
                    ,d.ICB_Map
                    ,b.National_Grouping_Code as Region_Code
                    ,c.Region as Region_Name
                    ,CONCAT(b.National_Grouping_Code, ': ', c.Region) as Region_Code_Name
                    ,c.Region_Map
        from df_pld a
        left join df_ref_prov b on a.ODS_CODE = b.Organisation_Code
        left join df_ref_region c on b.National_Grouping_Code = c.Region_Code
        left join df_ref_icb d on b.High_Level_Health_Authority_Code = d.ICB_Code"""

    # execute sql 
    df_ref_prov_out = spark.sql(sql_query)

    # write data
    df_ref_prov_out.write.mode('overwrite').parquet(out_path)

# COMMAND ----------

### function to generate commissioner reference data
def commissioner_ref(clean_path, out_path): 

    # read clean tobacco patient level data
    df_pld = spark.read.parquet(clean_path)

    # read subicb reference data 
    df_ref_subicb = spark.read.parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/reference/Internal/Reference/Sub_ICBs_2022_23/Published/1/Internal_Reference_Sub_ICBs_2022_23_00000.parquet")

    # read icb reference data
    df_ref_icb = spark.read.table("tobacco_ref_icb")

    # read region reference data 
    df_ref_region = spark.read.table("tobacco_ref_region")

    ### prepare commissioner reference data 

    # create temp tables to call
    df_pld.createOrReplaceTempView("df_pld");

    df_ref_subicb.createOrReplaceTempView("df_ref_subicb");

    df_ref_icb.createOrReplaceTempView("df_ref_icb");

    df_ref_region.createOrReplaceTempView("df_ref_region");

    # define sql to generate subicb reference data
    sql_query = """select distinct a.Sub_ICB_Location_ODS_Code as Organisation_Code
                    ,a.Sub_ICB_Location_Name as Organisation_Name
                    ,CONCAT(a.Sub_ICB_Location_ODS_Code, ': ', a.Sub_ICB_Location_Name) as Organisation_Code_Name
                    ,a.ICB_Code
                    ,a.Integrated_Care_Board_Name as ICB_Name
                    ,CONCAT(a.ICB_Code, ': ', a.Integrated_Care_Board_Name) as ICB_Code_Name
                    ,d.ICB_Map
                    ,a.Region_Code
                    ,c.Region as Region_Name
                    ,CONCAT(a.Region_Code, ': ', c.Region) as Region_Code_Name
                    ,c.Region_Map
        from df_ref_subicb a
        left join df_ref_region c on a.Region_Code = c.Region_Code
        left join df_ref_icb d on a.ICB_Code = d.ICB_Code"""

    df_ref_com_out = spark.sql(sql_query)

    # write data
    df_ref_com_out.write.mode('overwrite').parquet(out_path)

# COMMAND ----------

### function to read required SUS data (PAT Intermediate)
def read_sus(min_date, max_date, out_path): 

    # read SUS folders individually -- Discontinued method
    # df_sus_2020 = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/UDALSQLMART/PatActivity/PAT_Intermediate_Table_APC/Published/1/2020/")

    # df_sus_2021 = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/UDALSQLMART/PatActivity/PAT_Intermediate_Table_APC/Published/1/2021/")

    # df_sus_2022 = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/UDALSQLMART/PatActivity/PAT_Intermediate_Table_APC/Published/1/2022/")

    # df_sus_2023 = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/UDALSQLMART/PatActivity/PAT_Intermediate_Table_APC/Published/1/2023/")

    # join sus tables
    #df_sus = df_sus_2022.union(df_sus_2023) \
    #    .union(df_sus_2021) \
    #        .union(df_sus_2020)


    df_sus = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/UDALSQLMART/PatActivity/PAT_Intermediate_Table_APC/Published/1/")

    df_sus = df_sus.filter("Attendance_Date >= '" + min_date + "'")
    df_sus = df_sus.filter("Attendance_Date <= '" + max_date + "'")

    # write data
    df_sus.write.mode('overwrite').parquet(out_path)

# COMMAND ----------

### function to read required MSDS data 
def read_msds(min_date, max_date, out_path):

    # read required msds booking data files
    df_msds_2122 = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/MESH/MSDS/MSD101PregnancyBooking/Published/1/FY2021-22/")
    df_msds_2223 = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/MESH/MSDS/MSD101PregnancyBooking/Published/1/FY2022-23/")
    df_msds_2324 = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/MESH/MSDS/MSD101PregnancyBooking/Published/1/FY2023-24/")

    # join files together and filter, for now we only need ID and Date of booking, but this can be expanded in the future
    df_msds = df_msds_2122.union(df_msds_2223) 
    df_msds = df_msds.union(df_msds_2324)

    # filter for required months 
    df_msds = df_msds.filter("AntenatalAppDate > '" + min_date + "'")
    df_msds = df_msds.filter("AntenatalAppDate < '" + max_date + "'")

    # create temp table to query
    df_msds.createOrReplaceTempView("df_msds");

    # filter to one record and min app date per pregnancy id. There are multiple records for each pregnancy ID 
    sql_query = "select a.OrgCodeProvider \
                        ,a.PregnancyID \
                    ,min(AntenatalAppDate) as MinBookingDate \
                from df_msds a \
                group by a.OrgCodeProvider \
                    ,a.PregnancyID"

    # run query
    df_msds = spark.sql(sql_query)

    ## write data 
    df_msds.write.mode('overwrite').parquet(out_path)

# COMMAND ----------

### function to build elements
def build_elements(clean_path, out_path): 

    ### read in required datasets
    # read clean tobacco patient level data 
    df_pld = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/clean/patient")

    # read tobacco aggregate level data 
    df_agg = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/aggregate")

    # read sus data
    df_sus = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/sus")

    # read in msds data 
    df_msds = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/msds")

    # read elements reference data
    df_ref_elements = spark.read.table("tobacco_ref_elements")

    # filter to two elements while building 
    df_ref_elements = df_ref_elements.filter("Reporting_Flag == '1' and Element_Source <> 'SUS+'")

    # read ethnic category reference data
    df_ref_eth = spark.read.parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/reference/UKHD/Data_Dictionary/Ethnic_Category_Code_SCD/Published/1/UKHD_Data_Dictionary_Ethnic_Category_Code_SCD_00000.parquet")

    # read imd reference data 
    df_ref_imd = spark.read.parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/aggregated/UKHF/Demography/Index_Of_Multiple_Deprivation_By_LSOA1/Published/1/00000_UKHF_Demography_Index_Of_Multiple_Deprivation_By_LSOA1_1.parquet")

    # filter imd data to most recent snapshot
    df_ref_imd = df_ref_imd.filter("Effective_Snapshot_Date = '2019-12-31'")

    # read provider reference data
    df_ref_prov = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/ref/prov")

    # create temp table to call using sql script
    df_ref_prov.createOrReplaceTempView("df_ref_prov");

    ### prepare elements output table 
    # create empty data frame to insert into 
    columns = StructType([
    StructField('Element_ID', StringType(), True),
    StructField('Date', DateType(), True),
    StructField('Provider_Code', StringType(), True),
    StructField('Commissioner_Code', StringType(), True),
    StructField('Setting_Type', StringType(), True),
    StructField('Health_Inequalities_Group', StringType(), True),
    StructField('Health_Inequalities_SubGroup', StringType(), True),
    StructField('Value', IntegerType(), True)
    ])

    df_elements_out = spark.createDataFrame(data = [],schema = columns)

    ### build elements for pld based measures
    # create empty data frame to insert into 
    columns = StructType([
    StructField('Element_ID', StringType(), True),
    StructField('Date', DateType(), True),
    StructField('Provider_Code', StringType(), True),
    StructField('Commissioner_Code', StringType(), True),
    StructField('Setting_Type', StringType(), True),
    StructField('Gender', StringType(), True),
    StructField('Age', StringType(), True),
    StructField('Ethnicity', StringType(), True),
    StructField('Ethnic_Group', StringType(), True),
    StructField('IMD_Quintile', StringType(), True),
    StructField('Value', IntegerType(), True)
    ])

    df_elements_out_pld = spark.createDataFrame(data = [],schema = columns)

    # create temp table to call using sql script
    df_pld.createOrReplaceTempView("df_pld");

    df_ref_eth.createOrReplaceTempView("df_ref_eth");

    df_ref_imd.createOrReplaceTempView("df_ref_imd");

    # limit reference data to pld based elements
    df_ref_elements_pld = df_ref_elements.filter("Element_Source == 'Tobacco Patient Level Dataset'")

    # convert raw table to pandas to allow for looping
    df_loop = df_ref_elements_pld.toPandas()

    # define loop to run through each element and build data
    for index, row in df_loop.iterrows():

        # define element id for current loop
        element_id = row[1]

        # define sql logic for current loop
        element_logic = row[6]

        # define sql query
        sql_query = "select " + str(element_id) + " as Element_ID \
                        ,cast(concat(Year(Activity_Date), '-', Month(Activity_Date), '-', '01') as date) as Date \
                        ,ODS_Code as Provider_Code \
                        ,SubICB_Code as Commissioner_Code \
                        ,case \
                            when Setting_Type = '1' and Activity_Type = '1' then 'Acute Inpatient' \
                            when Setting_Type = '1' and Activity_Type = '2' then 'Acute Outpatient' \
                            when Setting_Type = '2' and Activity_Type = '1' then 'Mental Health Inpatient' \
                            when Setting_Type = '2' and Activity_Type = '3' then 'Mental Health Inpatient' \
                            when Setting_Type = '2' and Activity_Type = '4' then 'Specialist Community Mental Health' \
                            when Setting_Type = '3' then 'Maternity' \
                            else 'Unknown' end as Setting_Type \
                        ,case \
                            when Gender = '1' then 'Male' \
                            when Gender = '2' then 'Female' \
                            when Gender = '3' then 'Is not stated' \
                            else 'Unknown' end as Gender \
                        ,case \
                            when Der_Age_at_CDS_Activity_date < 18 then 'Under 18' \
                            when Der_Age_at_CDS_Activity_date >= 18 and Der_Age_at_CDS_Activity_date < 35 then '18-34' \
                            when Der_Age_at_CDS_Activity_date >= 35 and Der_Age_at_CDS_Activity_date < 44 then '35-44' \
                            when Der_Age_at_CDS_Activity_date >= 45 and Der_Age_at_CDS_Activity_date < 60 then '45-59' \
                            when Der_Age_at_CDS_Activity_date >= 60 then '60 and over' else 'Unknown' end as Age \
                        ,b.Main_Description as Ethnicity \
                        ,case \
                            when Ethnicity in ('A','B','C') then 'White' \
                            when Ethnicity in ('D','E','F','G') then 'Mixed or multiple ethnic groups' \
                            when Ethnicity in ('H','J','K','L') then 'Asian or Asian British' \
                            when Ethnicity in ('M','N','P') then 'Black, African, Caribbean or Black British' \
                            when Ethnicity in ('R','S') then 'Other ethnic group' \
                            else 'Is not stated' end as Ethnic_Group \
                        ,case \
                            when c.IMD_Decile in ('1','2') then 'Quintile 1 - Most deprived' \
                            when c.IMD_Decile in ('3','4') then 'Quintile 2' \
                            when c.IMD_Decile in ('5','6') then 'Quintile 3' \
                            when c.IMD_Decile in ('7','8') then 'Quintile 4' \
                            when c.IMD_Decile in ('9','10') then 'Quintile 5 - Least deprived' \
                            else 'Unknown' end as IMD_Quintile \
                        ,count(RecordID) as Value \
                    from df_pld a \
                    left join (select distinct Main_Code_Text, Main_Description from df_ref_eth) b on a.Ethnicity = b.Main_Code_Text \
                    left join df_ref_imd c on a.LSOA_OF_RESIDENCE = c.LSOA_Code \
                    where " \
                    + str(element_logic) + \
                    " group by cast(concat(Year(Activity_Date), '-', Month(Activity_Date), '-', '01') as date) \
                    ,ODS_Code \
                    ,SubICB_Code \
                    ,case \
                            when Setting_Type = '1' and Activity_Type = '1' then 'Acute Inpatient' \
                            when Setting_Type = '1' and Activity_Type = '2' then 'Acute Outpatient' \
                            when Setting_Type = '2' and Activity_Type = '1' then 'Mental Health Inpatient' \
                            when Setting_Type = '2' and Activity_Type = '3' then 'Mental Health Inpatient' \
                            when Setting_Type = '2' and Activity_Type = '4' then 'Specialist Community Mental Health' \
                            when Setting_Type = '3' then 'Maternity' \
                            else 'Unknown' end \
                    ,case \
                            when Gender = '1' then 'Male' \
                            when Gender = '2' then 'Female' \
                            when Gender = '3' then 'Is not stated' \
                            else 'Unknown' end \
                    ,case \
                            when Der_Age_at_CDS_Activity_date < 18 then 'Under 18' \
                            when Der_Age_at_CDS_Activity_date >= 18 and Der_Age_at_CDS_Activity_date < 35 then '18-34' \
                            when Der_Age_at_CDS_Activity_date >= 35 and Der_Age_at_CDS_Activity_date < 44 then '35-44' \
                            when Der_Age_at_CDS_Activity_date >= 45 and Der_Age_at_CDS_Activity_date < 60 then '45-59' \
                            when Der_Age_at_CDS_Activity_date >= 60 then '60 and over' else 'Unknown' end  \
                    ,b.Main_Description \
                    ,case \
                            when Ethnicity in ('A','B','C') then 'White' \
                            when Ethnicity in ('D','E','F','G') then 'Mixed or multiple ethnic groups' \
                            when Ethnicity in ('H','J','K','L') then 'Asian or Asian British' \
                            when Ethnicity in ('M','N','P') then 'Black, African, Caribbean or Black British' \
                            when Ethnicity in ('R','S') then 'Other ethnic group' \
                            else 'Is not stated' end \
                    ,case \
                        when c.IMD_Decile in ('1','2') then 'Quintile 1 - Most deprived' \
                        when c.IMD_Decile in ('3','4') then 'Quintile 2' \
                        when c.IMD_Decile in ('5','6') then 'Quintile 3' \
                        when c.IMD_Decile in ('7','8') then 'Quintile 4' \
                        when c.IMD_Decile in ('9','10') then 'Quintile 5 - Least deprived' \
                        else 'Unknown' end"

        # execute sql query
        df_element_pld = spark.sql(sql_query)

        # write output to table 
        df_elements_out_pld = df_elements_out_pld.union(df_element_pld)

    # reformat data into long structure and calculate total group
    df_elements_total = df_elements_out_pld.groupBy("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type").sum("Value") \
        .withColumn("Health_Inequalities_Group", lit("Total")) \
        .withColumn("Health_Inequalities_SubGroup",lit("Total")) \
        .withColumnRenamed("sum(Value)","Value") \
        .select("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Health_Inequalities_Group","Health_Inequalities_SubGroup","Value")

    df_elements_gender = df_elements_out_pld.groupBy("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Gender").sum("Value") \
        .withColumn("Health_Inequalities_Group", lit("Gender")) \
        .withColumnRenamed("Gender","Health_Inequalities_SubGroup") \
        .withColumnRenamed("sum(Value)","Value") \
        .select("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Health_Inequalities_Group","Health_Inequalities_SubGroup","Value")

    df_elements_age = df_elements_out_pld.groupBy("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Age").sum("Value") \
        .withColumn("Health_Inequalities_Group", lit("Age")) \
        .withColumnRenamed("Age","Health_Inequalities_SubGroup") \
        .withColumnRenamed("sum(Value)","Value") \
        .select("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Health_Inequalities_Group","Health_Inequalities_SubGroup","Value")

    df_elements_eth = df_elements_out_pld.groupBy("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Ethnicity").sum("Value") \
        .withColumn("Health_Inequalities_Group", lit("Ethnicity")) \
        .withColumnRenamed("Ethnicity","Health_Inequalities_SubGroup") \
        .withColumnRenamed("sum(Value)","Value") \
        .select("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Health_Inequalities_Group","Health_Inequalities_SubGroup","Value")

    df_elements_ethgroup = df_elements_out_pld.groupBy("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Ethnic_Group").sum("Value") \
        .withColumn("Health_Inequalities_Group", lit("Ethnic_Group")) \
        .withColumnRenamed("Ethnic_Group","Health_Inequalities_SubGroup") \
        .withColumnRenamed("sum(Value)","Value") \
        .select("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Health_Inequalities_Group","Health_Inequalities_SubGroup","Value")

    df_elements_imd = df_elements_out_pld.groupBy("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","IMD_Quintile").sum("Value") \
        .withColumn("Health_Inequalities_Group", lit("IMD_Quintile")) \
        .withColumnRenamed("IMD_Quintile", "Health_Inequalities_SubGroup") \
        .withColumnRenamed("sum(Value)", "Value") \
        .select("Element_ID","Date","Provider_Code","Commissioner_Code","Setting_Type","Health_Inequalities_Group","Health_Inequalities_SubGroup","Value")

    df_elements_out_pld = df_elements_total.union(df_elements_gender) \
        .union(df_elements_age) \
        .union(df_elements_eth) \
        .union(df_elements_ethgroup) \
        .union(df_elements_imd)
        
    df_elements_out = df_elements_out.union(df_elements_out_pld)

    ### build elements for aggregate based measures 
    # create temp table to call using sql script
    df_agg.createOrReplaceTempView("df_agg");

    # define sql query
    sql_query_ip = "select '001' as Element_ID \
                    ,concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') as Date \
                    ,ODS_CODE as Provider_Code \
                    ,NULL as Commissioner_Code \
                    ,'Acute Inpatient' as Setting_Type \
                    ,'Total' as Health_Inequalities_Group \
                    ,'Total' as Health_Inequalities_SubGroup \
                    ,SUM(COMPLETESMOKINGSTATUS_IP) as Value \
            from df_agg \
            group by concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') \
            ,ODS_CODE"

    # execute sql query
    df_element_agg_ip = spark.sql(sql_query_ip)

    # write output to table 
    df_elements_out = df_elements_out.union(df_element_agg_ip)

    # define sql query
    sql_query_mat = "select '001' as Element_ID \
                    ,concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') as Date \
                    ,ODS_CODE as Provider_Code \
                    ,NULL as Commissioner_Code \
                    ,'Maternity' as Setting_Type \
                    ,'Total' as Health_Inequalities_Group \
                    ,'Total' as Health_Inequalities_SubGroup \
                    ,SUM(COMPLETESMOKINGSTATUS_MATERNITY) as Value \
            from df_agg \
            group by concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') \
            ,ODS_CODE"

    # execute sql query
    df_element_agg_mat = spark.sql(sql_query_mat)

    # write output to table 
    df_elements_out = df_elements_out.union(df_element_agg_mat)

    # define sql query
    sql_query_op = "select '001' as Element_ID \
                    ,concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') as Date \
                    ,ODS_CODE as Provider_Code \
                    ,NULL as Commissioner_Code \
                    ,'Acute Outpatient' as Setting_Type \
                    ,'Total' as Health_Inequalities_Group \
                    ,'Total' as Health_Inequalities_SubGroup \
                    ,SUM(COMPLETESMOKINGSTATUS_OP) as Value \
            from df_agg \
            group by concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') \
            ,ODS_CODE"

    # execute sql query
    df_element_agg_op = spark.sql(sql_query_op)

    # write output to table 
    df_elements_out = df_elements_out.union(df_element_agg_op)

    # define sql query
    sql_query_com = "select '001' as Element_ID \
                    ,concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') as Date \
                    ,ODS_CODE as Provider_Code \
                    ,NULL as Commissioner_Code \
                    ,'Acute Community' as Setting_Type \
                    ,'Total' as Health_Inequalities_Group \
                    ,'Total' as Health_Inequalities_SubGroup \
                    ,SUM(COMPLETESMOKINGSTATUS_COMMUNITY) as Value \
            from df_agg \
            group by concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') \
            ,ODS_CODE"

    # execute sql query
    df_element_agg_com = spark.sql(sql_query_com)

    # write output to table 
    df_elements_out = df_elements_out.union(df_element_agg_com)

    ### build elements for sus based measures
    # create temp table to call using sql script 
    df_sus.createOrReplaceTempView("df_sus");

    sql_query_sus = "select '010' as Element_ID \
                    ,cast(concat(Year(Attendance_Date), '-', Month(Attendance_Date), '-', '01') as date) as Date \
                    ,Provider_Current as Provider_Code \
                    ,NULL as Commissioner_Code \
                    ,'Acute Inpatient' as Setting_Type \
                    ,'Total' as Health_Inequalities_Group \
                    ,'Total' as Health_Inequalities_SubGroup \
                ,SUM(Adjusted) as Value \
            from df_sus a \
            inner join (select distinct ODS_Code from df_ref_prov) b on a.Provider_Current = b.ODS_Code \
            where Dimention_4 = 'Specific Acute' \
            and Dimention_1 in ('Ord. Elective Admission', 'Other Non-elective Admission', 'Emergency Admission')	\
            and Record_Classification <> 'Inpatient - Unclassified Spell'	\
            and Age >= 16 \
            and LOS_Adjusted > 0 \
            group by Provider_Current \
                    ,cast(concat(Year(Attendance_Date), '-', Month(Attendance_Date), '-', '01') as date)"

    # prepare output
    df_element_sus = spark.sql(sql_query_sus)
    
    # write output to table 
    df_elements_out = df_elements_out.union(df_element_sus)

    ### build elements for msds based measures 
    # create temp table to call using sql script 
    df_msds.createOrReplaceTempView("df_msds"); 

    sql_query_msds = "select '010' as Element_ID \
                    ,cast(concat(Year(MinBookingDate), '-', Month(MinBookingDate), '-', '01') as date) as Date \
                    ,OrgCodeProvider as Provider_Code \
                    ,NULL as Commissioner_Code \
                    ,'Maternity' as Setting_Type \
                    ,'Total' as Health_Inequalities_Group \
                    ,'Total' as Health_Inequalities_SubGroup \
                    ,Count(*) as Value \
                    from df_msds a \
                    inner join df_ref_prov b on a.OrgCodeProvider = b.ODS_Code \
                    group by OrgCodeProvider \
                    ,cast(concat(Year(MinBookingDate), '-', Month(MinBookingDate), '-', '01') as date)"

    df_elements_msds = spark.sql(sql_query_msds)

    df_elements_out = df_elements_out.union(df_elements_msds)

    # write data
    df_elements_out.write.mode('overwrite').parquet(out_path)

# COMMAND ----------

### function to build metrics 
def build_metrics(element_path, out_path): 

    ### read in required datasets 

    # elements data
    df_elements = spark.read.parquet(element_path)

    # metrics reference data
    df_ref_metrics = spark.read.table("tobacco_ref_metrics")

    df_ref_metrics = df_ref_metrics.filter("Reporting_Flag == '1'")

    ### prepare metrics output table 
    # create empty data frame to insert into 
    columns = StructType([
    StructField('Metric_ID', StringType(), True),
    StructField('Date', DateType(), True),
    StructField('Provider_Code', StringType(), True),
    StructField('Commissioner_Code', StringType(), True),
    StructField('Setting_Type', StringType(), True),
    StructField('Health_Inequalities_Group', StringType(), True),
    StructField('Health_Inequalities_SubGroup', StringType(), True),
    StructField('Numerator', IntegerType(), True),
    StructField('Denominator', IntegerType(), True)
    ])

    df_metrics_out = spark.createDataFrame(data = [],schema = columns)

    ### loop to build metric data 

    # create temp tables to call using sql script
    df_elements.createOrReplaceTempView("df_elements");

    df_ref_metrics.createOrReplaceTempView("df_ref_metrics");

    # convert raw table to pandas to allow for looping
    df_loop = df_ref_metrics.toPandas()

    # define loop to run through each element and build data
    for index, row in df_loop.iterrows():

        # define metric id for current loop 
        metric_id = row[2]

        # define numerator id for current loop
        numerator_id = row[8]

        # define denominator id for current loop 
        denominator_id = row[12]

        # define metric type for current loop
        metric_type = row[4]

        # if statement to deal with numerators and denominators
        if metric_type == "Proportion":

            # define sql query to construct metric for current loop 
            sql_query = "select '" + str(metric_id) + "' as Metric_ID \
                            ,Date \
                            ,Provider_Code \
                            ,Commissioner_Code \
                            ,Setting_Type \
                            ,Health_Inequalities_Group \
                            ,Health_Inequalities_SubGroup \
                            ,sum(case when Element_ID = " + str(numerator_id) + " then Value else 0 end) as Numerator \
                            ,sum(case when Element_ID = " + str(denominator_id) + " then Value else 0 end) as Denominator \
                        from df_elements \
                        where Element_ID = " + str(numerator_id) + " or Element_ID = " + str(denominator_id) + " \
                        group by Date \
                            ,Provider_Code \
                            ,Commissioner_Code \
                            ,Setting_Type \
                            ,Health_Inequalities_Group \
                            ,Health_Inequalities_SubGroup"

        else:
            sql_query = "select '" + str(metric_id) + "' as Metric_ID \
                            ,Date \
                            ,Provider_Code \
                            ,Commissioner_Code \
                            ,Setting_Type \
                            ,Health_Inequalities_Group \
                            ,Health_Inequalities_SubGroup \
                            ,sum(case when Element_ID = " + str(numerator_id) + " then Value else 0 end) as Numerator \
                            ,null as Denominator \
                        from df_elements \
                        where Element_ID = " + str(numerator_id) + " \
                        group by Date \
                            ,Provider_Code \
                            ,Commissioner_Code \
                            ,Setting_Type \
                            ,Health_Inequalities_Group \
                            ,Health_Inequalities_SubGroup"

        # execute sql query
        df_metric = spark.sql(sql_query)

        # write output to table 
        df_metrics_out = df_metrics_out.union(df_metric)
    
    # write data
    df_metrics_out.write.mode('overwrite').parquet(out_path)

# COMMAND ----------

### function to aggregate metrics 
def aggregate_metrics(metric_path, out_path): 

    ### load required datasets 
    metric_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/intermediate/metrics"

    # metrics data
    df_metrics = spark.read.parquet(metric_path)

    # provider reference data
    df_ref_prov = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/ref/prov")

    # commissioner reference data
    df_ref_com = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/ref/com")

    # create temp tables to call using sql script
    df_metrics.createOrReplaceTempView("df_metrics");

    df_ref_prov.createOrReplaceTempView("df_ref_prov");

    df_ref_com.createOrReplaceTempView("df_ref_com");

    # create empty data frame to insert into 
    columns = StructType([
    StructField('Metric_ID', StringType(), True),
    StructField('Date', DateType(), True),
    StructField('Level', StringType(), True),
    StructField('Aggregation_Source', StringType(), True),
    StructField('Org_Code', StringType(), True),
    StructField('Setting_Type', StringType(), True),
    StructField('Health_Inequalities_Group', StringType(), True),
    StructField('Health_Inequalities_SubGroup', StringType(), True),
    StructField('Numerator', IntegerType(), True),
    StructField('Denominator', IntegerType(), True)
    ])

    df_metrics_agg_out = spark.createDataFrame(data = [],schema = columns)

    ### provider aggregations

    # provider level
    sql_query = "select Metric_ID \
                ,Date \
                ,'Provider' as Level \
                ,'Provider' as Aggregation_Source \
                ,Provider_Code as Org_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup \
                ,sum(Numerator) as Numerator \
                ,sum(Denominator) as Denominator \
        from df_metrics \
        group by Metric_ID \
                ,Date \
                ,Provider_Code  \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup"

    df_metrics_agg = spark.sql(sql_query)

    df_metrics_agg_out = df_metrics_agg_out.union(df_metrics_agg)

    # icb level
    sql_query = "select Metric_ID \
                ,Date \
                ,'ICB' as Level \
                ,'Provider' as Aggregation_Source \
                ,b.ICB_Code as Org_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup \
                ,sum(Numerator) as Numerator \
                ,sum(Denominator) as Denominator \
        from df_metrics a \
        left join df_ref_prov b on a.Provider_Code = b.ODS_Code \
        group by Metric_ID \
                ,Date \
                ,ICB_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup" 

    df_metrics_agg = spark.sql(sql_query)

    df_metrics_agg_out = df_metrics_agg_out.union(df_metrics_agg)

    # region level
    sql_query = "select Metric_ID \
                ,Date \
                ,'Region' as Level \
                ,'Provider' as Aggregation_Source \
                ,Region_Code as Org_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup \
                ,sum(Numerator) as Numerator \
                ,sum(Denominator) as Denominator \
        from df_metrics a \
        left join df_ref_prov b on a.Provider_Code = b.ODS_Code \
        group by Metric_ID \
                ,Date \
                ,Region_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup" 

    df_metrics_agg = spark.sql(sql_query)

    df_metrics_agg_out = df_metrics_agg_out.union(df_metrics_agg)

    # national level
    sql_query = "select Metric_ID \
                ,Date \
                ,'National' as Level \
                ,'Provider' as Aggregation_Source \
                ,'ENG' as Org_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup \
                ,sum(Numerator) as Numerator \
                ,sum(Denominator) as Denominator \
        from df_metrics \
        group by Metric_ID \
                ,Date \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup" 

    df_metrics_agg = spark.sql(sql_query)

    df_metrics_agg_out = df_metrics_agg_out.union(df_metrics_agg)

    ### commissioner aggregations 
    # provider level
    sql_query = "select Metric_ID \
                ,Date \
                ,'Commissioner' as Level \
                ,'Commissioner' as Aggregation_Source \
                ,Commissioner_Code as Org_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup \
                ,sum(Numerator) as Numerator \
                ,sum(Denominator) as Denominator \
        from df_metrics \
        group by Metric_ID \
                ,Date \
                ,Commissioner_Code  \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup"

    df_metrics_agg = spark.sql(sql_query)

    df_metrics_agg_out = df_metrics_agg_out.union(df_metrics_agg)

    # icb level
    sql_query = "select Metric_ID \
                ,Date \
                ,'ICB' as Level \
                ,'Commissioner' as Aggregation_Source \
                ,b.ICB_Code as Org_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup \
                ,sum(Numerator) as Numerator \
                ,sum(Denominator) as Denominator \
        from df_metrics a \
        left join df_ref_com b on a.Commissioner_Code = b.Organisation_Code \
        group by Metric_ID \
                ,Date \
                ,ICB_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup" 

    df_metrics_agg = spark.sql(sql_query)

    df_metrics_agg_out = df_metrics_agg_out.union(df_metrics_agg)

    # region level
    sql_query = "select Metric_ID \
                ,Date \
                ,'Region' as Level \
                ,'Commissioner' as Aggregation_Source \
                ,Region_Code as Org_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup \
                ,sum(Numerator) as Numerator \
                ,sum(Denominator) as Denominator \
        from df_metrics a \
        left join df_ref_com b on a.Commissioner_Code = b.Organisation_Code \
        group by Metric_ID \
                ,Date \
                ,Region_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup" 

    df_metrics_agg = spark.sql(sql_query)

    df_metrics_agg_out = df_metrics_agg_out.union(df_metrics_agg)

    # national level
    sql_query = "select Metric_ID \
                ,Date \
                ,'National' as Level \
                ,'Commissioner' as Aggregation_Source \
                ,'ENG' as Org_Code \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup \
                ,sum(Numerator) as Numerator \
                ,sum(Denominator) as Denominator \
        from df_metrics \
        group by Metric_ID \
                ,Date \
                ,Setting_Type \
                ,Health_Inequalities_Group \
                ,Health_Inequalities_SubGroup" 

    df_metrics_agg = spark.sql(sql_query)

    df_metrics_agg_out = df_metrics_agg_out.union(df_metrics_agg)

    # write data
    df_metrics_agg_out.write.mode('overwrite').parquet(out_path)
    df_metrics_agg_out.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/intermediate/metrics_agg_raw")

# COMMAND ----------

### function to prepare tableau metric table
def build_tableau_metrics(metric_path, out_path): 

    ### load required datasets 

    # aggregated metrics data
    df_metrics_agg = spark.read.parquet(metric_path)

    # metrics reference data
    df_ref_metrics = spark.read.table("tobacco_ref_metrics")

    # provider reference data
    df_ref_prov = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/ref/prov")

    # commissioner reference data
    df_ref_com = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/ref/com")

    # create temp tables to call using sql script
    df_metrics_agg.createOrReplaceTempView("df_metrics_agg");

    df_ref_metrics.createOrReplaceTempView("df_ref_metrics");

    df_ref_prov.createOrReplaceTempView("df_ref_prov");

    df_ref_com.createOrReplaceTempView("df_ref_com");

    ### calculate quartiles 

    # define query to calculate quartiles
    sql_query = """select Metric_ID 
                    ,Date 
                    ,Setting_Type 
                    ,Aggregation_Source 
                    ,Health_Inequalities_Group
                    ,Health_Inequalities_SubGroup
                    ,Level 
                    ,Org_Code 
                    ,case when sum(Numerator) < 5 and sum(Numerator) > 0 then 0 else cast(round(sum(Numerator)/5.0,0)*5 as int) end as Numerator 
                    ,case when sum(Denominator) < 5 and sum(Denominator) > 0 then 0 else cast(round(sum(Denominator)/5.0,0)*5 as int) end as Denominator 
                    ,ntile(4) over ( partition by Date 
                                        ,Setting_Type 
                                        ,Level 
                                        ,Metric_ID 
                                        ,Aggregation_Source 
                                        ,Health_Inequalities_Group
                                        ,Health_Inequalities_SubGroup
                                        order by cast(case when sum(Numerator) < 5 and sum(Numerator) > 0 then 0 else cast(round(sum(Numerator)/5.0,0)*5 as int) end as float) / nullif(cast(case when sum(Denominator) < 5 and sum(Denominator) > 0 then 0 else cast(round(sum(Denominator)/5.0,0)*5 as int) end  as float),0) asc) as Quartile 
                    from df_metrics_agg 
                    group by Metric_ID 
                            ,Date 
                            ,Setting_Type 
                            ,Aggregation_Source 
                            ,Health_Inequalities_Group
                            ,Health_Inequalities_SubGroup
                            ,Level 
                            ,Org_Code"""

    # execute query
    df_quartiles = spark.sql(sql_query)

    # prepare as table to read into sql
    df_quartiles.createOrReplaceTempView("df_quartiles");

    ### prepare tableau output 

    # define sql query to prepare output 
    sql_query = """select dat.Metric_ID
            ,concat(ref_metrics.Metric_ID, ': ', ref_metrics.Metric_Name) as Metric_Name
            ,ref_metrics.Metric_Category
            ,ref_metrics.Metric_Type
            ,dat.Date
            ,dat.Setting_Type
            ,dat.Aggregation_Source
            ,dat.Health_Inequalities_Group
            ,dat.Health_Inequalities_SubGroup
            ,dat.Level
            ,dat.Org_Code
            ,case when dat.Level = 'Provider' then ref_prov.ODS_NAME
                when dat.Level = 'Sub-ICB' then ref_com.Organisation_Name
                when dat.Level = 'ICB' then ref_icb.ICB_Name
                when dat.Level = 'Region' then ref_reg.Region_Name
                when dat.Level = 'National' then 'England' end as Org_Name
            ,case when dat.Level = 'Provider' then ref_prov.ICB_Code
                when dat.Level = 'Sub-ICB' then ref_com.ICB_Code
                when dat.Level = 'ICB' then ref_icb.ICB_Code end as ICB_Code
            ,case when dat.Level = 'Provider' then ref_prov.ICB_Name
                when dat.Level = 'Sub-ICB' then ref_com.ICB_Name
                when dat.Level = 'ICB' then ref_icb.ICB_Name end as ICB_Name
            ,case when dat.Level = 'Provider' then ref_prov.ICB_Map
                when dat.Level = 'Sub-ICB' then ref_com.ICB_Map
                when dat.Level = 'ICB' then ref_icb.ICB_Map end as ICB_Map
            ,case when dat.Level = 'Provider' then ref_prov.Region_Code
                when dat.Level = 'Sub-ICB' then ref_com.Region_Code
                when dat.Level = 'ICB' then ref_icb.Region_Code
                when dat.Level = 'Region' then ref_reg.Region_Code end as Region_Code
            ,case when dat.Level = 'Provider' then ref_prov.Region_Name
                when dat.Level = 'Sub-ICB' then ref_com.Region_Name
                when dat.Level = 'ICB' then ref_icb.Region_Name
                when dat.Level = 'Region' then ref_reg.Region_Name end as Region_Name
            ,case when dat.Level = 'Provider' then ref_prov.Region_Map
                when dat.Level = 'Sub-ICB' then ref_com.Region_Map
                when dat.Level = 'ICB' then ref_icb.Region_Map
                when dat.Level = 'Region' then ref_reg.Region_Map end as Region_Map
            ,case when sum(dat.Numerator) < 5 and sum(dat.Numerator) > 0 then 0 else cast(round(sum(dat.Numerator)/5.0,0)*5 as int) end as Numerator
            ,ref_metrics.Numerator_Element_Description as Numerator_Description
            ,ref_metrics.Numerator_Element_Source as Numerator_Source
            ,ref_metrics.Numerator_Element_Logic as Numerator_Logic
            ,case when sum(dat.Numerator) < 5 and sum(dat.Numerator) > 0 then 1 else 0 end as Numerator_Suppressed_Flag 
            ,case when sum(dat.Denominator) < 5 and sum(dat.Denominator) > 0 then 0 else cast(round(sum(dat.Denominator)/5.0,0)*5 as int) end as Denominator
            ,ref_metrics.Denominator_Element_Description as Denominator_Description
            ,ref_metrics.Denominator_Element_Logic as Denominator_Logic
            ,ref_metrics.Denominator_Element_Source as Denominator_Source
            ,case when sum(dat.Denominator) < 5 and sum(dat.Denominator) > 0 then 1 else 0 end as Denominator_Suppressed_Flag
            ,Q1.Q1 * 100 as Lower_Quartile
            ,Q3.Q3 * 100 as Upper_Quartile
            ,Q1.Q1_Min * 100 as Min_Value
            ,Q4.Q4 * 100 as Max_Value
            ,Q2.Q2 * 100 as Median
    from df_metrics_agg dat
    left join df_ref_metrics ref_metrics on dat.Metric_ID = ref_metrics.Metric_ID
    left join df_ref_prov ref_prov on dat.Org_Code = ref_prov.ODS_Code
    left join df_ref_com ref_com on dat.Org_Code = ref_com.Organisation_Code
    left join (select distinct ICB_Code as Org_Code, ICB_Code, ICB_Name, ICB_Map, Region_Code, Region_Name, Region_Map from df_ref_prov) ref_icb on dat.Org_Code = ref_icb.Org_Code
    left join (select distinct Region_Code as Org_Code, Region_Code, Region_Name, Region_Map  from df_ref_prov) ref_reg on dat.Org_Code = ref_reg.Org_Code
    left join (
        select Metric_ID
            ,date
            ,Setting_Type
            ,Level
            ,Aggregation_Source
            ,Health_Inequalities_Group
            ,Health_Inequalities_SubGroup
            ,Max(CAST(Numerator as float) / nullif(cast(Denominator as float),0)) Q1
            ,Max(CAST(Numerator as float) / nullif(cast(Denominator as float),0)) Q1_Min
        from df_quartiles
        where Quartile = 1
        group by date
            ,Setting_Type
            ,Level
            ,Metric_ID
            ,Aggregation_Source
            ,Health_Inequalities_Group
            ,Health_Inequalities_SubGroup
        ) Q1 on dat.date = Q1.date
        and dat.Setting_Type = Q1.Setting_Type
        and dat.Level = Q1.Level
        and dat.Metric_ID = Q1.Metric_ID
        and dat.Aggregation_Source = Q1.Aggregation_Source
        and dat.Health_Inequalities_Group = Q1.Health_Inequalities_Group
        and dat.Health_Inequalities_SubGroup = Q1.Health_Inequalities_SubGroup
    left join (
        select Metric_ID
            ,date
            ,Setting_Type
            ,Level
            ,Aggregation_Source
            ,Health_Inequalities_Group
            ,Health_Inequalities_SubGroup
            ,Max(CAST(Numerator as float) / nullif(cast(Denominator as float),0)) Q2
            ,Max(CAST(Numerator as float) / nullif(cast(Denominator as float),0)) Q2_Min
        from df_quartiles
        where Quartile = 2
        group by date
            ,Setting_Type
            ,Level
            ,Metric_ID
            ,Aggregation_Source
            ,Health_Inequalities_Group
            ,Health_Inequalities_SubGroup
        ) Q2 on dat.date = Q2.date
        and dat.Setting_Type = Q2.Setting_Type
        and dat.Level = Q2.Level
        and dat.Metric_ID = Q2.Metric_ID
        and dat.Aggregation_Source = Q2.Aggregation_Source
        and dat.Health_Inequalities_Group = Q2.Health_Inequalities_Group
        and dat.Health_Inequalities_SubGroup = Q2.Health_Inequalities_SubGroup
    left join (
        select Metric_ID
            ,date
            ,Setting_Type
            ,Level
            ,Aggregation_Source
            ,Health_Inequalities_Group
            ,Health_Inequalities_SubGroup
            ,Max(CAST(Numerator as float) / nullif(cast(Denominator as float),0)) Q3
            ,Max(CAST(Numerator as float) / nullif(cast(Denominator as float),0)) Q3_Min
        from df_quartiles
        where Quartile = 3
        group by date
            ,Setting_Type
            ,Level
            ,Metric_ID
            ,Aggregation_Source
            ,Health_Inequalities_Group
            ,Health_Inequalities_SubGroup
        ) Q3 on dat.date = Q3.date
        and dat.Setting_Type = Q3.Setting_Type
        and dat.Level = Q3.Level
        and dat.Metric_ID = Q3.Metric_ID
        and dat.Aggregation_Source = Q3.Aggregation_Source
        and dat.Health_Inequalities_Group = Q3.Health_Inequalities_Group
        and dat.Health_Inequalities_SubGroup = Q3.Health_Inequalities_SubGroup
    left join (
        select Metric_ID
            ,date
            ,Setting_Type
            ,Level
            ,Aggregation_Source
            ,Health_Inequalities_Group
            ,Health_Inequalities_SubGroup
            ,Max(CAST(Numerator as float) / nullif(cast(Denominator as float),0)) Q4
            ,Max(CAST(Numerator as float) / nullif(cast(Denominator as float),0)) Q4_Min
        from df_quartiles
        where Quartile = 4
        group by date
            ,Setting_Type
            ,Level
            ,Metric_ID
            ,Aggregation_Source
            ,Health_Inequalities_Group
            ,Health_Inequalities_SubGroup
        ) Q4 on dat.date = Q4.date
        and dat.Setting_Type = Q4.Setting_Type
        and dat.Level = Q4.Level
        and dat.Metric_ID = Q4.Metric_ID
        and dat.Aggregation_Source = Q4.Aggregation_Source
        and dat.Health_Inequalities_Group = Q4.Health_Inequalities_Group
        and dat.Health_Inequalities_SubGroup = Q4.Health_Inequalities_SubGroup
    and dat.Setting_Type = case when ref_metrics.Metric_Setting = 'Maternity' then ref_metrics.Metric_Setting 
                            else dat.Setting_Type end
    group by dat.Metric_ID
            ,concat(ref_metrics.Metric_ID, ': ', ref_metrics.Metric_Name)
            ,ref_metrics.Metric_Category
            ,ref_metrics.Metric_Type
            ,dat.Date
            ,dat.Setting_Type
            ,dat.Aggregation_Source
            ,dat.Health_Inequalities_Group
            ,dat.Health_Inequalities_SubGroup
            ,dat.Level
            ,dat.Org_Code
            ,case when dat.Level = 'Provider' then ref_prov.ODS_NAME
                when dat.Level = 'Sub-ICB' then ref_com.Organisation_Name
                when dat.Level = 'ICB' then ref_icb.ICB_Name
                when dat.Level = 'Region' then ref_reg.Region_Name
                when dat.Level = 'National' then 'England' end 
            ,case when dat.Level = 'Provider' then ref_prov.ICB_Code
                when dat.Level = 'Sub-ICB' then ref_com.ICB_Code
                when dat.Level = 'ICB' then ref_icb.ICB_Code end
            ,case when dat.Level = 'Provider' then ref_prov.ICB_Name
                when dat.Level = 'Sub-ICB' then ref_com.ICB_Name
                when dat.Level = 'ICB' then ref_icb.ICB_Name end
            ,case when dat.Level = 'Provider' then ref_prov.ICB_Map
                when dat.Level = 'Sub-ICB' then ref_com.ICB_Map
                when dat.Level = 'ICB' then ref_icb.ICB_Map end
            ,case when dat.Level = 'Provider' then ref_prov.Region_Code
                when dat.Level = 'Sub-ICB' then ref_com.Region_Code
                when dat.Level = 'ICB' then ref_icb.Region_Code
                when dat.Level = 'Region' then ref_reg.Region_Code end
            ,case when dat.Level = 'Provider' then ref_prov.Region_Name
                when dat.Level = 'Sub-ICB' then ref_com.Region_Name
                when dat.Level = 'ICB' then ref_icb.Region_Name
                when dat.Level = 'Region' then ref_reg.Region_Name end
            ,case when dat.Level = 'Provider' then ref_prov.Region_Map
                when dat.Level = 'Sub-ICB' then ref_com.Region_Map
                when dat.Level = 'ICB' then ref_icb.Region_Map
                when dat.Level = 'Region' then ref_reg.Region_Map end
            ,ref_metrics.Numerator_Element_Description
            ,ref_metrics.Numerator_Element_Logic
            ,ref_metrics.Numerator_Element_Source
            ,ref_metrics.Denominator_Element_Description
            ,ref_metrics.Denominator_Element_Logic
            ,ref_metrics.Denominator_Element_Source
            ,Q1.Q1 * 100
            ,Q3.Q3 * 100
            ,Q1.Q1_Min * 100
            ,Q4.Q4 * 100
            ,Q2.Q2 * 100"""

    # execute sql
    df_tableau = spark.sql(sql_query)

    # write data
    df_tableau.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv(out_path)

# COMMAND ----------

### function to build tableau dq table
def build_tableau_dq(out_path): 

        ### read in required datasets
        # raw patient level data
        df_patient_raw = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/patient")

        # raw aggregate level data
        df_aggregate_raw = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/aggregate")

        # provider reference data
        df_ref_prov = spark.read.parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/reference/UKHD/ODS/All_Providers_SCD/Published/1/UKHD_ODS_All_Providers_SCD_00000.parquet")
        df_ref_prov = df_ref_prov.filter("Effective_To is null")

        # processed provider reference data
        df_ref_prov_processed = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/ref/prov")

        # expected provider reference data
        df_ref_prov_expctd = spark.read.table("tobacco_provider_ref")

        # sdcs submissions data
        df_sdcs = spark.read.table("tobacco_sdcs")
        df_sdcs.createOrReplaceTempView("df_sdcs");
        sql_query = "select distinct `Reporting Period` as ReportingPeriod, `Organisation Code` as OrganisationCode, `Status Of Submission` as StatusOfSubmission from df_sdcs where `Status Of Submission` = 'Complete'"
        df_sdcs = spark.sql(sql_query)
        df_sdcs.createOrReplaceTempView("df_sdcs");

        # region reference data
        df_ref_region = spark.read.table("tobacco_ref_region")

        # icb reference data#
        df_ref_icb = spark.read.table("tobacco_ref_icb")

        # read sus data
        df_sus = spark.read.option("header","true").option("recursiveFileLookup","true").parquet("abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/SUS/APC/APCS_Core/Published/1/")

        # read in msds data 
        df_msds = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/msds")

        ### create temp tables to call using sql script
        df_patient_raw.createOrReplaceTempView("df_patient_raw");
        df_aggregate_raw.createOrReplaceTempView("df_aggregate_raw");
        df_ref_prov.createOrReplaceTempView("df_ref_prov");
        df_ref_prov_processed.createOrReplaceTempView("df_ref_prov_processed");
        df_ref_prov_expctd.createOrReplaceTempView("df_ref_prov_expctd");
        df_sus.createOrReplaceTempView("df_sus");
        df_msds.createOrReplaceTempView("df_msds");
        df_ref_region.createOrReplaceTempView("df_ref_region");
        df_ref_icb.createOrReplaceTempView("df_ref_icb");


        ### prepare list of distinct reporting periods

        sql_query = """select distinct concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01')  as Reporting_Period			
                        ,'1' as Join_Ref	
                        from df_patient_raw"""

        df_periods = spark.sql(sql_query)
        df_periods.createOrReplaceTempView("df_periods");

        ### prepare base table of all potential submission / provider combinations 

        sql_query = """select distinct Reporting_Period			
                                ,a.Org_Code	
                                ,Organisation_Name
                                ,National_Grouping_Code as Region_Code	
                                ,High_Level_Health_Authority_Code as ICB_Code
                                ,Setting_Type	
                                ,Setting_type as Setting_Type_Split 			
                        from df_ref_prov_expctd a 			
                        left join df_ref_prov b on a.Org_Code = b.Organisation_Code		
                        cross join df_periods		
                        order by Org_Code, Reporting_Period, Setting_Type"""

        df_base = spark.sql(sql_query)
        df_base.createOrReplaceTempView("df_base");

        ### prepare sdcs data

        sql_query = """select distinct cast(concat(right(ReportingPeriod,4),'-',left(right(ReportingPeriod,7),2),'-','01') as timestamp) as Reporting_Period
                                ,left(OrganisationCode,3) as Organisation_Code
                                ,count(distinct Reporting_Period) as Count_Previous_Submissions
                        from df_sdcs a 
                        left join(select distinct  cast(concat(right(ReportingPeriod,4),'-',left(right(ReportingPeriod,7),2),'-','01') as timestamp) as Reporting_Period
                                        ,left(OrganisationCode,3) as Organisation_Code
                                from df_sdcs a
                                where StatusofSubmission = 'Complete') b 
                        on left(a.OrganisationCode,3) = b.Organisation_Code 
                        and  cast(concat(right(ReportingPeriod,4),'-',left(right(ReportingPeriod,7),2),'-','01') as timestamp) > b.Reporting_Period
                        where a.StatusofSubmission = 'Complete'
                        group by cast(concat(right(ReportingPeriod,4),'-',left(right(ReportingPeriod,7),2),'-','01') as timestamp)
                        ,left(OrganisationCode,3)"""

        df_sdcs_prev = spark.sql(sql_query)
        df_sdcs_prev.createOrReplaceTempView("df_sdcs_prev");

        ### prepare aggregate data

        sql_query = """select AuditID			
                ,concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') as Reporting_Period	
                ,ODS_Code
                ,coalesce(COMPLETESMOKINGSTATUS_COMMUNITY,0) + coalesce(COMPLETESMOKINGSTATUS_IP,0) + coalesce(COMPLETESMOKINGSTATUS_MATERNITY,0) + coalesce(COMPLETESMOKINGSTATUS_OP,0) + coalesce(COMPLETESMOKINGSTATUS_COMMUNITY,0) + coalesce(COMPLETESMOKINGSTATUS_COMMUNITY_IP,0) + coalesce(COMPLETESMOKINGSTATUS_MENTALHEALTH_CMHS,0) + coalesce(COMPLETESMOKINGSTATUS_MENTALHEALTH_IP,0) + coalesce(COMPLETESMOKINGSTATUS_MENTALHEALTH_OP,0) as COMPLETE_SMOKING_STATUS
                from df_aggregate_raw"""

        df_agg = spark.sql(sql_query)
        df_agg.createOrReplaceTempView("df_agg");

        ### prepare record level data
        ### Tom - 12/10/23 - changed SEEN_INHOUSE to = "Y" instead of is not null - lines 143 and 144
        sql_query = """select concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') as Reporting_Period			
                        ,c.Region_Code	
                        ,c.ICB_Code
                        ,a.ODS_CODE	
                        ,concat(a.ODS_CODE, ': ', a.ODS_NAME) as ODS_Code_Name	
                        ,case when SETTING_TYPE = '1' and Activity_Type in ('1','2') then 'Physical Acute' 	
                                when SETTING_TYPE = '1' and Activity_Type = '3' then 'Children or Community' 	
                                when SETTING_TYPE = '2' then 'Adult Mental Health' 
                                when SETTING_TYPE = '3' then 'Maternity' end as Setting_Type
                        ,COUNT(*) as Count_Records	
                        ,COUNT(distinct pseudo_nhs_number_ncdr) as Count_Unique_Patients	
                        ,round(cast(SUM(case when Activity_Date is not null then 1 else 0 end) as float) / 
                                cast(COUNT(*) as float),2) as Percent_Date_Valid
                        ,round(cast(SUM(case when Setting_Type = '3' and PregnancyID is not null then 1 
                                                when Setting_Type <> 3 and Hospital_Spell_No is not null then 1 else 0 end) as float) /
                        CAST(Count(*) as float),2) as Percent_Records_Linkable
                        ,round(cast(sum(case when SMOKINGSTATUS is not null then 1 else 0 end) as float) /
                                cast(COUNT(*) as float),2) as Percent_SmokingStatus_Complete	
                        ,ROUND(cast(sum(case when TobaccoDependence_CarePlan is not null and SEEN_INHOUSE = "Y" then 1 else 0 end) as float) /
                                nullif(cast(sum(case when SEEN_INHOUSE = 'Y' then 1 else 0 end) as float),0),2) as Percent_CarePlan_Complete 	
                        ,cast(sum(case when INHOUSE_REFERRAL = 'Y' then 1 else 0 end) as float) as Referrals_Count				
                from df_patient_raw a 			
                LEFT join df_ref_prov_processed c on a.ODS_CODE = c.ODS_Code	
                left join (select distinct Hospital_Spell_No from df_sus) d on a.HOSPITAL_SPELL_ID = d.Hospital_Spell_No
                left join df_msds e on a.PREGNANCY_ID = e.PregnancyID
                group by concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01')			
                        ,c.Region_Code	
                        ,c.ICB_Code
                        ,concat(a.ODS_CODE, ': ', a.ODS_NAME)	
                        ,case when SETTING_TYPE = '1' and Activity_Type in ('1','2') then 'Physical Acute' 	
                                when SETTING_TYPE = '1' and Activity_Type = '3' then 'Children or Community' 	
                                when SETTING_TYPE = '2' then 'Adult Mental Health' 
                                when SETTING_TYPE = '3' then 'Maternity' end
                ,a.ODS_CODE	 """

        df_record = spark.sql(sql_query)
        df_record.createOrReplaceTempView("df_record");

        ### prepare wide staging table 

        sql_query = """select 	base.Reporting_Period
                        ,base.Org_Code
                        ,base.Organisation_Name
                        ,base.Region_Code
                        ,base.ICB_Code
                        ,base.Setting_Type
                        ,sdcs.StatusOfSubmission as SDCS_Submission_Complete
                        ,record.Count_Records as Patient_Records
                        ,record.Count_Unique_Patients as Unique_Patients
                        ,summary.COMPLETE_SMOKING_STATUS as Aggregate_Records
                        ,sdcs_prev.Count_Previous_Submissions
                        ,record.Percent_Date_Valid
                        ,record.Percent_SmokingStatus_Complete
                        ,record.Percent_CarePlan_Complete
                        ,record.Referrals_Count
                        ,record.Percent_Records_Linkable
                        ,case when record.Count_Records >= 10 then 1 else 0 end + 
                        case when summary.COMPLETE_SMOKING_STATUS >= 10 then 1 else 0 end + 
                        case when record.Referrals_Count >= 10 then 1 else 0 end + 
                        case when record.Percent_Date_Valid > 0.75 then 1 else 0 end + 
                        case when record.Percent_SmokingStatus_Complete > 0.75 then 1 else 0 end +
                        case when record.Percent_Records_Linkable > 0.75 then 1 else 0 end +
                        case when record.Percent_CarePlan_Complete > 0.75 then 1 else 0 end as Count_Passes
                from df_base base
                left join df_record record on base.Org_Code = record.ODS_CODE
                                        and base.Reporting_Period = record.Reporting_Period
                                        and base.Setting_Type = record.Setting_Type
                left join df_agg summary on base.Org_Code = summary.ODS_Code
                                        and base.Reporting_Period = summary.Reporting_Period
                left join df_sdcs_prev sdcs_prev on base.Reporting_Period = sdcs_prev.Reporting_Period
                                        and base.Org_Code = sdcs_prev.Organisation_Code 
                left join (select ReportingPeriod as Reporting_Period
                                ,OrganisationCode
                                ,StatusOfSubmission
                        from df_sdcs) sdcs
                on base.Reporting_Period = sdcs.Reporting_Period
                and base.Org_Code = sdcs.OrganisationCode  """

        df_wide = spark.sql(sql_query)
        df_wide.createOrReplaceTempView("df_wide");


        #### prepare long table for output

        sql_query = """select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,b.Region_Code
                        ,b.Region as Region_Name
                        ,c.ICB_Code
                        ,c.ICB_Name
                        ,Setting_Type
                        ,Metric_Name
                        ,Metric_Type
                        ,Value
                        ,DQ_Check
                from 
                (


                select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,Region_Code
                        ,ICB_Code
                        ,Setting_Type
                        ,'Patient Records Submitted' as Metric_Name
                        ,'Count' as Metric_Type
                        ,Patient_Records as Value
                        ,case when Patient_Records >= 10 then 'Pass' else 'Fail' end as DQ_Check
                from df_wide

                union all 

                select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,Region_Code
                        ,ICB_Code
                        ,Setting_Type
                        ,'Unique Patient Records Submitted' as Metric_Name
                        ,'Count' as Metric_Type
                        ,Unique_Patients as Value
                        ,null as DQ_Check
                from df_wide

                union all 

                select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,Region_Code
                        ,ICB_Code
                        ,Setting_Type
                        ,'Aggregate Records Submitted' as Metric_Name
                        ,'Count' as Metric_Type
                        ,Aggregate_Records as Value
                        ,case when Aggregate_Records >= 10 then 'Pass' else 'Fail' end as DQ_Check
                from df_wide

                union all 

                select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,Region_Code
                        ,ICB_Code
                        ,Setting_Type
                        ,'Patient Referrals Submitted' as Metric_Name
                        ,'Count' as Metric_Type
                        ,Referrals_Count as Value
                        ,case when Referrals_Count >= 10 then 'Pass' else 'Fail' end as DQ_Check
                from df_wide

                union all 

                select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,Region_Code
                        ,ICB_Code
                        ,Setting_Type
                        ,'Activity Date Valid' as Metric_Name
                        ,'Proportion' as Metric_Type
                        ,Percent_Date_Valid as Value
                        ,case when Percent_Date_Valid > 0.75 then 'Pass' else 'Fail' end as DQ_Check
                from df_wide

                union all 

                select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,Region_Code
                        ,ICB_Code
                        ,Setting_Type
                        ,'Smoking Status Field Complete' as Metric_Name
                        ,'Proportion' as Metric_Type
                        ,Percent_SmokingStatus_Complete as Value
                        ,case when Percent_SmokingStatus_Complete > 0.75 then 'Pass' else 'Fail' end as DQ_Check
                from df_wide

                union all 

                select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,Region_Code
                        ,ICB_Code
                        ,Setting_Type
                        ,'Care Plan Field Complete' as Metric_Name
                        ,'Proportion' as Metric_Type
                        ,Percent_CarePlan_Complete as Value
                        ,case when Percent_CarePlan_Complete > 0.75 then 'Pass' else 'Fail' end as DQ_Check
                from df_wide

                union all 

                select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,Region_Code
                        ,ICB_Code
                        ,Setting_Type
                        ,'Records Linkable' as Metric_Name
                        ,'Proportion' as Metric_Type
                        ,Percent_Records_Linkable as Value
                        ,case when Percent_Records_Linkable > 0.75 then 'Pass' else 'Fail' end as DQ_Check
                from df_wide

                union all 

                select Reporting_Period
                        ,Org_Code
                        ,Organisation_Name
                        ,Region_Code
                        ,ICB_Code
                        ,Setting_Type
                        ,'DQ Score' as Metric_Name
                        ,'Count' as Metric_Type
                        ,Count_Passes as Value
                        ,case when SDCS_Submission_Complete is not null and Count_Passes = 7 and (Patient_Records > 0 or Patient_Records is not null) then 'A'
                                when SDCS_Submission_Complete is not null and Count_Passes = 6 and (Patient_Records > 0 or Patient_Records is not null) then 'B6'
                                when SDCS_Submission_Complete is not null and Count_Passes = 5 and (Patient_Records > 0 or Patient_Records is not null) then 'B5'
                                when SDCS_Submission_Complete is not null and Count_Passes = 4 and (Patient_Records > 0 or Patient_Records is not null) then 'B4'
                                when SDCS_Submission_Complete is not null and Count_Passes = 3 and (Patient_Records > 0 or Patient_Records is not null) then 'B3'
                                when SDCS_Submission_Complete is not null and Count_Passes = 2 and (Patient_Records > 0 or Patient_Records is not null) then 'B2'
                                when SDCS_Submission_Complete is not null and Count_Passes = 1 and (Patient_Records > 0 or Patient_Records is not null) then 'B1' 
                                when SDCS_Submission_Complete is not null and Count_Passes = 0 and (Patient_Records > 0 or Patient_Records is not null) then 'C0' 
                                when SDCS_Submission_Complete is not null and (Patient_Records = 0 or Patient_Records is null) then 'CX'
                                when SDCS_Submission_Complete is null and Count_Previous_Submissions > 0 then 'DP'
                                when SDCS_Submission_Complete is null and (Count_Previous_Submissions = 0 or Count_Previous_Submissions is null) then 'D0'
                                else 'NA' end as DQ_Check
                from df_wide
                ) a
                left join df_ref_region b on a.Region_Code = b.Region_Code
                left join df_ref_icb c on a.ICB_Code = c.ICB_Code
                        """

        df_out = spark.sql(sql_query)


        df_out.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv(out_path)

# COMMAND ----------

### function to build tableau pathways table
def build_tableau_pathways(out_path, out_path_raw): 

    ### load required datasets 
    # df = spark.read.csv("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/outputs/tobacco_tableau_metrics")
    df = spark.read.format("csv").option("header","true").load("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/outputs/tobacco_tableau_metrics")
    df = df.filter("Health_Inequalities_Group = 'Total'")
    df.createOrReplaceTempView("df")



    sql_query = """select *
        from (
        select 'Link' as link_field
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'28 Days' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers not referred to the in-house service' as Stage_2 
                ,null as Stage_3
                ,null as Stage_4 
                ,null as Stage_5
                ,sum(case when Metric_ID = 'P.020.001' then Numerator else 0 end) - sum(case when Metric_ID = 'T.030.020' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all 

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'28 Days' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are not seen by the service' as Stage_3
                ,null as Stage_4 
                ,null as Stage_5
                ,sum(case when Metric_ID = 'T.030.020' then Numerator else 0 end) - sum(case when Metric_ID = 'T.032.030' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all 

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'28 Days' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
                ,'Smokers seen by the in-house service who have not set a quit date' as Stage_4 
                ,null as Stage_5
                ,sum(case when Metric_ID = 'T.032.030' then Numerator else 0 end) - sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'28 Days' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
                ,'Smokers seen by the in-house service who have set a quit date' as Stage_4 
                ,'Smokers seen by the in-house service who set a quit date and have not quit' as Stage_5
                ,sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.284.061' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all 

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'28 Days' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'On-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
                ,'Smokers seen by the in-house service who have set a quit date' as Stage_4 
                ,'Smokers seen by the in-house service who set a quit date and have quit' as Stage_5
                ,sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - (sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.284.061' then Numerator else 0 end)) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source



        union all 


        select 'Link' as link_field
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'36 Weeks' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers not referred to the in-house service' as Stage_2 
                ,null as Stage_3
                ,null as Stage_4 
                ,null as Stage_5
                ,sum(case when Metric_ID = 'P.020.001' then Numerator else 0 end) - sum(case when Metric_ID = 'T.030.020' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all 

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'36 Weeks' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are not seen by the service' as Stage_3
                ,null as Stage_4 
                ,null as Stage_5
                ,sum(case when Metric_ID = 'T.030.020' then Numerator else 0 end) - sum(case when Metric_ID = 'T.032.030' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all 

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'36 Weeks' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
                ,'Smokers seen by the in-house service who have not set a quit date' as Stage_4 
                ,null as Stage_5
                ,sum(case when Metric_ID = 'T.032.030' then Numerator else 0 end) - sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'36 Weeks' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
                ,'Smokers seen by the in-house service who have set a quit date' as Stage_4 
                ,'Smokers seen by the in-house service who set a quit date and have not quit' as Stage_5
                ,case when Setting_Type = 'Maternity'
                    then sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.490.091' then Numerator else 0 end)
                    else sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.284.061' then Numerator else 0 end) end as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all 

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'36 Weeks' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'On-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
                ,'Smokers seen by the in-house service who have set a quit date' as Stage_4 
                ,'Smokers seen by the in-house service who set a quit date and have quit' as Stage_5
                ,case when Setting_Type = 'Maternity' 
                    then sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - (sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.490.091' then Numerator else 0 end))
                    else sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - (sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.284.061' then Numerator else 0 end)) end as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        
        


        union all 


        select 'Link' as link_field
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'Delivery' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers not referred to the in-house service' as Stage_2 
                ,null as Stage_3
                ,null as Stage_4 
                ,null as Stage_5
                ,sum(case when Metric_ID = 'P.020.001' then Numerator else 0 end) - sum(case when Metric_ID = 'T.030.020' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all 

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'Delivery' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are not seen by the service' as Stage_3
                ,null as Stage_4 
                ,null as Stage_5
                ,sum(case when Metric_ID = 'T.030.020' then Numerator else 0 end) - sum(case when Metric_ID = 'T.032.030' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all 

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'Delivery' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
                ,'Smokers seen by the in-house service who have not set a quit date' as Stage_4 
                ,null as Stage_5
                ,sum(case when Metric_ID = 'T.032.030' then Numerator else 0 end) - sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'Delivery' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'Off-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
                ,'Smokers seen by the in-house service who have set a quit date' as Stage_4 
                ,'Smokers seen by the in-house service who set a quit date and have not quit' as Stage_5
                ,case when Setting_Type = 'Maternity'
                    then sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.492.091' then Numerator else 0 end)
                    else sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.284.061' then Numerator else 0 end) end as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source

        union all 

        select 'Link' as link
                ,Date
                ,Aggregation_Source
                ,Level
                ,Org_Name
                ,Setting_Type
                ,'Delivery' as Quit_Type
                ,sum(Numerator_Suppressed_Flag) as Numerator_Suppressed_Flag
                ,sum(Denominator_Suppressed_Flag) as Denominator_Suppressed_Flag
                ,'On-Pathway' as Pathway_Ref
                ,'Smokers Identified in Hospital' as Stage_1
                ,'Smokers referred to the in-house service' as Stage_2 
                ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
                ,'Smokers seen by the in-house service who have set a quit date' as Stage_4 
                ,'Smokers seen by the in-house service who set a quit date and have quit' as Stage_5
                ,case when Setting_Type = 'Maternity' 
                    then sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - (sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.492.091' then Numerator else 0 end))
                    else sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - (sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.284.061' then Numerator else 0 end)) end as Size
        from df
        where Aggregation_Source = 'Provider'
        group by Date
                ,Level
                ,Org_Name
                ,Setting_Type
                ,Aggregation_Source
        
        ) a """

    df_pathways = spark.sql(sql_query)

    # df_pathways.write.mode('overwrite').parquet(out_path)
    df_pathways.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv(out_path)



    ### raw dataset for data packs
    df = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/intermediate/metrics_agg")

    df = df.filter("Health_Inequalities_Group = 'Total'")
    df.createOrReplaceTempView("df")



    sql_query = """select *
    from (
    select 'Link' as link_field
            ,Date
            ,Aggregation_Source
            ,Level
            ,Org_Code
            ,Setting_Type
            ,'Off-Pathway' as Pathway_Ref
            ,'Smokers Identified in Hospital' as Stage_1
            ,'Smokers not referred to the in-house service' as Stage_2 
            ,null as Stage_3
            ,null as Stage_4 
            ,null as Stage_5
            ,sum(case when Metric_ID = 'P.020.001' then Numerator else 0 end) - sum(case when Metric_ID = 'T.030.020' then Numerator else 0 end) as Size
    from df
    where Aggregation_Source = 'Provider'
    group by Date
            ,Level
            ,Org_Code
            ,Setting_Type
            ,Aggregation_Source

    union all 

    select 'Link' as link
            ,Date
            ,Aggregation_Source
            ,Level
            ,Org_Code
            ,Setting_Type
            ,'Off-Pathway' as Pathway_Ref
            ,'Smokers Identified in Hospital' as Stage_1
            ,'Smokers referred to the in-house service' as Stage_2 
            ,'Smokers referred to the in-house service that are not seen by the service' as Stage_3
            ,null as Stage_4 
            ,null as Stage_5
            ,sum(case when Metric_ID = 'T.030.020' then Numerator else 0 end) - sum(case when Metric_ID = 'T.032.030' then Numerator else 0 end) as Size
    from df
    where Aggregation_Source = 'Provider'
    group by Date
            ,Level
            ,Org_Code
            ,Setting_Type
            ,Aggregation_Source

    union all 

    select 'Link' as link
            ,Date
            ,Aggregation_Source
            ,Level
            ,Org_Code
            ,Setting_Type
            ,'Off-Pathway' as Pathway_Ref
            ,'Smokers Identified in Hospital' as Stage_1
            ,'Smokers referred to the in-house service' as Stage_2 
            ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
            ,'Smokers seen by the in-house service who have not set a quit date' as Stage_4 
            ,null as Stage_5
            ,sum(case when Metric_ID = 'T.032.030' then Numerator else 0 end) - sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) as Size
    from df
    where Aggregation_Source = 'Provider'
    group by Date
            ,Level
            ,Org_Code
            ,Setting_Type
            ,Aggregation_Source

    union all

    select 'Link' as link
            ,Date
            ,Aggregation_Source
            ,Level
            ,Org_Code
            ,Setting_Type
            ,'Off-Pathway' as Pathway_Ref
            ,'Smokers Identified in Hospital' as Stage_1
            ,'Smokers referred to the in-house service' as Stage_2 
            ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
            ,'Smokers seen by the in-house service who have set a quit date' as Stage_4 
            ,'Smokers seen by the in-house service who set a quit date and have not quit' as Stage_5
            ,sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.284.061' then Numerator else 0 end) as Size
    from df
    where Aggregation_Source = 'Provider'
    group by Date
            ,Level
            ,Org_Code
            ,Setting_Type
            ,Aggregation_Source

    union all 

    select 'Link' as link
            ,Date
            ,Aggregation_Source
            ,Level
            ,Org_Code
            ,Setting_Type
            ,'On-Pathway' as Pathway_Ref
            ,'Smokers Identified in Hospital' as Stage_1
            ,'Smokers referred to the in-house service' as Stage_2 
            ,'Smokers referred to the in-house service that are seen by the service' as Stage_3
            ,'Smokers seen by the in-house service who have set a quit date' as Stage_4 
            ,'Smokers seen by the in-house service who set a quit date and have quit' as Stage_5
            ,sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - (sum(case when Metric_ID = 'T.061.032' then Numerator else 0 end) - sum(case when Metric_ID = 'O.284.061' then Numerator else 0 end)) as Size
    from df
    where Aggregation_Source = 'Provider'
    group by Date
            ,Level
            ,Org_Code
            ,Setting_Type
            ,Aggregation_Source
    ) a """

    df_pathways = spark.sql(sql_query)

    # df_pathways.write.mode('overwrite').parquet(out_path)
    df_pathways.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv(out_path_raw)

# COMMAND ----------

### function to build tableau coverage table
def build_sof(min_date, max_date, out_path):

    ### load required datasets
    df_ref_prov_expctd = spark.read.table("tobacco_provider_ref")

    df_pld_clean = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/clean/patient")

    df_ref_prov = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/ref/prov")

    ### create temp tables to call
    df_ref_prov_expctd.createOrReplaceTempView("df_ref_prov_expctd");
    df_pld_clean.createOrReplaceTempView("df_pld_clean");
    df_ref_prov.createOrReplaceTempView("df_ref_prov");

    ### define months to prepare data for
    sql_query = "SELECT sequence(to_date('" + str(min_date) + "'), to_date('" + str(max_date) + "'), interval 1 month) as date"
    df_months = spark.sql(sql_query)
    df_months = spark.sql(sql_query).withColumn("date", explode(col("date")))
    df_months = df_months.withColumn("join_ref", lit(1))
    df_months.createOrReplaceTempView("df_months");

    ### get count of providers per ICB setting type
    sql_query = """select b.ICB_Code
                            ,case when Setting_Type in ('Children or Community', 'Physical Acute', 'Adult Mental Health') then 'Inpatient' 
                            else Setting_Type end as Setting_Type
                            ,COUNT(Org_Code) as Provider_Count
                            ,'1' as Join_Ref
                    from df_ref_prov_expctd a 
                    left join df_ref_prov b on a.Org_Code = b.ODS_Code
                    where b.ICB_Code is not null
                    group by b.ICB_Code
                            ,case when Setting_Type in ('Children or Community', 'Physical Acute', 'Adult Mental Health') then 'Inpatient' 
                    else Setting_Type end"""

    df_prov = spark.sql(sql_query)
    df_prov.createOrReplaceTempView("df_prov");

    ### join prov and month tables for base table 
    sql_query = """select ICB_Code
                            ,Setting_Type
                            ,date as Activity_Month
                            ,Provider_Count
                    from df_prov a 
                    left join df_months b on a.Join_Ref = b.Join_Ref"""

    df_base = spark.sql(sql_query)
    df_base.createOrReplaceTempView("df_base");

    ### get count of in house referrals per provider per month  
    sql_query = """select ODS_CODE
                            ,case when a.SETTING_TYPE in ('1','2') and Activity_Type = '1' then 'Inpatient'
                                when a.SETTING_TYPE = '3' then 'Maternity'
                                else NULL end as Setting_Type
                            ,concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01')  as Activity_Month
                            ,sum(case when INHOUSE_REFERRAL = 'Y' then 1 else 0 end) as INHOUSE_REFERRAL_COUNT
                    from df_pld_clean a
                    where ((a.Setting_Type in ('1','2') and Activity_Type in ('1'))
                    OR a.Setting_Type = '3')
                    group by ODS_CODE
                            ,concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') 
                            ,case when a.SETTING_TYPE in ('1','2') and Activity_Type = '1' then 'Inpatient'
                                when a.SETTING_TYPE = '3' then 'Maternity'
                    else NULL end"""

    df_referrals = spark.sql(sql_query)
    df_referrals.createOrReplaceTempView("df_referrals");

    ### assess which combinations have > 10 referrals 
    sql_query = """select b.ICB_Code
                        ,a.Setting_Type
                        ,Activity_Month
                        ,sum(case when INHOUSE_REFERRAL_COUNT >= 10 then 1 else 0 end) as INHOUSE_REFERRAL_FLAG_COUNT
                from df_referrals a 
                left join (select Org_Code
                        ,case when Setting_Type in ('Children or Community', 'Physical Acute','Adult Mental Health') then 'Inpatient' 
                        else Setting_Type end as Setting_Type
                        ,ICB_Code
                from df_ref_prov_expctd a
                left join df_ref_prov b on a.Org_Code = b.ODS_Code
                where ICB_Code is not null) b on a.ODS_CODE = b.Org_Code and a.Setting_Type = b.Setting_Type
                group by b.ICB_Code
                        ,a.Setting_Type
                ,Activity_Month"""

    df_referrals = spark.sql(sql_query)
    df_referrals.createOrReplaceTempView("df_referrals");

    ### join reference data, calculated data and summarise by ICB
    sql_query = """select a.ICB_Code
                    ,c.ICB_Name
                    ,a.Setting_Type
                    ,a.Activity_Month
                    ,Coalesce(INHOUSE_REFERRAL_FLAG_COUNT,0) as Numerator
                    ,Provider_Count as Denominator
                    ,coalesce(cast(cast(INHOUSE_REFERRAL_FLAG_COUNT as decimal(5,2)) / cast(Provider_Count as decimal(5,2)) as decimal(10,4)),0) as Proportion
            from df_base a 
            left join df_referrals b on a.ICB_Code = b.ICB_Code and a.Activity_Month = b.Activity_Month and a.Setting_Type = b.Setting_Type
            left join (select distinct ICB_Code
                                    ,ICB_Name
                                        from df_ref_prov) c on a.ICB_Code = c.ICB_Code"""
    df_staging = spark.sql(sql_query)
    df_staging.createOrReplaceTempView("df_staging");

    ### prepare table in requested output format
    sql_query = """select MetricID
                            ,MetricName
                            ,Domain
                            ,OrgCode
                            ,OrgType
                            ,Activity_Month
                            ,'Monthly' as PeriodType
                            ,MetricType
                            ,Value
                            ,Last_Modified
                    from
                    (select 'S116a' as MetricID
                            ,'Proportion of adult acute inpatient settings offering smoking cessation services' as MetricName
                            ,'Quality of care, access and outcomes' as Domain
                            ,'ICB' as OrgType
                            ,ICB_Code as OrgCode
                            ,Activity_Month
                            ,'Num' as MetricType
                            ,Numerator as Value 
                            ,current_timestamp() as Last_Modified 
                    from df_staging
                    where Setting_Type = 'Inpatient'

                    union all 

                    select 'S116a' as MetricID
                            ,'Proportion of adult acute inpatient settings offering smoking cessation services' as MetricName
                            ,'Quality of care, access and outcomes' as Domain
                            ,'ICB' as OrgType
                            ,ICB_Code as OrgCode
                            ,Activity_Month
                            ,'Den' as MetricType
                            ,Denominator as Value 
                            ,current_timestamp() as Last_Modified 
                    from df_staging
                    where Setting_Type = 'Inpatient'

                    union all 

                    select 'S116a' as MetricID
                            ,'Proportion of adult acute inpatient settings offering smoking cessation services' as MetricName
                            ,'Quality of care, access and outcomes' as Domain
                            ,'ICB' as OrgType
                            ,ICB_Code as OrgCode
                            ,Activity_Month
                            ,'Rate' as MetricType
                            ,Proportion as Value 
                            ,current_timestamp() as Last_Modified 
                    from df_staging
                    where Setting_Type = 'Inpatient'

                    union all 

                    select 'S116b' as MetricID
                            ,'Proportion of maternity inpatient settings offering smoking cessation services' as MetricName
                            ,'Quality of care, access and outcomes' as Domain
                            ,'ICB' as OrgType
                            ,ICB_Code as OrgCode
                            ,Activity_Month
                            ,'Num' as MetricType
                            ,Numerator as Value 
                            ,current_timestamp() as Last_Modified 
                    from df_staging
                    where Setting_Type = 'Maternity'

                    union all 

                    select 'S116b' as MetricID
                            ,'Proportion of maternity inpatient settings offering smoking cessation services' as MetricName
                            ,'Quality of care, access and outcomes' as Domain
                            ,'ICB' as OrgType
                            ,ICB_Code as OrgCode
                            ,Activity_Month
                            ,'Den' as MetricType
                            ,Denominator as Value 
                            ,current_timestamp() as Last_Modified 
                    from df_staging
                    where Setting_Type = 'Maternity'

                    union all 

                    select 'S116b' as MetricID
                            ,'Proportion of maternity inpatient settings offering smoking cessation services' as MetricName
                            ,'Quality of care, access and outcomes' as Domain
                            ,'ICB' as OrgType
                            ,ICB_Code as OrgCode
                            ,Activity_Month
                            ,'Rate' as MetricType
                            ,Proportion as Value 
                            ,current_timestamp() as Last_Modified 
                    from df_staging
                    where Setting_Type = 'Maternity'

                    ) a"""

    df_out = spark.sql(sql_query)

    df_out = df_out.withColumn("Year",substring("Activity_Month",1,4)).withColumn("Month",substring("Activity_Month",6,2))
    df_out = df_out.withColumn("Period", concat_ws(" ", "Year", "Month"))
    df_out = df_out.select("MetricID", "MetricName", "Domain", "OrgCode", "OrgType", "Period", "PeriodType", "MetricType", "Value", "Last_Modified")

    df_out.coalesce(1).write.mode('overwrite').options(header='True', delimiter=',').csv(out_path)

# COMMAND ----------

### build performance report 

## reference data
df_patient_raw = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/patient")
df_ref_prov_expctd = spark.read.table("tobacco_provider_ref")
df_dq = spark.read.format("csv").option("header","true").load("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/outputs/tobacco_tableau_dq")
df_sus = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/sus")
df_ref_prov = spark.read.parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/reference/UKHD/ODS/All_Providers_SCD/Published/1/UKHD_ODS_All_Providers_SCD_00000.parquet")
df_ref_prov = df_ref_prov.filter("Effective_To is null")
df_msds = spark.read.parquet("abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/msds")
df_ref_prev = spark.read.table("tobacco_ref_referrals")
df_dqmi = spark.read.table("tobacco_dqmi")


df_patient_raw.createOrReplaceTempView("df_patient_raw");
df_ref_prov_expctd.createOrReplaceTempView("df_ref_prov_excptd");
df_dq.createOrReplaceTempView("df_dq");
df_sus.createOrReplaceTempView("df_sus");
df_ref_prov.createOrReplaceTempView("df_ref_prov");
df_msds.createOrReplaceTempView("df_msds");
df_ref_prev.createOrReplaceTempView("df_ref_prev");
df_dqmi.createOrReplaceTempView("df_dqmi");


# prepare tobacco dependence record level data
sql_query = """ select concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') as Reporting_Period
                        ,ODS_Code
                        ,ODS_Name
                        ,case when SETTING_TYPE = '1' and Activity_Type in ('1','2') then 'Physical Acute' 	
                                when SETTING_TYPE = '1' and Activity_Type = '3' then 'Children or Community' 	
                                when SETTING_TYPE = '2' then 'Adult Mental Health' 
                                when SETTING_TYPE = '3' then 'Maternity' else 'Other' end as Setting_Type
                        ,count(*) as Count_Smokers
                        from df_patient_raw
                        where SmokingStatus = '1'
                        group by concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01')
                        ,ODS_Code
                        ,ODS_Name
                        ,case when SETTING_TYPE = '1' and Activity_Type in ('1','2') then 'Physical Acute' 	
                                when SETTING_TYPE = '1' and Activity_Type = '3' then 'Children or Community' 	
                                when SETTING_TYPE = '2' then 'Adult Mental Health' 
                                when SETTING_TYPE = '3' then 'Maternity' else 'Other' end"""

df_smokers = spark.sql(sql_query)    
df_smokers.createOrReplaceTempView("df_smokers");
                          
                    
sql_query = """select concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') as Reporting_Period
                        ,ODS_Code
                        ,ODS_Name
                        ,case when SETTING_TYPE = '1' and Activity_Type in ('1','2') then 'Physical Acute' 	
                                when SETTING_TYPE = '1' and Activity_Type = '3' then 'Children or Community' 	
                                when SETTING_TYPE = '2' then 'Adult Mental Health' 
                                when SETTING_TYPE = '3' then 'Maternity' else 'Other' end as Setting_Type
                        ,count(*) as Count_Smokers_Referred
                        from df_patient_raw
                        where SmokingStatus = '1'
                        and Inhouse_Referral = 'Y'
                        group by concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01')
                        ,ODS_Code
                        ,ODS_Name
                        ,case when SETTING_TYPE = '1' and Activity_Type in ('1','2') then 'Physical Acute' 	
                                when SETTING_TYPE = '1' and Activity_Type = '3' then 'Children or Community' 	
                                when SETTING_TYPE = '2' then 'Adult Mental Health' 
                                when SETTING_TYPE = '3' then 'Maternity' else 'Other' end """

df_smokers_referred = spark.sql(sql_query)    
df_smokers_referred.createOrReplaceTempView("df_smokers_referred");                                
                    
sql_query = """select concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') as Reporting_Period
                        ,ODS_Code
                        ,ODS_Name
                        ,case when SETTING_TYPE = '1' and Activity_Type in ('1','2') then 'Physical Acute' 	
                                when SETTING_TYPE = '1' and Activity_Type = '3' then 'Children or Community' 	
                                when SETTING_TYPE = '2' then 'Adult Mental Health' 
                                when SETTING_TYPE = '3' then 'Maternity' else 'Other' end as Setting_Type
                        ,count(*) as Count_Smokers_Seen
                        from df_patient_raw
                        where SmokingStatus = '1'
                        and InHouse_Referral = 'Y'
                        and Seen_InHouse = 'Y'
                        group by concat(right(Reporting_Period,4),'-',left(right(Reporting_Period,7),2),'-','01') 
                        ,ODS_Code
                        ,ODS_Name
                        ,case when SETTING_TYPE = '1' and Activity_Type in ('1','2') then 'Physical Acute' 	
                                when SETTING_TYPE = '1' and Activity_Type = '3' then 'Children or Community' 	
                                when SETTING_TYPE = '2' then 'Adult Mental Health' 
                                when SETTING_TYPE = '3' then 'Maternity' else 'Other' end """

df_smokers_seen = spark.sql(sql_query)    
df_smokers_seen.createOrReplaceTempView("df_smokers_seen");

### prepare dq data
sql_query = """select *
                from df_dq
                where Metric_Name = 'DQ Score'"""

df_dq = spark.sql(sql_query)
df_dq.createOrReplaceTempView("df_dq");



### query ip data
sql_query = """select Provider_Current as Org_Code
                    ,'Physical Acute' as Setting_Type
                    ,avg(Patients) as patient_Count
                    from

                        (select cast(concat(Year(Date), '-', Month(Date), '-', '01') as date) as Date 
                              		,Provider_Current
                              		,count(Patients) as Patients
                              from (
                              
                              	select min(Attendance_date) as Date			
                              			,case when Provider = 'RA4' then Provider else Provider_Current end	as Provider_Current
                              			,Patient_ID as Patients	
                              		from df_sus a 			
                              		inner join (select * from df_ref_prov) c 
                              				on a.Provider_Current = c.Organisation_Code			
                              		where Attendance_Date between '2022-04-01' and '2023-03-31'			
                              		and Dimention_4 = 'Specific Acute'			
                              		and left(PAT_Commissioner_Type,1) in ('1','2','3')			
                              		and Dimention_1 in ('Ord. Elective Admission', 'Other Non-Elective Admission', 'Emergency Admission')			
                              		and Record_Classification <> 'Inpatient - Unclassified Spell'			
                              		and Age > 17			
                              		and LOS_Adjusted > 0			
                              	group by case when Provider = 'RA4' then Provider else Provider_Current end
                              				,Patient_ID
                              
                              ) a 
                              group by cast(concat(Year(Date), '-', Month(Date), '-', '01') as date)
                              		,Provider_Current ) b
                                    
                    group by Provider_Current"""
                                
df_ip = spark.sql(sql_query)
df_ip.createOrReplaceTempView("df_ip");


### query mh data
sql_query = """select Provider_Current as Org_Code
                    ,'Adult Mental Health' as Setting_Type
                    ,avg(Patients) as patient_Count
                    from

                        (select cast(concat(Year(Date), '-', Month(Date), '-', '01') as date) as Date 
                              		,Provider_Current
                              		,count(Patients) as Patients
                              from (
                              
                              	select min(Attendance_date) as Date			
                              			,case when Provider = 'RA4' then Provider else Provider_Current end	as Provider_Current
                              			,Patient_ID as Patients	
                              		from df_sus a 			
                              		inner join (select * from df_ref_prov) c 
                              				on a.Provider_Current = c.Organisation_Code			
                              		where Attendance_Date between '2022-04-01' and '2023-03-31'			
                              		and Dimention_4 = 'MH and LD'			
                              		and left(PAT_Commissioner_Type,1) in ('1','2','3')			
                              		and Dimention_1 in ('Ord. Elective Admission', 'Other Non-Elective Admission', 'Emergency Admission')			
                              		and Record_Classification <> 'Inpatient - Unclassified Spell'			
                              		and Age > 17			
                              		and LOS_Adjusted > 0			
                              	group by case when Provider = 'RA4' then Provider else Provider_Current end
                              				,Patient_ID
                              
                              ) a 
                              group by cast(concat(Year(Date), '-', Month(Date), '-', '01') as date)
                              		,Provider_Current ) b
                                    
                    group by Provider_Current"""

df_mh = spark.sql(sql_query)
df_mh.createOrReplaceTempView("df_mh");


### query maternity data       
sql_query = """select Org_Code
                        ,'Maternity' as Setting_Type
                        ,avg(Value) as patient_Count
                from 

                (select cast(concat(Year(MinBookingDate), '-', Month(MinBookingDate), '-', '01') as date) as Date \
                ,OrgCodeProvider as Org_Code \
                ,Count(*) as Value \
                from df_msds a \
                group by OrgCodeProvider \
                ,cast(concat(Year(MinBookingDate), '-', Month(MinBookingDate), '-', '01') as date)) a
                
                group by Org_Code"""

df_mat = spark.sql(sql_query)
df_mat.createOrReplaceTempView("df_mat");


### combine patient data
sql_query = """select a.Org_Code
                        ,a.Setting_Type
                        ,a.patient_count
                        ,b.expected_prevalence
                        ,a.patient_count * b.expected_prevalence as Estimated_Smokers
                from 
            (
            select * from df_ip 

            union all 

            select * from df_mat

            union all 

            select * from df_mh) a 
            left join df_ref_prev b on a.Org_Code = b.Org_Code """

df_patients = spark.sql(sql_query) 
df_patients.createOrReplaceTempView("df_patients");

#### prepare dqmi data
sql_query = """ select distinct `Data Provider Code` as Org_Code
                        ,'2023-08-01' as Reporting_Period
                        ,DQMI as DQMI_Score
                from df_dqmi 
                where Dataset = 'Tobacco Dependency'
                and `Reporting Period` = '01/07/2023'"""

df_dqmi = spark.sql(sql_query)
df_dqmi.createOrReplaceTempView("df_dqmi");

### prepare output
sql_query = """select concat(a.Org_Code, a.Setting_Type) as Lookup
                        ,a.Reporting_Period
                        ,a.Org_Code
                        ,a.Setting_Type
                        ,case when b.Count_Smokers < 5 and b.Count_Smokers > 0 then 99999 else cast(round(b.Count_Smokers/5.0,0)*5 as int) end as Count_Smokers_Submitted
                        ,case when e.Estimated_Smokers < 5 and e.Estimated_Smokers > 0 then 99999 else cast(round(e.Estimated_Smokers/5.0,0)*5 as int) end as Count_Smokers_Estimated        
                        ,case when c.Count_Smokers_Referred < 5 and c.Count_Smokers_Referred > 0 then 99999 else cast(round(c.Count_Smokers_Referred/5.0,0)*5 as int) end as Count_Referrals
                        ,case when d.Count_Smokers_Seen < 5 and d.Count_Smokers_Seen > 0 then 99999 else cast(round(d.Count_Smokers_Seen/5.0,0)*5 as int) end as Count_Referrals_Seen
                        ,case when d.Count_Smokers_Seen < 5 and d.Count_Smokers_Seen > 0 then 0 else cast(round(d.Count_Smokers_Seen/5.0,0)*5 as int) end / case when e.Estimated_Smokers < 5 and e.Estimated_Smokers > 0 then 0 else cast(round(e.Estimated_Smokers/5.0,0)*5 as int) end as `% of Expected Throughput`
                        ,f.DQMI_Score
                        ,a.DQ_Check
                from df_dq a 
                left join df_smokers b on a.Org_Code = b.ODS_Code and a.Setting_Type = b.Setting_Type and a.Reporting_Period = b.Reporting_Period
                left join df_smokers_referred c on a.Org_Code = c.ODS_Code and a.Setting_Type = c.Setting_Type and a.Reporting_Period = c.Reporting_Period
                left join df_smokers_seen d on a.Org_Code = d.ODS_Code and a.Setting_Type = d.Setting_Type and a.Reporting_Period = d.Reporting_Period
                left join df_patients e on a.Org_Code = e.Org_Code and a.Setting_Type = e.Setting_Type
                left join df_dqmi f on a.Org_Code = f.Org_Code"""

df_out = spark.sql(sql_query)

display(df_out)



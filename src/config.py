# Databricks notebook source
### define parameters to read in data

# dates
min_date = "2022-04-01"
max_date = "2023-08-31"

# patient level data paths
root_path_patient = "abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/MESH/Tobacco/TobaccoDependenceRecord/Published/1/"
schema_path_patient = "abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/MESH/Tobacco/TobaccoDependenceRecord/Published/1/2023/01/MESH_Tobacco_TobaccoDependenceRecord_202301_00000.parquet"
file_prefix_patient = "MESH_Tobacco_TobaccoDependenceRecord_"
raw_path_patient = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/patient"
clean_path_patient = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/clean/patient"

# aggregate data paths
root_path_aggregate = "abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/MESH/Tobacco/TobaccoDependenceSummary/Published/1/"
schema_path_aggregate = "abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/patientlevel/MESH/Tobacco/TobaccoDependenceSummary/Published/1/2023/01/MESH_Tobacco_TobaccoDependenceSummary_202301_00000.parquet"
file_prefix_aggregate = "MESH_Tobacco_TobaccoDependenceSummary_"
raw_path_aggregate = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/aggregate"

# reference data paths 
ref_prov_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/ref/prov"
ref_com_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/ref/com"

# sus data paths (PAT intermediate)
sus_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/sus"

# msds data paths
msds_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/inputs/raw/msds"

# manipulated data paths
element_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/intermediate/elements"
metric_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/intermediate/metrics"
metric_aggregate_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/intermediate/metrics_agg"
tableau_metric_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/outputs/tobacco_tableau_metrics"
tableau_dq_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/outputs/tobacco_tableau_dq"
tableau_pathways_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/outputs/tobacco_tableau_pathways"
tableau_pathways_raw_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/outputs/tobacco_tableau_pathways_raw"
sof_path = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/AlcoholTobacco/tobacco/outputs/tobacco_sof"
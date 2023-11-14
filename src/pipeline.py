# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

### collect data
# patient level
#prepare_source_parquets(root_path = root_path_patient,
 #                       schema_path = schema_path_patient,
  #                      file_prefix = file_prefix_patient,
   #                     out_path = raw_path_patient,
    #                    min_date = min_date,
     #                   max_date = max_date)

# patient level
read_pld(out_path = raw_path_patient,
        min_date = min_date,
        max_date = max_date)

# aggregate level
prepare_source_parquets(root_path = root_path_aggregate,
                        schema_path = schema_path_aggregate,
                        file_prefix = file_prefix_aggregate,
                        out_path = raw_path_aggregate,
                        min_date = min_date,
                        max_date = max_date)

# patient level tobacco data
clean_patient(raw_path = raw_path_patient,
              out_path = clean_path_patient,
              min_date = min_date,
              max_date = max_date)

# sus data
read_sus(min_date = min_date,
         max_date = max_date,
         out_path = sus_path)

# msds data 
read_msds(min_date = min_date,
          max_date = max_date, 
          out_path = msds_path)

# COMMAND ----------

### build provider reference data
provider_ref(clean_path = clean_path_patient,
             out_path = ref_prov_path)

# COMMAND ----------

### build commissioner reference data
commissioner_ref(clean_path = clean_path_patient,
                 out_path = ref_com_path)

# COMMAND ----------

### build elements
build_elements(clean_path = clean_path_patient,
               out_path = element_path)

# COMMAND ----------

### build metrics
build_metrics(element_path = element_path,
              out_path = metric_path)

# COMMAND ----------

### aggregate data
aggregate_metrics(metric_path = metric_path, 
                  out_path = metric_aggregate_path)

# COMMAND ----------

### build tableau metric dataset
build_tableau_metrics(metric_path = metric_aggregate_path,
                      out_path = tableau_metric_path)

# COMMAND ----------

### build tableau dq dataset
build_tableau_dq(out_path = tableau_dq_path)

# COMMAND ----------

### build tableau pathways dataset
build_tableau_pathways(out_path = tableau_pathways_path,
                       out_path_raw = tableau_pathways_raw_path)

# COMMAND ----------

### build sof measures
build_sof(min_date = min_date,
          max_date = max_date,
          out_path = sof_path)
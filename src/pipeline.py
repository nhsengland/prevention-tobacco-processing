%run ./config
%run ./functions
# collect patient level data
read_pld()
# collect aggregate level data
read_agg()
# clean patient level tobacco data
clean_patient()
### build provider reference data
provider_ref()
### build commissioner reference data
commissioner_ref()
# collect msds data 
read_msds()
# collect mhsds data 
read_mhsds()
# collect sus apc data
read_sus()
### build elements
build_elements()
### build metrics
build_metrics()
### aggregate data
aggregate_metrics()
### build tableau metric dataset
build_tableau_metrics()
### build tableau dq dataset
build_tableau_dq()
### build tableau pathways dataset
build_tableau_pathways()

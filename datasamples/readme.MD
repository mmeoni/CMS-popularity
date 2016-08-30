Data Samples:

- T_XRD_RAW_FILE_AAA_2015.zip contains 698105 samples of CMS job logs for AAA XrootD federation (2015 data).
Columns are:
_corrupt_record,app_info,client_domain,client_host,end_time,file_lfn,file_size,read_average,read_bytes,read_bytes_at_close,read_max,read_min,read_operations,read_sigma,read_single_average,read_single_bytes,read_single_max,read_single_min,read_single_operations,read_single_sigma,read_vector_average,read_vector_bytes,read_vector_count_average,read_vector_count_max,read_vector_count_min,read_vector_count_sigma,read_vector_max,read_vector_min,read_vector_operations,read_vector_sigma,server_domain,server_host,server_site,server_username,start_time,unique_id,user_dn,user_fqan,user_protocol,user_role,user_vo,write_average,write_bytes,write_bytes_at_close,write_max,write_min,write_operations,write_sigma

- DATAFRAME_AAA_2015.zip contains 503182 samples of CMS popularity data (2015) used to train ML classifiers.
Features are:
week:Int, dataset_name:String, primary_dataset:String, processed_dataset:String, acquisition_era:String, processing_version:String, datatier:String, client_domain:String, username:String, server_domain:String, server_site:String, user_protocol:String, dataset_size:Double, numaccesses:Double, proctime:Double, readbytes:Double

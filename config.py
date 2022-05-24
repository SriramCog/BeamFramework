#Config_file_content

import pyarrow
import apache_beam as beam

pubsub_config = {
    'project':'', #Project_id of the running project
    'subscription':'' #id of the pubsub subscription
    }

dataflowrunner_config = {
    'project':'', #Project_id of the running project
    'runner':'DataflowRunner', #Specify the runner name of the enviroinment 
    #refer, https://beam.apache.org/documentation/runners/dataflow/ for available runners
    'jobname':'', #name of the dataflow job only accepts type [a-z0-9] 
    'temp_bucket':'', #Temp bucket/path location for processing 'gs://folder1/folder2/' format
    'staging_bucket':'', #Staging bucket/path location for processing 'gs://folder1/folder2' format
    'region':'', #region of dataflow runner
    'streaming_ind':True, #Boolean - Specifies the job is Streaming or not change to False for Batch workloads
    'save_main_session_ind':True, #Boolean - Leave it as True, Specify the function param's outside the __main__ session. False for Interactive runner
    }

write_config_json = {
    'filepath+filename':'/content/writeoutput/final_output',
    'filesuffix':'.json',
    'shard':0,
    'shardtype':'',
    'header':None
}
write_config_txt = {
    'filepath+filename':'/content/writeoutput/final_output',
    'filesuffix':'.txt',
    'shard':0,
    'shardtype':'',
    'header':None
}
write_config_csv = {
    'filepath+filename':'/content/writeoutput/final_output',
    'filesuffix':'.csv',
    'shard':0,
    'shardtype':'',
    'header':None
}

write_config_parquet = {
'filepath+filename':'/content/writeoutput/final_output',
'filesuffix':'.parquet',
'shard':1,
'shardtype':'',
'buffer':134217728,
'batch':500,
'code':'none',
'deprecated':'false',
'schema_table': pyarrow.schema([('registration_dttm', pyarrow.timestamp('ns')),
								('id', pyarrow.int32()), ('first_name', pyarrow.string()),('last_name', pyarrow.string()),
								('email', pyarrow.string()),
								('gender', pyarrow.string()),('ip_address', pyarrow.string()),('cc', pyarrow.string()),
								('country', pyarrow.string()),('birthdate', pyarrow.string()),('salary', pyarrow.int32()),
								('title', pyarrow.string()),('comments', pyarrow.string())])}
								
								
write_config_avro = {
'filepath+filename':'/content/writeoutput/final_output',
'filesuffix':'.avro',
'shard': 0,
'shardtype':'',
'code':'deflate',
'use_fastavro1':'False',
'schema_table':{
				"type":"record",
				"name" :"twitter_schema",
				"namespace" : "com.miguno.avro",
				"fields" : [{"name" : "username",
							"type" : "string",
							"default" : "NONE"},
							
							{"name" : "tweet",
							"type" : "string",
							"default" : "NONE"},
							
							{"name" : "timestamp",
							"type" : "long",
							"default": {}}]}
}

read_config_json = {
    'file_pattern':'/content/Input/test1.json',
    'min_bundle_size':0,
    'compression_type':'auto',
    'strip_trailing_newlines':True,
	'validate':True,
    'skip_header_lines':0
}

read_config_txt = {
    'file_pattern':'/content/Input/2017.txt',
    'min_bundle_size':0,
    'compression_type':'auto',
    'strip_trailing_newlines':True,
	'validate':True,
    'skip_header_lines':0
}

read_config_csv = {
    'file_pattern':'/content/Input/students_adaptability_level_online_education.csv',
    'min_bundle_size':0,
    'compression_type':'auto',
    'strip_trailing_newlines':True,
	'validate':True,
    'skip_header_lines':0
}

read_config_parquet = {
    'file_pattern':'/content/Input/example_test.parquet',
    'min_bundle_size':0,
    'validate':True,
    'columns':None
}

read_config_avro = {
    'file_pattern':'/content/Input/twitter.avro',
    'min_bundle_size':0,
    'validate':True
}

bigquery_config_write = {
    'table_name':'', #project_id:dataset_id.table format
    'table_schema':'', #'_c0:integer,_c1:float' format
    'create_disposition':beam.io.BigQueryDisposition.CREATE_IF_NEEDED, #if target table not found
    'write_disposition':beam.io.BigQueryDisposition.WRITE_APPEND #append or replace table
}

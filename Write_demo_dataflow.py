#!pip install apache_beam[gcp]
#
# !git clone https://github.com/Srimdu/beambqtest1
#
#!bq mk dataset1
#
#!gsutil mb gs://qwiklabs-gcp-03-bd1894c6c632
#
# !gsutil cp /beambqtest1/weather.csv gs://qwiklabs-gcp-03-bd1894c6c632/weather.csv
#
#
'''
git clone https://github.com/Srimdu/beambqtest1
bq mk dataset1
gsutil mb gs://qwiklabs-gcp-03-bd1894c6c632
gsutil cp beambqtest1/weather.csv gs://qwiklabs-gcp-03-bd1894c6c632/weather.csv
'''

# importing nessesary modules

import os
import logging
import pytz
from datetime import datetime
import time
import datetime as dt
import json
import csv
import pyarrow
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# logger part

current_dt = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y%m%d_%H%M%S')
directory = os.getcwd()  # or given gcs bucket like 'gs://qwiklabs-gcp-04-ccfab8a3c762/custom_joblogger/logs'
logfilename = "Beam_Transformation_" + current_dt + ".log"
finalpath = directory + '/' + logfilename
print('Log Path: ', finalpath)

if not (os.path.isdir(directory)):
    os.mkdir(directory)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# with open(finalpath,'a') as f1:
#  f1.write('New Log')
FileHandler = logging.FileHandler(finalpath, mode='a')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
FileHandler.setFormatter(formatter)
logger.addHandler(FileHandler)

# Config_file_content


# Config 1 - GCP config 

projectid = 'qwiklabs-gcp-01-d3435b61d35d'
bucket_name = 'gs://'+projectid
bigquery_datasetid = projectid+'.dataset1'
bigquery_datasetid1 = projectid+':dataset1'

# Config 2 - Dataflow runner config - all are required attributes

dataflowrunner_config = {
    'project': '',  # Project_id of the running project
    'runner': 'DataflowRunner',  # Specify the runner name of the enviroinment
    # refer, https://beam.apache.org/documentation/runners/dataflow/ for available runners
    'jobname': '',  # name of the dataflow job only accepts type [a-z0-9]
    'temp_bucket': '',  # Temp bucket/path location for processing 'gs://folder1/folder2/' format
    'staging_bucket': '',  # Staging bucket/path location for processing 'gs://folder1/folder2' format
    'region': '',  # region of dataflow runner
    'streaming_ind': True,  # Boolean - Specifies the job is Streaming or not change to False for Batch workloads
    'save_main_session_ind': True,
    # Boolean - Leave it as True, Specify the function param's outside the __main__ session. False for Interactive runner
}

# Config 3 - Batch io Config (read and write) change the required file config before calling on pcollection

write_config_json = {
    'filepath+filename': '/content/writeoutput/final_output',
    'filesuffix': '.json',
    'shard': 0,
    'shardtype': '',
    'header': None
}
write_config_txt = {
    'filepath+filename': '/content/writeoutput/final_output',
    'filesuffix': '.txt',
    'shard': 0,
    'shardtype': '',
    'header': None
}

write_config_csv = {
    'filepath+filename': '/content/writeoutput/final_output',
    'filesuffix': '.csv',
    'shard': 0,
    'shardtype': '',
    'header': None
}

write_config_parquet = {
    'filepath+filename': '/content/writeoutput/final_output',
    'filesuffix': '.parquet',
    'shard': 1,
    'shardtype': '',
    'buffer': 134217728,
    'batch': 500,
    'code': 'none',
    'deprecated': 'false',
    'schema_table': pyarrow.schema([('registration_dttm', pyarrow.timestamp('ns')),
                                    # schema format for info refer parquet io pakage on beam documentation
                                    ('id', pyarrow.int32()), ('first_name', pyarrow.string()),
                                    ('last_name', pyarrow.string()),
                                    ('email', pyarrow.string()),
                                    ('gender', pyarrow.string()), ('ip_address', pyarrow.string()),
                                    ('cc', pyarrow.string()),
                                    ('country', pyarrow.string()), ('birthdate', pyarrow.string()),
                                    ('salary', pyarrow.int32()),
                                    ('title', pyarrow.string()), ('comments', pyarrow.string())])}

write_config_avro = {
    'filepath+filename': '/content/writeoutput/final_output',
    'filesuffix': '.avro',
    'shard': 0,
    'shardtype': '',
    'code': 'deflate',
    'use_fastavro1': 'False',
    'schema_table': {  # schema format for info refer avro io pakage on beam documentation
        "type": "record",
        "name": "twitter_schema",
        "namespace": "com.miguno.avro",
        "fields": [{"name": "username",
                    "type": "string",
                    "default": "NONE"},

                   {"name": "tweet",
                    "type": "string",
                    "default": "NONE"},

                   {"name": "timestamp",
                    "type": "long",
                    "default": {}}]}
}

read_config_json = {
    'file_pattern': '/content/Input/test1.json',
    'min_bundle_size': 0,
    'compression_type': 'auto',
    'strip_trailing_newlines': True,
    'validate': True,
    'skip_header_lines': 0
}

read_config_txt = {
    'file_pattern': '/content/Input/2017.txt',
    'min_bundle_size': 0,
    'compression_type': 'auto',
    'strip_trailing_newlines': True,
    'validate': True,
    'skip_header_lines': 0
}

read_config_csv = {
    'file_pattern': '/content/Input/students_adaptability_level_online_education.csv',
    'min_bundle_size': 0,
    'compression_type': 'auto',
    'strip_trailing_newlines': True,
    'validate': True,
    'skip_header_lines': 0
}

read_config_parquet = {
    'file_pattern': '/content/Input/example_test.parquet',
    'min_bundle_size': 0,
    'validate': True,
    'columns': None
}

read_config_avro = {
    'file_pattern': '/content/Input/twitter.avro',
    'min_bundle_size': 0,
    'validate': True
}

# Config 4 - Bigquery io Config

write_config_bigquery = {
    'table_name': bigquery_datasetid1 + '.table1',  # project_id:dataset_id.table format
    'table_schema': 'Data_Precipitation:float,Date_Full:date,Date_Month:integer,Date_Week_of:integer,Date_Year:integer,Station_City:string,Station_Code:string,\
    Station_Location:string,Station_State:string,Data_Temperature_Avg_Temp:integer,Data_Temperature_Max_Temp:integer,Data_Temperature_Min_Temp:integer,\
    Data_Wind_Direction:integer,Data_Wind_Speed:float',  # '_c0:integer,_c1:float' format
    'create_disposition': beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  # if target table not found
    'write_disposition': beam.io.BigQueryDisposition.WRITE_APPEND  # append or replace table
}

read_config_bigquery = {
    'Query': 'SELECT * FROM `project_id.dataset_name.table_name`'
}

# Config 5 - Pubsub io Config

pubsub_config = {
    'project': '',  # Project_id of the running project
    'subscription': ''  # id of the pubsub subscription
}


# main_file_content

class Ingestion():

    def readPubSub(p_coll):

        try:
            pubsub_in_pc = (
                    p_coll
                    | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription='projects/{0}/subscriptions/{1}' \
                    .format(pubsub_config['project'], pubsub_config['subscription'])).with_output_types(bytes)
                    | "UTF-8 bytes to string" >> beam.Map(lambda x: x.decode("utf-8"))
                    | "MessageParse" >> beam.Map(lambda x: json.loads(x))
            )

            return pubsub_in_pc

        except Exception as exp:
            logger.error(
                "Failure in reading subscription {0} from Project {1}. Full topic name is 'projects/{0}/subscriptions/{1}' Exception : {2}".format(
                    pubsub_config['project'], pubsub_config['subscription'], exp))

    def read_json(pc):
        try:
            final = (pc
                     | beam.io.ReadFromText(read_config_json['file_pattern'],
                                            min_bundle_size=read_config_json['min_bundle_size'], \
                                            compression_type=read_config_json['compression_type'],
                                            strip_trailing_newlines=read_config_json['strip_trailing_newlines'],
                                            validate=read_config_json['validate'],
                                            skip_header_lines=read_config_json['skip_header_lines'])
                     )

            return final

        except Exception as exp:
            logger.error('Failure in reading json file {} Exception : {}'.format(read_config_json['file_pattern'], exp))

    def read_txt(pc):
        try:
            final = (pc
                     | beam.io.ReadFromText(read_config_txt['file_pattern'],
                                            min_bundle_size=read_config_txt['min_bundle_size'], \
                                            compression_type=read_config_txt['compression_type'],
                                            strip_trailing_newlines=read_config_txt['strip_trailing_newlines'],
                                            validate=read_config_txt['validate'],
                                            skip_header_lines=read_config_txt['skip_header_lines'])
                     )

            return final

        except Exception as exp:
            logger.error('Failure in reading file {} Exception : {}'.format(read_config_txt['file_pattern'], exp))

    def read_csv(pc):
        try:
            final = (pc
                     | beam.io.ReadFromText(read_config_csv['file_pattern'],
                                            min_bundle_size=read_config_csv['min_bundle_size'], \
                                            compression_type=read_config_csv['compression_type'],
                                            strip_trailing_newlines=read_config_csv['strip_trailing_newlines'],
                                            validate=read_config_csv['validate'],
                                            skip_header_lines=read_config_csv['skip_header_lines'])
                     )

            return final

        except Exception as exp:
            logger.error('Failure in reading csv file {} Exception : {}'.format(read_config_csv['file_pattern'], exp))

    def read_parquet(pc):
        try:
            final = (pc
                     | beam.io.ReadFromParquet(read_config_parquet['file_pattern'],
                                               min_bundle_size=read_config_parquet['min_bundle_size'], \
                                               validate=read_config_parquet['validate'],
                                               columns=read_config_parquet['columns'])
                     )

            return final

        except Exception as exp:
            logger.error(
                'Failure in reading parquet file {} Exception : {}'.format(read_config_parquet['file_pattern'], exp))

    def read_avro(pc):

        try:
            final = (pc
                     | beam.io.ReadFromAvro(read_config_avro['file_pattern'],
                                            min_bundle_size=read_config_avro['min_bundle_size'],
                                            validate=read_config_avro['validate'])
                     )

            return final

        except Exception as exp:
            logger.error('Failure in reading avro file {} Exception : {}'.format(read_config_avro['file_pattern'], exp))

    def read_bigquery(pc):

        try:
            output_pc = (
                    pc
                    | 'Query_Table' >> beam.io.ReadFromBigQuery(query=read_config_bigquery['Query'],
                                                                use_standard_sql=True
                                                               )
                # Each row is a dictionary where the keys are the BigQuery columns
            )

            return output_pc  # dict format

        except Exception as exp:
            logger.error('Failure in reading Bigquery SQL {} Exception : {}'.format(read_config_bigquery['Query'], exp))


class Load():

    def write_json(pc):
        try:
            final_write = (pc
                           | beam.io.WriteToText(write_config_json['filepath+filename'],
                                                 file_name_suffix=write_config_json['filesuffix'],
                                                 shard_name_template=write_config_json['shardtype'],
                                                 num_shards=write_config_json['shard'])
                           )

            return final_write

        except Exception as exp:
            logger.error(
                'Failure in writing json file {} Exception : {}'.format(write_config_json['filepath+filename'], exp))

    def write_txt(pc):
        try:
            final_write = (pc
                           | beam.io.WriteToText(write_config_txt['filepath+filename'],
                                                 file_name_suffix=write_config_txt['filesuffix'],
                                                 shard_name_template=write_config_txt['shardtype'],
                                                 num_shards=write_config_txt['shard'])
                           )

            return final_write

        except Exception as exp:
            logger.error(
                'Failure in writing text file {} Exception : {}'.format(write_config_txt['filepath+filename'], exp))

    def write_csv(pc):

        try:
            final_write = (pc
                           | beam.io.WriteToText(write_config_csv['filepath+filename'],
                                                 file_name_suffix=write_config_csv['filesuffix'],
                                                 shard_name_template=write_config_csv['shardtype'],
                                                 num_shards=write_config_csv['shard'])
                           )

            return final_write

        except Exception as exp:
            logger.error(
                'Failure in writing csv file {} Exception : {}'.format(write_config_csv['filepath+filename'], exp))

    def write_parquet(pc):

        try:
            final_write = (pc
                           | beam.io.WriteToParquet(write_config_parquet['filepath+filename'],
                                                    file_name_suffix=write_config_parquet['filesuffix'],
                                                    schema=write_config_parquet['schema_table'],
                                                    row_group_buffer_size=write_config_parquet['buffer'],
                                                    record_batch_size=write_config_parquet['batch'],
                                                    codec=write_config_parquet['code'],
                                                    use_deprecated_int96_timestamps=write_config_parquet['deprecated'],
                                                    shard_name_template=write_config_parquet['shardtype'], 
                                                    num_shards=write_config_parquet['shard'])
                           )

            return final_write

        except Exception as exp:
            logger.error(
                'Failure in writing parquet file {} Exception : {}'.format(write_config_parquet['filepath+filename'],
                                                                           exp))

    def write_avro(pc):

        try:
            final_write = (pc
                           | beam.io.WriteToAvro(write_config_avro['filepath+filename'],
                                                 file_name_suffix=write_config_avro['filesuffix'],
                                                 schema=write_config_avro['schema_table'],
                                                 codec=write_config_avro['code'],
                                                 shard_name_template=write_config_avro['shardtype'],
                                                 num_shards=write_config_avro['shard'],
                                                 use_fastavro=write_config_avro['use_fastavro1'])
                           )

            return final_write

        except Exception as exp:
            logger.error(
                'Failure in writing avro file {} Exception : {}'.format(write_config_avro['filepath+filename'], exp))

    def write_bigquery(pc):

        try:
            final = (pc
                     | beam.io.WriteToBigQuery(
                        write_config_bigquery['table_name'],
                        schema=write_config_bigquery['table_schema'],
                        create_disposition=write_config_bigquery['create_disposition'],
                        write_disposition=write_config_bigquery['write_disposition']))

            return final

        except Exception as exp:
            logger.error(
                'Failure in writing Bigquery table {} Exception : {}'.format(bigquery_config_write['table_name'], exp))


# choosing runner


runner_type, streaming_ind = ['DataflowRunner',
                              False]  # issue a list of job runner type and Boolean of Is this Stream job? (False for Batch)
# Available options for runnertype: ('DataflowRunner','DirectRunner','SparkRunner') Depends upon runner type give the option
# Available options for streaming_ind: (True, False) True if job is streaming job

dataflowrunner_config = {'project': projectid,
                         'runner': 'DataflowRunner',
                         'jobname': 'test_'+datetime.now(pytz.timezone('US/Eastern')).strftime('%Y%m%d_%H%M%S'),
                         'temp_bucket': bucket_name+'/temp_folder',
                         'staging_bucket': bucket_name+'/staging_folder',
                         'region': 'us-central1-a',
                         'streaming_ind': False,
                         'save_main_session_ind': True}

directrunner_config = {'runner': 'DirectRunner',
                       'streaming_ind': False,
                       'temp_bucket': bucket_name + '/temp_folder',
                       'staging_bucket': bucket_name + '/staging_folder'}

sparkrunner_config = {'runner': 'SparkRunner',
                      'streaming_ind': False,
                      'temp_bucket': bucket_name + '/temp_folder',
                      'staging_bucket': bucket_name + '/staging_folder'}

# building nessesary client information for Bigquery io in case of local and spark runner
if runner_type in ['DirectRunner', 'SparkRunner']:
    from google.cloud import bigquery

    # from config 1
    client = bigquery.Client()
    dataset = bigquery.Dataset(bigquery_datasetid)

def parse_doublequotes(element):
    for line in csv.reader([element], quotechar='"', delimiter=',', skipinitialspace=True):
        return line


def convert_types(data):
    # """Converts string values to their appropriate type."""
    data['Data_Precipitation'] = float(data['Data_Precipitation']) if 'Data_Precipitation' in data else None
    data['Date_Full'] = str(data['Date_Full']) if 'Date_Full' in data else None
    data['Date_Month'] = int(data['Date_Month']) if 'Date_Month' in data else None
    data['Date_Week_of'] = int(data['Date_Week_of']) if 'Date_Week_of' in data else None
    data['Date_Year'] = int(data['Date_Year']) if 'Date_Year' in data else None
    data['Station_City'] = str(data['Station_City']) if 'Station_City' in data else None
    data['Station_Code'] = str(data['Station_Code']) if 'Station_Code' in data else None
    data['Station_Location'] = str(data['Station_Location']) if 'Station_Location' in data else None
    data['Station_State'] = str(data['Station_State']) if 'Station_State' in data else None
    data['Data_Temperature_Avg_Temp'] = int(
    data['Data_Temperature_Avg_Temp']) if 'Data_Temperature_Avg_Temp' in data else None
    data['Data_Temperature_Max_Temp'] = int(
    data['Data_Temperature_Max_Temp']) if 'Data_Temperature_Max_Temp' in data else None
    data['Data_Temperature_Min_Temp'] = int(
    data['Data_Temperature_Min_Temp']) if 'Data_Temperature_Min_Temp' in data else None
    data['Data_Wind_Direction'] = int(data['Data_Wind_Direction']) if 'Data_Wind_Direction' in data else None
    data['Data_Wind_Speed'] = float(data['Data_Wind_Speed']) if 'Data_Wind_Speed' in data else None
    return data



if runner_type == 'DataflowRunner':
    pipeline_options = PipelineOptions(
        project='{0}'.format(dataflowrunner_config['project']),
        job_name=dataflowrunner_config['jobname'],
        save_main_session=dataflowrunner_config['save_main_session_ind'],
        staging_location=dataflowrunner_config['staging_bucket'],
        temp_location=dataflowrunner_config['temp_bucket'],
        region=dataflowrunner_config['region'],
        runner=dataflowrunner_config['runner'],
        streaming=dataflowrunner_config['streaming_ind'])

elif runner_type == 'SparkRunner':
    pipeline_options = PipelineOptions(
        project=projectid,
        runner=sparkrunner_config['runner'],
        streaming=sparkrunner_config['streaming_ind'],
        temp_location=sparkrunner_config['temp_bucket'],
        staging_location=sparkrunner_config['staging_bucket'])

elif runner_type == 'DirectRunner':
    pipeline_options = PipelineOptions(
        runner=directrunner_config['runner'],
        streaming=directrunner_config['streaming_ind'])

# declaring pipeline

p = beam.Pipeline(options=pipeline_options)

# setting up config

read_config_csv = {
    'file_pattern': bucket_name+'/weather.csv',
    'min_bundle_size': 0,
    'compression_type': 'auto',
    'strip_trailing_newlines': True,
    'validate': True,
    'skip_header_lines': 1
}


write_config_json = {
    'filepath+filename': bucket_name+'/weather',
    'filesuffix': '.json',
    'shard': 0,
    'shardtype': '',
    'header': None
}

write_config_txt = {
    'filepath+filename': bucket_name+'/weather',
    'filesuffix': '.txt',
    'shard': 0,
    'shardtype': '',
    'header': None
}

write_config_csv = {
    'filepath+filename': bucket_name+'/weather_out',
    'filesuffix': '.csv',
    'shard': 0,
    'shardtype': '',
    'header': None
}

write_config_parquet = {
    'filepath+filename': bucket_name+'/weather', 'filesuffix': '.parquet',
    'shard': 1,
    'shardtype': '',
    'buffer': 134217728,
    'batch': 1000,
    'code': 'none',
    'deprecated': 'false',
    'schema_table': pyarrow.schema([('Data_Precipitation', pyarrow.float32()),
      # schema format for info refer parquet io pakage on beam documentation
      ('Date_Full', pyarrow.string()),
      ('Date_Month', pyarrow.int32()),
      ('Date_Week_of', pyarrow.int32()),
      ('Date_Year', pyarrow.int32()),
      ('Station_City', pyarrow.string()),
      ('Station_Code', pyarrow.string()),
      ('Station_Location', pyarrow.string()),
      ('Station_State', pyarrow.string()),
	  ('Data_Temperature_Avg_Temp', pyarrow.int32()),
      ('Data_Temperature_Max_Temp', pyarrow.int32()),
      ('Data_Temperature_Min_Temp', pyarrow.int32()),
      ('Data_Wind_Direction', pyarrow.int32()),
      ('Data_Wind_Speed', pyarrow.float32())])}

write_config_avro = {
    'filepath+filename': bucket_name+'/weather',
    'filesuffix': '.avro',
    'shard': 1,
    'shardtype': '',
    'code': 'deflate',
    'use_fastavro1': 'False',
    'schema_table': {  # schema format for info refer avro io pakage on beam documentation
        "type": "record",
        "name": "weather",
        "namespace": "com.miguno.avro",
	          "fields": [{"name": "Data_Precipitation", "type": "float", "default": "NONE"},
                       {"name": "Date_Full", "type": "string", "default": "NONE"},
                       {"name": "Date_Month", "type": "int", "default": "NONE"},
				       {"name": "Date_Week_of", "type": "int", "default": "NONE"},
                       {"name": "Date_Year", "type": "int", "default": "NONE"},
                       {"name": "Station_City", "type": "string", "default": "NONE"},
				       {"name": "Station_Code", "type": "string", "default": "NONE"},
                       {"name": "Station_Location", "type": "string", "default": "NONE"},
                       {"name": "Station_State", "type": "string", "default": "NONE"},
				       {"name": "Data_Temperature_Avg_Temp", "type": "int", "default": "NONE"},
                       {"name": "Data_Temperature_Max_Temp", "type": "int", "default": "NONE"},
                       {"name": "Data_Temperature_Min_Temp", "type": "int", "default": "NONE"},
				       {"name": "Data_Wind_Direction", "type": "int", "default": "NONE"},
                       {"name": "Data_Wind_Speed", "type": "float", "default": "NONE"}]}}


read_config_bigquery = {
    'Query': 'SELECT * FROM `qwiklabs-gcp-03-bd1894c6c632.dataset1.table1` LIMIT 1000'
}

# calling first pc to read

#pc1 = Ingestion.read_bigquery(p)

def parse_doublequotes(element):
    for line in csv.reader([element], quotechar='"', delimiter=',', skipinitialspace=True):
        return line


def convert_types(data):
    # """Converts string values to their appropriate type."""
    data['Data_Precipitation'] = float(data['Data_Precipitation']) if 'Data_Precipitation' in data else None
    data['Date_Full'] = str(data['Date_Full']) if 'Date_Full' in data else None
    data['Date_Month'] = int(data['Date_Month']) if 'Date_Month' in data else None
    data['Date_Week_of'] = int(data['Date_Week_of']) if 'Date_Week_of' in data else None
    data['Date_Year'] = int(data['Date_Year']) if 'Date_Year' in data else None
    data['Station_City'] = str(data['Station_City']) if 'Station_City' in data else None
    data['Station_Code'] = str(data['Station_Code']) if 'Station_Code' in data else None
    data['Station_Location'] = str(data['Station_Location']) if 'Station_Location' in data else None
    data['Station_State'] = str(data['Station_State']) if 'Station_State' in data else None
    data['Data_Temperature_Avg_Temp'] = int(data['Data_Temperature_Avg_Temp']) if 'Data_Temperature_Avg_Temp' in data else None
    data['Data_Temperature_Max_Temp'] = int(data['Data_Temperature_Max_Temp']) if 'Data_Temperature_Max_Temp' in data else None
    data['Data_Temperature_Min_Temp'] = int(data['Data_Temperature_Min_Temp']) if 'Data_Temperature_Min_Temp' in data else None
    data['Data_Wind_Direction'] = int(data['Data_Wind_Direction']) if 'Data_Wind_Direction' in data else None
    data['Data_Wind_Speed'] = float(data['Data_Wind_Speed']) if 'Data_Wind_Speed' in data else None
    return data

pc1 = Ingestion.read_csv(p)

pc2 = (pc1 | 'split_obj1' >> beam.Map(parse_doublequotes))

pc3 = (pc2 | 'conv_text' >> beam.Map(lambda x: ','.join(x)))

pc4 = (pc2 
      | 'conv_dict' >> beam.Map(lambda x: {"Data_Precipitation": x[0], "Date_Full": x[1], "Date_Month": x[2], "Date_Week_of": x[3],
                       "Date_Year": x[4], "Station_City": x[5], "Station_Code": x[6], "Station_Location": x[7],
                       "Station_State": x[8], "Data_Temperature_Avg_Temp": x[9], "Data_Temperature_Max_Temp": x[10],
                       "Data_Temperature_Min_Temp": x[11], "Data_Wind_Direction": x[12], "Data_Wind_Speed": x[13]})
      | 'conv_type' >> beam.Map(convert_types))


Load.write_csv(pc3)

Load.write_json(pc4)

Load.write_txt(pc3)

Load.write_parquet(pc4)

Load.write_avro(pc4)





# creating second pcollection to apply custom transformations
'''
pc2 = (pc1
       | 'split obj' >> beam.Map(parse_doublequotes)
       #| 'print' >> beam.Map(print)
       | 'FormatToDict' >> beam.Map(
            lambda x: {"Data_Precipitation": x[0], "Date_Full": x[1], "Date_Month": x[2], "Date_Week_of": x[3],
                       "Date_Year": x[4], "Station_City": x[5], "Station_Code": x[6], "Station_Location": x[7],
                       "Station_State": x[8], "Data_Temperature_Avg_Temp": x[9], "Data_Temperature_Max_Temp": x[10],
                       "Data_Temperature_Min_Temp": x[11], "Data_Wind_Direction": x[12], "Data_Wind_Speed": x[13]})
       | 'convertdatatypes' >> beam.Map(convert_types))
# calling final write
pc3 = Load.write_bigquery(pc2)'''

#pc2 = (pc1 | 'print' >> beam.Map(print))

# running pipeline

pipeline_run = p.run()

pipeline_run.wait_until_finish()

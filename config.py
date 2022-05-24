#main_file_content

import apache_beam as beam
import json

def readPubSub(p_coll):

    pubsub_in_pc = (
            p_coll
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(topic='projects/{0}/subscriptions/{1}'.format(pubsub_config['project'],pubsub_config['subscription'])).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda x: x.decode("utf-8"))
            | "MessageParse" >> beam.Map(lambda x: json.loads(x))
        )
        
    return pubsub_in_pc

def write_json(pc):
        final_write = ( pc
                        |beam.io.WriteToText(write_config_json['filepath+filename'],file_name_suffix=write_config_json['filesuffix'],
                                             shard_name_template=write_config_json['shardtype'],num_shards=write_config_json['shard'])
                      ) 
        return final_write
        
def write_txt(pc):
        final_write = ( pc
                        |beam.io.WriteToText(write_config_txt['filepath+filename'],file_name_suffix=write_config_txt['filesuffix'],
                                             shard_name_template=write_config_txt['shardtype'],num_shards=write_config_txt['shard'])
                      ) 
        return final_write

def write_csv(pc):
        final_write = ( pc
                        |beam.io.WriteToText(write_config_csv['filepath+filename'],file_name_suffix=write_config_csv['filesuffix'],
                                             shard_name_template=write_config_csv['shardtype'],num_shards=write_config_csv['shard'])
                      ) 
        return final_write     

def write_parquet(pc):
        final_write = ( pc
                       |beam.io.WriteToParquet(write_config_parquet['filepath+filename'],file_name_suffix=write_config_parquet['filesuffix'],
                                               schema=write_config_parquet['schema_table'],row_group_buffer_size=write_config_parquet['buffer'], 
                                               record_batch_size=write_config_parquet['batch'],codec=write_config_parquet['code'], 
                                               use_deprecated_int96_timestamps=write_config_parquet['deprecated'],
                                               shard_name_template=write_config_parquet['shardtype'],num_shards=1)
)
        
        return final_write

def write_avro(pc):
        final_write = ( pc
                      |beam.io.WriteToAvro(write_config_avro['filepath+filename'],file_name_suffix=write_config_avro['filesuffix'],schema=write_config_avro['schema_table'],
                     codec=write_config_avro['code'],shard_name_template=write_config_avro['shardtype'],num_shards=write_config_avro['shard'],
                     use_fastavro=write_config_avro['use_fastavro1'])
)
        return final_write


def read_json(pc):
        final = ( pc
                        |beam.io.ReadFromText(read_config_json['file_pattern'],min_bundle_size=read_config_json['min_bundle_size'],compression_type=read_config_json['compression_type'],
                                              strip_trailing_newlines=read_config_json['strip_trailing_newlines'],validate=read_config_json['validate'],
                                              skip_header_lines=read_config_json['skip_header_lines'])
                      ) 
        return final

def read_txt(pc):
        final = ( pc
                        |beam.io.ReadFromText(read_config_txt['file_pattern'],min_bundle_size=read_config_txt['min_bundle_size'],compression_type=read_config_txt['compression_type'],
                                              strip_trailing_newlines=read_config_txt['strip_trailing_newlines'],validate=read_config_txt['validate'],
                                              skip_header_lines=read_config_txt['skip_header_lines'])
                      ) 
        return final  

def read_csv(pc):
        final = ( pc
                        |beam.io.ReadFromText(read_config_csv['file_pattern'],min_bundle_size=read_config_csv['min_bundle_size'],compression_type=read_config_csv['compression_type'],
                                              strip_trailing_newlines=read_config_csv['strip_trailing_newlines'],validate=read_config_csv['validate'],
                                              skip_header_lines=read_config_csv['skip_header_lines'])
                      ) 
        return final   

def read_parquet(pc):
        final = ( pc
                        |beam.io.ReadFromParquet(read_config_parquet['file_pattern'],min_bundle_size=read_config_parquet['min_bundle_size'],validate=read_config_parquet['validate'],
                                                 columns=read_config_parquet['columns'])
                      ) 
        return final   

def read_avro(pc):
        final = ( pc
                        |beam.io.ReadFromAvro(read_config_avro['file_pattern'],min_bundle_size=read_config_avro['min_bundle_size'],validate=read_config_avro['validate'])
                      ) 
        return final

def write_bigquery(pc):
        final = beam.io.WriteToBigQuery(
            bigquery_config_write['table_name'],
            schema=bigquery_config_write['table_schema'],
            create_disposition=bigquery_config_write['create_disposition'],
            write_disposition=bigquery_config_write['write_disposition'])

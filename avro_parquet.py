"""
    AVRO/PARQUET/JSON converter
"""
import copy
import json
import avro.schema
# from avro.datafile import DataFileWriter, DataFileReader
# from avro.io import DatumWriter, DatumReader
import pandas as pd
import datetime
import json, time, sys
import pandas as pd
import apache_beam as beam
import pyarrow
import os
import glob
import shutil, avro
import argparse
import fastavro
from fastavro import parse_schema
from fastavro import writer
from fastavro.schema import load_schema

# from file "schema.avsc"
default_schema = '''
{
 "namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "objectUai", "type": "string"},
     {"name": "sourceTimestamp",    "type": {   "type": "long",   "logicalType": "timestamp-micros" }},
     {"name": "variableName", "type": "string"},
     {"name": "dataValue", "type": "string"},
     {"name": "siteCode", "type": "string"},
     {"name": "objectType", "type": "string"}
 ]
}
'''
fa_default_schema = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "objectUai", "type": "string"},
        {"name": "sourceTimestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "variableName", "type": "string"},
        {"name": "dataValue", "type": "string"},
        {"name": "siteCode", "type": "string"},
        {"name": "objectType", "type": "string"}
    ]
}


# parquet_schema = pyarrow.schema(
#     [('objectUai', pyarrow.string()),
#      ('objectType', pyarrow.string()),
#      ('variableName', pyarrow.string()),
#      ('sourceTimestamp', pyarrow.timestamp('s', tz='+00:00')),
#      ('dataValue', pyarrow.string()),
#      ('siteCode', pyarrow.string())
#      ]
# )

def timestamp_as_string(d):
    """ replace timestamp as string"""
    r = {}
    for k, v in d.items():
        r[k] = str(v) if isinstance(v, datetime.datetime) else v
    return r


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AVRO/PARQUET/JSON converter ')
    parser.add_argument('-s', '--src', default="gs://vkunitki-dev/test_1.parquet")
    # parser.add_argument('--src',
    #                     default="/temp/udc/initial/METADATA_INPUT_BATTERYCHARGER_part-00000-5c8d9274-5572-46a1-8e29-95fd4ed9b4b5-c000.snappy.parquet")
    # parser.add_argument('--dest', default="out_1/test_2.avro")
    parser.add_argument('-d', '--dest', default="gs://vkunitki-dev/test_1.avro")
    parser.add_argument('-sc', '--schema', default="dafault")
    # parser.add_argument('-sc', '--schema', default="schema.avsc")
    parser.add_argument('-i', '--div', default="")
    parser.add_argument('-t', '--time', default=[])
    args = parser.parse_args()

    print(args.time)
    if args.schema == "dafault":
        avro_schema = fastavro.parse_schema(fa_default_schema)
        # avro_schema = load_schema("schema.avsc")
        parquet_schema = avro.schema.parse(default_schema)
    else:
        avro_schema = load_schema(args.schema)
        parquet_schema = avro.schema.parse(open(args.schema, "rb").read())
    # print(schema)
    with beam.Pipeline() as p:
        if args.src.find(".json") > 0:
            records = p | 'Read parquet' >> beam.io.ReadFromText(args.src)
            records | "OUTss" >> beam.Map(lambda line: print("****> ", args.div, line))
            # records | 'Count all elements' >> beam.combiners.Count.Globally() | beam.Map(print)

        if args.src.find(".parquet") > 0:
            records = p | 'Read parquet' >> beam.io.ReadFromParquet(args.src)
            # records | 'Count all elements' >> beam.combiners.Count.Globally() | beam.Map(print)

        if args.src.find(".avro") > 0:
            records = p | 'Read avro' >> beam.io.ReadFromAvro(args.src)
            # records | 'Count all elements' >> beam.combiners.Count.Globally() | beam.Map(print)

        if args.dest == "json":
            records | "OUT" >> beam.Map(lambda line: print(args.div, timestamp_as_string(line)))

        if args.dest in ["count", "null"]:
            records | 'Count ' >> beam.combiners.Count.Globally() | beam.Map(print)

        if args.dest.find(".avro") > 0:
            output = records | beam.Map(lambda line: line)
            # output | "OUT" >> beam.Map(lambda line: print("******>", line))
            _ = output | 'Write avro' >> beam.io.WriteToAvro(args.dest,
                                                             avro_schema,
                                                             num_shards=1,
                                                             shard_name_template='')

        if args.dest.find(".parquet") > 0:
            parquet_schema = avro.schema.parse(open(args.schema, "rb").read())
            output = records | beam.Map(lambda line: line)
            output | "OUT" >> beam.Map(lambda line: print("parquet>", line))
            _ = output | 'Write' >> beam.io.WriteToParquet(args.dest, avro_schema,  # parquet_schema,
                                                           shard_name_template='', num_shards=1)
            # output | "OUT" >> beam.Map(lambda line: print(args.div, line))

        # records | 'Count ' >> beam.combiners.Count.Globally() | beam.Map(print)
    result = p.run()
    result.wait_until_finish()

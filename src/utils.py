import json
import logging
import logging.config
import get_variables as gav
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
logging.config.fileConfig(gav.configs['logging_paths']['running_logconf_path'])
logger = logging.getLogger('Utils')


def get_schema_from_json_file(spark,schema_file_path):
    json_schema = None
    try:
        json_schema = spark.read.text(schema_file_path).collect()
    except Exception as e:
        logger.info('can not read the schema file because of error {}'.format(e))
    else:
        json_schema_string = '\n'.join([i.value for i in json_schema])
        json_schema = StructType.fromJson(json.loads(json_schema_string))
    return json_schema


def get_source_dataframe(spark, schema, source_file_path):
    try:
        source_df = (spark.read
                     .schema(schema)
                     .json(source_file_path))
        source_df = source_df.na.drop()

    except Exception as msg:
        logger.info('Can not source the dataframe because of this error == {}'.format(msg))
        source_df = None
    return source_df

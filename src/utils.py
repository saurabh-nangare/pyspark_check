import json
import logging
import logging.config
import get_variables as gav
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import explode, year, col
logging.config.fileConfig(gav.configs['logging_paths']['running_logconf_path'])
logger = logging.getLogger('Utils')


def get_schema_from_json_file(spark,schema_file_path):
    json_schema = None
    try:
        logger.info("trying to get schema for {}".format(schema_file_path))
        json_schema = spark.read.text(schema_file_path).collect()
    except Exception as e:
        logger.info('can not read the schema file because of error {}'.format(e))
    else:
        json_schema_string = '\n'.join([i.value for i in json_schema])
        json_schema = StructType.fromJson(json.loads(json_schema_string))
        logger.info('schema has been created for{}'.format(schema_file_path))
    return json_schema


def get_source_dataframe(spark, schema, source_file_path):
    try:
        logger.info("trying to create dataframe for{}".format(source_file_path))
        source_df = (spark.read
                     .schema(schema)
                     .json(source_file_path))
    except Exception as msg:
        logger.info('Can not source the dataframe because of this error == {}'.format(msg))
        source_df = None

    else:
        logger.info('dataframe has been created for {}'.format(source_file_path))
        logger.info('dropping null records if any for {}'.format(source_file_path))
        source_df = source_df.na.drop()
        logger.info('dropping null records if any has been completed for {}'.format(source_file_path))
    return source_df


def dataframe_writer(df,output_path,**kwargs):
    try:
        logger.info("trying to get the path for dataframe")
        df_name = None
        for name, value in kwargs['locals'].items():
            if value is df:
                df_name = name
                break

        output_path = output_path+df_name
        logger.info('path has been defined to write the dataframe {}'.format(output_path))

        logger.info("started writing the dataframe")
        df.coalesce(1).write.mode('overwrite').json(output_path)
        logger.info("dataframe writer has wrote the files at the destination")

    except Exception as msg:
        logger.error("dataframe_writer has been failed for writing {}, Error: {}".format(str(name),msg))




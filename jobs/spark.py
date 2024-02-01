from os import listdir, path
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logger
import json

#actual code
def start_spark(app_name='price_transparency_app', master='local[4]', jar_packages=[], py_files=[], configs={}):

    # add other config params
    conf = SparkConf()
    for key, val in configs.items():
        conf.set(key, val)
        conf.setExecutorEnv(key=key, value=val)

    spark_builder = SparkSession.builder.master(master).appName(app_name).config(conf=conf)
    # create session and retrieve Spark logger object
    spark = spark_builder.getOrCreate()
    spark_logger = logger.Log4j(spark)

    #add python dependencies
    sc = spark.sparkContext

    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    if not configs:
        spark_logger.info('Used default configs for Spark application')

    return spark, spark_logger, config_dict, sc



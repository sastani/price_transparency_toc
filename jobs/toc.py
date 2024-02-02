from os import makedirs, path
from pyspark.sql.functions import *
from pyspark.sql.types import *
from file_utils import *
from spark import start_spark
from parser import parse_response
import json

class _dataSchema:

    def __init__(self):
        # define the schema for all our processing
        self.schema = StructType([
            StructField('allowed_amount_file',
                        StructType([StructField('description', StringType(), True),
                                    StructField('location', StringType(), True)]), True),
            StructField('in_network_files', ArrayType(
                StructType([StructField('description', StringType(), True),
                            StructField('location', StringType(), True)]), True), True),
            StructField('reporting_plans', ArrayType(
                StructType([StructField('plan_id', StringType(), True),
                            StructField('plan_id_type', StringType(), True),
                            StructField('plan_market_type', StringType(), True),
                            StructField('plan_name', StringType(), True)]), True),
                        True)
        ])

schema = _dataSchema().schema

def main(mrf_url, insurer):
    # start Spark application and get Spark session, logger and config
    spark, spark_log, configs, sc = start_spark(
        app_name='toc_etl_job',
        py_files =['jobs/file_utils.py', 'jobs/logger.py', 'jobs/spark.py',
                   'jobs/parser.py'],
        configs={"spark.executor.memory": "12g",
                  "spark.driver.memory": "10g",
                  "spark.driver.host": "10.0.0.3",
                    "spark.driver.maxResultSize": "0",
                    "spark.executor.instances": "1",
                    "spark.sql.shuffle.partitions": "10",
                    "spark.default.parallelism": "2",
                    "spark.sql.parquet.enableVectorizedReader": "false",
                    "spark.sql.parquet.columnarReaderBatchSize": "1000"
                 }
        )

    #preprocess files and create necessary directories for files
    subdir = "report_objs"
    mrf_file_name = get_file_from_url(mrf_url)
    file_path = create_dir_path(insurer, subdir, mrf_file_name)
    file_path = file_path + "/"

    pre_process_data(spark, mrf_url, mrf_file_name, file_path, 10000)
    data = extract_data(spark, file_path)
    spark_log.info('Preprocessed file has been created for objs from ' + insurer + "'s TOC " + "at " + file_path)
    #read parquet file as stream and extract it into data frame

    spark_log.info('Data has been read into data frame for: ' + file_path)
    #actually process the data in the file
    #insurer name hardcoded but could be read from list/dictionary (of insurers), etc
    #use name of insurer to make repo which will hold output from two seperate dfs
    reporting_month, unique_files_df, plan_df  = process_file(mrf_file_name, data)
    # load data to monthly index file for insurer (represents all unique files for that month)
    load_data(unique_files_df, reporting_month, insurer, "index")
    #spark_log.info('Dataframe for index file has been persisted to disk')

    # load data to monthly plan file (represents mapping of plans to (unique) index files)
    load_data(plan_df, reporting_month, insurer, "plan")
    #spark_log.info('Dataframe for plan file has been persisted to disk')
    spark_log.info('Dataframe for index file and plan file has been persisted to disk')
    #finally, make result query
    #files_and_plans_df = result_query(unique_files_df, plan_df, "NY", "PPO", spark_log, reporting_month, insurer)
    #load data to result file for only NY PPO plans
    #load_data(files_and_plans_df, reporting_month, insurer, "result")
    spark_log.info('Dataframe for result file has been persisted to disk')


    #log the success of the job
    spark_log.info('Job is finished')

    spark.stop()
    return None


#pre process JSON data coming in as stream from web request
def pre_process_data(spark, url, file_name, file_path, num_objs_in_file):

    json_payload = []
    obj_count_str = str()

    for obj, obj_count in parse_response(url):
        json_payload.append(obj)
        if obj_count % num_objs_in_file == 0:
            obj_count_str = str(obj_count)
            obj_file_name = file_name + "-" + obj_count_str
            obj_file_path = file_path + obj_file_name + '.json'
            f = open(obj_file_path, 'w')
            print(json_payload)
            print(json.dump(json_payload, fp=f, separators=(',', ':')))
            json_payload = []

    # create file for any leftover objects
    if obj_count < num_objs_in_file:
        obj_file_name = file_name + '-0'
    else:
        obj_file_name = file_name + "-leftover"
    obj_file_path = file_path + obj_file_name
    f = open(obj_file_path + '.json', 'w')
    json.dump(json_payload, fp=f, separators=(',', ':'))
    #yield obj_file_path


def extract_data(spark, file_path):
    #rdd = pureReadText(file_path, spark)
    stream_df = spark.readStream.format("json") \
        .schema(schema) \
        .load(file_path + "/*")
    return stream_df

def process_file(file_name, data):
    reporting_month = get_date_from_file_name(file_name)
    plan_df, unique_files_df = transform_plan_to_file(file_name, data, reporting_month)
    #get only the columns from the dataframes we are interested in
    #unique_files_df = unique_files_df.select("url" "network_file_name")
    plan_df = plan_df.select("plan_id", "plan_id_type", "plan_name", "network_file_name")
    #some preprocessesing so we can get the state from the network file names
    unique_files_df = unique_files_df.withColumn("file_array", split(col("network_file_name"), "_"))
    #get first possible set of chars that could be the state
    unique_files_df = unique_files_df.withColumn("state1", unique_files_df["file_array"].getItem(0))
    #get second possible set of chars that could be the state
    unique_files_df = unique_files_df.withColumn("state2", unique_files_df["file_array"].getItem(2))
    unique_files_df = unique_files_df.drop("file_array")
    return reporting_month, unique_files_df, plan_df

def result_query(unique_files_df, plan_df, state, plan, spark_log, reporting_month, repo):

    #filter out files so we get only the ones we are interested in, which are in NY network
    unique_files_df_state = unique_files_df.filter((unique_files_df.state1 == state) | (unique_files_df.state2 == state))

    #load data to monthly index file for NY state (represents all unique files for that month)
    #load_data(unique_files_df_state, reporting_month, repo, "index")
    #spark_log.info('Dataframe for " + state + " state index file has been persisted to disk')

    #filter out plans so we get only the ones associated with PPO plan
    plan_df = plan_df.filter(col("plan_name").contains(plan))

    #load data to (PPO) plan file (represents mapping of plans to (unique) index files)
    #load_data(plan_df, reporting_month, repo, "plan")
    #spark_log.info("Dataframe for " + plan + " plan file has been persisted to disk")
    unique_files_df_state = unique_files_df_state.select(col("network_file_name").alias("file_name"), col("url").alias("file_url"))
    files_and_plans_df = unique_files_df_state.join(plan_df, unique_files_df_state.file_name == plan_df.network_file_name, "inner")
    files_and_plans_df = files_and_plans_df.select(col("file_name"), col("file_url"), col("plan_name"), col("plan_id"))
    return files_and_plans_df

def transform_plan_to_file(file_name, data, reporting_month):
    #only select columns we are interested in
    plan_df = data.select("reporting_plans", "in_network_files")
    # filter out any data without any in network files before we do any more transformations
    plan_df = plan_df.filter(size("in_network_files") != 0)
    # get row for each "reporting_plan" element and associated in network file array
    plan_df = plan_df.select(explode("reporting_plans").alias("reporting_plan"), "in_network_files")

    # get row for each "in_network_file" element and associated "reporting plan"
    plan_df = plan_df.select("reporting_plan", explode("in_network_files").alias("in_network_file"))
    # now we have a df with struct to struct relationships

    #convert reporting plan, getting column for every element in "reporting plan"
    plan_df = plan_df.select("reporting_plan.*", "in_network_file")

    # get cols for subfields of the in network files (description, location)
    plan_df = plan_df.select("plan_id", "plan_id_type", "plan_market_type", "plan_name", "in_network_file.*")
    plan_df = plan_df.select("plan_id", "plan_id_type", "plan_market_type", "plan_name",
                             col("location").alias("url"),
                             element_at(split("location", "/"), -1).alias("network_file_name"))

    unique_files = plan_df.select("url", "network_file_name").distinct()

    #unique_files = gen_file_ids(spark, unique_files, reporting_month)
    # add file ids to the plan dataframe for each in network file
    #plan_df = plan_df.join(unique_files, plan_df.network_file_name == unique_files.network_file_name, "inner").select(plan_df['*'], unique_files['file_id'])
    #TO DO find a good way of creating file ids with streaming
    unique_files = unique_files.select(lit(file_name).alias("index_file_name"),
                                       lit(get_date()).alias("date_index_processed"), "url",
                                       "network_file_name")

    return plan_df, unique_files

if __name__ == '__main__':
    mrf_url = "https://uhc-tic-mrf.azureedge.net/public-mrf/2024-01-01/2024-01-01_-A-1-PUMP-INC_index.json"
    main(mrf_url, "anthem")

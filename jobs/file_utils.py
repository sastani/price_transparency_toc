import os
import re
from datetime import datetime
from pathlib import Path

#utility methods
def get_root_dir():
    user_dir = str(Path(os.getcwd()).parents[1])
    return user_dir

def get_file_name(file_path):
    file_name = os.path.basename(file_path)
    return file_name

def create_dir_path(insurer, subdir, file_name):
    reporting_month = get_date_from_file_name(file_name)
    root_dir = get_root_dir()
    dir_path = root_dir + "/" + insurer + "/" + reporting_month + "/" + subdir
    return dir_path

def get_file_from_url(url):
    file_name_ext = url.split('/')[-1]
    file_name_comp = file_name_ext.split('.')
    file_name = file_name_comp[0]
    # get only the extension from file name
    # ext = file_name_comp[1] + '.' + file_name_comp[2]
    return file_name

def get_date():
    curr_date_time = datetime.now()
    return curr_date_time
def get_date_from_file_name(file_name):
    match = re.search('\d{4}-\d{2}-\d{2}', file_name)
    date = match.group()
    date_as_list = date.split("-")
    date = date_as_list[1] + "-" + date_as_list[0]
    return date

def get_index(spark, reporting_month):
    index_file_path = get_root_dir() + "/out/" + reporting_month + "/index.parquet"
    if os.path.exists(index_file_path):
        file_df = spark.read.parquet(index_file_path)
        return file_df
    else:
        return None

# load data to file, depending on its "type"
def load_data(df, month, insurer, type):
    file_dir = get_root_dir() + "/" + insurer + "/" + month + "/"
    file_path = file_dir + type
    if type == "index":
        pass
        #df.write.mode("append").parquet(file_path + ".parquet")
        #df.repartition("network_file_name").write.partitionBy("network_file_name").mode("append").parquet(file_path + ".parquet")
    elif type == "plan":
        pass
        #df.write.mode("append").parquet(file_path + ".parquet")
        #df.repartition("plan_name").write.partitionBy("plan_name").mode("append").parquet(file_path + ".parquet")
    else:
        df = df.repartition("network_file_name")
        query = df.writeStream.outputMode("append").format("parquet")\
            .option("checkpointLocation", (file_path + "/checkpoint"))\
            .option("path", (file_path + ".parquet")).trigger(processingTime='5 seconds')\
            .start().awaitTermination()
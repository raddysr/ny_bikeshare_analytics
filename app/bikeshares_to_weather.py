import os,sys, logging, json, re
from typing import Callable, Optional
from pyspark.sql.functions import col, split, lit, concat


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOG_FILE = f"{PROJECT_DIR}/logs/app-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s -- LINE:%(lineno)d - %(name)s -- %(levelname)s -- %(funcName)s -- %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger("py4j")


sys.path.insert(1,PROJECT_DIR)
from class_sparksession import pyspark_init


def open_config(filepath) -> dict:
    if isinstance(filepath, str) and os.path.exists(filepath):
        with open(filepath, 'r') as f:
            data = json.load(f)
        return data

config = open_config(f'{PROJECT_DIR}/configs/config.json') 

def start_spark_session(config):
    spark = pyspark_init.PySparkInstance().spark_starter(config)
    return spark

spark = start_spark_session(config)


bike_sharesDf = spark.read.csv(f'{PROJECT_DIR}/ny_bikeshare_full_2016/201605-citibike-tripdata.csv', header=True)
#bike_sharesDf = spark.read.csv(f'{PROJECT_DIR}/data/sample_bikeshares.csv', header=True)

bike_sharesDf_transform = bike_sharesDf.withColumn('date', split(bike_sharesDf['starttime'], ' ').getItem(0))

bike_sharesDf_final = bike_sharesDf_transform.withColumn('year', split(bike_sharesDf_transform['date'], '/').getItem(2))\
                        .withColumn('month', split(bike_sharesDf_transform['date'], '/').getItem(1))\
                        .withColumn('day', split(bike_sharesDf_transform['date'], '/').getItem(0))\
                        .withColumn('date_join', concat(col('day'), lit('-'), ('month'), lit('-'), col('year')))\
                        .drop('date')\
                        .drop(col('month'))\
                        .drop(col('year'))

ny_weatherdf = spark.read.csv(f'{PROJECT_DIR}/data/weather_data_nyc_2016.csv', header=True)

final_df = bike_sharesDf_final\
    .join(ny_weatherdf, bike_sharesDf_final['date_join'] == ny_weatherdf['date'])

final_df = final_df.drop('date_join')

test_df = final_df

test_df.coalesce(1).write.option("header",True)\
        .csv(f"{PROJECT_DIR}/data/result")

print(bike_sharesDf_final.count())
print(test_df.count())
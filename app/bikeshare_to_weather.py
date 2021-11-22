import os, sys

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(1,PROJECT_DIR)

from classes.sparkinit import SparkInstance
from classes.utilities import Common as util


config = util.open_config(f'{PROJECT_DIR}/configs/config.json') 

spark = SparkInstance(config).spark_starter()

bikeshareDf = spark.read.csv('./data/sample.csv', header=True)

transformed_bikeshareDf = util.parse_date(bikeshareDf)

final_bikeshareDf = transformed_bikeshareDf\
    .withColumnRenamed('start station name', 'start_station_name')\
    .withColumnRenamed('end station name',  'end_station_name')\
    .withColumnRenamed('birth year', 'birth_year')


weatherDf = spark.read.csv('./data/weather_data_nyc_2016.csv', header=True)

weatherDf = weatherDf.withColumnRenamed('maximum temperature', 'maximum_temperature')\
                    .withColumnRenamed( 'minimum temperature', 'minimum_temperature')\
                    .withColumnRenamed('average temperature', 'average_temperature')\
                    .withColumnRenamed('snow fall', 'snow_fall')\
                    .withColumnRenamed('snow depth', 'snow_depth')

bikeshare_wheaterDf = final_bikeshareDf.join(weatherDf, final_bikeshareDf['date_for_join'] == weatherDf['date'], 'inner')

bikeshare_wheaterDf_final = bikeshare_wheaterDf.select( 'tripduration', 
                                                        'start_station_name', 
                                                        'end_station_name',
                                                        'usertype',
                                                        'birth_year',
                                                        'gender',
                                                        'date',
                                                        'maximum_temperature',
                                                        'minimum_temperature',
                                                        'average_temperature',
                                                        'precipitation',
                                                        'snow_fall',
                                                        'snow_depth')

util.handle_df_columns(bikeshare_wheaterDf, final_bikeshareDf)

bikeshare_wheaterDf_final.write.mode('overwrite').csv('./data/result_csv')

bikeshare_wheater_analyticsDf = spark.read.csv('./data/result_csv', schema=util.schema(), header=True)

bikeshare_wheater_analyticsDf = util.preanalytics_filter(bikeshare_wheater_analyticsDf)

bikeshare_wheater_analyticsDf.write.mode('overwrite').parquet('./data/result_parquet')

analyticsDf = spark.read.parquet('./data/result_parquet')


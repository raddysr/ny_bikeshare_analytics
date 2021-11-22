import json, os
from pyspark.sql.functions import col, split, lit, concat


class Common:

    def open_config(filepath) -> dict:
        if isinstance(filepath, str) and os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
            return data
    
    def parse_date(df):
        df = df.withColumn('date', split(df['starttime'], ' ').getItem(0))
            
        df = df.withColumn('year', split(df['date'], '/').getItem(2))\
            .withColumn('day', split(df['date'], '/').getItem(1))\
            .withColumn('month', split(df['date'], '/').getItem(0)).drop('date')\
            .withColumn('date_for_join', concat(col('day'), lit('-'), ('month'), lit('-'), col('year'))).drop(col('month')).drop(col('day'))\
            .drop('year')
        return df
    
    def handle_df_columns(df_1, df_2):
        flag = "NOK!"
        if  df_1.count()  == df_2.count():
            flag ="OK!"

        if flag != "OK!":
            raise Exception("Somethings is wrong with DFs")
        print(flag)

    def schema():
        from pyspark.sql.types import StructType, StructField, StringType,  FloatType, IntegerType

        schema = StructType([
            StructField('tripduration', IntegerType()),
            StructField('start_station_name', StringType()),
            StructField('end_station_name', StringType()),
            StructField('user_type', StringType()),
            StructField('birthyear', IntegerType()),
            StructField('gender', IntegerType()),
            StructField('date', StringType()),
            StructField('maximum_temperature', FloatType()),
            StructField('minimum_temperature', FloatType()),
            StructField('average_temperature', FloatType()),
            StructField('precipitation', FloatType()),
            StructField('snow_fall', FloatType()),
            StructField('snow_depth', FloatType()),
        ])
        return schema

    def preanalytics_filter(df):
        df = df.filter(df['tripduration'] > 0)\
            .filter(df['tripduration'] < 5000)\
            .filter(df['maximum_temperature'] > -25).filter(df['maximum_temperature'] < 125)\
            .filter(df['minimum_temperature'] > -25).filter(df['minimum_temperature'] < 125)\
            .filter(df['average_temperature'] > -25).filter(df['average_temperature'] < 125)
        return df

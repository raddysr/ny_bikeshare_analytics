from pyspark.sql import SparkSession

class SparkInstance():

    def __init__(self, config):
        self.app_name = config['spark_conf']['app_name']
        self.master = config['spark_conf']['master']

    def spark_starter(self) -> SparkSession:
        spark = SparkSession.builder\
                .appName(self.app_name)\
                .master(self.master)\
                .getOrCreate()

        return spark


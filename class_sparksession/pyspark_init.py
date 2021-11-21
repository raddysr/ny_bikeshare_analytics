import json, os, re, sys
from typing import Callable, Optional
import logging

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


class PySparkInstance:
    def __init__(self):
        self.conf = "AZ SUM GOTIN PI$$$"

    def spark_starter(self, config) -> SparkSession:
        spark = SparkSession.builder\
                .appName(config['spark_conf']['app_name'])\
                .master(config['spark_conf']['master'])\
                .getOrCreate()

        return spark
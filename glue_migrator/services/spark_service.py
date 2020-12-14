from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext

class SparkService:
    @staticmethod
    def get_spark_env():
        conf = SparkConf()
        sc = SparkContext(conf=conf)
        sc.setLogLevel('ERROR')
        sql_context = SQLContext(sc)
        return (conf, sc, sql_context)
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster('local[*]').setAppName('validation')
conf.set('spark.scheduler.mode', 'FAIR')

def session() -> SparkSession:
    spark = SparkSession.builder.appName('Validation').config(conf=conf).getOrCreate()
    return spark

def sparkcontxt() -> SparkContext:
    sc = SparkContext(conf=conf)
    return sc

from session import ConnctSession
import sys
from MainJob import *
from pyspark.sql.functions import col

if __name__ == '__main__':

    datafile = sys.argv[1]
    table_metadata = sys.argv[2]
    rule = sys.argv[3]
    spark = ConnctSession.session()

    status=execute(spark,datafile,table_metadata,rule)
    df = spark.read.format("csv").option("header", "true").load("test/*")
    df.orderBy(col("Primary_key_val").cast(IntegerType()), ascending=True)\
        .repartition(1)\
        .write\
        .save(path='final4.csv',header=True,format='csv',sep=',',mode='overwrite')



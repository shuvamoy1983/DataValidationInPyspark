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
    print(status)

    if(status):
        df = spark.read.option("header", "true").csv("test.csv/*.csv")
        df.orderBy(col("Primary_key_val").cast(IntegerType()), ascending=True).repartition(1).write.save(path='final3.csv',
                                                                                                     header=True,
                                                                                                     format='csv',
                                                                                                     sep=',')



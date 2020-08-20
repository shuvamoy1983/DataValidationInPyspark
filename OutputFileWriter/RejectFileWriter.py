
from pyspark.sql.types import *
from pyspark.sql.functions import col

class RejectFileWriter:

    def RejectItems(spark):
        df =spark.read.parquet("test/*").orderBy(col("Priority"), col("Primary_key_val").cast(IntegerType()),ascending=True)
        df.select(col("RunID"), col("Data_Source_Name"), col("Application_Name"),
         col("Table_Name"), col("Attribute_Name"), col("inComingRule"), col("Primary_key_val"),
         col("ErrVal"), col("ErrCd"), col("Action"), col("ErrMsg"),
         col("Run_Timestamp"))\
         .repartition(1)\
         .write\
         .save(path='RejectFile.csv',header=True,format='csv',sep=',',mode='overwrite')
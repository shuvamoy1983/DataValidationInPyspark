from session import ConnctSession
import sys
from MainJob import *
from pyspark.sql.functions import col, row_number
import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.window import Window

if __name__ == '__main__':

    datafile = sys.argv[1]
    table_metadata = sys.argv[2]
    rule = sys.argv[3]
    spark = ConnctSession.session()

    #status=execute(spark,datafile,table_metadata,rule)
    df = spark.read.parquet("test/*").orderBy(col("Priority"),col("Primary_key_val").cast(IntegerType()),ascending=True)
    #df.select(col("RunID"), col("Data_Source_Name"), col("Application_Name"),
                  # col("Table_Name"), col("Attribute_Name"), col("inComingRule"), col("Primary_key_val"),
                  # col("ErrVal"), col("ErrCd"), col("Action"), col("ErrMsg"),
                  # col("Run_Timestamp"))\
      # .repartition(1)\
       #.write\
       #.save(path='final4.csv',header=True,format='csv',sep=',',mode='overwrite')

    #df_pk=df.select(col("Primary_key_val").alias("pk"))
    #df3=df.select(col("RunID"),col("Table_Name"),col("Primary_key_val"),col("Attribute_Name")
                   # ,col("Action"))

    df1=df.select(col("RunID"),col("Table_Name"),col("Primary_key_val"),col("Attribute_Name"),col("ErrVal")
                   ,col("Action")).withColumn("json", f.to_json(f.struct("Attribute_Name", "ErrVal")))
    df1.orderBy(col('Primary_key_val'))
    df1.createOrReplaceTempView("test")
    g=spark.sql("select primary_key_val, \
              Runid,table_name,\
              case when Action='Reject' then Attribute_Name||':'||ErrVal end as Reject_val, \
              case when Action='warning' then Attribute_Name||':'||ErrVal end as Warn_Val \
               from test")

    dfRej = g.where(col("Reject_val").isNotNull()).groupBy('primary_key_val','Runid','table_name').agg(f.collect_list("Reject_val").alias("Rejected_attributes"))
    dfReject=dfRej.select(col("primary_key_val").alias("primary_key_val1"), col("Rejected_attributes"),col('Runid').alias('Rid'),col('table_name').alias('tb_name'))

    dfWarn = g.where(col("Warn_Val").isNotNull()).groupBy('primary_key_val','Runid','table_name').agg(f.collect_list("Warn_Val").alias("Warning_attributes"))
    audit= dfWarn.join(dfReject, dfWarn.primary_key_val == dfReject.primary_key_val1 , how='full')

    adt=audit.select(f.when(audit.Runid.isNull(), audit.Rid).otherwise(audit.Runid).alias('RunId'),f.when(audit.table_name.isNull(), audit.tb_name).otherwise(audit.table_name).alias('table_name'),audit.Warning_attributes.cast("string"), audit.Rejected_attributes.cast("string"),f.when(audit.primary_key_val1.isNull(), audit.primary_key_val).otherwise(audit.primary_key_val1).alias('primary_key_val'))
    adt.orderBy(col("Primary_key_val").cast(IntegerType()),ascending=True).show(truncate=False)
    adt.orderBy(col("Primary_key_val").cast(IntegerType()),ascending=True).repartition(1)\
      .write\
      .save(path='audit.csv',header=True,format='csv',mode='overwrite')


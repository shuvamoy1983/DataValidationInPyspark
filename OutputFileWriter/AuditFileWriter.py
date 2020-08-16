from session import ConnctSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark.sql.functions as f

class AuditJob:

    def auditWriter(spark,selectColsDF):

        df = spark.read.parquet("test/*").orderBy(col("Priority"),col("Primary_key_val").cast(IntegerType()),ascending=True)
        valid = spark.read.parquet("valid/*")
        #df1=df.join(valid,df.Primary_key_val == valid.ID,how='full')


        df1 = df.select(col("RunID"), col("Table_Name"), col("Primary_key_val"), col("Attribute_Name"), col("ErrVal")
                        , col("Action")).withColumn("json", f.to_json(f.struct("Attribute_Name", "ErrVal")))

        df1.createOrReplaceTempView("test")
        g = spark.sql("select primary_key_val, \
                     Runid,table_name,\
                     case when Action='Reject' then Attribute_Name||':'||ErrVal end as Reject_val, \
                     case when Action='warning' then Attribute_Name||':'||ErrVal end as Warn_Val \
                      from test")

        dfRej = g.where(col("Reject_val").isNotNull()).groupBy('primary_key_val', 'Runid', 'table_name').agg(
            f.collect_list("Reject_val").alias("Rejected_attributes"))
        dfReject = dfRej.select(col("primary_key_val").alias("primary_key_val1"), col("Rejected_attributes"),
                                col('Runid').alias('Rid'), col('table_name').alias('tb_name'))

        dfWarn = g.where(col("Warn_Val").isNotNull()).groupBy('primary_key_val', 'Runid', 'table_name')\
            .agg(f.collect_list("Warn_Val").alias("Warning_attributes"))
        audit = dfWarn.join(dfReject, dfWarn.primary_key_val == dfReject.primary_key_val1, how='full')

        adt = audit.select(f.when(audit.Runid.isNull(), audit.Rid).otherwise(audit.Runid).alias('RunId'),
                           f.when(audit.table_name.isNull(), audit.tb_name)
                           .otherwise(audit.table_name).alias('table_name'), audit.Warning_attributes.cast("string"),
                           f.when(audit.primary_key_val1.isNull(), audit.primary_key_val).otherwise(
                               audit.primary_key_val1).alias('primary_key_val'),
                           audit.Rejected_attributes.cast("string"))

        adtWrite= adt.join(selectColsDF, adt["primary_key_val"] == selectColsDF["primary_key_val"], how='inner') \
        .select("RunId","table_name",adt["primary_key_val"],"Rejected_attributes","pass_attribute","Warning_attributes")

        adtWrite.printSchema()

        adtWrite.orderBy(col("Primary_key_val").cast(IntegerType()), ascending=True).repartition(1) \
            .write \
            .save(path='audit.csv', header=True, format='csv', mode='overwrite')

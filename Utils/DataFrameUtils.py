from pyspark.sql.types import *
from pyspark.sql.functions import col
from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.functions import array_except, when, array


def newlogic(spark, data, metadata):
    field = [StructField("ID", LongType(), True), StructField("Attribute_Name", StringType(), True),
             StructField("rec", StringType(), True)]
    schema = StructType(field)
    # df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    cols = data.columns
    dt = data.crossJoin(metadata).cache()
    dt.createOrReplaceTempView('t1')
    Id = data.select("ID").alias("tmpID")
    Id.createOrReplaceTempView('id')

    SeriesAppend = []
    for i in cols:
        if i == "ID":
            continue
        else:
            i = spark.sql(
                "select * from (select distinct ID,Attribute_Name,case when Attribute_Name='{0}' then Attribute_Name||':'||{0} end as rec "
                "from t1 ) where rec is not null".format(i))
        SeriesAppend.append(i.cache())
    df_series = reduce(DataFrame.union, SeriesAppend)
    #df_series.filter(col("ID").isin(4, 8)).show(truncate=False)
    df_rec = df_series.groupBy('ID').agg(f.collect_list(col("rec")).alias("pass_attribute"))
    # df_rec.filter(col("ID").isin(4,8)).show(truncate=False)
    df = spark.read.parquet("test/*").orderBy(col("Primary_key_val").cast(IntegerType()),ascending=True).select("Primary_key_val")

    #df1 = df.groupBy('Primary_key_val').agg(f.collect_list(col("Attribute_Name")).alias("ExludedCols"))

    return df_rec.join(df, df_rec.ID == df.Primary_key_val).distinct()


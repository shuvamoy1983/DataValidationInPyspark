from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql.functions import array_except, when, array

def findNonRejectedCols(spark,data,metadata):
    df = spark.read.parquet("test/*").orderBy(col("Priority"), col("Primary_key_val").cast(IntegerType()),
                                              ascending=True)

    df1=df.groupBy('primary_key_val').agg(f.collect_list(col("Attribute_Name")).alias("ExludedCols"))
    cols = data.columns
    mdf = metadata.filter(col("Attribute_Name").isin(cols)).select("Attribute_Name","Table_Name")

    mtCol=mdf.groupBy('Table_Name').agg(f.collect_list(col("Attribute_Name")).alias("mt_col"))
    mergeCols = df1.crossJoin(mtCol)
    mrg=mergeCols.withColumn("pass_attribute", array_except(col("mt_col"), col("ExludedCols")))
    listOfCols=mrg.select(col("primary_key_val"), col("ExludedCols"), col("pass_attribute").cast("string"))
    return listOfCols






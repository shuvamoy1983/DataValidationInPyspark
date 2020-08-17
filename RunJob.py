from session import ConnctSession
import sys
from MainJob import *
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.window import Window
from MainJob import *
from pyspark.sql import  SQLContext
from pyspark.sql.types import *
from OutputFileWriter import RejectFileWriter
from OutputFileWriter import AuditFileWriter
from Utils import DataFrameUtils
import os
import shutil

if __name__ == '__main__':

    datafile = sys.argv[1]
    table_metadata = sys.argv[2]
    rule = sys.argv[3]
    spark = ConnctSession.session()
    data = spark.read.option("Header", "true").csv(datafile)
    tableMetadata = spark.read.option("Header", "true").csv(table_metadata)
    rule = spark.read.option("Header", "true").csv(rule)

    cols = data.columns
    data=createUniqueIdentifier(data)
    status=execute(data,tableMetadata,rule,cols)
    RejectFileWriter.RejectFileWriter.RejectItems(spark)
    passRec=DataFrameUtils.newlogic(spark,data,tableMetadata)
    AuditFileWriter.AuditJob.auditWriter(spark,passRec)


#valid = spark.read.parquet("valid/0/","valid/1/")
#valid.show(10)


import sys
from session import ConnctSession
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import IntegerType
from RuleValidation import CheckRule
import threading
from ThreadJobRun import threadExe

import time
from random import random


def executeValidation(i,mattr,mdf,dataCp):
    if (mattr[i][1].find('|') > 0):
        split_metadata_rule_val = mattr[i][1].split('|')
        attribute = mattr[i][0]
        mdf=mdf.where(col("Attribute_Name")== attribute)
        CheckRule.RuleCheck.Multi_rule_validate(split_metadata_rule_val, attribute, mdf, dataCp,i)

    else:
        metadata_rule_val = mattr[i][1]
        attribute = mattr[i][0]
        mdf = mdf.where(col("Attribute_Name") == attribute)
        CheckRule.RuleCheck.Single_rule_validate(metadata_rule_val, attribute, mdf, dataCp,i)


def execute(spark,datafile,table_metadata,rule):
    data=spark.read.option("Header", "true").csv(datafile)
    tableMetadata=spark.read.option("Header", "true").csv(table_metadata)
    rule=spark.read.option("Header", "true").csv(rule)

    dataCp=data.withColumn("ID", monotonically_increasing_id() +1)
    metadataDf = tableMetadata \
        .select(tableMetadata.Rules_Applicable,
                tableMetadata.Attribute_Name,
                tableMetadata.Table_Primary_Key,
                tableMetadata.Data_Source_Name,
                tableMetadata.Application_Name,
                tableMetadata.Table_Name,
                tableMetadata.Priority
                ).orderBy(tableMetadata.Priority,  ascending=True)
    #metadataDf.show()
    cols=data.columns
    mdf=metadataDf.filter(col("Attribute_Name").isin(cols))
    mattr=mdf.select(col("Attribute_Name"),col("Rules_Applicable")).rdd.map(lambda l: list(l)).collect()
    mcnt=mdf.count()
    threads = []
    try:
        for i in range(mcnt):
            t = threading.Thread(target=executeValidation, args=(i, mattr, mdf, dataCp))
            threads.append(t)
            t.start()

        for thread in threads:
            thread.join()

    except(KeyboardInterrupt):
        print('Keyboard - Interrupted')
        sys.exit()
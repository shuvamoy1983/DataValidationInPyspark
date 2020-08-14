import sys
from session import ConnctSession
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import IntegerType
from RuleValidation import CheckRule
import threading

def executeValidation(i,mattr,mdf,dataCp):
    if (mattr[i][1].find('|') > 0):
        split_metadata_rule_val = mattr[i][1].split('|')
        attribute = mattr[i][0]
        CheckRule.RuleCheck.Multi_rule_validate(split_metadata_rule_val, attribute, mdf, dataCp)
    else:
        metadata_rule_val = mattr[i][1]
        attribute = mattr[i][0]
        CheckRule.RuleCheck.Single_rule_validate(metadata_rule_val, attribute, mdf, dataCp)

if __name__ == '__main__':

    datafile=sys.argv[1]
    table_metadata=sys.argv[2]
    rule=sys.argv[3]
    spark = ConnctSession.session()


    data=spark.read.option("Header", "true").csv(datafile)
    tableMetadata=spark.read.option("Header", "true").csv(table_metadata)
    rule=spark.read.option("Header", "true").csv(rule)

    dataCp=data.withColumn("ID", monotonically_increasing_id() +1)
    #dataCp=dataCp.repartition('ID')
    data_cnt=data.count()
    cnt=tableMetadata.count()

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

    #for i in range(mcnt):
     #   t = threading.Thread(target=executeValidation, args=(i,mattr,mdf,dataCp))
     #   t.start()

    df=spark.read.option("header", "true").csv("test.csv/*.csv")
    #df.orderBy(col("Primary_key_val").cast(IntegerType()),ascending=True).show()
    df.orderBy(col("Primary_key_val").cast(IntegerType()),ascending=True).repartition(1).write.save(path='final.csv', header=True,format='csv', sep=',')
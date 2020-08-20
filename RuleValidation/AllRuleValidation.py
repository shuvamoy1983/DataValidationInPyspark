from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql.functions import trim
from pyspark.sql import Row
from session import ConnctSession
import time
import datetime
import csv
from OutputFileWriter import fileWriter
from pyspark.sql.types import IntegerType
from pyspark.storagelevel import StorageLevel

from RuleValidation import ErrorFileMessage

class AttributeValidate:

    def chkNull(attribute, data, metadaDF, inComingRule,n):
        path='test/'+str(n)
        validpath = 'valid/' + str(n)

        dp = data.where(col(attribute).isNull()).select(col(attribute),col("ID"))
        validation = dp.crossJoin(metadaDF)

        cnt=validation.where(trim(validation.Action)=='Reject').where(validation[attribute].isNull()).count()
        if cnt > 0:
            ErrCd = 'ER6'
            ErrMsg = attribute + ' cloumn has null Value'
            ErrVal = 'null'

            err = ErrorFileMessage.ErrorDetection.errorMessege(ErrVal, ErrCd, ErrMsg,inComingRule)
            rslt=validation.crossJoin(err).repartition(5)

            rslt.select(col("RunID"),col("Data_Source_Name"),col("Application_Name"),
                        col("Table_Name"),col("Attribute_Name"),col("inComingRule"),col("ID").alias("Primary_key_val"),
                        col("ErrVal"),col("ErrCd"),col("Action"),col("ErrMsg"),col("timeStamp").alias("Run_Timestamp"),
                        col("Priority")) \
                 .write.option("mode","append").parquet(path)
                #.save(path=path, header=True,format='csv', mode='append', sep=',')

        Warncnt = validation.where(trim(validation.Action) == 'Warning').where(validation[attribute].isNull()).count()

        if Warncnt > 0:
            #action = 'warning'
            ErrCd = 'ER6'
            ErrMsg = attribute[0] + ' cloumn has null Value'
            ErrVal = 'null'
            print(attribute + ' cloumn has null Value')
            err = ErrorFileMessage.ErrorDetection.errorMessege(ErrVal, ErrCd, ErrMsg,inComingRule)
            rslt = validation.crossJoin(err).repartition(5)

            rslt.select(col("RunID"), col("Data_Source_Name"), col("Application_Name"),
                   col("Table_Name"), col("Attribute_Name"), col("inComingRule"), col("ID").alias("Primary_key_val"),
                   col("ErrVal"), col("ErrCd"), col("Action"), col("ErrMsg"),
                   col("timeStamp").alias("Run_Timestamp"),col("Priority")).write.option("mode","append").parquet(path)



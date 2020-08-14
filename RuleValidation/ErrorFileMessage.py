from session import ConnctSession
import time
import datetime

class ErrorDetection:
    sc = ConnctSession.SparkContext()
    runId = sc._jsc.sc().applicationId()

    def errorMessege(ErrVal, ErrCd, ErrMsg,action,inComingRule):
            ts = time.time()
            st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            columns = ['RunID','ErrVal', 'ErrCd', 'ErrMsg','Action','inComingRule','timeStamp']
            return ErrorDetection.sc.parallelize([(ErrorDetection.runId,ErrVal, ErrCd, ErrMsg,action,inComingRule,st)]).toDF(columns)

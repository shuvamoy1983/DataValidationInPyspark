import threading
from RuleValidation import CheckRule

class ThreadExe:
    threads = []
    def executeValidation(i, mattr, mdf, dataCp):
        if (mattr[i][1].find('|') > 0):
            split_metadata_rule_val = mattr[i][1].split('|')
            attribute = mattr[i][0]
            CheckRule.RuleCheck.Multi_rule_validate(split_metadata_rule_val, attribute, mdf, dataCp, i)

        else:
            metadata_rule_val = mattr[i][1]
            attribute = mattr[i][0]
            CheckRule.RuleCheck.Single_rule_validate(metadata_rule_val, attribute, mdf, dataCp, i)

    def runThread(n,mattr, mdf, dataCp):
        t = threading.Thread(target=ThreadExe.executeValidation, args=(n, mattr, mdf, dataCp))
        ThreadExe.threads.append(t)
        t.start()

        for thread in ThreadExe.threads:
            thread.join()
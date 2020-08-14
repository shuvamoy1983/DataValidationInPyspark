import os.path
import csv

class OutputWritter:

    def isDirExist(filename):
        if(os.path.isdir(filename)):
          return True
        else:
            return False

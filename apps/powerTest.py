from core.powerCore import PowerControlCore
import sys

    
if __name__ == "__main__":
    def log(prtStr, level=None):
        print(prtStr)

    obj = PowerControlCore.load(sys.argv[1:][0], log)
    obj.mergeAndProcessData(obj.planUpdateTime)

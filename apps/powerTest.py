from core.powerCore import PowerControlCore
import sys

    
if __name__ == "__main__":
    obj = PowerControlCore.load(sys.argv[1:][0], print)
    obj.mergeAndProcessData(obj.planUpdateTime)

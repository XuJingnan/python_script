import time

from Constants import *

if __name__ == "__main__":
    day = time.strftime(DAY_FORMAT, time.localtime())
    inFile = open(day + SECOND_OUTPUT_FILE_SUFFIX)
    outFile = open(day + MINUTE_OUTPUT_FILE_SUFFIX, "w")
    flag = False
    try:
        key = None
        accumulatedValues = None
        accumulatedNumber = 0
        while True:
            line = inFile.readline()
            if line == "":
                flag = True
                time.sleep(1)
                continue
            values = line.strip().split(",")
            newKey = values[0][0:15] + "0:00"
            if newKey != key:
                if accumulatedValues is not None:
                    accumulatedValues = [str(v / accumulatedNumber) for v in accumulatedValues]
                    outFile.write(",".join([str(key), ",".join(accumulatedValues)]) + "\n")
                    outFile.flush()
                accumulatedNumber = 1
                accumulatedValues = [float(v) for v in values[1:]]
                key = newKey
            else:
                accumulatedNumber += 1
                for i in range(0, len(accumulatedValues)):
                    accumulatedValues[i] += float(values[i + 1])
                if values[0][11:19] == "23:59:59":
                    accumulatedValues = [str(v / accumulatedNumber) for v in accumulatedValues]
                    outFile.write(",".join([str(key), ",".join(accumulatedValues)]) + "\n")
                    outFile.flush()
                    accumulatedValues = None
                    accumulatedNumber = 0
                    inFile.close()
                    outFile.close()
                    day = time.strftime(DAY_FORMAT, time.localtime())
                    inFile = open(day + SECOND_OUTPUT_FILE_SUFFIX)
                    outFile = open(day + MINUTE_OUTPUT_FILE_SUFFIX, "w")
            if flag:
                time.sleep(1)
    finally:
        inFile.close()
        outFile.close()

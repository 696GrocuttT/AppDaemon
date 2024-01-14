from datetime   import datetime
from datetime   import timedelta
from datetime   import timezone
import re
import math



class PowerUtils():
    def __init__(self, log):
        self.log = log

        
    def powerForPeriod(self, data, startTime, endTime, valueIdxOffset=0):
        power = 0.0
        for forecastPeriod in data:
            forecastStartTime = forecastPeriod[0]
            forecastEndTime   = forecastPeriod[1]
            forecastPower     = forecastPeriod[2+valueIdxOffset]
            # is it an exact match
            if  startTime == forecastStartTime and endTime == forecastEndTime:
                power = power + forecastPower 
                # If its an exact match we should be done, so exit early
                break
            # is it a complete match
            elif startTime <= forecastStartTime and endTime >= forecastEndTime:
                power = power + forecastPower
            # period all within forecost
            elif startTime >= forecastStartTime and endTime <= forecastEndTime:
                # scale the forecast power to the length of the period
                power = power + ( forecastPower * ((endTime         - startTime) / 
                                                   (forecastEndTime - forecastStartTime)) )
            # partial match before
            elif endTime >= forecastStartTime and endTime <= forecastEndTime:
                power = power + ( forecastPower * ((endTime         - forecastStartTime) / 
                                                   (forecastEndTime - forecastStartTime)) )
            # partial match after
            elif startTime >= forecastStartTime and startTime <= forecastEndTime:
                power = power + ( forecastPower * ((forecastEndTime - startTime) / 
                                                   (forecastEndTime - forecastStartTime)) )
        return power


    def opOnSeries(self, a, b, operation, aValueIdxOffset=0, bValueIdxOffset=0):
        return list(map(lambda aSample: ( aSample[0], 
                                          aSample[1], 
                                          operation(aSample[2+aValueIdxOffset], 
                                                    self.powerForPeriod(b, aSample[0], aSample[1], bValueIdxOffset)) ),
                        a))


    def seriesToString(self, series, newLineStr, mergeable=False):
        if mergeable:
            series = self.mergeSeries(series)
        formatStr = "{0:%d %B %H:%M} -> {1:%H:%M} :"
        # Look at the types of the first element of the series to build the rest of the format string
        if series:
            for valueIdx in range(2, len(series[0])):
                # boolean values can be an instance of 'int', so we have to check for bools and exclude them
                if (isinstance(series[0][valueIdx], float) or isinstance(series[0][valueIdx], int)) and not isinstance(series[0][valueIdx], bool):
                    formatStr = formatStr + " {{{0}:.3f}}".format(valueIdx)
                else:
                    formatStr = formatStr + " {{{0}}}".format(valueIdx)            
        strings = map(lambda x: formatStr.format(*x), series)
        return newLineStr.join(strings)


    def mergeSeries(self, series):
        mergedSeries = []
        if series:
            valueIdxList = list(range(2, len(series[0])))
        for item in series:
            # If we already have an item in the merged list, and the last item of that list 
            # has an end time that matches the start time of the new item. Merge them.
            if mergedSeries and mergedSeries[-1][1] == item[0]:
                updatedElement = [mergedSeries[-1][0], item[1]]
                for idx in valueIdxList:
                    updatedElement.append(mergedSeries[-1][idx] + item[idx])
                mergedSeries[-1] = tuple(updatedElement)
            else:
                mergedSeries.append(item)
        return mergedSeries


    def printSeries(self, series, title, mergeable=False, level="WARNING"):
        self.log(title + ":\n" + self.seriesToString(series, "\n", mergeable), level=level)
        
        
    def combineSeries(self, baseSeries, *args):
        output = []
        for idx, baseSample in enumerate(baseSeries):
            outputElement = list(baseSample)
            for extraSeries in args:
                outputElement.append(extraSeries[idx][2])
            output.append(tuple(outputElement))
        return output
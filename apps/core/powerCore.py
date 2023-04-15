from datetime   import datetime
from datetime   import timedelta
from datetime   import timezone
from statistics import mean
import re
import math
import numpy
import matplotlib.pyplot as plt
import json
import os
import pickle
import sys


class PowerControlCore():
    def __init__(self, args, log):
        self.log                   = log
        self.args                  = args
        self.maxChargeRate         = float(args['batteryChargeRateLimit'])
        self.batteryGridChargeRate = float(args['batteryGridChargeRate'])
        self.batReservePct         = float(args['batteryReservePercentage'])
        self.batFullPct            = float(args['batteryFullPercentage'])
        self.gasEfficiency         = float(args['gasHotWaterEfficiency'])
        self.eddiTargetPower       = float(args['eddiTargetPower'])
        self.eddiPowerLimit        = float(args['eddiPowerLimit'])
        self.minBuySelMargin       = float(args['minBuySelMargin'])
        self.minBuyUseMargin       = float(args['minBuyUseMargin'])
        self.gasRate               = 0
        self.batFullPctHysteresis  = 3
        self.batEfficiency         = 0.9
        self.futureTimeWindow      = timedelta(hours=24)
        self.stateSavesPath        = "/conf/stateSaves"
        self.solarData             = []
        self.exportRateData        = []
        self.importRateData        = []
        self.usageData             = []
        self.solarChargingPlan     = []
        self.gridChargingPlan      = []
        self.houseGridPoweredPlan  = []
        self.standbyPlan           = []
        self.dischargePlan         = []
        self.dischargeToHousePlan  = []
        self.eddiPlan              = []
        self.planUpdateTime        = None


    def save(self):
        # Save the state in case we needed for future debug
        slotMidTime = self.planUpdateTime + timedelta(minutes=15)
        fileName    = "{0}/{1:02.0f}_{2:02.0f}.pickle".format(self.stateSavesPath, slotMidTime.hour, 
                                                              30 * math.floor(float(slotMidTime.minute)/30))
        # We can't serialise the logger, so nul out the loger on self so we can serialise 
        # safely, then restore it afterwards
        log      = self.log
        self.log = None
        try:
            with open(fileName, 'wb') as handle:
                pickle.dump(self, handle, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as error:
            log("Error saving state: " + str(error))
        self.log = log


    def load(fileName, log):
        obj = None
        try:
            with open(fileName, 'rb') as handle:
                obj     = pickle.load(handle)
                obj.log = log
        except Exception as error:
            print("Error loading state: " + str(error))
        return obj


    def toFloat(self, string, default):
        try:
            value = float(string)
        except ValueError:
            value = default
        return value

        
    def powerForPeriod(self, data, startTime, endTime, valueIdxOffset=0):
        power = 0.0
        for forecastPeriod in data:
            forecastStartTime = forecastPeriod[0]
            forecastEndTime   = forecastPeriod[1]
            forecastPower     = forecastPeriod[2+valueIdxOffset]
            # is it a complete match
            if startTime <= forecastStartTime and endTime >= forecastEndTime:
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
        

    def seriesToString(self, series, mergeable=False):
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
        return "\n".join(strings)


    def printSeries(self, series, title, mergeable=False):
        self.log(title + ":\n" + self.seriesToString(series, mergeable))


    def opOnSeries(self, a, b, operation, aValueIdxOffset=0, bValueIdxOffset=0):
        return list(map(lambda aSample: ( aSample[0], 
                                          aSample[1], 
                                          operation(aSample[2+aValueIdxOffset], 
                                                    self.powerForPeriod(b, aSample[0], aSample[1], bValueIdxOffset)) ),
                        a))


    def seriesToTariff(self, series, midnight):
        mergedPlan    = self.mergeSeries(series)
        secondsInADay = 24 * 60 * 60
        tariff        = map(lambda x: [int((x[0] - midnight).total_seconds()),
                                       int((x[1] - midnight).total_seconds())], mergedPlan)
        newTariff     = []
        for period in tariff:
            start = period[0]
            end   = period[1]
            if start < secondsInADay:
                start = max(start, 0)
                end   = min(end, secondsInADay-1)
                newTariff.append([start, end])
        return newTariff


    def combineSeries(self, baseSeries, *args):
        output = []
        for idx, baseSample in enumerate(baseSeries):
            outputElement = list(baseSample)
            for extraSeries in args:
                outputElement.append(extraSeries[idx][2])
            output.append(tuple(outputElement))
        return output


    def extendSeries(self, inputSeries, extendBy):                                      
        outputSeries = list(inputSeries)
        if outputSeries:
            endTime = outputSeries[-1][1] + extendBy
            while outputSeries[-1][1] < endTime:
                # Get the details of the last slot
                periodStartTime = outputSeries[-1][0] 
                periodEndTime   = outputSeries[-1][1] 
                periodDuration  = periodEndTime - periodStartTime
                # compute the details of the next slot
                periodStartTime = periodEndTime 
                periodEndTime   = periodEndTime + periodDuration
                power           = self.powerForPeriod(outputSeries, 
                                                      periodStartTime - timedelta(hours=24), 
                                                      periodEndTime   - timedelta(hours=24)) 
                outputSeries.append((periodStartTime, periodEndTime, power))
        return outputSeries
        
        
    def mergeAndProcessData(self, now):
        self.log("Updating schedule")        
        # Calculate the solar surplus after house load, we base this on the usage time 
        # series dates as that's typically a finer granularity than the solar forecast. Similarly 
        # we work out the house usage after any forecast solar. The solar forecast has 3 values in 
        # the following order, a 50th percentile followed by a low and high estimate of the power 
        # for each period. We carry this through to the generated series so we can more accuratly 
        # plan the battery charge / house usage.
        solarSurplus    = self.combineSeries(self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, b-a)), 
                                             self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, b-a), 0, 1), 
                                             self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, b-a), 0, 2))
        solarUsage      = self.combineSeries(self.opOnSeries(solarSurplus,   self.solarData, lambda a, b: b-a),
                                             self.opOnSeries(solarSurplus,   self.solarData, lambda a, b: b-a, 1, 1),
                                             self.opOnSeries(solarSurplus,   self.solarData, lambda a, b: b-a, 2, 2))
        usageAfterSolar = self.combineSeries(self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, a-b)),
                                             self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, a-b), 0, 1),
                                             self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, a-b), 0, 2))
        # Remove rates that are in the past
        exportRateData = self.exportRateData
        importRateData = self.importRateData
        if self.args.get('extendTariff', False):
            exportRateData = self.extendSeries(exportRateData, self.futureTimeWindow)
            importRateData = self.extendSeries(importRateData, self.futureTimeWindow)
        exportRateData = list(filter(lambda x: x[1] >= now, exportRateData))
        importRateData = list(filter(lambda x: x[1] >= now, importRateData))
        # remove any import rate data that is outside the time range for the export rates and vice 
        # versa. This means we can safely evelauate everything together
        exportRateEndTime           = max(exportRateData, key=lambda x: x[1])[1]
        importRateEndTime           = max(importRateData, key=lambda x: x[1])[1]
        exportRateData              = list(filter(lambda x: x[1] <= importRateEndTime, exportRateData))
        importRateData              = list(filter(lambda x: x[1] <= exportRateEndTime, importRateData))
        self.originalExportRateData = list(exportRateData)
        self.originalImportRateData = list(importRateData)
        # We can't import and export at the same time, so remove and import rates slots for times when
        # there's a solar surplus. In reality it isn't quite this simple, eg if there's 1KW of surplus 
        # we could charge at 3kw by pulling some from the grid. But for the moment this hybrid style 
        # charging isn't worth the extra complexity it would involve
        importRateData = list(filter(lambda x: self.powerForPeriod(solarSurplus, x[0], x[1]) <= 0, importRateData))
        
        # calculate the charge plan, and work out what's left afterwards
        (solarChargingPlan, gridChargingPlan, 
         dischargePlan, houseGridPoweredPlan) = self.calculateChargePlan(exportRateData, importRateData, solarUsage, solarSurplus, usageAfterSolar, now)
        postBatteryChargeSurplus              = self.opOnSeries(solarSurplus, solarChargingPlan, lambda a, b: a-b)
        # Calculate the times when we want the battery in standby mode. IE when there's solar surplus 
        # but we don't want to charge or discharge.
        standbyPlan = []
        for rate in exportRateData:
            curSolarSurplus =  self.powerForPeriod(solarSurplus,         rate[0], rate[1])
            isPlanned       = (self.powerForPeriod(solarChargingPlan,    rate[0], rate[1]) > 0 or
                               self.powerForPeriod(gridChargingPlan,     rate[0], rate[1]) > 0 or
                               self.powerForPeriod(houseGridPoweredPlan, rate[0], rate[1]) > 0 or
                               self.powerForPeriod(dischargePlan,        rate[0], rate[1]) > 0)
            if (curSolarSurplus > 0) and not isPlanned: 
                standbyPlan.append((rate[0], rate[1], curSolarSurplus))
        # Create a background plan for info only that shows when we're just powering the house from the battery.
        usageForRateSlotsOnly = self.opOnSeries(exportRateData, self.usageData, lambda a, b: b)
        dischargeToHousePlan  = self.opOnSeries(usageForRateSlotsOnly, solarChargingPlan,    lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.opOnSeries(dischargeToHousePlan,  gridChargingPlan,     lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.opOnSeries(dischargeToHousePlan,  houseGridPoweredPlan, lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.opOnSeries(dischargeToHousePlan,  standbyPlan,          lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.opOnSeries(dischargeToHousePlan,  dischargePlan,        lambda a, b: 0 if b else a)
        dischargeToHousePlan  = list(filter(lambda x: x[2], dischargeToHousePlan))

        # Calculate the eddi plan based on any remaining surplus
        eddiPlan = self.calculateEddiPlan(exportRateData, postBatteryChargeSurplus, solarChargingPlan)
        
        # Create a fake tariff with peak time covering the discharge plan
        midnight                     = now.replace(hour=0, minute=0, second=0, microsecond=0)
        # Filter out anything except the 2 hours. This prevents the powerwall not behaving properly
        # because it thinks it won't have enough time to charge later.
        tariffEnd                    = now + timedelta(hours=2)
        dischargePlanNextHour        = list(filter(lambda x: x[1] <= tariffEnd, dischargePlan))
        standbyPlanNextHour          = list(filter(lambda x: x[1] <= tariffEnd, standbyPlan))
        houseGridPoweredPlanNextHour = list(filter(lambda x: x[1] <= tariffEnd, houseGridPoweredPlan))
        solarChargingPlanNextHour    = list(filter(lambda x: x[1] <= tariffEnd, solarChargingPlan))
        # Normally we wouldn't have the solarChargePlan as one of the peak periods. There is some deep 
        # twisted logic to this. Firstly it doesn't actually matter as we set the powerwall to Self-powered 
        # when we want to charge from solar, which doesn't use the tariff plan. The powerwall sometimes
        # takes awhile to respond to tariff updates. This means that if the plan changes from change to 
        # standby then we don't want this to impact the tariff plan we need (which could take awhile to
        # update). To get round this we pre-emptivly set charging periods to peak in the tariff plan in 
        # case we need to swap. We also extend the peak period into the past a bit. This prevents any
        # strange behaviour given we have to have to change the battery settings just before the start of 
        # each hour.
        hourStart     = (now + timedelta(minutes=15)).replace(minute=0, second=0, microsecond=0)
        peakPlan      = [(hourStart - timedelta(minutes=15), hourStart + timedelta(hours=2), 0)]
        peakPeriods   = self.seriesToTariff(peakPlan, midnight)
        self.defPrice = "0.10 0.10 OFF_PEAK"
        self.pwTariff = {"0.90 0.90 ON_PEAK": peakPeriods}
        self.printSeries(solarChargingPlan,    "Solar charging plan",       mergeable=True)
        self.printSeries(gridChargingPlan,     "Grid charging plan",        mergeable=True)
        self.printSeries(houseGridPoweredPlan, "House grid powered plan",   mergeable=True)
        self.printSeries(standbyPlan,          "Standby plan",              mergeable=True)
        self.printSeries(dischargePlan,        "Discharging plan",          mergeable=True)
        self.printSeries(dischargeToHousePlan, "Discharging to house plan", mergeable=True)
        self.printSeries(eddiPlan,             "Eddi plan",                 mergeable=True)
        self.solarChargingPlan    = solarChargingPlan
        self.gridChargingPlan     = gridChargingPlan
        self.houseGridPoweredPlan = houseGridPoweredPlan
        self.standbyPlan          = standbyPlan
        self.dischargePlan        = dischargePlan
        self.dischargeToHousePlan = dischargeToHousePlan
        self.eddiPlan             = eddiPlan
        self.planUpdateTime       = now
        

    def eddiTargetRate(self):
        return self.gasRate / self.gasEfficiency


    def calculateEddiPlan(self, exportRateData, solarSurplus, batterSolarChangePlan):
        # Calculate the target rate for the eddi
        eddiPlan          = []
        eddiTargetRate    = self.eddiTargetRate()
        eddiPowerRequired = self.eddiTargetPower - self.eddiPowerUsedToday
        ratesCheapFirst   = sorted(exportRateData, key=lambda x: x[2])
        for rate in ratesCheapFirst:
            if rate[2] > eddiTargetRate:
                break
            maxPower   = ((rate[1] - rate[0]).total_seconds() / (60 * 60)) * self.eddiPowerLimit
            power      = self.powerForPeriod(solarSurplus, rate[0], rate[1])
            powerTaken = max(min(power, maxPower), 0)
            # We still plan to use the eddi even if the forcast says there won't be a surplus.
            # This is in case the forcast is wrong, or there are dips in usage or peaks in 
            # generation that lead to short term surpluses
            eddiPlan.append((rate[0], rate[1], powerTaken))
            if power > 0:
                eddiPowerRequired = eddiPowerRequired - powerTaken
                if eddiPowerRequired < 0:
                    break
        # Add on any slots where the battery is charging and the rate is below the threshold. 
        # This means we divert any surplus that wasn't forecast that the battery could change 
        # from. EG if the battery fills up early, or we exceed the battery charge rate.
        for chargePeriod in batterSolarChangePlan:
            # If the entry is already in the eddi plan, don't try and add it again
            if not any(x[0] == chargePeriod[0] for x in eddiPlan):
                exportRate = next(filter(lambda x: x[0] == chargePeriod[0], exportRateData))
                if exportRate[2] <= eddiTargetRate:
                    eddiPlan.append((chargePeriod[0], chargePeriod[1], 0))
        eddiPlan.sort(key=lambda x: x[0])
        return eddiPlan
 
 
    def convertToAppPercentage(self, value):
        # The battery reserves 5% so the battery is never completely empty. This is fudged in 
        # the app as it shows an adjusted percentage scale. This formula replicates that so we
        # can directyl compare percentages
        return (value - 5) / 0.95


    def convertToRealPercentage(self, value):
        # Calculates the inverse of the convertToAppPercentage function
        return (value * 0.95) + 5


    def genBatLevelForecast(self, exportRateData, usageAfterSolar, solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, now, percentileIndex):
        batForecast      = []
        # For full charge detection we compare against 99% full, this is so any minor changes 
        # is battery capacity or energe when we're basically fully charged, and won't charge 
        # any more, don't cause any problems.
        batFullPct       = min(self.batFullPct, 99)
        batReserveEnergy = self.batteryCapacity * (self.convertToRealPercentage(self.batReservePct) / 100)
        batteryRemaining = self.batteryEnergy
        emptyInAnySlot   = False
        fullInAnySlot    = False
        totChargeEnerge  = 0.0
        # The rate data is just used as a basis for the timeline
        for (index, rate) in enumerate(exportRateData):
            chargeEnergy     = (self.powerForPeriod(solarChargingPlan,    rate[0], rate[1], percentileIndex) +
                                self.powerForPeriod(gridChargingPlan,     rate[0], rate[1]))
            batteryRemaining = (batteryRemaining + chargeEnergy - 
                                self.powerForPeriod(usageAfterSolar,      rate[0], rate[1], percentileIndex) +
                                self.powerForPeriod(houseGridPoweredPlan, rate[0], rate[1]))
            totChargeEnerge  = totChargeEnerge + chargeEnergy
            fullyChanged     = batteryRemaining >= self.batteryCapacity
            empty            = batteryRemaining <= batReserveEnergy
            if fullyChanged:
                fullInAnySlot    = True
                batteryRemaining = self.batteryCapacity
            if empty:
                emptyInAnySlot   = True
                batteryRemaining = batReserveEnergy
            pct = round(self.convertToAppPercentage((batteryRemaining / self.batteryCapacity) * 100), 1)
            batForecast.append((rate[0], rate[1], batteryRemaining, fullyChanged, empty, pct))
           
        # calculate the end time of the last fully charged slot
        lastFullSlotEndTime = None
        if fullInAnySlot:
            lastFullSlotEndTime = next(filter(lambda x: x[3], reversed(batForecast)))[1]
        # We need to work out if the battery is fully charged in a time slot after 4pm on the
        # last day of the forecast. When calculating the battery full energy we add a bit of
        # hysteresis based on whether there are any charge slots in the current plan before midday. 
        # This effectily means that we aim to charge to a slightly higher value and when we
        # discharge we'll only add extra charge slots if we go below a slightly lower value. The 
        # aim of this is to prevent slight changes in usage etc from suddenly causing an extra high
        # cost charging slot to be added at the last minute.
        # NOTE: We pick a target full time of 4:30pm as this is after we get the next days price info. 
        #       So making sure we're in a reasonable state of charge before we know how bad/good the 
        #       next day is going to be.
        hysteresis                = self.batFullPctHysteresis if totChargeEnerge else -self.batFullPctHysteresis
        batFullEnergy             = self.batteryCapacity * ((self.batFullPct + hysteresis) / 100)
        lastTargetFullTime        = batForecast[-1][0].replace(hour=16, minute=30, second=0, microsecond=0)
        fullChargeAfterTargetTime = any(x[0] >= lastTargetFullTime and x[2] >= batFullEnergy for x in batForecast)
        # We also indicate the battery is fully charged if its after midday now, and its currently 
        # fully charged. This prevents an issue where the current time slot is never allowed to 
        # discharge if we don't have a charging period for tomorrow mapped out already
        if not fullChargeAfterTargetTime:
            if self.batteryEnergy > batFullEnergy and now >= lastTargetFullTime:
                fullChargeAfterTargetTime = True
        return (batForecast, lastTargetFullTime, fullChargeAfterTargetTime, lastFullSlotEndTime, emptyInAnySlot)


    def chooseRate(self, rateA, rateB, notAfterTime):
        foundRate = []
        isRateA   = None
        # if requested don't use any slots after the specified time
        if notAfterTime:
            rateA = list(filter(lambda x: x[0] <= notAfterTime, rateA))
            rateB = list(filter(lambda x: x[0] <= notAfterTime, rateB))
        # choose the cheapest of the two rates, but checking for corner cases like no rates left
        if rateA and rateB:
            isRateA   = rateA[0][2] < rateB[0][2]
            foundRate = rateA[0] if isRateA else rateB[0]
        elif rateA:
            isRateA   = True 
            foundRate = rateA[0]
        elif rateB:
            isRateA   = False
            foundRate = rateB[0]
        return (foundRate, isRateA)


    def chooseRate3(self, rateA, rateB, rateC, notAfterTime):
        (foundRate, isRateA)  = self.chooseRate(rateA,     rateB, notAfterTime)
        foundRate             = [foundRate] if foundRate else []
        (foundRate, isRateAB) = self.chooseRate(foundRate, rateC, notAfterTime)
        rateId = (2 if not isRateAB else
                  0 if     isRateA  else 1)
        return (foundRate, rateId)


    def allocateChangingSlots(self, exportRateData, availableChargeRates, availableImportRates, availableHouseGridPoweredRates, solarChargingPlan, 
                              gridChargingPlan, houseGridPoweredPlan, solarSurplus, usageAfterSolar, now, maxImportRate, topUpToChargeCost = None):
        # We create a local copy of the available rates as there some cases (if there's no solar
        # surplus) where we don't want to remove an entry from the availableChargeRates array, 
        # but we need to remove it locally so we can keep track of which items we've used, and 
        # which are still available
        availableChargeRatesLocal           = list(availableChargeRates)
        availableImportRatesLocal           = list(availableImportRates)
        availableImportRatesLocalUnused     = list(availableImportRatesLocal)
        availableHouseGridPoweredRatesLocal = list(availableHouseGridPoweredRates)
        # The percentile index is used to select the 50th percentile (index 0) or the low (index 1)
        # or high (index 2) estimates. Which one we choose changes based on whether we're trying to 
        # make sure the battery doesn't go flat, or whether we're topping it up and don't want to 
        # over charge it and end up with a surplus that just goes to the grid. Unless we're explicitly 
        # being asked to add a topup, we start off with the low estimate as the first passes are to 
        # ensure the battery doesn't go flat, with later passes topping it up.
        percentileIndex                     = 2 if topUpToChargeCost else 1
        # Keep producing a battery forecast and adding the cheapest charging slots until the battery is full
        maxChargeCost                       = 0
        (batProfile, fullEndTimeThresh,
         fullyCharged, lastFullSlotEndTime, 
         empty)                             = self.genBatLevelForecast(exportRateData, usageAfterSolar, solarChargingPlan, 
                                                                       gridChargingPlan, houseGridPoweredPlan, now, percentileIndex)
        # initialise the allow empty before variable to the start of the profile so it has no effect to start with
        allowEmptyBefore                    = batProfile[0][0]
        maxAllowedChargeCost                = topUpToChargeCost if topUpToChargeCost else math.inf
        while empty or topUpToChargeCost or not fullyCharged:
            # If the battery has gone flat during at any point, make sure the charging slot we search for is before the point it went flat
            chargeBefore   = None
            firstEmptySlot = None
            if empty:
                percentileIndex = 1
                firstEmptySlot  = next(filter(lambda x: x[4] and x[0] >= allowEmptyBefore, batProfile), None)
                if firstEmptySlot:
                    firstEmptySlot = firstEmptySlot[1]
                    chargeBefore   = firstEmptySlot
            else:
                percentileIndex = 2
                # If we're topping up the battery to full, then don't add slots after the full theshold end 
                # time, as they won't actually help meet the full battery criteria.
                chargeBefore    = fullEndTimeThresh
            # Search for a charging slot
            (chargeRate, rateId) = self.chooseRate3(availableChargeRatesLocal, availableImportRatesLocal, availableHouseGridPoweredRatesLocal, chargeBefore)                
            if chargeRate:
                timeInSlot = (chargeRate[1] - chargeRate[0]).total_seconds() / (60 * 60)
                # The charge cost is the cost to get x amount of energy in the battery, due to the overheads
                # this is higher than the cost of the rate used to charge the battery.
                chargeCost = chargeRate[2] / self.batEfficiency
                # Pre calculate if the charge rase is below the max import rate. For this comparison we
                # use the raw charge cost and don't take account of the battery efficency, is this gives
                # us an apples to apples comparison with the import rates.
                belowMaxImportRate = chargeRate[2] < maxImportRate
                # Only allow charging if there's room in the battery for this slot, and its below the max
                # charge cost allowed
                willCharge = (chargeCost <= maxAllowedChargeCost) and not next(filter(lambda x: x[0] == chargeRate[0], batProfile))[3]
                # Don't add any charging slots that are before the last fully charged slot, as it won't help
                # get the battery to fully change at our target time, and it just fills the battery with more 
                # expensive electricity when there's cheaper electriticy available later.
                if lastFullSlotEndTime:
                    willCharge = willCharge and chargeRate[1] >= lastFullSlotEndTime
                if rateId == 0: # solar
                    maxCharge = timeInSlot * self.maxChargeRate
                    powerMed  = self.powerForPeriod(solarSurplus, chargeRate[0], chargeRate[1])
                    powerLow  = self.powerForPeriod(solarSurplus, chargeRate[0], chargeRate[1], 1)
                    powerHigh = self.powerForPeriod(solarSurplus, chargeRate[0], chargeRate[1], 2)
                    power     = (powerMed, powerLow, powerHigh)[percentileIndex]
                    # we can only add something to the charge plan if there's surplus solar
                    willCharge = willCharge and power > 0
                    if willCharge:
                        solarChargingPlan.append((chargeRate[0], chargeRate[1], min(powerMed,  maxCharge), 
                                                                                min(powerLow,  maxCharge),
                                                                                min(powerHigh, maxCharge)))
                        # we can only use a charging slot once, so remove it from the available list            
                        availableChargeRates.remove(chargeRate)
                    # We always remove the rate from the local array, otherwise we could end up trying 
                    # to add the same zero power rate again and again. We don't want to remove these rates
                    # from the availableChargeRates as we want these slots to be available outside this 
                    # function for other types of activity
                    availableChargeRatesLocal.remove(chargeRate)
                elif rateId == 1: # grid charge
                    # Don't charge off the max grid powered slot, its better to just let the battery go flat 
                    # in this case. 
                    willCharge = willCharge and belowMaxImportRate
                    # We don't want to end up charging the battery when its cheaper to just run the house 
                    # directly from the grid. So if the battery is going to be empty, check what the 
                    # electricity import rate is for the slot where it goes empty and compare that to the
                    # cheapest charge rate we've found to determine if we should use this charge rate or not.
                    if firstEmptySlot:
                        emptySlotCost = next(filter(lambda x: x[1] == firstEmptySlot, self.originalImportRateData), None)[2]
                        cheapEnough   = (chargeCost <= emptySlotCost - self.minBuyUseMargin)
                        # If we're not using the slot because its not cheap enough, then we shouldn't remove
                        # the slot from the list of available slots. This is because we might encounter an
                        # empty slot later on where the cost differential is large enough to warrant using
                        # this slot. There is a side effect to this. Becauase we might not be removing the
                        # slot from the available slot list, we need another way of making sure we don't just
                        # try the same slot next time arround and end up in an infinite loop. To handle all of 
                        # this we maintain two sets of slot lists:
                        #   availableImportRatesLocal: Is the list of slots currently being considered, we 
                        #     always remove entries from this as we check them. Even if the reason we're 
                        #     rejected the slot is that its not cheap enough. This slot list is used for 
                        #     checking on the next iteration, so this behaviour prevents infinite loops.
                        #   availableImportRatesLocalUnused: This list contains all the unused slots, we only 
                        #     remove a slot from this list if we've eliminated the slot for a reason other than 
                        #     it not being cheap enough. Every time we update allowEmptyBefore we restore 
                        #     availableImportRatesLocal based on whats in availableImportRatesLocalUnused so we 
                        #     can reconsider slots there were rejected because they weren't cheap enough for 
                        #     the empty slot cost we were considering at the time.
                        slotUsed      = not willCharge or  cheapEnough
                        willCharge    =     willCharge and cheapEnough
                    # We don't want to buy power from the grid if we're going going empty, just to top up the 
                    # battery for the sake of it. So we only allow grid charging to fill the battery if there's
                    # solar slots left that we can export at a higher price than the grid import. Because the
                    # chooseRate3() function will always choose the cheapest slot available. This boils down 
                    # to just checking that there are solar charge slots still available. The exception to this 
                    # is if we've been asked to top up to an explicit charge cost.
                    else:
                        slotUsed   = True
                        willCharge = willCharge and (availableChargeRatesLocal or topUpToChargeCost)
                    # If the charge slot is still valid, add it to the plan now
                    if willCharge:
                        chargeTaken = timeInSlot * self.batteryGridChargeRate
                        # we can only use a charging slot once, so remove it from the available list
                        availableImportRates.remove(chargeRate)
                        gridChargingPlan.append((chargeRate[0], chargeRate[1], chargeTaken))
                    # Same reason as above, always remove the local charge rate
                    availableImportRatesLocal.remove(chargeRate)
                    # See detaied explanation where slotUsed is set above
                    if slotUsed:
                        availableImportRatesLocalUnused.remove(chargeRate)
                elif rateId == 2: # house on grid power
                    # Because we're not actually charging the battery, the "chargeCost" is just the rate, and
                    # doesn't take into account the battery efficency.
                    chargeCost = chargeRate[2]
                    # Don't run the house on grid power if the slot is the max grid powered price, we might as
                    # well just let the battery go flat, and in some cases due to the margins we wouldn't actually
                    # end up using that much grid power as we'd pre-planned it.
                    willCharge = willCharge and belowMaxImportRate
                    if willCharge:
                        usage     = self.powerForPeriod(usageAfterSolar, chargeRate[0], chargeRate[1])
                        usageLow  = self.powerForPeriod(usageAfterSolar, chargeRate[0], chargeRate[1], 1)
                        usageHigh = self.powerForPeriod(usageAfterSolar, chargeRate[0], chargeRate[1], 2)
                        # we can only use a charging slot once, so remove it from the available list
                        availableHouseGridPoweredRates.remove(chargeRate)
                        houseGridPoweredPlan.append((chargeRate[0], chargeRate[1], usage, usageLow, usageHigh))
                    # Same reason as above, always remove the local charge rate
                    availableHouseGridPoweredRatesLocal.remove(chargeRate)
                    
                if willCharge:
                    maxChargeCost = max(maxChargeCost, chargeCost)
                    # update the battery profile based on the new charging plan
                    (batProfile, _, fullyCharged, 
                     lastFullSlotEndTime, empty) = self.genBatLevelForecast(exportRateData, usageAfterSolar, solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, now, percentileIndex)   
            elif firstEmptySlot:
                # If the battery gets empty then the code above we restrict the search for a charging 
                # slot to the time before it gets empty. This can result in not finding a charge slot. 
                # In this case we don't terminate the search we just allow the battery to be empty for 
                # that slot and try again to change during a later slot.
                allowEmptyBefore          = firstEmptySlot
                # See detaied explanation where slotUsed is set above
                availableImportRatesLocal = list(availableImportRatesLocalUnused)
            else:
                break
        return (batProfile, fullyCharged, empty, maxChargeCost)

    
    def houseRateForPeriod(self, startTime, endTime, exportRateData, importRateData, solarSurplus):
        surplus = self.powerForPeriod(solarSurplus, startTime, endTime)
        if surplus > 0:
            rate = next(filter(lambda x: x[0] == startTime, exportRateData), None)
        else:
            rate = next(filter(lambda x: x[0] == startTime, importRateData), None)
        return rate


    def maxHouseRateForEmpty(self, batProfile, exportRateData, importRateData, solarSurplus):
        maxRate = None
        for batEntry in filter(lambda x: x[4], batProfile):
            curRate = self.houseRateForPeriod(batEntry[0], batEntry[1], exportRateData, importRateData, solarSurplus)
            if maxRate == None:
                maxRate = curRate[2]
            else:
                maxRate = max(maxRate, curRate[2])
        return maxRate

    
    def calculateChargePlan(self, exportRateData, importRateData, solarUsage, solarSurplus, usageAfterSolar, now):
        solarChargingPlan    = []
        gridChargingPlan     = []
        dischargePlan        = []
        houseGridPoweredPlan = []
        availableChargeRates = sorted(exportRateData, key=lambda x: x[2])
        availableImportRates = sorted(importRateData, key=lambda x: (x[2], x[0]))
        minImportChargeRate  = min(map(lambda x: x[2], self.originalImportRateData)) / self.batEfficiency
        maxImportRate        = max(map(lambda x: x[2], self.originalImportRateData))
        # We create a set of effective "charge" rates associated with not discharging the battery. The 
        # idea is that if we choose not to discharge for a period that's the same as charging the battery 
        # with the same amount of power. It's actually better than this because not cycling the battery
        # means we reduce the battery wear, and don't have the battery efficency overhead. 
        availableHouseGridPoweredRates = list(availableImportRates)

        # We don't want to discharge the battery for any slots where the cost of running the house off 
        # the grid is lower than what we've previously paid to charge the battery. So add any grid 
        # powered rates that are below the current charge cost
        for rate in list(filter(lambda x: x[2] < self.maxChargeCost, availableHouseGridPoweredRates)):
            availableHouseGridPoweredRates.remove(rate)
            usage = self.powerForPeriod(usageAfterSolar, rate[0], rate[1])
            houseGridPoweredPlan.append((rate[0], rate[1], usage))

        # calculate the initial charging profile
        (batProfile, _, _, newMaxChargeCost) = self.allocateChangingSlots(exportRateData, availableChargeRates, availableImportRates, availableHouseGridPoweredRates,  
                                                                          solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, solarSurplus, usageAfterSolar, now,
                                                                          maxImportRate)
        maxChargeCost                        = max(newMaxChargeCost, self.maxChargeCost)

        # look at the most expensive rate and see if there's solar usage we can flip to battery usage so
        # we can export more. We only do this if we still end up fully charged. We can't use the 
        # availableChargeRates list directly, as we need to remove entries as we go, and we still need 
        # to have a list of available charge slots after this step.
        potentialDischargeRates = list(availableChargeRates)
        while potentialDischargeRates:
            mostExpenciveRate = potentialDischargeRates[-1]
            del potentialDischargeRates[-1]
            solarUsageForRate = self.powerForPeriod(solarUsage, mostExpenciveRate[0], mostExpenciveRate[1])
            if solarUsageForRate > 0:
                newDischargeSlot                  = (mostExpenciveRate[0], mostExpenciveRate[1], solarUsageForRate)
                adjustBy                          = [newDischargeSlot]
                # Create a new adjusted version of the solar suprlus and usage after solar accounting for the
                # slow we're proposing to discharge in. NOTE: We do this 3 times for the 50th percental and
                # the low and high estitames of the solar data.
                newSolarSurplus                   = self.combineSeries(self.opOnSeries(solarSurplus,    adjustBy, lambda a, b: a+b),
                                                                       self.opOnSeries(solarSurplus,    adjustBy, lambda a, b: a+b, 1, 0),
                                                                       self.opOnSeries(solarSurplus,    adjustBy, lambda a, b: a+b, 2, 0))
                newUsageAfterSolar                = self.combineSeries(self.opOnSeries(usageAfterSolar, adjustBy, lambda a, b: a+b),
                                                                       self.opOnSeries(usageAfterSolar, adjustBy, lambda a, b: a+b, 1, 0),
                                                                       self.opOnSeries(usageAfterSolar, adjustBy, lambda a, b: a+b, 2, 0))
                newAvailableChargeRates           = list(availableChargeRates)
                newSolarChargingPlan              = list(solarChargingPlan)
                # We can't change in the slot we're trying to discharge in, so remove this from the trial list.
                newAvailableChargeRates.remove(mostExpenciveRate)
                # We can't charge and discharge at the same time, so remove the proposed discharge slot from 
                # the available charge rates. We also do the same for the existing import slots. It can make
                # sense to swap one import slot for export because the import and export prices are so different.
                newAvailableImportRates           = list(filter(lambda x: x[0] != newDischargeSlot[0], availableImportRates))
                newAvailableHouseGridPoweredRates = list(filter(lambda x: x[0] != newDischargeSlot[0], availableHouseGridPoweredRates))
                newGridChargingPlan               = list(filter(lambda x: x[0] != newDischargeSlot[0], gridChargingPlan))
                newHouseGridPoweredPlan           = list(filter(lambda x: x[0] != newDischargeSlot[0], houseGridPoweredPlan))
                (batProfile, fullyCharged, 
                 empty, newMaxChargeCost)         = self.allocateChangingSlots(exportRateData, newAvailableChargeRates, newAvailableImportRates, newAvailableHouseGridPoweredRates, 
                                                                               newSolarChargingPlan, newGridChargingPlan, newHouseGridPoweredPlan, newSolarSurplus, newUsageAfterSolar, 
                                                                               now, maxImportRate)    
                newMaxChargeCost                  = max(maxChargeCost, newMaxChargeCost)
                # If we're still fully charged after swapping a slot to discharging, then make that the plan 
                # of record by updating the arrays. We also skip a potential discharge period if the 
                # difference between the cost of the charge / discharge periods isn't greater than the 
                # threshold. This reduces battery cycling if there's not much to be gained from it.
                newMaxCostRate          = newMaxChargeCost
                newMaxHouseRateForEmpty = self.maxHouseRateForEmpty(batProfile, exportRateData, importRateData, solarSurplus)
                if newMaxHouseRateForEmpty != None:
                    newMaxCostRate = max(newMaxChargeCost, newMaxHouseRateForEmpty)
                if fullyCharged and mostExpenciveRate[2] - newMaxCostRate > self.minBuySelMargin:
                    maxChargeCost                  = newMaxChargeCost
                    dischargePlan.append(newDischargeSlot)
                    solarSurplus                   = newSolarSurplus         
                    usageAfterSolar                = newUsageAfterSolar     
                    availableChargeRates           = newAvailableChargeRates
                    availableImportRates           = newAvailableImportRates
                    solarChargingPlan              = newSolarChargingPlan
                    gridChargingPlan               = newGridChargingPlan
                    houseGridPoweredPlan           = newHouseGridPoweredPlan
                    availableHouseGridPoweredRates = newAvailableHouseGridPoweredRates
                    # We can't discharge for a slot if its already been used as a charge slot. So filter out 
                    # any potential discharge slots if they're not still in the available charge list.
                    potentialDischargeRates        = list(filter(lambda x: x in availableChargeRates, potentialDischargeRates))

        self.printSeries(batProfile, "Battery profile - pre topup")
        # Now allocate any final charge slots topping up the battery as much as possible, but not exceeding
        # the minimum of the lowest import cost or the max solar charge cost. This means we won't end up
        # increasing the overall charge cost per/kwh. In addition, this means that we'll top up to 100%
        # overright if that's the cheaper option, or if the solar export is a lower cost we'll end up topping
        # up to 100% during the day. This in turn means we're more likely to be prepared for the next day. EG
        # if we need a higher charge level at the end of the day if we need to make it all the way to the
        # next days solar charge period, or a lower charge level at the end of the day because we only need
        # to make it to the overright charge period max charge cost we've already established. One usecase
        # for this adding additional night time grid charge slots
        potentialSolarChargeSlots = list(filter(lambda x: x[2], solarSurplus))
        solarChargeExportRates    = self.opOnSeries(potentialSolarChargeSlots, exportRateData, lambda a, b: b)
        maxSolarChargeCost        = max(map(lambda x: x[2] / self.batEfficiency, solarChargeExportRates), default=0)
        topUpMaxCost              = min(maxSolarChargeCost, minImportChargeRate)
        (batProfile, _, _, 
         newMaxChargeCost)        = self.allocateChangingSlots(exportRateData, availableChargeRates, availableImportRates, availableHouseGridPoweredRates,  
                                                               solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, solarSurplus, usageAfterSolar, now, 
                                                               maxImportRate, topUpMaxCost)    
        maxChargeCost             = max(maxChargeCost, newMaxChargeCost)

        soc = self.convertToAppPercentage((self.batteryEnergy / self.batteryCapacity) * 100)
        self.log("Current battery charge {0:.3f}".format(soc))
        self.log("Battery top up cost threshold {0:.3f}".format(topUpMaxCost))
        self.log("Max battery charge cost {0:.2f}".format(maxChargeCost))
        self.printSeries(batProfile, "Battery profile - post topup")
        # calculate the pre-eddi export profile. Remote charging power and surplus outside the period we
        # have export rates for (because we won't have a plan for those periods yet).
        exportProfile = self.opOnSeries(solarSurplus, exportRateData, lambda a, b: a if b else 0)
        exportProfile = self.opOnSeries(exportProfile, solarChargingPlan, lambda a, b: a - b)
        exportProfile = list(filter(lambda x: x[2], exportProfile))
        self.printSeries(exportProfile, "Export profile - pre eddi")
        solarChargingPlan.sort(key=lambda x: x[0])
        gridChargingPlan.sort(key=lambda x: x[0])
        dischargePlan.sort(key=lambda x: x[0])
        houseGridPoweredPlan.sort(key=lambda x: x[0])
        # When calculating the battery profile we allow the "house on grid power" and "grid charging" plans to
        # overlap. However we need to remove this overlap before returning the plan to the caller.
        houseGridPoweredPlan = self.opOnSeries(houseGridPoweredPlan, gridChargingPlan, lambda a, b: 0 if b else a)
        houseGridPoweredPlan = list(filter(lambda x: x[2], houseGridPoweredPlan))
        return (solarChargingPlan, gridChargingPlan, dischargePlan, houseGridPoweredPlan)
    
    

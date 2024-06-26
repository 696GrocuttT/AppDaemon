from datetime   import datetime
from datetime   import timedelta
from datetime   import timezone
from statistics import mean
from core.powerUtils import PowerUtils
import re
import math
import numpy
import matplotlib.pyplot as plt
import json
import os
import pickle
import sys
import copy
import functools



class BatteryAllocateState():
    def __init__(self, exportRateData, importRateData, solarSurplus, usageAfterSolar, core):
        self.utils                    = core.utils
        self.batProfile               = []
        self.solarChargingPlan        = []
        self.gridChargingPlan         = []
        self.houseGridPoweredPlan     = []
        self.dischargeExportSolarPlan = []
        self.dischargeToGridPlan      = []
        self.eddiSolarPlan            = []
        self.eddiGridPlan             = []
        self.gridSummary              = {}
        self.maxChargeCost            = core.maxChargeCost
        self.solarSurplus             = solarSurplus
        self.usageAfterSolar          = usageAfterSolar
        self.exportRateData           = exportRateData
        self.importRateData           = importRateData
        self.availableExportRates     = sorted(exportRateData, key=lambda x: x[2])
        self.availableImportRates     = sorted(importRateData, key=lambda x: (x[2], x[0]))
        # We create a set of effective "charge" rates associated with not discharging the battery. The 
        # idea is that if we choose not to discharge for a period that's the same as charging the battery 
        # with the same amount of power. It's actually better than this because not cycling the battery
        # means we reduce the battery wear, and don't have the battery efficency overhead. 
        self.availableHouseGridPoweredRates = list(self.availableImportRates)
 

    def updateChangeCost(self, cost):  
        self.maxChargeCost = max(self.maxChargeCost, cost)


    def sortPlans(self):
        self.solarChargingPlan.sort(key=lambda x: x[0])
        self.gridChargingPlan.sort(key=lambda x: x[0])
        self.houseGridPoweredPlan.sort(key=lambda x: x[0])
        self.dischargeExportSolarPlan.sort(key=lambda x: x[0])
        self.dischargeToGridPlan.sort(key=lambda x: x[0])
        self.eddiSolarPlan.sort(key=lambda x: x[0])
        self.eddiGridPlan.sort(key=lambda x: x[0])


    def setTo(self, fromState):
        self.batProfile                     = fromState.batProfile
        self.solarChargingPlan              = fromState.solarChargingPlan
        self.gridChargingPlan               = fromState.gridChargingPlan
        self.houseGridPoweredPlan           = fromState.houseGridPoweredPlan
        self.dischargeExportSolarPlan       = fromState.dischargeExportSolarPlan
        self.dischargeToGridPlan            = fromState.dischargeToGridPlan
        self.eddiSolarPlan                  = fromState.eddiSolarPlan
        self.eddiGridPlan                   = fromState.eddiGridPlan
        self.maxChargeCost                  = fromState.maxChargeCost
        self.availableExportRates           = fromState.availableExportRates
        self.availableImportRates           = fromState.availableImportRates
        self.availableHouseGridPoweredRates = fromState.availableHouseGridPoweredRates
        self.solarSurplus                   = fromState.solarSurplus
        self.usageAfterSolar                = fromState.usageAfterSolar
        self.exportRateData                 = fromState.exportRateData
        self.importRateData                 = fromState.importRateData


    def copy(self):
        newState = copy.copy(self)
        newState.batProfile                     = list(newState.batProfile)
        newState.solarChargingPlan              = list(newState.solarChargingPlan)
        newState.gridChargingPlan               = list(newState.gridChargingPlan)
        newState.houseGridPoweredPlan           = list(newState.houseGridPoweredPlan)
        newState.dischargeExportSolarPlan       = list(newState.dischargeExportSolarPlan)
        newState.dischargeToGridPlan            = list(newState.dischargeToGridPlan)
        newState.eddiSolarPlan                  = list(newState.eddiSolarPlan)
        newState.eddiGridPlan                   = list(newState.eddiGridPlan)
        newState.availableExportRates           = list(newState.availableExportRates)
        newState.availableImportRates           = list(newState.availableImportRates)
        newState.availableHouseGridPoweredRates = list(newState.availableHouseGridPoweredRates)
        newState.solarSurplus                   = list(newState.solarSurplus)
        newState.usageAfterSolar                = list(newState.usageAfterSolar)
        newState.exportRateData                 = list(newState.exportRateData)
        newState.importRateData                 = list(newState.importRateData)
        return newState


    def exportProfile(self):
        # Remote charging power and surplus outside the period we have export rates for
        # (because we won't have a plan for those periods yet).
        exportProfile = self.utils.opOnSeries(self.solarSurplus, self.exportRateData,           lambda a, b: a if b else 0)
        exportProfile = self.utils.opOnSeries(exportProfile,     self.solarChargingPlan,        lambda a, b: a - b)
        exportProfile = self.utils.opOnSeries(exportProfile,     self.dischargeExportSolarPlan, lambda a, b: a + b)
        exportProfile = self.utils.opOnSeries(exportProfile,     self.dischargeToGridPlan,      lambda a, b: a + b)
        exportProfile = self.utils.opOnSeries(exportProfile,     self.eddiSolarPlan,            lambda a, b: a - b)
        exportCosts   = self.utils.opOnSeries(exportProfile,     self.exportRateData,           lambda a, b: a * b)
        exportRates   = self.utils.opOnSeries(exportProfile,     self.exportRateData,           lambda a, b: b)
        exportProfile = self.utils.combineSeries(exportProfile,   exportCosts,                   exportRates)
        return list(filter(lambda x: x[2], exportProfile))


    def importProfile(self):
        usageWhenGridChanging = self.utils.opOnSeries(self.gridChargingPlan, self.usageAfterSolar, lambda a, b: b)
        # use the input rate to make sure all time slots are populated. Otherwise we only 
        # end up producing a series when there's a charge plan
        importProfile = self.utils.opOnSeries(self.importRateData, self.gridChargingPlan,     lambda a, b: b if a else 0)
        importProfile = self.utils.opOnSeries(importProfile,       self.houseGridPoweredPlan, lambda a, b: a + b)
        importProfile = self.utils.opOnSeries(importProfile,       usageWhenGridChanging,     lambda a, b: a + b)
        importProfile = self.utils.opOnSeries(importProfile,       self.eddiGridPlan,         lambda a, b: a + b)
        importCosts   = self.utils.opOnSeries(importProfile,       self.importRateData,       lambda a, b: a * b)
        importRates   = self.utils.opOnSeries(importProfile,       self.importRateData,       lambda a, b: b)
        importProfile = self.utils.combineSeries(importProfile,     importCosts,               importRates)
        return list(filter(lambda x: x[2], importProfile))



class PowerControlCore():
    def __init__(self, args, log):
        self.log                      = log
        self.args                     = args
        self.utils                    = PowerUtils(self.log)
        self.maxChargeRate            = float(args['batteryChargeRateLimit'])
        self.maxDischargeRate         = float(args['batteryDischargeRateLimit'])
        self.batteryGridChargeRate    = float(args['batteryGridChargeRate'])
        self.batTargetReservePct      = float(args['batteryTargetReservePercentage'])
        self.batAbsMinReservePct      = float(args['batteryAbsMinReservePercentage'])
        self.batFullPct               = float(args['batteryFullPercentage'])
        self.gasEfficiency            = float(args['gasHotWaterEfficiency'])
        self.eddiTargetPower          = float(args['eddiTargetPower'])
        self.eddiPowerLimit           = float(args['eddiPowerLimit'])
        self.gridExportLimit          = float(args['gridExportLimit']) 
        self.minBuySelMargin          = float(args['minBuySelMargin'])
        self.minBuySelNotFullMargin   = float(args['minBuySelNotFullMargin'])
        self.minBuyUseMargin          = float(args['minBuyUseMargin'])
        self.gasRate                  = 0
        self.batFullPctHysteresis     = 3
        self.batEfficiency            = 0.9
        self.futureTimeWindow         = timedelta(hours=28)
        self.stateSavesPath           = "/conf/stateSaves"
        self.solarData                = []
        self.exportRateData           = []
        self.importRateData           = []
        self.usageData                = []
        self.solarChargingPlan        = []
        self.gridChargingPlan         = []
        self.houseGridPoweredPlan     = []
        self.standbyPlan              = []
        self.dischargeExportSolarPlan = []
        self.dischargeToGridPlan      = []
        self.dischargeToHousePlan     = []
        self.eddiSolarPlan            = []
        self.eddiGridPlan             = []
        self.planUpdateTime           = None


    def save(self, now):
        self.planUpdateTime = now
        # Save the state in case we needed for future debug
        slotMidTime = self.planUpdateTime + timedelta(minutes=15)
        fileName    = "{0}/{1:02.0f}_{2:02.0f}.pickle".format(self.stateSavesPath, slotMidTime.hour, 
                                                              30 * math.floor(float(slotMidTime.minute)/30))
        # We can't serialise the logger, so nul out the loger on self so we can serialise 
        # safely, then restore it afterwards
        log        = self.log
        self.log   = None
        utils      = self.utils
        self.utils = None
        try:
            with open(fileName, 'wb') as handle:
                pickle.dump(self, handle, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as error:
            log("Error saving state: " + str(error))
        self.log   = log
        self.utils = utils


    def load(fileName, log):
        obj = None
        try:
            with open(fileName, 'rb') as handle:
                obj     = pickle.load(handle)
                obj.log = log
        except Exception as error:
            print("Error loading state: " + str(error))
        obj.utils = PowerUtils(obj.log)
        return obj


    def toFloat(self, string, default):
        try:
            value = float(string)
        except ValueError:
            value = default
        return value


    def seriesToTariff(self, series, midnight):
        mergedPlan    = self.utils.mergeSeries(series)
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


    def extendSeries(self, inputSeries, extendBy = timedelta(), extendTo = None):                                      
        outputSeries = list(inputSeries)
        if outputSeries:
            endTime = (extendTo if extendTo else outputSeries[-1][1]) + extendBy
            while outputSeries[-1][1] < endTime:
                # Get the details of the last slot
                periodStartTime = outputSeries[-1][0] 
                periodEndTime   = outputSeries[-1][1] 
                periodDuration  = periodEndTime - periodStartTime
                # compute the details of the next slot
                periodStartTime = periodEndTime 
                periodEndTime   = periodEndTime + periodDuration
                power           = self.utils.powerForPeriod(outputSeries, 
                                                            periodStartTime - timedelta(hours=24), 
                                                            periodEndTime   - timedelta(hours=24)) 
                outputSeries.append((periodStartTime, periodEndTime, power))
        return outputSeries
        
        
    def mergeAndProcessData(self, now):
        self.log("Updating schedule")        
        # Remove rates that are in the past
        exportRateData = self.exportRateData
        importRateData = self.importRateData
        if self.args.get('extendTariff', False):
            # Calculate the end time to extend to, but make sure its midnight.
            endTime        = (exportRateData[-1][1] + self.futureTimeWindow).replace(hour=0, minute=0, second=0, microsecond=0)
            exportRateData = self.extendSeries(exportRateData, timedelta(), endTime)
            importRateData = self.extendSeries(importRateData, timedelta(), endTime)
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
        # apply saving sessions
        importRatesOverridden = False
        exportRatesOverridden = False
        for (listName, sessions) in self.savingSession.items():
            # We assume every session will be joined, so we use the overrides for both available and joined 
            # sessions to give us as much warning as possible. However we ignore any sessions that haven't 
            # been joined if they're about to start, as this could happen if we fail to join one.
            if listName == "available":
                sessions = list(filter(lambda x: now <= x[0] - timedelta(minutes=30) or x[1] <= now, sessions))
            for (start, end, price) in sessions:
                for (index, rate) in enumerate(exportRateData):
                    if start <= rate[0] and rate[1] <= end:
                        exportRatesOverridden = True
                        extendExportPlanTo    = end
                        exportRateData[index] = (rate[0], rate[1], price)
        # Apply any tariff overrides
        extendExportPlanTo = now
        if self.tariffOverrideType == "Export":
            for (index, rate) in enumerate(exportRateData):
                if self.tariffOverrideStart <= rate[0] and rate[1] <= self.tariffOverrideEnd:
                    exportRatesOverridden = True
                    extendExportPlanTo    = self.tariffOverrideEnd
                    exportRateData[index] = (rate[0], rate[1], self.tariffOverridePrice)
        elif self.tariffOverrideType == "Import":
            overridden = False
            for (index, rate) in enumerate(importRateData):
                if self.tariffOverrideStart <= rate[0] and rate[1] <= self.tariffOverrideEnd:
                    importRatesOverridden = True
                    importRateData[index] = (rate[0], rate[1], self.tariffOverridePrice)
        # Print out any overridden rates
        if exportRatesOverridden:
            self.utils.printSeries(exportRateData, "Overridden export rate")
        if importRatesOverridden:
            self.utils.printSeries(importRateData, "Overridden import rate")

        # Calculate the solar surplus after house load, we base this on the usage time 
        # series dates as that's typically a finer granularity than the solar forecast. Similarly 
        # we work out the house usage after any forecast solar. The solar forecast has 3 values in 
        # the following order, a 50th percentile followed by a low and high estimate of the power 
        # for each period. We carry this through to the generated series so we can more accuratly 
        # plan the battery charge / house usage.
        usageData       = self.extendSeries(self.usageData, timedelta(), exportRateData[-1][1])
        usageData       = list(filter(lambda x: x[0] >= exportRateData[0][0] and x[1] <= exportRateData[-1][1], usageData))
        solarSurplus    = self.utils.combineSeries(self.utils.opOnSeries(usageData,    self.solarData, lambda a, b: max(0, b-a)), 
                                                   self.utils.opOnSeries(usageData,    self.solarData, lambda a, b: max(0, b-a), 0, 1), 
                                                   self.utils.opOnSeries(usageData,    self.solarData, lambda a, b: max(0, b-a), 0, 2))
        solarUsage      = self.utils.combineSeries(self.utils.opOnSeries(solarSurplus, self.solarData, lambda a, b: b-a),
                                                   self.utils.opOnSeries(solarSurplus, self.solarData, lambda a, b: b-a, 1, 1),
                                                   self.utils.opOnSeries(solarSurplus, self.solarData, lambda a, b: b-a, 2, 2))
        usageAfterSolar = self.utils.combineSeries(self.utils.opOnSeries(usageData,    self.solarData, lambda a, b: max(0, a-b)),
                                                   self.utils.opOnSeries(usageData,    self.solarData, lambda a, b: max(0, a-b), 0, 1),
                                                   self.utils.opOnSeries(usageData,    self.solarData, lambda a, b: max(0, a-b), 0, 2))
        
        # calculate the charge plan, and work out what's left afterwards
        batPlans                 = self.calculateChargePlan(exportRateData, importRateData, solarUsage, solarSurplus, usageAfterSolar, now, extendExportPlanTo)
        self.utils.printSeries(batPlans.exportProfile(), "Export profile - pre eddi")
        postBatteryChargeSurplus = self.utils.opOnSeries(solarSurplus, batPlans.solarChargingPlan, lambda a, b: a-b)
        # Calculate the times when we want the battery in standby mode. IE when there's solar surplus 
        # but we don't want to charge or discharge.
        standbyPlan = []
        for rate in exportRateData:
            curSolarSurplus =  self.utils.powerForPeriod(solarSurplus,                      rate[0], rate[1])
            isPlanned       = (self.utils.powerForPeriod(batPlans.solarChargingPlan,        rate[0], rate[1]) > 0 or
                               self.utils.powerForPeriod(batPlans.gridChargingPlan,         rate[0], rate[1]) > 0 or
                               self.utils.powerForPeriod(batPlans.houseGridPoweredPlan,     rate[0], rate[1]) > 0 or
                               self.utils.powerForPeriod(batPlans.dischargeExportSolarPlan, rate[0], rate[1]) > 0 or
                               self.utils.powerForPeriod(batPlans.dischargeToGridPlan,      rate[0], rate[1]) > 0)
            if (curSolarSurplus > 0) and not isPlanned: 
                standbyPlan.append((rate[0], rate[1], curSolarSurplus))
        # Create a background plan for info only that shows when we're just powering the house from the battery.
        usageForRateSlotsOnly = self.utils.opOnSeries(exportRateData,        self.usageData,                    lambda a, b: b)
        dischargeToHousePlan  = self.utils.opOnSeries(usageForRateSlotsOnly, batPlans.solarChargingPlan,        lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.utils.opOnSeries(dischargeToHousePlan,  batPlans.gridChargingPlan,         lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.utils.opOnSeries(dischargeToHousePlan,  batPlans.houseGridPoweredPlan,     lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.utils.opOnSeries(dischargeToHousePlan,  standbyPlan,                       lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.utils.opOnSeries(dischargeToHousePlan,  batPlans.dischargeExportSolarPlan, lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.utils.opOnSeries(dischargeToHousePlan,  batPlans.dischargeToGridPlan,      lambda a, b: 0 if b else a)
        dischargeToHousePlan  = list(filter(lambda x: x[2], dischargeToHousePlan))

        # Calculate the eddi plan based on any remaining surplus
        self.calculateEddiPlan(exportRateData, importRateData, postBatteryChargeSurplus, batPlans, now)
        exportProfile  = batPlans.exportProfile()
        importProfile  = batPlans.importProfile()
        profileReducer = lambda a, b: (None, None, a[2]+b[2], a[3]+b[3], None)
        initialVal     = (None, None, 0, 0, None)
        exportSummary  = functools.reduce(profileReducer, exportProfile, initialVal)
        importSummary  = functools.reduce(profileReducer, importProfile, initialVal)
        netSummary     = (None, None, importSummary[2]-exportSummary[2], importSummary[3]-exportSummary[3], None)
        def summaryFormatter(typeStr, data):
            rate        = 100*data[3]/data[2] if data[2] != 0 else 0
            summaryStr  = "{3} summary: {0:.2f} kWh @ £{1:.2f} = {2:.2f}p/kWh".format(data[2], data[3], rate, typeStr)
            summaryDict = { "energy": data[2], 
                            "cost":   data[3], 
                            "rate":   rate } 
            return (summaryStr, summaryDict)
        
        (exportStr, exportDict) = summaryFormatter("Export", exportSummary)
        (importStr, importDict) = summaryFormatter("Import", importSummary)
        (netStr,    netDict)    = summaryFormatter("Net",    netSummary)
        self.utils.printSeries(exportProfile, "Export profile - post eddi")
        self.log(exportStr)
        self.utils.printSeries(importProfile, "Import profile - post eddi")
        self.log(importStr)
        self.log(netStr)
        self.gridSummary       = {"import": importDict,
                                  "export": exportDict,
                                  "net":    netDict}
        profileIsoConvert      = lambda a: {"start":  a[0].isoformat(), 
                                            "end":    a[1].isoformat(), 
                                            "energy": a[2], 
                                            "cost":   a[3],
                                            "rate":   a[4]}
        self.exportProfileISO  = list(map(profileIsoConvert, exportProfile))
        self.importProfileISO  = list(map(profileIsoConvert, importProfile))
        rateIsoConvert         = lambda a: {"start":  a[0].isoformat(), 
                                            "end":    a[1].isoformat(), 
                                            "rate":   a[2]}
        self.exportRateDataISO = list(map(rateIsoConvert, exportRateData))
        self.importRateDataISO = list(map(rateIsoConvert, importRateData))
        
        # Create a fake tariff with peak time covering the discharge plan
        # Normally we wouldn't have the solarChargePlan as one of the peak periods. There is some deep 
        # twisted logic to this. Firstly it doesn't actually matter as we set the powerwall to Self-powered 
        # when we want to charge from solar, which doesn't use the tariff plan. The powerwall sometimes
        # takes awhile to respond to tariff updates. This means that if the plan changes from change to 
        # standby then we don't want this to impact the tariff plan we need (which could take awhile to
        # update). To get round this we pre-emptivly set charging periods to peak in the tariff plan in 
        # case we need to swap. We also extend the peak period into the past a bit. This prevents any
        # strange behaviour given we have to have to change the battery settings just before the start of 
        # each hour.
        midnight      = now.replace(hour=0, minute=0, second=0, microsecond=0)
        hourStart     = (now + timedelta(minutes=15)).replace(minute=0, second=0, microsecond=0)
        peakPlan      = [(hourStart - timedelta(minutes=15), hourStart + timedelta(hours=3), 0)]
        peakPeriods   = self.seriesToTariff(peakPlan, midnight)
        self.defPrice = "0.10 0.10 OFF_PEAK"
        self.pwTariff = {"0.90 0.90 ON_PEAK": peakPeriods}
        self.utils.printSeries(batPlans.solarChargingPlan,        "Solar charging plan",         mergeable=True)
        self.utils.printSeries(batPlans.gridChargingPlan,         "Grid charging plan",          mergeable=True)
        self.utils.printSeries(batPlans.houseGridPoweredPlan,     "House grid powered plan",     mergeable=True)
        self.utils.printSeries(standbyPlan,                       "Standby plan",                mergeable=True)
        self.utils.printSeries(batPlans.dischargeExportSolarPlan, "Discharge export solar plan", mergeable=True)
        self.utils.printSeries(batPlans.dischargeToGridPlan,      "Discharge to grid plan",      mergeable=True)
        self.utils.printSeries(dischargeToHousePlan,              "Discharging to house plan",   mergeable=True)
        self.utils.printSeries(batPlans.eddiSolarPlan,            "Eddi solar plan",             mergeable=True)
        self.utils.printSeries(batPlans.eddiGridPlan,             "Eddi grid plan",              mergeable=True)
        self.solarChargingPlan        = batPlans.solarChargingPlan
        self.gridChargingPlan         = batPlans.gridChargingPlan
        self.houseGridPoweredPlan     = batPlans.houseGridPoweredPlan
        self.standbyPlan              = standbyPlan
        self.dischargeExportSolarPlan = batPlans.dischargeExportSolarPlan
        self.dischargeToGridPlan      = batPlans.dischargeToGridPlan
        self.dischargeToHousePlan     = dischargeToHousePlan
        self.maxChargeCost            = batPlans.maxChargeCost
        self.eddiSolarPlan            = batPlans.eddiSolarPlan
        self.eddiGridPlan             = batPlans.eddiGridPlan
        self.planUpdateTime           = now


    def calculateEddiPlan(self, exportRateData, importRateData, solarSurplus, batPlans, now):
        # Calculate the target rate for the eddi
        eddiSolarPlan  = []
        eddiGridPlan   = []
        eddiTargetRate = self.gasRate / self.gasEfficiency
        
        # Calculate the start time for the eddi plan. This has to be in the past so we calculate 
        # how much energy we've already sent to the eddi
        eddiDayStart = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if eddiDayStart >= now:
            eddiDayStart = eddiDayStart - timedelta(days=1)
        eddiEnergyForSlot = self.utils.powerForPeriod(self.eddiData, eddiDayStart, now)
        # Now create a plan
        slotStartTime       = eddiDayStart
        slotEndTime         = eddiDayStart + timedelta(days=1)
        planEndTime         = exportRateData[-1][1]
        eddiPowerReqForSlot = []
        while slotStartTime < planEndTime:
            eddiPowerReqForSlot.append((slotStartTime, slotEndTime, self.eddiTargetPower - eddiEnergyForSlot))
            # Rotate the vars for the next slot, inc zeroing the energy as what the eddi has 
            # done so far only applies to the first slot in the plan.
            slotStartTime     = slotEndTime
            slotEndTime       = slotEndTime + timedelta(days=1)
            eddiEnergyForSlot = 0
        
        # For any slots where we're planning to run off the grid we also have the opertunity to 
        # eddi off the grid without draining the battery. Calculate the available slots that 
        # could be used.
        gridUseRates      =                self.utils.opOnSeries(batPlans.houseGridPoweredPlan, importRateData, lambda a, b: b)
        gridUseRates      = gridUseRates + self.utils.opOnSeries(batPlans.gridChargingPlan,     importRateData, lambda a, b: b)
        gridUseRates      = list(map(lambda a: (a[0], a[1], a[2], False), gridUseRates))
        # combine with the export rates for solar and sort based on price        
        solarSurplus      = list(filter(lambda x: x[2], solarSurplus))
        solarSurplusRates = self.utils.opOnSeries(solarSurplus, exportRateData, lambda a, b: b)
        solarSurplusRates = list(map(lambda a: (a[0], a[1], a[2], True), solarSurplusRates))
        ratesCheapFirst   = sorted(gridUseRates + solarSurplusRates, key=lambda x: x[2])
        # Create the eddi plan by looking for rates that are below the threshold where gas 
        # becomes a better option
        for rate in ratesCheapFirst:
            if rate[2] > eddiTargetRate:
                break
            # find the eddi slot that we're trying to fill for the rate time period
            foundSlot = list(filter(lambda slot: slot[1][0] <= rate[0] and rate[1] <= slot[1][1], enumerate(eddiPowerReqForSlot)))
            if not foundSlot:
                continue 
            powerReqSlotIdx  = foundSlot[0][0]
            powerReqSlotInfo = foundSlot[0][1]
            # Calculate the amount of power available
            maxPower = ((rate[1] - rate[0]).total_seconds() / (60 * 60)) * self.eddiPowerLimit
            # is this a solar or grid slot
            if rate[3]:
                power      = self.utils.powerForPeriod(solarSurplus, rate[0], rate[1])
                powerTaken = max(min(power, maxPower), 0)
                # We still plan to use the eddi even if the forcast says there won't be a 
                # surplus. This is in case the forcast is wrong, or there are dips in usage 
                # or peaks in generation that lead to short term surpluses
                eddiSolarPlan.append((rate[0], rate[1], powerTaken))
            else:
                # Since this is a grid slot we can pull as much power as we want
                powerTaken = maxPower
                eddiGridPlan.append((rate[0], rate[1], powerTaken))
            eddiPowerRequired = powerReqSlotInfo[2] - powerTaken
            if eddiPowerRequired <= 0:
                del eddiPowerReqForSlot[powerReqSlotIdx]
            else:
                eddiPowerReqForSlot[powerReqSlotIdx] = (powerReqSlotInfo[0], powerReqSlotInfo[1], eddiPowerRequired)
        # Add on any slots where the battery is charging and the rate is below the threshold. 
        # This means we divert any surplus that wasn't forecast that the battery could change 
        # from. EG if the battery fills up early, or we exceed the battery charge rate.
        for chargePeriod in batPlans.solarChargingPlan:
            # If the entry is already in the eddi plan, don't try and add it again
            if not (any(x[0] == chargePeriod[0] for x in eddiSolarPlan) or 
                    any(x[0] == chargePeriod[0] for x in eddiGridPlan)):
                exportRate = next(filter(lambda x: x[0] == chargePeriod[0], exportRateData))
                if exportRate[2] <= eddiTargetRate:
                    eddiSolarPlan.append((chargePeriod[0], chargePeriod[1], 0))
        eddiSolarPlan.sort(key=lambda x: x[0])
        eddiGridPlan.sort(key=lambda  x: x[0])
        batPlans.eddiSolarPlan = eddiSolarPlan
        batPlans.eddiGridPlan  = eddiGridPlan
 
 
    def convertToAppPercentage(self, value):
        # The battery reserves 5% so the battery is never completely empty. This is fudged in 
        # the app as it shows an adjusted percentage scale. This formula replicates that so we
        # can directyl compare percentages
        return (value - 5) / 0.95


    def convertToRealPercentage(self, value):
        # Calculates the inverse of the convertToAppPercentage function
        return (value * 0.95) + 5


    def genBatLevelForecast(self, state, now, percentileIndex):
        state.batProfile = []
        # For full charge detection we compare against 99% full, this is so any minor changes 
        # is battery capacity or energe when we're basically fully charged, and won't charge 
        # any more, don't cause any problems.
        batFullPct            = min(self.batFullPct, 99)
        batTargetResEnergy    = self.batteryCapacity * (self.convertToRealPercentage(self.batTargetReservePct) / 100)
        batAbsMinResEnergy    = self.batteryCapacity * (self.convertToRealPercentage(self.batAbsMinReservePct) / 100)
        batteryRemaining      = self.batteryEnergy
        emptyInAnySlot        = False
        totallyEmptyInAnySlot = False
        fullInAnySlot         = False
        totChargeEnergy       = 0.0
        # The rate data is just used as a basis for the timeline
        for (index, rate) in enumerate(state.exportRateData):
            chargeEnergy     = (self.utils.powerForPeriod(state.solarChargingPlan,        rate[0], rate[1], percentileIndex) +
                                self.utils.powerForPeriod(state.gridChargingPlan,         rate[0], rate[1]))
            batteryRemaining = (batteryRemaining + chargeEnergy - 
                                self.utils.powerForPeriod(state.usageAfterSolar,          rate[0], rate[1], percentileIndex) -
                                self.utils.powerForPeriod(state.dischargeExportSolarPlan, rate[0], rate[1]) -
                                self.utils.powerForPeriod(state.dischargeToGridPlan,      rate[0], rate[1]) +
                                self.utils.powerForPeriod(state.houseGridPoweredPlan,     rate[0], rate[1], percentileIndex))
            totChargeEnergy  = totChargeEnergy + chargeEnergy
            fullyChanged     = batteryRemaining >= self.batteryCapacity
            empty            = batteryRemaining <= batTargetResEnergy
            totallyEmpty     = batteryRemaining <= batAbsMinResEnergy
            if fullyChanged:
                fullInAnySlot    = True
                batteryRemaining = self.batteryCapacity
            if empty:
                emptyInAnySlot   = True
            if totallyEmpty:      
                totallyEmptyInAnySlot = True
                batteryRemaining      = batAbsMinResEnergy
            pct = round(self.convertToAppPercentage((batteryRemaining / self.batteryCapacity) * 100), 1)
            state.batProfile.append((rate[0], rate[1], batteryRemaining, fullyChanged, empty, pct))
           
        # calculate the end time of the last fully charged and empty slots
        lastFullSlotEndTime = None
        if fullInAnySlot:
            lastFullSlotEndTime = next(filter(lambda x: x[3], reversed(state.batProfile)))[1]
        lastEmptySlotEndTime = None
        if emptyInAnySlot:
            lastEmptySlotEndTime = next(filter(lambda x: x[4], reversed(state.batProfile)))[1]
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
        hysteresis                = self.batFullPctHysteresis if totChargeEnergy else -self.batFullPctHysteresis
        batFullEnergy             = self.batteryCapacity * ((batFullPct + hysteresis) / 100)
        lastTargetFullTime        = state.batProfile[-1][0].replace(hour=22, minute=30, second=0, microsecond=0)
        fullChargeAfterTargetTime = any(x[0] >= lastTargetFullTime and x[2] >= batFullEnergy for x in state.batProfile)
        # We also indicate the battery is fully charged if its after the target time now, and its 
        # currently fully charged. This prevents an issue where the current time slot is never  
        # allowed to discharge if we don't have a charging period for tomorrow mapped out already
        if not fullChargeAfterTargetTime:
            if self.batteryEnergy > batFullEnergy and now >= lastTargetFullTime:
                fullChargeAfterTargetTime = True
        return (lastTargetFullTime, fullChargeAfterTargetTime, lastFullSlotEndTime, emptyInAnySlot, totallyEmptyInAnySlot, lastEmptySlotEndTime)


    def chooseRate(self, rateA, rateB, notAfterTime=None):
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


    def chooseRate3(self, rateA, rateB, rateC, notAfterTime=None):
        (foundRate, isRateA)  = self.chooseRate(rateA,     rateB, notAfterTime)
        foundRate             = [foundRate] if foundRate else []
        (foundRate, isRateAB) = self.chooseRate(foundRate, rateC, notAfterTime)
        rateId = (2 if not isRateAB else
                  0 if     isRateA  else 1)
        return (foundRate, rateId)


    def allocateChangingSlots(self, state, now, maxImportRate, topUpToChargeCost = None):
        # We create a local copy of the available rates as there some cases (if there's no solar
        # surplus) where we don't want to remove an entry from the availableExportRates array, 
        # but we need to remove it locally so we can keep track of which items we've used, and 
        # which are still available
        availableImportRatesLocal           = list(state.availableImportRates)
        availableImportRatesLocalUnused     = list(state.availableImportRates)
        availableHouseGridPoweredRatesLocal = list(state.availableHouseGridPoweredRates)
        # The percentile index is used to select the 50th percentile (index 0) or the low (index 1)
        # or high (index 2) estimates. Which one we choose changes based on whether we're trying to 
        # make sure the battery doesn't go flat, or whether we're topping it up and don't want to 
        # over charge it and end up with a surplus that just goes to the grid. Unless we're explicitly 
        # being asked to add a topup, we start off with the low estimate as the first passes are to 
        # ensure the battery doesn't go flat, with later passes topping it up.
        percentileIndex                     = 2 if topUpToChargeCost else 1
        # Compute a list of times where's a rate, and we have a +ve solar surplus for the selected
        # percentile. First filter out the slots we don't have a rate for. Then filter out any zero 
        # surplus slots
        nonZeroSolarSurplus                 = self.utils.opOnSeries(state.availableExportRates, state.solarSurplus, lambda a, b: b, 0, percentileIndex)
        nonZeroSolarSurplus                 = list(filter(lambda x: x[2], nonZeroSolarSurplus))
        # Now create a local list of charge rates, but only for the slots where there's a non-zero surplus.        
        availableExportRatesLocal           = self.utils.opOnSeries(nonZeroSolarSurplus, state.availableExportRates, lambda a, b: b)        
        # We don't want to discharge the battery for any slots where the cost of running the house off 
        # the grid is lower than what we've previously paid to charge the battery. So add any grid 
        # powered rates that are below the current charge cost
        def addBelowChargeCostHouseGridPoweredSlots():
            for rate in list(filter(lambda x: x[2] < state.maxChargeCost, availableHouseGridPoweredRatesLocal)):
                usage     = self.utils.powerForPeriod(state.usageAfterSolar, rate[0], rate[1])
                usageLow  = self.utils.powerForPeriod(state.usageAfterSolar, rate[0], rate[1], 1)
                usageHigh = self.utils.powerForPeriod(state.usageAfterSolar, rate[0], rate[1], 2)
                # we can only use a charging slot once, so remove it from the available list
                availableHouseGridPoweredRatesLocal.remove(rate)
                state.availableHouseGridPoweredRates.remove(rate)
                state.houseGridPoweredPlan.append((rate[0], rate[1], usage, usageLow, usageHigh))
        addBelowChargeCostHouseGridPoweredSlots()
        # Keep producing a battery forecast and adding the cheapest charging slots until the battery is full
        (fullEndTimeThresh,   fullyCharged, 
         lastFullSlotEndTime, empty, 
         totallyEmptyInAnySlot, 
         lastEmptySlotEndTime)              = self.genBatLevelForecast(state, now, percentileIndex)
        # initialise the allow empty before variable to the start of the profile so it has no effect to start with
        allowEmptyBefore                    = state.batProfile[0][0]
        maxAllowedChargeCost                = topUpToChargeCost if topUpToChargeCost else math.inf
        # Define helper function to check if charging is required, this is so we can be sure to apply
        # the same formula in multiple place
        def chargeRequired(empty, topUpToChargeCost, fullyCharged):
            return empty or topUpToChargeCost or not fullyCharged
                                        
        # Keep searching for a slot while there's a need for it, using the common healper function 
        # defined above 
        while chargeRequired(empty, topUpToChargeCost, fullyCharged):
            addBelowChargeCostHouseGridPoweredSlots()
            # If the battery has gone flat during at any point, make sure the charging slot we search
            # for is before the point it went flat
            chargeBefore   = None
            firstEmptySlot = None
            if empty:
                percentileIndex = 1
                firstEmptySlot  = next(filter(lambda x: x[4] and x[0] >= allowEmptyBefore, state.batProfile), None)
                if firstEmptySlot:
                    firstEmptySlot = firstEmptySlot[1]
                    chargeBefore   = firstEmptySlot
            else:
                percentileIndex = 2
                # If the only reason we're looking for slots is to hit the battery full criteria then 
                # don't add slots after the full theshold end time, as they won't actually help meet
                # the full battery criteria.
                if not chargeRequired(empty, topUpToChargeCost, True):
                    chargeBefore = fullEndTimeThresh
            # Search for a charging slot
            (chargeRate, rateId) = self.chooseRate3(availableExportRatesLocal, availableImportRatesLocal, availableHouseGridPoweredRatesLocal, chargeBefore)                
            if chargeRate:
                timeInSlot = (chargeRate[1] - chargeRate[0]).total_seconds() / (60 * 60)
                # The charge cost is the cost to get x amount of energy in the battery, due to the overheads
                # this is higher than the cost of the rate used to charge the battery. We don't apply the 
                # efficency factor when using rate type 2, as this is powering the house directly off the
                # grid, so the "chargeCost" is just the rate, and doesn't take into account the battery efficency.
                chargeCost = chargeRate[2] if rateId == 2 else (chargeRate[2] / self.batEfficiency)
                # Pre calculate if the charge rate is below the max import rate. For this comparison we
                # use the raw charge cost and don't take account of the battery efficency, is this gives
                # us an apples to apples comparison with the import rates.
                belowMaxImportRate = chargeRate[2] < maxImportRate
                # Calculate the space left in the battery for this slot, We can't charge more than this
                maxChargeEnergy = self.batteryCapacity - self.utils.powerForPeriod(state.batProfile, chargeRate[0], chargeRate[1])
                # Only allow charging if there's room in the battery for this slot, and its below the max
                # charge cost allowed
                willCharge = (chargeCost <= maxAllowedChargeCost) and not next(filter(lambda x: x[0] == chargeRate[0], state.batProfile))[3]
                # Don't add any charging slots that are before the last fully charged slot, as it won't help
                # get the battery to fully change at our target time, and it just fills the battery with more 
                # expensive electricity when there's cheaper electriticy available later.
                if lastFullSlotEndTime:
                    willCharge = willCharge and chargeRate[1] >= lastFullSlotEndTime
                # Similarly, its only worth charging in a slot if the reason for charging isn't just that 
                # we're empty, or if the charge slot is before the point we go empty
                if lastEmptySlotEndTime:
                    willCharge = willCharge and (chargeRate[1] <= lastEmptySlotEndTime or chargeRequired(False, topUpToChargeCost, fullyCharged))                
                # We also don't want to run the house of the grid if the slot we go empty on is the same cost 
                # as the slot we're evaluating. Instead we just let the battery go flat in this case as we 
                # might not actually end up using that much power to flatten it if the usage forecast is 
                # pesermistic. When it reaches the absolute empty threshold we don't do this any more and must
                # force at least running the house from the grid. If we don't have the totallyEmptyInAnySlot 
                # term in the conditition then the algorithm can treat the battery as an infinite store of
                # energy and keep running it off an empty battery forever. This in itself isn't a problem as 
                # the house will naturally pull from the grid when the battery gives up, but it has knockon
                # effects on the charge cost etc that breaks other aspects of the system.                    
                if firstEmptySlot and willCharge and not totallyEmptyInAnySlot:
                    # Calculate the minimum change cost available taking into account the fact that some list 
                    # of rates may be empty
                    minAvailableRate = math.inf
                    if availableExportRatesLocal:
                        minAvailableRate = min(minAvailableRate, min(map(lambda x: x[2], availableExportRatesLocal)))
                    if availableImportRatesLocal:
                        minAvailableRate = min(minAvailableRate, min(map(lambda x: x[2], availableImportRatesLocal)))
                    if availableHouseGridPoweredRatesLocal:
                        minAvailableRate = min(minAvailableRate, min(map(lambda x: x[2], availableHouseGridPoweredRatesLocal)))
                    emptySlotCost    = next(filter(lambda x: x[1] == firstEmptySlot, self.originalImportRateData), None)[2]
                    # We use the raw charge rate instead of chargeCost here because to do a like for like 
                    # comparison we don't want to take into account the battery efficency when comparing the
                    # rates (as it's not factored into minAvailableRate).
                    willCharge       = willCharge and ((chargeRate[2] < emptySlotCost) or (chargeRate[2] == minAvailableRate))
                    
                def gridUsageAllowed():
                    # Don't run the house on grid power if the slot is the max grid powered price, we might as
                    # well just let the battery go flat, and in some cases due to the margins we wouldn't actually
                    # end up using that much grid power as we'd pre-planned it.
                    return belowMaxImportRate
                
                if rateId == 0: # solar
                    maxCharge = min(timeInSlot * self.maxChargeRate, maxChargeEnergy)
                    powerMed  = self.utils.powerForPeriod(state.solarSurplus, chargeRate[0], chargeRate[1])
                    powerLow  = self.utils.powerForPeriod(state.solarSurplus, chargeRate[0], chargeRate[1], 1)
                    powerHigh = self.utils.powerForPeriod(state.solarSurplus, chargeRate[0], chargeRate[1], 2)
                    power     = (powerMed, powerLow, powerHigh)[percentileIndex]
                    # we can only add something to the charge plan if there's surplus solar
                    willCharge = willCharge and power > 0
                    if willCharge:
                        state.solarChargingPlan.append((chargeRate[0], chargeRate[1], min(powerMed,  maxCharge), 
                                                                                      min(powerLow,  maxCharge),
                                                                                      min(powerHigh, maxCharge)))
                        # we can only use a charging slot once, so remove it from the available list            
                        state.availableExportRates.remove(chargeRate)
                    # We always remove the rate from the local array, otherwise we could end up trying 
                    # to add the same zero power rate again and again. We don't want to remove these rates
                    # from the availableExportRates as we want these slots to be available outside this 
                    # function for other types of activity
                    availableExportRatesLocal.remove(chargeRate)
                elif rateId == 1: # grid charge
                    willCharge = willCharge and gridUsageAllowed()
                    # We don't want to end up charging the battery when its cheaper to just run the house 
                    # directly from the grid. So if the battery is going to be empty (and that's the only 
                    # reason we're looking for a charge slot), check what the electricity import rate is for
                    # the slot where it goes empty and compare that to the cheapest charge rate we've found
                    # to determine if we should use this charge rate or not.
                    if firstEmptySlot and not chargeRequired(False, topUpToChargeCost, fullyCharged):
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
                        slotUsed   = not willCharge or  cheapEnough
                        willCharge =     willCharge and cheapEnough
                    else:
                        slotUsed = True
                    # If the charge slot is still valid, add it to the plan now
                    solarCharge = self.utils.powerForPeriod(state.solarChargingPlan, chargeRate[0], chargeRate[1])
                    chargeTaken = min((timeInSlot * self.batteryGridChargeRate) - solarCharge, maxChargeEnergy)
                    if willCharge and chargeTaken > 0:
                        # we can only use a charging slot once, so remove it from the available list
                        state.availableImportRates.remove(chargeRate)
                        state.gridChargingPlan.append((chargeRate[0], chargeRate[1], chargeTaken))
                    # Same reason as above, always remove the local charge rate
                    availableImportRatesLocal.remove(chargeRate)
                    # See detaied explanation where slotUsed is set above
                    if slotUsed:
                        availableImportRatesLocalUnused.remove(chargeRate)
                elif rateId == 2: # house on grid power
                    willCharge = willCharge and gridUsageAllowed()
                    if willCharge:
                        usage     = self.utils.powerForPeriod(state.usageAfterSolar, chargeRate[0], chargeRate[1])
                        usageLow  = self.utils.powerForPeriod(state.usageAfterSolar, chargeRate[0], chargeRate[1], 1)
                        usageHigh = self.utils.powerForPeriod(state.usageAfterSolar, chargeRate[0], chargeRate[1], 2)
                        # we can only use a charging slot once, so remove it from the available list
                        state.availableHouseGridPoweredRates.remove(chargeRate)
                        state.houseGridPoweredPlan.append((chargeRate[0], chargeRate[1], usage, usageLow, usageHigh))
                    # Same reason as above, always remove the local charge rate
                    availableHouseGridPoweredRatesLocal.remove(chargeRate)
                    
                if willCharge:
                    state.updateChangeCost(chargeCost)
                    addBelowChargeCostHouseGridPoweredSlots()
                    # update the battery profile based on the new charging plan
                    (_, fullyCharged, lastFullSlotEndTime, empty, 
                     totallyEmptyInAnySlot, lastEmptySlotEndTime) = self.genBatLevelForecast(state, now, percentileIndex)   
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

        return (fullyCharged, empty)

    
    def houseRateForPeriod(self, startTime, endTime, exportRateData, importRateData, solarSurplus):
        surplus = self.utils.powerForPeriod(solarSurplus, startTime, endTime)
        if surplus > 0:
            rate = next(filter(lambda x: x[0] == startTime, exportRateData), None)
        else:
            rate = next(filter(lambda x: x[0] == startTime, importRateData), None)
        return rate


    def maxHouseRateForEmpty(self, state):
        maxRate = None
        for batEntry in filter(lambda x: x[4], state.batProfile):
            curRate = self.houseRateForPeriod(batEntry[0], batEntry[1], state.exportRateData, state.importRateData, state.solarSurplus)
            if maxRate == None:
                maxRate = curRate[2]
            else:
                maxRate = max(maxRate, curRate[2])
        return maxRate

    
    def calculateChargePlan(self, exportRateData, importRateData, solarUsage, solarSurplus, usageAfterSolar, now, extendExportPlanTo):
        minImportChargeRate = min(map(lambda x: x[2], self.originalImportRateData)) / self.batEfficiency
        maxImportRate       = max(map(lambda x: x[2], self.originalImportRateData))
        # calculate the initial charging profile
        batAllocateState    = BatteryAllocateState(exportRateData, importRateData, solarSurplus, usageAfterSolar, self)
        self.allocateChangingSlots(batAllocateState, now, maxImportRate)

        # Now we have a change plan, see if we can swap some of the slots to discharge to the grid to improve the
        # income
        def dischargeGridExportSlotTest(state, slot):
            newState               = None
            timeInSlot             = (slot[1] - slot[0]).total_seconds() / (60 * 60)
            maxExportForSlot       = timeInSlot * self.gridExportLimit
            maxDischargeForSlot    = timeInSlot * self.maxDischargeRate
            solarSurplusForSlot    = self.utils.powerForPeriod(state.solarSurplus,    slot[0], slot[1])
            usageAfterSolarForSlot = self.utils.powerForPeriod(state.usageAfterSolar, slot[0], slot[1])
            dischargeForSlot       = min(maxDischargeForSlot - usageAfterSolarForSlot, max(0, maxExportForSlot - solarSurplusForSlot))
            if dischargeForSlot > 0:
                newState = state.copy()
                newState.dischargeToGridPlan.append((slot[0], slot[1], dischargeForSlot))
            return newState   
            
        self.addDischargeSlots(batAllocateState, now, maxImportRate, dischargeGridExportSlotTest, extendExportPlanTo)

        # Now we have a change plan, see if we can swap some of the slots to discharge to cover the house usage to
        # improve the income
        def dischargeExportSolarSlotTest(state, slot):
            newState          = None
            solarUsageForSlot = self.utils.powerForPeriod(solarUsage, slot[0], slot[1])
            if solarUsageForSlot > 0:
                newState = state.copy()
                newState.dischargeExportSolarPlan.append((slot[0], slot[1], solarUsageForSlot))
            return newState   
            
        self.addDischargeSlots(batAllocateState, now, maxImportRate, dischargeExportSolarSlotTest, extendExportPlanTo)
   
        self.utils.printSeries(batAllocateState.batProfile, "Battery profile - pre topup")
        # Now allocate any final charge slots topping up the battery as much as possible, but not exceeding
        # the max charge cost. This means we won't end up increasing the overall charge cost per/kwh. In
        # addition, this means that we'll top up to 100% overright if that's the cheaper option, or if the 
        # solar is a lower cost we'll end up topping up to 100% during the day. This in turn means we're 
        # more likely to be prepared for the next day. EG if we need a higher charge level at the end of
        # the day if we need to make it all the way to the next days solar charge period, or a lower charge 
        # level at the end of the day because we only need to make it to the overright charge period max 
        # charge cost we've already established. 
        topUpMaxCost = batAllocateState.maxChargeCost * float(self.args.get('topUpCostTolerance', 1))
        self.allocateChangingSlots(batAllocateState, now, maxImportRate, batAllocateState.maxChargeCost)    

        self.log("Battery top up cost threshold {0:.3f}".format(topUpMaxCost))
        self.log("Max battery charge cost {0:.2f}".format(batAllocateState.maxChargeCost))
        self.utils.printSeries(batAllocateState.batProfile, "Battery profile - post topup")
        # When calculating the battery profile we allow the "house on grid power" and "grid charging" plans to
        # overlap. However we need to remove this overlap before returning the plan to the caller.
        batAllocateState.houseGridPoweredPlan = self.utils.opOnSeries(batAllocateState.houseGridPoweredPlan, batAllocateState.gridChargingPlan, lambda a, b: 0 if b else a)
        batAllocateState.houseGridPoweredPlan = list(filter(lambda x: x[2], batAllocateState.houseGridPoweredPlan))
        batAllocateState.sortPlans()
        return batAllocateState


    def addDischargeSlots(self, batAllocateState, now, maxImportRate, slotTest, extendExportPlanTo):
        # Limit the length of time into the future that we calculate the discharge slots
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        endTime  = midnight + timedelta(hours=24)
        if now.hour > 20:
            endTime = endTime + timedelta(hours=24)
        # Make sure we plan upto at least the end of the export override end time
        if endTime < extendExportPlanTo:
            endTime = extendExportPlanTo
        # look at the most expensive rate and see if there's solar usage we can flip to battery usage so
        # we can export more. We only do this if we still end up fully charged. We can't use the 
        # availableExportRates list directly, as we need to remove entries as we go, and we still need 
        # to have a list of available charge slots after this step. We sort the list to favour the most 
        # profitable slots, then the earliest day, then the latest slot on that day (which is likely to 
        # be when there's the least solar, so we consider the largest power slots first).
        potentialDischargeRates = sorted(filter(lambda x: x[0] < endTime, batAllocateState.availableExportRates), 
                                         key=lambda x: ( x[2], 
                                                         -x[0].replace(hour=0, minute=0, second=0, microsecond=0).timestamp(), 
                                                         x[0].replace(year=2000, month=1, day=1).timestamp() ))
        # We also need to filter out any slots that we're importing / charging from potential discharge 
        # opertinuties
        def filterOutChangeSlotsFromPotentialDischargeSlots(potentialDischargeRates, batState):
            potentialDischargeRates = self.utils.opOnSeries(potentialDischargeRates, batState.solarChargingPlan,    lambda a, b: 0 if b else a)
            potentialDischargeRates = self.utils.opOnSeries(potentialDischargeRates, batState.houseGridPoweredPlan, lambda a, b: 0 if b else a)
            potentialDischargeRates = self.utils.opOnSeries(potentialDischargeRates, batState.gridChargingPlan,     lambda a, b: 0 if b else a)
            return potentialDischargeRates
        potentialDischargeRates = filterOutChangeSlotsFromPotentialDischargeSlots(potentialDischargeRates, batAllocateState)
        
        while potentialDischargeRates:
            mostExpenciveRate = potentialDischargeRates[-1]
            del potentialDischargeRates[-1]
            # Do a quick test between the previous max change cost and the export rate we're testing to see 
            # if it's we exceed the minimum dischange margin. This isn't the full store as dischanging in a
            # slot may mean we need extra (more expensive) charge slots, but it gives us a early test to 
            # reduce CPU overheads that won't give us false negatives.
            if mostExpenciveRate[2] - batAllocateState.maxChargeCost <= self.minBuySelMargin:
                continue
            
            # Check if discharging in this slot is plausable. The slot test function must return a new 
            # state object if it is, and it can't be the same as the existing state object as we need
            # to modify it during the checks to see if this slot is indeed possible to discharge in.
            newBatAllocateState = slotTest(batAllocateState, mostExpenciveRate)
            assert(newBatAllocateState != batAllocateState)
            if newBatAllocateState:
                # We can't change in the slot we're trying to discharge in, so remove this from the trial list.
                newBatAllocateState.availableExportRates.remove(mostExpenciveRate)
                # We can't charge and discharge at the same time, so remove the proposed discharge slot from 
                # the available charge rates. We also do the same for the existing import slots. It can make
                # sense to swap one import slot for export because the import and export prices are so different.
                newBatAllocateState.availableImportRates           = list(filter(lambda x: x[0] != mostExpenciveRate[0], newBatAllocateState.availableImportRates))
                newBatAllocateState.availableHouseGridPoweredRates = list(filter(lambda x: x[0] != mostExpenciveRate[0], newBatAllocateState.availableHouseGridPoweredRates))
                newBatAllocateState.gridChargingPlan               = list(filter(lambda x: x[0] != mostExpenciveRate[0], newBatAllocateState.gridChargingPlan))
                newBatAllocateState.houseGridPoweredPlan           = list(filter(lambda x: x[0] != mostExpenciveRate[0], newBatAllocateState.houseGridPoweredPlan))
                (fullyCharged, empty)                              = self.allocateChangingSlots(newBatAllocateState, now, maxImportRate)  
                # If we're still fully charged after swapping a slot to discharging, then make that the plan 
                # of record by updating the arrays. We also skip a potential discharge period if the 
                # difference between the cost of the charge / discharge periods isn't greater than the 
                # threshold. This reduces battery cycling if there's not much to be gained from it.
                newMaxCostRate          = newBatAllocateState.maxChargeCost
                newMaxHouseRateForEmpty = self.maxHouseRateForEmpty(newBatAllocateState)
                if newMaxHouseRateForEmpty != None:
                    newMaxCostRate = max(newBatAllocateState.maxChargeCost, newMaxHouseRateForEmpty)
                buySelMargin = mostExpenciveRate[2] - newMaxCostRate
                reqMargin    = self.minBuySelMargin if fullyCharged else self.minBuySelNotFullMargin
                if  buySelMargin > reqMargin:
                    batAllocateState.setTo(newBatAllocateState)
                    # Refilter the potential slots to take account of the new charging slots that have been allocated
                    potentialDischargeRates = filterOutChangeSlotsFromPotentialDischargeSlots(potentialDischargeRates, batAllocateState)

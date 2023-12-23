import hassapi as hass
from datetime   import datetime
from datetime   import timedelta
from datetime   import timezone
from statistics import mean
from core.powerCore import PowerControlCore
import re
import math
import numpy
import matplotlib.pyplot as plt
import json
import os
import importlib
import sys


#importlib.reload(sys.modules["core.powerCore"])


class PowerControl(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))        
        self.core                              = PowerControlCore(self.args, self.log)
        self.solarForecastMargin               = float(self.args['solarForecastMargin'])
        self.solarForecastLowPercentile        = float(self.args['solarForecastLowPercentile'])
        self.solarForecastHighPercentile       = float(self.args['solarForecastHighPercentile'])
        self.usageMargin                       = float(self.args['houseLoadMargin'])
        self.houseLoadEntityName               = self.args['houseLoadEntity']
        self.usageDaysHistory                  = self.args['usageDaysHistory']
        self.eddiOutputEntityName              = self.args['eddiOutputEntity']
        self.eddiSolarPowerUsedTodayEntityName = self.args['eddiSolarPowerUsedTodayEntity']
        self.eddiGridPowerUsedTodayEntityName  = self.args['eddiGridPowerUsedTodayEntity']
        self.solarLifetimeProdEntityName       = self.args['solarLifetimeProductionEntity']
        self.batteryModeOutputEntityName       = self.args['batteryModeOutputEntity']
        self.batteryPlanSummaryEntityName      = self.args['batteryPlanSummaryEntity']
        self.prevMaxChargeCostEntity           = self.args['batteryChargeCostEntity']
        self.batOutputTimeOffset               = timedelta(seconds=int(self.args['batteryOutputTimeOffset']))
        self.solarTuningDaysHistory            = 14
        self.solarActualsFileName              = "/conf/solarActuals.json" 
        self.solarProductionFileName           = "/conf/solarProduction.json" 
        self.solarTuningPath                   = "/conf/solarTuning"
        self.prevSolarLifetimeProd             = None
        self.prevSolarLifetimeProdTime         = None
        self.solarTuningModels                 = {}
        self.rawSolarData                      = []
        self.solarDataUntuned                  = []
        self.tariffOverrides                   = {'gas':    {},
                                                  'export': {},
                                                  'import': {}}

        # Leads the solar actuals if there's available
        self.solarActuals = {}
        if os.path.isfile(self.solarActualsFileName):
            with open(self.solarActualsFileName) as file:
                self.solarActuals = dict(map(lambda x: (int(x[0]), x[1]), json.load(file).items()))
        # Do the same for the solar production
        self.solarProduction = []
        if os.path.isfile(self.solarProductionFileName):
            with open(self.solarProductionFileName) as file:
                self.solarProduction = list(map(lambda x: (datetime.fromtimestamp(int(x[0])).astimezone(timezone.utc), 
                                                           datetime.fromtimestamp(int(x[1])).astimezone(timezone.utc),
                                                           x[2]), json.load(file)))                
        self.updateSolarTuning()
        # Setup getting the solar forecast data
        solarTodayEntityName    = self.args['solarForecastTodayEntity']
        solarTomorrowEntityName = self.args['solarForecastTomorrowEntity']
        solarD3EntityName       = self.args['solarForecastD3Entity']
        self.rawSolarData.append(self.get_state(solarTodayEntityName,    attribute='detailedForecast'))
        self.rawSolarData.append(self.get_state(solarTomorrowEntityName, attribute='detailedForecast'))
        self.rawSolarData.append(self.get_state(solarD3EntityName,       attribute='detailedForecast'))
        self.listen_state(self.solarChanged, solarTodayEntityName,    attribute='detailedForecast', kwargs=0) 
        self.listen_state(self.solarChanged, solarTomorrowEntityName, attribute='detailedForecast', kwargs=1)
        self.listen_state(self.solarChanged, solarD3EntityName,       attribute='detailedForecast', kwargs=2)
        self.parseSolar()
        # Setup getting the export rates
        exportRateEntityCurDayName  = self.args['exportRateEntityCurDay']
        exportRateEntityNextDayName = self.args['exportRateEntityNextDay']
        self.rawExportRates         = []
        self.rawExportRates.append(self.get_state(exportRateEntityCurDayName,   attribute='rates'))
        self.rawExportRates.append(self.get_state(exportRateEntityNextDayName,  attribute='rates'))        
        self.listen_state(self.exportRatesChanged, exportRateEntityCurDayName,  attribute='rates', kwargs=0) 
        self.listen_state(self.exportRatesChanged, exportRateEntityNextDayName, attribute='rates', kwargs=1) 
        self.core.exportRateData    = self.parseRates(self.rawExportRates, "export") or []
        # same again for the import rate
        importRateEntityCurDayName  = self.args['importRateEntityCurDay']
        importRateEntityNextDayName = self.args['importRateEntityNextDay']
        self.rawImportRates         = []
        self.rawImportRates.append(self.get_state(importRateEntityCurDayName,   attribute='rates'))
        self.rawImportRates.append(self.get_state(importRateEntityNextDayName,  attribute='rates'))
        self.listen_state(self.importRatesChanged, importRateEntityCurDayName,  attribute='rates', kwargs=0) 
        self.listen_state(self.importRatesChanged, importRateEntityNextDayName, attribute='rates', kwargs=1) 
        self.core.importRateData    = self.parseRates(self.rawImportRates, "import") or []
        # Setup getting batter stats        
        batteryCapacityEntityName = self.args['batteryCapacity']
        batteryEnergyEntityName   = self.args['batteryEnergy']
        self.core.batteryCapacity = float(self.get_state(batteryCapacityEntityName)) / 1000
        self.core.batteryEnergy   = float(self.get_state(batteryEnergyEntityName))   / 1000
        self.listen_state(self.batteryCapacityChanged, batteryCapacityEntityName) 
        self.listen_state(self.batteryEnergyChanged, batteryEnergyEntityName)
        # Setup getting gas rate
        gasRateEntityName = self.args['gasRateEntity']
        self.gasRateChanged(None, None, math.nan, self.get_state(gasRateEntityName), None)
        self.listen_state(self.gasRateChanged, gasRateEntityName) 
        # Gets tariff override details
        tariffOverrideStartEntityName = self.args['tariffOverrideStart']
        tariffOverrideEndEntityName   = self.args['tariffOverrideEnd']
        tariffOverridePriceEntityName = self.args['tariffOverridePrice']
        tariffOverrideTypeEntityName  = self.args['tariffOverrideType']
        self.tariffOverrideStartChanged(None, None, None, self.get_state(tariffOverrideStartEntityName,  attribute='timestamp'), None)
        self.tariffOverrideEndChanged(None,   None, None, self.get_state(tariffOverrideEndEntityName,    attribute='timestamp'), None)
        self.tariffOverridePriceChanged(None, None, None, self.get_state(tariffOverridePriceEntityName), None)
        self.tariffOverrideTypeChanged(None,  None, None, self.get_state(tariffOverrideTypeEntityName),  None)
        self.listen_state(self.tariffOverrideStartChanged, tariffOverrideStartEntityName, attribute='timestamp') 
        self.listen_state(self.tariffOverrideEndChanged,   tariffOverrideEndEntityName,   attribute='timestamp') 
        self.listen_state(self.tariffOverridePriceChanged, tariffOverridePriceEntityName) 
        self.listen_state(self.tariffOverrideTypeChanged,  tariffOverrideTypeEntityName) 
        # Schedule an update of the usage forcast every 6 hours
        self.run_every(self.updateUsageHistory, "now", 6*60*60)
        # Schedule the solar production recording and output update for 30 mintues, on the half hour boundary.
        # The output update is offset by a little so its just before the Tesla batch update so the settings
        # don't get delayed until the next update.
        now       = datetime.now() 
        period    = timedelta(minutes=30)
        startTime = now.replace(minute=0, second=0, microsecond=0)
        while startTime < now:
            startTime = startTime + period
        self.run_every(self.recordSolarProduction, startTime, 30*60)
        startTime = now.replace(minute=0, second=0, microsecond=0) + self.batOutputTimeOffset
        while startTime < now:
            startTime = startTime + period
        # Schedule an update of the eddi usage info just before the update of the main outputs. This way we won't delay 
        # the main computation if the eddi update takes time    
        self.run_every(self.fastEddiHistoryCallBack1, startTime - timedelta(minutes=1), 30*60)
        self.run_every(self.updateOutputs, startTime, 30*60)
        

    def recordSolarProduction(self, kwargs):
        curSolarLifetimeProd     = float(self.get_state(self.solarLifetimeProdEntityName))
        curSolarLifetimeProdTime = datetime.now(datetime.now(timezone.utc).astimezone().tzinfo)
        curSolarLifetimeProdTime = curSolarLifetimeProdTime.replace(second=0, microsecond=0)
        self.log("Solar production: " + str(self.prevSolarLifetimeProd) + " -> "+ str(curSolarLifetimeProd) )
        if self.prevSolarLifetimeProd != None:
            production = curSolarLifetimeProd - self.prevSolarLifetimeProd
            if production:
                self.solarProduction.append((self.prevSolarLifetimeProdTime, curSolarLifetimeProdTime, production))
                # Output the updated data to the file for long term persistence
                with open(self.solarProductionFileName, 'w') as file:
                    saveData = list(map(lambda x: (x[0].timestamp(), x[1].timestamp(), x[2]), self.solarProduction))
                    json.dump(saveData, file, ensure_ascii=False)
                
        # Rotate the vars for next time
        self.prevSolarLifetimeProd     = curSolarLifetimeProd
        self.prevSolarLifetimeProdTime = curSolarLifetimeProdTime
            

    def updateOutputs(self, kwargs):
        self.log("Updating outputs")
        # Adjust for the time offset that was applied when this function was scheduled.
        now                     = datetime.now(datetime.now(timezone.utc).astimezone().tzinfo) - self.batOutputTimeOffset
        maxChargeCost           = float(self.get_state(self.prevMaxChargeCostEntity))
        self.core.maxChargeCost = maxChargeCost
        self.core.save(now)
        self.core.mergeAndProcessData(now)
        # The time 15 minutes in the future (ie the middle of a time slot) to find a 
        # slot that starts now. This avoids any issues with this event firing a little 
        # early / late.
        slotMidTime              = now + timedelta(minutes=15)
        dischargeExportSolarInfo = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.dischargeExportSolarPlan), None)
        dischargeToGridInfo      = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.dischargeToGridPlan),      None)
        gridChargeInfo           = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.gridChargingPlan),         None)
        houseGridPowerdeInfo     = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.houseGridPoweredPlan),     None)
        solarChargeInfo          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.solarChargingPlan),        None)
        standbyInfo              = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.standbyPlan),              None)
        modeInfo                 = ("Discharge"          if dischargeExportSolarInfo else
                                    "Discharge to grid"  if dischargeToGridInfo      else
                                    "Standby"            if standbyInfo              else 
                                    "Grid charge"        if gridChargeInfo           else 
                                    "House grid powered" if houseGridPowerdeInfo     else "Solar charge")        
        eddiSolarInfo            = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.eddiSolarPlan), None)
        eddiGridInfo             = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.eddiGridPlan),  None)
        eddiInfo                 = ("boost" if eddiGridInfo  else
                                    "on"    if eddiSolarInfo else "off")
        # generate a summary string for the combined plan
        summary                  = ( list(map(lambda x: ("D", x[0]), self.core.mergeSeries(self.core.dischargeExportSolarPlan))) +
                                     list(map(lambda x: ("E", x[0]), self.core.mergeSeries(self.core.dischargeToGridPlan)))      +
                                     list(map(lambda x: ("C", x[0]), self.core.mergeSeries(self.core.solarChargingPlan)))        +
                                     list(map(lambda x: ("G", x[0]), self.core.mergeSeries(self.core.gridChargingPlan)))         +
                                     list(map(lambda x: ("H", x[0]), self.core.mergeSeries(self.core.houseGridPoweredPlan)))     +
                                     list(map(lambda x: ("S", x[0]), self.core.mergeSeries(self.core.standbyPlan)))              +
                                     list(map(lambda x: ("B", x[0]), self.core.mergeSeries(self.core.dischargeToHousePlan))) )
        summary.sort(key=lambda x: x[1])
        summary                  = list(map(lambda x: "{0}{1:%H%M}".format(*x)[:-1], summary))
        summary                  = ",".join(summary)

        # Update the prev max charge cost. We do this by resetting it aronud 4:30pm (when 
        # we've got the rate data for the next day), and updating if if we're starting a 
        # charging slot.
        if now.hour == 16 and now.minute > 15 and now.minute < 45:
            prevMaxChargeCost = 0
        else:
            prevMaxChargeCost = maxChargeCost
        if solarChargeInfo:
            curRrate          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.exportRateData), 0)
            prevMaxChargeCost = max(prevMaxChargeCost, curRrate[2] / self.core.batEfficiency)
        elif gridChargeInfo:
            curRrate          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.importRateData), 0)
            prevMaxChargeCost = max(prevMaxChargeCost, curRrate[2] / self.core.batEfficiency)
        elif houseGridPowerdeInfo:
            curRrate          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.core.importRateData), 0)
            prevMaxChargeCost = max(prevMaxChargeCost, curRrate[2])
        self.set_state(self.prevMaxChargeCostEntity, state=prevMaxChargeCost)

        self.set_state(self.batteryPlanSummaryEntityName, state=summary, attributes=self.core.gridSummary)
        self.set_state(self.batteryModeOutputEntityName, state=modeInfo, attributes={"planUpdateTime":           self.core.planUpdateTime,
                                                                                     "stateUpdateTime":          now,
                                                                                     "dischargeExportSolarPlan": self.core.seriesToString(self.core.dischargeExportSolarPlan, "<br/>", mergeable=True),
                                                                                     "dischargeToGridPlan":      self.core.seriesToString(self.core.dischargeToGridPlan,      "<br/>", mergeable=True),
                                                                                     "dischargeToHousePlan":     self.core.seriesToString(self.core.dischargeToHousePlan,     "<br/>", mergeable=True),
                                                                                     "solarChargingPlan":        self.core.seriesToString(self.core.solarChargingPlan,        "<br/>", mergeable=True),
                                                                                     "gridChargingPlan":         self.core.seriesToString(self.core.gridChargingPlan,         "<br/>", mergeable=True),
                                                                                     "houseGridPoweredPlan":     self.core.seriesToString(self.core.houseGridPoweredPlan,     "<br/>", mergeable=True),
                                                                                     "standbyPlan":              self.core.seriesToString(self.core.standbyPlan,              "<br/>", mergeable=True),
                                                                                     "tariff":                   self.core.pwTariff,
                                                                                     "defPrice":                 self.core.defPrice})
        self.set_state(self.eddiOutputEntityName,        state=eddiInfo, attributes={"planUpdateTime":           self.core.planUpdateTime,
                                                                                     "stateUpdateTime":          now,
                                                                                     "solarPlan":                self.core.seriesToString(self.core.eddiSolarPlan, "<br/>", mergeable=True),
                                                                                     "gridPlan":                 self.core.seriesToString(self.core.eddiGridPlan,  "<br/>", mergeable=True)})
        # Update the solar actuals and tuning at the end of the day
        if now.hour == 23 and now.minute > 15 and now.minute < 45:
            self.updateSolarActuals(now)
            self.updateSolarTuning()


    def updateSolarTuning(self):
        # Convert to a series array so we can use all the normal utilitiy functions
        solarActualsSeries = list(map(lambda x: (datetime.fromtimestamp(x[0]).astimezone(timezone.utc), 
                                                 datetime.fromtimestamp(x[1][0]).astimezone(timezone.utc), 
                                                 x[1][1]), self.solarActuals.items()))
        solarActualsSeries.sort(key=lambda x: x[0])

        # pass the production values through a operation so we get a series that's got the same number of elements
        # (and for the same times) as the actuals series. Then combine it with the estimated actuals series.
        pairedValues = self.core.combineSeries(solarActualsSeries,
                                               self.core.opOnSeries(solarActualsSeries, self.solarProduction, lambda a, b: b))
        # Combine all the samples for each timeslot
        timeSlots = {}
        for pair in pairedValues:
            key = (pair[0].replace(year=2000, month=1, day=1), 
                   pair[1].replace(year=2000, month=1, day=1))
            if key not in timeSlots:
                timeSlots[key] = []
            timeSlots[key].append((pair[2], pair[3]))
        
        # Make sure the plot output dir exists
        if not os.path.exists(self.solarTuningPath):
            os.mkdir(self.solarTuningPath)
        # Now go through each time slot and creaty a model that best fits the data
        models = {}
        for key, data in timeSlots.items():
            # Filter out obviously wrong values and seperate out the data, and fit the model
            data             = list(filter(lambda x: x[0] < 0.75 or x[1], data))            
            estimatedActuals = list(map(lambda x: x[0], data))
            production       = list(map(lambda x: x[1], data))
            model            = numpy.poly1d(numpy.polyfit(estimatedActuals, production, 1))
            # Don't publish the model to use in turing forecasts if we don't have many points
            if len(data) >= 7:
                models[key] = model            
            # Now we have the model, plot and save a small graph showing the tuning profile
            polyline = numpy.linspace(0, max(estimatedActuals), 50) 
            plt.scatter(estimatedActuals, production)
            plt.plot(polyline, model(polyline))
            plt.savefig(self.solarTuningPath + "/{0:%H-%M}.png".format(key[0].astimezone()))
            plt.close()
        
        # Update the global models
        self.solarTuningModels = models
    

    def updateSolarActuals(self, now):
        # Add any current estimated actuals to the main history dict. We do most of the storage 
        # and manipulation as a dict so its quick to insert, delete, and search for items.
        for solarSample in self.solarDataUntuned:
            if solarSample[0] < now:
                startTime = int(solarSample[0].timestamp())
                endTime   = int(solarSample[1].timestamp())
                self.solarActuals[startTime] = [endTime, solarSample[2]]
        # Filter out any really old samples
        discardTime = (now - timedelta(days=self.solarTuningDaysHistory)).timestamp()
        for key in list(self.solarActuals.keys()):
            if key < discardTime:
                del self.solarActuals[key] 
        
        # Save the new data off to a file so we preserve it accross restarts    
        with open(self.solarActualsFileName, 'w') as file:
            json.dump(self.solarActuals, file, ensure_ascii=False)


    def gasRateChanged(self, entity, attribute, old, new, kwargs):
        new = self.core.toFloat(new, None)
        if new != None:
            override = self.tariffOverrides['gas']
            if override:
                new = override.get(new, new)
            self.log("Gas rate changed {0:.3f} -> {1:.3f}".format(float(old), new))
            self.core.gasRate = new


    def tariffOverrideStartChanged(self, entity, attribute, old, new, kwargs):
        new = datetime.fromtimestamp(int(new)).astimezone(timezone.utc)
        self.log("Tariff override start {0}".format(new))
        self.core.tariffOverrideStart = new


    def tariffOverrideEndChanged(self, entity, attribute, old, new, kwargs):
        new = datetime.fromtimestamp(int(new)).astimezone(timezone.utc) 
        self.log("Tariff override end {0}".format(new))
        self.core.tariffOverrideEnd = new
        

    def tariffOverridePriceChanged(self, entity, attribute, old, new, kwargs):
        new = self.core.toFloat(new, None)
        if new != None:
            self.log("Tariff override price {0:.3f}".format(new))
            self.core.tariffOverridePrice = new


    def tariffOverrideTypeChanged(self, entity, attribute, old, new, kwargs):
        self.log("Tariff override type {0}".format(new))
        self.core.tariffOverrideType = new
        # only update the plan if this isn't the initial call during app boot.
        if old:
            self.updateOutputs(None)

        
    def batteryCapacityChanged(self, entity, attribute, old, new, kwargs):
        new = float(new) / 1000
        self.log("Battery capacity changed {0:.3f} -> {1:.3f}".format(self.core.batteryCapacity, new))
        self.core.batteryCapacity = new        


    def batteryEnergyChanged(self, entity, attribute, old, new, kwargs):
        new = float(new) / 1000
        self.log("Battery energy changed {0:.3f} -> {1:.3f}".format(self.core.batteryEnergy, new))
        self.core.batteryEnergy = new
        
        
    def solarChanged(self, entity, attribute, old, new, kwargs):
        index                    = kwargs['kwargs']
        self.rawSolarData[index] = new
        self.parseSolar()

    
    def exportRatesChanged(self, entity, attribute, old, new, kwargs):
        if new:
            index                      = kwargs['kwargs']
            self.log("Export rates changed: i:{0} l:{1}".format(index, len(new)))
            self.rawExportRates[index] = new
            newRates                   = self.parseRates(self.rawExportRates, "export")
            if newRates:
                self.core.exportRateData = newRates


    def importRatesChanged(self, entity, attribute, old, new, kwargs):
        if new:
            index                      = kwargs['kwargs']
            self.log("Import rates changed: i:{0} l:{1}".format(index, len(new)))
            self.rawImportRates[index] = new
            newRates                   = self.parseRates(self.rawImportRates, "import")
            if newRates:
                self.core.importRateData = newRates


    def parseSolar(self):
        self.log("Updating solar forecast")
        # flatten the forecasts arrays for the different days
        flatForecast = [x for xs in self.rawSolarData for x in xs]        
        powerData    = list(map(lambda x: (datetime.fromisoformat(x['period_start']), 
                                           x['pv_estimate'],
                                           x.get('pv_estimate10'),
                                           x.get('pv_estimate90')), 
                                flatForecast))
        powerData.sort(key=lambda x: x[0])
        timeRangeTunedPowerData   = []
        timeRangeUntunedPowerData = []
        prevStartTime             = None
        prevPower                 = None
        prevMetaData              = None
        prevMinEstimate           = None
        prevMaxEstimate           = None
        dailyTotals               = {}
        # Reformat the data so we end up with a tuple with elements (startTime, end , power)
        for data in powerData:
            curStartTime = data[0]
            if prevPower:
                # Calculate a new set of predictions based on the tuned solar models
                key   = (prevStartTime.astimezone(timezone.utc).replace(year=2000, month=1, day=1), 
                         curStartTime.astimezone(timezone.utc).replace(year=2000, month=1, day=1))
                model = self.solarTuningModels.get(key)
                prevPowerUntuned = prevPower
                if model:
                    prevPower       = round(max(model(prevPower),       0), 3)
                    prevMinEstimate = round(max(model(prevMinEstimate), 0), 3)
                    prevMaxEstimate = round(max(model(prevMaxEstimate), 0), 3)
                # Update the outputs
                timeRangeUntunedPowerData.append( (prevStartTime, curStartTime, prevPowerUntuned) )
                timeRangeTunedPowerData.append(   (prevStartTime, curStartTime, prevPower, prevMinEstimate, prevMaxEstimate, prevMetaData) )
                prevDate = prevStartTime.date()
                if prevDate not in dailyTotals:
                    dailyTotals[prevDate] = [0,0,0]
                dailyTotals[prevDate][0] = dailyTotals[prevDate][0] + prevPower
                dailyTotals[prevDate][1] = dailyTotals[prevDate][1] + prevMinEstimate
                dailyTotals[prevDate][2] = dailyTotals[prevDate][2] + prevMaxEstimate
                
            prevStartTime = curStartTime
            # Process the estimates
            percentile10 = data[2]
            percentile50 = data[1]
            percentile90 = data[3]
            if percentile10:
                percentile10 = round(percentile10, 3)
            if percentile90:
                percentile90 = round(percentile90, 3)
            if percentile10 and percentile90:
                # Extrapolate a polynomial from the percentiles and the fact that we should get 0 power at the 
                # 0th percentile. Then use the polynomial to generate the percentiles we need.
                forecastPercentiles = [0, 10, 50, 90]
                forecastValues      = [0, percentile10, percentile50, percentile90]
                polyLine            = numpy.poly1d(numpy.polyfit(forecastPercentiles, forecastValues, 3))
                prevMinEstimate     = polyLine(self.solarForecastLowPercentile)
                prevMaxEstimate     = polyLine(self.solarForecastHighPercentile)
            else:
                prevMinEstimate = percentile50 * self.solarForecastMargin
                prevMaxEstimate = percentile50
            prevMinEstimate = round(prevMinEstimate, 3)
            prevMaxEstimate = round(prevMaxEstimate, 3)
            prevPower       = round(percentile50, 3)
            prevMetaData    = (percentile10, prevPower, percentile90)
        self.core.printSeries(timeRangeTunedPowerData, "Solar forecast")
        for totals in dailyTotals:
            vals = dailyTotals[totals]
            self.log("Total for {0:%d %B} : {1:.3f} {2:.3f} {3:.3f}".format(totals, vals[0], vals[1], vals[2]))
        self.solarDataUntuned = timeRangeUntunedPowerData
        self.core.solarData   = timeRangeTunedPowerData


    def parseRates(self, rawRateData, type):
        self.log("Updating " + type + " tariff rates")
        rateData = None
        if rawRateData:
            # Flattend the rate data first so we have one array for all days. We turn this into a dictionary to remove any duplicates
            rawRateData = {x['start']:x for xs in rawRateData for x in xs}                    
            rateData    = list(map(lambda x: (datetime.fromisoformat(x['start']).astimezone(),
                                              datetime.fromisoformat(x['end']).astimezone(), 
                                              x['value_inc_vat']), 
                                   rawRateData.values()))
            override    = self.tariffOverrides[type]
            if override:
                rateData = list(map(lambda x: (x[0], x[1], override.get(x[2], x[2])), 
                                    rateData))
            rateData.sort(key=lambda x: x[0])    
            self.core.printSeries(rateData, "Rate data (" + type + ")")
        return rateData


    def updateUsageHistory(self, kwargs):
        self.log("Updating usage history")
        # Calculate a time in the past to start profiling usage from
        startTime               = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        startTime               = startTime - timedelta(days=self.usageDaysHistory) 
        self.usageStartTime     = startTime
        # Now request the history data. Note: we subtract a further 2 hours from the start time so 
        # we're guaranteed to get data from before the start time we requested
        self.usageFetchFromTime = startTime - timedelta(hours=2)
        self.get_history(entity_id  = self.houseLoadEntityName,
                         start_time = self.usageFetchFromTime,
                         callback   = self.usageHistoryCallback)
        
        
    def usageHistoryCallback(self, kwargs):
        self.usagePowerData = kwargs
        self.get_history(entity_id  = self.eddiSolarPowerUsedTodayEntityName,
                         start_time = self.usageFetchFromTime,
                         callback   = self.usageEddiHistoryCallBack1)


    def processUsageDataToTimeRange(self, rawUsageData):
        rawUsageData = rawUsageData.get("result", [])
        rawUsageData = rawUsageData[0] if rawUsageData else []
        rawUsageData = list(map(lambda x: (datetime.fromisoformat(x['last_changed']), 
                                           x['state']), 
                                rawUsageData))
        rawUsageData.sort(key=lambda x: x[0])    
        timeRangeUsageData = []
        startPower         = None
        startTime          = None
        powerOffset        = 0
        prevPower          = 0
        # Reformat the data so we end up with a tuple with elements (startTime, end , power delta)
        for data in rawUsageData:
            try:
                curSampleEndTime  = data[0]
                # Some data series reset at the end of the day, spot a reduction in the value and 
                # use that to update the offset we apply to turn it into a monotonic series.
                curSampleEndPower = float(data[1])
                if curSampleEndPower == 0:
                    powerOffset = prevPower
                curSampleEndPower = curSampleEndPower + powerOffset
                prevPower         = curSampleEndPower
                # Reformat the data to the correct from-to, and delta format
                if startTime:
                    timeRangeUsageData.append( (startTime, curSampleEndTime, curSampleEndPower - startPower) )
                startPower = curSampleEndPower
                startTime  = curSampleEndTime
            except ValueError:
                # just ignore invalid samples
                pass
        return timeRangeUsageData


    def processEddiData(self):
        self.log("Eddi data updated")
        timeRangeEddiGridUsageData  = self.processUsageDataToTimeRange(self.eddiGridData)
        timeRangeEddiSolarUsageData = self.processUsageDataToTimeRange(self.eddiSolarData)
        self.core.eddiData          = self.core.opOnSeries(timeRangeEddiGridUsageData, timeRangeEddiSolarUsageData, lambda a, b: a+b)


    def fastEddiHistoryCallBack1(self, kwargs):
        self.eddiSolarData = kwargs
        self.get_history(entity_id  = self.eddiGridPowerUsedTodayEntityName,
                         start_time = self.usageFetchFromTime,
                         callback   = self.fastEddiHistoryCallBack2)


    def fastEddiHistoryCallBack2(self, kwargs):
        self.eddiGridData = kwargs
        self.processEddiData()
        

    def usageEddiHistoryCallBack1(self, kwargs):
        self.eddiSolarData = kwargs
        self.get_history(entity_id  = self.eddiGridPowerUsedTodayEntityName,
                         start_time = self.usageFetchFromTime,
                         callback   = self.usageEddiHistoryCallBack2)


    def usageEddiHistoryCallBack2(self, kwargs):
        self.eddiGridData = kwargs
        self.processEddiData()
        timeRangeUsageData = self.processUsageDataToTimeRange(self.usagePowerData)

        # Now go through the data creating an average usage for each time period based on the last x days history
        forecastUsage          = []
        now                    = datetime.now(datetime.now(timezone.utc).astimezone().tzinfo)
        forecastUsageStartTime = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for _ in range(0,24*2):
            # calculate the start / end time of the timeslot
            forecastUsageEndTime = forecastUsageStartTime + timedelta(minutes=30)
            # go back over the last few days for this time period and get the usage
            avgUsage = 0.0
            for days in range(1, self.usageDaysHistory+1):
                daysDelta      = timedelta(days=days)
                # For each half hour period we subtract the eddi usage from the total usage. We don't
                # want the eddi distorting the usage totals as we explicitly plan the eddi usage seperately.
                usageForPeriod = self.core.powerForPeriod(timeRangeUsageData, 
                                                          forecastUsageStartTime - daysDelta, 
                                                          forecastUsageEndTime   - daysDelta)
                eddiForPeriod  = self.core.powerForPeriod(self.core.eddiData, 
                                                          forecastUsageStartTime - daysDelta, 
                                                          forecastUsageEndTime   - daysDelta)
                avgUsage       = avgUsage + (usageForPeriod - eddiForPeriod)
            avgUsage = (avgUsage / self.usageDaysHistory) * self.usageMargin
            # finally add the data to the usage array
            forecastUsage.append((forecastUsageStartTime, forecastUsageEndTime, avgUsage)) 
            forecastUsageStartTime = forecastUsageEndTime
        # Extend the usage forcast into the future by the number of house in the planning window
        forecastUsage = self.core.extendSeries(forecastUsage, timedelta(hours=24))        
        self.core.printSeries(forecastUsage, "Usage forecast")
        self.core.usageData = forecastUsage
        # If there's not been an output update so far, force it now
        if not bool(self.core.planUpdateTime): 
            self.updateOutputs(None)        


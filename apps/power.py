import hassapi as hass
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


class PowerControl(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))        
        self.solarForecastMargin          = float(self.args['solarForecastMargin'])
        self.solarForecastLowPercentile   = float(self.args['solarForecastLowPercentile'])
        self.solarForecastHighPercentile  = float(self.args['solarForecastHighPercentile'])
        self.houseLoadEntityName          = self.args['houseLoadEntity']
        self.usageDaysHistory             = self.args['usageDaysHistory']
        self.eddiOutputEntityName         = self.args['eddiOutputEntity']
        self.eddiPowerUsedTodayEntityName = self.args['eddiPowerUsedTodayEntity']
        self.solarLifetimeProdEntityName  = self.args['solarLifetimeProductionEntity']
        self.batteryModeOutputEntityName  = self.args['batteryModeOutputEntity']
        self.batteryPlanSummaryEntityName = self.args['batteryPlanSummaryEntity']
        self.batOutputTimeOffset          = timedelta(seconds=int(self.args['batteryOutputTimeOffset']))
        self.usageMargin                  = float(self.args['houseLoadMargin'])
        self.maxChargeRate                = float(self.args['batteryChargeRateLimit'])
        self.batteryGridChargeRate        = float(self.args['batteryGridChargeRate'])
        self.batReservePct                = float(self.args['batteryReservePercentage'])
        self.batFullPct                   = float(self.args['batteryFullPercentage'])
        self.gasEfficiency                = float(self.args['gasHotWaterEfficiency'])
        self.eddiTargetPower              = float(self.args['eddiTargetPower'])
        self.eddiPowerLimit               = float(self.args['eddiPowerLimit'])
        self.minBuySelMargin              = float(self.args['minBuySelMargin'])
        self.minBuyUseMargin              = float(self.args['minBuyUseMargin'])
        self.prevMaxChargeCostEntity      = self.args['batteryChargeCostEntity']
        self.batFullPctHysteresis         = 3
        self.batEfficiency                = 0.9
        self.solarTuningDaysHistory       = 14
        self.solarActualsFileName         = "/conf/solarActuals.json" 
        self.solarProductionFileName      = "/conf/solarProduction.json" 
        self.solarTuningPath              = "/conf/solarTuning"
        
        self.solarData                 = []
        self.exportRateData            = []
        self.importRateData            = []
        self.usageData                 = []
        self.rawSolarData              = []
        self.solarChargingPlan         = []
        self.gridChargingPlan          = []
        self.houseGridPoweredPlan      = []
        self.standbyPlan               = []
        self.dischargePlan             = []
        self.dischargeToHousePlan      = []
        self.eddiPlan                  = []
        self.planUpdateTime            = None
        self.prevSolarLifetimeProd     = None
        self.prevSolarLifetimeProdTime = None
        self.solarTuningModels         = {}
        self.tariffOverrides           = {'gas':    {},
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
        # Setup getting the solar forecast data
        solarTodayEntityName    = self.args['solarForecastTodayEntity']
        solarTomorrowEntityName = self.args['solarForecastTomorrowEntity']
        self.rawSolarData.append(self.get_state(solarTodayEntityName,    attribute='detailedForecast'))
        self.rawSolarData.append(self.get_state(solarTomorrowEntityName, attribute='detailedForecast'))
        self.listen_state(self.solarChanged, solarTodayEntityName,    attribute='detailedForecast', kwargs=0) 
        self.listen_state(self.solarChanged, solarTomorrowEntityName, attribute='detailedForecast', kwargs=1)
        self.parseSolar()
        # Setup getting the export rates
        exportRateEntityName = self.args['exportRateEntity']
        rawRateData          = self.get_state(exportRateEntityName, attribute='rates')
        self.listen_state(self.exportRatesChanged, exportRateEntityName, attribute='rates') 
        self.exportRateData  = self.parseRates(rawRateData, "export")
        # same again for the import rate
        importRateEntityName = self.args['importRateEntity']
        rawRateData          = self.get_state(importRateEntityName, attribute='rates')
        self.listen_state(self.importRatesChanged, importRateEntityName, attribute='rates') 
        self.importRateData  = self.parseRates(rawRateData, "import")
        # Setup getting batter stats        
        batteryCapacityEntityName = self.args['batteryCapacity']
        batteryEnergyEntityName   = self.args['batteryEnergy']
        self.batteryCapacity      = float(self.get_state(batteryCapacityEntityName)) / 1000
        self.batteryEnergy        = float(self.get_state(batteryEnergyEntityName))   / 1000
        self.listen_state(self.batteryCapacityChanged, batteryCapacityEntityName) 
        self.listen_state(self.batteryEnergyChanged, batteryEnergyEntityName)
        # Setup getting gas rate
        gasRateEntityName = self.args['gasRateEntity']
        self.gasRateChanged(None, None, math.nan, self.get_state(gasRateEntityName), None)
        self.listen_state(self.gasRateChanged, gasRateEntityName) 
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
        now                  = datetime.now(datetime.now(timezone.utc).astimezone().tzinfo) - self.batOutputTimeOffset
        self.mergeAndProcessData(now)
        # The time 15 minutes in the future (ie the middle of a time slot) to find a 
        # slot that starts now. This avoids any issues with this event firing a little 
        # early / late.
        slotMidTime          = now + timedelta(minutes=15)
        dischargeInfo        = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.dischargePlan),        None)
        gridChargeInfo       = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.gridChargingPlan),     None)
        houseGridPowerdeInfo = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.houseGridPoweredPlan), None)
        solarChargeInfo      = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.solarChargingPlan),    None)
        standbyInfo          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.standbyPlan),          None)
        modeInfo             = ("Discharge"          if dischargeInfo        else
                                "Standby"            if standbyInfo          else 
                                "Grid charge"        if gridChargeInfo       else 
                                "House grid powered" if houseGridPowerdeInfo else "Solar charge")        
        eddiInfo             = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.eddiPlan), None)
        eddiInfo             = "on" if eddiInfo else "off"
        # generate a summary string for the combined plan
        summary              = ( list(map(lambda x: ("D", x[0]), self.mergeSeries(self.dischargePlan)))        +
                                 list(map(lambda x: ("C", x[0]), self.mergeSeries(self.solarChargingPlan)))    +
                                 list(map(lambda x: ("G", x[0]), self.mergeSeries(self.gridChargingPlan)))     +
                                 list(map(lambda x: ("H", x[0]), self.mergeSeries(self.houseGridPoweredPlan))) +
                                 list(map(lambda x: ("S", x[0]), self.mergeSeries(self.standbyPlan)))          +
                                 list(map(lambda x: ("B", x[0]), self.mergeSeries(self.dischargeToHousePlan))) )
        summary.sort(key=lambda x: x[1])
        summary              = list(map(lambda x: "{0}{1:%H%M}".format(*x)[:-1], summary))
        summary              = ",".join(summary)

        # Update the prev max charge cost. We do this by resetting it aronud 4:30pm (when 
        # we've got the rate data for the next day), and updating if if we're starting a 
        # charging slot.
        if now.hour == 16 and now.minute > 15 and now.minute < 45:
            prevMaxChargeCost = 0
        else:
            prevMaxChargeCost = float(self.get_state(self.prevMaxChargeCostEntity))
        if solarChargeInfo:
            curRrate          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.exportRateData), 0)
            prevMaxChargeCost = max(prevMaxChargeCost, curRrate[2] / self.batEfficiency)
        elif gridChargeInfo:
            curRrate          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.importRateData), 0)
            prevMaxChargeCost = max(prevMaxChargeCost, curRrate[2] / self.batEfficiency)
        elif houseGridPowerdeInfo:
            curRrate          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.importRateData), 0)
            prevMaxChargeCost = max(prevMaxChargeCost, curRrate[2])
        self.set_state(self.prevMaxChargeCostEntity, state=prevMaxChargeCost)

        self.set_state(self.batteryPlanSummaryEntityName, state=summary)
        self.set_state(self.batteryModeOutputEntityName, state=modeInfo,      attributes={"planUpdateTime":       self.planUpdateTime,
                                                                                          "stateUpdateTime":      now,
                                                                                          "dischargePlan":        self.seriesToString(self.dischargePlan,        mergeable=True),
                                                                                          "solarChargingPlan":    self.seriesToString(self.solarChargingPlan,    mergeable=True),
                                                                                          "gridChargingPlan":     self.seriesToString(self.gridChargingPlan,     mergeable=True),
                                                                                          "houseGridPoweredPlan": self.seriesToString(self.houseGridPoweredPlan, mergeable=True),
                                                                                          "standbyPlan":          self.seriesToString(self.standbyPlan,          mergeable=True),
                                                                                          "tariff":               self.pwTariff,
                                                                                          "defPrice":             self.defPrice})
        self.set_state(self.eddiOutputEntityName,        state=eddiInfo,      attributes={"planUpdateTime":       self.planUpdateTime,
                                                                                          "stateUpdateTime":      now,
                                                                                          "plan":                 self.seriesToString(self.eddiPlan, mergeable=True)})
        # Update the solar actuals and tuning at the end of the day
        if now.hour == 23 and now.minute > 15 and now.minute < 45:
            pass
        if True:
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
        pairedValues = self.combineSeries(solarActualsSeries,
                                          self.opOnSeries(solarActualsSeries, self.solarProduction, lambda a, b: b))
        # Combine all the samples for each timeslot
        timeSlots = {}
        for pair in pairedValues:
            key = (pair[0].astimezone().replace(year=2000, month=1, day=1), 
                   pair[1].astimezone().replace(year=2000, month=1, day=1))
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
            if len(data) > 5:
                models[key] = model            
            # Now we have the model, plot and save a small graph showing the tuning profile
            polyline = numpy.linspace(0, max(estimatedActuals), 50) 
            plt.scatter(estimatedActuals, production)
            plt.plot(polyline, model(polyline))
            plt.savefig(self.solarTuningPath + "/{0:%H-%M}.png".format(key[0]))
            plt.close()
        
        # Update the global models
        self.solarTuningModels = models
    

    def updateSolarActuals(self, now):
        # Add any current estimated actuals to the main history dict. We do most of the storage 
        # and manipulation as a dict so its quick to insert, delete, and search for items.
        for solarSample in self.solarData:
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


    def toFloat(self, string, default):
        try:
            value = float(string)
        except ValueError:
            value = default
        return value


    def gasRateChanged(self, entity, attribute, old, new, kwargs):
        new = float(new)
        override = self.tariffOverrides['gas']
        if override:
            new = override.get(new, new)
        self.log("Gas rate changed {0:.3f} -> {1:.3f}".format(float(old), new))
        self.gasRate = new
                    
        
    def batteryCapacityChanged(self, entity, attribute, old, new, kwargs):
        new = float(new) / 1000
        self.log("Battery capacity changed {0:.3f} -> {1:.3f}".format(self.batteryCapacity, new))
        self.batteryCapacity = new        


    def batteryEnergyChanged(self, entity, attribute, old, new, kwargs):
        new = float(new) / 1000
        self.log("Battery energy changed {0:.3f} -> {1:.3f}".format(self.batteryEnergy, new))
        self.batteryEnergy = new
        
        
    def solarChanged(self, entity, attribute, old, new, kwargs):
        index                    = kwargs['kwargs']
        self.rawSolarData[index] = new
        self.parseSolar()

    
    def exportRatesChanged(self, entity, attribute, old, new, kwargs):
        if new:
            self.exportRateData = self.parseRates(new, "export")


    def importRatesChanged(self, entity, attribute, old, new, kwargs):
        if new:
            self.importRateData = self.parseRates(new, "import")


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
        timeRangePowerData = []
        prevStartTime      = None
        prevPower          = None
        prevMetaData       = None
        prevMinEstimate    = None
        prevMaxEstimate    = None
        dailyTotals        = {}
        # Reformat the data so we end up with a tuple with elements (startTime, end , power)
        for data in powerData:
            curStartTime = data[0]
            if prevPower:
                timeRangePowerData.append( (prevStartTime, curStartTime, prevPower, prevMinEstimate, prevMaxEstimate, prevMetaData) )
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
        self.printSeries(timeRangePowerData, "Solar forecast")
        for totals in dailyTotals:
            vals = dailyTotals[totals]
            self.log("Total for {0:%d %B} : {1:.3f} {2:.3f} {3:.3f}".format(totals, vals[0], vals[1], vals[2]))
        self.solarData = timeRangePowerData


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
        

    def parseRates(self, rawRateData, type):
        self.log("Updating " + type + " tariff rates")
        rateData = list(map(lambda x: (datetime.fromisoformat(x['from']).astimezone(),
                                       datetime.fromisoformat(x['to']).astimezone(), 
                                       x['rate']/100), 
                            rawRateData))
        override = self.tariffOverrides[type]
        if override:
            rateData = list(map(lambda x: (x[0], x[1], override.get(x[2], x[2])), 
                                rateData))
        rateData.sort(key=lambda x: x[0])    
        self.printSeries(rateData, "Rate data (" + type + ")")
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
        self.get_history(entity_id  = self.eddiPowerUsedTodayEntityName,
                         start_time = self.usageFetchFromTime,
                         callback   = self.eddiHistoryCallBack)


    def processUsageDataToTimeRange(self, rawUsageData):
        rawUsageData = list(map(lambda x: (datetime.fromisoformat(x['last_changed']), 
                                           x['state']), 
                                rawUsageData["result"][0]))
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


    def eddiHistoryCallBack(self, kwargs):
        # Convert the raw data from HA into time series delta data
        timeRangeEddiUsageData = self.processUsageDataToTimeRange(kwargs)
        timeRangeUsageData     = self.processUsageDataToTimeRange(self.usagePowerData)

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
                usageForPeriod = self.powerForPeriod(timeRangeUsageData, 
                                                     forecastUsageStartTime - daysDelta, 
                                                     forecastUsageEndTime   - daysDelta)
                eddiForPeriod  = self.powerForPeriod(timeRangeEddiUsageData, 
                                                     forecastUsageStartTime - daysDelta, 
                                                     forecastUsageEndTime   - daysDelta)
                avgUsage       = avgUsage + (usageForPeriod - eddiForPeriod)
            avgUsage = (avgUsage / self.usageDaysHistory) * self.usageMargin
            # finally add the data to the usage array
            forecastUsage.append((forecastUsageStartTime, forecastUsageEndTime, avgUsage)) 
            forecastUsageStartTime = forecastUsageEndTime
        # Double up the forecast so it covers tomorrow as well as today. That way we have a full 
        # rolling day regardless of the start time.
        dayDelta          = timedelta(days=1)
        tomorrowsForecast = list(map(lambda x: (x[0]+dayDelta, 
                                                x[1]+dayDelta, 
                                                x[2]), 
                                 forecastUsage))
        forecastUsage.extend(tomorrowsForecast)
        self.printSeries(forecastUsage, "Usage forecast")
        self.usageData = forecastUsage
        # If there's not been an output update so far, force it now
        if not bool(self.planUpdateTime): 
            self.updateOutputs(None)        


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
        exportRateData    = list(filter(lambda x: x[1] >= now, self.exportRateData))
        importRateData    = list(filter(lambda x: x[1] >= now, self.importRateData))
        # remove any import rate data that is outside the time range for the export rates and vice 
        # versa. This means we can safely evelauate everything together
        exportRateEndTime = max(self.exportRateData, key=lambda x: x[1])[1]
        importRateEndTime = max(self.importRateData, key=lambda x: x[1])[1]
        exportRateData    = list(filter(lambda x: x[1] <= importRateEndTime, exportRateData))
        importRateData    = list(filter(lambda x: x[1] <= exportRateEndTime, importRateData))
        # We can't import and export at the same time, so remove and import rates slots for times when
        # there's a solar surplus. In reality it isn't quite this simple, eg if there's 1KW of surplus 
        # we could charge at 3kw by pulling some from the grid. But for the moment this hybrid style 
        # charging isn't worth the extra complexity it would involve
        importRateData    = list(filter(lambda x: self.powerForPeriod(solarSurplus, x[0], x[1]) <= 0, importRateData))
        
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
        eddiPowerRequired = self.eddiTargetPower - self.toFloat(self.get_state(self.eddiPowerUsedTodayEntityName), 0)
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
         empty)                             = self.genBatLevelForecast(exportRateData, usageAfterSolar, solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, now, percentileIndex)
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
                        emptySlotCost = next(filter(lambda x: x[1] == firstEmptySlot, self.importRateData), None)[2]
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
        minImportChargeRate  = min(map(lambda x: x[2], self.importRateData)) / self.batEfficiency
        maxImportRate        = max(map(lambda x: x[2], self.importRateData))
        # We create a set of effective "charge" rates associated with not discharging the battery. The 
        # idea is that if we choose not to discharge for a period that's the same as charging the battery 
        # with the same amount of power. It's actually better than this because not cycling the battery
        # means we reduce the battery wear, and don't have the battery efficency overhead. 
        availableHouseGridPoweredRates = list(availableImportRates)

        # We don't want to discharge the battery for any slots where the cost of running the house off 
        # the grid is lower than what we've previously paid to charge the battery. So add any grid 
        # powered rates that are below the current charge cost
        maxChargeCost = float(self.get_state(self.prevMaxChargeCostEntity))
        for rate in list(filter(lambda x: x[2] < maxChargeCost, availableHouseGridPoweredRates)):
            availableHouseGridPoweredRates.remove(rate)
            usage = self.powerForPeriod(usageAfterSolar, rate[0], rate[1])
            houseGridPoweredPlan.append((rate[0], rate[1], usage))

        # calculate the initial charging profile
        (batProfile, _, _, newMaxChargeCost) = self.allocateChangingSlots(exportRateData, availableChargeRates, availableImportRates, availableHouseGridPoweredRates,  
                                                                          solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, solarSurplus, usageAfterSolar, now,
                                                                          maxImportRate)
        maxChargeCost                        = max(newMaxChargeCost, maxChargeCost)

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
    
    
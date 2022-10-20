import hassapi as hass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import re
import math


class PowerControl(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))        
        self.solarForecastMargin          = float(self.args['solarForecastMargin'])
        self.houseLoadEntityName          = self.args['houseLoadEntity']
        self.usageDaysHistory             = self.args['usageDaysHistory']
        self.eddiOutputEntityName         = self.args['eddiOutputEntity']
        self.eddiPowerUsedTodayEntityName = self.args['eddiPowerUsedTodayEntity']
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
        
        self.solarData            = []
        self.exportRateData       = []
        self.importRateData       = []
        self.usageData            = []
        self.rawSolarData         = []
        self.solarChargingPlan    = []
        self.gridChargingPlan     = []
        self.houseGridPoweredPlan = []
        self.standbyPlan          = []
        self.dischargePlan        = []
        self.dischargeToHousePlan = []
        self.eddiPlan             = []
        self.planUpdateTime       = None
        self.tariffOverrides      = {'gas':    {0.1473675: 0.1031},
                                     'export': {},
                                     'import': {0.6742669499999999: 0.4435, 
                                                0.2551668:          0.1489} }
        # Setup getting the solar forecast data
        solarTodayEntityName    = self.args['solarForecastTodayEntity']
        solarTomorrowEntityName = self.args['solarForecastTomorrowEntity']
        self.rawSolarData.append(self.get_state(solarTodayEntityName,    attribute='forecast'))
        self.rawSolarData.append(self.get_state(solarTomorrowEntityName, attribute='forecast'))
        self.listen_state(self.solarChanged, solarTodayEntityName,    attribute='forecast', kwargs=0) 
        self.listen_state(self.solarChanged, solarTomorrowEntityName, attribute='forecast', kwargs=1)
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
        # Schedule an the output update of the 30 mintues, on the half hour boundary
        now       = datetime.now() 
        startTime = now.replace(minute=0, second=0, microsecond=0) + self.batOutputTimeOffset
        while startTime < now:
            startTime = startTime + timedelta(minutes=30)
        self.run_every(self.updateOutputs, startTime, 30*60)
        

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
            prevMaxChargeCost = max(prevMaxChargeCost, curRrate[2])
        elif gridChargeInfo:
            curRrate          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.importRateData), 0)
            prevMaxChargeCost = max(prevMaxChargeCost, curRrate[2])
        elif houseGridPowerdeInfo:
            curRrate          = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.importRateData), 0)
            prevMaxChargeCost = max(prevMaxChargeCost, curRrate[2] * self.batEfficiency)
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
        self.exportRateData = self.parseRates(new, "export")


    def importRatesChanged(self, entity, attribute, old, new, kwargs):
        self.importRateData = self.parseRates(new, "import")

    
    def parseSolar(self):
        self.log("Updating solar forecast")
        # flatten the forecasts arrays for the different days
        flatForecast = [x for xs in self.rawSolarData for x in xs]        
        powerData    = list(map(lambda x: (datetime.fromisoformat(x['period_end']), 
                                           x['pv_estimate']), 
                                flatForecast))
        powerData.sort(key=lambda x: x[0])
        timeRangePowerData = []
        startTime          = None
        # Reformat the data so we end up with a tuple with elements (startTime, end , power)
        for data in powerData:
            curSampleEndTime = data[0]
            if startTime and startTime != curSampleEndTime:
                timeRangePowerData.append( (startTime, curSampleEndTime, data[1] * self.solarForecastMargin) )
            startTime = curSampleEndTime
        self.printSeries(timeRangePowerData, "Solar forecast")
        self.solarData = timeRangePowerData


    def powerForPeriod(self, data, startTime, endTime):
        power = 0.0
        for forecastPeriod in data:
            forecastStartTime = forecastPeriod[0]
            forecastEndTime   = forecastPeriod[1]
            forecastPower     = forecastPeriod[2]
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
        startTime           = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        startTime           = startTime - timedelta(days=self.usageDaysHistory) 
        self.usageStartTime = startTime
        # Now request the history data. Note: we subtract a further 2 hours from the start time so 
        # we're guaranteed to get data from before the start time we requested
        self.get_history(entity_id  = self.houseLoadEntityName,
                         start_time = startTime - timedelta(hours=2), 
                         callback   = self.usageHistoryCallback)
        
        
    def usageHistoryCallback(self, kwargs):
        powerData = list(map(lambda x: (datetime.fromisoformat(x['last_changed']), 
                                        x['state']), 
                             kwargs["result"][0]))        
        powerData.sort(key=lambda x: x[0])
        timeRangeUsageData = []
        startPower         = None
        startTime          = None
        # Reformat the data so we end up with a tuple with elements (startTime, end , power delta)
        for data in powerData:
            try:
                curSampleEndTime  = data[0]
                curSampleEndPower = float(data[1])
                if startTime:
                    timeRangeUsageData.append( (startTime, curSampleEndTime, curSampleEndPower - startPower) )
                startPower = curSampleEndPower
                startTime  = curSampleEndTime
            except ValueError:
                # just ignore invalid samples
                pass
        
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
                daysDelta = timedelta(days=days)
                avgUsage  = avgUsage + self.powerForPeriod(timeRangeUsageData, 
                                                           forecastUsageStartTime - daysDelta, 
                                                           forecastUsageEndTime   - daysDelta)
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
        for item in series:
            # If we already have an item in the merged list, and the last item of that list 
            # has an end time that matches the start time of the new item. Merge them.
            if mergedSeries and mergedSeries[-1][1] == item[0]:
                mergedSeries[-1] = (mergedSeries[-1][0], item[1], mergedSeries[-1][2] + item[2])
            else:
                mergedSeries.append(item)
        return mergedSeries
        

    def seriesToString(self, series, mergeable=False):
        if mergeable:
            series = self.mergeSeries(series)
        if series and len(series[0]) > 4:
            strings = map(lambda x: "{0:%d %B %H:%M} -> {1:%H:%M} : {2:.3f} {3} {4}".format(*x), series)
        elif series and len(series[0]) > 3:
            strings = map(lambda x: "{0:%d %B %H:%M} -> {1:%H:%M} : {2:.3f} {3}".format(*x), series)
        else:
            strings = map(lambda x: "{0:%d %B %H:%M} -> {1:%H:%M} : {2:.3f}".format(*x), series)
        return "\n".join(strings)


    def printSeries(self, series, title, mergeable=False):
        self.log(title + ":\n" + self.seriesToString(series, mergeable))


    def opOnSeries(self, a, b, operation):
        return list(map(lambda aSample: ( aSample[0], 
                                     aSample[1], 
                                     operation(aSample[2], self.powerForPeriod(b, aSample[0], aSample[1])) ),
                        a))


    def seriesToTariff(self, series, midnight):
        mergedPlan    = self.mergeSeries(series)
        tariff        = list(map(lambda x: [int((x[0] - midnight).total_seconds()),
                                            int((x[1] - midnight).total_seconds())], mergedPlan))
        secondsInADay = 24 * 60 * 60
        return list(filter(lambda x: x[0] < secondsInADay, tariff))


    def mergeAndProcessData(self, now):
        self.log("Updating schedule")        
        # Calculate the solar surplus after house load, we base this on the usage time 
        # series dates as that's typically a finer granularity than the solar forecast. Similarly 
        # we work out the house usage after any forecast solar.
        solarSurplus    = self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, b-a))
        solarUsage      = self.opOnSeries(solarSurplus,   self.solarData, lambda a, b: b-a)
        usageAfterSolar = self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, a-b))
        
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

            
    def calculateEddiPlan(self, exportRateData, solarSurplus, batterSolarChangePlan):
        # Calculate the target rate for the eddi
        eddiPlan          = []
        eddiTargetRate    = self.gasRate / self.gasEfficiency
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
    
    
    def genBatLevelForecast(self, exportRateData, usageAfterSolar, solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, now):
        batForecast      = []
        # For full charge detection we compare against 99% full, this is so any minor changes 
        # is battery capacity or energe when we're basically fully charged, and won't charge 
        # any more, don't cause any problems.
        batFullPct       = min(self.batFullPct, 99)
        batReserveEnergy = self.batteryCapacity * (self.batReservePct / 100)
        batteryRemaining = self.batteryEnergy
        emptyInAnySlot   = False
        fullInAnySlot    = False
        totChargeEnerge  = 0.0
        # The rate data is just used as a basis for the timeline
        for (index, rate) in enumerate(exportRateData):
            chargeEnergy     = (self.powerForPeriod(solarChargingPlan,    rate[0], rate[1]) + 
                                self.powerForPeriod(gridChargingPlan,     rate[0], rate[1]))
            batteryRemaining = (batteryRemaining + chargeEnergy - 
                                self.powerForPeriod(usageAfterSolar,      rate[0], rate[1]) + 
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
            batForecast.append((rate[0], rate[1], batteryRemaining, fullyChanged, empty))
           
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


    def allocateChangingSlots(self, exportRateData, availableChargeRates, availableImportRates, availableHouseGridPoweredRates, solarChargingPlan, 
                              gridChargingPlan, houseGridPoweredPlan, solarSurplus, usageAfterSolar, now, maxHouseGridPoweredRate, topUpToChargeCost = None):
        # We create a local copy of the available rates as there some cases (if there's no solar
        # surplus) where we don't want to remove an entry from the availableChargeRates array, 
        # but we need to remove it locally so we can keep track of which items we've used, and 
        # which are still available
        availableChargeRatesLocal           = list(availableChargeRates)
        availableImportRatesLocal           = list(availableImportRates)
        availableHouseGridPoweredRatesLocal = list(availableHouseGridPoweredRates)
        # Keep producing a battery forecast and adding the cheapest charging slots until the battery is full
        maxChargeCost                       = 0
        (batProfile, fullEndTimeThresh,
         fullyCharged, lastFullSlotEndTime, 
         empty)                             = self.genBatLevelForecast(exportRateData, usageAfterSolar, solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, now)
        # initialise the allow empty before variable to the start of the profile so it has no effect to start with
        allowEmptyBefore                    = batProfile[0][0]
        maxAllowedChargeCost                = topUpToChargeCost if topUpToChargeCost else math.inf
        while empty or topUpToChargeCost or not fullyCharged:
            # If the battery has gone flat during at any point, make sure the charging slot we search for is before the point it went flat
            chargeBefore   = None
            firstEmptySlot = None
            if empty:
                firstEmptySlot = next(filter(lambda x: x[4] and x[0] >= allowEmptyBefore, batProfile), None)
                if firstEmptySlot:
                    firstEmptySlot = firstEmptySlot[1]
                    chargeBefore   = firstEmptySlot
            else:
                # If we're topping up the battery to full, then don't add slots after the full theshold end 
                # time, as they won't actually help meet the full battery criteria.
                chargeBefore = fullEndTimeThresh
            # Search for a charging slot
            (chargeRate, rateId) = self.chooseRate3(availableChargeRatesLocal, availableImportRatesLocal, availableHouseGridPoweredRatesLocal, chargeBefore)                
            if chargeRate:
                timeInSlot = (chargeRate[1] - chargeRate[0]).total_seconds() / (60 * 60)
                # Only allow charging if there's room in the battery for this slot, and its below the max
                # charge cost allowed
                willCharge = (chargeRate[2] <= maxAllowedChargeCost) and not next(filter(lambda x: x[0] == chargeRate[0], batProfile))[3]
                # Don't add any charging slots that are before the last fully charged slot, as it won't help
                # get the battery to fully change at our target time, and it just fills the battery with more 
                # expensive electricity when there's cheaper electriticy available later.
                if lastFullSlotEndTime:
                    willCharge = willCharge and chargeRate[1] >= lastFullSlotEndTime
                if rateId == 0: # solar
                    maxCharge = timeInSlot * self.maxChargeRate
                    power     = self.powerForPeriod(solarSurplus, chargeRate[0], chargeRate[1])
                    # we can only add something to the charge plan if there's surplus solar
                    willCharge = willCharge and power > 0
                    if willCharge:
                        solarChargingPlan.append((chargeRate[0], chargeRate[1], min(power, maxCharge)))
                        # we can only use a charging slot once, so remove it from the available list            
                        availableChargeRates.remove(chargeRate)
                    # We always remove the rate from the local array, otherwise we could end up trying 
                    # to add the same zero power rate again and again. We don't want to remove these rates
                    # from the availableChargeRates as we want these slots to be available outside this 
                    # function for other types of activity
                    availableChargeRatesLocal.remove(chargeRate)
                elif rateId == 1: # grid charge
                    # We don't want to end up charging the battery when its cheaper to just run the house 
                    # directly from the grid. So if the battery is going to be empty, check what the 
                    # electricity import rate is for the slot where it goes empty and compare that to the
                    # cheapest charge rate we've found to determine if we should use this charge rate or not.
                    if firstEmptySlot:
                        emptySlotCost = next(filter(lambda x: x[1] == firstEmptySlot, self.importRateData), None)[2]
                        willCharge    = willCharge and (chargeRate[2] <= emptySlotCost - self.minBuyUseMargin)
                    # We don't want to buy power from the grid if we're going going empty, just to top up the 
                    # battery for the sake of it. So we only allow grid charging to fill the battery if there's
                    # solar slots left that we can export at a higher price than the grid import. Because the
                    # chooseRate3() function will always choose the cheapest slot available. This boils down 
                    # to just checking that there are solar charge slots still available. The exception to this 
                    # is if we've been asked to top up to an explicit charge cost.
                    else:
                        willCharge = willCharge and (availableChargeRatesLocal or topUpToChargeCost)
                    # If the charge slot is still valid, add it to the plan now
                    if willCharge:
                        chargeTaken = timeInSlot * self.batteryGridChargeRate
                        # we can only use a charging slot once, so remove it from the available list
                        availableImportRates.remove(chargeRate)
                        gridChargingPlan.append((chargeRate[0], chargeRate[1], chargeTaken))
                    # Same reason as above, always remove the local charge rate
                    availableImportRatesLocal.remove(chargeRate)
                elif rateId == 2: # house on grid power
                    # Don't run the house on grid power if the slot is the max grid powered price, we might as
                    # well just let the battery go flat, and in some cases due to the margins we wouldn't actually
                    # end up using that much grid power as we'd pre-planned it.
                    willCharge = willCharge and chargeRate[2] < maxHouseGridPoweredRate
                    if willCharge:
                        usage = self.powerForPeriod(usageAfterSolar, chargeRate[0], chargeRate[1])
                        # we can only use a charging slot once, so remove it from the available list
                        availableHouseGridPoweredRates.remove(chargeRate)
                        houseGridPoweredPlan.append((chargeRate[0], chargeRate[1], usage))
                    # Same reason as above, always remove the local charge rate
                    availableHouseGridPoweredRatesLocal.remove(chargeRate)
                    
                if willCharge:
                    maxChargeCost = max(maxChargeCost, chargeRate[2])
                    # update the battery profile based on the new charging plan
                    (batProfile, _, fullyCharged, 
                     lastFullSlotEndTime, empty) = self.genBatLevelForecast(exportRateData, usageAfterSolar, solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, now)   
            elif firstEmptySlot:
                # If the battery gets empty then the code above we restrict the search for a charging 
                # slot to the time before it gets empty. This can result in not finding a charge slot. 
                # In this case we don't terminate the search we just allow the battery to be empty for 
                # that slot and try again to change during a later slot.
                allowEmptyBefore = firstEmptySlot
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
        minImportRate        = min(map(lambda x: x[2], self.importRateData))
        maxImportRate        = max(map(lambda x: x[2], self.importRateData))
        # We create a set of effective "charge" rates associated with not discharging the battery. The 
        # idea is that if we choose not to discharge for a period that's the same as charging the battery 
        # with the same amount of power. It's actually better than this because not cycling the battery
        # means we reduce the battery wear, and don't have the battery efficency overhead. The new rates
        # and calculated at 90% of the cost of the import rates, due to the efficency factor. This also 
        # means we always choose a slot to be grid power the house before we try and charge the battery 
        # during that slot. This is important as we can't grid charge the battery unless we're also grid 
        # powering the house
        availableHouseGridPoweredRates = [(x[0], x[1], x[2] * self.batEfficiency) for x in availableImportRates]
        maxHouseGridPoweredRate        = maxImportRate * self.batEfficiency

        # We don't want to discharge the battery for any slots where the cost of running the house off 
        # the grid is lower than what we've previously paid to charge the battery. So add any grid 
        # powered rates that are below the current charge cost
        maxChargeCost = float(self.get_state(self.prevMaxChargeCostEntity))
        for rate in list(filter(lambda x: x[2] <= maxChargeCost, availableHouseGridPoweredRates)):
            availableHouseGridPoweredRates.remove(rate)
            usage = self.powerForPeriod(usageAfterSolar, rate[0], rate[1])
            houseGridPoweredPlan.append((rate[0], rate[1], usage))

        # calculate the initial charging profile
        (batProfile, _, _, newMaxChargeCost) = self.allocateChangingSlots(exportRateData, availableChargeRates, availableImportRates, availableHouseGridPoweredRates,  
                                                                          solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, solarSurplus, usageAfterSolar, now,
                                                                          maxHouseGridPoweredRate)
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
                newSolarSurplus                   = self.opOnSeries(solarSurplus,    adjustBy, lambda a, b: a+b)
                newUsageAfterSolar                = self.opOnSeries(usageAfterSolar, adjustBy, lambda a, b: a+b)
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
                                                                               now, maxHouseGridPoweredRate)    
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

        # Now allocate any final charge slots topping up the battery as much as possible, but not exceeding
        # the minimum import cost. This means that we'll top up to 100% overright if that's the cheaper option,
        # or if the solar export is a lower cost we'll end up topping up to 100% during the day. This in turn 
        # means we're more likely to be prepared for the next day. EG if we need a higher charge level at the
        # end of the day if we need to make it all the way to the next days solar charge period, or a lower 
        # charge level at the end of the day because we only need to make it to the overright charge period
        # max charge cost we've already established. One usecase for this adding additional night time grid
        # charge slots
        (batProfile, _, _, 
         newMaxChargeCost) = self.allocateChangingSlots(exportRateData, availableChargeRates, availableImportRates, availableHouseGridPoweredRates,  
                                                        solarChargingPlan, gridChargingPlan, houseGridPoweredPlan, solarSurplus, usageAfterSolar, now, 
                                                        maxHouseGridPoweredRate, minImportRate)    
        maxChargeCost      = max(maxChargeCost, newMaxChargeCost)

        soc = (self.batteryEnergy / self.batteryCapacity) * 100
        self.log("Current battery change {0:.3f}".format(soc))
        self.log("Battery change cost {0:.2f}".format(maxChargeCost))
        self.printSeries(batProfile, "Battery profile")
        solarChargingPlan.sort(key=lambda x: x[0])
        gridChargingPlan.sort(key=lambda x: x[0])
        dischargePlan.sort(key=lambda x: x[0])
        houseGridPoweredPlan.sort(key=lambda x: x[0])
        # When calculating the battery profile we allow the "house on frid power" and "grid charging" plans to
        # overlap. However we need to remove this overlap before returning the plan to the caller.
        houseGridPoweredPlan = self.opOnSeries(houseGridPoweredPlan, gridChargingPlan, lambda a, b: 0 if b else a)
        houseGridPoweredPlan = list(filter(lambda x: x[2], houseGridPoweredPlan))
        return (solarChargingPlan, gridChargingPlan, dischargePlan, houseGridPoweredPlan)
    
    
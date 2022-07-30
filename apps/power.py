import hassapi as hass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import re


class PowerControl(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))        
        self.solarForecastMargin          = float(self.args['solarForecastMargin'])
        self.houseLoadEntityName          = self.args['houseLoadEntity']
        self.usageDaysHistory             = self.args['usageDaysHistory']
        self.eddiOutputEntityName         = self.args['eddiOutputEntity']
        self.batteryModeOutputEntityName  = self.args['batteryModeOutputEntity']
        self.batteryPlanSummaryEntityName = self.args['batteryPlanSummaryEntity']
        self.usageMargin                  = float(self.args['houseLoadMargin'])
        self.maxChargeRate                = float(self.args['batteryChargeRateLimit'])
        self.batteryGridChargeRate        = float(self.args['batteryGridChargeRate'])
        self.batReservePct                = float(self.args['batteryReservePercentage'])
        self.gasEfficiency                = float(self.args['gasHotWaterEfficiency'])
        self.eddiTargetPower              = float(self.args['eddiTargetPower'])
        self.eddiPowerLimit               = float(self.args['eddiPowerLimit'])
        self.minBuySelMargin              = float(self.args['minBuySelMargin'])
        self.prevMaxChargeCostEntity      = self.args['batteryChargeCostEntity']
        
        self.solarData            = []
        self.exportRateData       = []
        self.importRateData       = []
        self.usageData            = []
        self.rawSolarData         = []
        self.solarChargingPlan    = []
        self.gridChargingPlan     = []
        self.standbyPlan          = []
        self.dischargePlan        = []
        self.dischargeToHousePlan = []
        self.eddiPlan             = []
        self.planUpdateTime       = None
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
        self.gasRate      = float(self.get_state(gasRateEntityName))
        self.listen_state(self.gasRateChanged, gasRateEntityName) 
        # Schedule an update of the usage forcast every 6 hours
        self.run_every(self.updateUsageHistory, "now", 6*60*60)
        # Schedule an the output update of the 30 mintues, on the half hour boundary
        now       = datetime.now() 
        startTime = now.replace(minute=0, second=0, microsecond=0) 
        while startTime < now:
            startTime = startTime + timedelta(minutes=30)
        self.run_every(self.updateOutputs, startTime, 30*60)
        

    def updateOutputs(self, kwargs):
        self.log("Updating outputs")
        self.mergeAndProcessData()
        # The time 15 minutes in the future (ie the middle of a time slot) to find a 
        # slot that starts now. This avoids any issues with this event firing a little 
        # early / late.
        now             = datetime.now(datetime.now(timezone.utc).astimezone().tzinfo)
        slotMidTime     = now + timedelta(minutes=15)
        dischargeInfo   = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.dischargePlan),    None)
        gridChargeInfo  = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.gridChargingPlan), None)
        standbyInfo     = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.standbyPlan),      None)
        modeInfo        = ("Discharge"    if dischargeInfo   else
                           "Standby"      if standbyInfo     else 
                           "Grid charge"  if gridChargeInfo  else "Solar charge")        
        eddiInfo        = next(filter(lambda x: x[0] < slotMidTime and slotMidTime < x[1], self.eddiPlan), None)
        eddiInfo        = "on" if eddiInfo else "off"
        # generate a summary string for the combined plan
        summary         = ( list(map(lambda x: ("D", x[0]), self.mergeSeries(self.dischargePlan)))     +
                            list(map(lambda x: ("C", x[0]), self.mergeSeries(self.solarChargingPlan))) +
                            list(map(lambda x: ("G", x[0]), self.mergeSeries(self.gridChargingPlan)))  +
                            list(map(lambda x: ("S", x[0]), self.mergeSeries(self.standbyPlan)))       + 
                            list(map(lambda x: ("H", x[0]), self.mergeSeries(self.dischargeToHousePlan))) )
        summary.sort(key=lambda x: x[1])
        summary         = list(map(lambda x: "{0}{1:%H%M}".format(*x)[:-1], summary))
        summary         = ",".join(summary)

        self.set_state(self.batteryPlanSummaryEntityName, state=summary)
        self.set_state(self.batteryModeOutputEntityName, state=modeInfo,      attributes={"planUpdateTime":    self.planUpdateTime,
                                                                                          "stateUpdateTime":   now,
                                                                                          "dischargePlan":     self.seriesToString(self.dischargePlan,     mergeable=True),
                                                                                          "solarChargingPlan": self.seriesToString(self.solarChargingPlan, mergeable=True),
                                                                                          "gridChargingPlan":  self.seriesToString(self.gridChargingPlan,  mergeable=True),
                                                                                          "standbyPlan":       self.seriesToString(self.standbyPlan,       mergeable=True),
                                                                                          "tariff":            self.pwTariff,
                                                                                          "defPrice":          self.defPrice})
        self.set_state(self.eddiOutputEntityName,        state=eddiInfo,      attributes={"planUpdateTime":    self.planUpdateTime,
                                                                                          "stateUpdateTime":   now,
                                                                                          "plan":              self.seriesToString(self.eddiPlan, mergeable=True)})


    def gasRateChanged(self, entity, attribute, old, new, kwargs):
        new = float(new)
        self.log("Gas rate changed {0:.3f} -> {1:.3f}".format(self.gasRate, new))
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
            if startTime:
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
        if series and len(series[0]) > 3:
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


    def mergeAndProcessData(self):
        self.log("Updating schedule")        
        # Calculate the solar surplus after house load, we base this on the usage time 
        # series dates as that's typically a finer granularity than the solar forecast. Similarly 
        # we work out the house usage after any forecast solar.
        solarSurplus    = self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, b-a))
        solarUsage      = self.opOnSeries(solarSurplus,   self.solarData, lambda a, b: b-a)
        usageAfterSolar = self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, a-b))
        
        # Remove rates that are in the past
        now               = datetime.now(datetime.now(timezone.utc).astimezone().tzinfo)
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
        (solarChargingPlan, 
         gridChargingPlan, dischargePlan) = self.calculateChargePlan(exportRateData, importRateData, solarUsage, solarSurplus, usageAfterSolar)
        postBatteryChargeSurplus          = self.opOnSeries(solarSurplus, solarChargingPlan, lambda a, b: a-b)
        # Calculate the times when we want the battery in standby mode. IE when there's solar surplus 
        # but we don't want to charge or discharge.
        standbyPlan = []
        for rate in exportRateData:
            curSolarSurplus =  self.powerForPeriod(solarSurplus,      rate[0], rate[1])
            isCharge        = (self.powerForPeriod(solarChargingPlan, rate[0], rate[1]) > 0 or
                               self.powerForPeriod(gridChargingPlan,  rate[0], rate[1]) > 0)
            isDischarge     =  self.powerForPeriod(dischargePlan,     rate[0], rate[1]) > 0
            if (curSolarSurplus > 0) and not (isCharge or isDischarge): 
                standbyPlan.append((rate[0], rate[1], curSolarSurplus))
        # Create a background plan for info only that shows when we're just powering the house from the battery.
        usageForRateSlotsOnly = self.opOnSeries(exportRateData, self.usageData, lambda a, b: b)
        dischargeToHousePlan  = self.opOnSeries(usageForRateSlotsOnly, solarChargingPlan, lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.opOnSeries(dischargeToHousePlan,  gridChargingPlan,  lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.opOnSeries(dischargeToHousePlan,  standbyPlan,       lambda a, b: 0 if b else a)
        dischargeToHousePlan  = self.opOnSeries(dischargeToHousePlan,  dischargePlan,     lambda a, b: 0 if b else a)
        dischargeToHousePlan  = list(filter(lambda x: x[2], dischargeToHousePlan))

        # Calculate the eddi plan based on any remaining surplus
        eddiPlan = self.calculateEddiPlan(exportRateData, postBatteryChargeSurplus)
        
        # Create a fake tariff with peak time covering the discharge plan
        midnight              = now.replace(hour=0, minute=0, second=0, microsecond=0)
        # Filter out anything except the 2 hours. This prevents the powerwall not behaving properly
        # because it thinks it won't have enough time to charge later.
        tariffEnd             = now + timedelta(hours=2)
        dischargePlanNextHour = list(filter(lambda x: x[1] <= tariffEnd, dischargePlan))
        standbyPlanNextHour   = list(filter(lambda x: x[1] <= tariffEnd, standbyPlan))
        combinedPeakPlan      = sorted(dischargePlanNextHour + standbyPlanNextHour, key=lambda x: x[0])
        combinedPeakPlan      = self.mergeSeries(combinedPeakPlan)
        combinedPeakPeriods   = self.seriesToTariff(combinedPeakPlan, midnight)
        self.defPrice         = "0.10 0.10 OFF_PEAK"
        self.pwTariff         = {"0.90 0.90 ON_PEAK": combinedPeakPeriods}
        self.printSeries(solarChargingPlan,    "Solar charging plan",       mergeable=True)
        self.printSeries(gridChargingPlan,     "Grid charging plan",        mergeable=True)
        self.printSeries(standbyPlan,          "Standby plan",              mergeable=True)
        self.printSeries(dischargePlan,        "Discharging plan",          mergeable=True)
        self.printSeries(dischargeToHousePlan, "Discharging to house plan", mergeable=True)
        self.printSeries(eddiPlan,             "Eddi plan",                 mergeable=True)
        self.solarChargingPlan    = solarChargingPlan
        self.gridChargingPlan     = gridChargingPlan
        self.standbyPlan          = standbyPlan
        self.dischargePlan        = dischargePlan
        self.dischargeToHousePlan = dischargeToHousePlan
        self.eddiPlan             = eddiPlan
        self.planUpdateTime       = now

            
    def calculateEddiPlan(self, exportRateData, solarSurplus):
        # Calculate the target rate for the eddi
        eddiPlan          = []
        eddiTargetRate    = self.gasRate / self.gasEfficiency
        eddiPowerRequired = self.eddiTargetPower
        ratesCheapFirst   = sorted(exportRateData, key=lambda x: x[2])
        for rate in ratesCheapFirst:
            if rate[2] > eddiTargetRate:
                break
            maxPower = ((rate[1] - rate[0]).total_seconds() / (60 * 60)) * self.eddiPowerLimit
            power    = self.powerForPeriod(solarSurplus, rate[0], rate[1])
            if power > 0:
                powerTaken        = min(power, maxPower)
                eddiPowerRequired = eddiPowerRequired - powerTaken
                eddiPlan.append((rate[0], rate[1], powerTaken))
                if eddiPowerRequired < 0:
                    break     
        eddiPlan.sort(key=lambda x: x[0])
        return eddiPlan
    
    
    def genBatLevelForecast(self, exportRateData, usageAfterSolar, solarChargingPlan, gridChargingPlan):
        batForecast      = []            
        batteryRemaining = self.batteryEnergy
        # The rate data is just used as a basis for the timeline
        for (index, rate) in enumerate(exportRateData):
            batteryRemaining = (batteryRemaining - 
                                self.powerForPeriod(usageAfterSolar,   rate[0], rate[1]) + 
                                self.powerForPeriod(solarChargingPlan, rate[0], rate[1]) + 
                                self.powerForPeriod(gridChargingPlan,  rate[0], rate[1]))
            fullyChanged = batteryRemaining >= self.batteryCapacity
            if fullyChanged:
                batteryRemaining = self.batteryCapacity
            batForecast.append((rate[0], rate[1], batteryRemaining, fullyChanged))
           
        # We need to work out if the battery is fully charged in a time slot after 
        # miday on the last day of the forecast
        lastMidday            = batForecast[-1][0].replace(hour=12, minute=0, second=0, microsecond=0)
        fullChargeAfterMidday = any(x[0] >= lastMidday and x[3] for x in batForecast)
        # We also indicate the battery is fully charged if its after midday now, and its currently 
        # fully charged. This prevents an issue where the current time slot is never allowed to 
        # discharge if we don't have a charging period for tomorrow mapped out already
        if not fullChargeAfterMidday:
            now = datetime.now(datetime.now(timezone.utc).astimezone().tzinfo)
            # For full charge detection we compare against 99% full, this is so any minor changes 
            # is battery capacity or energe when we're basically fully charged, and won't charge 
            # any more, don't cause any problems.
            soc = (self.batteryEnergy / self.batteryCapacity) * 100
            if soc > 99 and now >= lastMidday:
                fullChargeAfterMidday = True
        return (batForecast, fullChargeAfterMidday)


    def chooseRate(self, rateA, rateB, notAfterTime=None):
        foundRate = None
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


    def allocateChangingSlots(self, exportRateData, availableChargeRates, availableImportRates, solarChargingPlan, gridChargingPlan, solarSurplus, usageAfterSolar):
        # We create a local copy of the available rates as there some cases (if there's no solar
        # surplus) where we don't want to remove an entry from the availableChargeRates array, 
        # but we need to remove it locally so we can keep track of which items we've used, and 
        # which are still available
        availableChargeRatesLocal = list(availableChargeRates)
        # Walk through the time slots (using the rates as a base timeline) predicting the battery 
        # capacity at the end of each time slot. If we get below the reserve level, add the cheapest 
        # previous rate to the charge plan. This Section basically makes sure we don't flatten the 
        # battery. It doesn't make sure we charge the battery, that comes later.
        batReserveEnergy = self.batteryCapacity * (self.batReservePct / 100)
        batteryRemaining = self.batteryEnergy
        maxChargeCost    = 0
        for rate in exportRateData:
            # Have we got enough energy for this time slot
            usage            = self.powerForPeriod(usageAfterSolar,   rate[0], rate[1])
            charge           = self.powerForPeriod(solarChargingPlan, rate[0], rate[1])
            batteryRemaining = batteryRemaining - usage + charge
            while batteryRemaining <= batReserveEnergy:
                # We need to add a charging slot. This won't select any slots that are in the future
                (chargeRate, isSolarRate) = self.chooseRate(availableChargeRatesLocal, availableImportRates, rate[0])                
                if chargeRate:
                    willCharge = True
                    if isSolarRate:
                        maxCharge  = ((chargeRate[1] - chargeRate[0]).total_seconds() / (60 * 60)) * self.maxChargeRate
                        power      = self.powerForPeriod(solarSurplus, chargeRate[0], chargeRate[1])
                        # We only add a charging slot if there's solar surplus
                        willCharge = power > 0
                        if willCharge:
                            chargeTaken = min(power, maxCharge)
                            # we can only use a charging slot once, so remove it from the available list
                            availableChargeRates.remove(chargeRate)
                            solarChargingPlan.append((chargeRate[0], chargeRate[1], chargeTaken))
                        
                        # We always remove the rate from the local array, otherwise we could end up trying 
                        # to add the same zero power rate again and again. We don't want to remove these rates
                        # from the availableChargeRates as we want these slots to be available outside this 
                        # function for other types of activity
                        availableChargeRatesLocal.remove(chargeRate)
                    else:
                        chargeTaken = ((chargeRate[1] - chargeRate[0]).total_seconds() / (60 * 60)) * self.batteryGridChargeRate
                        # we can only use a charging slot once, so remove it from the available list
                        availableImportRates.remove(chargeRate)
                        gridChargingPlan.append((chargeRate[0], chargeRate[1], chargeTaken))

                    if willCharge:
                        batteryRemaining = batteryRemaining + chargeTaken
                        # Since the charge rates are already sorted in cost order, we know the current 
                        # one we're adding is always the most expensive one so far.
                        maxChargeCost = chargeRate[2]
                else:
                    # no available slots
                    break

        # Now we have a minimum charging plan that'll mean we don't run out, top up the battery with 
        # the cheapest slots we've got left.
        (batProfile, fullyCharged) = self.genBatLevelForecast(exportRateData, usageAfterSolar, solarChargingPlan, gridChargingPlan)
        while not fullyCharged:
            (chargeRate, isSolarRate) = self.chooseRate(availableChargeRatesLocal, availableImportRates)                
            if chargeRate:
                willCharge = True
                if isSolarRate:
                    maxCharge = ((chargeRate[1] - chargeRate[0]).total_seconds() / (60 * 60)) * self.maxChargeRate
                    power     = self.powerForPeriod(solarSurplus, chargeRate[0], chargeRate[1])
                    # we can only add something to the charge plan if there's surplus solar and room in the 
                    # battery during that time slot
                    willCharge = power > 0 and not next(filter(lambda x: x[0] == chargeRate[0], batProfile))[3]
                    if willCharge:
                        solarChargingPlan.append((chargeRate[0], chargeRate[1], min(power, maxCharge)))
                        # we can only use a charging slot once, so remove it from the available list            
                        availableChargeRates.remove(chargeRate)
                    
                    # Same reason as above, always remove the local charge rate
                    availableChargeRatesLocal.remove(chargeRate)
                else:
                    chargeTaken = ((chargeRate[1] - chargeRate[0]).total_seconds() / (60 * 60)) * self.batteryGridChargeRate
                    # we can only use a charging slot once, so remove it from the available list
                    availableImportRates.remove(chargeRate)
                    gridChargingPlan.append((chargeRate[0], chargeRate[1], chargeTaken))

                if willCharge:
                    # Since the charge rates are already sorted in cost order, we know the current 
                    # one we're adding is always the most expensive one so far.
                    maxChargeCost = chargeRate[2]
                    # update the battery profile based on the new charging plan
                    (batProfile, fullyCharged) = self.genBatLevelForecast(exportRateData, usageAfterSolar, solarChargingPlan, gridChargingPlan)   
            else:
                break
        return (batProfile, fullyCharged, maxChargeCost)

    
    def calculateChargePlan(self, exportRateData, importRateData, solarUsage, solarSurplus, usageAfterSolar):        
        solarChargingPlan    = []
        gridChargingPlan     = []
        dischargePlan        = []
        availableChargeRates = sorted(exportRateData, key=lambda x: x[2])
        availableImportRates = sorted(importRateData, key=lambda x: (x[2], x[0]))

        # calculate the initial charging profile
        (batProfile, _, newMaxChargeCost) = self.allocateChangingSlots(exportRateData, availableChargeRates, availableImportRates, 
                                                                       solarChargingPlan, gridChargingPlan, solarSurplus, usageAfterSolar)
        # If we're haven't needed to use any charging slots, then use the previous value for the charging cost
        prevMaxChargeCost = float(self.get_state(self.prevMaxChargeCostEntity))
        maxChargeCost     = newMaxChargeCost if solarChargingPlan else prevMaxChargeCost
        
        # look at the most expensive rate and see if there's solar usage we can flip to battery usage so
        # we can export more. We only do this if we still end up fully charged
        while availableChargeRates:
            mostExpenciveRate = availableChargeRates[-1]
            del availableChargeRates[-1]
            solarUsageForRate = self.powerForPeriod(solarUsage, mostExpenciveRate[0], mostExpenciveRate[1])
            if solarUsageForRate > 0:
                adjustBy                   = [(mostExpenciveRate[0], mostExpenciveRate[1], solarUsageForRate)]
                newSolarSurplus            = self.opOnSeries(solarSurplus,    adjustBy, lambda a, b: a+b)
                newUsageAfterSolar         = self.opOnSeries(usageAfterSolar, adjustBy, lambda a, b: a+b)
                newAvailableChargeRates    = list(availableChargeRates)
                newAvailableImportRates    = list(availableImportRates)
                newSolarChargingPlan       = list(solarChargingPlan)
                newGridChargingPlan        = list(gridChargingPlan)
                (batProfile, fullyCharged, 
                 newMaxChargeCost)         = self.allocateChangingSlots(exportRateData, newAvailableChargeRates, newAvailableImportRates, 
                                                                        newSolarChargingPlan, newGridChargingPlan, newSolarSurplus, newUsageAfterSolar)    
                newMaxChargeCost           = max(maxChargeCost, newMaxChargeCost)
                # If we're still fully charged after swapping a slot to discharging, then make that the plan 
                # of record by updating the arrays. We also skip a potential discharge period if the 
                # difference between the cost of the charge / discharge periods isn't greater than the 
                # threshold. This reduces battery cycling if there's not much to be gained from it.
                if fullyCharged and mostExpenciveRate[2] - newMaxChargeCost > self.minBuySelMargin:
                    maxChargeCost        = newMaxChargeCost
                    dischargePlan.append(adjustBy[0])
                    solarSurplus         = newSolarSurplus         
                    usageAfterSolar      = newUsageAfterSolar     
                    availableChargeRates = newAvailableChargeRates
                    availableImportRates = newAvailableImportRates
                    solarChargingPlan    = newSolarChargingPlan
                    gridChargingPlan     = newGridChargingPlan

        # Update the cost of charging so we have an accurate number next time around
        self.set_state(self.prevMaxChargeCostEntity, state=maxChargeCost)
        soc = (self.batteryEnergy / self.batteryCapacity) * 100
        self.log("Current battery change {0:.3f}".format(soc))
        self.printSeries(batProfile, "Battery profile")
        solarChargingPlan.sort(key=lambda x: x[0])
        gridChargingPlan.sort(key=lambda x: x[0])
        dischargePlan.sort(key=lambda x: x[0])
        return (solarChargingPlan, gridChargingPlan, dischargePlan)
    
    
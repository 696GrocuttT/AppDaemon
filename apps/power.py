import hassapi as hass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import re


class PowerControl(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))        
        self.solarForecastMargin = float(self.args['solarForecastMargin'])
        self.houseLoadEntityName = self.args['houseLoadEntity']
        self.usageDaysHistory    = self.args['usage_days_history']
        self.usageMargin         = float(self.args['houseLoadMargin'])
        self.maxChargeRate       = float(self.args['batteryChargeRateLimit'])
        self.batReservePct       = float(self.args['batteryReservePercentage'])
        self.gasEfficiency       = float(self.args['gasHotWaterEfficiency'])
        self.eddiTargetPower     = float(self.args['eddiTargetPower'])
        self.eddiPowerLimit      = float(self.args['eddiPowerLimit'])
        
        self.solarData       = []
        self.rateData        = []
        self.usageData       = []
        self.rawSolarData    = []
        self.chargingPlan    = []
        self.eddiPlan        = []
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
        self.listen_state(self.ratesChanged, exportRateEntityName, attribute='rates') 
        self.parseRates(rawRateData)
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


    def gasRateChanged(self, entity, attribute, old, new, kwargs):
        new = float(new)
        self.log("Gas rate changed {0:.3f} -> {1:.3f}".format(self.gasRate, new))
        self.gasRate = new
        self.mergeAndProcessData()    
                    
        
    def batteryCapacityChanged(self, entity, attribute, old, new, kwargs):
        new = float(new) / 1000
        # only recalculate everything if there's been a significant change in value
        if abs(self.batteryCapacity - new) > 0.1:
            self.log("Battery capacity changed {0:.3f} -> {1:.3f}".format(self.batteryCapacity, new))
            self.batteryCapacity = new
            self.mergeAndProcessData()        


    def batteryEnergyChanged(self, entity, attribute, old, new, kwargs):
        new = float(new) / 1000
        # only recalculate everything if there's been a significant change in value
        if abs(self.batteryEnergy - new) > 0.1:
            self.log("Battery energy changed {0:.3f} -> {1:.3f}".format(self.batteryEnergy, new))
            self.batteryEnergy = new
            self.mergeAndProcessData()
        
        
    def solarChanged(self, entity, attribute, old, new, kwargs):
        index                    = kwargs['kwargs']
        self.rawSolarData[index] = new
        self.parseSolar()
        self.mergeAndProcessData()

    
    def ratesChanged(self, entity, attribute, old, new, kwargs):
        self.parseRates(new)
        self.mergeAndProcessData()

    
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
        

    def parseRates(self, rawRateData):
        self.log("Updating tariff rates")
        rateData = list(map(lambda x: (datetime.fromisoformat(x['from']).astimezone(),
                                       datetime.fromisoformat(x['to']).astimezone(), 
                                       x['rate']/100), 
                            rawRateData))
        rateData.sort(key=lambda x: x[0])        
        self.rateData = rateData        


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
        self.usageData = forecastUsage
        # process the update
        self.mergeAndProcessData()


    def printSeries(self, series, title, mergeable=False):
        if mergeable:
            mergedSeries = []
            for item in series:
                # If we already have an item in the merged list, and the last item of that list 
                # has an end time that matches the start time of the new item. Merge them.
                if mergedSeries and mergedSeries[-1][1] == item[0]:
                    mergedSeries[-1] = (mergedSeries[-1][0], item[1], mergedSeries[-1][2] + item[2])
                else:
                    mergedSeries.append(item)
            series = mergedSeries    
        strings = map(lambda x: "{0:%d %B %H:%M} -> {1:%H:%M} : {2:.3f}".format(*x), series)
        self.log(title + ":\n" + "\n".join(strings))


    def opOnSeries(self, a, b, operation):
        return list(map(lambda aSample: ( aSample[0], 
                                     aSample[1], 
                                     operation(aSample[2], self.powerForPeriod(b, aSample[0], aSample[1])) ),
                                             
                        a))


    def mergeAndProcessData(self):
        self.log("Updating schedule")        
        # Calculate the solar surplus after house load, we base this on the usage time 
        # series dates as that's typically a finer granularity than the solar forecast. Similarly 
        # we work out the house usage after any forecast solar.
        solarSurplus    = self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, b-a))
        solarUsage      = self.opOnSeries(solarSurplus,   self.solarData, lambda a, b: b-a)
        usageAfterSolar = self.opOnSeries(self.usageData, self.solarData, lambda a, b: max(0, a-b))
        
        # Remove rates that are in the past
        now             = datetime.now(datetime.now(timezone.utc).astimezone().tzinfo)
        rateData        = list(filter(lambda x: x[1] >= now, self.rateData))                            
        
        # calculate the charge plan, and work out what's left afterwards
        (chargingPlan, dischargePlan) = self.calculateChargePlan(rateData, solarUsage, solarSurplus, usageAfterSolar)
        postBatteryChargeSurplus      = self.opOnSeries(solarSurplus, chargingPlan, lambda a, b: a-b)
        
        # Calculate the eddi plan based on any remaining surplus
        eddiPlan = self.calculateEddiPlan(rateData, postBatteryChargeSurplus)
        
        self.printSeries(chargingPlan,  "Charging plan",    mergeable=True)
        self.printSeries(dischargePlan, "Discharging plan", mergeable=True)
        self.printSeries(eddiPlan,      "Eddi plan",        mergeable=True)
        self.chargingPlan = chargingPlan
        self.eddiPlan     = eddiPlan
            
            
    def calculateEddiPlan(self, rateData, solarSurplus):
        # Calculate the target rate for the eddi
        eddiPlan          = []
        eddiTargetRate    = self.gasRate / self.gasEfficiency
        eddiPowerRequired = self.eddiTargetPower
        ratesCheapFirst   = sorted(rateData, key=lambda x: x[2])
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
    
    
    def genBatLevelForecast(self, rateData, usageAfterSolar, chargingPlan):
        batForecast      = []            
        batteryRemaining = self.batteryEnergy
        # The rate data is just used as a basis for the timeline
        for (index, rate) in enumerate(rateData):
            batteryRemaining = (batteryRemaining - 
                                self.powerForPeriod(usageAfterSolar, rate[0], rate[1]) + 
                                self.powerForPeriod(chargingPlan,    rate[0], rate[1]))
            fullyChanged = batteryRemaining >= self.batteryCapacity
            if fullyChanged:
                batteryRemaining = self.batteryCapacity
            batForecast.append((rate[0], rate[1], batteryRemaining, fullyChanged))
           
        # We need to work out if the battery is fully charged in a time slot after 
        # miday on the last day of the forecast
        lastMidday            = batForecast[-1][0].replace(hour=12, minute=0, second=0, microsecond=0)
        fullChargeAfterMidday = any(x[0] >= lastMidday and x[3] for x in batForecast)
        
        self.printSeries(batForecast,  "bat profile plan"   )
        return (batForecast, fullChargeAfterMidday)
    
    
    def allocateChangingSlots(self, rateData, availableChargeRates, chargingPlan, solarSurplus, usageAfterSolar):
        # Walk through the time slots (using the rates as a base timeline) predicting the battery 
        # capacity at the end of each time slot. If we get below the reserve level, add the cheapest 
        # previous rate to the charge plan. This Section basically makes sure we don't flatten the 
        # battery. It doesn't make sure we charge the battery, that comes later.
        batReserveEnergy = self.batteryCapacity * (self.batReservePct / 100)
        batteryRemaining = self.batteryEnergy
        for rate in rateData:
            # Have we got enough energy for this time slot
            usage            = self.powerForPeriod(usageAfterSolar, rate[0], rate[1])
            charge           = self.powerForPeriod(chargingPlan,    rate[0], rate[1])
            batteryRemaining = batteryRemaining - usage + charge
            if batteryRemaining <= batReserveEnergy:                
                # We need to add a charging slot, NOTE: we create a list from the existing 
                # availableChargeRates list so we can do concurent modification
                for (index, chargeRate) in enumerate(list(availableChargeRates)):
                    maxCharge = ((chargeRate[1] - chargeRate[0]).total_seconds() / (60 * 60)) * self.maxChargeRate
                    power     = self.powerForPeriod(solarSurplus, chargeRate[0], chargeRate[1])
                    # We only add a charging slot if there's solar surplus and the slot isn't in the
                    # future
                    if power > 0 and chargeRate[0] <= rate[0]:
                        chargeTaken      = min(power, maxCharge)
                        batteryRemaining = batteryRemaining + chargeTaken
                        # we can only use a charging slot once, so remove it from the available list
                        availableChargeRates.remove(chargeRate)
                        chargingPlan.append((chargeRate[0], chargeRate[1], chargeTaken))
                        if batteryRemaining > batReserveEnergy:
                            break    

        # Now we have a minimum charging plan that'll mean we don't run out, top up the battery with 
        # the cheapest slots we've got left. NOTE: We create a list from the availableChargeRates list 
        # so we don't get problems with concurent modification when we delete used items from the list.
        (batProfile, fullyCharged) = self.genBatLevelForecast(rateData, usageAfterSolar, chargingPlan)
        for (index, chargeRate) in enumerate(list(availableChargeRates)):
            if fullyCharged:
                break
            else:
                maxCharge = ((chargeRate[1] - chargeRate[0]).total_seconds() / (60 * 60)) * self.maxChargeRate
                power     = self.powerForPeriod(solarSurplus, chargeRate[0], chargeRate[1])
                # we can only add something to the charge plan if there's surplus solar and room in the 
                # battery during that time slot
                if power > 0 and not next(filter(lambda x: x[0] == chargeRate[0], batProfile))[3]:
                    chargingPlan.append((chargeRate[0], chargeRate[1], min(power, maxCharge)))
                    # we can only use a charging slot once, so remove it from the available list            
                    availableChargeRates.remove(chargeRate)
                    # update the battery profile based on the new charging plan
                    (batProfile, fullyCharged) = self.genBatLevelForecast(rateData, usageAfterSolar, chargingPlan)   
        return fullyCharged

    
    def calculateChargePlan(self, rateData, solarUsage, solarSurplus, usageAfterSolar):        
        chargingPlan         = []
        dischargePlan        = []
        availableChargeRates = sorted(rateData, key=lambda x: x[2])
        
        # calculate the initial charging profile
        self.allocateChangingSlots(rateData, availableChargeRates, chargingPlan, solarSurplus, usageAfterSolar)
        
        # look at the most expensive rate and see if there's solar usage we can flip to battery usage so
        # we can export more. We only do this if we still end up fully charged
        while availableChargeRates:
            mostExpenciveRate = availableChargeRates[-1]
            del availableChargeRates[-1]
            solarUsageForRate = self.powerForPeriod(solarUsage, mostExpenciveRate[0], mostExpenciveRate[1])
            if solarUsageForRate > 0:
                adjustBy                = [(mostExpenciveRate[0], mostExpenciveRate[1], solarUsageForRate)]
                newSolarSurplus         = self.opOnSeries(solarSurplus,    adjustBy, lambda a, b: a+b)
                newUsageAfterSolar      = self.opOnSeries(usageAfterSolar, adjustBy, lambda a, b: a+b)
                newAvailableChargeRates = list(availableChargeRates)
                newChargingPlan         = list(chargingPlan)
                fullyCharged            = self.allocateChangingSlots(rateData, newAvailableChargeRates, newChargingPlan, newSolarSurplus, newUsageAfterSolar)    
                # If we're still fully charged after swapping a slot to discharging, then make that the plan 
                # of record by updating the arrays
                if fullyCharged:
                    dischargePlan.append(mostExpenciveRate)
                    solarSurplus         = newSolarSurplus         
                    usageAfterSolar      = newUsageAfterSolar     
                    availableChargeRates = newAvailableChargeRates
                    chargingPlan         = newChargingPlan
                    
        chargingPlan.sort(key=lambda x: x[0])
        dischargePlan.sort(key=lambda x: x[0])
        return (chargingPlan, dischargePlan)
    
    
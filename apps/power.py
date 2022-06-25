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


    def mergeAndProcessData(self):
        self.log("Updating schedule")        
        # Calculate the solar surplus after house load, we base this on the usage time 
        # series dates as that's typically a finer granularity than the solar forecast.
        solarSurplus = map(lambda usage: (usage[0], 
                                          usage[1], 
                                          max(0, self.powerForPeriod(self.solarData, usage[0] , usage[1]) - usage[2])),
                           self.usageData)        
        solarSurplus = list(filter(lambda x: x[2] > 0, solarSurplus))
        
        # Remove rates that are in the past
        now      = datetime.now(datetime.now(timezone.utc).astimezone().tzinfo)
        rateData = list(filter(lambda x: x[1] >= now, self.rateData))                            
        
        # Sort the rate time slots by price, and then work out which ones we should 
        # use to change the battery.
        chargingPlan    = []
        ratesCheapFirst = sorted(rateData, key=lambda x: x[2])
        chargeRequired  = self.batteryCapacity - self.batteryEnergy
        for rate in ratesCheapFirst:
            maxCharge = ((rate[1] - rate[0]).total_seconds() / (60 * 60)) * self.maxChargeRate
            power     = self.powerForPeriod(solarSurplus, rate[0], rate[1])
            if power > 0:
                chargeTaken    = min(power, maxCharge)
                chargeRequired = chargeRequired - chargeTaken
                chargingPlan.append((rate[0], rate[1], chargeTaken))
                if chargeRequired < 0:
                    break
        chargingPlan.sort(key=lambda x: x[0])
            
        # calculate the surplus after battery charging    
        postBatteryChargeSurplus = map(lambda surplus: (surplus[0], 
                                                        surplus[1], 
                                                        surplus[2] - self.powerForPeriod(chargingPlan, surplus[0], surplus[1])),
                                       solarSurplus)  
        postBatteryChargeSurplus = list(filter(lambda x: x[2] > 0, postBatteryChargeSurplus))              
        
        # Calculate the target rate for the eddi
        eddiPlan          = []
        eddiTargetRate    = self.gasRate / self.gasEfficiency
        eddiPowerRequired = self.eddiTargetPower
        for rate in ratesCheapFirst:
            if rate[2] > eddiTargetRate:
                break
            maxPower = ((rate[1] - rate[0]).total_seconds() / (60 * 60)) * self.eddiPowerLimit
            power    = self.powerForPeriod(postBatteryChargeSurplus, rate[0], rate[1])
            if power > 0:
                powerTaken        = min(power, maxPower)
                eddiPowerRequired = eddiPowerRequired - powerTaken
                eddiPlan.append((rate[0], rate[1], powerTaken))
                if eddiPowerRequired < 0:
                    break     
        eddiPlan.sort(key=lambda x: x[0])
            
        self.printSeries(chargingPlan, "Charging plan", mergeable=True)
        self.printSeries(eddiPlan,     "Eddi plan",     mergeable=True)
        self.chargingPlan = chargingPlan
        self.eddiPlan     = eddiPlan
            
            

import hassapi as hass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import re


class PowerControl(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))
        self.houseLoadEntityName = self.args['houseLoadEntity']
        self.usageDaysHistory    = self.args['usage_days_history']

        self.solarData     = []
        self.rateData      = []
        self.forecastUsage = []
        # Setup getting the solar forecast data
        forecastEntityName = self.args['solarForecastTodayEntity']
        rawForecastData    = self.get_state(forecastEntityName, attribute='forecast')
        self.listen_state(self.forecast_changed, forecastEntityName, attribute='forecast') 
        self.parseForecast(rawForecastData)
        # Setup getting the export rates
        exportRateEntityName = self.args['exportRateEntity']
        rawRateData          = self.get_state(exportRateEntityName, attribute='rates')
        self.listen_state(self.rates_changed, exportRateEntityName, attribute='rates') 
        self.parseRates(rawRateData)
        # Schedule an update of the usage forcast every 6 hours
        self.run_every(self.updateUsageHistory, "now", 6*60*60)
        
        
    def forecast_changed(self, entity, attribute, old, new, kwargs):
        self.parseForecast(new)
        self.mergeAndProcessData()

    
    def rates_changed(self, entity, attribute, old, new, kwargs):
        self.parseRates(new)
        self.mergeAndProcessData()

    
    def parseForecast(self, rawForecastData):
        self.log("Updating solar forecast")
        powerData = list(map(lambda x: (datetime.fromisoformat(x['period_end']), 
                                        x['pv_estimate']), 
                             rawForecastData))
        powerData.sort(key=lambda x: x[0])
        timeRangePowerData = []
        startTime          = None
        # Reformat the data so we end up with a tuple with elements (startTime, end , power)
        for data in powerData:
            curSampleEndTime = data[0]
            if startTime:
                timeRangePowerData.append( (startTime, curSampleEndTime, data[1]) )
            startTime = curSampleEndTime
        self.solarData = timeRangePowerData


    def powerForPeriod(self, data, startTime, endTime):
        power = 0
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
        rateData = list(map(lambda x: (datetime.fromisoformat(x['from']),
                                       datetime.fromisoformat(x['to']), 
                                       x['rate']), 
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
            avgUsage = avgUsage / self.usageDaysHistory
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
        self.forecastUsage = forecastUsage
        # process the update
        self.mergeAndProcessData()


    def printSeries(self, series, title):    
        strings = map(lambda x: "{0:%d %B %H:%M} -> {1:%H:%M} : {2:.2}".format(*x), series)
        self.log(title + ":\n" + "\n".join(strings))


    def mergeAndProcessData(self):
        self.log("Updating schedule")
        self.printSeries(self.forecastUsage, "usage")
        for data in self.rateData:
            power = self.powerForPeriod(self.solarData, data[0] , data[1])
            #self.log(str(power) + " " + str(data[0]) + "   " +str(data[1]))


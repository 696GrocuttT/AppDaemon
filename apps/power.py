import hassapi as hass
from datetime import datetime
from datetime import timedelta
import re


class PowerControl(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))

        self.solarData = []
        self.rateData  = []
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
        
        # Process the data we've just fetched
        self.mergeAndProcessRatesAndForecast()
        
        
    def forecast_changed(self, entity, attribute, old, new, kwargs):
        self.parseForecast(new)
        self.mergeAndProcessRatesAndForecast()

    
    def rates_changed(self, entity, attribute, old, new, kwargs):
        self.parseRates(new)
        self.mergeAndProcessRatesAndForecast()

    
    def parseForecast(self, rawForecastData):
        powerData = list(map(lambda x: (x['pv_estimate'], 
                                   datetime.fromisoformat(x['period_end'])), 
                             rawForecastData))
        powerData.sort(key=lambda x: x[1])
        timeRangePowerData = []
        startTime          = None
        # Reformat the data so we end up with a tuple with elements (startTime, end , power)
        for data in powerData:
            curSampleEndTime = data[1]
            if startTime:
                timeRangePowerData.append( (startTime, curSampleEndTime, data[0]) )
            startTime = curSampleEndTime
        self.solarData = timeRangePowerData


    def forecastPowerForPeriod(self, startTime, endTime):
        power = 0
        for forecastPeriod in self.solarData:
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
        rateData = list(map(lambda x: (datetime.fromisoformat(x['from']),
                                       datetime.fromisoformat(x['to']), 
                                       x['rate']), 
                            rawRateData))
        rateData.sort(key=lambda x: x[0])
        self.rateData = rateData        


    def mergeAndProcessRatesAndForecast(self):
        for data in self.rateData:
            self.log(" ")
            power = self.forecastPowerForPeriod(data[0] , data[1])
            self.log(str(power) + " " + str(data[0]) + "   " +str(data[1]))

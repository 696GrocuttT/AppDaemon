import hassapi as hass
from datetime import datetime
from datetime import timedelta
import re


class WeatherMonitor(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))
        entityBaseNames = ['weather', 'probability_of_precipitation', 'visibility_distance',
                           'wind_direction', 'wind_gust', 'wind_speed',
                           'temperature', 'humidity']
        self.outputEntity  = self.args['outputEntity']
        self.locationsDict = {}
        for location in self.args['locations']:
            entityNameDict  = {}
            entityValueDict = {}
            title           = location['title']
            prefix          = location.get('entityPrefix', '')
            suffix          = location.get('entitySuffix', '')
            for baseName in entityBaseNames:
                entityName = location.get(baseName, prefix + baseName + suffix)
                # get the initial val and register the listener
                units = self.get_state(entityName, attribute='unit_of_measurement')
                if not units:
                    units = ''
                entityNameDict[entityName] = baseName
                entityValueDict[baseName]  = {'value': self.get_state(entityName),
                                              'units': units}
                self.listen_state(self.state_changed, entityName, kwargs=title)
            self.locationsDict[title] = {'nameDict':  entityNameDict,
                                         'valueDict': entityValueDict,
                                         'outputStr': ''}
            self.updateOutputStr(title)
        

    def state_changed(self, entity, attribute, old, new, kwargs):
        title    = kwargs['kwargs']
        baseName = self.locationsDict[title]['nameDict'][entity]
        self.locationsDict[title]['valueDict'][baseName]['value'] = new
        self.updateOutputStr(title)

    
    def itemStr(self, values, key, forceUnits=None, scale=1):
        data  = values[key]
        value = data['value']
        try:
            value = str(int((float(value) * scale) + 0.5))
        except ValueError:
            pass
        if value.islower():
            value = value.title()
        units = forceUnits if forceUnits != None else data['units']
        space = ''
        if units:
            if units[0].isalpha():
                space = ' '
        return f'{value}{space}{units}'
    
    
    def updateOutputStr(self, title):
        values = self.locationsDict[title]['valueDict']
        output = []
        output.append(self.itemStr(values, 'weather'))
        output.append("Rain: " + self.itemStr(values, 'probability_of_precipitation') + 
                      " Vis: " + self.itemStr(values, 'visibility_distance'))
        output.append("Temp: " + self.itemStr(values, 'temperature') +
                      " Hum: " + self.itemStr(values, 'humidity')) 
        output.append("Wind: " + self.itemStr(values, 'wind_direction')           + 
                      " @ "    + self.itemStr(values, 'wind_speed', scale = 0.868976, forceUnits='') +
                      "/"      + self.itemStr(values, 'wind_gust',  scale = 0.868976, forceUnits='kts'))
        outputStr = title + "\\0" + "\\n".join(output)
        outputStr = outputStr.replace('\xb0C', ' C')
        self.log(outputStr)
        self.locationsDict[title]['outputStr'] = outputStr
        locationsStrs = '\\0'.join(map(lambda x: x[1]['outputStr'], self.locationsDict.items()))
        self.set_state(self.outputEntity, state=locationsStrs[0:255], attributes={"fullText": locationsStrs})


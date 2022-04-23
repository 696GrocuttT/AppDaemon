import hassapi as hass
from datetime import datetime
from datetime import timedelta
import re


class SystemMonitor(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))
        monEntities       = self.args["entities"]
        self.outputEntity = self.args['outputEntity']
        if not "log_level" in self.args:
            self.set_log_level("WARNING")

        # Go through the list of entity types to listen for, setting up the listeners and 
        # the data structures to go with them
        self.monDict = {}
        for monEntity in monEntities:
            attribCond       = monEntity.get('attributeCond', {})
            nameRegex        = re.compile(monEntity['nameRegex'])
            entityRegex      = re.compile(monEntity['entityRegex'])
            matchingEntities = filter(entityRegex.match, self.get_state())
            for entity in matchingEntities:
                # check if there's any other conditions
                attributesMatch = True
                for (key, expValue) in attribCond.items():
                    curEntityObj = self.get_entity(entity)
                    if expValue != curEntityObj.get_state(attribute=key):
                        attributesMatch = False
                
                if attributesMatch:
                    frendlyName = self.friendly_name(entity)
                    nameMatch   = nameRegex.search(frendlyName)
                    if nameMatch:
                        frendlyName = nameMatch.group(1)
                    self.monDict[entity] = {"value": self.get_state(entity),
                                            "name":  frendlyName}
                    for cfgItem in ["triggerValue", "message", "priority"]:
                        self.monDict[entity][cfgItem] = monEntity[cfgItem]
                    self.listen_state(self.state_changed, entity)
        self.update_warning_strings()
        

    def state_changed(self, entity, attribute, old, new, kwargs):
        self.monDict[entity]["value"] = new
        self.update_warning_strings()


    def update_warning_strings(self):
        messages = {}
        for (key, entityDict) in self.monDict.items():
            if entityDict["value"] == entityDict["triggerValue"]:
                message = entityDict["message"].replace("%name%", entityDict["name"])
                messages[message] = entityDict["priority"]
        messages    = sorted(messages.items(), key=lambda item: item[1], reverse=True)
        renderedTxt =  "\\n".join(map(lambda x: x[0] , messages))
        self.set_state(self.outputEntity, state=renderedTxt[0:255], attributes={"fullText": renderedTxt})
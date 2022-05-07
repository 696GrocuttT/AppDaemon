import hassapi as hass
from datetime import datetime
from datetime import timedelta
import re


class SystemMonitor(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))
        monEntities       = self.args["entities"]
        self.outputEntity = self.args['outputEntity']
        self.alertEntity  = self.args.get("alertEntity", None)
        if not "log_level" in self.args:
            self.set_log_level("WARNING")

        # Go through the list of entity types to listen for, setting up the listeners and 
        # the data structures to go with them
        self.alertLevel = 0
        self.monList    = []
        for monEntity in monEntities:
            attribCond       = monEntity.get('attributeCond', {})
            attribName       = monEntity.get('attributeName', None)
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
                    invertTrigger = monEntity.get('invertTrigger', False)
                    monDict = {"value":         self.get_state(entity, attribute=attribName),
                               "name":          frendlyName,
                               "invertTrigger": invertTrigger}
                    for cfgItem in ["triggerValue", "message", "priority"]:
                        monDict[cfgItem] = monEntity[cfgItem]
                    self.monList.append(monDict)
                    # check if this is a simple state trigger, or a duration trigger
                    duration = monEntity.get('duration', None)
                    if duration:
                        value = monEntity["triggerValue"]
                        if invertTrigger:
                            self.listen_state(self.state_changed, entity, attribute=attribName, 
                                              duration=duration, old=value, kwargs=len(self.monList)-1)
                            self.listen_state(self.state_changed, entity, attribute=attribName, 
                                              new=value, kwargs=len(self.monList)-1)
                        else:
                            self.listen_state(self.state_changed, entity, attribute=attribName, 
                                              duration=duration, new=value, kwargs=len(self.monList)-1)
                            self.listen_state(self.state_changed, entity, attribute=attribName, 
                                              old=value, kwargs=len(self.monList)-1)
                    else:
                        self.listen_state(self.state_changed, entity, attribute=attribName, kwargs=len(self.monList)-1)
        self.update_warning_strings()
    

    def state_changed(self, entity, attribute, old, new, kwargs):
        index                        = kwargs['kwargs']
        self.monList[index]["value"] = new
        self.update_warning_strings()


    def update_warning_strings(self):
        messages      = {}
        curAlertLevel = 0
        for entityDict in self.monList:
            valueMatch = entityDict["value"] == entityDict["triggerValue"]
            if valueMatch != entityDict["invertTrigger"]:
                message           = entityDict["message"].replace("%name%", entityDict["name"])
                priority          = entityDict["priority"]
                messages[message] = priority
                if priority > curAlertLevel:
                    curAlertLevel = priority
        messages    = sorted(messages.items(), key=lambda item: item[1], reverse=True)
        renderedTxt =  "\\n".join(map(lambda x: x[0] , messages))
        self.set_state(self.outputEntity, state=renderedTxt[0:255], attributes={"fullText": renderedTxt})
        if self.alertEntity:
            alert = curAlertLevel > 5
            if (curAlertLevel > self.alertLevel) or not alert:
                self.set_state(self.alertEntity, state=("on" if alert else "off"))
        # update the alert level for next time
        self.alertLevel = curAlertLevel
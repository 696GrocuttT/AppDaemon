import hassapi as hass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from dateutil import tz
from core.powerUtils import PowerUtils
import math



class CheapestTime(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))
        self.utils                   = PowerUtils(self.log) 
        batteryPlanSummaryEntityName = self.args['batteryPlanSummaryEntity']
        self.startTimeEntityName     = self.args['startTimeEntity']
        self.programTime             = None
        self.intReadyFlag            = True
        
        # Setup getting the rates, profiles, and charge cost
        for listName in ["import", "export"]:
            attributeName = listName + "Rates"
            self.rateChanged(None, None, None, self.get_state(batteryPlanSummaryEntityName,   attribute=attributeName), {'kwargs': listName})
            self.listen_state(self.rateChanged, batteryPlanSummaryEntityName,  attribute=attributeName, kwargs=listName)
            attributeName = listName + "Profile"
            self.profileChanged(None, None, None, self.get_state(batteryPlanSummaryEntityName,   attribute=attributeName), {'kwargs': listName})
            self.listen_state(self.profileChanged, batteryPlanSummaryEntityName,  attribute=attributeName, kwargs=listName) 
        self.chargeCostChanged(None, None, None, self.get_state(batteryPlanSummaryEntityName, attribute="maxChargeCost"), None)
        self.listen_state(self.chargeCostChanged, batteryPlanSummaryEntityName, attribute="maxChargeCost") 
        # listen for the condition entities
        self.conditions = self.args.get('conditions', [])
        for (index, condition) in enumerate(self.conditions):
            entityName = condition['entity']
            self.conditionChanged(None, None, None, self.get_state(entityName), {'kwargs': index})
            self.listen_state(self.conditionChanged, entityName, kwargs=index)
        # Listen for ready set/clear events
        clearReadyEntityName = self.args.get('clearReadyEntity', None)
        if clearReadyEntityName:
            setReadyEntityName   = self.args['setReadyEntity']        
            self.clearReadyValue = self.args['clearReadyValue']
            self.setReadyValue   = self.args['setReadyValue']        
            self.clearReadyChanged(None, None, None, self.get_state(clearReadyEntityName), None)
            self.listen_state(self.clearReadyChanged, clearReadyEntityName)        
            self.setReadyChanged(None, None, None, self.get_state(setReadyEntityName), None)
            self.listen_state(self.setReadyChanged, setReadyEntityName)
        # listen for finish by info 
        finishByOnEntityName = self.args.get('finishByOnEntity',   None)
        if finishByOnEntityName:    
            finishByOn = self.get_state(finishByOnEntityName)
            self.listen_state(self.finishByOnChanged, finishByOnEntityName)
            finishByTimeEntityName  = self.args.get('finishByTimeEntity', None)
            self.finishByTimeChanged(None, None, None, self.get_state(finishByTimeEntityName), None)
            self.listen_state(self.finishByTimeChanged, finishByTimeEntityName)
        else:
            finishByOn = False
        self.finishByOnChanged(None, None, None, finishByOn, None)
        # Get the time required for the program to run
        programTimeEntityName = self.args['programTimeEntity']
        self.programTimeChanged(None, None, None, self.get_state(programTimeEntityName), None)
        self.listen_state(self.programTimeChanged, programTimeEntityName) 
        # Set the default state to an empty string so we've always got something
        self.set_state(self.startTimeEntityName, state="", attributes={"display": None})
        # Now we have all the data do an initial plan
        self.createPlan(None)
        # Schedule future updates every 30 minutes to align with rate charges
        now       = datetime.now() 
        period    = timedelta(minutes=30)
        startTime = now.replace(minute=0, second=0, microsecond=0) 
        while startTime < now:
            startTime = startTime + period
        self.run_every(self.createPlan, startTime, 30*60)


    def finishByOnChanged(self, entity, attribute, old, new, kwargs):
        self.finishByOn = new == "on"
        self.log("finish by enabled " + str(self.finishByOn))
        # Update the plan immediatly as this notification is in responce to a user action
        self.createPlan(None)


    def finishByTimeChanged(self, entity, attribute, old, new, kwargs):
        finishBySplit = new.split(':')
        now           = datetime.now(tz.gettz())
        finishByTime  = now.replace(hour        = int(finishBySplit[0]), 
                                    minute      = int(finishBySplit[1]), 
                                    second      = int(finishBySplit[2]),
                                    microsecond = 0)
        if finishByTime < now:
            finishByTime = finishByTime + timedelta(days=1)
        self.finishByTime = finishByTime
        self.log("finish by " + str(self.finishByTime))
        # Update the plan immediatly as this notification is in responce to a user action
        self.createPlan(None)


    def clearReadyChanged(self, entity, attribute, old, new, kwargs):
        if self.clearReadyValue == new:
            self.intReadyFlag = False
            self.log("clear ready")
            # Update the plan immediatly as this notification is in responce to a user action
            self.createPlan(None)


    def setReadyChanged(self, entity, attribute, old, new, kwargs):
        if self.setReadyValue == new:
            self.intReadyFlag = True
            self.log("set ready")
            # Update the plan immediatly as this notification is in responce to a user action
            self.createPlan(None)


    def conditionChanged(self, entity, attribute, old, new, kwargs):
        index = kwargs['kwargs']
        self.conditions[index]['curValue'] = new
        # Update the plan immediatly as this notification is in responce to a user action
        self.createPlan(None)


    def programTimeChanged(self, entity, attribute, old, new, kwargs):
        splitTimeStr = new.split(':')
        if len(splitTimeStr) == 2:
            self.programTime = timedelta(hours=int(splitTimeStr[0]), minutes=int(splitTimeStr[1]))
        else:
            self.programTime = None
        self.log("program length changed " + str(self.programTime))
        # Update the plan immediatly as this notification is in responce to a user action
        self.createPlan(None)
        

    def chargeCostChanged(self, entity, attribute, old, new, kwargs):
        self.maxChargeCost = new
    
    
    def rateChanged(self, entity, attribute, old, new, kwargs):
        rateName = kwargs['kwargs']
        rateData = list(map(lambda x: (datetime.fromisoformat(x['start']).astimezone(),
                                       datetime.fromisoformat(x['end']).astimezone(), 
                                       x['rate']), 
                            new))
        rateData.sort(key=lambda x: x[0])    
        self.utils.printSeries(rateData, "Rate data (" + rateName + ")", level="DEBUG")
        if rateName == "import":
            self.importRateData = rateData
        else:
            self.exportRateData = rateData


    def profileChanged(self, entity, attribute, old, new, kwargs):
        profileName = kwargs['kwargs']
        profileData = list(map(lambda x: (datetime.fromisoformat(x['start']).astimezone(),
                                          datetime.fromisoformat(x['end']).astimezone(), 
                                          x['energy']), 
                               new))
        profileData.sort(key=lambda x: x[0])    
        self.utils.printSeries(profileData, "Profile data (" + profileName + ")", level="DEBUG")
        if profileName == "import":
            self.importProfile = profileData
        else:
            self.exportProfile = profileData


    def createPlan(self, kwargs):
        # We only plan if there's a valid program time
        bestCost   = math.inf
        bestPlan   = []
        attributes = {'intReadyFlag': self.intReadyFlag}
        if self.programTime:
            self.log("program length " + str(self.programTime))
            # Check any conditions
            conditionsPassed = True
            for condition in self.conditions:
                passed                          = (condition['curValue'] != condition['expectedValue']) == condition.get('invert', False)
                attributes[condition['entity']] = passed
                conditionsPassed                = conditionsPassed and passed
                self.log("condition check " + condition['entity'] + " " + str(passed))
            
            if conditionsPassed and self.intReadyFlag:
                # Create a rate series that's a combination of the import rate, export rate, or battery charge cost 
                # depending on what's being used at any given point in time
                usedImportRates     = self.utils.opOnSeries(self.importProfile,  self.importRateData, lambda a, b: b)
                usedExportRates     = self.utils.opOnSeries(self.exportProfile,  self.exportRateData, lambda a, b: b)
                paddedImportProfile = self.utils.opOnSeries(self.importRateData, self.importProfile,  lambda a, b: b)
                paddedExportProfile = self.utils.opOnSeries(self.exportRateData, self.exportProfile,  lambda a, b: b)
                paddedDefaultRate   = self.utils.opOnSeries(paddedImportProfile, paddedExportProfile, lambda a, b: 0 if (a > 0) or (b > 0) else self.maxChargeCost)
                combRates           = self.utils.opOnSeries(paddedDefaultRate,   usedImportRates,     lambda a, b: a + b)
                combRates           = self.utils.opOnSeries(combRates,           usedExportRates,     lambda a, b: a + b)
                combRates           = sorted(combRates, key=lambda x: x[0])
                # Filter out any rates that end after the finish by time
                if self.finishByOn:
                    combRates = list(filter(lambda x: x[1] <= self.finishByTime, combRates))
    
                # Check each slot in the rates as a potential starting point
                numRates = len(combRates)
                for rateStartIdx in range(numRates):
                    candidateCost     = 0
                    candidatePlan     = []
                    curtIdx           = rateStartIdx
                    remainingPlanTime = self.programTime
                    while remainingPlanTime > timedelta() and curtIdx < numRates:
                        slot                   = combRates[curtIdx]
                        slotLength             = slot[1] - slot[0]
                        slotLengthUsed         = min(slotLength, remainingPlanTime)
                        slotLengthDecimalHours = slotLengthUsed.total_seconds() / (60 * 60)
                        # We don't have power usage throughout the machine cycles, so just assume its a flat unity 
                        # power for the moment.
                        slotCost               = slot[2] * slotLengthDecimalHours
                        candidateCost          = candidateCost + slotCost
                        candidatePlan.append(slot + (slotCost,))
                        # rotate vars for the next slot
                        remainingPlanTime      = remainingPlanTime - slotLength   
                        curtIdx                = curtIdx + 1
                    # do we have a valid candidate plan
                    if remainingPlanTime > timedelta():
                        break
                    elif candidateCost < bestCost:
                        bestCost = candidateCost
                        bestPlan = candidatePlan

        # if we found a plan then output it for debug
        if bestPlan:
            self.utils.printSeries(bestPlan, "Appliance plan" ) 
            startTime             = bestPlan[0][0]
            endTime               = bestPlan[-1][1]
            startTimeStr          = startTime.isoformat()
            startTimeLocalStr     = startTime.astimezone(tz.gettz()).strftime("%-I:%M %p")
            endTimeLocalStr       = endTime.astimezone(tz.gettz()).strftime("%-I:%M %p")
            attributes['display'] = startTimeLocalStr + " > " + endTimeLocalStr
        else:
            startTimeStr          = ""
            attributes['display'] = None
        self.set_state(self.startTimeEntityName, state=startTimeStr, attributes=attributes)

  
                    
        

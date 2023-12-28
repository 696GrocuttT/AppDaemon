import hassapi as hass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from core.powerUtils import PowerUtils
import math



class CheapestTime(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))
        self.utils                   = PowerUtils(self.log) 
        batteryPlanSummaryEntityName = self.args['batteryPlanSummaryEntity']

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
        # Get the time required for the program to run
        programTimeEntityName = self.args['programTimeEntity']
        self.programTimeChanged(None, None, None, self.get_state(programTimeEntityName), None)
        self.listen_state(self.programTimeChanged, programTimeEntityName) 
        # Now we have all the data do an initial plan
        self.createPlan(None)
        # Schedule future updates every 30 minutes to align with rate charges
        now       = datetime.now() 
        period    = timedelta(minutes=30)
        startTime = now.replace(minute=0, second=0, microsecond=0) 
        while startTime < now:
            startTime = startTime + period
        self.run_every(self.createPlan, startTime, 30*60)


    def programTimeChanged(self, entity, attribute, old, new, kwargs):
        splitTimeStr = new.split(':')
        if len(splitTimeStr) == 2:
            self.programTime = timedelta(hours=int(splitTimeStr[0]), minutes=int(splitTimeStr[1]))
        else:
            self.programTime = None
        

    def chargeCostChanged(self, entity, attribute, old, new, kwargs):
        self.maxChargeCost = new
    
    
    def rateChanged(self, entity, attribute, old, new, kwargs):
        rateName = kwargs['kwargs']
        rateData = list(map(lambda x: (datetime.fromisoformat(x['start']).astimezone(),
                                       datetime.fromisoformat(x['end']).astimezone(), 
                                       x['rate']), 
                            new))
        rateData.sort(key=lambda x: x[0])    
        self.utils.printSeries(rateData, "Rate data (" + rateName + ")")
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
        self.utils.printSeries(profileData, "Profile data (" + profileName + ")")
        if profileName == "import":
            self.importProfile = profileData
        else:
            self.exportProfile = profileData


    def createPlan(self, kwargs):
        # We only plan if there's a valid program time
        bestCost = math.inf
        bestPlan = []
        if self.programTime:
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

            # Check each slock in the rates as a potential starting point
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
  
                    
        

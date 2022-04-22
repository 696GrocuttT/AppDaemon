import hassapi as hass
from datetime import datetime
from datetime import timedelta


class BasicInactiveAutoOff(hass.Hass):
    def initialize(self):
        self.log("Starting with arguments " + str(self.args))
        self.inputEntityName      = self.args["inputEntity"]
        self.outputEntityName     = self.args["outputEntity"]        
        self.autoOffTimeDelay     = self.args.get("autoOffTimeDelay", 120)
        self.manualOffToAutoDelay = timedelta(seconds=self.args.get("manualOffToAutoDelay", 20))
        if not "log_level" in self.args:
            self.set_log_level("WARNING")
        # Precompile all the conditions, but keep the string versions for logging. Then evaluate all the 
        # conditions just to test them and produce some log entries for easy debugging
        self.extraOnConditions    = self.args.get("extraOnConditions", [])
        self.extraOnConditions    = list(map(lambda x: (x, compile(x, "<string>", "eval")), self.extraOnConditions))
        self.log("On conditions:")
        self.evalConditions(self.extraOnConditions)
        self.extraOffConditions   = self.args.get("extraOffConditions", [])
        self.extraOffConditions   = list(map(lambda x: (x, compile(x, "<string>", "eval")), self.extraOffConditions))
        self.log("Off conditions:")
        self.evalConditions(self.extraOffConditions)
        self.timer                = None
        self.auto                 = False
        self.pendingAuto          = False
        self.outputLastChanged    = datetime.now() - self.manualOffToAutoDelay
        if eventName := self.args.get("inputEvent", None):
            self.listen_event(self.inupt_event, eventName)
        self.listen_state(self.input_changed,  self.inputEntityName)
        self.listen_state(self.output_changed, self.outputEntityName)


    # helper function for use in the extraOnConditions argument
    def isAfterTime(self, *, hour, minute=0):
        now = datetime.now()
        return ((now.hour > hour) or
                (now.hour == hour and now.minute >= minute))


    # helper function for use in the extraOnConditions argument
    def isBeforeTime(self, *, hour, minute=0):
        now = datetime.now()
        return ((now.hour < hour) or
                (now.hour == hour and now.minute < minute))


    def evalConditions(self, conditions):
        conditionsPassed = True
        for condition in conditions:
            conditionVal     = eval(condition[1])
            conditionsPassed = conditionsPassed and conditionVal
            self.log("condition \"" + condition[0] + "\" is "  +  str(conditionVal))
        return conditionsPassed
        
        
    # Getter for off delay so we can easily override it in a subclass
    def offDelay(self):
        return self.autoOffTimeDelay


    def inupt_event(self, event_name, data, kwargs):
        self.log("Input event recieved")
        # If the input entity isn't already on, trigger fake an on then off sequence
        if self.get_state(self.inputEntityName) == "off":
            self.log("Triggered from input event")
            self.input_changed(None, None, "off", "on", None)
            self.input_changed(None, None, "on", "off", None)


    def input_changed(self, entity, attribute, old, new, kwargs):
        self.log("input " + old + " --> " + new + ". auto " + str(self.auto))
        # if there's a running timer, cancel it
        if self.timer:
            if self.timer_running(self.timer):
                self.cancel_timer(self.timer)
        if new == "on":
            # Only turn on the output if its off, and its not recently been manually turned off
            if ( self.get_state(self.outputEntityName) == "off" and 
                 (self.auto or (datetime.now() - self.outputLastChanged) > self.manualOffToAutoDelay) ):
                # Evaluate any additional conditions
                if self.evalConditions(self.extraOnConditions): 
                    self.pendingAuto = True
                    self.turn_on(self.outputEntityName)
        else:
            # schedule a timer to turn the output off after a delay
            self.timer = self.run_in(self.output_off, self.offDelay())


    def output_changed(self, entity, attribute, old, new, kwargs):
        self.log("output " + old + " --> " + new + ". auto " + str(self.auto) + " --> " + str(self.pendingAuto))
        self.auto              = self.pendingAuto
        self.pendingAuto       = False
        self.outputLastChanged = datetime.now()


    def output_off(self, kwargs):
        self.log("Off timer fired. auto: " + str(self.auto))
        # only turn off the output if it was turned on by the automation
        if self.auto and self.get_state(self.outputEntityName) == "on":
            # Check any extra conditions
            if self.evalConditions(self.extraOffConditions): 
                self.pendingAuto = True
                self.turn_off(self.outputEntityName)



class BathroomFanAutoOff(BasicInactiveAutoOff):
    def initialize(self):
        self.autoOffDurationTheshold = timedelta(seconds=self.args.get("autoOffDurationTheshold", 5*60))
        self.inputOnTime             = datetime.now()
        self.onDuration              = timedelta(seconds=0)
        super().initialize()


    def input_changed(self, entity, attribute, old, new, kwargs):
        now = datetime.now()
        if new == "on":
            self.inputOnTime = now
        else:
            self.onDuration = now - self.inputOnTime
        super().input_changed(entity, attribute, old, new, kwargs)


    def offDelay(self):
        delay = super().offDelay() if self.onDuration > self.autoOffDurationTheshold else 0
        self.log("onDuration " + str(self.onDuration.total_seconds()) + " delay " + str(delay))
        return delay


import hassapi as hass

#
# Hello World App
#
# Args:
#


class BaseInactiveAutoOff(hass.Hass):
    def initialize(self):
        self.log("Hello from AppDaemon. Woof", log="main_log")
        self.log("You are now ready to run Apps!")

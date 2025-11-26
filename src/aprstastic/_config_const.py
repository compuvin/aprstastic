DEFAULT_CONFIG_CONTENT = """# 
# APRSTASTIC CONFIGURATION FILE (version: 1)
# Be sure to at least modify 'call_sign' and 'aprsis_passcode'.
#


# Radio call sign of the gateway itself (analogy, iGate's call sign)
call_sign: N0CALL 


# APRS-IS passcode. Search Google for how to get this
aprsis_passcode: 12345 


# APRS-IS Server Connection Settings
#aprsis_host: rotate.aprs.net
#aprsis_port: 14580


# Maximum length of an APRS text message. 
# If null, or commented out, default to 67 as per APRS specification
# max_aprs_message_length: 128


# Meshtastic interface options:
#   type: serial (default) or tcp
#   serial: specify 'device' or leave blank for auto-detect
#   tcp: set 'host' (and optional 'port', default 4403)
meshtastic_interface: 
   type: serial
#  device: /dev/ttyACM0
#  host: 192.168.1.50
#  port: 4403


# Beacon new registrations to APRS-IS to facilitate discovery
beacon_registrations: true 


# Should the gateway beacon its own position
gateway_beacon:
  enabled: true
  icon: "OGM"                 # 'M' in a diamond, representing a Gateway
#  latitude: 47.6205063,      # Leave commented to read position from Meshtastic device
#  longitude: -122.3518523    # Leave commented to read position from Meshtastic device


# Where should logs be stored?
# If null, (or commented out), store logs in the `logs` dir, sibling to this file. 
#logs_dir: null


# Where should data be stored?
# If null, (or commented out), store data in the `data` dir, sibling to this file. 
#data_dir: null
"""

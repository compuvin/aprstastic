import aprslib
from aprslib.parsing import parse
import pubsub
import time
import sys
import json
import time
import logging
import warnings
import random
import threading
import re
import os
import traceback
import meshtastic.stream_interface
import meshtastic.serial_interface
import meshtastic.tcp_interface
from datetime import datetime
from meshtastic.util import findPorts
import math

from queue import Queue, Empty
from .__about__ import __version__
from ._aprs_client import APRSClient
from ._aprs_symbols import get_symbol_code
from ._registry import CallSignRegistry

logger = logging.getLogger("aprstastic")

from aprslib.parsing import parse

MAX_APRS_TEXT_MESSAGE_LENGTH = 67
MAX_APRS_POSITION_MESSAGE_LENGTH = 43

APRS_SOFTWARE_ID = "APZMAG"  # Experimental Meshtastic-APRS Gateway
MQTT_TOPIC = "meshtastic.receive"
REGISTRATION_BEACON = "MESHID-01"
GATEWAY_BEACON_INTERVAL = 3600  # Station beacons once an hour

SERIAL_WATCHDOG_INTERVAL = 60  # Check the serial state every minute
MESHTASTIC_WATCHDOG_INTERVAL = (
    60 * 15
)  # After how long should we become worried the Meshtastic device is quiet?

# APRS station cache settings
DEFAULT_APRS_STATION_MAX_AGE_HOURS = 6

# Beacons that mean unregister
APRS_TOMBSTONE = "N0NE-01"
MESH_TOMBSTONE = "!00000000"

# Icons
DEFAULT_GATEWAY_ICON = "OGM"
DEFAULT_NODE_ICON = "MV"
DEFAULT_GATEWAY_SYMBOL = "M&"
DEFAULT_NODE_SYMBOL = "/>"

# For uptime
TIME_DURATION_UNITS = (
    ("w", 60 * 60 * 24 * 7),
    ("d", 60 * 60 * 24),
    ("h", 60 * 60),
    ("m", 60),
    ("s", 1),
)


class Gateway(object):
    def __init__(self, config):
        self._config = config
        self._start_time = None
        self._meshtastic_interface_config = config.get("meshtastic_interface") or {}

        self._gateway_id = None
        self._gateway_call_sign = None

        self._interface = None
        self._mesh_rx_queue = Queue()

        self._aprs_client = None
        self._max_aprs_message_length = config.get("max_aprs_message_length")
        if self._max_aprs_message_length is None:
            self._max_aprs_message_length = MAX_APRS_TEXT_MESSAGE_LENGTH

        self._reply_to = {}
        self._filtered_call_signs = []
        self._beacon_registrations = False

        self._next_beacon_time = 0
        self._next_serial_check_time = 0
        self._last_meshtastic_packet_time = 0

        self._registry = CallSignRegistry(config.get("data_dir"))
        self._aprs_station_positions = {}
        self._mesh_positions = {}

    def run(self):
        # For measuring uptime
        self._start_time = time.time()

        # Connect to the Meshtastic device
        self._interface = self._get_interface(self._meshtastic_interface_config)
        if self._interface is None:
            raise ValueError("No meshtastic device detected or specified.")

        def on_recv(packet, interface=None):
            self._mesh_rx_queue.put(packet)

        pubsub.pub.subscribe(on_recv, MQTT_TOPIC)
        node_info = self._interface.getMyNodeInfo()
        self._gateway_id = node_info.get("user", {}).get("id")
        logger.debug(f"Gateway device id: {self._gateway_id}")

        # Create an initial list of known call signs based on the device node database

        # Myself
        self._gateway_call_sign = self._config.get("call_sign", "").upper().strip()
        self._filtered_call_signs.append(self._gateway_call_sign)

        # The registraion beacon
        self._beacon_registrations = self._config.get("beacon_registrations", True)
        if self._beacon_registrations:
            self._filtered_call_signs.append(REGISTRATION_BEACON)

        # Recently seen nodes
        for node in self._interface.nodesByNum.values():
            presumptive_id = f"!{node['num']:08x}"

            if presumptive_id not in self._registry:
                continue

            # Heard more than a day ago
            last_heard = node.get("lastHeard")
            if last_heard is None or last_heard + 3600 * 24 < time.time():
                continue

            self._filtered_call_signs.append(
                self._registry[presumptive_id]["call_sign"]
            )

        # Connect to APRS IS
        aprsis_passcode = self._config.get("aprsis_passcode")
        aprsis_host = self._config.get("aprsis_host", "rotate.aprs.net")
        aprsis_port = self._config.get("aprsis_port", 14580)
        self._aprs_client = APRSClient(
            self._gateway_call_sign,
            aprsis_passcode,
            aprsis_host,
            aprsis_port,
            self._build_aprsis_filter(),
        )

        logger.debug("Pausing for 2 seconds...")
        time.sleep(2.0)
        logger.debug("Starting main loop.")

        gateway_beacon = self._config.get("gateway_beacon", {})

        self._last_meshtastic_packet_time = self._start_time

        while True:
            # There are four independent steps performed by this loop: servicing watchdogs,
            # beaconing, reading from Meshtastic, and reading from APRS.
            # Make sure that errors in one don't stop the others.
            now = time.time()

            # 1. Service the watchdogs
            ############################
            reconnect = False

            # Periodically check on the state of the device connection
            if now > self._next_serial_check_time:
                self._next_serial_check_time = now + SERIAL_WATCHDOG_INTERVAL
                if not self._is_interface_connected():
                    logger.warn("Meshtastic connection is not open.")
                    reconnect = True

            # Check if the Meshtastic device has gone silent a while
            if (
                reconnect == False
                and now - self._last_meshtastic_packet_time
                > MESHTASTIC_WATCHDOG_INTERVAL
            ):
                self._last_meshtastic_packet_time = now
                logger.warn("No message from Meshtastic device for 15 minutes.")
                reconnect = True
                # This might be a frozen device. It may not be recoverable.

            # Reconnect if needed
            if reconnect:
                logger.warn("Attempting to reconnect in 30 seconds.")
                time.sleep(30)
                try:
                    if pubsub.pub.isSubscribed(on_recv, MQTT_TOPIC):
                        pubsub.pub.unsubscribe(on_recv, MQTT_TOPIC)
                    self._interface = self._get_interface(
                        self._meshtastic_interface_config
                    )
                    if self._interface is not None:
                        pubsub.pub.subscribe(on_recv, MQTT_TOPIC)
                except Exception as e:
                    logger.error(traceback.format_exc())

            # 2. Beacon the gateway position
            ################################
            try:
                if now > self._next_beacon_time and gateway_beacon.get("enabled"):
                    # If the latitude and longitude are not set in the config, then read it from the radio
                    gate_lat = gateway_beacon.get("latitude")
                    gate_lon = gateway_beacon.get("longitude")
                    if gate_lat is None or gate_lon is None:
                        gate_position = self._interface.getMyNodeInfo().get(
                            "position", {}
                        )
                        gate_lat = gate_position.get("latitude")
                        gate_lon = gate_position.get("longitude")

                    # If we still don't have a position, check again in one minute
                    if gate_lat is None or gate_lon is None:
                        self._next_beacon_time = now + 60
                    else:
                        self._send_aprs_gateway_beacon(
                            gate_lat,
                            gate_lon,
                            gateway_beacon.get("icon", DEFAULT_GATEWAY_ICON),
                            "aprstastic: " + self._gateway_id,
                        )
                        self._next_beacon_time = now + GATEWAY_BEACON_INTERVAL
            except Exception as e:
                logger.error(traceback.format_exc())

            # 3. Read a Meshastic packet
            ############################
            try:
                mesh_packet = None
                try:
                    mesh_packet = self._mesh_rx_queue.get(block=False)
                except Empty:
                    pass

                if mesh_packet is not None:
                    self._process_meshtastic_packet(mesh_packet)
            except Exception as e:
                logger.error(traceback.format_exc())

            # 4. Read an APRS packet
            ########################
            try:
                aprs_packet = self._aprs_client.recv()
                if aprs_packet is not None:
                    self._process_aprs_packet(aprs_packet)
            except Exception as e:
                logger.error(traceback.format_exc())

            # Yield
            time.sleep(0.001)

    def _get_interface(
        self, interface_config=None
    ) -> meshtastic.stream_interface.StreamInterface:
        config = interface_config or {}
        interface_type = (config.get("type") or "serial").lower()

        if interface_type == "serial":
            device = config.get("device")
            if device is None:
                ports = findPorts(True)
                if len(ports) == 1:
                    device = ports[0]
                else:
                    logger.error(
                        "Please specify the correct serial port in 'aprstastic.yaml'. "
                        f"Possible values include: {ports}"
                    )
                    return None
            dev = meshtastic.serial_interface.SerialInterface(device)
            logger.info(f"Connected to serial device: {device}")
            return dev

        if interface_type == "tcp":
            host = config.get("host") or "localhost"
            port = config.get("port") or meshtastic.tcp_interface.DEFAULT_TCP_PORT
            dev = meshtastic.tcp_interface.TCPInterface(
                hostname=host, portNumber=int(port)
            )
            logger.info(f"Connected to TCP host: {host}:{port}")
            return dev

        logger.error(
            f"Unsupported meshtastic_interface type '{interface_type}'. "
            "Valid values are 'serial' and 'tcp'."
        )
        return None

    def _is_interface_connected(self):
        if self._interface is None:
            return False

        stream = getattr(self._interface, "stream", None)
        if stream is not None and hasattr(stream, "is_open"):
            try:
                return stream.is_open
            except Exception:
                return False

        socket_obj = getattr(self._interface, "socket", None)
        if socket_obj is not None:
            return True

        return True

    def _process_meshtastic_packet(self, packet):
        self._last_meshtastic_packet_time = time.time()

        fromId = packet.get("fromId", None)
        toId = packet.get("toId", None)
        portnum = packet.get("decoded", {}).get("portnum")

        # Don't bother logging my telemetry
        if portnum != "TELEMETRY_APP" or fromId != self._gateway_id:
            logger.info(f"{fromId} -> {toId}: {portnum}")

        # Record that we have spotted the ID
        should_announce = self._spotted(fromId)

        if portnum == "POSITION_APP":
            position = packet.get("decoded", {}).get("position") or {}
            lat = position.get("latitude")
            lon = position.get("longitude")
            if lat is not None and lon is not None:
                self._mesh_positions[fromId] = {
                    "latitude": lat,
                    "longitude": lon,
                    "time": position.get("time"),
                }

            if fromId not in self._registry:
                return

            # Special icon disables position sharing
            if self._registry[fromId]["icon"] == "$$":
                logger.info(f"{fromId} has disabled position reporting")
            else:
                if lat is None or lon is None:
                    return
                self._send_aprs_position(
                    self._registry[fromId]["call_sign"],
                    lat,
                    lon,
                    position.get("time"),
                    self._registry[fromId]["icon"],
                    "aprstastic: " + fromId,
                )

        if portnum == "TEXT_MESSAGE_APP":
            message_bytes = packet["decoded"]["payload"]
            message_string = message_bytes.decode("utf-8")

            if toId == "^all" and message_string.lower().strip().startswith("aprs?"):
                self._send_mesh_message(
                    fromId,
                    "APRS-tastic Gateway available here. Welcome. Reply '?' for more info.",
                )
                return

            if toId != self._gateway_id:
                # Not for me
                return

            if message_string.strip() == "?":
                # Different call signs for registered and non-registered devices
                if fromId not in self._registry:
                    self._send_mesh_message(
                        fromId,
                        "Send and receive APRS messages by registering your call sign. HAM license required.\n\nReply with:\n!register [CALLSIGN]-[SSID]\nE.g.,\n!register N0CALL-1\n\nSee https://github.com/afourney/aprstastic for more.",
                    )
                    return
                else:
                    self._send_mesh_message(
                        fromId,
                        "Send APRS messages by replying here, and prefixing your message with the dest callsign. E.g., 'WLNK-1: hello'\n\nSee https://github.com/afourney/aprstastic for more.",
                    )
                    return

            if message_string.strip() == "!id" or message_string.strip() == "!version":
                # Let clients query the gateway call sign and version number
                self._send_mesh_message(
                    fromId,
                    f"Gateway call sign: {self._gateway_call_sign}, Uptime: {self._uptime()}, Version: {__version__}",
                )
                return

            if message_string.lower().strip() == "!stations":
                self._handle_stations_request(fromId)
                return

            if message_string.lower().strip().startswith("!register"):
                # Allow operatores to join
                m = re.search(
                    r"^!register:?\s+([a-z0-9]{4,7}\-[0-9]{1,2})(\s+(\$\$|[a-zA-Z0-9]{2,3}))?$",
                    message_string.lower().strip(),
                )
                if m:
                    # Extract the call sign
                    call_sign = m.group(1)
                    if call_sign is None:
                        call_sign = ""
                    else:
                        call_sign = call_sign.upper()

                    # Extract and validate the icon
                    icon = m.group(3)
                    if icon is None:
                        pass
                    elif icon == "":
                        icon = None
                    elif icon == "$$":
                        pass
                    else:
                        icon = icon.upper()
                        symbol = get_symbol_code(icon)
                        if symbol is None:
                            self._send_mesh_message(
                                fromId,
                                "Invalid icon. See: https://github.com/afourney/aprstastic/blob/main/APRS_SYMBOLS.md",
                            )
                            return

                    # Update the database
                    if fromId in self._registry:
                        # Update
                        self._registry.add_registration(fromId, call_sign, icon, True)
                        self._send_mesh_message(fromId, "Registration updated.")
                    else:
                        # New
                        self._registry.add_registration(fromId, call_sign, icon, True)
                        self._spotted(fromId)
                        self._send_mesh_message(
                            fromId,
                            "Registered. Send APRS messages by replying here, and prefixing your message with the dest callsign. E.g., 'WLNK-1: hello' ",
                        )
                    self._spotted(fromId)  # Run this again to update subscriptions

                    # Beacon the registration to APRS-IS to facilitate building a shared roaming mapping
                    if self._beacon_registrations:
                        self._send_registration_beacon(fromId, call_sign, icon)
                else:
                    self._send_mesh_message(
                        fromId,
                        "Invalid call sign + ssid.\nSYNTAX: !register [CALLSIGN]-[SSID]\nE.g.,\n!register N0CALL-1",
                    )
                return

            if message_string.lower().strip().startswith("!unregister"):
                if fromId not in self._registry:
                    self._send_mesh_message(
                        fromId, "Device is not registered. Nothing to do."
                    )
                    return
                call_sign = self._registry[fromId]["call_sign"]
                self._registry.add_registration(fromId, None, None, True)
                self._registry.add_registration(None, call_sign, None, True)

                if self._beacon_registrations:
                    self._send_registration_beacon(fromId, APRS_TOMBSTONE, None)
                    self._send_registration_beacon(MESH_TOMBSTONE, call_sign, None)

                if call_sign in self._filtered_call_signs:
                    self._filtered_call_signs.remove(call_sign)

                self._update_aprsis_filter()
                self._send_mesh_message(fromId, "Device unregistered.")
                return

            if fromId not in self._registry:
                self._send_mesh_message(
                    fromId,
                    "Unknown device. HAM license required!\nRegister by replying with:\n!register [CALLSIGN]-[SSID]\nE.g.,\n!register N0CALL-1",
                )
                return

            m = re.search(r"^([A-Za-z0-9]+(\-[A-Za-z0-9]+)?):(.*)$", message_string)
            if m:
                tocall = m.group(1)
                self._reply_to[fromId] = tocall
                message = m.group(3).strip()
                self._send_aprs_message(
                    self._registry[fromId]["call_sign"], tocall, message
                )
                return
            elif fromId in self._reply_to:
                self._send_aprs_message(
                    self._registry[fromId]["call_sign"],
                    self._reply_to[fromId],
                    message_string,
                )
                return
            else:
                self._send_mesh_message(
                    fromId,
                    "Please prefix your message with the dest callsign. E.g., 'WLNK-1: hello'",
                )
                return

        # At this point the message was not handled yet. Announce yourself
        if should_announce:
            self._send_mesh_message(
                fromId,
                "APRS-tastic Gateway available here. Welcome. Reply '?' for more info.",
            )

    def _process_aprs_packet(self, packet):
        self._record_aprs_station(packet)

        if packet.get("format") == "message":
            fromcall = packet.get("from", "N0CALL").strip().upper()
            tocall = packet.get("addresse", "").strip().upper()

            # Is this an ack?
            if packet.get("response") == "ack":
                logger.debug(
                    f"Received ACK to {tocall}'s message #" + packet.get("msgNo", "")
                )
                return

            # Is this a registration beacon?
            if tocall == REGISTRATION_BEACON:
                mesh_id = packet.get("message_text", "").lower().strip()
                m = re.search(r"^(![a-f0-9]{8})(:(\$\$|[A-Za-z0-9]{2,3}))?$", mesh_id)
                if m:
                    mesh_id = m.group(1)
                    icon = m.group(3)

                    # Validate the icon
                    if icon is None:
                        pass
                    elif icon == "":
                        icon = None
                    elif icon != "$$":
                        icon = icon.upper()
                        symbol = get_symbol_code(icon)
                        if symbol is None:
                            icon = None

                    # Handle tombstones
                    if mesh_id == MESH_TOMBSTONE:
                        mesh_id = None
                    if fromcall == APRS_TOMBSTONE:
                        fromcall = None

                    logger.info(
                        f"Observed registration beacon: {mesh_id}: {fromcall}, icon: {icon}",
                    )
                    self._registry.add_registration(mesh_id, fromcall, icon, False)
                else:
                    # Not necessarily and error. Could be from a future version
                    logger.debug(
                        f"Unknown registration beacon: {packet.get('raw')}",
                    )
                return

            # Ack all remaining messages (which aren't themselves acks, and aren't beacons)
            self._send_aprs_ack(tocall, fromcall, packet.get("msgNo", ""))

            # Message was sent to the gateway itself. Respond with station information.
            if tocall == self._gateway_call_sign:
                self._send_aprs_message(
                    tocall,
                    fromcall,
                    f"Gateway ID: {self._gateway_id}, Uptime: {self._uptime()}, Version: {__version__}",
                )
                return

            # Figure out where the packet is going
            toId = None
            for k in self._registry:
                if tocall == self._registry[k]["call_sign"].strip().upper():
                    toId = k
                    break
            if toId is None:
                logger.error(f"Unkown recipient: {tocall}")
                return

            # Forward the message
            message = packet.get("message_text")
            if message is not None:
                self._reply_to[toId] = fromcall
                self._send_mesh_message(toId, fromcall + ": " + message)

    def _send_aprs_message(self, fromcall, tocall, message):
        message_chunks = self._chunk_message(message, self._max_aprs_message_length)

        while len(tocall) < 9:
            tocall += " "

        for chunk in message_chunks:
            packet = (
                fromcall
                + ">"
                + APRS_SOFTWARE_ID
                + ",WIDE1-1,qAR,"
                + self._gateway_call_sign
                + "::"
                + tocall
                + ":"
                + chunk.strip()
                + "{"
                + str(random.randint(0, 999))
            )
            logger.debug("Sending to APRS: " + packet)
            self._aprs_client.send(packet)

    def _send_aprs_ack(self, fromcall, tocall, messageId):
        while len(tocall) < 9:
            tocall += " "
        packet = (
            fromcall
            + ">"
            + APRS_SOFTWARE_ID
            + ",WIDE1-1,qAR,"
            + self._gateway_call_sign
            + "::"
            + tocall
            + ":ack"
            + messageId
        )
        logger.debug("Sending to APRS: " + packet)
        self._aprs_client.send(packet)

    def _aprs_lat(self, lat):
        aprs_lat_ns = "N" if lat >= 0 else "S"
        lat = abs(lat)
        aprs_lat_deg = int(lat)
        aprs_lat_min = float((lat - aprs_lat_deg) * 60)
        return f"%02d%05.2f%s" % (aprs_lat_deg, aprs_lat_min, aprs_lat_ns)

    def _aprs_lon(self, lon):
        aprs_lon_ew = "E" if lon >= 0 else "W"
        lon = abs(lon)
        aprs_lon_deg = abs(int(lon))
        aprs_lon_min = float((lon - aprs_lon_deg) * 60)
        return f"%03d%05.2f%s" % (aprs_lon_deg, aprs_lon_min, aprs_lon_ew)

    def _send_aprs_position(self, fromcall, lat, lon, t, icon, message):
        message = self._truncate_message(message, MAX_APRS_POSITION_MESSAGE_LENGTH)

        aprs_lat = self._aprs_lat(lat)
        aprs_lon = self._aprs_lon(lon)

        # Get the icon
        if icon is None:
            icon = DEFAULT_NODE_ICON

        # Convert it into a symbol
        symbol = get_symbol_code(icon)
        if symbol is None:
            symbol = DEFAULT_NODE_SYMBOL

        aprs_msg = None
        if t is None:
            # No timestamp
            aprs_msg = "=" + aprs_lat + symbol[0] + aprs_lon + symbol[1] + message
        else:
            aprs_ts = datetime.utcfromtimestamp(t).strftime("%d%H%M")
            if len(aprs_ts) == 5:
                aprs_ts = "0" + aprs_ts + "z"
            else:
                aprs_ts = aprs_ts + "z"
            aprs_msg = (
                "@" + aprs_ts + aprs_lat + symbol[0] + aprs_lon + symbol[1] + message
            )

        packet = (
            fromcall
            + ">"
            + APRS_SOFTWARE_ID
            + ",WIDE1-1,qAR,"
            + self._gateway_call_sign
            + ":"
            + aprs_msg
        )
        logger.debug(f"Sending to APRS: {packet}")
        self._aprs_client.send(packet)

    def _send_aprs_gateway_beacon(self, lat, lon, icon, message):
        aprs_lat = self._aprs_lat(lat)
        aprs_lon = self._aprs_lon(lon)

        # Convert the icon to a symbol
        symbol = get_symbol_code(icon)
        if symbol is None:
            symbol = DEFAULT_GATEWAY_SYMBOL

        aprs_msg = "!" + aprs_lat + symbol[0] + aprs_lon + symbol[1] + message
        packet = (
            self._gateway_call_sign + ">" + APRS_SOFTWARE_ID + ",TCPIP*:" + aprs_msg
        )
        logger.debug(f"Beaconing to APRS: {packet}")
        self._aprs_client.send(packet)

    def _send_mesh_message(self, destid, message):
        logger.info(f"Sending to '{destid}': {message}")
        self._interface.sendText(
            text=message, destinationId=destid, wantAck=True, wantResponse=False
        )

    def _spotted(self, node_id):
        """
        Checks if the node is registered, and newly spotted -- in which
        case we need to update the APRS filters, and also greet the user.
        In this case, it returns true. Otherwise, false.
        """

        # We spotted them, but they aren't registered
        if node_id not in self._registry:
            return False

        call_sign = self._registry[node_id]["call_sign"]

        if call_sign in self._filtered_call_signs:
            return False

        # Ok it's new, update the filters
        self._filtered_call_signs.append(call_sign)
        self._update_aprsis_filter()
        return True

    def _build_aprsis_filter(self):
        parts = []
        if len(self._filtered_call_signs) > 0:
            parts.append("g/" + "/".join(self._filtered_call_signs))

        lat, lon = self._get_aprsis_filter_location()
        if lat is not None and lon is not None:
            distance_km = self._config.get("aprsis_filter_distance_km", 80)
            try:
                distance_km = float(distance_km)
                parts.append(f"r/{lat}/{lon}/{distance_km}")
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid 'aprsis_filter_distance_km' value. Range filter disabled."
                )

        if len(parts) == 0:
            return None

        return " ".join(parts)

    def _get_aprsis_filter_location(self):
        lat = self._config.get("aprsis_filter_latitude")
        lon = self._config.get("aprsis_filter_longitude")
        if lat is None or lon is None:
            beacon = self._config.get("gateway_beacon", {}) or {}
            lat = beacon.get("latitude")
            lon = beacon.get("longitude")

        if lat is None or lon is None:
            return (None, None)

        try:
            return (float(lat), float(lon))
        except (TypeError, ValueError):
            logger.warning(
                "Invalid APRS-IS filter location values. Range filter disabled."
            )
            return (None, None)

    def _update_aprsis_filter(self):
        self._aprs_client.set_filter(self._build_aprsis_filter())

    def _record_aprs_station(self, packet):
        lat = packet.get("latitude")
        lon = packet.get("longitude")
        fromcall = packet.get("from")
        if lat is None or lon is None or fromcall is None:
            return

        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            return

        self._aprs_station_positions[fromcall.strip().upper()] = {
            "latitude": lat,
            "longitude": lon,
            "time": time.time(),
        }
        self._prune_aprs_station_cache()

    def _prune_aprs_station_cache(self):
        max_age_hours = self._config.get(
            "aprsis_station_max_age_hours", DEFAULT_APRS_STATION_MAX_AGE_HOURS
        )
        try:
            max_age_hours = float(max_age_hours)
        except (TypeError, ValueError):
            max_age_hours = DEFAULT_APRS_STATION_MAX_AGE_HOURS
        if max_age_hours <= 0:
            return

        cutoff = time.time() - (max_age_hours * 60 * 60)
        stale_calls = [
            call
            for call, info in self._aprs_station_positions.items()
            if info.get("time", 0) < cutoff
        ]
        for call in stale_calls:
            del self._aprs_station_positions[call]

    def _haversine_km(self, lat1, lon1, lat2, lon2):
        r = 6371.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = (
            math.sin(dphi / 2) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        )
        return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def _handle_stations_request(self, fromId):
        if fromId not in self._registry:
            self._send_mesh_message(
                fromId,
                "Unknown device. HAM license required!\nRegister by replying with:\n!register [CALLSIGN]-[SSID]\nE.g.,\n!register N0CALL-1",
            )
            return

        self._prune_aprs_station_cache()

        position = self._mesh_positions.get(fromId)
        if position is None:
            self._send_mesh_message(
                fromId,
                "No position available for your node yet. Share a position and try again.",
            )
            return

        lat = position.get("latitude")
        lon = position.get("longitude")
        if lat is None or lon is None:
            self._send_mesh_message(
                fromId,
                "No valid position available for your node yet. Share a position and try again.",
            )
            return

        if len(self._aprs_station_positions) == 0:
            self._send_mesh_message(
                fromId,
                "No APRS station positions received yet. Try again shortly.",
            )
            return

        stations = []
        for call, info in self._aprs_station_positions.items():
            if "time" not in info:
                continue
            dist_km = self._haversine_km(
                lat, lon, info["latitude"], info["longitude"]
            )
            stations.append((dist_km, call))

        stations.sort(key=lambda x: x[0])
        stations = stations[:10]

        lines = ["Closest APRS stations:"]
        for dist_km, call in stations:
            lines.append(f"{call} {dist_km:.1f} km")

        self._send_mesh_message(fromId, "\n".join(lines))

    def _uptime(self):
        if self._start_time is None:
            return "None"

        seconds = int(time.time() - self._start_time)

        if seconds < 1:
            return "0s"

        parts = []
        for unit, div in TIME_DURATION_UNITS:
            amount, seconds = divmod(int(seconds), div)
            if amount > 0:
                parts.append("{}{}".format(amount, unit))
            if len(parts) >= 3:  # Don't get overly precise
                break

        return ", ".join(parts)

    def _truncate_message(self, message, max_bytes):
        m_len = len(message.encode("utf-8"))

        # Nothing to do
        if m_len <= max_bytes:
            return message

        # Warn about the message being too long
        warnings.warn(
            f"Message of length {m_len} bytes exceeds the protocol maximum of {max_bytes} bytes."
        )

        # Trim first by characters to get close, then remove one character at a time
        strlen = max_bytes
        message = message[0:strlen]
        while len(message.encode("utf-8")) > max_bytes:
            # Chop a character
            strlen -= 1
            message = message[0:strlen]
        return message

    def _chunk_message(self, message, max_bytes):
        chunks = list()

        # We want to break on spaces
        tokens = re.split(r"(\s+)", message)
        if len(tokens) == 0:
            return chunks

        buffer = tokens.pop(0)  # Always include at least one token
        while len(tokens) > 0:
            token = tokens.pop(0)
            if len((buffer + token).encode("utf-8")) > max_bytes:
                chunks.append(buffer)
                buffer = token
            else:
                buffer += token
        if len(buffer) > 0:
            chunks.append(buffer)
            buffer = ""

        return chunks

    def _send_registration_beacon(self, device_id, call_sign, icon):
        logger.info(
            f"Beaconing registration {call_sign} <-> {device_id} (icon: {icon}), to {REGISTRATION_BEACON}"
        )
        if icon is None:
            self._send_aprs_message(call_sign, REGISTRATION_BEACON, device_id)
        else:
            self._send_aprs_message(
                call_sign, REGISTRATION_BEACON, device_id + ":" + icon
            )

"""
Omega Platinum Series Serial Communication Protocol Implementation.

Implements the command/response protocol for CN32Pt, CN16Pt, CN16DPt,
CN8Pt, CN8DPt, CN8EPt, DP32Pt, DP16Pt, DP8Pt, DP8EPt devices.

Protocol reference: Omega Platinum Series Serial Communication Protocol manual.
"""

import serial
import time
import logging
import threading

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sensor / input type look-up tables
# ---------------------------------------------------------------------------

SENSOR_TYPES = {
    0: "Thermocouple",
    1: "RTD",
    2: "Process Input",
    3: "Thermistor",
    4: "Remote",
}

TC_TYPES = {0: "J", 1: "K", 2: "T", 3: "E", 4: "N", 6: "R", 7: "S", 8: "B", 9: "C"}

RTD_WIRING = {0: "2-Wire", 1: "3-Wire", 2: "4-Wire"}

RTD_CURVE = {
    0: "385/100Ω",
    1: "385/500Ω",
    2: "385/1000Ω",
    3: "392/100Ω",
    4: "3916/100Ω",
}

PROCESS_RANGE = {
    0: "4-20 mA",
    1: "0-24 mA",
    5: "±10 Vdc",
    6: "±1.0 Vdc",
    7: "±0.1 Vdc",
}

THERMISTOR_TYPE = {0: "2.25K", 1: "5K", 2: "10K"}

TEMP_UNITS = {0: "None", 1: "°C", 2: "°F"}

DISPLAY_COLORS = {1: "GREEN", 2: "RED", 3: "AMBER"}

SYSTEM_STATES = {
    0: "LOAD",
    1: "IDLE",
    2: "INPUT_ADJUST",
    3: "CONTROL_ADJUST",
    4: "MODIFY",
    5: "WAIT",
    6: "RUN",
    7: "STANDBY",
    8: "STOP",
    9: "PAUSE",
    10: "FAULT",
    11: "SHUTDOWN",
    12: "AUTOTUNE",
}

# Baud rate index → actual rate
BAUD_RATES = {
    0: 300,
    1: 600,
    2: 1200,
    3: 2400,
    4: 4800,
    5: 9600,
    6: 19200,
    7: 38400,
    8: 57600,
    9: 115200,
}

PARITY_MAP = {0: "N", 1: "O", 2: "E"}
DATABITS_MAP = {0: 7, 1: 8}
STOPBITS_MAP = {0: 1, 1: 2}


class OmegaPlatinumProtocol:
    """
    Implements the Omega Platinum Series command/response serial protocol.

    Command structure:
        *[address]<class><cmd_id>[ params]<CR>

    Where:
        * = start of frame
        address = optional hex 00-C7 (decimal 0-199)
        class = G (Get), P (Put), R (Read), W (Write)
        cmd_id = hex command identifier
        params = space-separated parameter list
        <CR> = carriage return (0x0D)
    """

    SOF = "*"
    EOF = "\r"

    # Command classes
    GET = "G"
    PUT = "P"
    READ = "R"
    WRITE = "W"

    # --- Command IDs (hex strings) ---
    CMD_INPUT_CONFIG = "100"
    CMD_FILTER_CONSTANT = "101"
    CMD_CURRENT_READING = "110"
    CMD_PEAK_READING = "111"
    CMD_VALLEY_READING = "112"
    CMD_DISPLAY_CONFIG = "200"
    CMD_SERIAL_ADDR = "300"
    CMD_SERIAL_CONFIG = "310"
    CMD_SERIAL_DATA_MODE = "311"
    CMD_SERIAL_DATA_FORMAT = "312"
    CMD_SERIAL_PARAMS = "313"
    CMD_SERIAL_MODBUS = "314"
    CMD_USB_ADDR = "301"
    CMD_USB_CONFIG = "320"
    CMD_SETPOINT1 = "400"
    CMD_SETPOINT2 = "410"
    CMD_PID_CONFIG = "500"
    CMD_PID_P = "503"
    CMD_PID_I = "504"
    CMD_PID_D = "505"
    CMD_OUTPUT_MODE = "600"
    CMD_ALARM_CONFIG = "620"
    CMD_ALARM_HI = "621"
    CMD_ALARM_LO = "622"
    CMD_VERSION = "F20"
    CMD_BOOTLOADER_VER = "F22"
    CMD_RUN_MODE = "F23"

    def __init__(self):
        self.echo_mode = True  # assume echo is ON by default

    # ------------------------------------------------------------------
    # Command building
    # ------------------------------------------------------------------

    def build_command(
        self, cmd_class: str, cmd_id: str, address: int | None = None, params: str | None = None
    ) -> str:
        """Build a protocol command string."""
        cmd = self.SOF
        if address is not None:
            cmd += f"{address:02X}"
        cmd += cmd_class + cmd_id
        if params:
            cmd += " " + params
        cmd += self.EOF
        return cmd

    # --- Convenience builders ---

    def cmd_get_current_reading(self, address: int | None = None) -> str:
        return self.build_command(self.GET, self.CMD_CURRENT_READING, address)

    def cmd_get_peak_reading(self, address: int | None = None) -> str:
        return self.build_command(self.GET, self.CMD_PEAK_READING, address)

    def cmd_get_valley_reading(self, address: int | None = None) -> str:
        return self.build_command(self.GET, self.CMD_VALLEY_READING, address)

    def cmd_get_version(self, address: int | None = None) -> str:
        return self.build_command(self.GET, self.CMD_VERSION, address)

    def cmd_get_bootloader_version(self, address: int | None = None) -> str:
        return self.build_command(self.GET, self.CMD_BOOTLOADER_VER, address)

    def cmd_get_run_mode(self, address: int | None = None) -> str:
        return self.build_command(self.GET, self.CMD_RUN_MODE, address)

    def cmd_read_input_config(self, address: int | None = None) -> str:
        return self.build_command(self.READ, self.CMD_INPUT_CONFIG, address)

    def cmd_read_display_config(self, address: int | None = None) -> str:
        return self.build_command(self.READ, self.CMD_DISPLAY_CONFIG, address)

    def cmd_read_serial_config(self, address: int | None = None) -> str:
        return self.build_command(self.READ, self.CMD_SERIAL_CONFIG, address)

    def cmd_read_serial_params(self, address: int | None = None) -> str:
        return self.build_command(self.READ, self.CMD_SERIAL_PARAMS, address)

    def cmd_get_setpoint1(self, address: int | None = None) -> str:
        return self.build_command(self.GET, self.CMD_SETPOINT1, address)

    def cmd_write_setpoint1(self, value: float, address: int | None = None) -> str:
        return self.build_command(self.WRITE, self.CMD_SETPOINT1, address, str(value))

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def parse_reading_response(self, raw: str, echo: bool | None = None) -> float | None:
        """
        Parse a reading response (current / peak / valley).

        With echo ON:   "G110+32.0"  or  "01G110+32.0" (addressed)
        With echo OFF:  "+32.0"
        """
        if echo is None:
            echo = self.echo_mode

        text = raw.strip()
        if not text:
            return None

        try:
            if echo:
                # The echo contains optional hex address + command class + command ID
                # e.g. "01G110+32.0" or "G110+32.0"
                # Find the command class letter (G/R/W/P) followed by cmd ID digits,
                # then the value starts after those digits.
                import re
                m = re.search(r'[GRWP][0-9A-Fa-f]{3}(.+)', text)
                if m:
                    value_part = m.group(1).strip()
                    return float(value_part)
                # Fallback: try to find a signed float anywhere
                m = re.search(r'([+-]?\d+\.?\d*)', text)
                if m:
                    return float(m.group(1))
                return None
            else:
                return float(text)
        except (ValueError, IndexError):
            logger.warning("Failed to parse reading: %r", raw)
            return None

    def parse_version_response(self, raw: str, echo: bool | None = None) -> str | None:
        """
        Parse version response.

        With echo: "GF2001000500"  → "01.00.05.00"
        Without:   "01000500"      → "01.00.05.00"
        """
        if echo is None:
            echo = self.echo_mode

        text = raw.strip()
        if not text:
            return None

        try:
            if echo:
                # Remove the echo prefix "GF20" or "<addr>GF20"
                idx = text.upper().find("F20")
                if idx >= 0:
                    text = text[idx + 3 :]
                else:
                    return None

            if len(text) >= 8:
                major = text[0:2]
                minor = text[2:4]
                fix = text[4:6]
                build = text[6:8]
                return f"{major}.{minor}.{fix}.{build}"
        except (ValueError, IndexError):
            pass
        return None

    def parse_run_mode_response(self, raw: str, echo: bool | None = None) -> str | None:
        """Parse run mode / system state response."""
        if echo is None:
            echo = self.echo_mode

        text = raw.strip()
        if not text:
            return None

        try:
            if echo:
                idx = text.upper().find("F23")
                if idx >= 0:
                    text = text[idx + 3 :]
                else:
                    return None

            state_val = int(text.strip(), 16)
            return SYSTEM_STATES.get(state_val, f"UNKNOWN({state_val})")
        except (ValueError, IndexError):
            pass
        return None

    def parse_input_config_response(self, raw: str, echo: bool | None = None) -> dict | None:
        """Parse input configuration response → sensor type info."""
        if echo is None:
            echo = self.echo_mode

        text = raw.strip()
        if not text:
            return None

        try:
            if echo:
                idx = text.upper().find("100")
                if idx >= 0:
                    text = text[idx + 3 :]
                else:
                    return None

            # Parameters: STYPE SI1 SI2 (single hex digit each, space-separated or concatenated)
            parts = text.strip().split()
            if len(parts) == 1:
                # Might be concatenated
                vals = [int(c, 16) for c in text.strip() if c.strip()]
            else:
                vals = [int(p, 16) for p in parts]

            if len(vals) < 1:
                return None

            stype = vals[0]
            si1 = vals[1] if len(vals) > 1 else 0
            si2 = vals[2] if len(vals) > 2 else 0

            result = {"sensor_type": SENSOR_TYPES.get(stype, f"Unknown({stype})"), "raw_type": stype}

            if stype == 0:
                result["tc_type"] = TC_TYPES.get(si1, f"Unknown({si1})")
            elif stype == 1:
                result["rtd_wiring"] = RTD_WIRING.get(si1, f"Unknown({si1})")
                result["rtd_curve"] = RTD_CURVE.get(si2, f"Unknown({si2})")
            elif stype == 2:
                result["process_range"] = PROCESS_RANGE.get(si1, f"Unknown({si1})")
            elif stype == 3:
                result["thermistor_type"] = THERMISTOR_TYPE.get(si1, f"Unknown({si1})")

            return result
        except (ValueError, IndexError):
            pass
        return None

    def parse_display_config_response(self, raw: str, echo: bool | None = None) -> dict | None:
        """Parse display config → decimal point, units, color, brightness."""
        if echo is None:
            echo = self.echo_mode

        text = raw.strip()
        if not text:
            return None

        try:
            if echo:
                idx = text.upper().find("200")
                if idx >= 0:
                    text = text[idx + 3 :]
                else:
                    return None

            parts = text.strip().split()
            if len(parts) == 1:
                vals = [int(c, 16) for c in text.strip() if c.strip()]
            else:
                vals = [int(p, 16) for p in parts]

            if len(vals) < 2:
                return None

            result = {}
            result["units"] = TEMP_UNITS.get(vals[1], f"Unknown({vals[1]})")
            if len(vals) > 2:
                result["color"] = DISPLAY_COLORS.get(vals[2], f"Unknown({vals[2]})")
            return result
        except (ValueError, IndexError):
            pass
        return None


class OmegaConnection:
    """
    Manages the serial connection to one or more Omega Platinum devices.

    Handles sending commands and receiving responses with proper timeouts.
    """

    DEFAULT_TIMEOUT = 1.0  # seconds
    RESPONSE_DELAY = 0.05  # small delay before reading

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        parity: str = "N",
        databits: int = 8,
        stopbits: int = 1,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        self.port = port
        self.baudrate = baudrate
        self.parity = parity
        self.databits = databits
        self.stopbits = stopbits
        self.timeout = timeout
        self.serial: serial.Serial | None = None
        self.protocol = OmegaPlatinumProtocol()
        self._lock = threading.Lock()  # serialize access on shared bus

    @property
    def is_open(self) -> bool:
        return self.serial is not None and self.serial.is_open

    def open(self) -> bool:
        """Open the serial connection."""
        if self.is_open:
            return True
        try:
            parity_map = {"N": serial.PARITY_NONE, "O": serial.PARITY_ODD, "E": serial.PARITY_EVEN}
            stopbits_map = {1: serial.STOPBITS_ONE, 2: serial.STOPBITS_TWO}

            self.serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                parity=parity_map.get(self.parity, serial.PARITY_NONE),
                bytesize=self.databits,
                stopbits=stopbits_map.get(self.stopbits, serial.STOPBITS_ONE),
                timeout=self.timeout,
            )
            logger.info(
                "Opened %s @ %d %d%s%d",
                self.port,
                self.baudrate,
                self.databits,
                self.parity,
                self.stopbits,
            )
            return True
        except serial.SerialException as e:
            logger.error("Failed to open %s: %s", self.port, e)
            self.serial = None
            return False

    def close(self):
        """Close the serial connection."""
        if self.serial and self.serial.is_open:
            self.serial.close()
            logger.info("Closed %s", self.port)
        self.serial = None

    def send_command(self, command: str) -> str | None:
        """Send a command string and return the raw response.

        Thread-safe: only one command can be in-flight on the bus at a time.
        """
        if not self.is_open:
            return None

        with self._lock:
            try:
                # Flush any stale bytes from a previous exchange
                self.serial.reset_input_buffer()
                self.serial.reset_output_buffer()

                # Send
                self.serial.write(command.encode("ascii"))
                self.serial.flush()

                # Small delay for device to process
                time.sleep(self.RESPONSE_DELAY)

                # Read response until CR
                response = b""
                start = time.time()
                while (time.time() - start) < self.timeout:
                    if self.serial.in_waiting > 0:
                        chunk = self.serial.read(self.serial.in_waiting)
                        response += chunk
                        if b"\r" in response:
                            break
                    else:
                        time.sleep(0.01)

                if response:
                    decoded = response.decode("ascii", errors="replace").strip()
                    logger.debug("TX: %r  RX: %r", command.strip(), decoded)
                    return decoded

            except (serial.SerialException, OSError) as e:
                logger.error("Communication error on %s: %s", self.port, e)

            return None

    def get_current_reading(self, address: int | None = None) -> float | None:
        """Query and return the current process reading."""
        cmd = self.protocol.cmd_get_current_reading(address)
        resp = self.send_command(cmd)
        if resp and "Failed" not in resp:
            return self.protocol.parse_reading_response(resp)
        return None

    def get_peak_reading(self, address: int | None = None) -> float | None:
        cmd = self.protocol.cmd_get_peak_reading(address)
        resp = self.send_command(cmd)
        if resp and "Failed" not in resp:
            return self.protocol.parse_reading_response(resp)
        return None

    def get_valley_reading(self, address: int | None = None) -> float | None:
        cmd = self.protocol.cmd_get_valley_reading(address)
        resp = self.send_command(cmd)
        if resp and "Failed" not in resp:
            return self.protocol.parse_reading_response(resp)
        return None

    def get_version(self, address: int | None = None) -> str | None:
        cmd = self.protocol.cmd_get_version(address)
        resp = self.send_command(cmd)
        if resp and "Failed" not in resp:
            return self.protocol.parse_version_response(resp)
        return None

    def get_run_mode(self, address: int | None = None) -> str | None:
        cmd = self.protocol.cmd_get_run_mode(address)
        resp = self.send_command(cmd)
        if resp and "Failed" not in resp:
            return self.protocol.parse_run_mode_response(resp)
        return None

    def get_input_config(self, address: int | None = None) -> dict | None:
        cmd = self.protocol.cmd_read_input_config(address)
        resp = self.send_command(cmd)
        if resp and "Failed" not in resp:
            return self.protocol.parse_input_config_response(resp)
        return None

    def get_display_config(self, address: int | None = None) -> dict | None:
        cmd = self.protocol.cmd_read_display_config(address)
        resp = self.send_command(cmd)
        if resp and "Failed" not in resp:
            return self.protocol.parse_display_config_response(resp)
        return None

    def get_setpoint1(self, address: int | None = None) -> float | None:
        cmd = self.protocol.cmd_get_setpoint1(address)
        resp = self.send_command(cmd)
        if resp and "Failed" not in resp:
            return self.protocol.parse_reading_response(resp)
        return None

    def probe(self, address: int | None = None) -> bool:
        """
        Probe whether a device responds at the given address.
        Returns True if a valid response is received.
        """
        cmd = self.protocol.cmd_get_version(address)
        resp = self.send_command(cmd)
        if resp and "Failed" not in resp and len(resp) > 2:
            # Check for at least some hex-looking content
            return True
        return False

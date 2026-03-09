"""
PMON Modbus RTU Driver — Omega DP16Pt / CNPt Series Process Monitors.

Uses pymodbus to communicate over a shared RS-485 bus.
One ModbusSerialClient instance is shared across all unit addresses.

Register map (from CNPt Series Programming User's Guide Modbus Interface):
  0x0210  Process Value      (2 regs, IEEE 754 float, big-endian)
  0x0240  System Status      (1 reg)
  0x0248  Reading Config     (1 reg)
  0x0400  Setpoint 1         (2 regs, IEEE 754 float, big-endian)
"""

import struct
import logging
import threading
import time

from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusIOException

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Register addresses
# ---------------------------------------------------------------------------
REG_PROCESS_VALUE = 0x0210   # 2 regs  → IEEE 754 float
REG_STATUS        = 0x0240   # 1 reg
REG_RDGCNF        = 0x0248   # 1 reg   (2 = FFF.F, 3 = FFFF)
REG_SETPOINT1     = 0x0400   # 2 regs  → IEEE 754 float

# Status codes
STATUS_RUNNING = 0x0006

STATUS_NAMES = {
    0x0000: "LOAD",
    0x0001: "IDLE",
    0x0002: "INPUT_ADJ",
    0x0003: "CTRL_ADJ",
    0x0004: "MODIFY",
    0x0005: "WAIT",
    0x0006: "RUNNING",
    0x0007: "STANDBY",
    0x0008: "STOP",
    0x0009: "PAUSE",
    0x000A: "FAULT",
    0x000B: "SHUTDOWN",
    0x000C: "AUTOTUNE",
}

# PT-100 RTD plausibility range
MIN_TEMP = -90   # °C
MAX_TEMP = 500   # °C


class PMONModbusConnection:
    """
    Single Modbus RTU connection shared by all units on one RS-485 bus.

    Thread-safe: every register transaction acquires ``_lock``.
    """

    INTER_COMMAND_DELAY = 0.05   # 50 ms bus turnaround

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        parity: str = "N",
        databits: int = 8,
        stopbits: int = 1,
        timeout: float = 0.3,
    ):
        self.port = port
        self.baudrate = baudrate
        self.parity = parity
        self.databits = databits
        self.stopbits = stopbits
        self.timeout = timeout

        self.client = ModbusSerialClient(
            port=port,
            baudrate=baudrate,
            bytesize=databits,
            parity=parity,
            stopbits=stopbits,
            timeout=timeout,
        )
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    @property
    def is_open(self) -> bool:
        return self.client.is_socket_open()

    def open(self) -> bool:
        """Open the serial port (idempotent)."""
        if self.is_open:
            return True
        try:
            ok = self.client.connect()
            if ok:
                logger.info("Opened %s @ %d %d%s%d (Modbus RTU)",
                            self.port, self.baudrate, self.databits,
                            self.parity, self.stopbits)
            else:
                logger.error("Failed to open %s", self.port)
            return ok
        except Exception as e:
            logger.error("Error opening %s: %s", self.port, e)
            return False

    def close(self):
        """Close the serial port."""
        try:
            if self.is_open:
                self.client.close()
                logger.info("Closed %s", self.port)
        except Exception as e:
            logger.error("Error closing %s: %s", self.port, e)

    # ------------------------------------------------------------------
    # Low-level register helpers
    # ------------------------------------------------------------------

    def _read_float_register(self, register: int, unit: int) -> float | None:
        """Read 2 holding registers and decode as big-endian IEEE 754 float."""
        with self._lock:
            try:
                # Clear stale data
                if hasattr(self.client, "serial") and self.client.serial:
                    self.client.serial.reset_input_buffer()

                resp = self.client.read_holding_registers(
                    address=register, count=2, device_id=unit,
                )
                time.sleep(self.INTER_COMMAND_DELAY)

                if resp.isError():
                    return None

                raw = struct.pack(">HH", resp.registers[0], resp.registers[1])
                value = struct.unpack(">f", raw)[0]
                return value

            except (ModbusIOException, Exception) as e:
                logger.debug("Float read failed reg=0x%04X unit=%d: %s", register, unit, e)
                return None

    def _read_u16_register(self, register: int, unit: int) -> int | None:
        """Read a single holding register and return its 16-bit value."""
        with self._lock:
            try:
                if hasattr(self.client, "serial") and self.client.serial:
                    self.client.serial.reset_input_buffer()

                resp = self.client.read_holding_registers(
                    address=register, count=1, device_id=unit,
                )
                time.sleep(self.INTER_COMMAND_DELAY)

                if resp.isError():
                    return None

                return resp.registers[0]

            except (ModbusIOException, Exception) as e:
                logger.debug("U16 read failed reg=0x%04X unit=%d: %s", register, unit, e)
                return None

    # ------------------------------------------------------------------
    # High-level queries
    # ------------------------------------------------------------------

    def read_temperature(self, unit: int) -> float | None:
        """
        Read the current process value (temperature) from a unit.

        Returns the temperature in °C, or None on error.
        """
        value = self._read_float_register(REG_PROCESS_VALUE, unit)
        if value is None:
            return None

        # Sanity: near-zero often means comms glitch
        if abs(value) < 0.001:
            logger.debug("Unit %d: near-zero reading %.6f — likely comms error", unit, value)
            return None

        # Range check
        if not (MIN_TEMP <= value <= MAX_TEMP):
            logger.debug("Unit %d: out-of-range %.2f", unit, value)
            return None

        return value

    def read_status(self, unit: int) -> int | None:
        """Read the system status register. Returns raw 16-bit value."""
        return self._read_u16_register(REG_STATUS, unit)

    def read_status_name(self, unit: int) -> str | None:
        """Read the system status and return a human-readable name."""
        val = self.read_status(unit)
        if val is None:
            return None
        return STATUS_NAMES.get(val, f"UNKNOWN(0x{val:04X})")

    def read_setpoint1(self, unit: int) -> float | None:
        """Read setpoint 1 (2-register float)."""
        return self._read_float_register(REG_SETPOINT1, unit)

    def read_config(self, unit: int) -> int | None:
        """Read reading configuration register (decimal format)."""
        return self._read_u16_register(REG_RDGCNF, unit)

    def probe(self, unit: int) -> bool:
        """Check if a unit responds on the bus (reads status register)."""
        val = self.read_status(unit)
        return val is not None

    # ------------------------------------------------------------------
    # Convenience: serial params string
    # ------------------------------------------------------------------

    @property
    def serial_params_str(self) -> str:
        return f"{self.baudrate} {self.databits}{self.parity}{self.stopbits}"

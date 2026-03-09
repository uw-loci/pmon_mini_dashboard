"""
Auto-discovery module for Omega DP16Pt / CNPt Process Monitors (Modbus RTU).

Scans available COM ports to detect connected PMON units by probing
their Modbus status register at each slave address.
"""

import serial.tools.list_ports
import logging
from dataclasses import dataclass, field

from pmon_modbus import PMONModbusConnection

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredDevice:
    """Represents a discovered PMON unit on the Modbus bus."""

    port: str
    address: int           # Modbus slave ID (1-247)
    baudrate: int
    parity: str
    databits: int
    stopbits: int
    status: int | None = None
    label: str = ""

    def __post_init__(self):
        if not self.label:
            self.label = f"Unit {self.address} ({self.port})"

    @property
    def serial_params_str(self) -> str:
        return f"{self.baudrate} {self.databits}{self.parity}{self.stopbits}"

    @property
    def connection_key(self) -> str:
        return f"{self.port}:{self.address}:{self.baudrate}:{self.databits}{self.parity}{self.stopbits}"


def list_available_ports() -> list[dict]:
    """List all available COM ports with descriptions."""
    ports = []
    for p in serial.tools.list_ports.comports():
        ports.append({
            "port": p.device,
            "description": p.description or "",
            "hwid": p.hwid or "",
            "manufacturer": p.manufacturer or "",
        })
    ports.sort(key=lambda x: x["port"])
    return ports


def discover_devices_on_port(
    port: str,
    baudrate: int = 9600,
    parity: str = "N",
    databits: int = 8,
    stopbits: int = 1,
    max_address: int = 10,
    progress_callback=None,
    timeout: float = 0.3,
) -> list[DiscoveredDevice]:
    """
    Probe Modbus slave addresses 1..max_address on the given COM port.

    Returns a list of DiscoveredDevice for every unit that responds.
    """
    devices: list[DiscoveredDevice] = []

    conn = PMONModbusConnection(
        port=port,
        baudrate=baudrate,
        parity=parity,
        databits=databits,
        stopbits=stopbits,
        timeout=timeout,
    )

    try:
        if not conn.open():
            logger.error("Cannot open %s for discovery", port)
            return devices

        for addr in range(1, max_address + 1):
            if progress_callback:
                progress_callback(f"Probing {port} slave {addr}/{max_address}...")

            if conn.probe(addr):
                status = conn.read_status(addr)
                dev = DiscoveredDevice(
                    port=port,
                    address=addr,
                    baudrate=baudrate,
                    parity=parity,
                    databits=databits,
                    stopbits=stopbits,
                    status=status,
                )
                devices.append(dev)
                logger.info("Found device: %s (status=0x%04X)", dev.label,
                            status if status is not None else 0)
    finally:
        conn.close()

    return devices

"""
PMON Mini Dashboard - Omega Platinum Series Temperature Monitor Dashboard.

A tkinter-based GUI dashboard for monitoring up to 6 Omega Platinum Series
temperature controllers/monitors over serial connections.

Features:
- COM port selection with refresh
- Manual and automatic serial parameter configuration
- Auto-discovery of devices (protocol detection + address scanning)
- Real-time temperature monitoring with configurable poll interval
- Peak/valley tracking per device
- Setpoint display
- Connection status indicators
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import time
import logging
from datetime import datetime
from dataclasses import dataclass, field

from pmon_modbus import PMONModbusConnection, STATUS_NAMES
from auto_discover import (
    list_available_ports,
    discover_devices_on_port,
    DiscoveredDevice,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_MONITORS = 6
DEFAULT_POLL_INTERVAL_MS = 2000  # 2 seconds
MIN_POLL_INTERVAL_MS = 500
MAX_POLL_INTERVAL_MS = 30000

COLORS = {
    "bg": "#1e1e2e",
    "card_bg": "#2d2d44",
    "card_border": "#3d3d5c",
    "text": "#e0e0e0",
    "text_dim": "#8888aa",
    "accent": "#6c9bd2",
    "good": "#4caf50",
    "warn": "#ff9800",
    "error": "#f44336",
    "temp_normal": "#00e676",
    "temp_high": "#ff5252",
    "temp_low": "#448aff",
    "header_bg": "#252540",
    "button": "#4a6fa5",
    "button_hover": "#5a8fd5",
    "entry_bg": "#3a3a55",
}


@dataclass
class MonitorState:
    """Runtime state for a single monitor."""

    device: DiscoveredDevice | None = None
    slave_id: int = 0               # Modbus slave address (1-247)
    current_temp: float | None = None
    peak_temp: float | None = None
    valley_temp: float | None = None
    setpoint: float | None = None
    run_mode: str | None = None
    units: str = "°C"
    is_connected: bool = False
    last_update: datetime | None = None
    error_count: int = 0
    consecutive_errors: int = 0
    read_count: int = 0
    last_good_temp: float | None = None
    history: list = field(default_factory=list)


class Dashboard:
    """Main dashboard application."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("PMON Mini Dashboard — Modbus RTU Monitor")
        self.root.geometry("1280x820")
        self.root.minsize(1000, 700)
        self.root.configure(bg=COLORS["bg"])

        # State
        self.monitors: list[MonitorState] = [MonitorState() for _ in range(MAX_MONITORS)]
        self._modbus_conn: PMONModbusConnection | None = None
        self.polling = False
        self.poll_interval_ms = DEFAULT_POLL_INTERVAL_MS
        self._poll_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._discovery_thread: threading.Thread | None = None

        # GUI refs
        self.monitor_frames: list[dict] = []
        self.log_text: scrolledtext.ScrolledText | None = None

        self._build_ui()
        self._refresh_ports()
        self._setup_default_devices()

    # ==================================================================
    # UI Construction
    # ==================================================================

    def _build_ui(self):
        # Style
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background=COLORS["bg"])
        style.configure("Card.TFrame", background=COLORS["card_bg"])
        style.configure(
            "TLabel", background=COLORS["bg"], foreground=COLORS["text"], font=("Segoe UI", 10)
        )
        style.configure(
            "Card.TLabel",
            background=COLORS["card_bg"],
            foreground=COLORS["text"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "Header.TLabel",
            background=COLORS["header_bg"],
            foreground=COLORS["accent"],
            font=("Segoe UI", 12, "bold"),
        )
        style.configure(
            "Temp.TLabel",
            background=COLORS["card_bg"],
            foreground=COLORS["temp_normal"],
            font=("Consolas", 28, "bold"),
        )
        style.configure(
            "Unit.TLabel",
            background=COLORS["card_bg"],
            foreground=COLORS["text_dim"],
            font=("Consolas", 14),
        )
        style.configure(
            "Status.TLabel",
            background=COLORS["card_bg"],
            foreground=COLORS["text_dim"],
            font=("Segoe UI", 9),
        )
        style.configure(
            "Connected.TLabel",
            background=COLORS["card_bg"],
            foreground=COLORS["good"],
            font=("Segoe UI", 9, "bold"),
        )
        style.configure(
            "Disconnected.TLabel",
            background=COLORS["card_bg"],
            foreground=COLORS["error"],
            font=("Segoe UI", 9, "bold"),
        )
        style.configure(
            "TButton",
            font=("Segoe UI", 10),
        )
        style.configure(
            "Accent.TButton",
            font=("Segoe UI", 10, "bold"),
        )

        # Main layout: top bar + monitor grid + bottom log
        self._build_top_bar()
        self._build_monitor_grid()
        self._build_bottom_panel()

    def _build_top_bar(self):
        """Connection and control bar at the top."""
        top = tk.Frame(self.root, bg=COLORS["header_bg"], pady=6, padx=10)
        top.pack(fill=tk.X, side=tk.TOP)

        # --- Port selection ---
        tk.Label(top, text="COM Port:", bg=COLORS["header_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 10)).pack(side=tk.LEFT, padx=(0, 4))

        self.port_var = tk.StringVar()
        self.port_combo = ttk.Combobox(top, textvariable=self.port_var, width=12, state="readonly")
        self.port_combo.pack(side=tk.LEFT, padx=(0, 4))

        btn_refresh = ttk.Button(top, text="⟳", width=3, command=self._refresh_ports)
        btn_refresh.pack(side=tk.LEFT, padx=(0, 10))

        # --- Serial params ---
        tk.Label(top, text="Baud:", bg=COLORS["header_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 10)).pack(side=tk.LEFT, padx=(0, 2))
        self.baud_var = tk.StringVar(value="9600")
        baud_combo = ttk.Combobox(
            top, textvariable=self.baud_var, width=7, state="readonly",
            values=["300", "600", "1200", "2400", "4800", "9600", "19200", "38400", "57600", "115200"],
        )
        baud_combo.pack(side=tk.LEFT, padx=(0, 6))

        tk.Label(top, text="Parity:", bg=COLORS["header_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 10)).pack(side=tk.LEFT, padx=(0, 2))
        self.parity_var = tk.StringVar(value="N")
        parity_combo = ttk.Combobox(
            top, textvariable=self.parity_var, width=5, state="readonly",
            values=["N", "O", "E"],
        )
        parity_combo.pack(side=tk.LEFT, padx=(0, 6))

        tk.Label(top, text="Data:", bg=COLORS["header_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 10)).pack(side=tk.LEFT, padx=(0, 2))
        self.databits_var = tk.StringVar(value="8")
        databits_combo = ttk.Combobox(
            top, textvariable=self.databits_var, width=3, state="readonly",
            values=["7", "8"],
        )
        databits_combo.pack(side=tk.LEFT, padx=(0, 6))

        tk.Label(top, text="Stop:", bg=COLORS["header_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 10)).pack(side=tk.LEFT, padx=(0, 2))
        self.stopbits_var = tk.StringVar(value="1")
        stopbits_combo = ttk.Combobox(
            top, textvariable=self.stopbits_var, width=3, state="readonly",
            values=["1", "2"],
        )
        stopbits_combo.pack(side=tk.LEFT, padx=(0, 10))

        # --- Address scan range ---
        tk.Label(top, text="Addr scan 0–", bg=COLORS["header_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 10)).pack(side=tk.LEFT, padx=(0, 0))
        self.max_addr_var = tk.StringVar(value="10")
        addr_entry = ttk.Entry(top, textvariable=self.max_addr_var, width=4)
        addr_entry.pack(side=tk.LEFT, padx=(0, 10))

        # --- Buttons ---
        self.btn_discover = ttk.Button(
            top, text="📡 Discover Devices", command=self._on_discover, style="Accent.TButton"
        )
        self.btn_discover.pack(side=tk.LEFT, padx=(0, 4))

        self.btn_connect = ttk.Button(
            top, text="▶ Start Monitoring", command=self._on_toggle_polling, style="Accent.TButton"
        )
        self.btn_connect.pack(side=tk.LEFT, padx=(0, 4))

        self.btn_disconnect_all = ttk.Button(
            top, text="⏹ Disconnect All", command=self._on_disconnect_all
        )
        self.btn_disconnect_all.pack(side=tk.LEFT, padx=(0, 4))

        # --- Poll interval ---
        tk.Label(top, text="Poll(s):", bg=COLORS["header_bg"], fg=COLORS["text"],
                 font=("Segoe UI", 10)).pack(side=tk.RIGHT, padx=(4, 2))
        self.poll_var = tk.StringVar(value=str(DEFAULT_POLL_INTERVAL_MS / 1000))
        poll_entry = ttk.Entry(top, textvariable=self.poll_var, width=5)
        poll_entry.pack(side=tk.RIGHT, padx=(0, 0))

    def _build_monitor_grid(self):
        """Build the 2x3 grid of monitor cards."""
        grid_frame = tk.Frame(self.root, bg=COLORS["bg"], padx=10, pady=6)
        grid_frame.pack(fill=tk.BOTH, expand=True, side=tk.TOP)

        for i in range(2):
            grid_frame.rowconfigure(i, weight=1)
        for j in range(3):
            grid_frame.columnconfigure(j, weight=1)

        for idx in range(MAX_MONITORS):
            row, col = divmod(idx, 3)
            card = self._build_monitor_card(grid_frame, idx)
            card["frame"].grid(row=row, column=col, padx=5, pady=5, sticky="nsew")

    def _build_monitor_card(self, parent, index: int) -> dict:
        """Build a single monitor card widget."""
        card = tk.Frame(parent, bg=COLORS["card_bg"], highlightbackground=COLORS["card_border"],
                        highlightthickness=1, padx=10, pady=8)

        refs = {"frame": card}

        # Header row
        header = tk.Frame(card, bg=COLORS["card_bg"])
        header.pack(fill=tk.X, pady=(0, 4))

        refs["title"] = tk.Label(
            header, text=f"Monitor #{index + 1}", bg=COLORS["card_bg"],
            fg=COLORS["accent"], font=("Segoe UI", 11, "bold"), anchor="w"
        )
        refs["title"].pack(side=tk.LEFT)

        refs["status_indicator"] = tk.Label(
            header, text="● OFFLINE", bg=COLORS["card_bg"],
            fg=COLORS["error"], font=("Segoe UI", 9, "bold"), anchor="e"
        )
        refs["status_indicator"].pack(side=tk.RIGHT)

        # Temperature display
        temp_frame = tk.Frame(card, bg=COLORS["card_bg"])
        temp_frame.pack(fill=tk.X, pady=(4, 4))

        refs["temp_value"] = tk.Label(
            temp_frame, text="---.-", bg=COLORS["card_bg"],
            fg=COLORS["temp_normal"], font=("Consolas", 32, "bold"), anchor="w"
        )
        refs["temp_value"].pack(side=tk.LEFT)

        refs["temp_unit"] = tk.Label(
            temp_frame, text="°C", bg=COLORS["card_bg"],
            fg=COLORS["text_dim"], font=("Consolas", 16), anchor="sw"
        )
        refs["temp_unit"].pack(side=tk.LEFT, padx=(2, 0), pady=(8, 0))

        # Stats row (peak / valley / setpoint)
        stats = tk.Frame(card, bg=COLORS["card_bg"])
        stats.pack(fill=tk.X, pady=(2, 2))

        for label_text, key in [("Peak:", "peak"), ("Valley:", "valley"), ("SP:", "setpoint")]:
            f = tk.Frame(stats, bg=COLORS["card_bg"])
            f.pack(side=tk.LEFT, expand=True, fill=tk.X)
            tk.Label(f, text=label_text, bg=COLORS["card_bg"], fg=COLORS["text_dim"],
                     font=("Segoe UI", 9)).pack(side=tk.LEFT)
            refs[key] = tk.Label(f, text="---", bg=COLORS["card_bg"], fg=COLORS["text"],
                                 font=("Consolas", 10))
            refs[key].pack(side=tk.LEFT, padx=(2, 8))

        # Info row (sensor type, firmware, run mode)
        info = tk.Frame(card, bg=COLORS["card_bg"])
        info.pack(fill=tk.X, pady=(4, 0))

        refs["info_line1"] = tk.Label(
            info, text="Sensor: --  |  FW: --", bg=COLORS["card_bg"],
            fg=COLORS["text_dim"], font=("Segoe UI", 9), anchor="w"
        )
        refs["info_line1"].pack(fill=tk.X)

        refs["info_line2"] = tk.Label(
            info, text="Port: --  |  State: --", bg=COLORS["card_bg"],
            fg=COLORS["text_dim"], font=("Segoe UI", 9), anchor="w"
        )
        refs["info_line2"].pack(fill=tk.X)

        # Last update
        refs["last_update"] = tk.Label(
            card, text="Last: --", bg=COLORS["card_bg"],
            fg=COLORS["text_dim"], font=("Segoe UI", 8), anchor="e"
        )
        refs["last_update"].pack(fill=tk.X, pady=(4, 0))

        self.monitor_frames.append(refs)
        return refs

    def _build_bottom_panel(self):
        """Build the log/status panel at the bottom."""
        bottom = tk.Frame(self.root, bg=COLORS["bg"], padx=10, pady=6)
        bottom.pack(fill=tk.X, side=tk.BOTTOM)

        # Status bar
        status_bar = tk.Frame(bottom, bg=COLORS["header_bg"], pady=2, padx=6)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM, pady=(4, 0))

        self.status_label = tk.Label(
            status_bar, text="Ready. Select COM port and discover devices.",
            bg=COLORS["header_bg"], fg=COLORS["text_dim"], font=("Segoe UI", 9), anchor="w"
        )
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.device_count_label = tk.Label(
            status_bar, text="Devices: 0/6",
            bg=COLORS["header_bg"], fg=COLORS["accent"], font=("Segoe UI", 9, "bold"), anchor="e"
        )
        self.device_count_label.pack(side=tk.RIGHT)

        # Log area (collapsible)
        log_frame = tk.LabelFrame(
            bottom, text=" Communication Log ", bg=COLORS["bg"],
            fg=COLORS["text_dim"], font=("Segoe UI", 9), padx=4, pady=4
        )
        log_frame.pack(fill=tk.X, side=tk.BOTTOM)

        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=6, bg=COLORS["entry_bg"], fg=COLORS["text"],
            font=("Consolas", 9), state=tk.DISABLED, wrap=tk.WORD
        )
        self.log_text.pack(fill=tk.X)

        btn_clear = ttk.Button(log_frame, text="Clear Log", command=self._clear_log)
        btn_clear.pack(side=tk.RIGHT, pady=(2, 0))

    # ==================================================================
    # Logging
    # ==================================================================

    def _log(self, msg: str, level: str = "INFO"):
        """Add a message to the GUI log."""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        line = f"[{timestamp}] {level}: {msg}\n"

        def _update():
            if self.log_text:
                self.log_text.configure(state=tk.NORMAL)
                self.log_text.insert(tk.END, line)
                self.log_text.see(tk.END)
                self.log_text.configure(state=tk.DISABLED)

        self.root.after(0, _update)

    def _clear_log(self):
        if self.log_text:
            self.log_text.configure(state=tk.NORMAL)
            self.log_text.delete("1.0", tk.END)
            self.log_text.configure(state=tk.DISABLED)

    def _set_status(self, msg: str):
        """Update the status bar."""
        self.root.after(0, lambda: self.status_label.configure(text=msg))

    # ==================================================================
    # Port management
    # ==================================================================

    def _refresh_ports(self):
        """Refresh the list of available COM ports."""
        ports = list_available_ports()
        port_names = [p["port"] for p in ports]
        self.port_combo["values"] = port_names

        if port_names and not self.port_var.get():
            self.port_var.set(port_names[0])

        self._log(f"Found {len(port_names)} COM port(s): {', '.join(port_names) if port_names else 'none'}")

        for p in ports:
            if p["description"]:
                self._log(f"  {p['port']}: {p['description']}")

    # ==================================================================
    # Device discovery (Modbus RTU)
    # ==================================================================

    def _on_discover(self):
        """Discover PMON units on the selected port via Modbus."""
        port = self.port_var.get()
        if not port:
            messagebox.showwarning("No Port", "Please select a COM port first.")
            return

        if self._discovery_thread and self._discovery_thread.is_alive():
            messagebox.showinfo("Busy", "A scan is already in progress.")
            return

        if self.polling:
            self._stop_polling()

        self.btn_discover.configure(state=tk.DISABLED)

        baudrate = int(self.baud_var.get())
        parity = self.parity_var.get()
        databits = int(self.databits_var.get())
        stopbits = int(self.stopbits_var.get())

        try:
            max_addr = int(self.max_addr_var.get())
        except ValueError:
            max_addr = 10

        self._log(f"Discovering Modbus devices on {port} @ {baudrate} {databits}{parity}{stopbits}, "
                  f"slave addresses 1-{max_addr}...")
        self._set_status(f"Scanning {port}...")

        def _worker():
            try:
                devices = discover_devices_on_port(
                    port=port,
                    baudrate=baudrate,
                    parity=parity,
                    databits=databits,
                    stopbits=stopbits,
                    max_address=max_addr,
                    progress_callback=lambda msg: self.root.after(0, lambda m=msg: self._set_status(m)),
                    timeout=0.3,
                )
                self.root.after(0, lambda: self._apply_discovered_devices(devices))
            except Exception as e:
                self._log(f"Discovery error: {e}", "ERROR")
                self._set_status(f"Discovery failed: {e}")
            finally:
                self.root.after(0, lambda: self.btn_discover.configure(state=tk.NORMAL))

        self._discovery_thread = threading.Thread(target=_worker, daemon=True)
        self._discovery_thread.start()

    def _apply_discovered_devices(self, devices: list[DiscoveredDevice]):
        """Assign discovered devices to monitor slots."""
        self._disconnect_all()

        count = min(len(devices), MAX_MONITORS)
        if count > 0:
            # Build one shared Modbus connection for all units on same port/params
            dev0 = devices[0]
            self._modbus_conn = PMONModbusConnection(
                port=dev0.port,
                baudrate=dev0.baudrate,
                parity=dev0.parity,
                databits=dev0.databits,
                stopbits=dev0.stopbits,
            )

        for i in range(MAX_MONITORS):
            if i < count:
                dev = devices[i]
                self.monitors[i] = MonitorState(device=dev, slave_id=dev.address)
                self._update_card_device_info(i)
                self._log(f"Slot {i + 1}: {dev.label} @ {dev.serial_params_str}")
            else:
                self._reset_card(i)

        self.device_count_label.configure(text=f"Devices: {count}/{MAX_MONITORS}")
        self._set_status(f"Found {count} device(s). Click 'Start Monitoring' to begin.")
        if count == 0:
            self._log("No devices found. Check connections and serial parameters.", "WARN")

    def _update_card_device_info(self, idx: int):
        """Update a monitor card with device info."""
        refs = self.monitor_frames[idx]
        mon = self.monitors[idx]
        dev = mon.device

        if dev is None:
            self._reset_card(idx)
            return

        refs["title"].configure(text=f"Monitor #{idx + 1} — Slave {dev.address}")
        refs["info_line1"].configure(text=f"Modbus RTU  |  Slave ID: {dev.address}")

        port_str = f"Port: {dev.port}"
        refs["info_line2"].configure(text=f"{port_str} @ {dev.serial_params_str}  |  State: --")
        refs["status_indicator"].configure(text="● READY", fg=COLORS["warn"])

    def _reset_card(self, idx: int):
        """Reset a monitor card to default empty state."""
        refs = self.monitor_frames[idx]
        refs["title"].configure(text=f"Monitor #{idx + 1}")
        refs["status_indicator"].configure(text="● OFFLINE", fg=COLORS["error"])
        refs["temp_value"].configure(text="---.-", fg=COLORS["text_dim"])
        refs["temp_unit"].configure(text="°C")
        refs["peak"].configure(text="---")
        refs["valley"].configure(text="---")
        refs["setpoint"].configure(text="---")
        refs["info_line1"].configure(text="Sensor: --  |  FW: --")
        refs["info_line2"].configure(text="Port: --  |  State: --")
        refs["last_update"].configure(text="Last: --")

        self.monitors[idx] = MonitorState()

    # ==================================================================
    # Default device setup
    # ==================================================================

    def _setup_default_devices(self):
        """Pre-configure monitor slots for device addresses 1-6 on the selected port."""
        port = self.port_var.get()
        if not port:
            self._log("No COM port available — skipping default device setup.", "WARN")
            return

        self._disconnect_all()

        baudrate = int(self.baud_var.get())
        parity = self.parity_var.get()
        databits = int(self.databits_var.get())
        stopbits = int(self.stopbits_var.get())

        self._modbus_conn = PMONModbusConnection(
            port=port,
            baudrate=baudrate,
            parity=parity,
            databits=databits,
            stopbits=stopbits,
        )

        for i in range(MAX_MONITORS):
            addr = i + 1  # slave addresses 1 through 6
            dev = DiscoveredDevice(
                port=port,
                address=addr,
                baudrate=baudrate,
                parity=parity,
                databits=databits,
                stopbits=stopbits,
                label=f"Unit {addr} ({port})",
            )
            self.monitors[i] = MonitorState(device=dev, slave_id=addr)
            self._update_card_device_info(i)

        self.device_count_label.configure(text=f"Devices: {MAX_MONITORS}/{MAX_MONITORS}")
        self._log(f"Default devices configured: Units 1-{MAX_MONITORS} on {port} @ {baudrate} {databits}{parity}{stopbits}")
        self._set_status(f"Units 1-{MAX_MONITORS} ready on {port}. Click 'Start Monitoring' to begin.")

    # ==================================================================
    # Polling / monitoring (Modbus RTU)
    # ==================================================================

    def _on_toggle_polling(self):
        """Toggle monitoring on/off."""
        if self.polling:
            self._stop_polling()
        else:
            self._start_polling()

    def _start_polling(self):
        """Start polling all configured monitors over Modbus."""
        # Auto-setup if no devices configured
        active = [m for m in self.monitors if m.device is not None]
        if not active:
            self._setup_default_devices()
            active = [m for m in self.monitors if m.device is not None]
            if not active:
                messagebox.showwarning("No Devices", "No devices configured and no COM port available.")
                return

        # Parse poll interval
        try:
            interval = float(self.poll_var.get())
            self.poll_interval_ms = max(MIN_POLL_INTERVAL_MS,
                                         min(int(interval * 1000), MAX_POLL_INTERVAL_MS))
        except ValueError:
            self.poll_interval_ms = DEFAULT_POLL_INTERVAL_MS

        # Open the single shared Modbus connection
        if self._modbus_conn and not self._modbus_conn.is_open:
            if self._modbus_conn.open():
                self._log(f"Modbus connected on {self._modbus_conn.port}")
            else:
                self._log(f"Failed to open {self._modbus_conn.port}", "ERROR")
                messagebox.showerror("Connection Failed",
                                     f"Cannot open {self._modbus_conn.port}.\n"
                                     "Check that no other application is using the port.")
                return

        # Mark all monitors as connected (they share one bus)
        for mon in self.monitors:
            if mon.device:
                mon.is_connected = self._modbus_conn.is_open if self._modbus_conn else False

        self.polling = True
        self._stop_event.clear()
        self.btn_connect.configure(text="⏸ Stop Monitoring")
        self._set_status("Monitoring active (Modbus RTU)...")

        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def _stop_polling(self):
        """Stop the polling loop."""
        self.polling = False
        self._stop_event.set()
        self.btn_connect.configure(text="▶ Start Monitoring")
        self._set_status("Monitoring stopped.")
        self._log("Monitoring stopped.")

    def _poll_loop(self):
        """Background polling loop — reads all units sequentially over one COM port."""
        while not self._stop_event.is_set():
            conn = self._modbus_conn
            if conn is None or not conn.is_open:
                # Try to reopen once
                if conn and not conn.open():
                    for idx, mon in enumerate(self.monitors):
                        if mon.device:
                            mon.is_connected = False
                            self.root.after(0, lambda i=idx: self._update_card_values(i))
                    self._stop_event.wait(2.0)
                    continue

            for idx, mon in enumerate(self.monitors):
                if self._stop_event.is_set():
                    break
                if mon.device is None:
                    continue
                self._poll_single(idx, mon, conn)

            # Wait for next poll cycle
            self._stop_event.wait(self.poll_interval_ms / 1000.0)

    def _poll_single(self, idx: int, mon: MonitorState, conn: PMONModbusConnection):
        """Poll a single PMON unit via Modbus RTU."""
        unit = mon.slave_id

        try:
            # Read temperature (process value — 2 regs, IEEE 754)
            temp = conn.read_temperature(unit)
            if temp is not None:
                mon.current_temp = temp
                mon.last_good_temp = temp
                mon.read_count += 1
                mon.last_update = datetime.now()
                mon.consecutive_errors = 0

                # Track peak / valley
                if mon.peak_temp is None or temp > mon.peak_temp:
                    mon.peak_temp = temp
                if mon.valley_temp is None or temp < mon.valley_temp:
                    mon.valley_temp = temp

                # History (last 60 readings)
                mon.history.append(temp)
                if len(mon.history) > 60:
                    mon.history.pop(0)
            else:
                raise ValueError("No temperature reading")

            # Status + setpoint (less frequently — every 5th read)
            if mon.read_count % 5 == 0:
                status_name = conn.read_status_name(unit)
                if status_name:
                    mon.run_mode = status_name

                sp = conn.read_setpoint1(unit)
                if sp is not None:
                    mon.setpoint = sp

            mon.is_connected = True
            mon.error_count = 0

        except Exception as e:
            mon.consecutive_errors += 1
            mon.error_count += 1

            # Keep last good reading for a few errors
            if mon.consecutive_errors < 5 and mon.last_good_temp is not None:
                mon.current_temp = mon.last_good_temp
            else:
                mon.is_connected = False

            # Rate-limit error logging
            if mon.consecutive_errors <= 3 or mon.consecutive_errors % 10 == 0:
                self._log(f"Poll error slot {idx + 1} (unit {unit}): {e}", "ERROR")

        # Update UI
        self.root.after(0, lambda i=idx: self._update_card_values(i))

    def _update_card_values(self, idx: int):
        """Update a monitor card with the latest readings (called on main thread)."""
        refs = self.monitor_frames[idx]
        mon = self.monitors[idx]

        if mon.device is None:
            return

        # Status indicator
        if mon.is_connected:
            refs["status_indicator"].configure(text="● ONLINE", fg=COLORS["good"])
        elif mon.consecutive_errors > 0 and mon.last_good_temp is not None:
            refs["status_indicator"].configure(text="● STALE", fg=COLORS["warn"])
        else:
            refs["status_indicator"].configure(text="● OFFLINE", fg=COLORS["error"])

        # Temperature
        if mon.current_temp is not None:
            temp_str = f"{mon.current_temp:.1f}"
            color = COLORS["temp_normal"]

            if mon.setpoint is not None:
                diff = abs(mon.current_temp - mon.setpoint)
                if diff < 2.0:
                    color = COLORS["temp_normal"]
                elif mon.current_temp > mon.setpoint:
                    color = COLORS["temp_high"]
                else:
                    color = COLORS["temp_low"]

            refs["temp_value"].configure(text=temp_str, fg=color)
        else:
            refs["temp_value"].configure(text="---.-", fg=COLORS["text_dim"])

        # Peak / Valley / Setpoint
        refs["peak"].configure(text=f"{mon.peak_temp:.1f}" if mon.peak_temp is not None else "---")
        refs["valley"].configure(text=f"{mon.valley_temp:.1f}" if mon.valley_temp is not None else "---")
        refs["setpoint"].configure(text=f"{mon.setpoint:.1f}" if mon.setpoint is not None else "---")

        # Info line 2
        dev = mon.device
        state_str = mon.run_mode or "--"
        refs["info_line2"].configure(
            text=f"Port: {dev.port} @ {dev.serial_params_str}  |  State: {state_str}"
        )

        # Last update
        if mon.last_update:
            refs["last_update"].configure(
                text=f"Last: {mon.last_update.strftime('%H:%M:%S')}  "
                     f"(reads: {mon.read_count}, errors: {mon.error_count})"
            )

    # ==================================================================
    # Disconnect
    # ==================================================================

    def _on_disconnect_all(self):
        """Disconnect all monitors."""
        self._stop_polling()
        self._disconnect_all()
        self._set_status("All devices disconnected.")

    def _disconnect_all(self):
        """Close the shared Modbus connection."""
        if self._modbus_conn and self._modbus_conn.is_open:
            self._modbus_conn.close()

        for idx, mon in enumerate(self.monitors):
            mon.is_connected = False
            if mon.device:
                refs = self.monitor_frames[idx]
                refs["status_indicator"].configure(text="● OFFLINE", fg=COLORS["error"])

    # ==================================================================
    # Cleanup
    # ==================================================================

    def on_closing(self):
        """Handle window close."""
        self._stop_polling()
        self._disconnect_all()
        self.root.destroy()


def main():
    """Entry point for the dashboard."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    root = tk.Tk()
    app = Dashboard(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
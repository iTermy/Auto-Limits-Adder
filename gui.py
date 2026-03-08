"""
gui.py — Settings & launcher GUI for the Auto-Execution Bot.

On first run (no license key stored), a license activation dialog is shown
before the main settings window. Settings are persisted to config.json and
the .env file. The user can launch the bot directly from the GUI.
"""

import json
import os
import subprocess
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, scrolledtext, ttk

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent.resolve()
CONFIG_PATH = BASE_DIR / "config.json"
ENV_PATH    = BASE_DIR / ".env"

# Instruments that are always excluded (not supported by broker)
MANDATORY_EXCLUDED = {"USOILSPOT"}

# ─────────────────────────────────────────────
# Colour palette — muted financial dark theme
# ─────────────────────────────────────────────
BG        = "#1a1d23"
BG2       = "#22262e"
BG3       = "#2a2f3a"
BORDER    = "#3a3f4c"
FG        = "#d4d8e0"
FG_DIM    = "#7a8090"
ACCENT    = "#4a90d9"
ACCENT_HO = "#5ba0e8"
RED       = "#c0392b"
RED_HO    = "#d44444"
GREEN     = "#27ae60"
FONT_BODY  = ("Segoe UI", 9)
FONT_SMALL = ("Segoe UI", 8)
FONT_BOLD  = ("Segoe UI", 9, "bold")
FONT_TITLE = ("Segoe UI", 11, "bold")
FONT_MONO  = ("Consolas", 8)

# ─────────────────────────────────────────────
# Config helpers
# ─────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_config(cfg: dict) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

def load_env() -> dict:
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env

def save_env(env: dict) -> None:
    lines = [f"{k}={v}" for k, v in env.items() if v]
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

def get_license_key() -> str:
    return load_config().get("license", {}).get("key", "").strip()

def set_license_key(key: str) -> None:
    cfg = load_config()
    cfg.setdefault("license", {})["key"] = key.strip()
    save_config(cfg)

# ─────────────────────────────────────────────
# Widget helpers
# ─────────────────────────────────────────────

def make_frame(parent, bg=None, **kwargs) -> tk.Frame:
    return tk.Frame(parent, bg=bg or BG, **kwargs)

def make_label(parent, text, bold=False, dim=False, title=False, **kwargs) -> tk.Label:
    kwargs.pop("tip", None)
    kwargs.setdefault("fg", FG_DIM if dim else FG)
    kwargs.setdefault("font", FONT_TITLE if title else (FONT_BOLD if bold else FONT_BODY))
    try:
        bg = kwargs.pop("bg", None) or parent["bg"]
    except Exception:
        bg = BG
    return tk.Label(parent, text=text, bg=bg, **kwargs)

def make_entry(parent, width=26, show=None, **kwargs) -> tk.Entry:
    return tk.Entry(
        parent, width=width, bg=BG3, fg=FG, insertbackground=FG,
        relief="flat", bd=0, font=FONT_BODY, highlightthickness=1,
        highlightbackground=BORDER, highlightcolor=ACCENT, show=show or "", **kwargs
    )

def make_button(parent, text, command, accent=False, danger=False, **kwargs) -> tk.Button:
    bg = ACCENT if accent else (RED if danger else BG3)
    ho = ACCENT_HO if accent else (RED_HO if danger else BORDER)
    btn = tk.Button(
        parent, text=text, command=command,
        bg=bg, fg=FG, activebackground=ho, activeforeground=FG,
        relief="flat", bd=0, font=FONT_BOLD, cursor="hand2",
        padx=12, pady=5, **kwargs
    )
    btn.bind("<Enter>", lambda e: btn.config(bg=ho))
    btn.bind("<Leave>", lambda e: btn.config(bg=bg))
    return btn

def make_combobox(parent, values, width=18, **kwargs) -> ttk.Combobox:
    style = ttk.Style()
    style.theme_use("clam")
    style.configure("Dark.TCombobox",
        fieldbackground=BG3, background=BG3, foreground=FG,
        selectbackground=ACCENT, selectforeground=FG,
        bordercolor=BORDER, arrowcolor=FG_DIM, relief="flat"
    )
    return ttk.Combobox(parent, values=values, width=width, style="Dark.TCombobox",
                        state="readonly", font=FONT_BODY, **kwargs)

def make_spinbox(parent, from_, to, increment=1.0, width=10, **kwargs) -> tk.Spinbox:
    return tk.Spinbox(
        parent, from_=from_, to=to, increment=increment, width=width,
        bg=BG3, fg=FG, insertbackground=FG, buttonbackground=BG3,
        relief="flat", bd=0, font=FONT_BODY, highlightthickness=1,
        highlightbackground=BORDER, highlightcolor=ACCENT, **kwargs
    )

def make_checkbutton(parent, variable) -> tk.Checkbutton:
    return tk.Checkbutton(
        parent, variable=variable,
        bg=BG, activebackground=BG, selectcolor=BG3,
        relief="flat", bd=0, cursor="hand2"
    )

def section_header(parent, text: str) -> None:
    f = make_frame(parent)
    f.pack(fill="x", padx=16, pady=(14, 4))
    make_label(f, text, bold=True).pack(side="left")
    tk.Frame(f, bg=BORDER, height=1).pack(
        side="left", fill="x", expand=True, padx=(8, 0), pady=4)

def labeled_row(parent, label_text: str, widget_factory) -> tk.Widget:
    f = make_frame(parent)
    f.pack(fill="x", padx=24, pady=3)
    make_label(f, label_text).pack(side="left")
    w = widget_factory(f)
    w.pack(side="right")
    return w

def info_note(parent, text: str) -> None:
    make_label(parent, text, dim=True, font=FONT_SMALL,
               wraplength=500, justify="left").pack(anchor="w", padx=24, pady=(0, 4))

# ─────────────────────────────────────────────
# Tooltip
# ─────────────────────────────────────────────
_tip_win = None

def _show_tooltip(widget, text):
    global _tip_win
    _hide_tooltip()
    x = widget.winfo_rootx()
    y = widget.winfo_rooty() + 22
    _tip_win = tw = tk.Toplevel(widget)
    tw.wm_overrideredirect(True)
    tw.wm_geometry(f"+{x}+{y}")
    tk.Label(tw, text=text, bg=BG3, fg=FG, font=FONT_SMALL,
             relief="flat", padx=6, pady=3, wraplength=280, justify="left").pack()

def _hide_tooltip():
    global _tip_win
    if _tip_win:
        _tip_win.destroy()
        _tip_win = None

# ─────────────────────────────────────────────
# License Activation Dialog
# ─────────────────────────────────────────────

class LicenseDialog(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.result = None
        self.title("Activate License")
        self.configure(bg=BG)
        self.resizable(False, False)
        self.grab_set()

        pad = make_frame(self)
        pad.pack(padx=32, pady=28, fill="both", expand=True)

        make_label(pad, "Auto-Execution Bot", title=True).pack(anchor="w")
        make_label(pad, "Enter your license key to activate.", dim=True).pack(
            anchor="w", pady=(2, 18))

        make_label(pad, "License Key").pack(anchor="w")
        self._key_var = tk.StringVar()
        key_entry = make_entry(pad, width=42)
        key_entry.config(textvariable=self._key_var)
        key_entry.pack(fill="x", pady=(2, 4))
        key_entry.focus_set()

        self._err_lbl = tk.Label(pad, text="", bg=BG, fg=RED, font=FONT_SMALL)
        self._err_lbl.pack(anchor="w")

        make_label(pad, "Your license key is provided by the bot admin via Discord.",
                   dim=True, font=FONT_SMALL, wraplength=340, justify="left").pack(
                   anchor="w", pady=(4, 18))

        bf = make_frame(pad)
        bf.pack(fill="x")
        make_button(bf, "Cancel", self.destroy).pack(side="right", padx=(6, 0))
        make_button(bf, "Activate", self._activate, accent=True).pack(side="right")

        self._key_var.trace_add("write", lambda *_: self._err_lbl.config(text=""))
        key_entry.bind("<Return>", lambda e: self._activate())
        self.after(50, self._center)

    def _center(self):
        self.update_idletasks()
        pw = self.master.winfo_x() + self.master.winfo_width() // 2
        ph = self.master.winfo_y() + self.master.winfo_height() // 2
        w, h = self.winfo_reqwidth(), self.winfo_reqheight()
        self.geometry(f"+{pw - w // 2}+{ph - h // 2}")

    def _activate(self):
        key = self._key_var.get().strip()
        if len(key) != 32 or not all(c in "0123456789abcdefABCDEF" for c in key):
            self._err_lbl.config(text="Key must be a 32-character hex string.")
            return
        set_license_key(key)
        self.result = key
        self.destroy()

# ─────────────────────────────────────────────
# Instrument Exclude List Editor
# ─────────────────────────────────────────────

class InstrumentExcludeEditor(tk.Frame):
    """
    Listbox editor for excluded instruments.
    USOILSPOT is always present and cannot be removed.
    """
    def __init__(self, parent, items: list, **kwargs):
        super().__init__(parent, bg=BG, **kwargs)
        merged = list(MANDATORY_EXCLUDED)
        for i in items:
            if i.upper() not in MANDATORY_EXCLUDED:
                merged.append(i.upper())
        self._items = merged
        self._build()

    def _build(self):
        list_frame = tk.Frame(self, bg=BG3, highlightbackground=BORDER,
                              highlightthickness=1)
        list_frame.pack(fill="both", expand=True)
        sb = tk.Scrollbar(list_frame, bg=BG3, troughcolor=BG2,
                          relief="flat", width=10, bd=0)
        self._lb = tk.Listbox(
            list_frame, bg=BG3, fg=FG, selectbackground=ACCENT,
            selectforeground=FG, relief="flat", bd=0, font=FONT_BODY,
            activestyle="none", height=5, yscrollcommand=sb.set
        )
        sb.config(command=self._lb.yview)
        sb.pack(side="right", fill="y")
        self._lb.pack(fill="both", expand=True, padx=2)
        for item in self._items:
            self._lb.insert(tk.END, item)
            if item in MANDATORY_EXCLUDED:
                self._lb.itemconfig(tk.END, fg=FG_DIM)

        ctrl = make_frame(self)
        ctrl.pack(fill="x", pady=(4, 0))
        self._entry = make_entry(ctrl, width=16)
        self._entry.pack(side="left")
        self._entry.bind("<Return>", lambda e: self._add())
        make_button(ctrl, "Add", self._add).pack(side="left", padx=(4, 0))
        make_button(ctrl, "Remove", self._remove, danger=True).pack(side="left", padx=(4, 0))

    def _add(self):
        val = self._entry.get().strip().upper()
        if val and val not in self._items:
            self._items.append(val)
            self._lb.insert(tk.END, val)
        self._entry.delete(0, tk.END)

    def _remove(self):
        sel = self._lb.curselection()
        if not sel:
            return
        idx = sel[0]
        item = self._items[idx]
        if item in MANDATORY_EXCLUDED:
            messagebox.showwarning("Cannot Remove",
                f"{item} is always excluded — it is not supported by the broker.",
                parent=self)
            return
        self._items.pop(idx)
        self._lb.delete(idx)

    def get_items(self) -> list:
        return list(self._items)

# ─────────────────────────────────────────────
# Per-Instrument TP Override Editor
# ─────────────────────────────────────────────

class PerInstrumentTPEditor(tk.Frame):
    """
    Each row = one instrument with: unit, TP threshold, trail amount.
    Columns: Instrument | Unit | TP Threshold | Trail Amount | [delete]
    """
    def __init__(self, parent,
                 tp_data: dict, tp_unit_data: dict,
                 trail_data: dict, trail_unit_data: dict,
                 partial_pct_var: tk.StringVar,
                 **kwargs):
        super().__init__(parent, bg=BG, **kwargs)
        self._rows = []
        self._build_header()

        instrs = list(dict.fromkeys(list(tp_data.keys()) + list(trail_data.keys())))
        for instr in instrs:
            unit      = tp_unit_data.get(instr, trail_unit_data.get(instr, "pips"))
            tp_val    = tp_data.get(instr, "")
            trail_val = trail_data.get(instr, "")
            self._add_row(instr, unit, tp_val, trail_val)

        make_button(self, "+ Add Instrument Override", self._add_empty).pack(
            anchor="w", pady=(6, 2))

    def _build_header(self):
        hdr = tk.Frame(self, bg=BG2)
        hdr.pack(fill="x")
        for col, w in [("Instrument", 10), ("Unit", 8), ("TP Threshold", 12), ("Trail Amount", 12)]:
            tk.Label(hdr, text=col, bg=BG2, fg=FG_DIM, font=FONT_SMALL,
                     width=w, anchor="w").pack(side="left", padx=(6, 0), pady=3)

    def _add_row(self, instr="", unit="pips", tp_val="", trail_val=""):
        f = tk.Frame(self, bg=BG3, pady=2)
        f.pack(fill="x", pady=1)

        e_instr = make_entry(f, width=10)
        e_instr.insert(0, instr)
        e_instr.pack(side="left", padx=(6, 4))

        cb_unit = make_combobox(f, ["pips", "dollars"], width=8)
        cb_unit.set(unit)
        cb_unit.pack(side="left", padx=(0, 4))

        e_tp = make_entry(f, width=9)
        e_tp.insert(0, str(tp_val))
        e_tp.pack(side="left", padx=(0, 4))

        e_trail = make_entry(f, width=9)
        e_trail.insert(0, str(trail_val))
        e_trail.pack(side="left", padx=(0, 4))

        row_ref = {"frame": f, "instr": e_instr, "unit": cb_unit,
                   "tp": e_tp, "trail": e_trail}
        btn = make_button(f, "✕", lambda r=row_ref: self._del_row(r), danger=True)
        btn.config(padx=6, pady=2)
        btn.pack(side="left")
        self._rows.append(row_ref)

    def _del_row(self, row_ref):
        row_ref["frame"].destroy()
        if row_ref in self._rows:
            self._rows.remove(row_ref)

    def _add_empty(self):
        self._add_row()

    def get_data(self):
        """Returns (tp_vals, tp_units, trail_vals, trail_units)."""
        tp_vals, tp_units, trail_vals, trail_units = {}, {}, {}, {}
        for r in self._rows:
            instr = r["instr"].get().strip().upper()
            unit  = r["unit"].get()
            tp_s  = r["tp"].get().strip()
            tr_s  = r["trail"].get().strip()
            if not instr:
                continue
            if tp_s:
                try:
                    tp_vals[instr]  = float(tp_s)
                    tp_units[instr] = unit
                except ValueError:
                    pass
            if tr_s:
                try:
                    trail_vals[instr]  = float(tr_s)
                    trail_units[instr] = unit
                except ValueError:
                    pass
        return tp_vals, tp_units, trail_vals, trail_units

# ─────────────────────────────────────────────
# Log panel
# ─────────────────────────────────────────────

class LogPanel(tk.Frame):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, bg=BG, **kwargs)
        hdr = make_frame(self)
        hdr.pack(fill="x", padx=8, pady=(8, 4))
        make_label(hdr, "Log Output", bold=True).pack(side="left")
        make_button(hdr, "Clear", self._clear).pack(side="right")

        self._text = scrolledtext.ScrolledText(
            self, bg=BG2, fg=FG, insertbackground=FG, font=FONT_MONO,
            relief="flat", bd=0, state="disabled", wrap="word",
            highlightthickness=1, highlightbackground=BORDER
        )
        self._text.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        self._text.tag_config("INFO",    foreground=FG_DIM)
        self._text.tag_config("WARNING", foreground="#e0a020")
        self._text.tag_config("ERROR",   foreground=RED)
        self._text.tag_config("CRITICAL",foreground=RED)
        self._text.tag_config("DEBUG",   foreground="#506080")

    def append(self, msg: str, level: str = "INFO"):
        self._text.config(state="normal")
        self._text.insert(tk.END, msg + "\n", level)
        self._text.see(tk.END)
        self._text.config(state="disabled")

    def _clear(self):
        self._text.config(state="normal")
        self._text.delete("1.0", tk.END)
        self._text.config(state="disabled")

# ─────────────────────────────────────────────
# Symbol Map Editor
# ─────────────────────────────────────────────

class SymbolMapEditor(tk.Frame):
    def __init__(self, parent, data: dict, **kwargs):
        super().__init__(parent, bg=BG, **kwargs)
        self._rows = []
        hdr = tk.Frame(self, bg=BG2)
        hdr.pack(fill="x")
        for col in ["DB Instrument", "→", "MT5 Symbol"]:
            tk.Label(hdr, text=col, bg=BG2, fg=FG_DIM, font=FONT_SMALL).pack(
                side="left", padx=(8, 0), pady=3)
        self._body = tk.Frame(self, bg=BG)
        self._body.pack(fill="both", expand=True)
        for k, v in data.items():
            self._add_row(k, v)
        make_button(self, "+ Add Mapping", self._add_empty).pack(anchor="w", pady=(4, 0))

    def _add_row(self, k="", v=""):
        f = make_frame(self._body)
        f.pack(fill="x", pady=1)
        ek = make_entry(f, width=14)
        ek.insert(0, k)
        ek.pack(side="left", padx=(0, 4))
        tk.Label(f, text="→", bg=BG, fg=FG_DIM, font=FONT_BODY).pack(side="left", padx=(0, 4))
        ev = make_entry(f, width=14)
        ev.insert(0, v)
        ev.pack(side="left", padx=(0, 4))
        row_ref = {"frame": f, "k": ek, "v": ev}
        btn = make_button(f, "✕", lambda r=row_ref: self._del_row(r), danger=True)
        btn.config(padx=5, pady=2)
        btn.pack(side="left")
        self._rows.append(row_ref)

    def _del_row(self, r):
        r["frame"].destroy()
        if r in self._rows:
            self._rows.remove(r)

    def _add_empty(self):
        self._add_row()

    def get_map(self) -> dict:
        result = {}
        for r in self._rows:
            k = r["k"].get().strip().upper()
            v = r["v"].get().strip()
            if k and v:
                result[k] = v
        return result

# ─────────────────────────────────────────────
# Main Settings Window
# ─────────────────────────────────────────────

class SettingsWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Auto-Execution Bot")
        self.configure(bg=BG)
        self.minsize(720, 620)
        self.geometry("780x720")
        self._bot_proc = None
        self._log_thread = None
        self._running = False

        self._cfg = load_config()
        self._env = load_env()

        self._build()
        self._load_values()
        self._check_license_on_start()
        self._center()

    # ──────── Layout ────────

    def _build(self):
        topbar = tk.Frame(self, bg=BG2, height=46)
        topbar.pack(fill="x")
        topbar.pack_propagate(False)
        make_label(topbar, "  Auto-Execution Bot", title=True, bg=BG2).pack(
            side="left", padx=4, pady=10)
        self._status_lbl = tk.Label(topbar, text="● Stopped",
            bg=BG2, fg=FG_DIM, font=FONT_BODY)
        self._status_lbl.pack(side="right", padx=16)

        nb_style = ttk.Style()
        nb_style.theme_use("clam")
        nb_style.configure("Dark.TNotebook", background=BG, borderwidth=0, tabmargins=0)
        nb_style.configure("Dark.TNotebook.Tab",
            background=BG2, foreground=FG_DIM, padding=[14, 6],
            font=FONT_BODY, borderwidth=0)
        nb_style.map("Dark.TNotebook.Tab",
            background=[("selected", BG)],
            foreground=[("selected", FG)])

        self._nb = ttk.Notebook(self, style="Dark.TNotebook")
        self._nb.pack(fill="both", expand=True)

        self._tab_general    = self._make_scroll_tab("General")
        self._tab_filters    = self._make_scroll_tab("Filters")
        self._tab_execution  = self._make_scroll_tab("Execution")
        self._tab_tp         = self._make_scroll_tab("Take Profit")
        self._tab_connection = self._make_scroll_tab("Connection")

        log_frame = tk.Frame(self._nb, bg=BG)
        self._nb.add(log_frame, text="  Log  ")
        self._log_panel = LogPanel(log_frame)
        self._log_panel.pack(fill="both", expand=True)

        self._build_general(self._tab_general)
        self._build_filters(self._tab_filters)
        self._build_execution(self._tab_execution)
        self._build_tp(self._tab_tp)
        self._build_connection(self._tab_connection)

        botbar = tk.Frame(self, bg=BG2, height=50)
        botbar.pack(fill="x", side="bottom")
        botbar.pack_propagate(False)
        inner = tk.Frame(botbar, bg=BG2)
        inner.pack(side="right", padx=12, pady=8)
        make_button(inner, "Save Settings", self._save, accent=True).pack(side="left", padx=(0, 8))
        self._start_btn = make_button(inner, "▶  Start Bot", self._toggle_bot, accent=True)
        self._start_btn.pack(side="left")

    def _make_scroll_tab(self, title: str) -> tk.Frame:
        outer = tk.Frame(self._nb, bg=BG)
        self._nb.add(outer, text=f"  {title}  ")
        canvas = tk.Canvas(outer, bg=BG, highlightthickness=0, bd=0)
        vsb = tk.Scrollbar(outer, orient="vertical", command=canvas.yview,
                           bg=BG2, troughcolor=BG2, relief="flat", width=10, bd=0)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        inner = tk.Frame(canvas, bg=BG)
        wid = canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(wid, width=e.width))
        canvas.bind_all("<MouseWheel>",
            lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units")
            if canvas.winfo_ismapped() else None)
        return inner

    # ──────── General tab ────────

    def _build_general(self, p):
        section_header(p, "License")
        f = make_frame(p)
        f.pack(fill="x", padx=24, pady=3)
        make_label(f, "License Key").pack(side="left")
        self._license_entry = make_entry(f, width=36, show="*")
        self._license_entry.pack(side="right")

        f2 = make_frame(p)
        f2.pack(fill="x", padx=24, pady=(0, 4))
        self._show_key_var = tk.BooleanVar(value=False)
        tk.Checkbutton(f2, text="Show key", variable=self._show_key_var,
                       bg=BG, fg=FG_DIM, activebackground=BG, selectcolor=BG3,
                       font=FONT_SMALL, cursor="hand2",
                       command=lambda: self._license_entry.config(
                           show="" if self._show_key_var.get() else "*")
                       ).pack(side="right")
        make_label(f2, "Provided by the bot admin via Discord.", dim=True,
                   font=FONT_SMALL).pack(side="left")

    # ──────── Filters tab ────────

    def _build_filters(self, p):
        section_header(p, "Excluded Instruments")
        info_note(p, "Orders will not be placed for instruments in this list. "
                     "USOILSPOT is permanently excluded (not supported by broker).")
        self._inst_editor = InstrumentExcludeEditor(p, [])
        self._inst_editor.pack(fill="x", padx=24, pady=(0, 8))

        section_header(p, "Asset Classes")
        info_note(p, "Tick each asset class you want the bot to trade. "
                     "Unticked classes are ignored entirely.")
        ac_frame = make_frame(p)
        ac_frame.pack(fill="x", padx=24, pady=(0, 8))
        self._ac_vars = {}
        classes = [
            ("metals",  "Metals  (XAUUSD, XAGUSD, etc.)"),
            ("forex",   "Forex   (EURUSD, GBPUSD, etc.)"),
            ("indices", "Indices (SPX500USD, NAS100USD, etc.)"),
            ("crypto",  "Crypto  (BTCUSDT, ETHUSDT, etc.)"),
            ("stocks",  "Stocks  (AMD.NAS, AAPL.NAS, etc.)"),
        ]
        for cls, label in classes:
            var = tk.BooleanVar(value=True)
            self._ac_vars[cls] = var
            tk.Checkbutton(ac_frame, text=label, variable=var,
                           bg=BG, fg=FG, activebackground=BG,
                           selectcolor=BG3, font=FONT_BODY, cursor="hand2"
                           ).pack(anchor="w", pady=1)

        section_header(p, "Scalp Signals")
        f = make_frame(p)
        f.pack(fill="x", padx=24, pady=3)
        make_label(f, "Include scalp signals").pack(side="left")
        self._scalp_var = tk.BooleanVar(value=True)
        make_checkbutton(f, self._scalp_var).pack(side="right")

        section_header(p, "Proximity Filter")
        info_note(p, "Only place orders within the specified pip distance of the current price.")
        self._prox_default = labeled_row(p, "Default (pips)",
            lambda f: make_spinbox(f, 10, 10000, 50))

        make_label(p, "Per asset class overrides", dim=True,
                   font=FONT_SMALL).pack(anchor="w", padx=24, pady=(8, 2))
        self._prox_ac = {}
        prox_defaults = [
            ("metals",  "Metals (pips)",  500),
            ("forex",   "Forex (pips)",   200),
            ("indices", "Indices (pips)", 1000),
            ("crypto",  "Crypto (pips)",  2000),
            ("stocks",  "Stocks (pips)",  300),
        ]
        for cls, label, _ in prox_defaults:
            sb = labeled_row(p, f"  {label}",
                lambda f: make_spinbox(f, 10, 50000, 50))
            self._prox_ac[cls] = sb

    # ──────── Execution tab ────────

    def _build_execution(self, p):
        section_header(p, "Risk & Lot Sizing")
        self._risk_pct = labeled_row(p, "Risk per signal (%)",
            lambda f: make_spinbox(f, 0.1, 100, 0.5, width=10, format="%.1f"))
        self._min_lot = labeled_row(p, "Minimum lot size",
            lambda f: make_spinbox(f, 0.01, 1.0, 0.01, width=10, format="%.2f"))
        self._lot_recheck = labeled_row(p, "Lot size recheck interval (s)",
            lambda f: make_spinbox(f, 30, 3600, 30))
        info_note(p, "Lot sizes are recalculated periodically from current account balance. "
                     "60–120 s is recommended.")

        section_header(p, "Feed Offset  (Indices & Crypto only)")
        info_note(p,
            "Indices (SPX500, NAS100) and crypto (BTC, ETH) are priced differently between "
            "your signal feed (OANDA / Binance) and ICMarkets MT5. The bot automatically "
            "computes the difference and places orders at the correct MT5-equivalent price. "
            "These settings control how often to re-check for drift and re-place if needed. "
            "The defaults are fine for most setups.")
        self._max_stale = labeled_row(p, "Max price data age before skipping (s)",
            lambda f: make_spinbox(f, 5, 120, 5))
        self._readjust_interval = labeled_row(p, "Offset re-check interval (s)",
            lambda f: make_spinbox(f, 10, 600, 10))
        self._readjust_threshold = labeled_row(p, "Re-place order if drift exceeds (pips)",
            lambda f: make_spinbox(f, 0.5, 50, 0.5, width=10, format="%.1f"))

    # ──────── Take Profit tab ────────

    def _build_tp(self, p):
        section_header(p, "Defaults")
        info_note(p,
            "Default values apply to all instruments not listed in the overrides below. "
            "Forex and stocks use pips. Gold, Silver, Indices, and Crypto use dollars "
            "— set per-instrument overrides accordingly.")

        self._tp_default = labeled_row(p, "TP threshold (profit level that triggers close)",
            lambda f: make_spinbox(f, 0.1, 10000, 0.5, width=10, format="%.1f"))
        self._trail_default = labeled_row(p, "Trail amount (distance to trail remaining position)",
            lambda f: make_spinbox(f, 0.1, 10000, 0.5, width=10, format="%.1f"))
        self._partial_pct = labeled_row(p, "Partial close %  (remainder is trailed)",
            lambda f: make_spinbox(f, 10, 100, 5, width=10))
        info_note(p, "e.g. 50% means close half at TP, trail the other half.")

        section_header(p, "Per-Instrument Overrides")
        info_note(p,
            "Set instrument-specific unit, TP threshold, and trail amount. "
            "Leave a field blank to inherit the default above. "
            "Both TP threshold and trail amount use the same unit per instrument.")
        self._partial_pct_var = tk.StringVar(value="50")
        self._tp_override_editor = PerInstrumentTPEditor(
            p, {}, {}, {}, {}, self._partial_pct_var)
        self._tp_override_editor.pack(fill="x", padx=24, pady=(4, 12))

    # ──────── Connection tab ────────

    def _build_connection(self, p):
        section_header(p, "Supabase Database")
        info_note(p, "The database URL is stored only in your local .env file and is never "
                     "displayed in this window. Keep .env out of shared or synced folders.")
        f_url = make_frame(p)
        f_url.pack(fill="x", padx=24, pady=3)
        make_label(f_url, "Database URL").pack(side="left")
        self._db_url_status = tk.Label(f_url, text="", bg=BG, fg=FG_DIM, font=FONT_SMALL)
        self._db_url_status.pack(side="right", padx=(0, 10))
        make_button(f_url, "Set / Update URL", self._prompt_db_url).pack(side="right")

        section_header(p, "MT5 Credentials")
        info_note(p,
            "Leave all three fields blank to attach to the already-running MT5 terminal "
            "using the account you are currently logged in to. "
            "Fill them in only if you need the bot to authenticate programmatically "
            "(e.g. running headless or on a VPS).")
        self._mt5_login    = labeled_row(p, "Login (account number)",
            lambda f: make_entry(f, width=20))
        self._mt5_password = labeled_row(p, "Password",
            lambda f: make_entry(f, width=20, show="*"))
        self._mt5_server   = labeled_row(p, "Server",
            lambda f: make_entry(f, width=20))
        f3 = make_frame(p)
        f3.pack(fill="x", padx=24, pady=(2, 0))
        self._show_mt5_var = tk.BooleanVar(value=False)
        tk.Checkbutton(f3, text="Show password", variable=self._show_mt5_var,
                       bg=BG, fg=FG_DIM, activebackground=BG, selectcolor=BG3,
                       font=FONT_SMALL, cursor="hand2",
                       command=lambda: self._mt5_password.config(
                           show="" if self._show_mt5_var.get() else "*")
                       ).pack(anchor="w")

        section_header(p, "Symbol Map")
        info_note(p, "Maps DB instrument names to MT5 symbol names. "
                     "Stocks get -24 appended automatically. "
                     "Everything else defaults to the DB name uppercased.")
        self._sym_editor = SymbolMapEditor(p, {})
        self._sym_editor.pack(fill="x", padx=24, pady=(4, 16))

    def _prompt_db_url(self):
        dlg = tk.Toplevel(self)
        dlg.title("Set Database URL")
        dlg.configure(bg=BG)
        dlg.resizable(False, False)
        dlg.grab_set()

        pad = make_frame(dlg)
        pad.pack(padx=28, pady=24, fill="both", expand=True)
        make_label(pad, "Supabase Database URL", bold=True).pack(anchor="w")
        make_label(pad, "Saved to .env — not displayed elsewhere.", dim=True,
                   font=FONT_SMALL).pack(anchor="w", pady=(2, 12))
        entry = make_entry(pad, width=58)
        entry.insert(0, self._env.get("SUPABASE_DB_URL", ""))
        entry.pack(fill="x", pady=(0, 12))
        entry.focus_set()

        def _do_save():
            val = entry.get().strip()
            if val:
                self._env["SUPABASE_DB_URL"] = val
                save_env(self._env)
                self._update_db_url_status()
            dlg.destroy()

        bf = make_frame(pad)
        bf.pack(fill="x")
        make_button(bf, "Cancel", dlg.destroy).pack(side="right", padx=(6, 0))
        make_button(bf, "Save", _do_save, accent=True).pack(side="right")
        entry.bind("<Return>", lambda e: _do_save())

        def _center_dlg():
            dlg.update_idletasks()
            x = self.winfo_x() + self.winfo_width()//2 - dlg.winfo_reqwidth()//2
            y = self.winfo_y() + self.winfo_height()//2 - dlg.winfo_reqheight()//2
            dlg.geometry(f"+{x}+{y}")
        dlg.after(50, _center_dlg)

    def _update_db_url_status(self):
        if self._env.get("SUPABASE_DB_URL", "").strip():
            self._db_url_status.config(text="✓ URL is set", fg=GREEN)
        else:
            self._db_url_status.config(text="Not set", fg=RED)

    # ──────── Load / Save ────────

    def _load_values(self):
        cfg = self._cfg
        env = self._env

        # General
        self._license_entry.delete(0, tk.END)
        self._license_entry.insert(0, cfg.get("license", {}).get("key", ""))

        # Filters
        filt     = cfg.get("filters", {})
        inst_cfg = filt.get("instruments", {})
        saved_list = inst_cfg.get("list", [])
        self._inst_editor._items = list(MANDATORY_EXCLUDED)
        for i in saved_list:
            if i.upper() not in MANDATORY_EXCLUDED:
                self._inst_editor._items.append(i.upper())
        self._inst_editor._lb.delete(0, tk.END)
        for item in self._inst_editor._items:
            self._inst_editor._lb.insert(tk.END, item)
            if item in MANDATORY_EXCLUDED:
                self._inst_editor._lb.itemconfig(tk.END, fg=FG_DIM)

        ac_cfg  = filt.get("asset_classes", {})
        ac_mode = ac_cfg.get("mode", "all")
        ac_list = ac_cfg.get("list", [])
        for cls, var in self._ac_vars.items():
            if ac_mode == "all":
                var.set(True)
            elif ac_mode == "include":
                var.set(cls in ac_list)
            elif ac_mode == "exclude":
                var.set(cls not in ac_list)

        self._scalp_var.set(filt.get("scalp_signals", True))

        prox    = cfg.get("proximity_filter", {})
        self._prox_default.delete(0, tk.END)
        self._prox_default.insert(0, str(prox.get("default_pips", 500)))
        per_ac   = prox.get("per_asset_class", {})
        defaults = {"metals": 500, "forex": 200, "indices": 1000, "crypto": 2000, "stocks": 300}
        for cls, sb in self._prox_ac.items():
            sb.delete(0, tk.END)
            sb.insert(0, str(per_ac.get(cls, defaults[cls])))

        # Execution
        ex = cfg.get("execution", {})
        self._risk_pct.delete(0, tk.END)
        self._risk_pct.insert(0, str(ex.get("risk_percent", 5.0)))
        self._min_lot.delete(0, tk.END)
        self._min_lot.insert(0, str(ex.get("min_lot", 0.01)))
        self._lot_recheck.delete(0, tk.END)
        self._lot_recheck.insert(0, str(ex.get("lot_recheck_interval_seconds", 120)))

        lp = cfg.get("live_prices", {})
        self._max_stale.delete(0, tk.END)
        self._max_stale.insert(0, str(lp.get("max_staleness_seconds", 30)))
        self._readjust_interval.delete(0, tk.END)
        self._readjust_interval.insert(0, str(lp.get("offset_readjust_interval_seconds", 60)))
        self._readjust_threshold.delete(0, tk.END)
        self._readjust_threshold.insert(0, str(lp.get("offset_readjust_threshold_pips", 2.0)))

        # TP
        tp  = cfg.get("tp", {})
        pt  = tp.get("profit_threshold", {})
        tr  = tp.get("trail", {})
        self._tp_default.delete(0, tk.END)
        self._tp_default.insert(0, str(pt.get("default", 7)))
        self._trail_default.delete(0, tk.END)
        self._trail_default.insert(0, str(tr.get("default", 3)))
        self._partial_pct.delete(0, tk.END)
        self._partial_pct.insert(0, str(tp.get("partial_close_percent", 50)))

        self._tp_override_editor.destroy()
        self._tp_override_editor = PerInstrumentTPEditor(
            self._tab_tp,
            pt.get("per_instrument", {}),
            pt.get("per_instrument_unit", {}),
            tr.get("per_instrument", {}),
            tr.get("per_instrument_unit", {}),
            self._partial_pct_var,
        )
        self._tp_override_editor.pack(fill="x", padx=24, pady=(4, 12))

        # Connection
        self._update_db_url_status()
        self._mt5_login.delete(0, tk.END)
        self._mt5_login.insert(0, env.get("MT5_LOGIN", ""))
        self._mt5_password.delete(0, tk.END)
        self._mt5_password.insert(0, env.get("MT5_PASSWORD", ""))
        self._mt5_server.delete(0, tk.END)
        self._mt5_server.insert(0, env.get("MT5_SERVER", ""))

        self._sym_editor.destroy()
        self._sym_editor = SymbolMapEditor(self._tab_connection, cfg.get("symbol_map", {}))
        self._sym_editor.pack(fill="x", padx=24, pady=(4, 16))

    def _save(self):
        cfg = self._cfg

        cfg["poll_interval_seconds"] = 5
        cfg.setdefault("license", {})["key"] = self._license_entry.get().strip()

        included_ac = [cls for cls, var in self._ac_vars.items() if var.get()]
        cfg["filters"] = {
            "instruments": {
                "mode": "exclude",
                "list": self._inst_editor.get_items(),
            },
            "asset_classes": {
                "mode": "include",
                "list": included_ac,
            },
            "directions": "both",
            "scalp_signals": self._scalp_var.get(),
            "signal_types": "all",
        }

        defaults_order = ["metals", "forex", "indices", "crypto", "stocks"]
        default_pips   = [500, 200, 1000, 2000, 300]
        per_ac_prox = {
            cls: _safe_int(self._prox_ac[cls].get(), dp)
            for cls, dp in zip(defaults_order, default_pips)
        }
        cfg["proximity_filter"] = {
            "enabled": True,
            "default_pips": _safe_int(self._prox_default.get(), 500),
            "per_asset_class": per_ac_prox,
            "per_instrument": cfg.get("proximity_filter", {}).get("per_instrument", {}),
        }

        cfg["execution"] = {
            "risk_percent": _safe_float(self._risk_pct.get(), 5.0),
            "min_lot": _safe_float(self._min_lot.get(), 0.01),
            "skip_if_price_past_limit": True,
            "place_all_limits_simultaneously": True,
            "lot_recheck_interval_seconds": _safe_int(self._lot_recheck.get(), 120),
        }
        cfg["live_prices"] = {
            "max_staleness_seconds": _safe_int(self._max_stale.get(), 30),
            "offset_readjust_interval_seconds": _safe_int(self._readjust_interval.get(), 60),
            "offset_readjust_threshold_pips": _safe_float(self._readjust_threshold.get(), 2.0),
        }

        tp_vals, tp_units, trail_vals, trail_units = self._tp_override_editor.get_data()
        cfg["tp"] = {
            "profit_threshold": {
                "unit": "pips",
                "default": _safe_float(self._tp_default.get(), 7),
                "per_instrument": tp_vals,
                "per_instrument_unit": tp_units,
            },
            "trail": {
                "unit": "pips",
                "default": _safe_float(self._trail_default.get(), 3),
                "per_instrument": trail_vals,
                "per_instrument_unit": trail_units,
            },
            "partial_close_percent": _safe_int(self._partial_pct.get(), 50),
        }

        cfg["symbol_map"] = self._sym_editor.get_map()
        save_config(cfg)
        self._cfg = cfg

        # MT5 creds only — SUPABASE_DB_URL is managed via _prompt_db_url
        login = self._mt5_login.get().strip()
        pwd   = self._mt5_password.get().strip()
        srv   = self._mt5_server.get().strip()
        for key, val in [("MT5_LOGIN", login), ("MT5_PASSWORD", pwd), ("MT5_SERVER", srv)]:
            if val:
                self._env[key] = val
            elif key in self._env:
                del self._env[key]
        save_env(self._env)

        self._log_panel.append("✓ Settings saved.", "INFO")
        messagebox.showinfo("Saved", "Settings saved successfully.", parent=self)

    # ──────── License check on start ────────

    def _check_license_on_start(self):
        if not get_license_key():
            self.after(120, self._prompt_license)

    def _prompt_license(self):
        dlg = LicenseDialog(self)
        self.wait_window(dlg)
        if dlg.result:
            self._license_entry.delete(0, tk.END)
            self._license_entry.insert(0, dlg.result)
            self._cfg = load_config()
            self._log_panel.append("License key saved.", "INFO")

    # ──────── Bot process control ────────

    def _toggle_bot(self):
        if self._running:
            self._stop_bot()
        else:
            self._start_bot()

    def _start_bot(self):
        if not self._cfg.get("license", {}).get("key", "").strip():
            messagebox.showerror("License Required",
                "Please enter a valid license key before starting the bot.", parent=self)
            self._nb.select(0)
            return
        if not self._env.get("SUPABASE_DB_URL", "").strip():
            messagebox.showerror("Database URL Required",
                "Please set the Supabase database URL in the Connection tab.", parent=self)
            self._nb.select(4)
            return

        self._save()

        self._bot_proc = subprocess.Popen(
            [sys.executable, str(BASE_DIR / "main.py")],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=str(BASE_DIR), encoding="utf-8", errors="replace",
        )
        self._running = True
        self._start_btn.config(text="■  Stop Bot", bg=RED)
        self._start_btn.bind("<Enter>", lambda e: self._start_btn.config(bg=RED_HO))
        self._start_btn.bind("<Leave>", lambda e: self._start_btn.config(bg=RED))
        self._status_lbl.config(text="● Running", fg=GREEN)
        self._log_panel.append("Bot started.", "INFO")
        self._nb.select(5)

        self._log_thread = threading.Thread(target=self._stream_logs, daemon=True)
        self._log_thread.start()
        self.after(500, self._poll_bot_status)

    def _stop_bot(self):
        if self._bot_proc and self._bot_proc.poll() is None:
            self._bot_proc.terminate()
            self._log_panel.append("Bot stop requested.", "WARNING")
        self._running = False
        self._start_btn.config(text="▶  Start Bot", bg=ACCENT)
        self._start_btn.bind("<Enter>", lambda e: self._start_btn.config(bg=ACCENT_HO))
        self._start_btn.bind("<Leave>", lambda e: self._start_btn.config(bg=ACCENT))
        self._status_lbl.config(text="● Stopped", fg=FG_DIM)

    def _stream_logs(self):
        if not self._bot_proc:
            return
        for line in self._bot_proc.stdout:
            line = line.rstrip()
            ll = line.lower()
            level = ("CRITICAL" if "critical" in ll else
                     "ERROR"    if "error"    in ll else
                     "WARNING"  if "warning"  in ll else
                     "DEBUG"    if "debug"    in ll else "INFO")
            self.after(0, lambda ln=line, lv=level: self._log_panel.append(ln, lv))

    def _poll_bot_status(self):
        if not self._running:
            return
        if self._bot_proc and self._bot_proc.poll() is not None:
            rc = self._bot_proc.returncode
            self._log_panel.append(f"Bot exited (code {rc}).",
                                   "WARNING" if rc else "INFO")
            self._stop_bot()
            return
        self.after(1000, self._poll_bot_status)

    # ──────── Utilities ────────

    def _center(self):
        self.update_idletasks()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        w, h   = self.winfo_width(), self.winfo_height()
        self.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")

    def on_close(self):
        if self._running:
            if not messagebox.askokcancel("Bot is running",
                    "The bot is still running. Stop it and exit?", parent=self):
                return
            self._stop_bot()
        self.destroy()

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _safe_float(val: str, default: float) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def _safe_int(val: str, default: int) -> int:
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    app = SettingsWindow()
    app.protocol("WM_DELETE_WINDOW", app.on_close)
    app.mainloop()
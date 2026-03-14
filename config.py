"""
config.py — Default config definition and config file helpers.

Kept separate from gui.py and main.py to avoid circular imports when
both modules need access to config utilities.
"""

import json
import sys
from pathlib import Path

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────

BASE_DIR    = (Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent).resolve()
CONFIG_PATH = BASE_DIR / "config.json"


# ─────────────────────────────────────────────
# Default config
# ─────────────────────────────────────────────

def default_config() -> dict:
    return {
        "poll_interval_seconds": 5,
        "filters": {
            "instruments": {
                "mode": "exclude",
                "list": ["USOILSPOT"]
            },
            "asset_classes": {
                "mode": "include",
                "list": ["metals", "forex", "indices", "crypto", "stocks"]
            },
            "directions": "both",
            "scalp_signals": True,
            "signal_types": "all"
        },
        "proximity_filter": {
            "enabled": True,
            "default_pips": 50,
            "per_asset_class": {
                "metals": 30,
                "forex": 40,
                "indices": 100,
                "crypto": 2000,
                "stocks": 10
            },
            "per_instrument": {
                "XAUUSD": 30
            }
        },
        "execution": {
            "risk_percent": 5.0,
            "min_lot": 0.01,
            "skip_if_price_past_limit": True,
            "place_all_limits_simultaneously": True,
            "lot_recheck_interval_seconds": 120
        },
        "tp": {
            "defaults": {
                "forex":     {"type": "pips",    "value": 7.0,   "trail": 3.0,  "description": "Standard forex pairs"},
                "forex_jpy": {"type": "pips",    "value": 7.0,   "trail": 3.0,  "description": "JPY pairs (auto-detected)"},
                "metals":    {"type": "dollars", "value": 7.0,   "trail": 3.0,  "description": "Gold, Silver, etc."},
                "indices":   {"type": "dollars", "value": 20.0,  "trail": 10.0, "description": "Stock indices"},
                "stocks":    {"type": "dollars", "value": 1.0,   "trail": 0.5,  "description": "Individual stocks"},
                "crypto":    {"type": "dollars", "value": 200.0, "trail": 50.0, "description": "Cryptocurrencies"},
                "oil":       {"type": "dollars", "value": 0.5,   "trail": 0.2,  "description": "Oil commodities"}
            },
            "scalp_defaults": {
                "forex":     {"type": "pips",    "value": 5.0,   "trail": 2.0,  "description": "Scalp - Standard forex pairs"},
                "forex_jpy": {"type": "pips",    "value": 5.0,   "trail": 2.0,  "description": "Scalp - JPY pairs (auto-detected)"},
                "metals":    {"type": "dollars", "value": 4.0,   "trail": 2.0,  "description": "Scalp - Gold, Silver, etc."},
                "indices":   {"type": "dollars", "value": 10.0,  "trail": 5.0,  "description": "Scalp - Stock indices"},
                "stocks":    {"type": "dollars", "value": 0.5,   "trail": 0.25, "description": "Scalp - Individual stocks"},
                "crypto":    {"type": "dollars", "value": 100.0, "trail": 25.0, "description": "Scalp - Cryptocurrencies"},
                "oil":       {"type": "dollars", "value": 0.2,   "trail": 0.1,  "description": "Scalp - Oil commodities"}
            },
            "overrides": {
                "NAS100USD": {"type": "dollars", "value": 50.0, "trail": 20.0},
                "US30USD":   {"type": "dollars", "value": 50.0, "trail": 25.0}
            },
            "scalp_overrides": {
                "NAS100USD": {"type": "dollars", "value": 25.0, "trail": 10.0},
                "US30USD":   {"type": "dollars", "value": 30.0, "trail": 15.0}
            },
            "partial_close_percent": 50
        },
        "symbol_map": {
            "XAUUSD":    "XAUUSD",
            "XAGUSD":    "XAGUSD",
            "SPX500USD": "US500",
            "NAS100USD": "USTEC",
            "BTCUSDT":   "BTCUSD",
            "ETHUSDT":   "ETHUSD",
            "EURUSD":    "EURUSD"
        },
        "live_prices": {
            "max_staleness_seconds": 30,
            "offset_readjust_interval_seconds": 60,
            "offset_readjust_threshold_pips": 2.0
        },
        "license": {
            "key": ""
        }
    }


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    # Auto-create with defaults on first run
    cfg = default_config()
    save_config(cfg)
    return cfg


def save_config(cfg: dict) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
"""
main.py — Auto-Execution Bot entry point.

Loads config and environment, connects to MT5 and Supabase, runs startup
reconciliation, then enters the main sync loop.

Usage:
    python main.py

Requirements:
    - Windows (MetaTrader5 package is Windows-only)
    - MT5 terminal installed and running
    - .env file in the same directory (see .env.example)
    - config.json in the same directory
"""

import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv

import db as supabase_db
import local_db
import mt5 as mt5_api
from sync import SyncEngine
from tp import TPEngine, DefaultTPStrategy
from license import validate_license, start_heartbeat, get_discord_id

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(path: str = "config.json") -> dict:
    config_path = Path(path)
    if not config_path.exists():
        logger.critical(f"config.json not found at '{config_path.resolve()}'")
        sys.exit(1)
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    logger.info(f"Config loaded from '{config_path}'.")
    return config


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown = False

def _handle_signal(sig, frame):
    global _shutdown
    logger.info(f"Received signal {sig} — initiating graceful shutdown.")
    _shutdown = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    global _shutdown

    # Load config
    config = load_config()
    poll_interval = config.get("poll_interval_seconds", 5)

    # Initialise local SQLite DB
    local_db.init_db()

    # Connect to MT5 — always attaches to the already-running terminal
    logger.info("Connecting to MT5...")
    connected = mt5_api.connect()
    if not connected:
        logger.critical("Failed to connect to MT5. Exiting.")
        sys.exit(1)

    # Connect to Supabase
    logger.info("Connecting to Supabase...")
    pool = await supabase_db.create_pool()

    # -----------------------------------------------------------------------
    # License validation
    # -----------------------------------------------------------------------
    license_cfg = config.get("license", {})
    license_key = license_cfg.get("key", "").strip()
    if not license_key:
        logger.critical(
            "No license key configured. "
            "Add your key to config.json under \"license\": { \"key\": \"...\" }. "
            "Run !activate in the Discord server to obtain a key."
        )
        await supabase_db.close_pool(pool)
        mt5_api.disconnect()
        sys.exit(1)

    mt5_account = str(mt5_api.get_account_number())
    logger.info(f"MT5 account: {mt5_account}")

    if not await validate_license(pool, license_key, mt5_account):
        logger.critical("License validation failed — exiting.")
        await supabase_db.close_pool(pool)
        mt5_api.disconnect()
        sys.exit(1)

    heartbeat_task = start_heartbeat(pool, license_key, mt5_account)
    # -----------------------------------------------------------------------

    # Build engine stack
    strategy  = DefaultTPStrategy(config)
    tp_engine = TPEngine(
        config,
        strategy    = strategy,
        pool        = pool,
        mt5_account = mt5_account,
        discord_id  = get_discord_id() or "",
        license_key = license_key,
        bot_version = "1.0.0",
    )
    sync      = SyncEngine(pool, config, tp_engine)

    # Startup reconciliation
    await sync.reconcile_on_startup()

    logger.info(f"Starting main loop (poll every {poll_interval}s). Press Ctrl+C to stop.")

    try:
        while not _shutdown:
            await sync.run_cycle()
            # Sleep in small increments so shutdown signal is caught quickly
            for _ in range(poll_interval * 10):
                if _shutdown:
                    break
                await asyncio.sleep(0.1)
    finally:
        logger.info("Shutting down...")
        if 'heartbeat_task' in locals() and not heartbeat_task.done():
            heartbeat_task.cancel()
        await supabase_db.close_pool(pool)
        mt5_api.disconnect()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
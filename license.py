"""
license.py — License validation for the Auto-Limits-Adder.

Queries the shared Supabase database to verify that the configured license key
is active and tied to the currently-connected MT5 account.

Public API
----------
validate_license(pool, license_key, mt5_account) -> bool
    One-shot validation. Returns True if the license is active and the
    MT5 account matches. Returns False (and logs the reason) otherwise.

start_heartbeat(pool, license_key, mt5_account, interval_seconds=900) -> asyncio.Task
    Starts a background asyncio task that calls validate_license() every
    `interval_seconds` (default 15 min). On failure, sets the global
    LICENSE_VALID flag to False so the SyncEngine can check it before
    placing new orders.

Usage in main.py
----------------
    from license import validate_license, start_heartbeat, is_license_valid

    # After MT5 connects, get the account number from the live session:
    mt5_account = str(mt5.account_info().login)

    # Startup check — exit immediately if invalid
    if not await validate_license(pool, config["license"]["key"], mt5_account):
        logger.critical("License validation failed. Exiting.")
        sys.exit(1)

    # Background revalidation — sets is_license_valid() flag on failure
    heartbeat_task = start_heartbeat(pool, config["license"]["key"], mt5_account)

    # In SyncEngine.run_cycle() guard:
    if not is_license_valid():
        logger.warning("License invalid — skipping order placement.")
        return
"""

import asyncio
import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global validity flag
# ---------------------------------------------------------------------------

_license_valid: bool = True   # Assume valid until proven otherwise
_heartbeat_task: Optional[asyncio.Task] = None
_discord_id: Optional[str] = None  # Resolved on first successful validation


def is_license_valid() -> bool:
    """
    Return True if the most recent license check passed.
    Called by SyncEngine before placing any new orders.
    """
    return _license_valid


def get_discord_id() -> Optional[str]:
    """Return the discord_id bound to the validated license key."""
    return _discord_id


# ---------------------------------------------------------------------------
# Core validation
# ---------------------------------------------------------------------------

async def validate_license(
    pool: asyncpg.Pool,
    license_key: str,
    mt5_account: str,
) -> bool:
    """
    Query Supabase and verify the license key is active and bound to this
    MT5 account.

    Returns True on success, False on any failure (key not found, revoked,
    wrong account, or DB error).

    Args:
        pool:         asyncpg connection pool (from db.create_pool)
        license_key:  The key from config.json → "license" → "key"
        mt5_account:  String form of mt5.account_info().login
    """
    if not license_key or not license_key.strip():
        logger.critical("License key is empty. Add it to config.json under 'license.key'.")
        return False

    if not mt5_account or not mt5_account.strip():
        logger.critical("MT5 account number is empty — cannot validate license.")
        return False

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status, mt5_account, discord_id FROM licenses WHERE license_key = $1",
                license_key,
            )
    except Exception as exc:
        logger.error(f"License DB query failed: {exc}")
        return False

    if row is None:
        logger.critical(
            "License key not found in database. "
            "Run !activate in the Discord server to register."
        )
        return False

    if row["status"] != "active":
        logger.critical(
            f"License has been revoked (status='{row['status']}'). "
            "Contact an admin in the Discord server."
        )
        return False

    if str(row["mt5_account"]) != str(mt5_account):
        logger.critical(
            f"License key is registered to MT5 account '{row['mt5_account']}', "
            f"but this terminal is logged in as '{mt5_account}'. "
            "License keys are locked to a single MT5 account."
        )
        return False

    global _discord_id
    _discord_id = str(row["discord_id"]) if row["discord_id"] else None
    logger.info(f"License validated OK (MT5: {mt5_account}, discord_id: {_discord_id}).")
    return True


# ---------------------------------------------------------------------------
# Background heartbeat
# ---------------------------------------------------------------------------

async def _heartbeat_loop(
    pool: asyncpg.Pool,
    license_key: str,
    mt5_account: str,
    interval_seconds: int,
) -> None:
    """Internal coroutine that re-validates on a fixed interval."""
    global _license_valid

    while True:
        await asyncio.sleep(interval_seconds)

        valid = await validate_license(pool, license_key, mt5_account)

        if not valid and _license_valid:
            # Transition from valid → invalid
            logger.critical(
                "Heartbeat: license re-validation FAILED. "
                "New order placement is suspended. "
                "Existing open positions are NOT affected — manage them manually in MT5."
            )
            _license_valid = False

        elif valid and not _license_valid:
            # Transition from invalid → valid (e.g. admin re-activated the key)
            logger.info("Heartbeat: license re-validation passed. Order placement re-enabled.")
            _license_valid = True

        else:
            logger.debug(
                f"Heartbeat: license check passed (MT5: {mt5_account}). "
                f"Next check in {interval_seconds}s."
            )


def start_heartbeat(
    pool: asyncpg.Pool,
    license_key: str,
    mt5_account: str,
    interval_seconds: int = 900,
) -> asyncio.Task:
    """
    Start the background license heartbeat and return the asyncio.Task.

    The task re-validates the license every `interval_seconds` (default 900s /
    15 minutes). On failure it sets the global `_license_valid` flag to False,
    which causes SyncEngine to skip new order placements without crashing or
    cancelling existing open positions.

    The returned Task can be cancelled on shutdown:
        heartbeat_task.cancel()

    Args:
        pool:             asyncpg connection pool
        license_key:      Value from config["license"]["key"]
        mt5_account:      str(mt5.account_info().login)
        interval_seconds: Seconds between checks (default 900 = 15 min)
    """
    global _license_valid, _heartbeat_task

    # Reset flag to valid when the heartbeat starts (we just validated on startup)
    _license_valid = True

    _heartbeat_task = asyncio.create_task(
        _heartbeat_loop(pool, license_key, mt5_account, interval_seconds)
    )
    logger.info(
        f"License heartbeat started — revalidating every {interval_seconds}s "
        f"(MT5: {mt5_account})."
    )
    return _heartbeat_task
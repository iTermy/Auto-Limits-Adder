"""
db.py — Supabase / asyncpg connection pool and read-only query helpers.

Convention:
  - All helpers accept a pool (asyncpg.Pool) and return dicts or lists of dicts.
  - conn.fetch / fetchrow / execute take positional args unpacked with *params.
  - Timestamp columns come back as native datetime objects — never call fromisoformat().
  - This module is strictly READ-ONLY against Supabase.
"""

import asyncpg
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Read-only connection (hardcoded — bot is distributed as a compiled binary)
# ---------------------------------------------------------------------------
# This role has SELECT-only access on: signals, limits, live_prices, licenses, bot_mode_status.
# It cannot INSERT, UPDATE, DELETE, or access any other table.
# Rotate this password via Supabase dashboard → Database → Roles if compromised.
_RO_DSN = "postgresql://execution_bot_ro.cqogevbfbrfzgbuxbhmn:oS%2495chu86HanS@aws-1-us-east-2.pooler.supabase.com:5432/postgres"


# ---------------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------------

async def create_pool() -> asyncpg.Pool:
    """Create and return an asyncpg connection pool using the hardcoded read-only DSN."""
    pool = await asyncpg.create_pool(
        dsn=_RO_DSN,
        min_size=1,
        max_size=5,
        server_settings={"search_path": "public"},
    )
    logger.debug("Supabase connection pool created.")
    return pool


async def close_pool(pool: asyncpg.Pool) -> None:
    await pool.close()
    logger.debug("Supabase connection pool closed.")


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _record_to_dict(record) -> dict:
    """Convert an asyncpg Record to a plain dict."""
    return dict(record)


def _records_to_dicts(records) -> list[dict]:
    return [_record_to_dict(r) for r in records]


# ---------------------------------------------------------------------------
# Signal queries
# ---------------------------------------------------------------------------

async def fetch_active_signals(pool: asyncpg.Pool) -> list[dict]:
    """
    Return all signals with status 'active' or 'hit'.
    These are the only signals the execution bot cares about.
    """
    query = """
        SELECT id, message_id, channel_id, instrument, direction,
               stop_loss, expiry_type, expiry_time, status,
               first_limit_hit_time, total_limits, limits_hit, scalp,
               created_at, updated_at
        FROM signals
        WHERE status IN ('active', 'hit')
        ORDER BY id
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)
    return _records_to_dicts(rows)


async def fetch_signal_by_id(pool: asyncpg.Pool, signal_id: int) -> Optional[dict]:
    """Fetch a single signal row by PK."""
    query = """
        SELECT id, message_id, channel_id, instrument, direction,
               stop_loss, expiry_type, expiry_time, status,
               first_limit_hit_time, total_limits, limits_hit, scalp,
               created_at, updated_at
        FROM signals
        WHERE id = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, signal_id)
    return _record_to_dict(row) if row else None


async def fetch_signals_by_ids(pool: asyncpg.Pool, signal_ids: list[int]) -> list[dict]:
    """Fetch multiple signals by a list of PKs."""
    if not signal_ids:
        return []
    query = """
        SELECT id, message_id, channel_id, instrument, direction,
               stop_loss, expiry_type, expiry_time, status,
               first_limit_hit_time, total_limits, limits_hit, scalp,
               created_at, updated_at
        FROM signals
        WHERE id = ANY($1::bigint[])
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, signal_ids)
    return _records_to_dicts(rows)


# ---------------------------------------------------------------------------
# Limit queries
# ---------------------------------------------------------------------------

async def fetch_pending_limits_for_signal(pool: asyncpg.Pool, signal_id: int) -> list[dict]:
    """Return all 'pending' limits for a given signal, ordered by sequence_number."""
    query = """
        SELECT id, signal_id, price_level, sequence_number, status,
               hit_time, hit_price, created_at
        FROM limits
        WHERE signal_id = $1 AND status = 'pending'
        ORDER BY sequence_number
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, signal_id)
    return _records_to_dicts(rows)


async def fetch_all_limits_for_signal(pool: asyncpg.Pool, signal_id: int) -> list[dict]:
    """Return ALL limits for a signal (any status), ordered by sequence_number."""
    query = """
        SELECT id, signal_id, price_level, sequence_number, status,
               hit_time, hit_price, created_at
        FROM limits
        WHERE signal_id = $1
        ORDER BY sequence_number
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, signal_id)
    return _records_to_dicts(rows)


async def fetch_pending_limits_for_signals(
    pool: asyncpg.Pool, signal_ids: list[int]
) -> list[dict]:
    """
    Bulk fetch all 'pending' limits for a list of signal IDs.
    Useful in the sync loop to reduce round-trips.
    """
    if not signal_ids:
        return []
    query = """
        SELECT l.id, l.signal_id, l.price_level, l.sequence_number, l.status,
               l.hit_time, l.hit_price, l.created_at,
               s.direction, s.stop_loss, s.instrument, s.total_limits
        FROM limits l
        JOIN signals s ON s.id = l.signal_id
        WHERE l.signal_id = ANY($1::bigint[]) AND l.status = 'pending'
        ORDER BY l.signal_id, l.sequence_number
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, signal_ids)
    return _records_to_dicts(rows)


async def fetch_limit_by_id(pool: asyncpg.Pool, limit_id: int) -> Optional[dict]:
    """Fetch a single limit row with its parent signal fields joined."""
    query = """
        SELECT l.id, l.signal_id, l.price_level, l.sequence_number, l.status,
               l.hit_time, l.hit_price, l.created_at,
               s.direction, s.stop_loss, s.instrument, s.total_limits,
               s.status AS signal_status, s.scalp
        FROM limits l
        JOIN signals s ON s.id = l.signal_id
        WHERE l.id = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, limit_id)
    return _record_to_dict(row) if row else None


# ---------------------------------------------------------------------------
# Combined convenience query — the main sync poll
# ---------------------------------------------------------------------------

async def fetch_active_signals_with_pending_limits(pool: asyncpg.Pool) -> list[dict]:
    """
    Return a list of active/hit signals, each with a 'pending_limits' key
    containing the list of pending limit dicts for that signal.

    This is the primary query used by the sync engine on each poll cycle.
    """
    signals = await fetch_active_signals(pool)
    if not signals:
        return []

    signal_ids = [s["id"] for s in signals]
    limits = await fetch_pending_limits_for_signals(pool, signal_ids)

    # Group limits by signal_id
    limits_by_signal: dict[int, list[dict]] = {}
    for lim in limits:
        limits_by_signal.setdefault(lim["signal_id"], []).append(lim)

    for sig in signals:
        sig["pending_limits"] = limits_by_signal.get(sig["id"], [])

    return signals


# ---------------------------------------------------------------------------
# Live prices (OANDA / Binance feed — written by Limits-Alert-Bot)
# ---------------------------------------------------------------------------

async def fetch_live_price(pool: asyncpg.Pool, symbol: str) -> dict | None:
    """
    Fetch the latest feed price for a symbol from the live_prices table.
    Returns a dict with keys: symbol, bid, ask, feed, updated_at
    Returns None if no row exists for the symbol.

    The caller is responsible for checking staleness via updated_at.
    """
    query = """
        SELECT symbol, bid, ask, feed, updated_at
        FROM live_prices
        WHERE symbol = $1
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, symbol)
    return _record_to_dict(row) if row else None


async def fetch_live_prices_bulk(
    pool: asyncpg.Pool, symbols: list[str]
) -> dict[str, dict]:
    """
    Fetch live prices for multiple symbols in one query.
    Returns a dict keyed by symbol.
    """
    if not symbols:
        return {}
    query = """
        SELECT symbol, bid, ask, feed, updated_at
        FROM live_prices
        WHERE symbol = ANY($1::text[])
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, symbols)
    return {r["symbol"]: _record_to_dict(r) for r in rows}


# ---------------------------------------------------------------------------
# Bot mode status (news_mode / spread_hour flags)
# ---------------------------------------------------------------------------

async def fetch_bot_mode_status(pool: asyncpg.Pool) -> Optional[dict]:
    """
    Fetch the current bot mode flags from the bot_mode_status table.
    Returns a dict with keys: id, news_mode, spread_hour, updated_at
    Returns None if the table is empty or unreachable.

    The table is expected to hold a single configuration row (always the
    row with the highest id, so the alert bot can insert a new row to
    push a state change without deleting history).
    """
    query = """
        SELECT id, news_mode, spread_hour, updated_at
        FROM bot_mode_status
        ORDER BY id DESC
        LIMIT 1
    """
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query)
        return _record_to_dict(row) if row else None
    except Exception as exc:
        logger.warning(f"fetch_bot_mode_status failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# License helpers
# ---------------------------------------------------------------------------

async def fetch_discord_id_for_license(
    pool: asyncpg.Pool, license_key: str
) -> Optional[str]:
    """Return the discord_id associated with a license key, or None."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT discord_id FROM licenses WHERE license_key = $1",
            license_key,
        )
    return row["discord_id"] if row else None


# ---------------------------------------------------------------------------
# TP outcomes — the only table the execution bot INSERTs into
# ---------------------------------------------------------------------------

async def insert_tp_outcome(pool: asyncpg.Pool, outcome: dict) -> bool:
    """
    Insert one row into tp_outcomes.

    Required keys in `outcome`:
        mt5_account, discord_id, license_key,
        mt5_ticket, symbol, db_instrument, asset_class, direction,
        outcome (one of: tp_partial, tp_trail_close, sl, breakeven_close, manual_close)

    Optional / nullable keys:
        signal_id, is_scalp, fill_price, close_price, lot_size,
        pnl_dollars, pnl_pips, tp_type, tp_threshold_value,
        tp_trail_amount, tp_partial_close_pct, tp_config_source,
        filled_at, bot_version

    Returns True on success, False on error.
    """
    query = """
        INSERT INTO tp_outcomes (
            mt5_account, discord_id, license_key,
            signal_id, mt5_ticket, symbol, db_instrument, asset_class,
            direction, is_scalp, outcome,
            fill_price, close_price, lot_size, pnl_dollars, pnl_pips,
            tp_type, tp_threshold_value, tp_trail_amount,
            tp_partial_close_pct, tp_config_source,
            filled_at, closed_at, bot_version
        ) VALUES (
            $1,  $2,  $3,
            $4,  $5,  $6,  $7,  $8,
            $9,  $10, $11,
            $12, $13, $14, $15, $16,
            $17, $18, $19,
            $20, $21,
            $22, NOW(), $23
        )
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                query,
                outcome.get("mt5_account"),
                outcome.get("discord_id"),
                outcome.get("license_key"),
                outcome.get("signal_id"),
                outcome.get("mt5_ticket"),
                outcome.get("symbol"),
                outcome.get("db_instrument"),
                outcome.get("asset_class"),
                outcome.get("direction"),
                outcome.get("is_scalp", False),
                outcome.get("outcome"),
                outcome.get("fill_price"),
                outcome.get("close_price"),
                outcome.get("lot_size"),
                outcome.get("pnl_dollars"),
                outcome.get("pnl_pips"),
                outcome.get("tp_type"),
                outcome.get("tp_threshold_value"),
                outcome.get("tp_trail_amount"),
                outcome.get("tp_partial_close_pct"),
                outcome.get("tp_config_source"),
                outcome.get("filled_at"),
                outcome.get("bot_version"),
            )
        return True
    except Exception as exc:
        logger.error(f"insert_tp_outcome failed: {exc}")
        return False
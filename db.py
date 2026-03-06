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
# Pool lifecycle
# ---------------------------------------------------------------------------

async def create_pool(dsn: str) -> asyncpg.Pool:
    """Create and return an asyncpg connection pool."""
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
    logger.info("Supabase connection pool created.")
    return pool


async def close_pool(pool: asyncpg.Pool) -> None:
    await pool.close()
    logger.info("Supabase connection pool closed.")


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
               s.direction, s.stop_loss, s.instrument, s.total_limits, s.status AS signal_status
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
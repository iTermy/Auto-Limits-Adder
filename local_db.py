"""
local_db.py — SQLite helpers for orders.db (local order mapping state).

This file is created automatically on first run. It is per-user and never
touches the shared Supabase database.

Schema:
    order_mappings:
        id           INTEGER PK AUTOINCREMENT
        limit_id     BIGINT UNIQUE          — maps to limits.id in Supabase
        signal_id    BIGINT                 — denormalised for fast lookups
        mt5_ticket   BIGINT UNIQUE          — MT5 order/position ticket
        order_type   TEXT                   — 'buy_limit'|'sell_limit'|'buy_stop'|'sell_stop'
        lot_size     REAL
        placed_at    TEXT                   — ISO timestamp
        filled_at    TEXT                   — ISO timestamp or NULL
        cancelled_at TEXT                   — ISO timestamp or NULL
        status       TEXT DEFAULT 'pending' — 'pending'|'filled'|'cancelled'|'error'
"""

import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = "orders.db"

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS order_mappings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    limit_id     BIGINT NOT NULL UNIQUE,
    signal_id    BIGINT NOT NULL,
    mt5_ticket   BIGINT NOT NULL UNIQUE,
    order_type   TEXT NOT NULL,
    lot_size     REAL,
    placed_at    TEXT NOT NULL,
    filled_at    TEXT,
    cancelled_at TEXT,
    status       TEXT NOT NULL DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_om_signal_id ON order_mappings(signal_id);
CREATE INDEX IF NOT EXISTS idx_om_status    ON order_mappings(status);
"""


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite database and return a connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    """Create tables if they don't exist. Safe to call on every startup."""
    with get_connection(db_path) as conn:
        conn.executescript(DDL)
    logger.info(f"Local SQLite DB initialised at '{db_path}'.")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Insert / create
# ---------------------------------------------------------------------------

def insert_order_mapping(
    limit_id: int,
    signal_id: int,
    mt5_ticket: int,
    order_type: str,
    lot_size: float,
    db_path: str = DB_PATH,
) -> int:
    """
    Insert a new pending order mapping.
    Returns the new row id.
    """
    sql = """
        INSERT INTO order_mappings
            (limit_id, signal_id, mt5_ticket, order_type, lot_size, placed_at, status)
        VALUES (?, ?, ?, ?, ?, ?, 'pending')
    """
    with get_connection(db_path) as conn:
        cur = conn.execute(sql, (limit_id, signal_id, mt5_ticket, order_type, lot_size, _now_iso()))
        conn.commit()
        return cur.lastrowid


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def get_pending_mappings(db_path: str = DB_PATH) -> list[dict]:
    """Return all rows with status='pending'. Used during startup reconciliation."""
    sql = "SELECT * FROM order_mappings WHERE status = 'pending'"
    with get_connection(db_path) as conn:
        rows = conn.execute(sql).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_mapping_by_limit_id(limit_id: int, db_path: str = DB_PATH) -> Optional[dict]:
    sql = "SELECT * FROM order_mappings WHERE limit_id = ?"
    with get_connection(db_path) as conn:
        row = conn.execute(sql, (limit_id,)).fetchone()
    return _row_to_dict(row) if row else None


def get_mapping_by_ticket(mt5_ticket: int, db_path: str = DB_PATH) -> Optional[dict]:
    sql = "SELECT * FROM order_mappings WHERE mt5_ticket = ?"
    with get_connection(db_path) as conn:
        row = conn.execute(sql, (mt5_ticket,)).fetchone()
    return _row_to_dict(row) if row else None


def get_mappings_by_signal_id(signal_id: int, db_path: str = DB_PATH) -> list[dict]:
    sql = "SELECT * FROM order_mappings WHERE signal_id = ?"
    with get_connection(db_path) as conn:
        rows = conn.execute(sql, (signal_id,)).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_filled_mappings_by_signal_id(signal_id: int, db_path: str = DB_PATH) -> list[dict]:
    sql = "SELECT * FROM order_mappings WHERE signal_id = ? AND status = 'filled'"
    with get_connection(db_path) as conn:
        rows = conn.execute(sql, (signal_id,)).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_all_tracked_signal_ids(db_path: str = DB_PATH) -> set[int]:
    """Return all distinct signal_ids we have any mapping for."""
    sql = "SELECT DISTINCT signal_id FROM order_mappings"
    with get_connection(db_path) as conn:
        rows = conn.execute(sql).fetchall()
    return {r["signal_id"] for r in rows}


# ---------------------------------------------------------------------------
# Status updates
# ---------------------------------------------------------------------------

def mark_filled(mt5_ticket: int, db_path: str = DB_PATH) -> None:
    """Mark an order mapping as filled (position opened)."""
    sql = """
        UPDATE order_mappings
        SET status = 'filled', filled_at = ?
        WHERE mt5_ticket = ? AND status = 'pending'
    """
    with get_connection(db_path) as conn:
        conn.execute(sql, (_now_iso(), mt5_ticket))
        conn.commit()
    logger.debug(f"Marked ticket {mt5_ticket} as filled in local DB.")


def mark_cancelled(mt5_ticket: int, db_path: str = DB_PATH) -> None:
    """Mark an order mapping as cancelled."""
    sql = """
        UPDATE order_mappings
        SET status = 'cancelled', cancelled_at = ?
        WHERE mt5_ticket = ? AND status = 'pending'
    """
    with get_connection(db_path) as conn:
        conn.execute(sql, (_now_iso(), mt5_ticket))
        conn.commit()
    logger.debug(f"Marked ticket {mt5_ticket} as cancelled in local DB.")


def mark_cancelled_by_limit_id(limit_id: int, db_path: str = DB_PATH) -> None:
    """Cancel all pending mappings for a given limit_id (e.g. limit removed from DB)."""
    sql = """
        UPDATE order_mappings
        SET status = 'cancelled', cancelled_at = ?
        WHERE limit_id = ? AND status = 'pending'
    """
    with get_connection(db_path) as conn:
        conn.execute(sql, (_now_iso(), limit_id))
        conn.commit()


def mark_error(mt5_ticket: int, db_path: str = DB_PATH) -> None:
    sql = "UPDATE order_mappings SET status = 'error' WHERE mt5_ticket = ?"
    with get_connection(db_path) as conn:
        conn.execute(sql, (mt5_ticket,))
        conn.commit()


def cancel_all_pending_for_signal(signal_id: int, db_path: str = DB_PATH) -> list[int]:
    """
    Mark all pending mappings for a signal as cancelled.
    Returns the list of mt5_tickets that were cancelled (so caller can cancel in MT5 too).
    """
    sql_select = "SELECT mt5_ticket FROM order_mappings WHERE signal_id = ? AND status = 'pending'"
    sql_update = """
        UPDATE order_mappings
        SET status = 'cancelled', cancelled_at = ?
        WHERE signal_id = ? AND status = 'pending'
    """
    with get_connection(db_path) as conn:
        tickets = [r["mt5_ticket"] for r in conn.execute(sql_select, (signal_id,)).fetchall()]
        conn.execute(sql_update, (_now_iso(), signal_id))
        conn.commit()
    return tickets
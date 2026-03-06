"""
mt5.py — MetaTrader 5 interface layer.

Handles connection lifecycle, order placement, cancellation, and position/order
queries. All MT5 API calls are isolated here so the rest of the bot never
imports MetaTrader5 directly.

Windows only: The MetaTrader5 package is not available on Linux/macOS.
"""

import logging
import math
from typing import Optional

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None  # Allows import on non-Windows for testing; will fail at runtime.

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MT5 order type constants (mirrors MetaTrader5 package values)
# ---------------------------------------------------------------------------

ORDER_TYPE_BUY_LIMIT  = 2
ORDER_TYPE_SELL_LIMIT = 3
ORDER_TYPE_BUY_STOP   = 4
ORDER_TYPE_SELL_STOP  = 5

ORDER_FILLING_RETURN = 2   # IOC-like; most brokers accept this for pending orders

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect(login: int = None, password: str = None, server: str = None) -> bool:
    """
    Initialise the MT5 terminal.

    If login/password/server are omitted (the common case), attaches to the
    already-running MT5 terminal and uses the currently logged-in account.
    Credentials are only needed if you want to authenticate programmatically.

    Returns True on success.
    """
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package not installed. Windows only.")

    kwargs = {}
    if login and password and server:
        kwargs = {"login": int(login), "password": password, "server": server}

    if not mt5.initialize(**kwargs):
        err = mt5.last_error()
        logger.error(f"MT5 initialize failed: {err}")
        return False

    account = mt5.account_info()
    if account is None:
        logger.error("MT5 account_info() returned None after initialize.")
        return False

    logger.info(
        f"MT5 connected — account {account.login}, "
        f"balance {account.balance:.2f} {account.currency}, "
        f"server {account.server}"
    )
    return True


def disconnect() -> None:
    if mt5 is not None:
        mt5.shutdown()
        logger.info("MT5 disconnected.")


def get_account_info() -> Optional[object]:
    info = mt5.account_info()
    if info is None:
        logger.error(f"MT5 account_info failed: {mt5.last_error()}")
    return info


# ---------------------------------------------------------------------------
# Symbol helpers
# ---------------------------------------------------------------------------

def get_current_price(symbol: str) -> Optional[tuple[float, float]]:
    """
    Return (bid, ask) for a symbol, or None on failure.
    """
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        logger.warning(f"No tick data for {symbol}: {mt5.last_error()}")
        return None
    return tick.bid, tick.ask


def get_current_spread(symbol: str) -> Optional[float]:
    """
    Return the current spread for a symbol as a raw price distance (ask - bid).
    Returns None if tick data is unavailable.
    """
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        logger.warning(f"No tick data for {symbol} when fetching spread: {mt5.last_error()}")
        return None
    return tick.ask - tick.bid


def get_pip_value(symbol: str, lot_size: float = 1.0) -> Optional[float]:
    """
    Return the monetary value of 1 pip for lot_size lots in account currency.
    Uses MT5 symbol_info to get tick_size and tick_value.
    pip_value = tick_value * (pip_size / tick_size) * lot_size
    For most pairs pip_size == tick_size * 10 (5-digit broker).
    """
    info = mt5.symbol_info(symbol)
    if info is None:
        logger.warning(f"symbol_info failed for {symbol}")
        return None

    tick_size  = info.trade_tick_size
    tick_value = info.trade_tick_value

    # Determine pip size: 4-digit forex = 0.0001, JPY pairs = 0.01, metals vary
    # We derive it from digits: 5-digit brokers have digits=5 for EUR/USD (pip = 0.0001)
    digits = info.digits
    if digits >= 4:
        pip_size = 10 ** -(digits - 1)   # e.g. digits=5 → 0.0001
    else:
        pip_size = 10 ** -digits

    if tick_size == 0:
        return None

    pip_value = tick_value * (pip_size / tick_size) * lot_size
    return pip_value


def price_to_pips(price_diff: float, symbol: str) -> float:
    """Convert a raw price difference to pips for a given symbol."""
    info = mt5.symbol_info(symbol)
    if info is None:
        return 0.0
    digits = info.digits
    if digits >= 4:
        pip_size = 10 ** -(digits - 1)
    else:
        pip_size = 10 ** -digits
    return abs(price_diff) / pip_size


def pips_to_price(pips: float, symbol: str) -> float:
    """Convert a pip count to a raw price distance."""
    info = mt5.symbol_info(symbol)
    if info is None:
        return 0.0
    digits = info.digits
    if digits >= 4:
        pip_size = 10 ** -(digits - 1)
    else:
        pip_size = 10 ** -digits
    return pips * pip_size


# ---------------------------------------------------------------------------
# Lot size calculation
# ---------------------------------------------------------------------------

def calculate_lot_size(
    account_balance: float,
    risk_percent: float,
    num_limits: int,
    sl_distance_price: float,
    symbol: str,
    min_lot: float = 0.01,
) -> float:
    """
    Calculate per-limit lot size based on risk percentage.

    Formula:
        lot = (balance * risk_pct/100) / (num_limits * sl_distance_pips * pip_value_per_lot)

    Rounded DOWN to the broker's volume step (typically 0.01).
    Never returns less than min_lot.
    """
    pip_value = get_pip_value(symbol, lot_size=1.0)
    if not pip_value or pip_value == 0:
        logger.error(f"Cannot calculate pip value for {symbol}")
        return min_lot

    sl_pips = price_to_pips(sl_distance_price, symbol)
    if sl_pips == 0:
        logger.error(f"SL distance is zero for {symbol}")
        return min_lot

    risk_amount = account_balance * (risk_percent / 100.0)
    lot = risk_amount / (num_limits * sl_pips * pip_value)

    # Round down to broker's volume step
    info = mt5.symbol_info(symbol)
    if info and info.volume_step > 0:
        step = info.volume_step
        lot = math.floor(lot / step) * step
    else:
        lot = math.floor(lot / min_lot) * min_lot

    lot = max(lot, min_lot)

    # Clamp to broker max
    if info and lot > info.volume_max:
        lot = info.volume_max
        logger.warning(f"Lot size clamped to broker max {lot} for {symbol}")

    return round(lot, 8)


# ---------------------------------------------------------------------------
# Order type resolution
# ---------------------------------------------------------------------------

def resolve_order_type(direction: str, limit_price: float, current_price: float) -> int:
    """
    Determine the correct MT5 pending order type based on signal direction
    and where the limit price sits relative to current market price.

        long  + limit below current → BUY_LIMIT
        long  + limit above current → BUY_STOP
        short + limit above current → SELL_LIMIT
        short + limit below current → SELL_STOP
    """
    if direction == "long":
        return ORDER_TYPE_BUY_LIMIT if limit_price < current_price else ORDER_TYPE_BUY_STOP
    else:  # short
        return ORDER_TYPE_SELL_LIMIT if limit_price > current_price else ORDER_TYPE_SELL_STOP


def order_type_to_str(order_type: int) -> str:
    mapping = {
        ORDER_TYPE_BUY_LIMIT:  "buy_limit",
        ORDER_TYPE_SELL_LIMIT: "sell_limit",
        ORDER_TYPE_BUY_STOP:   "buy_stop",
        ORDER_TYPE_SELL_STOP:  "sell_stop",
    }
    return mapping.get(order_type, "unknown")


# ---------------------------------------------------------------------------
# Place order
# ---------------------------------------------------------------------------

def place_pending_order(
    symbol: str,
    order_type: int,
    lot_size: float,
    price: float,
    sl: float,
    comment: str = "",
    magic: int = 20240001,
) -> Optional[int]:
    """
    Place a pending order (BUY_LIMIT / SELL_LIMIT / BUY_STOP / SELL_STOP).

    Returns the MT5 ticket number on success, or None on failure.
    """
    # Ensure symbol is selected in Market Watch
    if not mt5.symbol_select(symbol, True):
        logger.error(f"symbol_select failed for {symbol}")
        return None

    info = mt5.symbol_info(symbol)
    if info is None:
        logger.error(f"symbol_info is None for {symbol}")
        return None

    point  = info.point
    digits = info.digits

    request = {
        "action":       mt5.TRADE_ACTION_PENDING,
        "symbol":       symbol,
        "volume":       lot_size,
        "type":         order_type,
        "price":        round(price, digits),
        "sl":           round(sl, digits),
        "tp":           0.0,
        "deviation":    10,
        "magic":        magic,
        "comment":      comment[:31],   # MT5 comment field max 31 chars
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": ORDER_FILLING_RETURN,
    }

    result = mt5.order_send(request)

    if result is None:
        logger.error(f"order_send returned None for {symbol} [{order_type_to_str(order_type)}] @ {price}")
        return None

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        logger.error(
            f"order_send failed: retcode={result.retcode}, "
            f"comment='{result.comment}', symbol={symbol}, "
            f"type={order_type_to_str(order_type)}, price={price}, sl={sl}, lot={lot_size}"
        )
        return None

    logger.info(
        f"Placed {order_type_to_str(order_type)} order: ticket={result.order}, "
        f"symbol={symbol}, lot={lot_size}, price={price}, sl={sl}"
    )
    return result.order


# ---------------------------------------------------------------------------
# Cancel order
# ---------------------------------------------------------------------------

def cancel_pending_order(ticket: int) -> bool:
    """
    Cancel a pending order by ticket number.
    Returns True on success.
    """
    request = {
        "action": mt5.TRADE_ACTION_REMOVE,
        "order":  ticket,
    }
    result = mt5.order_send(request)

    if result is None:
        logger.error(f"order_send (cancel) returned None for ticket {ticket}")
        return False

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        logger.warning(
            f"Cancel failed: ticket={ticket}, retcode={result.retcode}, "
            f"comment='{result.comment}'"
        )
        return False

    logger.info(f"Cancelled pending order ticket={ticket}")
    return True


# ---------------------------------------------------------------------------
# Query pending orders
# ---------------------------------------------------------------------------

def get_pending_orders(magic: int = None) -> list[dict]:
    """
    Return all pending orders, optionally filtered by magic number.
    Each dict has keys: ticket, symbol, type, volume, price_open, sl, comment, magic.
    """
    orders = mt5.orders_get()
    if orders is None:
        return []

    result = []
    for o in orders:
        if magic is not None and o.magic != magic:
            continue
        result.append({
            "ticket":     o.ticket,
            "symbol":     o.symbol,
            "type":       o.type,
            "type_str":   order_type_to_str(o.type),
            "volume":     o.volume_current,
            "price_open": o.price_open,
            "sl":         o.sl,
            "comment":    o.comment,
            "magic":      o.magic,
        })
    return result


def get_pending_order_tickets(magic: int = None) -> set[int]:
    return {o["ticket"] for o in get_pending_orders(magic)}


# ---------------------------------------------------------------------------
# Query open positions
# ---------------------------------------------------------------------------

def get_open_positions(magic: int = None) -> list[dict]:
    """
    Return all open positions, optionally filtered by magic number.
    """
    positions = mt5.positions_get()
    if positions is None:
        return []

    result = []
    for p in positions:
        if magic is not None and p.magic != magic:
            continue
        result.append({
            "ticket":       p.ticket,
            "symbol":       p.symbol,
            "type":         p.type,         # 0=buy, 1=sell
            "volume":       p.volume,
            "price_open":   p.price_open,
            "price_current": p.price_current,
            "sl":           p.sl,
            "tp":           p.tp,
            "profit":       p.profit,
            "comment":      p.comment,
            "magic":        p.magic,
        })
    return result


def get_open_position_tickets(magic: int = None) -> set[int]:
    return {p["ticket"] for p in get_open_positions(magic)}


# ---------------------------------------------------------------------------
# Close / partial close position
# ---------------------------------------------------------------------------

def close_position(ticket: int, lot_size: float, symbol: str, comment: str = "") -> bool:
    """
    Close (all or part of) an open position.
    Returns True on success.
    """
    positions = mt5.positions_get(ticket=ticket)
    if not positions:
        logger.warning(f"close_position: ticket {ticket} not found in open positions.")
        return False

    pos   = positions[0]
    ptype = pos.type  # 0=buy, 1=sell

    close_type = mt5.ORDER_TYPE_SELL if ptype == 0 else mt5.ORDER_TYPE_BUY
    price_tick = mt5.symbol_info_tick(symbol)
    if price_tick is None:
        logger.error(f"No tick data for {symbol} during close.")
        return False

    price  = price_tick.bid if ptype == 0 else price_tick.ask
    digits = mt5.symbol_info(symbol).digits

    request = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "position":     ticket,
        "symbol":       symbol,
        "volume":       lot_size,
        "type":         close_type,
        "price":        round(price, digits),
        "deviation":    20,
        "magic":        pos.magic,
        "comment":      comment[:31],
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        retcode = result.retcode if result else "None"
        logger.error(f"Close position failed: ticket={ticket}, retcode={retcode}")
        return False

    logger.info(f"Closed {lot_size} lots of position ticket={ticket} ({symbol})")
    return True


def modify_position_sl(ticket: int, new_sl: float, symbol: str) -> bool:
    """
    Update the stop-loss on an open position without changing anything else.

    Used when the signal's stop_loss is edited after a limit has already filled
    (e.g. sender edits the Discord message to tighten/widen SL).

    Returns True on success, False on failure.
    """
    positions = mt5.positions_get(ticket=ticket)
    if not positions:
        logger.warning(f"modify_position_sl: ticket {ticket} not found in open positions.")
        return False

    pos    = positions[0]
    info   = mt5.symbol_info(symbol)
    digits = info.digits if info else 5

    rounded_sl = round(new_sl, digits)
    if pos.sl and round(pos.sl, digits) == rounded_sl:
        logger.debug(f"modify_position_sl: ticket {ticket} SL already at {rounded_sl} — no change needed.")
        return True

    request = {
        "action":   mt5.TRADE_ACTION_SLTP,
        "position": ticket,
        "sl":       rounded_sl,
        "tp":       pos.tp,
    }
    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        retcode = result.retcode if result else "None"
        logger.error(
            f"modify_position_sl failed: ticket={ticket}, symbol={symbol}, "
            f"new_sl={rounded_sl}, retcode={retcode}"
        )
        return False

    logger.info(
        f"Updated SL on position ticket={ticket} ({symbol}): "
        f"{pos.sl:.5f} → {rounded_sl:.5f}"
    )
    return True


def set_trailing_stop(ticket: int, trail_points: int, symbol: str) -> bool:
    """
    Set (or update) a trailing stop on an open position.
    trail_points: distance in MT5 points (not pips).
    Note: MT5 Python API does not natively support trailing stops via order_send —
    this sets a fixed SL at the trailing distance from current price, which must
    be updated on each tick loop iteration.
    """
    positions = mt5.positions_get(ticket=ticket)
    if not positions:
        logger.warning(f"set_trailing_stop: ticket {ticket} not found.")
        return False

    pos   = positions[0]
    ptype = pos.type
    tick  = mt5.symbol_info_tick(symbol)
    info  = mt5.symbol_info(symbol)

    if tick is None or info is None:
        return False

    digits = info.digits
    if ptype == 0:  # buy position
        new_sl = round(tick.bid - trail_points * info.point, digits)
        if pos.sl and new_sl <= pos.sl:
            return True  # already tighter
    else:  # sell position
        new_sl = round(tick.ask + trail_points * info.point, digits)
        if pos.sl and new_sl >= pos.sl:
            return True

    request = {
        "action":   mt5.TRADE_ACTION_SLTP,
        "position": ticket,
        "sl":       new_sl,
        "tp":       pos.tp,
    }
    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        retcode = result.retcode if result else "None"
        logger.debug(f"set_trailing_stop failed: ticket={ticket}, retcode={retcode}")
        return False

    return True
"""
sync.py — Core sync logic.

On every poll cycle:
  1. Fetch active/hit signals + their pending limits from Supabase.
  2. Filter signals based on config (instrument, asset class, direction, scalp, type).
  3. Diff what DB says should be pending vs what we have tracked locally + in MT5.
  4. Place missing orders, cancel orders for removed/cancelled limits.
  5. Detect fills: tickets that left pending orders and appear in positions.
  6. Hand filled positions off to the TP engine.

Design: The DB is the source of truth. Every cycle is idempotent — running it
twice in a row has no additional effect.
"""

import logging
from typing import Optional

import db as supabase_db
import local_db
import mt5 as mt5_api
from tp import TPEngine

logger = logging.getLogger(__name__)

MAGIC = 20240001   # Unique magic number for all orders placed by this bot


# ---------------------------------------------------------------------------
# Asset class detection
# ---------------------------------------------------------------------------

FOREX_PAIRS = {"EURUSD","GBPUSD","USDJPY","USDCHF","AUDUSD","NZDUSD",
               "USDCAD","GBPJPY","EURJPY","EURGBP","AUDJPY","CHFJPY",
               "EURCHF","EURAUD","EURCAD","EURNZD","GBPAUD","GBPCAD",
               "GBPCHF","GBPNZD","AUDCAD","AUDCHF","AUDNZD","CADCHF",
               "CADJPY","NZDCAD","NZDCHF","NZDJPY"}

INDEX_KEYWORDS = ["spx","nas","dax","jp225","uk100","de30","ftse","cac",
                  "nikkei","dow","hsi","asx","ibex","s&p"]

CRYPTO_PREFIXES = {"BTC","ETH","XRP","LTC","BNB","SOL","ADA","DOT","DOGE","MATIC"}

METALS = {"XAUUSD","XAGUSD","GOLD","SILVER"}


def get_asset_class(instrument: str) -> str:
    upper = instrument.upper()
    if upper in METALS:
        return "metals"
    if upper in FOREX_PAIRS or (len(upper) == 6 and upper.isalpha()):
        return "forex"
    lower = instrument.lower()
    if any(kw in lower for kw in INDEX_KEYWORDS):
        return "indices"
    if upper.endswith(".NAS") or upper.endswith(".NYSE"):
        return "stocks"
    if upper.endswith("USD") or upper.endswith("USDT"):
        prefix = upper.replace("USDT","").replace("USD","")
        if prefix in CRYPTO_PREFIXES:
            return "crypto"
    return "other"


# ---------------------------------------------------------------------------
# Signal filtering
# ---------------------------------------------------------------------------

def _channel_to_signal_type(channel_id: str) -> str:
    """
    Map a Discord channel_id to a signal type string.
    Extend this mapping to match your server's channel IDs.
    """
    # Example stubs — replace with real channel IDs
    channel_type_map: dict[str, str] = {
        # "123456789": "tolls",
        # "987654321": "pa",
    }
    return channel_type_map.get(str(channel_id), "setups")


def signal_passes_filter(signal: dict, filters: dict) -> bool:
    """Return True if a signal passes all configured filters."""
    instrument = signal.get("instrument", "")
    direction  = signal.get("direction", "")
    scalp      = signal.get("scalp", False)
    channel_id = signal.get("channel_id", "")

    # Instrument filter
    inst_cfg = filters.get("instruments", {})
    inst_mode = inst_cfg.get("mode", "all")
    inst_list = [i.upper() for i in inst_cfg.get("list", [])]
    if inst_mode == "include" and instrument.upper() not in inst_list:
        return False
    if inst_mode == "exclude" and instrument.upper() in inst_list:
        return False

    # Asset class filter
    ac_cfg  = filters.get("asset_classes", {})
    ac_mode = ac_cfg.get("mode", "all")
    ac_list = [a.lower() for a in ac_cfg.get("list", [])]
    if ac_mode != "all":
        asset_class = get_asset_class(instrument)
        if ac_mode == "include" and asset_class not in ac_list:
            return False
        if ac_mode == "exclude" and asset_class in ac_list:
            return False

    # Direction filter
    dir_filter = filters.get("directions", "both")
    if dir_filter != "both" and direction != dir_filter:
        return False

    # Scalp filter
    if not filters.get("scalp_signals", True) and scalp:
        return False

    # Signal type filter
    sig_type_filter = filters.get("signal_types", "all")
    if sig_type_filter != "all":
        sig_type = _channel_to_signal_type(channel_id)
        if sig_type != sig_type_filter:
            return False

    return True


# ---------------------------------------------------------------------------
# Symbol mapping
# ---------------------------------------------------------------------------

def map_instrument_to_symbol(instrument: str, symbol_map: dict) -> Optional[str]:
    """
    Convert a DB instrument string to an MT5 symbol name.
    Falls back to uppercase if not in the map.
    """
    return symbol_map.get(instrument) or symbol_map.get(instrument.upper()) or instrument.upper()


# ---------------------------------------------------------------------------
# SyncEngine
# ---------------------------------------------------------------------------

class SyncEngine:
    """
    Stateful sync engine. Holds references to the pool, config, and TP engine.
    Call run_cycle() on every poll interval.
    """

    def __init__(self, pool, config: dict, tp_engine: TPEngine):
        self.pool      = pool
        self.config    = config
        self.tp_engine = tp_engine

        self.filters    = config.get("filters", {})
        self.execution  = config.get("execution", {})
        self.symbol_map = config.get("symbol_map", {})

        self.risk_percent = self.execution.get("risk_percent", 5.0)
        self.min_lot      = self.execution.get("min_lot", 0.01)

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    async def run_cycle(self) -> None:
        try:
            await self._sync_orders()
            await self._detect_fills()
            self.tp_engine.run_tick()
        except Exception as exc:
            logger.exception(f"Unhandled error in sync cycle: {exc}")

    # ------------------------------------------------------------------
    # Startup reconciliation
    # ------------------------------------------------------------------

    async def reconcile_on_startup(self) -> None:
        """
        On startup: compare locally tracked 'pending' orders against live MT5
        pending orders and positions. Update local DB for anything that changed
        while the bot was offline.
        """
        logger.info("Running startup reconciliation...")

        pending_local = local_db.get_pending_mappings()
        if not pending_local:
            logger.info("No pending local mappings to reconcile.")
            return

        live_order_tickets    = mt5_api.get_pending_order_tickets(MAGIC)
        live_position_tickets = mt5_api.get_open_position_tickets(MAGIC)

        for mapping in pending_local:
            ticket = mapping["mt5_ticket"]

            if ticket in live_position_tickets:
                # Filled while offline
                logger.info(f"Reconcile: ticket {ticket} (limit_id={mapping['limit_id']}) → FILLED")
                local_db.mark_filled(ticket)
                self.tp_engine.register_position(ticket, mapping)

            elif ticket not in live_order_tickets:
                # Cancelled externally while offline
                logger.info(f"Reconcile: ticket {ticket} (limit_id={mapping['limit_id']}) → CANCELLED externally")
                local_db.mark_cancelled(ticket)

            else:
                logger.debug(f"Reconcile: ticket {ticket} still pending — OK")

        logger.info("Startup reconciliation complete.")

    # ------------------------------------------------------------------
    # Order sync
    # ------------------------------------------------------------------

    async def _sync_orders(self) -> None:
        """
        Fetch DB state, apply filters, then place/cancel orders to match.
        """
        signals = await supabase_db.fetch_active_signals_with_pending_limits(self.pool)

        # Apply signal filters
        signals = [s for s in signals if signal_passes_filter(s, self.filters)]

        # Build a set of (signal_id, limit_id) pairs that DB says should exist
        db_pending: dict[int, dict] = {}   # limit_id → limit row
        db_signals: dict[int, dict] = {}   # signal_id → signal row

        for signal in signals:
            db_signals[signal["id"]] = signal
            for lim in signal.get("pending_limits", []):
                db_pending[lim["id"]] = lim

        # What we have locally tracked as pending
        local_pending = local_db.get_pending_mappings()
        local_by_limit: dict[int, dict] = {m["limit_id"]: m for m in local_pending}

        # ---- Place missing orders ----
        for limit_id, lim in db_pending.items():
            if limit_id in local_by_limit:
                continue   # Already placed

            signal = db_signals.get(lim["signal_id"])
            if signal is None:
                continue

            await self._place_order_for_limit(lim, signal)

        # ---- Cancel orders for limits no longer in DB pending state ----
        for limit_id, mapping in local_by_limit.items():
            if limit_id not in db_pending:
                # This limit is no longer pending in DB — cancel MT5 order
                ticket = mapping["mt5_ticket"]
                logger.info(
                    f"Limit {limit_id} no longer pending in DB — cancelling ticket {ticket}"
                )
                success = mt5_api.cancel_pending_order(ticket)
                if success:
                    local_db.mark_cancelled(ticket)
                else:
                    # Order may already be gone; mark anyway
                    local_db.mark_cancelled(ticket)

        # ---- Cancel all orders for signals that reached a final status ----
        all_tracked_signal_ids = local_db.get_all_tracked_signal_ids()
        active_signal_ids = {s["id"] for s in signals}

        for signal_id in all_tracked_signal_ids:
            if signal_id not in active_signal_ids:
                # Signal may have become cancelled/stop_loss — purge pending orders
                pending_tickets = local_db.cancel_all_pending_for_signal(signal_id)
                for ticket in pending_tickets:
                    logger.info(
                        f"Signal {signal_id} gone from active — cancelling ticket {ticket}"
                    )
                    mt5_api.cancel_pending_order(ticket)

    async def _place_order_for_limit(self, lim: dict, signal: dict) -> None:
        """Place a single pending order for a limit level."""
        instrument = signal["instrument"]
        symbol     = map_instrument_to_symbol(instrument, self.symbol_map)
        direction  = signal["direction"]
        stop_loss  = signal["stop_loss"]
        limit_price = lim["price_level"]
        num_limits = signal.get("total_limits", 1) or 1

        # Get current price to determine order type
        prices = mt5_api.get_current_price(symbol)
        if prices is None:
            logger.warning(f"Cannot get price for {symbol} — skipping limit {lim['id']}")
            return

        bid, ask = prices
        mid_price = (bid + ask) / 2.0

        # Skip if price already past the limit (config guard)
        if self.execution.get("skip_if_price_past_limit", True):
            if direction == "long" and mid_price < limit_price:
                # BUY_STOP scenario — price needs to be below for this to be valid
                # For BUY_LIMIT, limit is below current price — that's fine
                pass
            if direction == "short" and mid_price > limit_price:
                pass
            # Strict check: if we'd need a market order (price already at/past level)
            already_past = (
                (direction == "long"  and mid_price <= limit_price and
                 abs(mid_price - limit_price) < mt5_api.pips_to_price(0.5, symbol)) or
                (direction == "short" and mid_price >= limit_price and
                 abs(mid_price - limit_price) < mt5_api.pips_to_price(0.5, symbol))
            )
            if already_past:
                logger.warning(
                    f"Price {mid_price} already at/past limit {limit_price} for {symbol} — skipping"
                )
                return

        order_type = mt5_api.resolve_order_type(direction, limit_price, mid_price)

        # Calculate lot size
        account = mt5_api.get_account_info()
        if account is None:
            logger.error("Cannot get account info for lot size calculation.")
            return

        sl_distance = abs(limit_price - stop_loss)

        lot_size = mt5_api.calculate_lot_size(
            account_balance=account.balance,
            risk_percent=self.risk_percent,
            num_limits=num_limits,
            sl_distance_price=sl_distance,
            symbol=symbol,
            min_lot=self.min_lot,
        )

        comment = f"lim:{lim['id']} sig:{signal['id']}"

        ticket = mt5_api.place_pending_order(
            symbol=symbol,
            order_type=order_type,
            lot_size=lot_size,
            price=limit_price,
            sl=stop_loss,
            comment=comment,
            magic=MAGIC,
        )

        if ticket is None:
            logger.error(
                f"Failed to place order for limit {lim['id']} (signal {signal['id']}, {symbol})"
            )
            return

        local_db.insert_order_mapping(
            limit_id=lim["id"],
            signal_id=signal["id"],
            mt5_ticket=ticket,
            order_type=mt5_api.order_type_to_str(order_type),
            lot_size=lot_size,
        )
        logger.info(
            f"Placed {mt5_api.order_type_to_str(order_type)} order: "
            f"ticket={ticket}, symbol={symbol}, lot={lot_size}, "
            f"price={limit_price}, sl={stop_loss}, "
            f"limit_id={lim['id']}, signal_id={signal['id']}"
        )

    # ------------------------------------------------------------------
    # Fill detection
    # ------------------------------------------------------------------

    async def _detect_fills(self) -> None:
        """
        Detect pending orders that have filled (transitioned to open position).
        Updates local DB and notifies the TP engine.
        """
        pending_local = local_db.get_pending_mappings()
        if not pending_local:
            return

        live_order_tickets    = mt5_api.get_pending_order_tickets(MAGIC)
        live_position_tickets = mt5_api.get_open_position_tickets(MAGIC)

        for mapping in pending_local:
            ticket = mapping["mt5_ticket"]

            if ticket in live_position_tickets:
                logger.info(
                    f"Fill detected: ticket={ticket}, "
                    f"limit_id={mapping['limit_id']}, signal_id={mapping['signal_id']}"
                )
                local_db.mark_filled(ticket)
                self.tp_engine.register_position(ticket, mapping)

            elif ticket not in live_order_tickets:
                # Disappeared from orders but not in positions — externally cancelled
                logger.info(
                    f"Order vanished (external cancel?): ticket={ticket}, "
                    f"limit_id={mapping['limit_id']}"
                )
                local_db.mark_cancelled(ticket)
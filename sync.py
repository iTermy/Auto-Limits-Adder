"""
sync.py — Core sync logic.

On every poll cycle:
  1. Fetch active/hit signals + their pending limits from Supabase.
  2. Filter signals based on config (instrument, asset class, direction, scalp, type).
  3. Diff what DB says should be pending vs what we have tracked locally + in MT5.
  4. Place missing orders, cancel orders for removed/cancelled limits.
  5. Detect fills: tickets that left pending orders and appear in positions.
  6. Hand filled positions off to the TP engine.

Index / Crypto offset handling
───────────────────────────────
Indices (OANDA feed) and crypto (Binance feed) have prices in the DB that differ
from ICMarkets MT5 prices. Rather than using the raw DB price_level directly as
the MT5 pending order price, we:

  1. Read the OANDA/Binance price from the live_prices table (written every ~5s
     by the Limits-Alert-Bot).
  2. Read the current MT5 price for the same instrument.
  3. Compute offset = mt5_mid - feed_mid.
  4. Place the pending order at: db_price_level + offset
     (i.e. the DB level translated into MT5 price space).
  5. The SL is also translated: db_stop_loss + offset.
     The pip distance between entry and SL is identical either way, so lot sizing
     is unaffected by the offset.

Offset readjustment
────────────────────
The feed-vs-MT5 spread can drift over time. Every offset_readjust_interval_seconds
(config, default 60s), the engine checks each live offset order:
  - Recompute the current offset.
  - If the offset has drifted more than offset_readjust_threshold_pips from
    the offset at placement time, cancel the existing order and re-place at the
    new adjusted price.
  - If live_prices data is stale (> max_staleness_seconds), skip readjustment
    and leave the order in place.

Design: The DB is the source of truth. Every cycle is idempotent — running it
twice in a row has no additional effect.
"""

import logging
import time
from datetime import datetime, timezone
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
    # Stocks must be checked before index keywords — '.NAS' suffix would otherwise
    # match the 'nas' index keyword and be misclassified as an index.
    if upper.endswith(".NAS") or upper.endswith(".NYSE"):
        return "stocks"
    lower = instrument.lower()
    if any(kw in lower for kw in INDEX_KEYWORDS):
        return "indices"
    if upper.endswith("USD") or upper.endswith("USDT"):
        prefix = upper.replace("USDT","").replace("USD","")
        if prefix in CRYPTO_PREFIXES:
            return "crypto"
    return "other"


def needs_feed_offset(instrument: str) -> bool:
    """Return True for instruments whose DB prices come from OANDA or Binance."""
    return get_asset_class(instrument) in ("indices", "crypto")


# ---------------------------------------------------------------------------
# Signal filtering
# ---------------------------------------------------------------------------

def _channel_to_signal_type(channel_id: str) -> str:
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

    inst_cfg  = filters.get("instruments", {})
    inst_mode = inst_cfg.get("mode", "all")
    inst_list = [i.upper() for i in inst_cfg.get("list", [])]
    if inst_mode == "include" and instrument.upper() not in inst_list:
        return False
    if inst_mode == "exclude" and instrument.upper() in inst_list:
        return False

    ac_cfg  = filters.get("asset_classes", {})
    ac_mode = ac_cfg.get("mode", "all")
    ac_list = [a.lower() for a in ac_cfg.get("list", [])]
    if ac_mode != "all":
        asset_class = get_asset_class(instrument)
        if ac_mode == "include" and asset_class not in ac_list:
            return False
        if ac_mode == "exclude" and asset_class in ac_list:
            return False

    dir_filter = filters.get("directions", "both")
    if dir_filter != "both" and direction != dir_filter:
        return False

    if not filters.get("scalp_signals", True) and scalp:
        return False

    sig_type_filter = filters.get("signal_types", "all")
    if sig_type_filter != "all":
        sig_type = _channel_to_signal_type(channel_id)
        if sig_type != sig_type_filter:
            return False

    return True


# ---------------------------------------------------------------------------
# Symbol mapping
# ---------------------------------------------------------------------------

def map_instrument_to_symbol(instrument: str, symbol_map: dict) -> str:
    """
    Convert a DB instrument string to an MT5 symbol name.
    Stocks (e.g. AMD.NAS) get a -24 suffix on ICMarkets, which enables
    24-hour trading and avoids session restriction errors.
    """
    # Explicit map entry always wins
    mapped = symbol_map.get(instrument) or symbol_map.get(instrument.upper())
    if mapped:
        return mapped

    upper = instrument.upper()
    if upper.endswith(".NAS") or upper.endswith(".NYSE"):
        return upper + "-24"

    return upper


def get_live_prices_key(instrument: str) -> str:
    """
    Return the key to use when querying live_prices for this instrument.
    The alert bot writes live_prices using the DB instrument name as-is
    (e.g. 'SPX500USD', 'BTCUSDT'), so we just normalise to uppercase.
    """
    return instrument.upper()


# ---------------------------------------------------------------------------
# Feed offset calculation
# ---------------------------------------------------------------------------

def _is_stale(updated_at: datetime, max_staleness_seconds: int) -> bool:
    if updated_at is None:
        return True
    age = (datetime.now(timezone.utc) - updated_at).total_seconds()
    return age > max_staleness_seconds


async def get_feed_offset(
    pool,
    instrument: str,
    mt5_symbol: str,
    max_staleness_seconds: int,
) -> Optional[float]:
    """
    Compute offset = mt5_mid - feed_mid for an index/crypto instrument.

    Queries live_prices using the DB instrument name (uppercased), which is
    how the alert bot writes it. Returns None if data is stale or unavailable.
    """
    feed_symbol = get_live_prices_key(instrument)

    live_row = await supabase_db.fetch_live_price(pool, feed_symbol)
    if live_row is None:
        logger.warning(f"No live_prices row for feed symbol '{feed_symbol}'.")
        return None

    if _is_stale(live_row["updated_at"], max_staleness_seconds):
        age = (datetime.now(timezone.utc) - live_row["updated_at"]).total_seconds()
        logger.warning(
            f"live_prices for '{feed_symbol}' is stale ({age:.0f}s old, "
            f"max {max_staleness_seconds}s) — skipping offset calculation."
        )
        return None

    feed_mid = (live_row["bid"] + live_row["ask"]) / 2.0

    mt5_prices = mt5_api.get_current_price(mt5_symbol)
    if mt5_prices is None:
        logger.warning(f"Cannot get MT5 price for '{mt5_symbol}'.")
        return None

    mt5_mid = (mt5_prices[0] + mt5_prices[1]) / 2.0
    offset  = mt5_mid - feed_mid

    logger.debug(
        f"Offset {instrument}: mt5={mt5_mid:.5f}, feed={feed_mid:.5f}, "
        f"offset={offset:+.5f}"
    )
    return offset


# ---------------------------------------------------------------------------
# SyncEngine
# ---------------------------------------------------------------------------

class SyncEngine:
    """
    Stateful sync engine. Call run_cycle() on every poll interval.
    """

    def __init__(self, pool, config: dict, tp_engine: TPEngine):
        self.pool      = pool
        self.config    = config
        self.tp_engine = tp_engine

        self.filters    = config.get("filters", {})
        self.execution  = config.get("execution", {})
        self.symbol_map = config.get("symbol_map", {})

        lp_cfg = config.get("live_prices", {})
        self.max_staleness           = lp_cfg.get("max_staleness_seconds", 30)
        self.readjust_interval       = lp_cfg.get("offset_readjust_interval_seconds", 60)
        self.readjust_threshold_pips = lp_cfg.get("offset_readjust_threshold_pips", 2.0)

        self.risk_percent = self.execution.get("risk_percent", 5.0)
        self.min_lot      = self.execution.get("min_lot", 0.01)

        self._last_readjust: float = 0.0   # monotonic time of last readjust pass

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    async def run_cycle(self) -> None:
        try:
            await self._sync_orders()
            await self._maybe_readjust_offset_orders()
            await self._detect_fills()
            self.tp_engine.run_tick()
        except Exception as exc:
            logger.exception(f"Unhandled error in sync cycle: {exc}")

    # ------------------------------------------------------------------
    # Startup reconciliation
    # ------------------------------------------------------------------

    async def reconcile_on_startup(self) -> None:
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
                logger.info(f"Reconcile: ticket {ticket} (limit_id={mapping['limit_id']}) -> FILLED")
                local_db.mark_filled(ticket)
                self.tp_engine.register_position(ticket, mapping)

            elif ticket not in live_order_tickets:
                logger.info(f"Reconcile: ticket {ticket} (limit_id={mapping['limit_id']}) -> CANCELLED externally")
                local_db.mark_cancelled(ticket)

            else:
                logger.debug(f"Reconcile: ticket {ticket} still pending — OK")

        logger.info("Startup reconciliation complete.")

    # ------------------------------------------------------------------
    # Order sync
    # ------------------------------------------------------------------

    async def _sync_orders(self) -> None:
        signals = await supabase_db.fetch_active_signals_with_pending_limits(self.pool)
        signals = [s for s in signals if signal_passes_filter(s, self.filters)]

        db_pending: dict[int, dict] = {}
        db_signals: dict[int, dict] = {}

        for signal in signals:
            db_signals[signal["id"]] = signal
            for lim in signal.get("pending_limits", []):
                db_pending[lim["id"]] = lim

        local_pending  = local_db.get_pending_mappings()
        local_by_limit = {m["limit_id"]: m for m in local_pending}

        # Also collect all tracked limit_ids (any status) to guard against re-placing
        # an order whose DB write failed mid-cycle last time.
        all_tracked_limit_ids = local_db.get_all_tracked_limit_ids()

        # Place missing orders
        for limit_id, lim in db_pending.items():
            if limit_id in local_by_limit:
                continue
            if limit_id in all_tracked_limit_ids:
                # Already have a mapping (filled/cancelled) — don't re-place
                continue
            signal = db_signals.get(lim["signal_id"])
            if signal is None:
                continue
            await self._place_order_for_limit(lim, signal)

        # Cancel orders for limits no longer pending in DB
        for limit_id, mapping in local_by_limit.items():
            if limit_id not in db_pending:
                ticket = mapping["mt5_ticket"]
                logger.info(f"Limit {limit_id} no longer pending in DB — cancelling ticket {ticket}")
                mt5_api.cancel_pending_order(ticket)
                local_db.mark_cancelled(ticket)

        # Cancel all orders for signals that left active/hit state
        all_tracked = local_db.get_all_tracked_signal_ids()
        active_ids  = {s["id"] for s in signals}

        for signal_id in all_tracked:
            if signal_id not in active_ids:
                pending_tickets = local_db.cancel_all_pending_for_signal(signal_id)
                for ticket in pending_tickets:
                    logger.info(f"Signal {signal_id} gone from active — cancelling ticket {ticket}")
                    mt5_api.cancel_pending_order(ticket)

    async def _place_order_for_limit(self, lim: dict, signal: dict) -> None:
        """
        Place a single pending order for a limit level.

        For index/crypto instruments: translates the DB price_level and stop_loss
        from feed-price space into MT5-price space using the live offset, then
        places a normal pending order at the adjusted price.

        The pip distance between entry and SL is identical in both price spaces
        (offset cancels out), so lot sizing uses the raw DB prices as-is.
        """
        instrument = signal["instrument"]
        symbol     = map_instrument_to_symbol(instrument, self.symbol_map)
        direction  = signal["direction"]
        db_sl      = signal["stop_loss"]
        db_price   = lim["price_level"]
        num_limits = signal.get("total_limits", 1) or 1

        use_offset = needs_feed_offset(instrument)
        offset     = 0.0
        feed_mid   = None
        mt5_mid    = None

        if use_offset:
            computed = await get_feed_offset(
                self.pool, instrument, symbol, self.max_staleness,
            )
            if computed is None:
                logger.warning(
                    f"Cannot compute feed offset for {instrument} — "
                    f"skipping limit {lim['id']} until live_prices is available."
                )
                return
            offset = computed

            # Capture prices now for offset metadata (used by readjustment)
            mt5_prices = mt5_api.get_current_price(symbol)
            if mt5_prices:
                mt5_mid  = (mt5_prices[0] + mt5_prices[1]) / 2.0
                feed_mid = mt5_mid - offset

        # Translate DB prices into MT5 price space
        mt5_order_price = db_price + offset
        mt5_sl          = db_sl + offset

        # Current MT5 price (for order type resolution)
        prices = mt5_api.get_current_price(symbol)
        if prices is None:
            logger.warning(f"Cannot get MT5 price for {symbol} — skipping limit {lim['id']}")
            return

        bid, ask    = prices
        mt5_mid_now = (bid + ask) / 2.0

        # Skip if price already past the adjusted limit
        if self.execution.get("skip_if_price_past_limit", True):
            tolerance = mt5_api.pips_to_price(0.5, symbol)
            past = (
                (direction == "long"  and mt5_mid_now <= mt5_order_price
                 and abs(mt5_mid_now - mt5_order_price) < tolerance) or
                (direction == "short" and mt5_mid_now >= mt5_order_price
                 and abs(mt5_mid_now - mt5_order_price) < tolerance)
            )
            if past:
                logger.warning(
                    f"MT5 price {mt5_mid_now:.5f} already at/past adjusted limit "
                    f"{mt5_order_price:.5f} for {symbol} — skipping limit {lim['id']}"
                )
                return

        order_type = mt5_api.resolve_order_type(direction, mt5_order_price, mt5_mid_now)

        # SL distance: offset cancels, so raw DB prices give correct pip distance
        sl_distance = abs(db_price - db_sl)

        account = mt5_api.get_account_info()
        if account is None:
            logger.error("Cannot get account info for lot size calculation.")
            return

        lot_size = mt5_api.calculate_lot_size(
            account_balance=account.balance,
            risk_percent=self.risk_percent,
            num_limits=num_limits,
            sl_distance_price=sl_distance,
            symbol=symbol,
            min_lot=self.min_lot,
        )

        comment = f"lim:{lim['id']} sig:{signal['id']}"
        if use_offset:
            comment += " off"

        ticket = mt5_api.place_pending_order(
            symbol=symbol,
            order_type=order_type,
            lot_size=lot_size,
            price=mt5_order_price,
            sl=mt5_sl,
            comment=comment,
            magic=MAGIC,
        )

        if ticket is None:
            logger.error(
                f"Failed to place order for limit {lim['id']} "
                f"(signal {signal['id']}, {symbol})"
            )
            return

        local_db.insert_order_mapping(
            limit_id=lim["id"],
            signal_id=signal["id"],
            mt5_ticket=ticket,
            order_type=mt5_api.order_type_to_str(order_type),
            lot_size=lot_size,
        )

        if use_offset and feed_mid is not None and mt5_mid is not None:
            local_db.update_offset_metadata(ticket, feed_mid, mt5_mid)

        logger.info(
            f"Placed {mt5_api.order_type_to_str(order_type)}: "
            f"ticket={ticket}, symbol={symbol}, lot={lot_size}, "
            f"price={mt5_order_price:.5f} (db={db_price:.5f}, offset={offset:+.5f}), "
            f"sl={mt5_sl:.5f}, limit_id={lim['id']}, signal_id={signal['id']}"
        )

    # ------------------------------------------------------------------
    # Offset readjustment
    # ------------------------------------------------------------------

    async def _maybe_readjust_offset_orders(self) -> None:
        """
        Gate: only runs the full readjust pass every readjust_interval seconds.
        """
        if time.monotonic() - self._last_readjust < self.readjust_interval:
            return
        self._last_readjust = time.monotonic()
        await self._readjust_offset_orders()

    async def _readjust_offset_orders(self) -> None:
        """
        For every live pending offset order, recompute the current offset.
        If drift exceeds the pip threshold, cancel and re-place at the new price.
        """
        offset_mappings = local_db.get_pending_offset_mappings()
        if not offset_mappings:
            return

        logger.debug(f"Offset readjustment check: {len(offset_mappings)} order(s).")

        for mapping in offset_mappings:
            limit_id = mapping["limit_id"]

            # Fetch fresh limit+signal data from DB
            lim_row = await supabase_db.fetch_limit_by_id(self.pool, limit_id)
            if lim_row is None or lim_row.get("status") != "pending":
                # Limit no longer pending in DB — _sync_orders will handle cancellation
                continue

            instrument = lim_row["instrument"]
            symbol     = map_instrument_to_symbol(instrument, self.symbol_map)

            current_offset = await get_feed_offset(
                self.pool, instrument, symbol, self.max_staleness,
            )

            if current_offset is None:
                # Stale / unavailable — leave order in place, update check time
                local_db.update_last_offset_check(mapping["mt5_ticket"])
                continue

            original_offset = mapping.get("offset_at_placement")
            if original_offset is None:
                local_db.update_last_offset_check(mapping["mt5_ticket"])
                continue

            drift_pips = mt5_api.price_to_pips(
                abs(current_offset - original_offset), symbol
            )
            local_db.update_last_offset_check(mapping["mt5_ticket"])

            if drift_pips < self.readjust_threshold_pips:
                logger.debug(
                    f"Ticket {mapping['mt5_ticket']} ({symbol}): "
                    f"drift {drift_pips:.1f} pips — OK."
                )
                continue

            logger.info(
                f"Offset drift {drift_pips:.1f} pips (threshold "
                f"{self.readjust_threshold_pips}) for ticket "
                f"{mapping['mt5_ticket']} ({symbol}) — re-placing."
            )

            cancelled = mt5_api.cancel_pending_order(mapping["mt5_ticket"])
            if not cancelled:
                logger.warning(
                    f"Could not cancel ticket {mapping['mt5_ticket']} for readjustment "
                    f"— may have just filled."
                )
                continue

            local_db.mark_cancelled(mapping["mt5_ticket"])

            # Reconstruct minimal dicts to re-use _place_order_for_limit
            signal_stub = {
                "id":           lim_row["signal_id"],
                "instrument":   lim_row["instrument"],
                "direction":    lim_row["direction"],
                "stop_loss":    lim_row["stop_loss"],
                "total_limits": lim_row.get("total_limits", 1),
            }
            lim_stub = {
                "id":          lim_row["id"],
                "signal_id":   lim_row["signal_id"],
                "price_level": lim_row["price_level"],
            }
            await self._place_order_for_limit(lim_stub, signal_stub)

    # ------------------------------------------------------------------
    # Fill detection
    # ------------------------------------------------------------------

    async def _detect_fills(self) -> None:
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
                logger.info(
                    f"Order vanished (external cancel?): ticket={ticket}, "
                    f"limit_id={mapping['limit_id']}"
                )
                local_db.mark_cancelled(ticket)
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

Spread adjustment
──────────────────
Every order placement fetches the live bid/ask spread from MT5 and shifts both
the entry price and stop-loss to account for it:

  LONG  entry  (+spread): BUY_LIMIT/BUY_STOP fires on ASK → place higher so
                           the order triggers when BID (mid) reaches the DB level.
  LONG  SL     (-spread): MT5 closes a long when BID drops → push SL further
                           down so spread noise doesn't trigger it prematurely.
  SHORT entry  (-spread): SELL_LIMIT/SELL_STOP fires on BID → place lower so
                           the order triggers when ASK (mid) reaches the DB level.
  SHORT SL     (+spread): MT5 closes a short when ASK rises → push SL further
                           up so spread noise doesn't trigger it prematurely.

Because the spread is re-fetched live on every placement (including readjust
re-placements), spread drift is handled automatically with no extra machinery.

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
        # How often (seconds) to recheck lot sizes + spread on live pending orders.
        # Runs on a separate timer from offset readjustment.  Default 300s (5 min).
        self.lot_recheck_interval: float = self.execution.get("lot_recheck_interval_seconds", 300)

        # Proximity filter — limits farther than this many pips from current price
        # are deferred (not placed in MT5) until price comes within range.
        # A value of 0 or None disables the filter entirely.
        prox_cfg = config.get("proximity_filter", {})
        self.proximity_enabled = prox_cfg.get("enabled", False)
        self.proximity_default_pips: float = prox_cfg.get("default_pips", 0.0)
        self.proximity_per_instrument: dict = {
            k.upper(): v for k, v in prox_cfg.get("per_instrument", {}).items()
        }
        self.proximity_per_asset_class: dict = {
            k.lower(): v for k, v in prox_cfg.get("per_asset_class", {}).items()
        }

        self._last_readjust:    float = 0.0   # monotonic time of last offset readjust pass
        self._last_lot_recheck: float = 0.0   # monotonic time of last lot-size recheck pass

    # ------------------------------------------------------------------
    # Proximity filter
    # ------------------------------------------------------------------

    def _get_proximity_threshold_pips(self, instrument: str) -> float:
        """
        Return the maximum pip distance from current price at which we will place
        a pending order for this instrument. 0 means no limit (disabled).

        Lookup priority: per_instrument > per_asset_class > default.
        """
        upper = instrument.upper()
        if upper in self.proximity_per_instrument:
            return float(self.proximity_per_instrument[upper])
        asset_class = get_asset_class(instrument)
        if asset_class in self.proximity_per_asset_class:
            return float(self.proximity_per_asset_class[asset_class])
        return float(self.proximity_default_pips)

    def _is_within_proximity(
        self,
        db_price: float,
        current_mt5_mid: float,
        symbol: str,
        instrument: str,
    ) -> bool:
        """
        Return True if the limit's price (already translated to MT5 space) is
        within the configured pip threshold of the current MT5 mid price.

        Always returns True when the proximity filter is disabled or threshold is 0.
        """
        if not self.proximity_enabled:
            return True

        threshold_pips = self._get_proximity_threshold_pips(instrument)
        if threshold_pips <= 0:
            return True

        distance_pips = mt5_api.price_to_pips(
            abs(db_price - current_mt5_mid), symbol
        )
        return distance_pips <= threshold_pips

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    async def run_cycle(self) -> None:
        try:
            await self._sync_orders()
            await self._sync_filled_position_sls()
            await self._maybe_readjust_offset_orders()
            await self._maybe_recheck_lot_sizes()
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

        # All limit_ids already tracked in order_mappings (any status).
        # Used to guard against re-placing when a mid-cycle DB write failed.
        all_tracked_limit_ids = local_db.get_all_tracked_limit_ids()

        # Limit_ids sitting in the deferred table (too far from price to place yet).
        deferred_limit_ids = local_db.get_deferred_limit_ids()

        # ------------------------------------------------------------------
        # Pre-compute average SL distance per signal (DB price space).
        #
        # The pip distance from entry to SL is identical in DB space and MT5
        # space (offset cancels out between the two prices), so we size lots
        # using the raw DB prices.  Using the *average* distance across all
        # pending limits of a signal makes every limit receive the same lot
        # size, so the combined risk of the full signal equals risk_percent.
        # ------------------------------------------------------------------
        avg_sl_distance_by_signal: dict[int, float] = {}
        for signal in signals:
            pending_lims = signal.get("pending_limits", [])
            if not pending_lims:
                continue
            db_sl = signal["stop_loss"]
            distances = [abs(lim["price_level"] - db_sl) for lim in pending_lims]
            avg_sl_distance_by_signal[signal["id"]] = sum(distances) / len(distances)

        # ------------------------------------------------------------------
        # Place missing orders / re-evaluate deferred limits
        # ------------------------------------------------------------------
        for limit_id, lim in db_pending.items():
            if limit_id in local_by_limit:
                continue  # Already have a live pending order — nothing to do.
            if limit_id in all_tracked_limit_ids:
                continue  # Has a mapping (filled / cancelled) — don't re-place.

            signal = db_signals.get(lim["signal_id"])
            if signal is None:
                continue

            # ---- Proximity check -----------------------------------------
            if self.proximity_enabled:
                instrument = signal["instrument"]
                symbol     = map_instrument_to_symbol(instrument, self.symbol_map)
                db_price   = lim["price_level"]

                # For offset instruments we need the translated MT5 price to
                # compute distance accurately. We fetch the offset here so we
                # can reuse it; if unavailable we fall back to the raw DB price
                # (the pip distance is the same regardless of offset).
                mt5_order_price = db_price
                if needs_feed_offset(instrument):
                    computed = await get_feed_offset(
                        self.pool, instrument, symbol, self.max_staleness,
                    )
                    if computed is not None:
                        mt5_order_price = db_price + computed

                prices = mt5_api.get_current_price(symbol)
                if prices is not None:
                    mt5_mid = (prices[0] + prices[1]) / 2.0
                    in_range = self._is_within_proximity(
                        mt5_order_price, mt5_mid, symbol, instrument
                    )
                else:
                    # Can't get price — treat as in-range and let _place_order_for_limit
                    # handle the failure gracefully.
                    in_range = True

                if not in_range:
                    threshold = self._get_proximity_threshold_pips(instrument)
                    distance  = mt5_api.price_to_pips(
                        abs(mt5_order_price - mt5_mid), symbol
                    ) if prices else float("nan")

                    if limit_id not in deferred_limit_ids:
                        logger.info(
                            f"Limit {limit_id} ({instrument} @ {db_price:.5f}) is "
                            f"{distance:.1f} pips away (threshold {threshold:.1f}) — "
                            f"deferring until price comes within range."
                        )
                        local_db.upsert_deferred_limit(limit_id, signal["id"])
                        deferred_limit_ids.add(limit_id)
                    else:
                        logger.debug(
                            f"Limit {limit_id} still deferred ({distance:.1f} pips, "
                            f"threshold {threshold:.1f})."
                        )
                    continue

                # Price is now within range — remove from deferred table and place.
                if limit_id in deferred_limit_ids:
                    logger.info(
                        f"Limit {limit_id} ({instrument}) has entered proximity range "
                        f"— placing order now."
                    )
                    local_db.remove_deferred_limit(limit_id)
                    deferred_limit_ids.discard(limit_id)

            await self._place_order_for_limit(
                lim, signal,
                avg_sl_distance=avg_sl_distance_by_signal.get(signal["id"]),
            )

        # ------------------------------------------------------------------
        # Cancel orders for limits no longer pending in DB
        # ------------------------------------------------------------------
        for limit_id, mapping in local_by_limit.items():
            if limit_id not in db_pending:
                ticket = mapping["mt5_ticket"]
                logger.info(f"Limit {limit_id} no longer pending in DB — cancelling ticket {ticket}")
                mt5_api.cancel_pending_order(ticket)
                local_db.mark_cancelled(ticket)

        # ------------------------------------------------------------------
        # Clean up deferred limits for limits no longer in DB
        # ------------------------------------------------------------------
        for limit_id in list(deferred_limit_ids):
            if limit_id not in db_pending:
                logger.info(
                    f"Deferred limit {limit_id} no longer pending in DB — removing."
                )
                local_db.remove_deferred_limit(limit_id)

        # ------------------------------------------------------------------
        # Cancel all orders / deferred entries for signals that left active/hit
        # ------------------------------------------------------------------
        all_tracked_signal_ids = local_db.get_all_tracked_signal_ids()
        all_deferred_signal_ids = local_db.get_all_deferred_signal_ids()
        active_ids = {s["id"] for s in signals}

        for signal_id in all_tracked_signal_ids:
            if signal_id not in active_ids:
                pending_tickets = local_db.cancel_all_pending_for_signal(signal_id)
                for ticket in pending_tickets:
                    logger.info(f"Signal {signal_id} gone from active — cancelling ticket {ticket}")
                    mt5_api.cancel_pending_order(ticket)

        for signal_id in all_deferred_signal_ids:
            if signal_id not in active_ids:
                removed = local_db.cancel_all_deferred_for_signal(signal_id)
                if removed:
                    logger.info(
                        f"Signal {signal_id} gone from active — "
                        f"removed {removed} deferred limit(s)."
                    )


    # ------------------------------------------------------------------
    # SL sync for already-filled positions
    # ------------------------------------------------------------------

    async def _sync_filled_position_sls(self) -> None:
        """
        When a signal's stop_loss is edited (e.g. sender updates the Discord
        message after a limit has already filled), the open MT5 position still
        carries the old SL.  This method detects that drift and updates the
        SL on every affected open position.

        Logic per filled mapping whose signal is still active/hit:
          1. Fetch the current stop_loss from the DB signal row.
          2. Compute what the MT5-space SL should be (apply feed offset for
             indices/crypto, plus spread adjustment matching the original
             direction).
          3. Compare to last_known_mt5_sl stored in orders.db.
          4. If different beyond one pip, call mt5_api.modify_position_sl()
             and update last_known_mt5_sl in local DB.
        """
        active_signals = await supabase_db.fetch_active_signals(self.pool)
        if not active_signals:
            return

        active_by_id = {s["id"]: s for s in active_signals}
        active_signal_ids = list(active_by_id.keys())

        filled_mappings = local_db.get_filled_mappings_by_signal_ids(active_signal_ids)
        if not filled_mappings:
            return

        for mapping in filled_mappings:
            signal_id = mapping["signal_id"]
            ticket    = mapping["mt5_ticket"]
            signal    = active_by_id.get(signal_id)
            if signal is None:
                continue

            instrument = signal["instrument"]
            symbol     = map_instrument_to_symbol(instrument, self.symbol_map)
            direction  = signal["direction"]
            db_sl      = signal["stop_loss"]

            # --- Translate DB stop_loss into MT5 price space ---
            offset = 0.0
            if needs_feed_offset(instrument):
                computed = await get_feed_offset(
                    self.pool, instrument, symbol, self.max_staleness,
                )
                if computed is None:
                    continue
                offset = computed

            spread = mt5_api.get_current_spread(symbol)
            if spread is None:
                spread = 0.0

            if direction == "long":
                mt5_sl = db_sl + offset - spread
            else:
                mt5_sl = db_sl + offset + spread

            last_known = mapping.get("last_known_mt5_sl")

            # On first run after migration, seed last_known_mt5_sl from MT5
            # so we only update on a genuine future change.
            if last_known is None:
                positions = mt5_api.get_open_positions()
                live_pos  = next((p for p in positions if p["ticket"] == ticket), None)
                if live_pos is None:
                    continue
                current_mt5_sl = live_pos.get("sl", 0.0) or 0.0
                local_db.update_known_mt5_sl(ticket, db_sl, current_mt5_sl)
                continue  # Seed only; evaluate on the next cycle.

            pip_size = mt5_api.pips_to_price(1.0, symbol)
            if pip_size <= 0:
                pip_size = 0.00001

            if abs(mt5_sl - last_known) < pip_size:
                continue

            logger.info(
                f"SL change detected for signal {signal_id}, ticket {ticket} "
                f"({symbol}): last_known_mt5_sl={last_known:.5f} -> new_mt5_sl={mt5_sl:.5f} "
                f"(db_sl={db_sl:.5f}, offset={offset:+.5f})"
            )

            success = mt5_api.modify_position_sl(ticket, mt5_sl, symbol)
            if success:
                local_db.update_known_mt5_sl(ticket, db_sl, mt5_sl)


    # ------------------------------------------------------------------
    # Lot size + spread recheck
    # ------------------------------------------------------------------

    async def _maybe_recheck_lot_sizes(self) -> None:
        """Gate: only runs every lot_recheck_interval seconds."""
        if time.monotonic() - self._last_lot_recheck < self.lot_recheck_interval:
            return
        self._last_lot_recheck = time.monotonic()
        await self._recheck_lot_sizes()

    async def _recheck_lot_sizes(self) -> None:
        """
        For every live pending order, recompute what the lot size should be
        given the current account balance and live spread.  If it differs from
        the stored lot_size by at least one volume step, cancel and re-place
        the order so risk stays anchored to risk_percent at all times.

        This catches balance changes (profits, deposits, withdrawals) and
        spread changes that have drifted since the order was originally placed.
        Lot size and spread are both recomputed fresh on each pass.
        """
        pending_mappings = local_db.get_pending_mappings()
        if not pending_mappings:
            return

        logger.debug(f"Lot-size recheck: {len(pending_mappings)} pending order(s).")

        account = mt5_api.get_account_info()
        if account is None:
            logger.warning("Lot-size recheck: cannot get account info — skipping.")
            return

        for mapping in pending_mappings:
            limit_id  = mapping["limit_id"]
            ticket    = mapping["mt5_ticket"]
            stored_lots = mapping.get("lot_size") or 0.0

            # Fetch fresh limit + signal data from DB
            lim_row = await supabase_db.fetch_limit_by_id(self.pool, limit_id)
            if lim_row is None or lim_row.get("status") != "pending":
                continue  # Limit gone / hit — _sync_orders will handle it

            instrument = lim_row["instrument"]
            symbol     = map_instrument_to_symbol(instrument, self.symbol_map)
            direction  = lim_row["direction"]
            db_sl      = lim_row["stop_loss"]
            db_price   = lim_row["price_level"]
            num_limits = lim_row.get("total_limits", 1) or 1

            # --- Compute avg SL distance across sibling pending limits ---
            sibling_limits = await supabase_db.fetch_pending_limits_for_signal(
                self.pool, lim_row["signal_id"]
            )
            if sibling_limits:
                distances = [abs(l["price_level"] - db_sl) for l in sibling_limits]
                avg_sl_distance = sum(distances) / len(distances)
            else:
                avg_sl_distance = abs(db_price - db_sl)

            # --- Fresh lot size with current balance and spread ---
            # Apply feed offset so spread is fetched against the right MT5 symbol
            offset = 0.0
            if needs_feed_offset(instrument):
                computed = await get_feed_offset(
                    self.pool, instrument, symbol, self.max_staleness,
                )
                if computed is None:
                    logger.debug(
                        f"Lot recheck: stale live_prices for {instrument} "
                        f"(ticket {ticket}) — skipping this cycle."
                    )
                    continue
                offset = computed

            spread = mt5_api.get_current_spread(symbol)
            if spread is None:
                spread = 0.0

            # Spread shifts the effective entry price but cancels out between
            # entry and SL (both shift the same direction), so avg_sl_distance
            # in DB space is still the right risk distance.  We pass it directly.
            new_lots = mt5_api.calculate_lot_size(
                account_balance=account.balance,
                risk_percent=self.risk_percent,
                num_limits=num_limits,
                sl_distance_price=avg_sl_distance,
                symbol=symbol,
                min_lot=self.min_lot,
            )

            # Only re-place if the difference is at least one volume step
            info = mt5_api.mt5.symbol_info(symbol)
            vol_step = (info.volume_step if info and info.volume_step > 0
                        else self.min_lot)

            if abs(new_lots - stored_lots) < vol_step:
                logger.debug(
                    f"Lot recheck ticket {ticket} ({symbol}): "
                    f"stored={stored_lots}, new={new_lots} — within one step, no change."
                )
                continue

            logger.info(
                f"Lot-size drift detected for ticket {ticket} ({symbol}): "
                f"stored={stored_lots} → new={new_lots} "
                f"(balance={account.balance:.2f}, avg_sl_dist={avg_sl_distance:.5f}) "
                f"— cancelling and re-placing."
            )

            cancelled = mt5_api.cancel_pending_order(ticket)
            if not cancelled:
                logger.warning(
                    f"Lot recheck: could not cancel ticket {ticket} "
                    f"— may have just filled, skipping."
                )
                continue

            local_db.mark_cancelled(ticket)

            # Re-place via the standard path (recomputes spread + offset fresh)
            signal_stub = {
                "id":           lim_row["signal_id"],
                "instrument":   instrument,
                "direction":    direction,
                "stop_loss":    db_sl,
                "total_limits": num_limits,
            }
            lim_stub = {
                "id":          lim_row["id"],
                "signal_id":   lim_row["signal_id"],
                "price_level": db_price,
            }
            await self._place_order_for_limit(
                lim_stub, signal_stub, avg_sl_distance=avg_sl_distance
            )

    async def _place_order_for_limit(
        self,
        lim: dict,
        signal: dict,
        avg_sl_distance: Optional[float] = None,
    ) -> None:
        """
        Place a single pending order for a limit level.

        For index/crypto instruments: translates the DB price_level and stop_loss
        from feed-price space into MT5-price space using the live offset, then
        places a normal pending order at the adjusted price.

        Spread adjustment
        ─────────────────
        The DB price_level/stop_loss represent the market price (mid/bid side) at
        which the signal should trigger.  MT5 pending orders fire on the wrong side
        of the spread, so both entry and SL are shifted by the current spread:

          LONG  entry  (+spread): BUY_LIMIT/BUY_STOP triggers on ASK  → shift up
          LONG  SL     (-spread): MT5 closes long when BID falls below → shift down (wider)
          SHORT entry  (-spread): SELL_LIMIT/SELL_STOP triggers on BID → shift down
          SHORT SL     (+spread): MT5 closes short when ASK rises above → shift up (wider)

        The spread is re-fetched live every time an order is placed, so drift is
        naturally handled — re-placement from the readjust loop also re-applies
        the current spread.
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

        # Step 1: translate DB prices into MT5 price space (feed offset)
        mt5_order_price = db_price + offset
        mt5_sl          = db_sl + offset

        # Current MT5 price (for order type resolution and spread fetch)
        prices = mt5_api.get_current_price(symbol)
        if prices is None:
            logger.warning(f"Cannot get MT5 price for {symbol} — skipping limit {lim['id']}")
            return

        bid, ask    = prices
        mt5_mid_now = (bid + ask) / 2.0

        # Step 2: spread adjustment
        # Re-fetch live spread so the adjustment is always current.
        spread = mt5_api.get_current_spread(symbol)
        if spread is None:
            logger.warning(
                f"Cannot fetch spread for {symbol} — placing without spread adjustment "
                f"for limit {lim['id']}"
            )
            spread = 0.0

        if direction == "long":
            # BUY orders trigger on ASK; DB price is bid-equivalent → shift entry up
            # SL triggers on BID; push it further down to avoid premature stops
            mt5_order_price += spread
            mt5_sl          -= spread
        else:  # short
            # SELL orders trigger on BID; DB price is ask-equivalent → shift entry down
            # SL triggers on ASK; push it further up to avoid premature stops
            mt5_order_price -= spread
            mt5_sl          += spread

        spread_pips = mt5_api.price_to_pips(spread, symbol)
        logger.debug(
            f"Spread adjustment {symbol}: spread={spread:.5f} ({spread_pips:.1f} pips), "
            f"direction={direction}, adjusted entry={mt5_order_price:.5f}, "
            f"adjusted sl={mt5_sl:.5f}"
        )

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

        # Lot sizing: use avg_sl_distance (pre-computed across all pending limits
        # of this signal) so every limit in the signal gets the same lot size
        # and the combined risk equals risk_percent.  Falls back to this limit's
        # own SL distance if no average was provided (e.g. single-limit signal).
        if avg_sl_distance is not None:
            sl_distance = avg_sl_distance  # DB price space — offset cancels between entry/SL
        else:
            # Spread-adjusted prices give the true MT5 risk distance for a single limit.
            sl_distance = abs(mt5_order_price - mt5_sl)

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
            db_stop_loss=signal["stop_loss"],
        )

        if use_offset and feed_mid is not None and mt5_mid is not None:
            local_db.update_offset_metadata(ticket, feed_mid, mt5_mid)

        logger.info(
            f"Placed {mt5_api.order_type_to_str(order_type)}: "
            f"ticket={ticket}, symbol={symbol}, lot={lot_size}, "
            f"price={mt5_order_price:.5f} (db={db_price:.5f}, offset={offset:+.5f}, "
            f"spread={spread:.5f}), sl={mt5_sl:.5f}, "
            f"limit_id={lim['id']}, signal_id={signal['id']}"
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

            # Reconstruct minimal dicts to re-use _place_order_for_limit.
            # Re-compute the average SL distance across all currently-pending
            # limits of this signal so the re-placed order keeps the same
            # equal lot sizing as the original batch.
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

            # Fetch sibling pending limits to recompute average SL distance.
            sibling_limits = await supabase_db.fetch_pending_limits_for_signal(
                self.pool, lim_row["signal_id"]
            )
            db_sl = lim_row["stop_loss"]
            if sibling_limits:
                distances = [abs(l["price_level"] - db_sl) for l in sibling_limits]
                avg_sl_distance = sum(distances) / len(distances)
            else:
                avg_sl_distance = abs(lim_row["price_level"] - db_sl)

            await self._place_order_for_limit(lim_stub, signal_stub, avg_sl_distance=avg_sl_distance)

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
"""
tp.py — Take-Profit engine.

Architecture
────────────
TPEngine is the orchestrator. It maintains a registry of open positions and
calls the active strategy on each tick.

Strategy interface (abstract base):
    class BaseTPStrategy:
        def on_tick(self, position: dict, context: TPContext) -> TPAction

TPContext bundles everything the strategy needs: current prices, all open
positions for the same signal, and config parameters.

TPAction is a dataclass describing what to do: close (full/partial), set
trailing stop, or do nothing.

To swap strategies, change the `strategy` kwarg in TPEngine.__init__ or
instantiate a different strategy class. All parameters live in config.json.

Built-in strategies
───────────────────
  DefaultTPStrategy  — mirrors the Limits-Alert-Bot logic:
      • Trigger when most-recently-hit limit is ≥ profit_threshold_pips in profit
        AND all other positions for the signal are ≥ breakeven_buffer_pips above entry.
      • On trigger:
          - Close ALL breakeven positions at 100%.
          - Close 50% of the profit position immediately.
          - Set trailing stop of trail_pips on remaining 50%.

Adding a new strategy
─────────────────────
    class MyStrategy(BaseTPStrategy):
        def on_tick(self, position, context):
            ...
            return TPAction(action="none")

    engine = TPEngine(config, strategy=MyStrategy(config))
"""

import logging
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import mt5 as mt5_api
import local_db
import db as supabase_db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TPContext:
    """All context a strategy needs to make a TP decision."""
    symbol:           str
    current_bid:      float
    current_ask:      float
    position_type:    int           # 0=buy, 1=sell
    entry_price:      float
    lot_size:         float
    signal_id:        int
    limit_id:         int
    # All open positions for the same signal (including this one)
    sibling_positions: list[dict] = field(default_factory=list)
    # Strategy params resolved for this instrument
    # profit_threshold and trail carry the resolved value; use_dollars flags
    # tell the strategy whether the value is in dollars or pips.
    profit_threshold:          float = 7.0
    trail:                     float = 3.0
    profit_threshold_dollars:  bool  = False
    trail_dollars:             bool  = False
    partial_close_pct:         float = 50.0
    is_scalp:                  bool  = False


@dataclass
class TPAction:
    """What the strategy wants to do for a given position on this tick."""
    action: str   # "none" | "close_full" | "close_partial" | "trail"
    close_lots: float = 0.0
    trail_pips: float = 0.0
    reason:     str   = ""


# ---------------------------------------------------------------------------
# Base strategy
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Asset class detection (mirrors sync.py logic)
# ---------------------------------------------------------------------------

def _detect_tp_asset_class(symbol: str) -> str:
    """Return one of: forex, forex_jpy, metals, indices, stocks, crypto, oil."""
    s = symbol.upper()
    # Metals
    if s in ("XAUUSD", "GOLD", "XAGUSD", "SILVER"):
        return "metals"
    # Oil
    if "OIL" in s or "WTI" in s or "BRENT" in s or s in ("USOILSPOT",):
        return "oil"
    # Stocks (check before index keywords to avoid .NAS misclassification)
    if s.endswith(".NAS") or s.endswith(".NYSE") or ".NAS-" in s or ".NYSE-" in s:
        return "stocks"
    # Indices
    if any(k in s for k in ("SPX", "NAS", "DAX", "JP225", "UK100", "DE30",
                              "US500", "USTEC", "HK50", "AUS200")):
        return "indices"
    # Crypto
    if s.endswith("USD") or s.endswith("USDT") or s.endswith("BTC"):
        if len(s) > 6:
            return "crypto"
    # Forex: 6-char pairs
    if len(s) == 6 and s.isalpha():
        if "JPY" in s:
            return "forex_jpy"
        return "forex"
    return "forex"  # safe fallback


class BaseTPStrategy(ABC):
    def __init__(self, config: dict):
        self.config = config
        self._tp_cfg = config.get("tp", {})

    def _resolve_params(self, symbol: str, is_scalp: bool = False) -> tuple[float, bool, float, bool]:
        """
        Resolve (profit_threshold, threshold_dollars, trail, trail_dollars)
        for a symbol, taking scalp flag into account.

        Priority: per-instrument override > asset-class default.
        New config format:
            {
              "defaults":        { "forex": {"type":"pips","value":5.0,"trail":3.0}, ... },
              "scalp_defaults":  { "forex": {"type":"pips","value":3.0,"trail":2.0}, ... },
              "overrides":       { "XAUUSD": {"type":"dollars","value":2.0,"trail":1.5} },
              "scalp_overrides": { "XAUUSD": {"type":"dollars","value":1.0,"trail":0.8} },
              "partial_close_percent": 50
            }
        """
        sym = symbol.upper()
        overrides_key = "scalp_overrides" if is_scalp else "overrides"
        defaults_key  = "scalp_defaults"  if is_scalp else "defaults"

        # Try per-instrument override first
        override = self._tp_cfg.get(overrides_key, {}).get(sym)
        if override:
            t = override.get("type", "pips")
            v = float(override.get("value", 5.0))
            tr = float(override.get("trail", v))
            dollars = (t == "dollars")
            return v, dollars, tr, dollars

        # Fall back to asset class default
        asset_class = _detect_tp_asset_class(sym)
        cls_defaults = self._tp_cfg.get(defaults_key, {})

        # Fallback chain: exact class → parent class → hardcoded
        hardcoded = {"forex": (5.0,"pips"), "forex_jpy": (10.0,"pips"),
                     "metals": (5.0,"dollars"), "indices": (20.0,"dollars"),
                     "stocks": (1.0,"dollars"), "crypto": (50.0,"dollars"),
                     "oil": (0.5,"dollars")}
        if asset_class in cls_defaults:
            entry = cls_defaults[asset_class]
        elif asset_class == "forex_jpy" and "forex" in cls_defaults:
            entry = cls_defaults["forex"]
        else:
            hv, ht = hardcoded.get(asset_class, (5.0, "pips"))
            entry = {"type": ht, "value": hv, "trail": hv}

        t  = entry.get("type", "pips")
        v  = float(entry.get("value", 5.0))
        tr = float(entry.get("trail", v))
        dollars = (t == "dollars")
        return v, dollars, tr, dollars

    def get_profit_threshold(self, symbol: str, is_scalp: bool = False) -> tuple[float, bool]:
        v, d, _, _ = self._resolve_params(symbol, is_scalp)
        return v, d

    def get_trail(self, symbol: str, is_scalp: bool = False) -> tuple[float, bool]:
        _, _, tr, td = self._resolve_params(symbol, is_scalp)
        return tr, td

    def get_partial_close_pct(self) -> float:
        return self._tp_cfg.get("partial_close_percent", 50)

    @abstractmethod
    def on_tick(self, position: dict, context: TPContext) -> TPAction:
        """
        Called on every tick for every tracked open position.
        Return a TPAction describing what to do.
        """
        ...


# ---------------------------------------------------------------------------
# Default strategy
# ---------------------------------------------------------------------------

class DefaultTPStrategy(BaseTPStrategy):
    """
    Mirrors the Limits-Alert-Bot auto-TP logic with a partial-close twist.

    Trigger conditions (per-signal, evaluated once per tick):
      1. The most-recently-hit position is >= profit_threshold in profit.
      2. All other open positions for this signal are at breakeven (>= entry).

    On trigger:
      • Breakeven positions: close 100% immediately.
      • Profit position (most recently hit): close 50% immediately, trail remaining 50%.

    Once a position has entered the trailing phase, update the trailing stop
    on every tick.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        # Track which tickets are in "trailing" phase
        self._trailing: dict[int, float] = {}   # ticket -> trail value in pips

    def on_tick(self, position: dict, context: TPContext) -> TPAction:
        ticket = position["ticket"]

        # If already trailing, just update the stop
        if ticket in self._trailing:
            return TPAction(
                action="trail",
                trail_pips=self._trailing[ticket],
                reason="trailing stop update",
            )

        # Check if trigger conditions are met across all sibling positions
        if not self._trigger_conditions_met(position, context):
            return TPAction(action="none")

        # Determine role: is this the "profit" position or a "breakeven" position?
        profit_ticket = self._most_recently_hit_ticket(context.sibling_positions)

        if ticket == profit_ticket:
            # Profit position: close partial, then trail
            close_pct  = context.partial_close_pct / 100.0
            close_lots = math.floor(position["volume"] * close_pct * 100) / 100  # floor to 0.01
            close_lots = max(close_lots, 0.01)

            # Convert trail to pips for MT5 (trail value stored in context is already
            # in the correct unit; convert dollars -> pips here if needed)
            if context.trail_dollars:
                trail_pips = mt5_api.price_to_pips(context.trail, context.symbol)
            else:
                trail_pips = context.trail
            self._trailing[ticket] = trail_pips

            return TPAction(
                action="close_partial",
                close_lots=close_lots,
                trail_pips=trail_pips,
                reason=f"TP triggered — closing {close_pct*100:.0f}%, trailing {trail_pips:.1f} pips",
            )
        else:
            # Breakeven position: close 100%
            return TPAction(
                action="close_full",
                close_lots=position["volume"],
                reason="TP triggered — closing breakeven position",
            )

    def _price_move(self, position: dict, context: TPContext) -> float:
        """Raw price-unit profit for a position (positive = in profit)."""
        entry = position.get("price_open", context.entry_price)
        if position.get("type", context.position_type) == 0:   # buy
            return context.current_bid - entry
        else:  # sell
            return entry - context.current_ask

    def _trigger_conditions_met(self, position: dict, context: TPContext) -> bool:
        """
        Returns True if BOTH conditions are met simultaneously:
          1. The most-recently-hit position is >= profit_threshold in profit.
          2. The COMBINED P&L of all other positions is >= 0 (breakeven in aggregate).

        Mirrors the server bot (tp_monitor.py) logic exactly:
          - Condition 1 must clear first — if the last limit hasn't hit its
            threshold yet, we never TP regardless of the other positions.
          - Condition 2 uses a combined sum, not a per-position check, so one
            earlier position being slightly underwater is acceptable as long as
            the others compensate.

        profit_threshold is compared in dollars if context.profit_threshold_dollars,
        otherwise in pips. The breakeven sum uses raw price-move units (same as
        the server bot's dollar-based calculate_pnl for non-forex instruments).
        """
        EPSILON = 1e-9

        siblings = context.sibling_positions
        if not siblings:
            return False

        profit_ticket = self._most_recently_hit_ticket(siblings)
        profit_pos = next((p for p in siblings if p["ticket"] == profit_ticket), None)
        if profit_pos is None:
            return False

        # Condition 1: last-hit position must have cleared the profit threshold.
        # If this hasn't happened yet there's nothing more to check.
        if context.profit_threshold_dollars:
            if self._price_move(profit_pos, context) < context.profit_threshold - EPSILON:
                return False
        else:
            profit_pips = mt5_api.price_to_pips(self._price_move(profit_pos, context), context.symbol)
            if profit_pips < context.profit_threshold - EPSILON:
                return False

        # Condition 2: combined P&L of all OTHER positions must be >= 0.
        # A single underwater position is fine as long as the aggregate is
        # non-negative — matches the server bot's combined_earlier_pnl check.
        earlier_positions = [p for p in siblings if p["ticket"] != profit_ticket]
        if earlier_positions:
            combined_pnl = sum(self._price_move(p, context) for p in earlier_positions)
            if combined_pnl < -EPSILON:
                return False

        return True

    def _most_recently_hit_ticket(self, positions: list[dict]) -> Optional[int]:
        """
        The 'most recently hit' position is the one with the highest ticket number
        (MT5 assigns tickets sequentially) among filled positions.
        """
        if not positions:
            return None
        return max(p["ticket"] for p in positions)

    def on_position_closed(self, ticket: int) -> None:
        """Call when a position is closed so trailing state can be cleaned up."""
        self._trailing.pop(ticket, None)


# ---------------------------------------------------------------------------
# TPEngine
# ---------------------------------------------------------------------------

class TPEngine:
    """
    Manages all open positions and drives the TP strategy on each tick.

    Usage:
        engine = TPEngine(config, pool=pool, mt5_account="123", discord_id="456",
                          license_key="abc...", strategy=DefaultTPStrategy(config))
        engine.register_position(ticket, mapping)  # called by sync on fill
        engine.run_tick()                           # called every poll cycle
    """

    def __init__(
        self,
        config: dict,
        strategy: BaseTPStrategy = None,
        pool=None,
        mt5_account: str = "",
        discord_id: str = "",
        license_key: str = "",
        bot_version: str = "",
        on_tp_fired=None,
    ):
        self.config      = config
        self.strategy    = strategy or DefaultTPStrategy(config)
        self.symbol_map  = config.get("symbol_map", {})
        self.pool        = pool
        self.mt5_account = mt5_account
        self.discord_id  = discord_id
        self.license_key = license_key
        self.bot_version = bot_version

        # Optional callback: on_tp_fired(signal_id) — called when a partial-close
        # TP triggers for a signal so the caller can cancel remaining pending orders.
        self._on_tp_fired = on_tp_fired

        # ticket → {ticket, signal_id, limit_id, symbol, lot_size, ...}
        self._positions: dict[int, dict] = {}

        # Track which signal_ids have already had TP fired this session so we
        # only invoke the callback once per signal (not on every subsequent tick).
        self._tp_fired_signals: set[int] = set()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_position(self, ticket: int, mapping: dict) -> None:
        """
        Register a newly filled position.
        Called by SyncEngine when it detects a fill.
        """
        # Fetch full position details from MT5
        live = [p for p in mt5_api.get_open_positions()
                if p["ticket"] == ticket]
        if not live:
            logger.warning(f"TPEngine.register_position: ticket {ticket} not found in MT5 positions.")
            # Store with mapping data as fallback
            self._positions[ticket] = {
                "ticket":   ticket,
                "signal_id": mapping["signal_id"],
                "limit_id":  mapping["limit_id"],
                "lot_size":  mapping.get("lot_size", 0.01),
            }
            return

        pos = live[0]
        pos["signal_id"] = mapping["signal_id"]
        pos["limit_id"]  = mapping["limit_id"]
        pos["is_scalp"]  = mapping.get("is_scalp", False)
        self._positions[ticket] = pos
        logger.info(
            f"TPEngine: registered position ticket={ticket}, "
            f"signal_id={mapping['signal_id']}, symbol={pos['symbol']}, "
            f"lots={pos['volume']}"
        )

    def force_close_signal(self, signal_id: int, reason: str = "forced") -> None:
        """
        Immediately close all tracked open positions for a signal at market.

        Called by ForcedExitMonitor when the operator manually marks a signal
        as 'cancelled' or 'breakeven' after limits have already filled.

        Each position is closed at full volume.  The outcome is recorded as
        the supplied reason string (e.g. 'manual_cancelled', 'manual_breakeven').
        Positions that are no longer open in MT5 (closed externally between
        cycles) are silently removed from the tracker.
        """
        positions_for_signal = [
            (ticket, pos) for ticket, pos in list(self._positions.items())
            if pos.get("signal_id") == signal_id
        ]

        if not positions_for_signal:
            logger.info(
                f"TPEngine.force_close_signal: no tracked positions for "
                f"signal {signal_id} — nothing to close."
            )
            return

        logger.warning(
            f"TPEngine.force_close_signal: closing {len(positions_for_signal)} "
            f"position(s) for signal {signal_id} (reason={reason})."
        )

        # Refresh live positions once so we have current volume and price.
        live_positions = {p["ticket"]: p for p in mt5_api.get_open_positions()}

        for ticket, pos in positions_for_signal:
            symbol = pos.get("symbol", "")
            live   = live_positions.get(ticket)

            if live is None:
                # Position already closed outside our control.
                logger.info(
                    f"TPEngine.force_close_signal: ticket {ticket} ({symbol}) "
                    f"not found in live positions — removing from tracker."
                )
                self._record_outcome_sync(ticket, pos, "manual_close")
                self._remove_position(ticket)
                continue

            volume = live.get("volume", pos.get("lot_size", 0.01))
            success = mt5_api.close_position(
                ticket, volume, symbol, comment=reason[:31]
            )

            if success:
                logger.info(
                    f"TPEngine.force_close_signal: closed ticket {ticket} "
                    f"({symbol}), {volume} lots. Reason: {reason}."
                )
                prices = mt5_api.get_current_price(symbol)
                pos_type = live.get("type", pos.get("type", 0))
                close_price = (prices[0] if pos_type == 0 else prices[1]) if prices else None
                self._record_outcome_sync(ticket, pos, reason, close_price=close_price)
                self._remove_position(ticket)
            else:
                logger.error(
                    f"TPEngine.force_close_signal: failed to close ticket {ticket} "
                    f"({symbol}). Will retry next cycle."
                )

    # ------------------------------------------------------------------
    # Tick loop
    # ------------------------------------------------------------------

    def run_tick(self) -> None:
        """
        Called every poll cycle. Evaluates TP conditions for all tracked positions.
        """
        if not self._positions:
            return

        # Refresh position data from MT5
        live_positions = {p["ticket"]: p for p in mt5_api.get_open_positions()}

        # Detect positions that closed outside our control
        closed_tickets = [t for t in list(self._positions) if t not in live_positions]
        for ticket in closed_tickets:
            pos = self._positions.get(ticket, {})
            # If this ticket was in the trailing phase, it's a trail close (trail stop hit)
            in_trailing = hasattr(self.strategy, "_trailing") and ticket in self.strategy._trailing
            outcome_type = "tp_trail_close" if in_trailing else "manual_close"
            logger.info(
                f"TPEngine: position {ticket} closed externally "
                f"(outcome={outcome_type}) — removing from tracker."
            )
            self._record_outcome_sync(ticket, pos, outcome_type, close_price=None)
            self._remove_position(ticket)

        # Group open positions by signal_id for sibling awareness
        by_signal: dict[int, list[dict]] = {}
        for ticket, pos in self._positions.items():
            if ticket not in live_positions:
                continue
            # Refresh live data
            live_data = live_positions[ticket]
            live_data["signal_id"] = pos["signal_id"]
            live_data["limit_id"]  = pos["limit_id"]
            live_data["is_scalp"]  = pos.get("is_scalp", False)
            self._positions[ticket] = live_data
            by_signal.setdefault(pos["signal_id"], []).append(live_data)

        # Process each position
        for signal_id, sibling_list in by_signal.items():
            for position in sibling_list:
                ticket = position["ticket"]
                symbol = position["symbol"]

                prices = mt5_api.get_current_price(symbol)
                if prices is None:
                    continue
                bid, ask = prices

                mapping = local_db.get_mapping_by_ticket(ticket)
                limit_id = position.get("limit_id", 0)
                is_scalp = position.get("is_scalp", False)

                pt_value, pt_dollars = self.strategy.get_profit_threshold(symbol, is_scalp)
                trail_value, trail_dollars = self.strategy.get_trail(symbol, is_scalp)

                context = TPContext(
                    symbol                     = symbol,
                    current_bid                = bid,
                    current_ask                = ask,
                    position_type              = position["type"],
                    entry_price                = position["price_open"],
                    lot_size                   = position["volume"],
                    signal_id                  = signal_id,
                    limit_id                   = limit_id,
                    sibling_positions          = sibling_list,
                    profit_threshold           = pt_value,
                    profit_threshold_dollars   = pt_dollars,
                    trail                      = trail_value,
                    trail_dollars              = trail_dollars,
                    partial_close_pct          = self.strategy.get_partial_close_pct(),
                    is_scalp                   = is_scalp,
                )

                action = self.strategy.on_tick(position, context)
                self._execute_action(ticket, position, context, action)

    # ------------------------------------------------------------------
    # Action execution
    # ------------------------------------------------------------------

    def _execute_action(
        self,
        ticket:   int,
        position: dict,
        context:  TPContext,
        action:   TPAction,
    ) -> None:

        if action.action == "none":
            return

        symbol = context.symbol

        if action.action == "close_full":
            logger.info(f"TPEngine: closing full position ticket={ticket} ({symbol}). Reason: {action.reason}")
            success = mt5_api.close_position(ticket, position["volume"], symbol, comment="tp_full")
            if success:
                # Determine outcome: breakeven_close or sl
                outcome_type = "breakeven_close"
                if "stop" in action.reason.lower() or "sl" in action.reason.lower():
                    outcome_type = "sl"
                prices = mt5_api.get_current_price(symbol)
                close_price = (prices[0] if position.get("type") == 0 else prices[1]) if prices else None
                self._record_outcome_sync(ticket, position, outcome_type, close_price=close_price, context=context)
                self._remove_position(ticket)

        elif action.action == "close_partial":
            logger.info(
                f"TPEngine: partial close ticket={ticket} ({symbol}), "
                f"lots={action.close_lots}. Reason: {action.reason}"
            )
            success = mt5_api.close_position(ticket, action.close_lots, symbol, comment="tp_partial")
            if success:
                prices = mt5_api.get_current_price(symbol)
                close_price = (prices[0] if position.get("type") == 0 else prices[1]) if prices else None
                self._record_outcome_sync(ticket, position, "tp_partial", close_price=close_price, context=context)
                if action.trail_pips > 0:
                    # Set initial trailing stop
                    trail_points = int(action.trail_pips * self._get_pip_points(symbol))
                    mt5_api.set_trailing_stop(ticket, trail_points, symbol)

                # Cancel all remaining pending orders for this signal now that
                # TP has fired — only invoke the callback once per signal.
                signal_id = position.get("signal_id")
                if signal_id is not None and signal_id not in self._tp_fired_signals:
                    self._tp_fired_signals.add(signal_id)
                    if self._on_tp_fired is not None:
                        try:
                            self._on_tp_fired(signal_id)
                        except Exception as exc:
                            logger.warning(
                                f"TPEngine: on_tp_fired callback raised for signal "
                                f"{signal_id}: {exc}"
                            )

        elif action.action == "trail":
            trail_points = int(action.trail_pips * self._get_pip_points(symbol))
            mt5_api.set_trailing_stop(ticket, trail_points, symbol)

    def _get_pip_points(self, symbol: str) -> int:
        """Return number of MT5 points per pip for a symbol."""
        try:
            import MetaTrader5 as _mt5
            info = _mt5.symbol_info(symbol)
            if info:
                digits = info.digits
                # 5-digit broker: 1 pip = 10 points
                return 10 if digits in (5, 3) else 1
        except Exception:
            pass
        return 10   # safe default for 5-digit brokers

    # ------------------------------------------------------------------
    # Outcome recording
    # ------------------------------------------------------------------

    def _record_outcome_sync(
        self,
        ticket: int,
        position: dict,
        outcome_type: str,
        close_price: Optional[float] = None,
        context: Optional[TPContext] = None,
    ) -> None:
        """
        Fire-and-forget: schedule an asyncio task to INSERT into tp_outcomes.
        Safe to call from synchronous run_tick() context.
        """
        if not self.pool:
            return

        try:
            import asyncio
            loop = asyncio.get_event_loop()
            loop.create_task(
                self._record_outcome(ticket, position, outcome_type, close_price, context)
            )
        except RuntimeError:
            pass  # No running event loop — skip recording

    # DB check constraint only allows these outcome values.
    _VALID_OUTCOMES = frozenset({
        "tp_partial", "tp_trail_close", "sl", "breakeven_close", "manual_close"
    })

    async def _record_outcome(
        self,
        ticket: int,
        position: dict,
        outcome_type: str,
        close_price: Optional[float] = None,
        context: Optional[TPContext] = None,
    ) -> None:
        """Build and insert a tp_outcomes row."""
        # Normalise to the DB check-constraint's allowed set.  Reason strings
        # like 'manual_breakeven' or 'manual_cancelled' come from
        # ForcedExitMonitor and are not valid DB values — map them to
        # 'manual_close' so the INSERT doesn't violate the constraint.
        if outcome_type not in self._VALID_OUTCOMES:
            outcome_type = "manual_close"
        symbol = position.get("symbol", "")
        lot_size = position.get("volume", position.get("lot_size"))
        fill_price = position.get("price_open")
        signal_id = position.get("signal_id")
        is_scalp = bool(position.get("is_scalp", False))
        direction = "long" if position.get("type", 0) == 0 else "short"

        # Map MT5 symbol back to DB instrument name (reverse symbol_map)
        reverse_map = {v.upper(): k for k, v in self.symbol_map.items()}
        db_instrument = reverse_map.get(symbol.upper(), symbol)

        asset_class = _detect_tp_asset_class(symbol)

        # Resolve TP config that was active for this position
        tp_type = tp_threshold_value = tp_trail_amount = tp_config_source = None
        tp_partial_close_pct = None
        if context:
            tp_type = "dollars" if context.profit_threshold_dollars else "pips"
            tp_threshold_value = context.profit_threshold
            tp_trail_amount = context.trail
            tp_partial_close_pct = int(context.partial_close_pct)
            # Determine config source: override or defaults
            sym_upper = symbol.upper()
            scalp_key = "scalp_overrides" if is_scalp else "overrides"
            defaults_key = "scalp_defaults" if is_scalp else "defaults"
            tp_cfg = self.config.get("tp", {})
            if sym_upper in tp_cfg.get(scalp_key, {}):
                tp_config_source = f"{scalp_key}.{sym_upper}"
            else:
                tp_config_source = f"{defaults_key}.{asset_class}"
        elif self.strategy:
            try:
                tv, td = self.strategy.get_profit_threshold(symbol, is_scalp)
                trv, _ = self.strategy.get_trail(symbol, is_scalp)
                tp_type = "dollars" if td else "pips"
                tp_threshold_value = tv
                tp_trail_amount = trv
                tp_partial_close_pct = int(self.strategy.get_partial_close_pct())
                tp_config_source = "defaults"
            except Exception:
                pass

        # PnL calculation (approximate from prices)
        pnl_dollars = pnl_pips = None
        if fill_price and close_price:
            price_move = (close_price - fill_price) if direction == "long" else (fill_price - close_price)
            pnl_pips = mt5_api.price_to_pips(price_move, symbol)
            # Approximate pnl in dollars: pips * pip_value_per_lot * lots
            # We don't have exact pip value here, so store pips for now; pnl_dollars left None
            # unless position already has profit info
        if "profit" in position:
            pnl_dollars = position["profit"]

        # filled_at: parse from local_db if available
        filled_at = None
        try:
            mapping = local_db.get_mapping_by_ticket(ticket)
            if mapping and mapping.get("filled_at"):
                filled_at = datetime.fromisoformat(mapping["filled_at"])
        except Exception:
            pass

        row = dict(
            mt5_account          = self.mt5_account,
            discord_id           = self.discord_id,
            license_key          = self.license_key,
            signal_id            = signal_id,
            mt5_ticket           = ticket,
            symbol               = symbol,
            db_instrument        = db_instrument,
            asset_class          = asset_class,
            direction            = direction,
            is_scalp             = is_scalp,
            outcome              = outcome_type,
            fill_price           = fill_price,
            close_price          = close_price,
            lot_size             = lot_size,
            pnl_dollars          = pnl_dollars,
            pnl_pips             = pnl_pips,
            tp_type              = tp_type,
            tp_threshold_value   = tp_threshold_value,
            tp_trail_amount      = tp_trail_amount,
            tp_partial_close_pct = tp_partial_close_pct,
            tp_config_source     = tp_config_source,
            filled_at            = filled_at,
            bot_version          = self.bot_version,
        )

        success = await supabase_db.insert_tp_outcome(self.pool, row)
        if not success:
            logger.warning(f"tp_outcomes: failed to record {outcome_type} for ticket={ticket}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def _remove_position(self, ticket: int) -> None:
        self._positions.pop(ticket, None)
        if hasattr(self.strategy, "on_position_closed"):
            self.strategy.on_position_closed(ticket)
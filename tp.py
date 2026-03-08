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
from typing import Optional

import mt5 as mt5_api
import local_db

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
        Returns True if:
          1. The most-recently-hit sibling has moved >= profit_threshold in profit.
          2. All other open positions for this signal are at or above entry (breakeven).

        profit_threshold is compared in dollars if context.profit_threshold_dollars,
        otherwise in pips. Breakeven check is always in the same unit.
        """
        siblings = context.sibling_positions
        if not siblings:
            return False

        profit_ticket = self._most_recently_hit_ticket(siblings)
        profit_pos = next((p for p in siblings if p["ticket"] == profit_ticket), None)
        if profit_pos is None:
            return False

        # Check profit position has hit threshold
        if context.profit_threshold_dollars:
            if self._price_move(profit_pos, context) < context.profit_threshold:
                return False
        else:
            profit_pips = mt5_api.price_to_pips(self._price_move(profit_pos, context), context.symbol)
            if profit_pips < context.profit_threshold:
                return False

        # Check all other positions are at breakeven (>= entry)
        for pos in siblings:
            if pos["ticket"] == profit_ticket:
                continue
            if self._price_move(pos, context) < 0:
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
        engine = TPEngine(config)
        engine.register_position(ticket, mapping)  # called by sync on fill
        engine.run_tick()                           # called every poll cycle
    """

    def __init__(self, config: dict, strategy: BaseTPStrategy = None):
        self.config   = config
        self.strategy = strategy or DefaultTPStrategy(config)
        self.symbol_map = config.get("symbol_map", {})

        # ticket → {ticket, signal_id, limit_id, symbol, lot_size, ...}
        self._positions: dict[int, dict] = {}

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
            logger.info(f"TPEngine: position {ticket} closed externally — removing from tracker.")
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
                self._remove_position(ticket)

        elif action.action == "close_partial":
            logger.info(
                f"TPEngine: partial close ticket={ticket} ({symbol}), "
                f"lots={action.close_lots}. Reason: {action.reason}"
            )
            success = mt5_api.close_position(ticket, action.close_lots, symbol, comment="tp_partial")
            if success and action.trail_pips > 0:
                # Set initial trailing stop
                trail_points = int(action.trail_pips * self._get_pip_points(symbol))
                mt5_api.set_trailing_stop(ticket, trail_points, symbol)

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
    # Cleanup
    # ------------------------------------------------------------------

    def _remove_position(self, ticket: int) -> None:
        self._positions.pop(ticket, None)
        if hasattr(self.strategy, "on_position_closed"):
            self.strategy.on_position_closed(ticket)
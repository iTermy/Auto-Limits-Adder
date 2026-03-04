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
    profit_threshold_pips: float = 7.0
    breakeven_buffer_pips: float = 2.0
    partial_close_pct:     float = 50.0
    trail_pips:            float = 3.0


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

class BaseTPStrategy(ABC):
    def __init__(self, config: dict):
        self.config = config
        self._tp_cfg = config.get("tp", {})

    def get_profit_threshold_pips(self, symbol: str) -> float:
        per_inst = self._tp_cfg.get("profit_threshold_pips", {}).get("per_instrument", {})
        default  = self._tp_cfg.get("profit_threshold_pips", {}).get("default", 7)
        return per_inst.get(symbol, default)

    def get_breakeven_buffer_pips(self, symbol: str) -> float:
        per_inst = self._tp_cfg.get("breakeven_buffer_pips", {}).get("per_instrument", {})
        default  = self._tp_cfg.get("breakeven_buffer_pips", {}).get("default", 2)
        return per_inst.get(symbol, default)

    def get_trail_pips(self, symbol: str) -> float:
        per_inst = self._tp_cfg.get("trail_pips", {}).get("per_instrument", {})
        default  = self._tp_cfg.get("trail_pips", {}).get("default", 3)
        return per_inst.get(symbol, default)

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
      1. The most-recently-hit position is ≥ profit_threshold_pips in profit.
      2. All other positions for this signal are ≥ breakeven_buffer_pips above entry.

    On trigger:
      • Breakeven positions: close 100% immediately.
      • Profit position (most recently hit): close 50% immediately, trail remaining 50%.

    Once a position has entered the trailing phase, update the trailing stop
    on every tick.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        # Track which tickets are in "trailing" phase
        self._trailing: dict[int, float] = {}   # ticket → trail_pips

    def on_tick(self, position: dict, context: TPContext) -> TPAction:
        ticket = position["ticket"]

        # If already trailing, just update the stop
        if ticket in self._trailing:
            return TPAction(
                action="trail",
                trail_pips=self._trailing[ticket],
                reason="trailing stop update",
            )

        # Compute how many pips this position is currently in profit
        profit_pips = self._pips_in_profit(position, context)

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

            trail = context.trail_pips
            self._trailing[ticket] = trail

            return TPAction(
                action="close_partial",
                close_lots=close_lots,
                trail_pips=trail,
                reason=f"TP triggered — closing {close_pct*100:.0f}%, trailing {trail} pips",
            )
        else:
            # Breakeven position: close 100%
            return TPAction(
                action="close_full",
                close_lots=position["volume"],
                reason="TP triggered — closing breakeven position",
            )

    def _pips_in_profit(self, position: dict, context: TPContext) -> float:
        entry = position.get("price_open", context.entry_price)
        if position.get("type", context.position_type) == 0:   # buy
            current = context.current_bid
            return mt5_api.price_to_pips(current - entry, context.symbol)
        else:  # sell
            current = context.current_ask
            return mt5_api.price_to_pips(entry - current, context.symbol)

    def _trigger_conditions_met(self, position: dict, context: TPContext) -> bool:
        """
        Returns True if:
          1. The most-recently-hit sibling is ≥ profit_threshold_pips in profit, AND
          2. All other siblings are ≥ breakeven_buffer_pips above entry.
        """
        siblings = context.sibling_positions
        if not siblings:
            return False

        profit_ticket = self._most_recently_hit_ticket(siblings)

        # Find the profit position and check it's enough in profit
        profit_pos = next((p for p in siblings if p["ticket"] == profit_ticket), None)
        if profit_pos is None:
            return False

        profit_pips = self._calc_pips_profit(profit_pos, context)
        if profit_pips < context.profit_threshold_pips:
            return False

        # Check all OTHER positions are at breakeven
        for pos in siblings:
            if pos["ticket"] == profit_ticket:
                continue
            pos_pips = self._calc_pips_profit(pos, context)
            if pos_pips < context.breakeven_buffer_pips:
                return False

        return True

    def _calc_pips_profit(self, position: dict, context: TPContext) -> float:
        entry = position.get("price_open", 0.0)
        if position.get("type", 0) == 0:   # buy
            return mt5_api.price_to_pips(context.current_bid - entry, context.symbol)
        else:
            return mt5_api.price_to_pips(entry - context.current_ask, context.symbol)

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

                context = TPContext(
                    symbol                = symbol,
                    current_bid           = bid,
                    current_ask           = ask,
                    position_type         = position["type"],
                    entry_price           = position["price_open"],
                    lot_size              = position["volume"],
                    signal_id             = signal_id,
                    limit_id              = limit_id,
                    sibling_positions     = sibling_list,
                    profit_threshold_pips = self.strategy.get_profit_threshold_pips(symbol),
                    breakeven_buffer_pips = self.strategy.get_breakeven_buffer_pips(symbol),
                    partial_close_pct     = self.strategy.get_partial_close_pct(),
                    trail_pips            = self.strategy.get_trail_pips(symbol),
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
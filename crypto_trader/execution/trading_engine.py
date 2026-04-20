"""
Trading engine that orchestrates strategy signals, risk management, and execution.
Includes startup cleanup, state persistence, and exchange-only SL/TP monitoring.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal
from pathlib import Path

from ..strategy.base import Strategy, Signal, SignalType
from ..data.market_data import MarketData
from ..execution.exchange import (
    ExchangeInterface, Order, OrderType, OrderSide
)
from ..execution.paper_exchange import PaperExchange
from ..risk.risk_manager import RiskManager
from ..infra.config import TradingConfig, TradingMode
from ..infra.logger import LogMixin

STATE_FILE = Path(".") / "trading_state.json"


class TradingEngine(LogMixin):
    """Core trading engine that connects strategy, risk, and execution."""

    def __init__(
        self,
        config: TradingConfig,
        strategy: Strategy,
        exchange: ExchangeInterface,
        market_data: MarketData,
        risk_manager: Optional[RiskManager] = None
    ):
        super().__init__()
        self.config = config
        self.strategy = strategy
        self.exchange = exchange
        self.market_data = market_data
        self.is_running = False
        self.is_paper = isinstance(exchange, PaperExchange)

        if risk_manager is None:
            risk_manager = RiskManager()
        self.risk_manager = risk_manager

        self.trade_count = 0
        self.signal_count = 0
        self.last_trade_time: Optional[datetime] = None
        self._sl_tp_map: Dict[str, Dict[str, float]] = {}

        self._has_open_position: Dict[str, bool] = {}
        self._tp_order_ids: Dict[str, str] = {}
        self._sl_order_ids: Dict[str, str] = {}

        from ..infra.config import get_config
        strategy_config = get_config().strategy
        self._confidence_threshold: float = strategy_config.confidence_threshold
        self._min_confidence: float = strategy_config.confidence_threshold
        self._max_confidence: float = min(strategy_config.confidence_threshold + 0.15, 0.95)
        self._confidence_step: float = 0.02
        self._confidence_decay_cycles: int = 0
        self._confidence_decay_interval: int = 3

        self._win_count: int = 0
        self._loss_count: int = 0
        self._total_closed: int = 0
        self._win_rate: float = 0.0
        self._win_rate_threshold: float = 0.80

        self._stop_event: Optional[asyncio.Event] = None

        self._load_state()

        self.logger.info(
            f"TradingEngine initialized: "
            f"mode={'PAPER' if self.is_paper else 'LIVE'}, "
            f"symbols={config.symbols}"
        )

    @property
    def confidence_threshold(self) -> float:
        return self._confidence_threshold

    @property
    def win_rate(self) -> float:
        if self._total_closed == 0:
            return 0.0
        return self._win_count / self._total_closed

    @property
    def has_position(self) -> bool:
        return any(self._has_open_position.values())

    def _adjust_confidence_on_win(self) -> None:
        self._confidence_threshold = max(
            self._min_confidence,
            self._confidence_threshold - self._confidence_step
        )
        self._confidence_decay_cycles = 0
        self.logger.info(
            f"[CONFIDENCE] Take profit hit -> threshold decreased to {self._confidence_threshold:.0%}"
        )

    def _adjust_confidence_on_loss(self) -> None:
        self._confidence_threshold = min(
            self._max_confidence,
            self._confidence_threshold + self._confidence_step
        )
        self._confidence_decay_cycles = 0
        self.logger.info(
            f"[CONFIDENCE] Stop loss hit -> threshold increased to {self._confidence_threshold:.0%} "
            f"(max={self._max_confidence:.0%})"
        )

    def _decay_confidence(self) -> None:
        if self._confidence_threshold > self._min_confidence:
            self._confidence_decay_cycles += 1
            if self._confidence_decay_cycles >= self._confidence_decay_interval:
                self._confidence_threshold = max(
                    self._min_confidence,
                    self._confidence_threshold - self._confidence_step * 0.5
                )
                self._confidence_decay_cycles = 0
                self.logger.info(
                    f"[CONFIDENCE] Decay -> threshold back to {self._confidence_threshold:.0%}"
                )

    def _save_state(self) -> None:
        try:
            state = {
                'confidence_threshold': self._confidence_threshold,
                'win_count': self._win_count,
                'loss_count': self._loss_count,
                'total_closed': self._total_closed,
                'trade_count': self.trade_count,
                'has_open_position': self._has_open_position,
                'sl_tp_map': self._sl_tp_map,
                'tp_order_ids': self._tp_order_ids,
                'sl_order_ids': self._sl_order_ids,
                'saved_at': datetime.now().isoformat(),
            }
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.debug(f"Failed to save state: {e}")

    def _load_state(self) -> None:
        try:
            if not STATE_FILE.exists():
                return
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
            self._confidence_threshold = state.get('confidence_threshold', self._confidence_threshold)
            self._win_count = state.get('win_count', 0)
            self._loss_count = state.get('loss_count', 0)
            self._total_closed = state.get('total_closed', 0)
            self.trade_count = state.get('trade_count', 0)
            self._has_open_position = state.get('has_open_position', {})
            self._sl_tp_map = state.get('sl_tp_map', {})
            self._tp_order_ids = state.get('tp_order_ids', {})
            self._sl_order_ids = state.get('sl_order_ids', {})
            self.logger.info(
                f"State restored: threshold={self._confidence_threshold:.0%}, "
                f"trades={self.trade_count}, positions={list(self._has_open_position.keys())}"
            )
        except Exception as e:
            self.logger.debug(f"Failed to load state: {e}")

    async def startup_cleanup(self) -> None:
        """Clean up all positions and orders on startup for a fresh start."""
        self.logger.info("=== Startup cleanup: closing all positions and cancelling all orders ===")
        for symbol in self.config.symbols:
            try:
                await self.exchange.cancel_all_orders(symbol)
                self.logger.info(f"[CLEANUP] Cancelled all orders for {symbol}")
            except Exception as e:
                self.logger.warning(f"[CLEANUP] Could not cancel all orders for {symbol}: {e}")

            await asyncio.sleep(1)

            try:
                positions = await self.exchange.get_positions()
                for pos in positions:
                    if pos.symbol == symbol and pos.amount and float(pos.amount) > 0:
                        close_side = OrderSide.SELL if pos.side.value == 'buy' else OrderSide.BUY
                        try:
                            close_order = await self.exchange.create_order(
                                symbol=symbol,
                                order_type=OrderType.MARKET,
                                side=close_side,
                                amount=pos.amount,
                                metadata={'reduce_only': True}
                            )
                            self.logger.info(
                                f"[CLEANUP] Closed position {symbol} {pos.side.value} "
                                f"{pos.amount} @ market, order_id={close_order.order_id}"
                            )
                        except Exception as e:
                            self.logger.warning(f"[CLEANUP] Failed to close position {symbol}: {e}")
            except Exception as e:
                self.logger.warning(f"[CLEANUP] Could not check positions for {symbol}: {e}")

            await asyncio.sleep(1)

            try:
                await self.exchange.cancel_all_orders(symbol)
                self.logger.info(f"[CLEANUP] Final cancel all orders for {symbol}")
            except Exception as e:
                self.logger.warning(f"[CLEANUP] Could not final cancel orders for {symbol}: {e}")

        self._has_open_position.clear()
        self._sl_tp_map.clear()
        self._tp_order_ids.clear()
        self._sl_order_ids.clear()
        self._save_state()
        self.logger.info("=== Startup cleanup complete, starting fresh ===")

    async def check_and_retrain(self) -> None:
        if self._total_closed < 5:
            return

        current_wr = self.win_rate
        self.logger.info(f"[WINRATE] {current_wr:.1%} ({self._win_count}W/{self._loss_count}L/{self._total_closed}total)")

        if current_wr < self._win_rate_threshold:
            self.logger.info(
                f"[EVOLVE] Win rate {current_wr:.1%} < {self._win_rate_threshold:.0%}, triggering model retraining..."
            )
            try:
                from ..strategy.ai_strategy import AIStrategy
                if isinstance(self.strategy, AIStrategy):
                    await self.strategy.retrain_model(self.market_data, self.config.symbols)
                    self.logger.info("[EVOLVE] Model retraining completed")
            except Exception as e:
                self.logger.error(f"[EVOLVE] Model retraining failed: {e}")

    async def _sync_positions_with_exchange(self) -> None:
        """Verify and sync local position state with exchange reality."""
        if self.is_paper:
            return

        try:
            positions = await self.exchange.get_positions()
            exchange_symbols = set()
            for pos in positions:
                if pos.amount and float(pos.amount) > 0:
                    exchange_symbols.add(pos.symbol)

            local_symbols = set(
                sym for sym, held in self._has_open_position.items() if held
            )

            missing_on_exchange = local_symbols - exchange_symbols
            extra_on_exchange = exchange_symbols - local_symbols

            if missing_on_exchange:
                self.logger.warning(
                    f"[SYNC] Local positions {missing_on_exchange} not found on exchange, clearing local state"
                )
                for sym in missing_on_exchange:
                    self._clear_position_state(sym)

            if extra_on_exchange:
                self.logger.warning(
                    f"[SYNC] Exchange has positions {extra_on_exchange} not tracked locally, "
                    f"closing them for consistency"
                )
                for sym in extra_on_exchange:
                    try:
                        await self.exchange.cancel_all_orders(sym)
                        for pos in positions:
                            if pos.symbol == sym and pos.amount and float(pos.amount) > 0:
                                close_side = OrderSide.SELL if pos.side.value == 'buy' else OrderSide.BUY
                                await self.exchange.create_order(
                                    symbol=sym,
                                    order_type=OrderType.MARKET,
                                    side=close_side,
                                    amount=pos.amount,
                                    metadata={'reduce_only': True}
                                )
                                self.logger.info(f"[SYNC] Closed untracked position {sym}")
                    except Exception as e:
                        self.logger.warning(f"[SYNC] Failed to close untracked position {sym}: {e}")

            if not missing_on_exchange and not extra_on_exchange:
                self.logger.info("[SYNC] Position state consistent with exchange")

            self._save_state()
        except Exception as e:
            self.log_exception("Failed to sync positions with exchange", e)

    async def run(self) -> None:
        self.is_running = True
        self._stop_event = asyncio.Event()
        self.logger.info("Trading engine started")

        try:
            await self._initialize_balance()

            if not self.is_paper:
                await self.startup_cleanup()
            else:
                await self._sync_positions_with_exchange()

            balance = await self.exchange.get_balance()
            usdt = balance.get('USDT', Decimal('0'))
            self.logger.info(f"Initial USDT Balance: {usdt:.2f}")

            cycle = 0
            last_retrain_cycle = 0
            consecutive_errors = 0
            max_consecutive_errors = 10

            while self.is_running and not self._stop_event.is_set():
                try:
                    cycle += 1
                    self.logger.info(f"[Cycle {cycle}] Starting trading cycle...")
                    self._decay_confidence()
                    await self._trading_cycle()

                    consecutive_errors = 0

                    if self._total_closed > 0 and self._total_closed % 5 == 0 and cycle - last_retrain_cycle >= 10:
                        await self.check_and_retrain()
                        last_retrain_cycle = cycle

                    try:
                        balance = await self.exchange.get_balance()
                        usdt = balance.get('USDT', Decimal('0'))
                        self.risk_manager.update_balance(balance)
                        pos_count = len(self._sl_tp_map)
                        wr = self.win_rate
                        ct = self._confidence_threshold
                        self.logger.info(
                            f"[Cycle {cycle}] Balance: {usdt:.2f} USDT | "
                            f"Trades: {self.trade_count} | Positions: {pos_count} | "
                            f"WinRate: {wr:.1%} | Confidence: {ct:.0%}"
                        )
                    except Exception as e:
                        self.logger.error(f"[Cycle {cycle}] Balance check error: {e}")

                    self._save_state()

                    interval = self.config.data.update_interval
                    self.logger.info(f"[Cycle {cycle}] Completed, sleeping {interval}s...")
                    for _ in range(interval):
                        if not self.is_running:
                            break
                        await asyncio.sleep(1)

                except asyncio.CancelledError:
                    self.logger.info("Trading loop cancelled")
                    break
                except Exception as e:
                    consecutive_errors += 1
                    self.log_exception(f"Error in trading cycle (consecutive: {consecutive_errors})", e)

                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.critical(
                            f"[CIRCUIT BREAKER] {consecutive_errors} consecutive errors, "
                            f"pausing trading for 5 minutes..."
                        )
                        await asyncio.sleep(300)
                        consecutive_errors = 0
                    else:
                        backoff = min(10 * consecutive_errors, 60)
                        self.logger.info(f"Backing off for {backoff}s before retry...")
                        await asyncio.sleep(backoff)
        finally:
            self.is_running = False
            self._save_state()
            self.logger.info("Trading engine stopped")

    def stop(self) -> None:
        self.is_running = False
        if self._stop_event is not None:
            self._stop_event.set()
        self.strategy.stop()
        self._save_state()
        self.logger.info("Trading engine stopping...")

    async def _initialize_balance(self) -> None:
        try:
            balance = await self.exchange.get_balance()
            self.risk_manager.update_balance(balance)
            if self.is_paper and hasattr(self.exchange, 'initial_capital'):
                self.logger.info(f"Initial capital: {self.exchange.initial_capital:.2f}")
            self.logger.info(f"Balance initialized: {dict(balance)}")
        except Exception as e:
            self.log_exception("Failed to initialize balance", e)

    async def _trading_cycle(self) -> None:
        symbols = self.config.symbols

        await self._check_tp_order_status()

        await self._check_positions_status()

        signals = await self.strategy.analyze_multiple(self.market_data, symbols)

        await self._update_market_prices(signals)

        for symbol, signal in signals.items():
            self.signal_count += 1
            await self._process_signal(signal)

        await self._update_positions()

        self._save_state()

        if self.is_paper and self.trade_count % 5 == 0:
            summary = self.exchange.get_portfolio_summary()
            self.logger.info(
                f"[PAPER] Portfolio: equity={summary['equity']:.2f}, "
                f"PnL={summary['total_pnl']:+.2f} ({summary['total_pnl_pct']:+.2f}%), "
                f"trades={summary['trades']}"
            )

    async def _check_tp_order_status(self) -> None:
        if self.is_paper:
            return

        filled_symbols = []
        for symbol, order_id in list(self._tp_order_ids.items()):
            try:
                order = await self.exchange.get_order(order_id, symbol)
                if order and order.status and order.status.value in ('closed', 'filled'):
                    self.logger.info(
                        f"[TP FILLED] Take profit order {order_id} filled for {symbol}"
                    )
                    filled_symbols.append(symbol)
                    self._on_position_closed(is_win=True)
            except Exception as e:
                self.logger.debug(f"Could not check TP order {order_id}: {e}")

        for symbol in filled_symbols:
            try:
                await self.exchange.cancel_all_orders(symbol)
                self.logger.info(f"Cancelled all remaining orders for {symbol} after TP fill")
            except Exception:
                pass
            self._clear_position_state(symbol)

    async def _check_positions_status(self) -> None:
        """Check if positions still exist on exchange. Detect SL/TP fills via position disappearance."""
        try:
            positions = await self.exchange.get_positions()
            position_symbols = set()
            for position in positions:
                position_symbols.add(position.symbol)

            for symbol in list(self._has_open_position.keys()):
                if self._has_open_position.get(symbol) and symbol not in position_symbols:
                    tp_order_id = self._tp_order_ids.get(symbol)
                    is_win = False
                    if tp_order_id:
                        try:
                            tp_order = await self.exchange.get_order(tp_order_id, symbol)
                            if tp_order and tp_order.status and tp_order.status.value in ('closed', 'filled'):
                                is_win = True
                        except Exception:
                            pass
                    try:
                        await self.exchange.cancel_all_orders(symbol)
                        self.logger.info(f"Cancelled all orders for {symbol} after position gone")
                    except Exception:
                        pass
                    self.logger.info(
                        f"[POSITION GONE] Position for {symbol} no longer exists on exchange, "
                        f"is_win={is_win}, clearing state"
                    )
                    self._on_position_closed(is_win=is_win)
                    self._clear_position_state(symbol)
                elif self._has_open_position.get(symbol) and symbol in position_symbols:
                    self.logger.info(
                        f"[POS HELD] {symbol} still held on exchange (position_symbols={position_symbols})"
                    )
        except Exception as e:
            self.log_exception("Failed to check positions status", e)

    def _on_position_closed(self, is_win: bool) -> None:
        self._total_closed += 1
        if is_win:
            self._win_count += 1
            self._adjust_confidence_on_win()
        else:
            self._loss_count += 1
            self._adjust_confidence_on_loss()

        self._win_rate = self.win_rate
        self.logger.info(
            f"[STATS] Win rate: {self._win_rate:.1%} "
            f"({self._win_count}W/{self._loss_count}L) | "
            f"Confidence threshold: {self._confidence_threshold:.0%}"
        )

    def _clear_position_state(self, symbol: str) -> None:
        self._sl_tp_map.pop(symbol, None)
        self._has_open_position.pop(symbol, None)
        self._tp_order_ids.pop(symbol, None)
        self._sl_order_ids.pop(symbol, None)

    async def _update_market_prices(self, signals: Dict[str, Signal]) -> None:
        if not self.is_paper:
            return

        prices = {}
        for symbol, signal in signals.items():
            if signal.price > 0:
                prices[symbol] = Decimal(str(signal.price))

        if prices:
            await self.exchange.update_market_prices(prices)

    async def _process_signal(self, signal: Signal) -> None:
        if signal.signal_type == SignalType.HOLD:
            return

        if self._has_open_position.get(signal.symbol, False):
            self.logger.info(
                f"Signal skipped: already holding position for {signal.symbol}, waiting for SL/TP"
            )
            return

        if signal.confidence < self._confidence_threshold:
            self.logger.info(
                f"Signal skipped: confidence {signal.confidence:.2%} < threshold {self._confidence_threshold:.0%} "
                f"for {signal.symbol}"
            )
            return

        side = (
            OrderSide.BUY
            if signal.signal_type == SignalType.BUY
            else OrderSide.SELL
        )

        balance = await self.exchange.get_balance()
        self.risk_manager.update_balance(balance)
        self.logger.info(
            f"[BALANCE] Available: {balance.get('USDT', 0):.2f} USDT"
        )

        self.logger.info(
            f"[FUTURES] Signal: {signal.signal_type.value} {signal.symbol} "
            f"confidence={signal.confidence:.2%} -> {'OPEN LONG' if side == OrderSide.BUY else 'OPEN SHORT'}"
        )

        entry_price = Decimal(str(signal.price))
        stop_loss_price = self.risk_manager.calculate_stop_loss(
            signal.symbol, entry_price, side, Decimal('0.02')
        )
        take_profit_price = self.risk_manager.calculate_take_profit(
            signal.symbol, entry_price, side, Decimal('1.0')
        )

        leverage = self.config.exchange.leverage

        position_size = self.risk_manager.calculate_position_size(
            signal.symbol, entry_price, stop_loss_price, signal.confidence, leverage
        )

        if position_size <= Decimal('0'):
            self.logger.warning(f"Position size is 0 for {signal.symbol}, skipping")
            return

        margin = position_size * entry_price / Decimal(str(leverage))
        self.logger.info(
            f"[FUTURES] Order: {side.value} {position_size:.6f} {signal.symbol} "
            f"@ {entry_price:.2f} | margin={margin:.2f} USDT, {leverage}x | "
            f"SL={stop_loss_price:.2f}, TP={take_profit_price:.2f}"
        )

        order = Order(
            order_id="pending",
            symbol=signal.symbol,
            order_type=OrderType.MARKET,
            side=side,
            amount=position_size,
            price=entry_price,
            status=None,
            timestamp=datetime.now(),
            metadata={
                'confidence': signal.confidence,
                'stop_loss': float(stop_loss_price),
                'take_profit': float(take_profit_price),
                'signal_type': signal.signal_type.value,
                'leverage': leverage
            }
        )

        allowed, reason = self.risk_manager.check_order_risk(order)
        if not allowed:
            self.logger.warning(f"Order blocked by risk manager: {reason}")
            return

        await self._execute_order(order)

    async def _execute_order(self, order: Order) -> None:
        try:
            executed = await self.exchange.create_order(
                symbol=order.symbol,
                order_type=order.order_type if hasattr(order, 'order_type') else OrderType.MARKET,
                side=order.side,
                amount=order.amount,
                price=order.price,
                metadata=order.metadata
            )

            self.trade_count += 1
            self.last_trade_time = datetime.now()

            mode_str = "PAPER" if self.is_paper else "LIVE"
            self.logger.info(
                f"[{mode_str}] Order executed: {order.side.value} "
                f"{order.amount:.6f} {order.symbol} @ {order.price:.2f}"
            )

            if order.metadata and order.metadata.get('stop_loss') and order.metadata.get('take_profit'):
                self._sl_tp_map[order.symbol] = {
                    'stop_loss': order.metadata['stop_loss'],
                    'take_profit': order.metadata['take_profit'],
                    'position_side': 'long' if order.side == OrderSide.BUY else 'short'
                }
                self._has_open_position[order.symbol] = True

                if not self.is_paper and order.metadata.get('take_profit'):
                    await self._place_tp_limit_order(order)

                if not self.is_paper and order.metadata.get('stop_loss'):
                    await self._place_sl_stop_order(order)

            balance = await self.exchange.get_balance()
            self.risk_manager.update_balance(balance)
            self.risk_manager.update_positions(await self.exchange.get_positions())
            self.logger.info(
                f"[BALANCE] After trade: {balance.get('USDT', 0):.2f} USDT"
            )

            self._save_state()

        except ValueError as e:
            self.logger.warning(f"Order rejected: {e}")
        except Exception as e:
            self.log_exception(f"Order execution failed for {order.symbol}", e)

    async def _place_tp_limit_order(self, order: Order) -> None:
        try:
            tp_price = Decimal(str(order.metadata['take_profit']))
            is_long = order.side == OrderSide.BUY
            tp_side = OrderSide.SELL if is_long else OrderSide.BUY

            tp_order = None
            for attempt in range(3):
                try:
                    tp_order = await self.exchange.create_order(
                        symbol=order.symbol,
                        order_type=OrderType.LIMIT,
                        side=tp_side,
                        amount=order.amount,
                        price=tp_price,
                        metadata={'reduce_only': True, 'is_tp_order': True}
                    )
                    break
                except Exception as e:
                    self.logger.warning(f"TP order attempt {attempt+1}/3 failed for {order.symbol}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(3)

            if tp_order and tp_order.order_id:
                self._tp_order_ids[order.symbol] = tp_order.order_id
                self.logger.info(
                    f"[TP ORDER] Placed TP limit order: {tp_side.value} "
                    f"{order.amount:.6f} {order.symbol} @ {tp_price:.2f} "
                    f"(order_id={tp_order.order_id})"
                )
        except Exception as e:
            self.logger.warning(f"Failed to place TP limit order for {order.symbol}: {e}")

    async def _place_sl_stop_order(self, order: Order) -> None:
        try:
            sl_price = Decimal(str(order.metadata['stop_loss']))
            is_long = order.side == OrderSide.BUY
            sl_side = OrderSide.SELL if is_long else OrderSide.BUY

            sl_order = None
            for attempt in range(3):
                try:
                    sl_order = await self.exchange.create_stop_order(
                        symbol=order.symbol,
                        side=sl_side,
                        amount=order.amount,
                        stop_price=sl_price,
                    )
                    break
                except Exception as e:
                    self.logger.warning(f"SL order attempt {attempt+1}/3 failed for {order.symbol}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(3)

            if sl_order and sl_order.order_id:
                self._sl_order_ids[order.symbol] = sl_order.order_id
                self.logger.info(
                    f"[SL ORDER] Placed SL stop order: {sl_side.value} "
                    f"{order.amount:.6f} {order.symbol} @ stop {sl_price:.2f} "
                    f"(order_id={sl_order.order_id})"
                )
        except Exception as e:
            self.logger.warning(f"Failed to place SL stop order for {order.symbol}: {e}")

    async def _update_positions(self) -> None:
        try:
            positions = await self.exchange.get_positions()
            self.risk_manager.update_positions(positions)

            if self.is_paper:
                for position in positions:
                    if position.symbol in self._sl_tp_map:
                        sl_tp = self._sl_tp_map[position.symbol]
                        if position.metadata is None:
                            position.metadata = {}
                        position.metadata.setdefault('stop_loss', sl_tp.get('stop_loss'))
                        position.metadata.setdefault('take_profit', sl_tp.get('take_profit'))
                        position.metadata.setdefault('position_side', sl_tp.get('position_side'))
                    await self._check_stop_loss_take_profit(position)
        except Exception as e:
            self.log_exception("Failed to update positions", e)

    async def _check_stop_loss_take_profit(self, position) -> None:
        if not self.is_paper:
            return

        if not hasattr(position, 'metadata') or not position.metadata:
            return

        stop_loss = position.metadata.get('stop_loss')
        take_profit = position.metadata.get('take_profit')

        if not stop_loss and not take_profit:
            return

        pos_side = position.metadata.get('position_side', '')
        is_long = pos_side == 'long'

        current_price = position.current_price
        should_close = False
        reason = ""

        if is_long:
            if stop_loss and current_price <= Decimal(str(stop_loss)):
                should_close = True
                reason = "Stop loss triggered"
            elif take_profit and current_price >= Decimal(str(take_profit)):
                should_close = True
                reason = "Take profit triggered"
        else:
            if stop_loss and current_price >= Decimal(str(stop_loss)):
                should_close = True
                reason = "Stop loss triggered"
            elif take_profit and current_price <= Decimal(str(take_profit)):
                should_close = True
                reason = "Take profit triggered"

        if should_close:
            close_side = OrderSide.SELL if is_long else OrderSide.BUY
            self.logger.info(
                f"[SL/TP] {reason} for {position.symbol} @ {current_price:.2f}"
            )
            try:
                await self.exchange.create_order(
                    symbol=position.symbol,
                    order_type=OrderType.MARKET,
                    side=close_side,
                    amount=position.amount,
                    price=current_price,
                    metadata={'reduce_only': True}
                )
                self.trade_count += 1
                self.last_trade_time = datetime.now()

                is_win = "Take profit" in reason
                self._on_position_closed(is_win=is_win)
            except Exception as e:
                self.log_exception(f"Failed to close position for {position.symbol}", e)
            finally:
                self._clear_position_state(position.symbol)

                try:
                    balance = await self.exchange.get_balance()
                    self.risk_manager.update_balance(balance)
                    self.risk_manager.update_positions(await self.exchange.get_positions())
                    self.logger.info(
                        f"[BALANCE] After close: {balance.get('USDT', 0):.2f} USDT"
                    )
                except Exception as e:
                    self.logger.warning(f"Balance update after close failed: {e}")

    async def refresh_prices(self) -> None:
        if self.is_paper:
            try:
                prices = {}
                for symbol in self.config.symbols:
                    ticker = await self.market_data.data_feed.fetch_ticker(symbol)
                    if ticker and ticker.get('last'):
                        prices[symbol] = Decimal(str(ticker['last']))
                if prices:
                    await self.exchange.update_market_prices(prices)
            except Exception as e:
                self.log_exception("Failed to refresh prices", e)

    def get_status(self) -> Dict[str, Any]:
        status = {
            'is_running': self.is_running,
            'mode': 'PAPER' if self.is_paper else 'LIVE',
            'trade_count': self.trade_count,
            'signal_count': self.signal_count,
            'last_trade_time': (
                self.last_trade_time.isoformat() if self.last_trade_time else None
            ),
            'risk_level': self.risk_manager.get_risk_level().value,
            'confidence_threshold': self._confidence_threshold,
            'win_rate': self.win_rate,
            'win_count': self._win_count,
            'loss_count': self._loss_count,
            'total_closed': self._total_closed,
            'has_position': self.has_position,
            'open_symbols': list(self._has_open_position.keys()),
        }

        if self.is_paper:
            status['portfolio'] = self.exchange.get_portfolio_summary()
        else:
            try:
                portfolio_value = self.risk_manager._get_portfolio_value()
                status['portfolio'] = {
                    'equity': float(portfolio_value),
                    'total_pnl': 0.0,
                    'total_pnl_pct': 0.0,
                    'position_count': len(self._sl_tp_map),
                    'trades': self.trade_count
                }
            except Exception as e:
                self.logger.debug(f"Failed to get portfolio value: {e}")
                status['portfolio'] = {
                    'equity': 0.0,
                    'total_pnl': 0.0,
                    'total_pnl_pct': 0.0,
                    'position_count': len(self._sl_tp_map),
                    'trades': self.trade_count
                }

        return status

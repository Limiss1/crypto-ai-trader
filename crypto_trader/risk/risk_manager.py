"""
Risk management module.
Implements risk controls, position sizing, and portfolio management.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
from enum import Enum

import numpy as np
import pandas as pd

from ..infra.config import get_config
from ..infra.logger import LogMixin
from ..execution.exchange import Order, Position, OrderSide


class RiskLevel(Enum):
    """Risk levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    EXTREME = "extreme"


class RiskMetrics:
    """Risk metrics for portfolio."""
    
    def __init__(self):
        """Initialize risk metrics."""
        self.total_value: Decimal = Decimal('0')
        self.cash_value: Decimal = Decimal('0')
        self.position_value: Decimal = Decimal('0')
        self.unrealized_pnl: Decimal = Decimal('0')
        self.realized_pnl: Decimal = Decimal('0')
        
        # Risk metrics
        self.daily_pnl: Decimal = Decimal('0')
        self.daily_return: Decimal = Decimal('0')
        self.max_drawdown: Decimal = Decimal('0')
        self.sharpe_ratio: Decimal = Decimal('0')
        self.sortino_ratio: Decimal = Decimal('0')
        self.volatility: Decimal = Decimal('0')
        
        # Position metrics
        self.position_count: int = 0
        self.avg_position_size: Decimal = Decimal('0')
        self.max_position_size: Decimal = Decimal('0')
        
        # Timestamp
        self.timestamp: datetime = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            'total_value': float(self.total_value),
            'cash_value': float(self.cash_value),
            'position_value': float(self.position_value),
            'unrealized_pnl': float(self.unrealized_pnl),
            'realized_pnl': float(self.realized_pnl),
            'daily_pnl': float(self.daily_pnl),
            'daily_return': float(self.daily_return),
            'max_drawdown': float(self.max_drawdown),
            'sharpe_ratio': float(self.sharpe_ratio),
            'sortino_ratio': float(self.sortino_ratio),
            'volatility': float(self.volatility),
            'position_count': self.position_count,
            'avg_position_size': float(self.avg_position_size),
            'max_position_size': float(self.max_position_size),
            'timestamp': self.timestamp.isoformat()
        }


class RiskManager(LogMixin):
    """Risk manager for trading operations."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize risk manager.
        
        Args:
            config: Risk configuration
        """
        super().__init__()
        
        # Load configuration
        if config is None:
            config = get_config().risk
        
        self.config = config
        
        # Risk limits
        self.max_drawdown = Decimal(str(config.max_drawdown))
        self.daily_loss_limit = Decimal(str(config.daily_loss_limit))
        self.max_open_positions = config.max_open_positions
        self.stop_loss_pct = Decimal(str(config.stop_loss_pct))
        self.take_profit_pct = Decimal(str(config.take_profit_pct))
        
        # Trading limits
        self.max_position_size = Decimal('0.1')  # Default 10% per position
        self.max_portfolio_risk = Decimal('0.02')  # 2% max risk per trade
        
        # Performance tracking
        self.initial_capital: Decimal = Decimal('0')
        self.daily_pnl_history: List[Decimal] = []
        self.position_history: List[Dict[str, Any]] = []
        self.risk_level: RiskLevel = RiskLevel.MEDIUM
        
        # Current state
        self.current_positions: List[Position] = []
        self.open_orders: List[Order] = []
        self.account_balance: Dict[str, Decimal] = {}
        
        # Risk metrics
        self.metrics = RiskMetrics()
        self.metrics_history: List[RiskMetrics] = []
        
        self.logger.info(f"Risk manager initialized with max drawdown: {self.max_drawdown}")
    
    def check_order_risk(self, order: Order) -> Tuple[bool, str]:
        """
        Check if order passes risk controls.
        
        Args:
            order: Order to check
        
        Returns:
            Tuple of (allowed, reason)
        """
        position_value = self._calculate_position_value(order)
        portfolio_value = self._get_portfolio_value()
        
        if portfolio_value > 0:
            leverage = Decimal(str(order.metadata.get('leverage', 1))) if order.metadata else Decimal('1')
            margin_required = position_value / leverage
            position_pct = margin_required / portfolio_value
            
            if position_pct > self.max_position_size:
                return False, f"Position size {position_pct:.2%} exceeds limit {self.max_position_size:.2%}"
        
        # Check open positions limit
        if len(self.current_positions) >= self.max_open_positions:
            return False, f"Maximum open positions ({self.max_open_positions}) reached"
        
        # Check daily loss limit
        daily_pnl = self._calculate_daily_pnl()
        if daily_pnl < -self.daily_loss_limit * portfolio_value:
            return False, f"Daily loss limit reached: {daily_pnl:.2f}"
        
        # Check max drawdown
        current_drawdown = self._calculate_drawdown()
        if current_drawdown > self.max_drawdown:
            return False, f"Max drawdown {current_drawdown:.2%} exceeded limit {self.max_drawdown:.2%}"
        
        return True, "Risk check passed"
    
    def calculate_position_size(
        self, 
        symbol: str, 
        entry_price: Decimal,
        stop_loss_price: Decimal,
        confidence: float,
        leverage: int = 10
    ) -> Decimal:
        """
        Calculate position size for USDT-margined futures.
        
        Uses fixed margin percentage (max_position_size) of available USDT balance.
        e.g. 100 USDT available, 10% margin = 10 USDT, 10x leverage = 100 USDT notional
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            stop_loss_price: Stop loss price
            confidence: Signal confidence [0, 1]
            leverage: Leverage multiplier
        
        Returns:
            Position size in base currency (contracts)
        """
        available = self.account_balance.get('USDT', Decimal('0'))
        
        if available <= 0:
            return Decimal('0')
        
        margin = available * self.max_position_size
        notional_value = margin * Decimal(str(leverage))
        position_size = notional_value / entry_price
        
        self.logger.info(
            f"[POSITION] Available: {available:.2f} USDT | "
            f"Margin: {margin:.2f} USDT ({float(self.max_position_size)*100:.0f}%) | "
            f"Notional: {notional_value:.2f} USDT ({leverage}x) | "
            f"Size: {position_size:.8f}"
        )
        
        return position_size
    
    def calculate_stop_loss(
        self,
        symbol: str,
        entry_price: Decimal,
        side: OrderSide,
        volatility: Decimal
    ) -> Decimal:
        stop_loss_pct = self.stop_loss_pct

        if side == OrderSide.BUY:
            stop_loss_price = entry_price * (Decimal('1') - stop_loss_pct)
        else:
            stop_loss_price = entry_price * (Decimal('1') + stop_loss_pct)

        return stop_loss_price
    
    def calculate_take_profit(
        self,
        symbol: str,
        entry_price: Decimal,
        side: OrderSide,
        risk_reward_ratio: Decimal = Decimal('1.0')
    ) -> Decimal:
        take_profit_pct = self.take_profit_pct

        if side == OrderSide.BUY:
            take_profit_price = entry_price * (Decimal('1') + take_profit_pct)
        else:
            take_profit_price = entry_price * (Decimal('1') - take_profit_pct)

        return take_profit_price
    
    def update_positions(self, positions: List[Position]) -> None:
        """Update current positions."""
        self.current_positions = positions
        self._update_metrics()
    
    def update_orders(self, orders: List[Order]) -> None:
        """Update open orders."""
        self.open_orders = orders
    
    def update_balance(self, balance: Dict[str, Decimal]) -> None:
        """Update account balance."""
        self.account_balance = balance
        
        # Track initial capital
        if self.initial_capital == 0:
            usdt_balance = balance.get('USDT', Decimal('0'))
            if usdt_balance > 0:
                self.initial_capital = float(usdt_balance)
                self.logger.info(f"Initial capital: {self.initial_capital:.2f}")
    
    def get_risk_level(self) -> RiskLevel:
        """Get current risk level."""
        return self.risk_level
    
    def should_reduce_risk(self) -> bool:
        """Determine if risk should be reduced."""
        current_drawdown = self._calculate_drawdown()
        daily_pnl = self._calculate_daily_pnl()
        
        portfolio_value = self._get_portfolio_value()
        
        # Check if we should reduce risk
        if (current_drawdown > self.max_drawdown * Decimal('0.8') or
            daily_pnl < -self.daily_loss_limit * portfolio_value * Decimal('0.7')):
            return True
        
        return False
    
    def get_trading_suggestion(self) -> Dict[str, Any]:
        """Get trading suggestions based on risk."""
        suggestion = {
            'action': 'normal',
            'position_sizing_multiplier': 1.0,
            'risk_level': self.risk_level.value,
            'reason': ''
        }
        
        if self.should_reduce_risk():
            suggestion['action'] = 'reduce'
            suggestion['position_sizing_multiplier'] = 0.5
            suggestion['reason'] = 'Risk levels elevated'
            self.risk_level = RiskLevel.HIGH
        else:
            self.risk_level = RiskLevel.MEDIUM
        
        return suggestion
    
    def _calculate_position_value(self, order: Order) -> Decimal:
        """Calculate position value for an order."""
        if order.price:
            return order.amount * order.price
        else:
            # For market orders, use current price (estimate)
            return order.amount * Decimal('1.0')  # Placeholder
    
    def _get_portfolio_value(self) -> Decimal:
        """Calculate total portfolio value (equity = available + margin + unrealized PnL)."""
        available = self.account_balance.get('USDT', Decimal('0'))
        margin_used = sum(
            (Decimal(str(p.metadata.get('margin', 0))) if p.metadata else Decimal('0')
             for p in self.current_positions),
            Decimal('0')
        )
        unrealized = sum(
            (p.unrealized_pnl for p in self.current_positions),
            Decimal('0')
        )
        total = available + margin_used + unrealized
        self.logger.debug(
            f"[EQUITY] Available: {available:.2f} + Margin: {margin_used:.2f} + "
            f"Unrealized: {unrealized:.4f} = {total:.2f} USDT"
        )
        return total
    
    def _calculate_daily_pnl(self) -> Decimal:
        """Calculate daily P&L."""
        if not self.daily_pnl_history:
            return Decimal('0')
        
        # Sum of today's P&L
        today = datetime.now().date()
        daily_sum = Decimal('0')
        
        for pnl in self.daily_pnl_history:
            # In a real implementation, we would filter by date
            daily_sum += pnl
        
        return daily_sum
    
    def _calculate_drawdown(self) -> Decimal:
        """Calculate current drawdown."""
        if self.initial_capital == 0:
            return Decimal('0')
        
        current_value = self._get_portfolio_value()
        initial = Decimal(str(self.initial_capital))
        
        if current_value >= initial:
            return Decimal('0')
        
        drawdown = (initial - current_value) / initial
        return drawdown
    
    def _update_metrics(self) -> None:
        """Update risk metrics."""
        self.metrics = RiskMetrics()
        
        portfolio_value = self._get_portfolio_value()
        self.metrics.total_value = portfolio_value
        self.metrics.cash_value = self.account_balance.get('USDT', Decimal('0'))
        self.metrics.position_value = portfolio_value - self.metrics.cash_value
        
        # Calculate P&L from positions
        unrealized_pnl = Decimal('0')
        for position in self.current_positions:
            unrealized_pnl += position.unrealized_pnl
        
        self.metrics.unrealized_pnl = unrealized_pnl
        
        # Update other metrics
        self.metrics.position_count = len(self.current_positions)
        
        if self.metrics.position_count > 0:
            position_values = [
                pos.amount * pos.current_price for pos in self.current_positions
            ]
            self.metrics.avg_position_size = sum(position_values) / len(position_values)
            self.metrics.max_position_size = max(position_values)
        
        # Store metrics in history
        self.metrics_history.append(self.metrics)
        
        # Keep history manageable
        if len(self.metrics_history) > 1000:
            self.metrics_history = self.metrics_history[-500:]


class PortfolioManager(LogMixin):
    """Portfolio management and allocation."""
    
    def __init__(self, risk_manager: RiskManager):
        """
        Initialize portfolio manager.
        
        Args:
            risk_manager: Risk manager instance
        """
        super().__init__()
        self.risk_manager = risk_manager
        
        # Portfolio allocation
        self.target_allocation: Dict[str, float] = {}
        self.current_allocation: Dict[str, float] = {}
        
        # Rebalancing settings
        self.rebalance_threshold = Decimal('0.05')  # 5% threshold
        self.last_rebalance_time: Optional[datetime] = None
    
    def calculate_allocation(self, symbols: List[str], strategy: str = "equal_risk") -> Dict[str, float]:
        """
        Calculate portfolio allocation.
        
        Args:
            symbols: List of symbols
            strategy: Allocation strategy
        
        Returns:
            Dictionary of symbol to allocation percentage
        """
        if strategy == "equal_risk":
            # Equal risk contribution
            allocation = {symbol: 1.0 / len(symbols) for symbol in symbols}
        elif strategy == "equal_weight":
            # Equal weight
            allocation = {symbol: 1.0 / len(symbols) for symbol in symbols}
        elif strategy == "risk_parity":
            # Risk parity (simplified)
            allocation = {symbol: 1.0 / len(symbols) for symbol in symbols}
        else:
            allocation = {symbol: 1.0 / len(symbols) for symbol in symbols}
        
        self.target_allocation = allocation
        return allocation
    
    def should_rebalance(self) -> bool:
        """Check if portfolio should be rebalanced."""
        if not self.current_allocation or not self.target_allocation:
            return False
        
        # Check if any allocation deviates beyond threshold
        for symbol, target_pct in self.target_allocation.items():
            current_pct = self.current_allocation.get(symbol, 0.0)
            
            if abs(current_pct - target_pct) > float(self.rebalance_threshold):
                return True
        
        # Check time since last rebalance
        if self.last_rebalance_time:
            time_since_rebalance = datetime.now() - self.last_rebalance_time
            if time_since_rebalance > timedelta(days=7):
                return True
        
        return False
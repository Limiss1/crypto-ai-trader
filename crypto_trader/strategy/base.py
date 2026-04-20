"""
Strategy base classes and interfaces.
Defines the contract for all trading strategies.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from enum import Enum

import pandas as pd
import numpy as np

from ..infra.config import get_config
from ..infra.logger import LogMixin
from ..data.market_data import MarketData
from ..execution.exchange import ExchangeInterface


class SignalType(Enum):
    """Trading signal types."""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    CLOSE = "close"


class Signal:
    """Trading signal with metadata."""
    
    def __init__(
        self,
        signal_type: SignalType,
        symbol: str,
        confidence: float,
        price: float,
        timestamp: datetime,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize trading signal.
        
        Args:
            signal_type: Type of signal
            symbol: Trading symbol
            confidence: Signal confidence (0.0 to 1.0)
            price: Current price
            timestamp: Signal timestamp
            metadata: Additional signal metadata
        """
        self.signal_type = signal_type
        self.symbol = symbol
        self.confidence = max(0.0, min(1.0, confidence))  # Clamp to [0, 1]
        self.price = price
        self.timestamp = timestamp
        self.metadata = metadata or {}
    
    def __repr__(self) -> str:
        return (f"Signal({self.signal_type.value}, {self.symbol}, "
                f"confidence={self.confidence:.2%}, price={self.price:.2f})")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert signal to dictionary."""
        return {
            'signal_type': self.signal_type.value,
            'symbol': self.symbol,
            'confidence': self.confidence,
            'price': self.price,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata
        }


class Strategy(ABC, LogMixin):
    """Abstract base class for trading strategies."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize strategy.
        
        Args:
            config: Strategy configuration
        """
        super().__init__()
        self.config = config or {}
        self.name = self.__class__.__name__
        self.is_running = False
        
        # Performance tracking
        self.signals_generated = 0
        self.last_signal_time: Optional[datetime] = None
        self.signal_history: List[Signal] = []
    
    @abstractmethod
    async def analyze(
        self, 
        data: MarketData, 
        symbol: str
    ) -> Signal:
        """
        Analyze market data and generate trading signal.
        
        Args:
            data: Market data instance
            symbol: Trading symbol
        
        Returns:
            Trading signal
        """
        pass
    
    async def analyze_multiple(
        self,
        data: MarketData,
        symbols: List[str]
    ) -> Dict[str, Signal]:
        """
        Analyze multiple symbols concurrently.
        
        Args:
            data: Market data instance
            symbols: List of trading symbols
        
        Returns:
            Dictionary mapping symbols to signals
        """
        tasks = [
            self.analyze(data, symbol)
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        signals = {}
        for symbol, result in zip(symbols, results):
            if isinstance(result, Exception):
                self.logger.error(f"Failed to analyze {symbol}: {result}")
                signals[symbol] = Signal(
                    signal_type=SignalType.HOLD,
                    symbol=symbol,
                    confidence=0.0,
                    price=0.0,
                    timestamp=datetime.now()
                )
            else:
                signals[symbol] = result
        
        return signals
    
    async def run(
        self,
        data: MarketData,
        exchange: ExchangeInterface,
        symbols: Optional[List[str]] = None
    ) -> None:
        """
        Run strategy in continuous loop.
        
        Args:
            data: Market data instance
            exchange: Exchange interface
            symbols: List of symbols to trade (default: from config)
        """
        if symbols is None:
            symbols = get_config().symbols
        
        self.is_running = True
        self.logger.info(f"Starting strategy {self.name} for symbols: {symbols}")
        
        while self.is_running:
            try:
                # Analyze all symbols
                signals = await self.analyze_multiple(data, symbols)
                
                # Process signals
                for symbol, signal in signals.items():
                    self._process_signal(signal, exchange)
                
                # Wait before next analysis
                await asyncio.sleep(60)  # Default: 1 minute
                
            except asyncio.CancelledError:
                self.logger.info(f"Strategy {self.name} cancelled")
                break
            except Exception as e:
                self.log_exception(f"Error in strategy {self.name}", e)
                await asyncio.sleep(10)  # Wait before retry
    
    def _process_signal(self, signal: Signal, exchange: ExchangeInterface) -> None:
        """
        Process trading signal.
        
        Args:
            signal: Trading signal
            exchange: Exchange interface
        """
        self.signals_generated += 1
        self.last_signal_time = signal.timestamp
        self.signal_history.append(signal)
        
        # Keep history manageable
        if len(self.signal_history) > 1000:
            self.signal_history = self.signal_history[-500:]
        
        # Log signal
        self.logger.info(f"Generated signal: {signal}")
        
        # TODO: Execute trades based on signal
        # This would be connected to the execution layer
    
    def stop(self) -> None:
        """Stop strategy execution."""
        self.is_running = False
        self.logger.info(f"Stopped strategy {self.name}")
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get strategy performance metrics."""
        if not self.signal_history:
            return {}
        
        # Calculate basic metrics
        buy_signals = [s for s in self.signal_history if s.signal_type == SignalType.BUY]
        sell_signals = [s for s in self.signal_history if s.signal_type == SignalType.SELL]
        
        avg_buy_confidence = np.mean([s.confidence for s in buy_signals]) if buy_signals else 0.0
        avg_sell_confidence = np.mean([s.confidence for s in sell_signals]) if sell_signals else 0.0
        
        return {
            'name': self.name,
            'signals_generated': self.signals_generated,
            'buy_signals': len(buy_signals),
            'sell_signals': len(sell_signals),
            'avg_buy_confidence': avg_buy_confidence,
            'avg_sell_confidence': avg_sell_confidence,
            'last_signal_time': self.last_signal_time.isoformat() if self.last_signal_time else None
        }


class DummyStrategy(Strategy):
    """Dummy strategy for testing."""
    
    async def analyze(self, data: MarketData, symbol: str) -> Signal:
        """Generate random signals for testing."""
        import random
        
        # Fetch recent data
        df = await data.get_ohlcv(symbol, limit=10)
        
        if len(df) == 0:
            return Signal(
                signal_type=SignalType.HOLD,
                symbol=symbol,
                confidence=0.0,
                price=0.0,
                timestamp=datetime.now()
            )
        
        current_price = df.iloc[-1]['close']
        
        # Generate random signal
        signal_type = random.choice([SignalType.BUY, SignalType.SELL, SignalType.HOLD])
        confidence = random.uniform(0.3, 0.9)
        
        return Signal(
            signal_type=signal_type,
            symbol=symbol,
            confidence=confidence,
            price=current_price,
            timestamp=datetime.now(),
            metadata={'strategy': 'dummy'}
        )
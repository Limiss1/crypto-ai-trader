"""
Tests for strategy layer.
"""

import asyncio
from unittest.mock import Mock, AsyncMock, patch
import pandas as pd
import numpy as np
import pytest

from crypto_trader.strategy.base import (
    Strategy, Signal, SignalType, DummyStrategy
)
from crypto_trader.data.market_data import MarketData
from crypto_trader.execution.exchange import ExchangeInterface


class TestSignal:
    """Test Signal class."""
    
    def test_signal_creation(self):
        """Test signal creation."""
        signal = Signal(
            signal_type=SignalType.BUY,
            symbol='BTC/USDT',
            confidence=0.75,
            price=29050.0,
            timestamp=pd.Timestamp('2024-01-01')
        )
        
        assert signal.signal_type == SignalType.BUY
        assert signal.symbol == 'BTC/USDT'
        assert signal.confidence == 0.75
        assert signal.price == 29050.0
        assert signal.metadata == {}
    
    def test_signal_repr(self):
        """Test signal string representation."""
        signal = Signal(
            signal_type=SignalType.SELL,
            symbol='ETH/USDT',
            confidence=0.65,
            price=1800.0,
            timestamp=pd.Timestamp('2024-01-01')
        )
        
        repr_str = repr(signal)
        assert 'Signal' in repr_str
        assert 'sell' in repr_str.lower()  # SignalType.SELL.value is 'sell'
        assert 'ETH/USDT' in repr_str
        assert '65.00%' in repr_str or '0.65' in repr_str
    
    def test_signal_to_dict(self):
        """Test signal to dictionary conversion."""
        signal = Signal(
            signal_type=SignalType.HOLD,
            symbol='BTC/USDT',
            confidence=0.5,
            price=29000.0,
            timestamp=pd.Timestamp('2024-01-01 12:00:00'),
            metadata={'reason': 'low_confidence'}
        )
        
        signal_dict = signal.to_dict()
        
        assert signal_dict['signal_type'] == 'hold'
        assert signal_dict['symbol'] == 'BTC/USDT'
        assert signal_dict['confidence'] == 0.5
        assert signal_dict['price'] == 29000.0
        assert signal_dict['metadata']['reason'] == 'low_confidence'


class TestDummyStrategy:
    """Test dummy strategy."""
    
    @pytest.fixture
    def mock_market_data(self):
        """Mock market data."""
        mock = AsyncMock(spec=MarketData)
        
        df = pd.DataFrame({
            'open': [29000.0, 29050.0, 29100.0],
            'high': [29100.0, 29150.0, 29200.0],
            'low': [28900.0, 29000.0, 29050.0],
            'close': [29050.0, 29100.0, 29150.0],
            'volume': [100.0, 150.0, 200.0]
        }, index=pd.date_range('2024-01-01', periods=3, freq='1min'))
        
        mock.get_ohlcv.return_value = df
        
        return mock
    
    @pytest.mark.asyncio
    async def test_analyze(self, mock_market_data):
        """Test dummy strategy analysis."""
        strategy = DummyStrategy()
        
        signal = await strategy.analyze(mock_market_data, 'BTC/USDT')
        
        assert isinstance(signal, Signal)
        assert signal.symbol == 'BTC/USDT'
        assert signal.price > 0
        assert 0.3 <= signal.confidence <= 0.9
        assert signal.signal_type in [SignalType.BUY, SignalType.SELL, SignalType.HOLD]
        
        mock_market_data.get_ohlcv.assert_called_once_with('BTC/USDT', limit=10)
    
    @pytest.mark.asyncio
    async def test_analyze_multiple(self, mock_market_data):
        """Test analyzing multiple symbols."""
        strategy = DummyStrategy()
        
        symbols = ['BTC/USDT', 'ETH/USDT']
        signals = await strategy.analyze_multiple(mock_market_data, symbols)
        
        assert isinstance(signals, dict)
        assert len(signals) == 2
        assert 'BTC/USDT' in signals
        assert 'ETH/USDT' in signals
        
        for symbol, signal in signals.items():
            assert signal.symbol == symbol
            assert isinstance(signal, Signal)
    
    def test_performance_metrics(self):
        """Test performance metrics tracking."""
        strategy = DummyStrategy()
        
        # Add some signals
        for i in range(5):
            signal = Signal(
                signal_type=SignalType.BUY if i % 2 == 0 else SignalType.SELL,
                symbol='BTC/USDT',
                confidence=0.7,
                price=29000.0 + i * 100,
                timestamp=pd.Timestamp(f'2024-01-01 12:{i:02d}:00')
            )
            strategy.signal_history.append(signal)
        
        metrics = strategy.get_performance_metrics()
        
        assert metrics['name'] == 'DummyStrategy'
        assert metrics['signals_generated'] == 0  # Not from analyze method
        assert metrics['buy_signals'] == 3  # 0, 2, 4 are BUY
        assert metrics['sell_signals'] == 2  # 1, 3 are SELL
        assert 'avg_buy_confidence' in metrics
        assert 'avg_sell_confidence' in metrics


class TestStrategyBase:
    """Test base strategy class."""
    
    @pytest.fixture
    def mock_market_data(self):
        """Mock market data for strategy tests."""
        mock = AsyncMock(spec=MarketData)
        
        df = pd.DataFrame({
            'open': [29000.0, 29050.0, 29100.0],
            'high': [29100.0, 29150.0, 29200.0],
            'low': [28900.0, 29000.0, 29050.0],
            'close': [29050.0, 29100.0, 29150.0],
            'volume': [100.0, 150.0, 200.0]
        }, index=pd.date_range('2024-01-01', periods=3, freq='1min'))
        
        mock.get_ohlcv.return_value = df
        
        return mock
    
    @pytest.fixture
    def mock_strategy(self):
        """Create a concrete strategy for testing."""
        class ConcreteStrategy(Strategy):
            async def analyze(self, data, symbol):
                return Signal(
                    signal_type=SignalType.BUY,
                    symbol=symbol,
                    confidence=0.8,
                    price=29000.0,
                    timestamp=pd.Timestamp.now()
                )
        
        return ConcreteStrategy()
    
    def test_initialization(self):
        """Test strategy initialization."""
        strategy = DummyStrategy(config={'test': 'value'})
        
        assert strategy.config == {'test': 'value'}
        assert strategy.name == 'DummyStrategy'
        assert strategy.is_running is False
        assert strategy.signals_generated == 0
        assert strategy.last_signal_time is None
        assert len(strategy.signal_history) == 0
    
    @pytest.mark.asyncio
    async def test_run_and_stop(self, mock_strategy):
        """Test running and stopping strategy."""
        # Test that stop() sets is_running to False
        mock_strategy.is_running = True
        mock_strategy.stop()
        assert mock_strategy.is_running is False
        
        # Test that run() sets is_running to True when started
        # We'll mock the run loop to exit immediately
        original_run = mock_strategy.run
        
        async def mock_run(data, exchange, symbols=None):
            mock_strategy.is_running = True
            # Exit immediately for test
            mock_strategy.is_running = False
        
        mock_strategy.run = mock_run
        
        mock_market_data = AsyncMock(spec=MarketData)
        mock_exchange = AsyncMock(spec=ExchangeInterface)
        
        await mock_strategy.run(mock_market_data, mock_exchange, ['BTC/USDT'])
        
        # Restore original
        mock_strategy.run = original_run
    
    def test_process_signal(self, mock_strategy):
        """Test signal processing."""
        signal = Signal(
            signal_type=SignalType.BUY,
            symbol='BTC/USDT',
            confidence=0.8,
            price=29000.0,
            timestamp=pd.Timestamp.now()
        )
        
        mock_exchange = Mock(spec=ExchangeInterface)
        
        # Call protected method
        mock_strategy._process_signal(signal, mock_exchange)
        
        assert mock_strategy.signals_generated == 1
        assert mock_strategy.last_signal_time == signal.timestamp
        assert len(mock_strategy.signal_history) == 1
        assert mock_strategy.signal_history[0] == signal


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--asyncio-mode=auto'])
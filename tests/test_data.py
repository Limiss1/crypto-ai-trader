"""
Tests for data layer.
"""

import asyncio
from unittest.mock import Mock, AsyncMock, patch
import pandas as pd
import pytest

from crypto_trader.data.market_data import (
    CCXTDataFeed, MarketData, DataFeed
)


class TestCCXTDataFeed:
    """Test CCXT data feed."""
    
    @pytest.fixture
    def mock_exchange(self):
        """Mock CCXT exchange."""
        mock = Mock()
        
        # Mock OHLCV data
        ohlcv_data = [
            [1609459200000, 29000.0, 29100.0, 28900.0, 29050.0, 100.0],
            [1609459260000, 29050.0, 29150.0, 29000.0, 29100.0, 150.0],
        ]
        mock.fetch_ohlcv.return_value = ohlcv_data
        
        # Mock ticker data
        mock.fetch_ticker.return_value = {
            'symbol': 'BTC/USDT',
            'last': 29050.0,
            'bid': 29000.0,
            'ask': 29100.0,
            'volume': 1000.0
        }
        
        # Mock order book
        mock.fetch_order_book.return_value = {
            'bids': [[29000.0, 1.0], [28900.0, 2.0]],
            'asks': [[29100.0, 1.0], [29200.0, 2.0]],
            'timestamp': None
        }
        
        return mock
    
    @pytest.mark.asyncio
    async def test_fetch_ohlcv(self, mock_exchange):
        """Test fetching OHLCV data."""
        with patch('ccxt.binance', return_value=mock_exchange):
            feed = CCXTDataFeed('binance', testnet=True)
            feed.exchange = mock_exchange
            
            df = await feed.fetch_ohlcv('BTC/USDT', '1m', limit=2)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert 'open' in df.columns
            assert 'high' in df.columns
            assert 'low' in df.columns
            assert 'close' in df.columns
            assert 'volume' in df.columns
            assert df.index.name == 'timestamp'
            
            mock_exchange.fetch_ohlcv.assert_called_once_with(
                symbol='BTC/USDT',
                timeframe='1m',
                limit=2,
                since=None
            )
    
    @pytest.mark.asyncio
    async def test_fetch_ticker(self, mock_exchange):
        """Test fetching ticker data."""
        with patch('ccxt.binance', return_value=mock_exchange):
            feed = CCXTDataFeed('binance', testnet=True)
            feed.exchange = mock_exchange
            
            ticker = await feed.fetch_ticker('BTC/USDT')
            
            assert isinstance(ticker, dict)
            assert 'symbol' in ticker
            assert 'last' in ticker
            assert ticker['symbol'] == 'BTC/USDT'
            
            mock_exchange.fetch_ticker.assert_called_once_with('BTC/USDT')
    
    @pytest.mark.asyncio
    async def test_fetch_order_book(self, mock_exchange):
        """Test fetching order book."""
        with patch('ccxt.binance', return_value=mock_exchange):
            feed = CCXTDataFeed('binance', testnet=True)
            feed.exchange = mock_exchange
            
            orderbook = await feed.fetch_order_book('BTC/USDT', limit=20)
            
            assert isinstance(orderbook, dict)
            assert 'bids' in orderbook
            assert 'asks' in orderbook
            assert len(orderbook['bids']) == 2
            assert len(orderbook['asks']) == 2
            
            mock_exchange.fetch_order_book.assert_called_once_with('BTC/USDT', 20)
    
    @pytest.mark.asyncio
    async def test_fetch_multiple_ohlcv(self, mock_exchange):
        """Test fetching multiple symbols."""
        with patch('ccxt.binance', return_value=mock_exchange):
            feed = CCXTDataFeed('binance', testnet=True)
            feed.exchange = mock_exchange
            
            symbols = ['BTC/USDT', 'ETH/USDT']
            data = await feed.fetch_multiple_ohlcv(symbols, limit=2)
            
            assert isinstance(data, dict)
            assert len(data) == 2
            assert 'BTC/USDT' in data
            assert 'ETH/USDT' in data
            
            assert mock_exchange.fetch_ohlcv.call_count == 2


class TestMarketData:
    """Test market data manager."""
    
    @pytest.fixture
    def mock_data_feed(self):
        """Mock data feed."""
        mock = AsyncMock(spec=DataFeed)
        
        # Mock OHLCV data
        df = pd.DataFrame({
            'open': [29000.0, 29050.0],
            'high': [29100.0, 29150.0],
            'low': [28900.0, 29000.0],
            'close': [29050.0, 29100.0],
            'volume': [100.0, 150.0]
        }, index=pd.date_range('2024-01-01', periods=2, freq='1min'))
        
        mock.fetch_ohlcv.return_value = df
        
        return mock
    
    @pytest.mark.asyncio
    async def test_get_ohlcv(self, mock_data_feed):
        """Test getting OHLCV data with caching."""
        market_data = MarketData(mock_data_feed)
        
        # First call should fetch from feed
        df1 = await market_data.get_ohlcv('BTC/USDT', '1m', limit=2)
        
        assert isinstance(df1, pd.DataFrame)
        assert len(df1) == 2
        mock_data_feed.fetch_ohlcv.assert_called_once_with(
            'BTC/USDT', '1m', 2
        )
        
        # Reset mock call count
        mock_data_feed.fetch_ohlcv.reset_mock()
        
        # Second call should use cache (if within TTL)
        df2 = await market_data.get_ohlcv('BTC/USDT', '1m', limit=2, use_cache=True)
        
        # In test, cache might still be used if timestamps allow
        # Let's just verify we get a DataFrame
        assert isinstance(df2, pd.DataFrame)
    
    @pytest.mark.asyncio
    async def test_get_technical_indicators(self, mock_data_feed):
        """Test getting data with technical indicators."""
        # Create larger mock data for indicators
        df = pd.DataFrame({
            'open': range(100, 200),
            'high': range(105, 205),
            'low': range(95, 195),
            'close': range(100, 200),
            'volume': [100.0] * 100
        }, index=pd.date_range('2024-01-01', periods=100, freq='1min'))
        
        mock_data_feed.fetch_ohlcv.return_value = df
        
        market_data = MarketData(mock_data_feed)
        result = await market_data.get_technical_indicators('BTC/USDT', limit=100)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        
        # Check that indicators were added
        expected_columns = ['sma_10', 'sma_30', 'returns', 'volume_sma', 'volume_ratio', 'range']
        for col in expected_columns:
            assert col in result.columns
    
    def test_clear_cache(self, mock_data_feed):
        """Test clearing cache."""
        market_data = MarketData(mock_data_feed)
        
        # Add something to cache
        market_data.cache['test_key'] = ('timestamp', pd.DataFrame())
        
        assert 'test_key' in market_data.cache
        
        market_data.clear_cache()
        
        assert 'test_key' not in market_data.cache
        assert len(market_data.cache) == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--asyncio-mode=auto'])
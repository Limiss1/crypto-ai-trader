"""
Market data acquisition module.
Provides interfaces for fetching historical and real-time market data.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod

import ccxt
import pandas as pd
import numpy as np

from ..infra.config import get_config
from ..infra.logger import LogMixin


class DataFeed(ABC, LogMixin):
    """Abstract base class for data feeds."""
    
    @abstractmethod
    async def fetch_ohlcv(
        self, 
        symbol: str, 
        timeframe: str = '1m',
        limit: int = 100,
        since: Optional[int] = None
    ) -> pd.DataFrame:
        """Fetch OHLCV (Open, High, Low, Close, Volume) data."""
        pass
    
    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """Fetch current ticker data."""
        pass
    
    @abstractmethod
    async def fetch_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """Fetch order book data."""
        pass


class CCXTDataFeed(DataFeed):
    """CCXT-based data feed supporting multiple exchanges."""
    
    def __init__(
        self, 
        exchange_id: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: bool = True,
        demo_api: Optional[str] = None,
        proxy: Optional[str] = None
    ):
        """
        Initialize CCXT data feed.
        
        Args:
            exchange_id: CCXT exchange ID (e.g., 'binance', 'coinbase')
            api_key: API key (optional for public endpoints)
            api_secret: API secret (optional for public endpoints)
            testnet: Use testnet/sandbox mode
            demo_api: Demo API base URL (e.g. https://demo-fapi.binance.com)
            proxy: HTTP/HTTPS proxy URL
        """
        super().__init__()
        
        exchange_class = getattr(ccxt, exchange_id)
        
        config = {
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'adjustForTimeDifference': True
            }
        }
        
        if proxy:
            config['proxies'] = {
                'http': proxy,
                'https': proxy,
            }
            config['aiohttp_proxy'] = proxy
        
        self.exchange = exchange_class(config)
        
        if demo_api:
            demo_base = demo_api.rstrip('/')
            self.exchange.urls['api'] = {
                'fapiPublic': f'{demo_base}/fapi/v1',
                'fapiPublicV2': f'{demo_base}/fapi/v2',
                'fapiPublicV3': f'{demo_base}/fapi/v3',
                'fapiPrivate': f'{demo_base}/fapi/v1',
                'fapiPrivateV2': f'{demo_base}/fapi/v2',
                'fapiPrivateV3': f'{demo_base}/fapi/v3',
                'dapiPublic': f'{demo_base}/dapi/v1',
                'dapiPrivate': f'{demo_base}/dapi/v1',
                'dapiPrivateV2': f'{demo_base}/dapi/v2',
                'sapi': f'{demo_base}/sapi/v1',
                'sapiPublic': f'{demo_base}/sapi/v1',
                'sapiPrivate': f'{demo_base}/sapi/v1',
                'sapiPrivateV2': f'{demo_base}/sapi/v2',
                'public': f'{demo_base}/api/v3',
                'private': f'{demo_base}/api/v3',
            }
            self.exchange.urls['test'] = self.exchange.urls['api'].copy()
            self.exchange.sandbox = False
            self.exchange.has['fetchCurrencies'] = False
            self.logger.info(f"Using demo API for data feed: {demo_base}")
        elif testnet:
            try:
                self.exchange.set_sandbox_mode(True)
                self.logger.info(f"Enabled testnet mode for {exchange_id}")
            except AttributeError:
                self.logger.warning(f"Testnet not supported for {exchange_id}")
        
        self.exchange_id = exchange_id
        
    async def fetch_ohlcv(
        self, 
        symbol: str, 
        timeframe: str = '1m',
        limit: int = 100,
        since: Optional[int] = None
    ) -> pd.DataFrame:
        """Fetch OHLCV data."""
        try:
            # CCXT methods are synchronous, run in thread pool
            loop = asyncio.get_event_loop()
            ohlcv = await loop.run_in_executor(
                None,
                lambda: self.exchange.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=limit,
                    since=since
                )
            )
            
            # Convert to DataFrame
            df = pd.DataFrame(
                ohlcv,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            self.log_exception(f"Failed to fetch OHLCV for {symbol}", e)
            raise
    
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """Fetch ticker data."""
        try:
            loop = asyncio.get_event_loop()
            ticker = await loop.run_in_executor(
                None,
                lambda: self.exchange.fetch_ticker(symbol)
            )
            return ticker
        except Exception as e:
            self.log_exception(f"Failed to fetch ticker for {symbol}", e)
            raise
    
    async def fetch_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """Fetch order book data."""
        try:
            loop = asyncio.get_event_loop()
            orderbook = await loop.run_in_executor(
                None,
                lambda: self.exchange.fetch_order_book(symbol, limit)
            )
            return orderbook
        except Exception as e:
            self.log_exception(f"Failed to fetch order book for {symbol}", e)
            raise
    
    async def fetch_multiple_ohlcv(
        self,
        symbols: List[str],
        timeframe: str = '1m',
        limit: int = 100
    ) -> Dict[str, pd.DataFrame]:
        """Fetch OHLCV data for multiple symbols concurrently."""
        tasks = [
            self.fetch_ohlcv(symbol, timeframe, limit)
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        data = {}
        for symbol, result in zip(symbols, results):
            if isinstance(result, Exception):
                self.logger.error(f"Failed to fetch data for {symbol}: {result}")
            else:
                data[symbol] = result
        
        return data


class MarketData:
    """High-level market data interface with caching and preprocessing."""
    
    def __init__(self, data_feed: DataFeed):
        """
        Initialize market data manager.
        
        Args:
            data_feed: Data feed instance
        """
        self.data_feed = data_feed
        self.cache: Dict[str, tuple] = {}
        self.cache_ttl = timedelta(seconds=10)
        
    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = '1m',
        limit: int = 100,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Get OHLCV data with optional caching.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe string
            limit: Number of candles
            use_cache: Use cached data if available and fresh
        
        Returns:
            OHLCV DataFrame
        """
        cache_key = f"{symbol}_{timeframe}_{limit}"
        
        # Check cache
        if use_cache and cache_key in self.cache:
            cached_time, cached_data = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_ttl:
                return cached_data.copy()
        
        # Fetch fresh data
        data = await self.data_feed.fetch_ohlcv(symbol, timeframe, limit)
        
        # Update cache
        self.cache[cache_key] = (datetime.now(), data.copy())
        
        return data
    
    async def get_technical_indicators(
        self,
        symbol: str,
        timeframe: str = '1m',
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get OHLCV data with technical indicators.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe string
            limit: Number of candles
        
        Returns:
            DataFrame with technical indicators
        """
        df = await self.get_ohlcv(symbol, timeframe, limit)
        
        # Add basic technical indicators
        if len(df) > 0:
            # Simple moving averages
            df['sma_10'] = df['close'].rolling(window=10).mean()
            df['sma_30'] = df['close'].rolling(window=30).mean()
            
            # Returns
            df['returns'] = df['close'].pct_change()
            
            # Volume indicators
            df['volume_sma'] = df['volume'].rolling(window=20).mean()
            df['volume_ratio'] = df['volume'] / df['volume_sma']
            
            # Price range
            df['range'] = (df['high'] - df['low']) / df['close']
            
            # Remove NaN values
            df = df.dropna()
        
        return df
    
    def clear_cache(self) -> None:
        """Clear data cache."""
        self.cache.clear()


def create_data_feed_from_config() -> DataFeed:
    """
    Create data feed from application configuration.
    
    Returns:
        Configured DataFeed instance
    """
    config = get_config()
    exchange_config = config.exchange
    
    return CCXTDataFeed(
        exchange_id=exchange_config.name.value,
        api_key=exchange_config.api_key,
        api_secret=exchange_config.api_secret,
        testnet=exchange_config.testnet,
        demo_api=exchange_config.demo_api,
        proxy=exchange_config.proxy
    )
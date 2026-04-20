"""
Free public data feed using direct HTTP requests.
No API key required, no rate limit issues for normal usage.
Supports multiple data sources with automatic fallback.
"""

import asyncio
import logging
import time as time_mod
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import pandas as pd
import aiohttp

from .market_data import DataFeed
from ..infra.logger import LogMixin


SYMBOL_MAP = {
    'BTC/USDT:USDT': 'BTCUSDT',
    'ETH/USDT:USDT': 'ETHUSDT',
    'BNB/USDT:USDT': 'BNBUSDT',
    'SOL/USDT:USDT': 'SOLUSDT',
    'XRP/USDT:USDT': 'XRPUSDT',
    'DOGE/USDT:USDT': 'DOGEUSDT',
    'ADA/USDT:USDT': 'ADAUSDT',
    'AVAX/USDT:USDT': 'AVAXUSDT',
    'DOT/USDT:USDT': 'DOTUSDT',
    'LINK/USDT:USDT': 'LINKUSDT',
    'MATIC/USDT:USDT': 'MATICUSDT',
    'LTC/USDT:USDT': 'LTCUSDT',
    'BTC/USDT': 'BTCUSDT',
    'ETH/USDT': 'ETHUSDT',
    'BNB/USDT': 'BNBUSDT',
    'SOL/USDT': 'SOLUSDT',
    'XRP/USDT': 'XRPUSDT',
}

COINGECKO_MAP = {
    'BTC/USDT:USDT': 'bitcoin',
    'ETH/USDT:USDT': 'ethereum',
    'BNB/USDT:USDT': 'binancecoin',
    'SOL/USDT:USDT': 'solana',
    'XRP/USDT:USDT': 'ripple',
    'DOGE/USDT:USDT': 'dogecoin',
    'ADA/USDT:USDT': 'cardano',
    'AVAX/USDT:USDT': 'avalanche-2',
    'DOT/USDT:USDT': 'polkadot',
    'LINK/USDT:USDT': 'chainlink',
    'MATIC/USDT:USDT': 'matic-network',
    'LTC/USDT:USDT': 'litecoin',
    'BTC/USDT': 'bitcoin',
    'ETH/USDT': 'ethereum',
}

COINCAP_MAP = {
    'BTC/USDT:USDT': 'bitcoin',
    'ETH/USDT:USDT': 'ethereum',
    'BNB/USDT:USDT': 'binance-coin',
    'SOL/USDT:USDT': 'solana',
    'XRP/USDT:USDT': 'xrp',
    'DOGE/USDT:USDT': 'dogecoin',
    'ADA/USDT:USDT': 'cardano',
    'AVAX/USDT:USDT': 'avalanche',
    'DOT/USDT:USDT': 'polkadot',
    'LINK/USDT:USDT': 'chainlink',
    'LTC/USDT:USDT': 'litecoin',
    'BTC/USDT': 'bitcoin',
    'ETH/USDT': 'ethereum',
}

TIMEFRAME_MAP = {
    '1m': '1m',
    '3m': '3m',
    '5m': '5m',
    '15m': '15m',
    '30m': '30m',
    '1h': '1h',
    '2h': '2h',
    '4h': '4h',
    '6h': '6h',
    '8h': '8h',
    '12h': '12h',
    '1d': '1d',
    '3d': '3d',
    '1w': '1w',
    '1M': '1M',
}

BINANCE_FUTURES_ENDPOINTS = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
]
BINANCE_SPOT_ENDPOINTS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]
BYBIT_ENDPOINT = "https://api.bybit.com"
OKX_ENDPOINT = "https://www.okx.com"
COINGECKO_ENDPOINT = "https://api.coingecko.com"
COINCAP_ENDPOINT = "https://api.coincap.io/v2"


class BinancePublicDataFeed(DataFeed, LogMixin):
    """
    Free public data feed with multi-source fallback.
    Tries: Binance Futures -> Binance Spot -> Bybit -> OKX -> CoinGecko -> CoinCap
    No API key needed for any source.
    """

    def __init__(self, proxy: Optional[str] = None, is_futures: bool = True):
        super().__init__()
        self.proxy = proxy
        self.is_futures = is_futures
        self._session: Optional[aiohttp.ClientSession] = None
        self._request_count = 0
        self._last_request_time: float = 0
        self._min_interval: float = 0.15
        self._source_status: Dict[str, bool] = {}
        self.logger.info("BinancePublicDataFeed initialized (multi-source fallback)")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15, connect=5)
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5, ssl=False)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CryptoTrader/1.0"}
            )
        return self._session

    async def _rate_limit(self):
        now = time_mod.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request_time = time_mod.time()
        self._request_count += 1

    def _convert_symbol(self, symbol: str) -> str:
        if symbol in SYMBOL_MAP:
            return SYMBOL_MAP[symbol]
        converted = symbol.replace('/USDT:USDT', 'USDT').replace('/USDT', 'USDT').replace('/', '')
        return converted

    async def _http_get(self, url: str, params: Dict[str, Any], source_name: str = "") -> Any:
        await self._rate_limit()
        session = await self._get_session()
        try:
            async with session.get(url, params=params, proxy=self.proxy) as resp:
                if resp.status == 200:
                    self._source_status[source_name] = True
                    return await resp.json()
                elif resp.status == 429:
                    retry_after = int(resp.headers.get('Retry-After', '3'))
                    self.logger.warning(f"[{source_name}] Rate limited, retry after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    async with session.get(url, params=params, proxy=self.proxy) as retry_resp:
                        return await retry_resp.json()
                else:
                    text = await resp.text()
                    self._source_status[source_name] = False
                    raise Exception(f"HTTP {resp.status}: {text[:200]}")
        except aiohttp.ClientError as e:
            self._source_status[source_name] = False
            raise

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = '1m',
        limit: int = 100,
        since: Optional[int] = None
    ) -> pd.DataFrame:
        last_error = None

        try:
            df = await self._fetch_binance(symbol, timeframe, limit, since)
            if len(df) > 0:
                return df
        except Exception as e:
            last_error = e
            self.logger.debug(f"Binance failed for {symbol} {timeframe}: {e}")

        try:
            df = await self._fetch_bybit(symbol, timeframe, limit, since)
            if len(df) > 0:
                return df
        except Exception as e:
            last_error = e
            self.logger.debug(f"Bybit failed for {symbol} {timeframe}: {e}")

        try:
            df = await self._fetch_okx(symbol, timeframe, limit, since)
            if len(df) > 0:
                return df
        except Exception as e:
            last_error = e
            self.logger.debug(f"OKX failed for {symbol} {timeframe}: {e}")

        try:
            df = await self._fetch_coingecko(symbol, timeframe, limit, since)
            if len(df) > 0:
                return df
        except Exception as e:
            last_error = e
            self.logger.debug(f"CoinGecko failed for {symbol} {timeframe}: {e}")

        try:
            df = await self._fetch_coincap(symbol, timeframe, limit, since)
            if len(df) > 0:
                return df
        except Exception as e:
            last_error = e
            self.logger.debug(f"CoinCap failed for {symbol} {timeframe}: {e}")

        if last_error:
            raise last_error
        raise Exception(f"All data sources failed for {symbol} {timeframe}")

    async def _fetch_binance(self, symbol: str, timeframe: str, limit: int, since: Optional[int]) -> pd.DataFrame:
        binance_symbol = self._convert_symbol(symbol)
        interval = TIMEFRAME_MAP.get(timeframe, timeframe)
        params = {'symbol': binance_symbol, 'interval': interval, 'limit': min(limit, 1500)}
        if since:
            params['startTime'] = since

        for base in BINANCE_FUTURES_ENDPOINTS:
            try:
                data = await self._http_get(f"{base}/fapi/v1/klines", params, "binance_futures")
                if data:
                    return self._parse_klines(data)
            except Exception:
                continue

        for base in BINANCE_SPOT_ENDPOINTS:
            try:
                data = await self._http_get(f"{base}/api/v3/klines", params, "binance_spot")
                if data:
                    return self._parse_klines(data)
            except Exception:
                continue

        raise Exception("All Binance endpoints failed")

    async def _fetch_bybit(self, symbol: str, timeframe: str, limit: int, since: Optional[int]) -> pd.DataFrame:
        bybit_symbol = self._convert_symbol(symbol)
        interval = TIMEFRAME_MAP.get(timeframe, timeframe)
        params = {
            'category': 'linear',
            'symbol': bybit_symbol,
            'interval': interval,
            'limit': min(limit, 1000),
        }
        if since:
            params['start'] = since

        data = await self._http_get(
            f"{BYBIT_ENDPOINT}/v5/market/kline", params, "bybit"
        )
        if data and data.get('retCode') == 0:
            rows = data.get('result', {}).get('list', [])
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].iloc[::-1]
            df.set_index('timestamp', inplace=True)
            return df
        raise Exception("Bybit returned no data")

    async def _fetch_okx(self, symbol: str, timeframe: str, limit: int, since: Optional[int]) -> pd.DataFrame:
        okx_instid = self._convert_symbol(symbol).replace('USDT', '-USDT')
        if not okx_instid.startswith('BTC') and not okx_instid.startswith('ETH'):
            okx_instid = okx_instid.replace('-', '-SWAP') if '-' in okx_instid else okx_instid
        else:
            okx_instid = okx_instid + '-SWAP' if '-SWAP' not in okx_instid else okx_instid

        interval = TIMEFRAME_MAP.get(timeframe, timeframe)
        params = {
            'instId': okx_instid,
            'bar': interval,
            'limit': str(min(limit, 300)),
        }

        data = await self._http_get(
            f"{OKX_ENDPOINT}/api/v5/market/candles", params, "okx"
        )
        if data and data.get('code') == '0':
            rows = data.get('data', [])
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'volCcy', 'volCcyQuote', 'confirm'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].iloc[::-1]
            df.set_index('timestamp', inplace=True)
            return df
        raise Exception("OKX returned no data")

    async def _fetch_coingecko(self, symbol: str, timeframe: str, limit: int, since: Optional[int]) -> pd.DataFrame:
        coin_id = COINGECKO_MAP.get(symbol)
        if not coin_id:
            raise Exception(f"CoinGecko: no mapping for {symbol}")

        days_map = {'1m': 1, '5m': 1, '15m': 1, '30m': 1, '1h': 1, '4h': 1, '1d': 30, '3d': 90, '1w': 365}
        days = days_map.get(timeframe, 1)

        params = {'vs_currency': 'usdt', 'days': str(days)}
        data = await self._http_get(
            f"{COINGECKO_ENDPOINT}/api/v3/coins/{coin_id}/market_chart", params, "coingecko"
        )

        if data and 'prices' in data:
            prices = data['prices']
            volumes = data.get('total_volumes', [])
            vol_map = {int(p[0] / 60000) * 60000: v[1] for p, v in zip(prices, volumes)} if volumes else {}

            records = []
            for ts, price in prices:
                ts_ms = int(ts / 60000) * 60000
                records.append({
                    'timestamp': pd.Timestamp(ts, unit='ms'),
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': vol_map.get(ts_ms, 0),
                })

            if not records:
                return pd.DataFrame()

            df = pd.DataFrame(records)
            df.set_index('timestamp', inplace=True)

            resample_map = {'1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min', '1h': '1h', '4h': '4h', '1d': '1D'}
            rs_rule = resample_map.get(timeframe)
            if rs_rule and len(df) > 1:
                df = df.resample(rs_rule).agg({
                    'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
                }).dropna()

            return df.tail(limit)
        raise Exception("CoinGecko returned no data")

    async def _fetch_coincap(self, symbol: str, timeframe: str, limit: int, since: Optional[int]) -> pd.DataFrame:
        asset_id = COINCAP_MAP.get(symbol)
        if not asset_id:
            raise Exception(f"CoinCap: no mapping for {symbol}")

        now_ms = int(time_mod.time() * 1000)
        interval_map = {'1m': 'm1', '5m': 'm5', '15m': 'm15', '30m': 'm30', '1h': 'h1', '4h': 'h6', '1d': 'd1'}
        ci_interval = interval_map.get(timeframe, 'm5')

        params = {
            'interval': ci_interval,
            'start': str(now_ms - limit * 60000 * 60),
            'end': str(now_ms),
        }

        data = await self._http_get(
            f"{COINCAP_ENDPOINT}/assets/{asset_id}/history", params, "coincap"
        )

        if data and 'data' in data and data['data']:
            records = []
            for item in data['data']:
                records.append({
                    'timestamp': pd.Timestamp(int(item['time']), unit='ms'),
                    'open': float(item['priceUsd']),
                    'high': float(item['priceUsd']),
                    'low': float(item['priceUsd']),
                    'close': float(item['priceUsd']),
                    'volume': 0,
                })

            df = pd.DataFrame(records)
            df.set_index('timestamp', inplace=True)
            return df.tail(limit)
        raise Exception("CoinCap returned no data")

    def _parse_klines(self, data: list) -> pd.DataFrame:
        if not data:
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades',
            'taker_buy_base', 'taker_buy_quote', 'ignore'
        ])

        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        return df

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        binance_symbol = self._convert_symbol(symbol)
        for base in BINANCE_FUTURES_ENDPOINTS:
            try:
                data = await self._http_get(f"{base}/fapi/v1/ticker/24hr", {'symbol': binance_symbol}, "binance_futures")
                if data:
                    return {
                        'symbol': symbol,
                        'last': float(data.get('lastPrice', 0)),
                        'high': float(data.get('highPrice', 0)),
                        'low': float(data.get('lowPrice', 0)),
                        'volume': float(data.get('volume', 0)),
                        'change': float(data.get('priceChangePercent', 0)),
                    }
            except Exception:
                continue
        raise Exception(f"Failed to fetch ticker for {symbol}")

    async def fetch_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        binance_symbol = self._convert_symbol(symbol)
        for base in BINANCE_FUTURES_ENDPOINTS:
            try:
                data = await self._http_get(f"{base}/fapi/v1/depth", {'symbol': binance_symbol, 'limit': limit}, "binance_futures")
                if data:
                    return {
                        'bids': [[float(p), float(q)] for p, q in data.get('bids', [])],
                        'asks': [[float(p), float(q)] for p, q in data.get('asks', [])],
                    }
            except Exception:
                continue
        raise Exception(f"Failed to fetch order book for {symbol}")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def __del__(self):
        if self._session and not self._session.closed:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._session.close())
                else:
                    loop.run_until_complete(self._session.close())
            except Exception:
                pass

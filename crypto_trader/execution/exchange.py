"""
Exchange interface abstraction.
Provides unified interface for multiple cryptocurrency exchanges.
"""

import asyncio
import logging
import math
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal
from enum import Enum

import ccxt

from ..infra.config import get_config
from ..infra.logger import LogMixin


class OrderType(Enum):
    """Order types."""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    STOP_MARKET = "stop_market"


class OrderSide(Enum):
    """Order sides."""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order statuses."""
    OPEN = "open"
    CLOSED = "closed"
    FILLED = "filled"
    CANCELED = "canceled"
    EXPIRED = "expired"
    REJECTED = "rejected"


class Order:
    """Order representation."""
    
    def __init__(
        self,
        order_id: str,
        symbol: str,
        order_type: OrderType,
        side: OrderSide,
        amount: Decimal,
        price: Optional[Decimal] = None,
        status: OrderStatus = OrderStatus.OPEN,
        timestamp: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize order.
        
        Args:
            order_id: Exchange order ID
            symbol: Trading symbol
            order_type: Order type
            side: Buy or sell
            amount: Order amount
            price: Order price (None for market orders)
            status: Order status
            timestamp: Order creation time
            metadata: Additional order metadata
        """
        self.order_id = order_id
        self.symbol = symbol
        self.order_type = order_type
        self.side = side
        self.amount = amount
        self.price = price
        self.status = status
        self.timestamp = timestamp or datetime.now()
        self.metadata = metadata or {}
    
    def __repr__(self) -> str:
        price_str = f"{self.price:.8f}" if self.price else "MARKET"
        return (f"Order({self.order_id}, {self.symbol}, {self.side.value}, "
                f"{self.order_type.value}, amount={self.amount:.8f}, "
                f"price={price_str})")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert order to dictionary."""
        return {
            'order_id': self.order_id,
            'symbol': self.symbol,
            'order_type': self.order_type.value,
            'side': self.side.value,
            'amount': float(self.amount),
            'price': float(self.price) if self.price else None,
            'status': self.status.value,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata
        }


class Position:
    """Position representation."""
    
    def __init__(
        self,
        symbol: str,
        side: OrderSide,
        amount: Decimal,
        entry_price: Decimal,
        current_price: Decimal,
        unrealized_pnl: Decimal,
        realized_pnl: Decimal = Decimal('0'),
        timestamp: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize position.
        
        Args:
            symbol: Trading symbol
            side: Position side (long/short)
            amount: Position amount
            entry_price: Entry price
            current_price: Current market price
            unrealized_pnl: Unrealized profit/loss
            realized_pnl: Realized profit/loss
            timestamp: Position creation time
            metadata: Additional position metadata
        """
        self.symbol = symbol
        self.side = side
        self.amount = amount
        self.entry_price = entry_price
        self.current_price = current_price
        self.unrealized_pnl = unrealized_pnl
        self.realized_pnl = realized_pnl
        self.timestamp = timestamp or datetime.now()
        self.metadata = metadata or {}
    
    @property
    def pnl_percentage(self) -> Decimal:
        """Calculate P&L percentage."""
        if self.entry_price == 0:
            return Decimal('0')
        
        if self.side == OrderSide.BUY:
            return ((self.current_price - self.entry_price) / self.entry_price) * Decimal('100')
        else:  # SELL (short)
            return ((self.entry_price - self.current_price) / self.entry_price) * Decimal('100')
    
    def __repr__(self) -> str:
        return (f"Position({self.symbol}, {self.side.value}, "
                f"amount={self.amount:.8f}, entry={self.entry_price:.2f}, "
                f"current={self.current_price:.2f}, P&L={self.unrealized_pnl:+.2f})")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary."""
        return {
            'symbol': self.symbol,
            'side': self.side.value,
            'amount': float(self.amount),
            'entry_price': float(self.entry_price),
            'current_price': float(self.current_price),
            'unrealized_pnl': float(self.unrealized_pnl),
            'realized_pnl': float(self.realized_pnl),
            'pnl_percentage': float(self.pnl_percentage),
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata
        }


class ExchangeInterface(ABC, LogMixin):
    """Abstract exchange interface."""
    
    @abstractmethod
    async def create_order(
        self,
        symbol: str,
        order_type: OrderType,
        side: OrderSide,
        amount: Decimal,
        price: Optional[Decimal] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Order:
        """Create new order."""
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel order."""
        pass
    
    @abstractmethod
    async def cancel_all_orders(self, symbol: str) -> bool:
        """Cancel all open orders for a symbol."""
        pass
    
    @abstractmethod
    async def get_order(self, order_id: str, symbol: str) -> Optional[Order]:
        """Get order by ID."""
        pass
    
    @abstractmethod
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get open orders."""
        pass
    
    @abstractmethod
    async def get_balance(self) -> Dict[str, Decimal]:
        """Get account balances."""
        pass
    
    @abstractmethod
    async def get_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """Get current positions."""
        pass
    
    @abstractmethod
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get current ticker."""
        pass


class CCXTExchange(ExchangeInterface):
    """CCXT-based exchange implementation for USDT-margined futures."""
    
    def __init__(
        self,
        exchange_id: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: bool = True,
        leverage: int = 5,
        demo_api: Optional[str] = None,
        proxy: Optional[str] = None
    ):
        """
        Initialize CCXT exchange for USDT-margined futures.
        
        Args:
            exchange_id: CCXT exchange ID
            api_key: API key
            api_secret: API secret
            testnet: Use testnet mode
            leverage: Default leverage for futures
            demo_api: Demo API base URL (e.g. https://demo-fapi.binance.com)
            proxy: HTTP/HTTPS proxy URL
        """
        super().__init__()
        
        exchange_class = getattr(ccxt, exchange_id)
        
        config = {
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'timeout': 30000,
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
        self.leverage = leverage
        
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
            self.logger.info(f"Using demo API: {demo_base}")
        elif testnet:
            try:
                self.exchange.set_sandbox_mode(True)
                self.logger.info(f"Enabled testnet mode for {exchange_id}")
            except AttributeError:
                self.logger.warning(f"Testnet not supported for {exchange_id}")
        
        self.exchange_id = exchange_id
        self._leverage_set: set = set()
        self._position_mode_set: set = set()
        self._markets_loaded: bool = False
    
    async def create_order(
        self,
        symbol: str,
        order_type: OrderType,
        side: OrderSide,
        amount: Decimal,
        price: Optional[Decimal] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Order:
        """Create order on USDT-margined futures."""
        try:
            loop = asyncio.get_event_loop()

            if not self._markets_loaded:
                try:
                    await loop.run_in_executor(None, lambda: self.exchange.load_markets())
                    self._markets_loaded = True
                    self.logger.info("Markets loaded for precision info")
                except Exception as e:
                    self.logger.warning(f"Failed to load markets: {e}")

            ccxt_side = side.value
            ccxt_type = order_type.value

            float_amount = float(amount)
            float_price = float(price) if price else None

            market = self.exchange.markets.get(symbol)
            if market:
                raw_amount_prec = market.get('precision', {}).get('amount', 8)
                raw_price_prec = market.get('precision', {}).get('price', 8)
                if isinstance(raw_amount_prec, float) and raw_amount_prec < 1:
                    amount_precision = int(round(-math.log10(raw_amount_prec)))
                else:
                    amount_precision = int(raw_amount_prec)
                if isinstance(raw_price_prec, float) and raw_price_prec < 1:
                    price_precision = int(round(-math.log10(raw_price_prec)))
                else:
                    price_precision = int(raw_price_prec)
                float_amount = round(float_amount, amount_precision)
                if float_price is not None:
                    float_price = round(float_price, price_precision)
                self.logger.debug(
                    f"Precision applied: amount={float_amount} (prec={amount_precision}), "
                    f"price={float_price} (prec={price_precision})"
                )

            params = {}
            if float_price is not None and ccxt_type != 'market':
                params['price'] = float_price

            if ccxt_type == 'limit':
                params['timeInForce'] = 'GTC'

            if metadata and metadata.get('reduce_only'):
                params['reduceOnly'] = True

            if symbol not in self._position_mode_set:
                try:
                    await loop.run_in_executor(
                        None,
                        lambda: self.exchange.fapiPrivatePostPositionSideDual({
                            'dualSidePosition': 'false'
                        })
                    )
                    self._position_mode_set.add(symbol)
                    self.logger.info(f"Set one-way position mode for {symbol}")
                except Exception as e:
                    self.logger.debug(f"Position mode already set or not supported for {symbol}: {e}")

            if symbol not in self._leverage_set:
                try:
                    await loop.run_in_executor(
                        None,
                        lambda: self.exchange.set_leverage(self.leverage, symbol)
                    )
                    self._leverage_set.add(symbol)
                    self.logger.info(f"Set leverage to {self.leverage}x for {symbol}")
                except Exception as e:
                    self.logger.warning(f"Could not set leverage for {symbol}: {e}")

            ccxt_order = await loop.run_in_executor(
                None,
                lambda: self.exchange.create_order(
                    symbol=symbol,
                    type=ccxt_type,
                    side=ccxt_side,
                    amount=float_amount,
                    price=float_price if (float_price is not None and ccxt_type != 'market') else None,
                    params=params
                )
            )

            order = self._ccxt_to_order(ccxt_order)
            self.logger.info(f"Created futures order: {order}")

            return order

        except Exception as e:
            self.log_exception(f"Failed to create order for {symbol}", e)
            raise

    async def create_stop_order(
        self,
        symbol: str,
        side: OrderSide,
        amount: Decimal,
        stop_price: Decimal,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Order:
        try:
            loop = asyncio.get_event_loop()

            if not self._markets_loaded:
                try:
                    await loop.run_in_executor(None, lambda: self.exchange.load_markets())
                    self._markets_loaded = True
                except Exception as e:
                    self.logger.warning(f"Failed to load markets: {e}")

            ccxt_side = side.value
            float_amount = float(amount)
            float_stop_price = float(stop_price)

            market = self.exchange.markets.get(symbol)
            if market:
                raw_amount_prec = market.get('precision', {}).get('amount', 8)
                raw_price_prec = market.get('precision', {}).get('price', 8)
                if isinstance(raw_amount_prec, float) and raw_amount_prec < 1:
                    amount_precision = int(round(-math.log10(raw_amount_prec)))
                else:
                    amount_precision = int(raw_amount_prec)
                if isinstance(raw_price_prec, float) and raw_price_prec < 1:
                    price_precision = int(round(-math.log10(raw_price_prec)))
                else:
                    price_precision = int(raw_price_prec)
                float_amount = round(float_amount, amount_precision)
                float_stop_price = round(float_stop_price, price_precision)

            binance_symbol = symbol.replace('/', '').replace(':USDT', '')

            try:
                algo_result = await loop.run_in_executor(
                    None,
                    lambda: self.exchange.fapiPrivatePostAlgoOrder({
                        'symbol': binance_symbol,
                        'side': ccxt_side.upper(),
                        'positionSide': 'BOTH',
                        'type': 'STOP_MARKET',
                        'algoType': 'CONDITIONAL',
                        'quantity': float_amount,
                        'triggerPrice': float_stop_price,
                        'reduceOnly': 'true',
                        'timeInForce': 'GTE_GTC',
                    })
                )
                algo_id = str(algo_result.get('algoId', algo_result.get('clientAlgoId', '')))
                self.logger.info(f"Created algo stop order: algoId={algo_id}, {ccxt_side} {float_amount} {symbol} @ stop {float_stop_price}")
                return Order(
                    order_id=algo_id,
                    symbol=symbol,
                    order_type=OrderType.STOP_MARKET,
                    side=side,
                    amount=Decimal(str(float_amount)),
                    price=Decimal(str(float_stop_price)),
                    status=OrderStatus.OPEN,
                    metadata={'stopPrice': str(float_stop_price), 'reduceOnly': True, 'is_algo': True}
                )
            except Exception as algo_err:
                self.logger.warning(f"Algo order endpoint failed, falling back to legacy: {algo_err}")
                params = {
                    'stopPrice': float_stop_price,
                    'reduceOnly': True,
                }
                ccxt_order = await loop.run_in_executor(
                    None,
                    lambda: self.exchange.create_order(
                        symbol=symbol,
                        type='stop_market',
                        side=ccxt_side,
                        amount=float_amount,
                        params=params
                    )
                )
                order = self._ccxt_to_order(ccxt_order)
                self.logger.info(f"Created stop order (legacy): {order}")
                return order

        except Exception as e:
            self.log_exception(f"Failed to create stop order for {symbol}", e)
            raise
    
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel order."""
        try:
            loop = asyncio.get_event_loop()
            
            await loop.run_in_executor(
                None,
                lambda: self.exchange.cancel_order(order_id, symbol)
            )
            
            self.logger.info(f"Cancelled order {order_id} for {symbol}")
            return True
            
        except Exception as e:
            self.log_exception(f"Failed to cancel order {order_id}", e)
            return False

    async def cancel_all_orders(self, symbol: str) -> bool:
        """Cancel all open orders (regular + algo/conditional) for a symbol."""
        result = True

        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.exchange.cancel_all_orders(symbol)
            )
            self.logger.info(f"Cancelled all regular open orders for {symbol}")
        except Exception as e:
            self.log_exception(f"Failed to cancel regular orders for {symbol}", e)
            result = False

        try:
            loop = asyncio.get_event_loop()
            binance_symbol = symbol.replace('/', '').replace(':USDT', '')
            await loop.run_in_executor(
                None,
                lambda: self.exchange.fapiPrivateDeleteAlgoOpenOrders({
                    'symbol': binance_symbol,
                    'algoType': 'CONDITIONAL',
                })
            )
            self.logger.info(f"Cancelled all algo/conditional open orders for {symbol}")
        except Exception as e:
            self.logger.debug(f"No algo orders to cancel for {symbol} or endpoint not available: {e}")

        return result
    
    async def get_order(self, order_id: str, symbol: str) -> Optional[Order]:
        """Get order by ID."""
        try:
            loop = asyncio.get_event_loop()
            
            ccxt_order = await loop.run_in_executor(
                None,
                lambda: self.exchange.fetch_order(order_id, symbol)
            )
            
            return self._ccxt_to_order(ccxt_order)
            
        except Exception as e:
            self.logger.error(f"Failed to get order {order_id}: {e}")
            return None
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get open orders (regular + algo/conditional)."""
        orders = []

        try:
            loop = asyncio.get_event_loop()
            ccxt_orders = await loop.run_in_executor(
                None,
                lambda: self.exchange.fetch_open_orders(symbol) if symbol else self.exchange.fetch_open_orders()
            )
            orders.extend([self._ccxt_to_order(o) for o in ccxt_orders])
        except Exception as e:
            self.log_exception("Failed to get regular open orders", e)

        try:
            loop = asyncio.get_event_loop()
            binance_symbol = symbol.replace('/', '').replace(':USDT', '') if symbol else None
            params = {'symbol': binance_symbol, 'algoType': 'CONDITIONAL'} if binance_symbol else {'algoType': 'CONDITIONAL'}
            algo_result = await loop.run_in_executor(
                None,
                lambda: self.exchange.fapiPrivateGetOpenAlgoOrders(params)
            )
            algo_orders = algo_result.get('orders', []) if isinstance(algo_result, dict) else []
            for ao in algo_orders:
                try:
                    algo_symbol_raw = ao.get('symbol', '')
                    algo_symbol = symbol or algo_symbol_raw
                    algo_type_str = ao.get('type', 'STOP_MARKET').lower()
                    try:
                        algo_type = OrderType(algo_type_str)
                    except ValueError:
                        algo_type = OrderType.STOP_MARKET
                    algo_side = OrderSide.BUY if ao.get('side', '').lower() == 'buy' else OrderSide.SELL
                    algo_amount = Decimal(str(ao.get('origQty', 0)))
                    algo_stop_price = ao.get('stopPrice')
                    algo_price = Decimal(str(algo_stop_price)) if algo_stop_price else None
                    algo_status_str = ao.get('status', '').lower()
                    status_map = {
                        'new': OrderStatus.OPEN,
                        'pending': OrderStatus.OPEN,
                        'triggered': OrderStatus.FILLED,
                        'cancelled': OrderStatus.CANCELED,
                    }
                    algo_status = status_map.get(algo_status_str, OrderStatus.OPEN)
                    algo_metadata = {'stopPrice': str(algo_stop_price)} if algo_stop_price else {}
                    if ao.get('reduceOnly'):
                        algo_metadata['reduceOnly'] = True

                    order = Order(
                        order_id=str(ao.get('algoId', ao.get('clientAlgoId', ''))),
                        symbol=algo_symbol,
                        order_type=algo_type,
                        side=algo_side,
                        amount=algo_amount,
                        price=algo_price,
                        status=algo_status,
                        metadata=algo_metadata
                    )
                    orders.append(order)
                except Exception as e:
                    self.logger.debug(f"Failed to parse algo order {ao}: {e}")
        except Exception as e:
            self.logger.debug(f"No algo orders or endpoint not available: {e}")

        return orders
    
    async def get_balance(self) -> Dict[str, Decimal]:
        """Get account balances."""
        try:
            loop = asyncio.get_event_loop()
            
            ccxt_balance = await loop.run_in_executor(
                None,
                lambda: self.exchange.fetch_balance()
            )
            
            # Extract free balances
            balances = {}
            for currency, data in ccxt_balance.get('free', {}).items():
                if data > 0:
                    balances[currency] = Decimal(str(data))
            
            return balances
            
        except Exception as e:
            self.log_exception("Failed to get balance", e)
            return {}
    
    async def get_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """Get USDT-margined futures positions."""
        try:
            loop = asyncio.get_event_loop()

            try:
                ccxt_positions = await loop.run_in_executor(
                    None,
                    lambda: self.exchange.fetch_positions([symbol] if symbol else None)
                )
            except Exception as e:
                self.logger.warning(f"fetch_positions failed, trying fapiPrivateV2GetAccount: {e}")
                try:
                    account = await loop.run_in_executor(
                        None,
                        lambda: self.exchange.fapiPrivateV2GetAccount()
                    )
                    ccxt_positions = account.get('positions', [])
                except Exception as e2:
                    self.logger.error(f"fapiPrivateV2GetAccount also failed: {e2}")
                    return []

            positions = []
            for p in ccxt_positions:
                contracts = Decimal(str(p.get('contracts', 0) or 0))
                if contracts <= 0:
                    continue

                pos_symbol = p.get('symbol', '')
                if symbol and pos_symbol != symbol:
                    continue

                side = OrderSide.BUY if p.get('side') == 'long' else OrderSide.SELL
                entry_price = Decimal(str(p.get('entryPrice', 0) or 0))
                notional = Decimal(str(p.get('notional', 0) or 0))
                unrealized_pnl = Decimal(str(p.get('unrealizedPnl', 0) or 0))
                mark_price = Decimal(str(p.get('markPrice', 0) or 0))
                current_price = mark_price if mark_price > 0 else entry_price

                positions.append(Position(
                    symbol=pos_symbol,
                    side=side,
                    amount=contracts,
                    entry_price=entry_price,
                    current_price=current_price,
                    unrealized_pnl=unrealized_pnl,
                    realized_pnl=Decimal('0'),
                    timestamp=datetime.now(),
                    metadata={
                        'leverage': p.get('leverage', 1),
                        'liquidation_price': p.get('liquidationPrice'),
                        'margin': float(p.get('initialMargin', 0) or 0),
                        'position_side': p.get('side', 'long'),
                        'stop_loss': None,
                        'take_profit': None
                    }
                ))

            return positions

        except Exception as e:
            self.log_exception("Failed to get positions", e)
            return []
    
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get ticker."""
        try:
            loop = asyncio.get_event_loop()
            
            ticker = await loop.run_in_executor(
                None,
                lambda: self.exchange.fetch_ticker(symbol)
            )
            
            return ticker
            
        except Exception as e:
            self.log_exception(f"Failed to get ticker for {symbol}", e)
            raise
    
    def _ccxt_to_order(self, ccxt_order: Dict[str, Any]) -> Order:
        """Convert CCXT order to our Order object."""
        # Map CCXT status to our status
        status_map = {
            'open': OrderStatus.OPEN,
            'closed': OrderStatus.CLOSED,
            'filled': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELED,
            'cancelled': OrderStatus.CANCELED,
            'expired': OrderStatus.EXPIRED,
            'rejected': OrderStatus.REJECTED
        }
        
        raw_type = ccxt_order['type']
        try:
            order_type = OrderType(raw_type)
        except ValueError:
            order_type = OrderType.STOP_MARKET if 'stop' in str(raw_type).lower() else OrderType.MARKET
        side = OrderSide(ccxt_order['side'])
        status = status_map.get(ccxt_order['status'], OrderStatus.OPEN)

        metadata = {}
        if ccxt_order.get('stopPrice'):
            metadata['stopPrice'] = ccxt_order['stopPrice']
        info = ccxt_order.get('info', {})
        if info.get('stopPrice') and 'stopPrice' not in metadata:
            metadata['stopPrice'] = info['stopPrice']
        if ccxt_order.get('reduceOnly'):
            metadata['reduceOnly'] = ccxt_order['reduceOnly']

        return Order(
            order_id=str(ccxt_order['id']),
            symbol=ccxt_order['symbol'],
            order_type=order_type,
            side=side,
            amount=Decimal(str(ccxt_order['amount'])),
            price=Decimal(str(ccxt_order['price'])) if ccxt_order.get('price') else None,
            status=status,
            timestamp=datetime.fromtimestamp(ccxt_order['timestamp'] / 1000) if ccxt_order.get('timestamp') else None,
            metadata=metadata if metadata else None
        )


def create_exchange_from_config() -> ExchangeInterface:
    """
    Create exchange interface from configuration for USDT-margined futures.
    
    Returns:
        Configured exchange interface
    """
    config = get_config()
    exchange_config = config.exchange
    
    return CCXTExchange(
        exchange_id=exchange_config.name.value,
        api_key=exchange_config.api_key,
        api_secret=exchange_config.api_secret,
        testnet=exchange_config.testnet,
        leverage=exchange_config.leverage,
        demo_api=exchange_config.demo_api,
        proxy=exchange_config.proxy
    )
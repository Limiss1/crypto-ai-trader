"""
Paper trading exchange for USDT-margined futures contracts.
Supports both LONG and SHORT positions with leverage.
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal
from enum import Enum

import ccxt

from .exchange import (
    ExchangeInterface, Order, Position,
    OrderType, OrderSide, OrderStatus
)
from ..infra.logger import LogMixin
from ..infra.config import get_config


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"


class FuturesPosition:
    """USDT-margined futures position."""

    def __init__(
        self,
        symbol: str,
        side: PositionSide,
        amount: Decimal,
        entry_price: Decimal,
        leverage: int = 1,
        margin: Decimal = Decimal('0'),
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.position_id = str(uuid.uuid4())[:8]
        self.symbol = symbol
        self.side = side
        self.amount = amount
        self.entry_price = entry_price
        self.current_price = entry_price
        self.leverage = leverage
        self.margin = margin
        self.unrealized_pnl: Decimal = Decimal('0')
        self.realized_pnl: Decimal = Decimal('0')
        self.liquidation_price: Optional[Decimal] = None
        self.timestamp: datetime = datetime.now()
        self.metadata: Dict[str, Any] = metadata or {}

        self._update_liquidation_price()

    def _update_liquidation_price(self) -> None:
        if self.leverage <= 0:
            return
        maintenance_margin_rate = Decimal('0.004')
        if self.side == PositionSide.LONG:
            self.liquidation_price = self.entry_price * (
                Decimal('1') - (Decimal('1') - maintenance_margin_rate) / Decimal(str(self.leverage))
            )
        else:
            self.liquidation_price = self.entry_price * (
                Decimal('1') + (Decimal('1') - maintenance_margin_rate) / Decimal(str(self.leverage))
            )

    def update_price(self, price: Decimal) -> None:
        self.current_price = price
        notional = self.amount * price
        if self.side == PositionSide.LONG:
            self.unrealized_pnl = (price - self.entry_price) * self.amount
        else:
            self.unrealized_pnl = (self.entry_price - price) * self.amount

    @property
    def notional_value(self) -> Decimal:
        return self.amount * self.current_price

    @property
    def margin_ratio(self) -> Decimal:
        if self.margin <= 0:
            return Decimal('0')
        equity = self.margin + self.unrealized_pnl
        return equity / self.margin

    def to_position(self) -> Position:
        order_side = OrderSide.BUY if self.side == PositionSide.LONG else OrderSide.SELL
        return Position(
            symbol=self.symbol,
            side=order_side,
            amount=self.amount,
            entry_price=self.entry_price,
            current_price=self.current_price,
            unrealized_pnl=self.unrealized_pnl,
            realized_pnl=self.realized_pnl,
            timestamp=self.timestamp,
            metadata={
                'position_side': self.side.value,
                'leverage': self.leverage,
                'margin': float(self.margin),
                'liquidation_price': float(self.liquidation_price) if self.liquidation_price else None,
                **self.metadata
            }
        )


class PaperExchange(ExchangeInterface):
    """Simulated USDT-margined futures exchange for paper trading."""

    def __init__(
        self,
        initial_balance: Dict[str, float] = None,
        default_leverage: int = 10,
        use_api_balance: bool = True
    ):
        super().__init__()

        self.use_api_balance = use_api_balance
        self._api_exchange = None
        self._api_balance_fetched = False

        if use_api_balance:
            try:
                config = get_config()
                exchange_config = config.exchange
                exchange_class = getattr(ccxt, exchange_config.name.value)
                ccxt_config = {
                    'apiKey': exchange_config.api_key,
                    'secret': exchange_config.api_secret,
                    'enableRateLimit': True,
                    'options': {
                        'defaultType': 'future',
                        'adjustForTimeDifference': True
                    }
                }
                if exchange_config.proxy:
                    ccxt_config['proxies'] = {
                        'http': exchange_config.proxy,
                        'https': exchange_config.proxy,
                    }
                    ccxt_config['aiohttp_proxy'] = exchange_config.proxy
                self._api_exchange = exchange_class(ccxt_config)
                
                demo_api = exchange_config.demo_api
                if demo_api:
                    demo_base = demo_api.rstrip('/')
                    self._api_exchange.urls['api'] = {
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
                    self._api_exchange.urls['test'] = self._api_exchange.urls['api'].copy()
                    self._api_exchange.sandbox = False
                    self._api_exchange.has['fetchCurrencies'] = False
                    self.logger.info(f"PaperExchange: Using demo API: {demo_base}")
                elif exchange_config.testnet:
                    try:
                        self._api_exchange.set_sandbox_mode(True)
                    except AttributeError:
                        pass
                self.logger.info(f"PaperExchange: Binance API client initialized for balance queries")
            except Exception as e:
                self.logger.warning(f"PaperExchange: Failed to init Binance API client: {e}")
                self._api_exchange = None

        if initial_balance is None:
            initial_balance = {"USDT": 10000.0}

        self.balances: Dict[str, Decimal] = {
            k: Decimal(str(v)) for k, v in initial_balance.items()
        }
        self.orders: Dict[str, Order] = {}
        self.futures_positions: Dict[str, FuturesPosition] = {}
        self.default_leverage = default_leverage
        self.trade_history: List[Dict[str, Any]] = []
        self.total_realized_pnl: Decimal = Decimal('0')
        self._market_prices: Dict[str, Decimal] = {}

        self.initial_capital = sum(self.balances.values())
        self.logger.info(
            f"PaperExchange (USDT Futures) initialized: "
            f"balance={dict(self.balances)}, leverage={default_leverage}x"
        )

    async def create_order(
        self,
        symbol: str,
        order_type: OrderType,
        side: OrderSide,
        amount: Decimal,
        price: Optional[Decimal] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Order:
        if price is None or price == Decimal('0'):
            price = self._market_prices.get(symbol)
            if price is None:
                raise ValueError(f"Cannot determine price for {symbol}")

        notional_value = amount * price
        leverage = self.default_leverage
        margin_required = notional_value / Decimal(str(leverage))

        available_balance = self.balances.get("USDT", Decimal('0'))

        if side == OrderSide.BUY:
            if symbol in self.futures_positions:
                pos = self.futures_positions[symbol]
                if pos.side == PositionSide.SHORT:
                    close_amount = min(amount, pos.amount)
                    close_pnl = (pos.entry_price - price) * close_amount
                    self.total_realized_pnl += close_pnl
                    self.balances["USDT"] += close_pnl
                    pos.amount -= close_amount
                    pos.realized_pnl += close_pnl
                    if pos.amount <= Decimal('0'):
                        self.balances["USDT"] += pos.margin
                        del self.futures_positions[symbol]
                    remaining = amount - close_amount
                    if remaining > Decimal('0'):
                        new_margin = remaining * price / Decimal(str(leverage))
                        if new_margin > self.balances.get("USDT", Decimal('0')):
                            raise ValueError(f"Insufficient margin: need {new_margin}, have {self.balances.get('USDT', 0)}")
                        self.balances["USDT"] -= new_margin
                        self.futures_positions[symbol] = FuturesPosition(
                            symbol=symbol,
                            side=PositionSide.LONG,
                            amount=remaining,
                            entry_price=price,
                            leverage=leverage,
                            margin=new_margin,
                            metadata=metadata
                        )
                else:
                    new_margin = notional_value / Decimal(str(leverage))
                    if new_margin > self.balances.get("USDT", Decimal('0')):
                        raise ValueError(f"Insufficient margin: need {new_margin}, have {self.balances.get('USDT', 0)}")
                    total_amount = pos.amount + amount
                    total_cost = pos.entry_price * pos.amount + price * amount
                    new_entry = total_cost / total_amount
                    pos.entry_price = new_entry
                    pos.amount = total_amount
                    pos.margin += new_margin
                    pos.leverage = leverage
                    self.balances["USDT"] -= new_margin
            else:
                if margin_required > available_balance:
                    raise ValueError(
                        f"Insufficient margin: need {margin_required:.2f} USDT, "
                        f"have {available_balance:.2f} USDT"
                    )
                self.balances["USDT"] -= margin_required
                self.futures_positions[symbol] = FuturesPosition(
                    symbol=symbol,
                    side=PositionSide.LONG,
                    amount=amount,
                    entry_price=price,
                    leverage=leverage,
                    margin=margin_required,
                    metadata=metadata
                )

        elif side == OrderSide.SELL:
            if symbol in self.futures_positions:
                pos = self.futures_positions[symbol]
                if pos.side == PositionSide.LONG:
                    close_amount = min(amount, pos.amount)
                    close_pnl = (price - pos.entry_price) * close_amount
                    self.total_realized_pnl += close_pnl
                    self.balances["USDT"] += close_pnl
                    pos.amount -= close_amount
                    pos.realized_pnl += close_pnl
                    if pos.amount <= Decimal('0'):
                        self.balances["USDT"] += pos.margin
                        del self.futures_positions[symbol]
                    remaining = amount - close_amount
                    if remaining > Decimal('0'):
                        new_margin = remaining * price / Decimal(str(leverage))
                        if new_margin > self.balances.get("USDT", Decimal('0')):
                            raise ValueError(f"Insufficient margin: need {new_margin}, have {self.balances.get('USDT', 0)}")
                        self.balances["USDT"] -= new_margin
                        self.futures_positions[symbol] = FuturesPosition(
                            symbol=symbol,
                            side=PositionSide.SHORT,
                            amount=remaining,
                            entry_price=price,
                            leverage=leverage,
                            margin=new_margin,
                            metadata=metadata
                        )
                else:
                    new_margin = notional_value / Decimal(str(leverage))
                    if new_margin > self.balances.get("USDT", Decimal('0')):
                        raise ValueError(f"Insufficient margin: need {new_margin}, have {self.balances.get('USDT', 0)}")
                    total_amount = pos.amount + amount
                    total_cost = pos.entry_price * pos.amount + price * amount
                    new_entry = total_cost / total_amount
                    pos.entry_price = new_entry
                    pos.amount = total_amount
                    pos.margin += new_margin
                    pos.leverage = leverage
                    self.balances["USDT"] -= new_margin
            else:
                if margin_required > available_balance:
                    raise ValueError(
                        f"Insufficient margin: need {margin_required:.2f} USDT, "
                        f"have {available_balance:.2f} USDT"
                    )
                self.balances["USDT"] -= margin_required
                self.futures_positions[symbol] = FuturesPosition(
                    symbol=symbol,
                    side=PositionSide.SHORT,
                    amount=amount,
                    entry_price=price,
                    leverage=leverage,
                    margin=margin_required,
                    metadata=metadata
                )

        order_id = str(uuid.uuid4())[:8]
        order = Order(
            order_id=order_id,
            symbol=symbol,
            order_type=order_type,
            side=side,
            amount=amount,
            price=price,
            status=OrderStatus.CLOSED,
            timestamp=datetime.now(),
            metadata={
                'paper_trade': True,
                'leverage': leverage,
                'margin': float(margin_required)
            }
        )
        self.orders[order_id] = order

        pos_side = "LONG" if side == OrderSide.BUY else "SHORT"
        if symbol in self.futures_positions and self.futures_positions[symbol].side.value != pos_side.lower():
            pos_side = "CLOSE"

        trade_record = {
            'order_id': order_id,
            'symbol': symbol,
            'side': side.value,
            'position_side': pos_side,
            'amount': float(amount),
            'price': float(price),
            'notional': float(notional_value),
            'margin': float(margin_required),
            'leverage': leverage,
            'timestamp': datetime.now().isoformat()
        }
        self.trade_history.append(trade_record)

        self.logger.info(
            f"[PAPER] {side.value.upper()} {pos_side} {amount} {symbol} @ {price:.2f} "
            f"(margin: {margin_required:.2f} USDT, {leverage}x)"
        )

        return order

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        if order_id in self.orders:
            self.orders[order_id].status = OrderStatus.CANCELED
            return True
        return False

    async def cancel_all_orders(self, symbol: str) -> bool:
        for oid, order in self.orders.items():
            if order.symbol == symbol and order.status == OrderStatus.OPEN:
                order.status = OrderStatus.CANCELED
        return True

    async def get_order(self, order_id: str, symbol: str) -> Optional[Order]:
        return self.orders.get(order_id)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        return [
            o for o in self.orders.values()
            if o.status == OrderStatus.OPEN
            and (symbol is None or o.symbol == symbol)
        ]

    async def get_balance(self) -> Dict[str, Decimal]:
        if self.use_api_balance and self._api_exchange and not self._api_balance_fetched:
            try:
                loop = asyncio.get_event_loop()
                ccxt_balance = await loop.run_in_executor(
                    None,
                    lambda: self._api_exchange.fetch_balance()
                )
                api_balances = {}
                for currency, data in ccxt_balance.get('free', {}).items():
                    if isinstance(data, (int, float)) and data > 0:
                        api_balances[currency] = Decimal(str(data))
                
                if 'USDT' in api_balances:
                    self.balances['USDT'] = api_balances['USDT']
                    self._api_balance_fetched = True
                    self.initial_capital = self.balances['USDT']
                    self.logger.info(
                        f"[BINANCE API] Initial USDT balance: {api_balances['USDT']:.2f}"
                    )
                else:
                    self.logger.warning(f"[BINANCE API] No USDT balance found, using local: {self.balances.get('USDT', 0):.2f}")
            except Exception as e:
                self.logger.warning(f"[BINANCE API] Failed to fetch balance: {e}, using local: {self.balances.get('USDT', 0):.2f}")
        
        return dict(self.balances)

    async def get_positions(self, symbol: Optional[str] = None) -> List[Position]:
        positions = [p.to_position() for p in self.futures_positions.values()]
        if symbol:
            positions = [p for p in positions if p.symbol == symbol]
        return positions

    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        price = self._market_prices.get(symbol, Decimal('0'))
        return {
            'symbol': symbol,
            'last': float(price),
            'bid': float(price * Decimal('0.999')),
            'ask': float(price * Decimal('1.001')),
            'timestamp': int(datetime.now().timestamp() * 1000)
        }

    async def update_market_prices(self, prices: Dict[str, Decimal]) -> None:
        self._market_prices = prices
        for symbol, price in prices.items():
            if symbol in self.futures_positions:
                self.futures_positions[symbol].update_price(price)

    async def _get_market_price(self, symbol: str) -> Optional[Decimal]:
        return self._market_prices.get(symbol)

    def get_portfolio_summary(self) -> Dict[str, Any]:
        total_unrealized = sum(p.unrealized_pnl for p in self.futures_positions.values())
        total_margin = sum(p.margin for p in self.futures_positions.values())
        equity = self.balances.get("USDT", Decimal('0')) + total_unrealized
        total_pnl = self.total_realized_pnl + total_unrealized
        total_pnl_pct = (
            (total_pnl / self.initial_capital * 100)
            if self.initial_capital > 0 else Decimal('0')
        )

        positions_info = []
        for p in self.futures_positions.values():
            positions_info.append({
                'symbol': p.symbol,
                'side': p.side.value,
                'amount': float(p.amount),
                'entry_price': float(p.entry_price),
                'current_price': float(p.current_price),
                'unrealized_pnl': float(p.unrealized_pnl),
                'margin': float(p.margin),
                'leverage': p.leverage,
                'liquidation_price': float(p.liquidation_price) if p.liquidation_price else None
            })

        return {
            'equity': float(equity),
            'available_balance': float(self.balances.get("USDT", Decimal('0'))),
            'total_margin_used': float(total_margin),
            'initial_capital': float(self.initial_capital),
            'unrealized_pnl': float(total_unrealized),
            'realized_pnl': float(self.total_realized_pnl),
            'total_pnl': float(total_pnl),
            'total_pnl_pct': float(total_pnl_pct),
            'positions': positions_info,
            'position_count': len(self.futures_positions),
            'trades': len(self.trade_history)
        }

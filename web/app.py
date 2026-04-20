import asyncio
import json
import logging
import os
import signal as signal_module
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel, Field

from crypto_trader.infra.config import (
    TradingConfig, ExchangeConfig, StrategyConfig, RiskConfig, DataConfig,
    TradingMode, ExchangeType, load_config, set_config, get_config
)
from crypto_trader.data.market_data import create_data_feed_from_config, MarketData
from crypto_trader.data.free_data_feed import BinancePublicDataFeed
from crypto_trader.execution.exchange import create_exchange_from_config, OrderSide, OrderType
from crypto_trader.execution.paper_exchange import PaperExchange
from crypto_trader.execution.trading_engine import TradingEngine
from crypto_trader.strategy.ai_strategy import AIStrategy, FIXED_FEATURE_COLUMNS
from crypto_trader.risk.risk_manager import RiskManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Crypto Trader Web", version="0.1.0")

_engine: Optional[TradingEngine] = None
_engine_task: Optional[asyncio.Task] = None
_engine_status: Dict[str, Any] = {}
_ws_clients: List[WebSocket] = []
_config_snapshot: Optional[Dict[str, Any]] = None
_log_lines: List[str] = []
_max_log_lines = 200
_cached_predictions: Dict[str, Any] = {}
_last_prediction_time: float = 0
_PREDICTION_INTERVAL: int = 30


class ExchangeConfigModel(BaseModel):
    name: str = Field(default="binance", description="交易所名称")
    api_key: Optional[str] = Field(default=None, description="API Key")
    api_secret: Optional[str] = Field(default=None, description="API Secret")
    testnet: bool = Field(default=True, description="使用测试网")
    demo_api: Optional[str] = Field(default=None, description="Demo API地址")
    proxy: Optional[str] = Field(default=None, description="代理地址")
    leverage: int = Field(default=10, ge=1, le=125, description="杠杆倍数")


class StrategyConfigModel(BaseModel):
    name: str = Field(default="ai", description="策略名称")
    confidence_threshold: float = Field(default=0.50, ge=0.0, le=1.0, description="置信度阈值")
    max_position_size: float = Field(default=0.1, gt=0.0, le=1.0, description="最大仓位比例")
    lookback_period: int = Field(default=1500, gt=0, description="回看K线数量")


class RiskConfigModel(BaseModel):
    max_drawdown: float = Field(default=0.15, ge=0.0, le=1.0, description="最大回撤")
    daily_loss_limit: float = Field(default=0.05, ge=0.0, le=1.0, description="每日亏损限制")
    max_open_positions: int = Field(default=3, gt=0, description="最大持仓数量")
    stop_loss_pct: float = Field(default=0.0015, ge=0.0, le=1.0, description="止损百分比")
    take_profit_pct: float = Field(default=0.003, ge=0.0, le=1.0, description="止盈百分比")


class DataConfigModel(BaseModel):
    cache_dir: str = Field(default="~/.crypto_trader/cache", description="缓存目录")
    historical_days: int = Field(default=30, gt=0, description="历史数据天数")
    update_interval: int = Field(default=60, gt=0, description="数据更新间隔(秒)")
    save_raw_data: bool = Field(default=False, description="保存原始数据")


class TradingStartModel(BaseModel):
    mode: str = Field(default="paper", description="交易模式: paper/live")
    symbols: List[str] = Field(default=["BTC/USDT:USDT", "ETH/USDT:USDT"], description="交易对")
    base_currency: str = Field(default="USDT", description="基础货币")
    initial_balance: float = Field(default=10000.0, description="模拟交易初始余额")
    exchange: ExchangeConfigModel = Field(default_factory=ExchangeConfigModel)
    strategy: StrategyConfigModel = Field(default_factory=StrategyConfigModel)
    risk: RiskConfigModel = Field(default_factory=RiskConfigModel)
    data: DataConfigModel = Field(default=DataConfigModel)


_market_data: Optional[MarketData] = None
_strategy: Optional[AIStrategy] = None


def _detect_system_proxy() -> Optional[str]:
    try:
        import winreg
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Internet Settings")
        enabled, _ = winreg.QueryValueEx(key, "ProxyEnable")
        server, _ = winreg.QueryValueEx(key, "ProxyServer")
        winreg.CloseKey(key)
        if enabled and server:
            if not server.startswith("http"):
                server = f"http://{server}"
            return server
    except Exception:
        pass
    return None


class LogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            _log_lines.append(msg)
            if len(_log_lines) > _max_log_lines:
                _log_lines.pop(0)
        except Exception:
            pass


log_handler = LogHandler()
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))
logging.getLogger('crypto_trader').addHandler(log_handler)


def _build_config_from_model(model: TradingStartModel) -> TradingConfig:
    exchange_type_map = {
        "binance": ExchangeType.BINANCE,
        "binanceusdm": ExchangeType.BINANCEUSDM,
        "coinbase": ExchangeType.COINBASE,
        "kraken": ExchangeType.KRAKEN,
        "okx": ExchangeType.OKX,
    }
    mode_map = {
        "paper": TradingMode.PAPER_TRADING,
        "live": TradingMode.LIVE_TRADING,
        "backtest": TradingMode.BACKTEST,
    }

    exchange_name = exchange_type_map.get(model.exchange.name, ExchangeType.BINANCE)

    config = TradingConfig(
        mode=mode_map.get(model.mode, TradingMode.PAPER_TRADING),
        symbols=model.symbols,
        base_currency=model.base_currency,
        exchange=ExchangeConfig(
            name=exchange_name,
            api_key=model.exchange.api_key,
            api_secret=model.exchange.api_secret,
            testnet=model.exchange.testnet,
            demo_api=model.exchange.demo_api,
            proxy=model.exchange.proxy,
            leverage=model.exchange.leverage,
        ),
        strategy=StrategyConfig(
            name=model.strategy.name,
            confidence_threshold=model.strategy.confidence_threshold,
            max_position_size=model.strategy.max_position_size,
            lookback_period=model.strategy.lookback_period,
        ),
        risk=RiskConfig(
            max_drawdown=model.risk.max_drawdown,
            daily_loss_limit=model.risk.daily_loss_limit,
            max_open_positions=model.risk.max_open_positions,
            stop_loss_pct=model.risk.stop_loss_pct,
            take_profit_pct=model.risk.take_profit_pct,
        ),
        data=DataConfig(
            cache_dir=Path(os.path.expanduser(model.data.cache_dir)),
            historical_days=model.data.historical_days,
            update_interval=model.data.update_interval,
            save_raw_data=model.data.save_raw_data,
        ),
    )
    return config


async def _broadcast_status():
    status = await _get_current_status()
    disconnected = []
    for ws in _ws_clients:
        try:
            await ws.send_text(json.dumps(status, ensure_ascii=False))
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        _ws_clients.remove(ws)


async def _get_current_status() -> Dict[str, Any]:
    global _engine, _engine_status, _market_data, _strategy

    if _engine is not None:
        try:
            _engine_status = _engine.get_status()
        except Exception as e:
            logger.error(f"获取引擎状态失败: {e}")

    is_running = _engine is not None and _engine.is_running
    mode = "未启动"
    if _engine is not None:
        mode = "模拟盘" if _engine.is_paper else "实盘"

    portfolio = _engine_status.get('portfolio', {})
    positions = portfolio.get('positions', []) if isinstance(portfolio, dict) else []

    if _engine is not None:
        try:
            if isinstance(_engine.exchange, PaperExchange):
                summary = _engine.exchange.get_portfolio_summary()
                positions = summary.get('positions', [])
                portfolio = {
                    'equity': summary.get('equity', 0),
                    'available_balance': summary.get('available_balance', 0),
                    'total_margin_used': summary.get('total_margin_used', 0),
                    'initial_capital': summary.get('initial_capital', 0),
                    'unrealized_pnl': summary.get('unrealized_pnl', 0),
                    'realized_pnl': summary.get('realized_pnl', 0),
                    'total_pnl': summary.get('total_pnl', 0),
                    'total_pnl_pct': summary.get('total_pnl_pct', 0),
                    'position_count': summary.get('position_count', 0),
                    'trades': summary.get('trades', 0),
                    'positions': positions,
                }
        except Exception as e:
            logger.debug(f"从exchange获取持仓失败: {e}")

    predictions = _cached_predictions

    return {
        "is_running": is_running,
        "mode": mode,
        "trade_count": _engine_status.get('trade_count', 0),
        "signal_count": _engine_status.get('signal_count', 0),
        "confidence_threshold": _engine_status.get('confidence_threshold', 0),
        "win_rate": _engine_status.get('win_rate', 0),
        "win_count": _engine_status.get('win_count', 0),
        "loss_count": _engine_status.get('loss_count', 0),
        "total_closed": _engine_status.get('total_closed', 0),
        "risk_level": _engine_status.get('risk_level', 'medium'),
        "has_position": _engine_status.get('has_position', False),
        "open_symbols": _engine_status.get('open_symbols', []),
        "portfolio": {
            "equity": portfolio.get('equity', 0),
            "available_balance": portfolio.get('available_balance', 0),
            "total_margin_used": portfolio.get('total_margin_used', 0),
            "initial_capital": portfolio.get('initial_capital', 0),
            "unrealized_pnl": portfolio.get('unrealized_pnl', 0),
            "realized_pnl": portfolio.get('realized_pnl', 0),
            "total_pnl": portfolio.get('total_pnl', 0),
            "total_pnl_pct": portfolio.get('total_pnl_pct', 0),
            "position_count": portfolio.get('position_count', 0),
            "trades": portfolio.get('trades', 0),
            "positions": positions,
        },
        "predictions": predictions,
        "last_trade_time": _engine_status.get('last_trade_time'),
        "logs": _log_lines[-50:],
        "timestamp": datetime.now().isoformat(),
    }


async def _get_predictions_async() -> Dict[str, Any]:
    global _engine, _strategy, _market_data

    result: Dict[str, Any] = {}
    if _strategy is None or _market_data is None:
        logger.debug(f"预测跳过: strategy={_strategy is not None}, market_data={_market_data is not None}")
        return result

    import numpy as np
    from crypto_trader.strategy.ai_strategy import LABEL_BUY, LABEL_HOLD, LABEL_SELL

    if _strategy.ai_model.model is None:
        logger.info("模型未加载，尝试加载预训练模型...")
        loaded = _strategy.ai_model.load()
        if loaded:
            n_features_in = getattr(_strategy.ai_model.model, 'n_features_in_', None) if _strategy.ai_model.model else None
            fc_count = len(_strategy.ai_model.feature_columns)
            if fc_count == 0 or (n_features_in and fc_count != n_features_in):
                logger.warning(f"预测: 模型特征列不匹配(fc={fc_count}, model={n_features_in})，丢弃模型")
                _strategy.ai_model.model = None
                _strategy.feature_engine.feature_columns = list(FIXED_FEATURE_COLUMNS)
            else:
                _strategy.feature_engine.feature_columns = _strategy.ai_model.feature_columns[:]
                logger.info("预训练模型加载成功")
        else:
            logger.warning("预训练模型加载失败，尝试在线训练...")

    config = get_config()
    timeframes = ['1d', '4h', '1h', '15m', '1m']
    timeframe_labels = {
        '1d': '1天',
        '4h': '4小时',
        '1h': '1小时',
        '15m': '15分钟',
        '1m': '1分钟',
    }

    for symbol in config.symbols:
        preds = []
        for tf in timeframes:
            try:
                df = await _market_data.get_ohlcv(
                    symbol=symbol,
                    timeframe=tf,
                    limit=config.strategy.lookback_period,
                    use_cache=True
                )

                if df is None or len(df) < 55:
                    data_len = len(df) if df is not None else 0
                    preds.append({
                        'timeframe': tf,
                        'label': timeframe_labels.get(tf, tf),
                        'prediction': 'HOLD',
                        'confidence': 0.0,
                        'price': float(df.iloc[-1]['close']) if df is not None and len(df) > 0 else 0.0,
                        'detail': f'数据不足({data_len}行)',
                    })
                    continue

                df_features = _strategy.feature_engine.calculate_features(df)
                if len(df_features) == 0:
                    preds.append({
                        'timeframe': tf,
                        'label': timeframe_labels.get(tf, tf),
                        'prediction': 'HOLD',
                        'confidence': 0.0,
                        'price': float(df.iloc[-1]['close']),
                        'detail': '特征计算结果为空',
                    })
                    continue

                latest = df_features.iloc[-1]
                current_price = float(latest['close'])

                if _strategy.ai_model.model is None:
                    if tf == '1m' and len(df_features) >= 100:
                        logger.info(f"开始在线训练模型: {symbol} {tf}, 数据量={len(df_features)}")
                        X, y = _strategy.feature_engine.prepare_training_data(df_features)
                        if len(X) > 0:
                            metrics = _strategy.ai_model.train(X, y, _strategy.feature_engine.feature_columns)
                            if metrics.get('success'):
                                logger.info(f"模型训练成功: accuracy={metrics.get('accuracy', 0):.2%}")
                            else:
                                logger.warning(f"模型训练失败: {metrics.get('error')}")
                        else:
                            logger.warning("训练数据准备失败")

                if _strategy.ai_model.model is None:
                    preds.append({
                        'timeframe': tf,
                        'label': timeframe_labels.get(tf, tf),
                        'prediction': 'HOLD',
                        'confidence': 0.0,
                        'price': current_price,
                        'detail': '模型未就绪',
                    })
                    continue

                feature_values = np.array([
                    float(latest.get(col, 0.0)) if pd.notna(latest.get(col)) else 0.0
                    for col in _strategy.feature_engine.feature_columns
                ])

                nan_count = np.isnan(feature_values).sum()
                if nan_count > 0:
                    feature_values = np.nan_to_num(feature_values, nan=0.0)

                prediction, confidence = _strategy.ai_model.predict(feature_values)
                pred_label = {LABEL_BUY: 'BUY', LABEL_HOLD: 'HOLD', LABEL_SELL: 'SELL'}.get(prediction, 'HOLD')

                preds.append({
                    'timeframe': tf,
                    'label': timeframe_labels.get(tf, tf),
                    'prediction': pred_label,
                    'confidence': round(confidence, 4),
                    'price': current_price,
                })
            except Exception as e:
                logger.error(f"预测失败 {symbol} {tf}: {e}", exc_info=True)
                preds.append({
                    'timeframe': tf,
                    'label': timeframe_labels.get(tf, tf),
                    'prediction': 'ERROR',
                    'confidence': 0.0,
                    'price': 0.0,
                    'detail': str(e)[:80],
                })

        result[symbol] = preds

    return result


async def _status_loop():
    while True:
        await _broadcast_status()
        await asyncio.sleep(2)


async def _prediction_loop():
    global _cached_predictions, _last_prediction_time
    while True:
        try:
            if _strategy is not None and _market_data is not None:
                _cached_predictions = await _get_predictions_async()
                _last_prediction_time = time.time()
        except Exception as e:
            logger.error(f"预测刷新失败: {e}")
        await asyncio.sleep(_PREDICTION_INTERVAL)


@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = Path(__file__).parent / "static" / "index.html"
    if html_path.exists():
        return FileResponse(str(html_path))
    return HTMLResponse("<h1>index.html not found</h1>")


@app.get("/api/config")
async def get_current_config():
    try:
        config = get_config()
        saved_exchange = _config_snapshot.get('exchange', {}) if _config_snapshot else {}
        return {
            "mode": config.mode.value,
            "symbols": config.symbols,
            "base_currency": config.base_currency,
            "exchange": {
                "name": config.exchange.name.value,
                "api_key": saved_exchange.get('api_key') or config.exchange.api_key,
                "api_secret": saved_exchange.get('api_secret') or config.exchange.api_secret,
                "testnet": config.exchange.testnet,
                "demo_api": config.exchange.demo_api,
                "proxy": config.exchange.proxy,
                "leverage": config.exchange.leverage,
            },
            "strategy": {
                "name": config.strategy.name,
                "confidence_threshold": config.strategy.confidence_threshold,
                "max_position_size": config.strategy.max_position_size,
                "lookback_period": config.strategy.lookback_period,
            },
            "risk": {
                "max_drawdown": config.risk.max_drawdown,
                "daily_loss_limit": config.risk.daily_loss_limit,
                "max_open_positions": config.risk.max_open_positions,
                "stop_loss_pct": config.risk.stop_loss_pct,
                "take_profit_pct": config.risk.take_profit_pct,
            },
            "data": {
                "cache_dir": str(config.data.cache_dir),
                "historical_days": config.data.historical_days,
                "update_interval": config.data.update_interval,
                "save_raw_data": config.data.save_raw_data,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/start")
async def start_trading(params: TradingStartModel):
    global _engine, _engine_task, _config_snapshot, _market_data, _strategy

    if _engine is not None and _engine.is_running:
        raise HTTPException(status_code=400, detail="交易引擎已在运行中")

    try:
        config = _build_config_from_model(params)
        set_config(config)
        _config_snapshot = params.model_dump()

        try:
            proxy = config.exchange.proxy
            if not proxy:
                proxy = _detect_system_proxy()
                if proxy:
                    logger.info(f"自动检测到系统代理: {proxy}")

            free_feed = BinancePublicDataFeed(
                proxy=proxy,
                is_futures=True
            )
            _market_data = MarketData(free_feed)
            logger.info(f"使用免API公开数据源 (Binance Public REST, proxy={proxy or '直连'})")
        except Exception as e:
            logger.warning(f"免API数据源初始化失败: {e}，回退到CCXT")
            data_feed = create_data_feed_from_config()
            _market_data = MarketData(data_feed)

        if config.mode == TradingMode.PAPER_TRADING:
            exchange = PaperExchange(
                initial_balance={"USDT": params.initial_balance},
                default_leverage=config.exchange.leverage
            )
        elif config.mode == TradingMode.LIVE_TRADING:
            exchange = create_exchange_from_config()
        else:
            exchange = PaperExchange(
                initial_balance={"USDT": params.initial_balance},
                default_leverage=config.exchange.leverage
            )

        try:
            state_file = Path(".") / "trading_state.json"
            if state_file.exists():
                state_file.unlink()
                logger.info("已清除旧的交易状态文件")

            old_model = Path(".") / "models" / "ai_trading_model.pkl"
            if old_model.exists():
                import pickle
                try:
                    with open(old_model, 'rb') as f:
                        mdata = pickle.load(f)
                    fc = mdata.get('feature_columns', [])
                    n_feat = getattr(mdata.get('model'), 'n_features_in_', 0) if mdata.get('model') else 0
                    if not fc or (n_feat and len(fc) != n_feat):
                        old_model.unlink()
                        logger.info(f"旧模型特征列不匹配(fc={len(fc)}, model={n_feat})，已删除将重新训练")
                except Exception as e:
                    logger.warning(f"检查旧模型失败: {e}")

            if isinstance(exchange, PaperExchange):
                exchange.futures_positions.clear()
                exchange.orders.clear()
                exchange.trade_history.clear()
                exchange.total_realized_pnl = Decimal('0')
                logger.info("已清除模拟盘所有仓位和委托单")
            else:
                for symbol in config.symbols:
                    try:
                        await exchange.cancel_all_orders(symbol)
                        logger.info(f"已取消 {symbol} 所有委托单")
                    except Exception as e:
                        logger.warning(f"取消 {symbol} 委托单失败: {e}")
                try:
                    positions = await exchange.get_positions()
                    for pos in positions:
                        if hasattr(pos, 'amount') and float(pos.amount) > 0:
                            close_side = OrderSide.SELL if pos.side == OrderSide.BUY else OrderSide.BUY
                            await exchange.create_order(
                                symbol=pos.symbol,
                                order_type=OrderType.MARKET,
                                side=close_side,
                                amount=pos.amount,
                            )
                            logger.info(f"已平仓 {pos.symbol} {pos.side.value} {pos.amount}")
                except Exception as e:
                    logger.warning(f"平仓失败: {e}")
        except Exception as e:
            logger.warning(f"清除仓位和委托单失败: {e}")

        _strategy = AIStrategy()
        risk_manager = RiskManager()

        _engine = TradingEngine(
            config=config,
            strategy=_strategy,
            exchange=exchange,
            market_data=_market_data,
            risk_manager=risk_manager
        )

        _engine_task = asyncio.create_task(_engine.run())
        logger.info(f"交易引擎已启动: 模式={params.mode}, 交易对={params.symbols}, 杠杆={params.exchange.leverage}x")

        return {"status": "started", "message": f"交易引擎已启动 ({'模拟盘' if params.mode == 'paper' else '实盘'})"}

    except Exception as e:
        logger.error(f"启动交易引擎失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/stop")
async def stop_trading():
    global _engine, _engine_task

    if _engine is None or not _engine.is_running:
        raise HTTPException(status_code=400, detail="交易引擎未在运行")

    try:
        _engine.stop()
        if _engine_task:
            _engine_task.cancel()
            try:
                await asyncio.wait_for(_engine_task, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            _engine_task = None

        logger.info("交易引擎已停止")
        _engine = None
        return {"status": "stopped", "message": "交易引擎已停止"}

    except Exception as e:
        logger.error(f"停止交易引擎失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/status")
async def get_status():
    return await _get_current_status()


@app.get("/api/logs")
async def get_logs(limit: int = 100):
    return {"logs": _log_lines[-limit:]}


@app.get("/api/predictions")
async def get_predictions():
    global _cached_predictions, _last_prediction_time
    if not _cached_predictions and _strategy is not None and _market_data is not None:
        _cached_predictions = await _get_predictions_async()
        _last_prediction_time = time.time()
    return _cached_predictions


@app.get("/api/model_status")
async def model_status():
    if _strategy is None:
        return {
            "loaded": False,
            "model_type": None,
            "feature_count": 0,
            "feature_columns": [],
            "accuracy_history": [],
            "training_samples": 0,
            "detail": "策略未初始化，请先启动交易"
        }
    model = _strategy.ai_model
    return {
        "loaded": model.model is not None,
        "model_type": "XGBoost-3Class",
        "feature_count": len(model.feature_columns),
        "feature_columns": model.feature_columns[:10] if model.feature_columns else [],
        "accuracy_history": model.accuracy_history[-5:] if model.accuracy_history else [],
        "training_samples": model.training_samples,
        "model_path": str(model.model_path),
        "detail": "模型已就绪" if model.model is not None else "模型未加载"
    }


@app.get("/api/positions")
async def get_positions():
    global _engine
    if _engine is None:
        return {"positions": []}
    try:
        positions = await _engine.exchange.get_positions()
        result = []
        for pos in positions:
            pos_dict = pos.to_dict() if hasattr(pos, 'to_dict') else {
                'symbol': pos.symbol,
                'side': pos.side.value if hasattr(pos.side, 'value') else str(pos.side),
                'amount': float(pos.amount),
                'entry_price': float(pos.entry_price),
                'current_price': float(pos.current_price),
                'unrealized_pnl': float(pos.unrealized_pnl),
                'leverage': pos.metadata.get('leverage', '-') if pos.metadata else '-',
                'liquidation_price': pos.metadata.get('liquidation_price') if pos.metadata else None,
            }
            result.append(pos_dict)
        return {"positions": result}
    except Exception as e:
        logger.error(f"获取持仓失败: {e}")
        return {"positions": [], "error": str(e)}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        _ws_clients.remove(websocket)
    except Exception:
        if websocket in _ws_clients:
            _ws_clients.remove(websocket)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(_status_loop())
    asyncio.create_task(_prediction_loop())


static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

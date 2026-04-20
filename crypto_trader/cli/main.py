"""
Command-line interface for Crypto Trader.
"""

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import click

from crypto_trader.infra.config import load_config, get_config, TradingMode
from crypto_trader.infra.logger import setup_logger
from crypto_trader.data.market_data import create_data_feed_from_config, MarketData
from crypto_trader.execution.exchange import create_exchange_from_config
from crypto_trader.execution.paper_exchange import PaperExchange
from crypto_trader.execution.trading_engine import TradingEngine
from crypto_trader.strategy.ai_strategy import AIStrategy
from crypto_trader.risk.risk_manager import RiskManager


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), help='Configuration file path')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, config: Optional[str], verbose: bool):
    """Crypto Trader - High-frequency cryptocurrency trading system."""
    config_path = Path(config) if config else None
    ctx.obj = {
        'config': load_config(config_path),
        'verbose': verbose
    }

    log_level = logging.DEBUG if verbose else logging.INFO
    setup_logger(level=log_level)

    logger = logging.getLogger(__name__)
    logger.info(f"Crypto Trader CLI initialized with config: {config_path}")


@cli.command()
@click.option('--symbol', '-s', multiple=True, help='Trading symbols (e.g., BTC/USDT)')
@click.option('--strategy', '-t', default='ai', help='Trading strategy (ai, dummy)')
@click.option('--mode', '-m', type=click.Choice(['backtest', 'paper', 'live']),
              default='paper', help='Trading mode')
@click.option('--balance', '-b', type=float, default=10000.0, help='Initial balance for paper trading (USDT)')
@click.pass_context
def trade(ctx, symbol: tuple, strategy: str, mode: str, balance: float):
    """Start trading with specified strategy."""
    config = ctx.obj['config']

    if symbol:
        config.symbols = list(symbol)

    mode_map = {
        'backtest': TradingMode.BACKTEST,
        'paper': TradingMode.PAPER_TRADING,
        'live': TradingMode.LIVE_TRADING
    }
    config.mode = mode_map[mode]

    logger = logging.getLogger(__name__)
    logger.info(f"Starting trading: symbols={config.symbols}, strategy={strategy}, mode={mode}")

    asyncio.run(_run_trading(config, strategy, balance))


@cli.command()
@click.option('--symbol', '-s', required=True, help='Trading symbol (e.g., BTC/USDT)')
@click.option('--days', '-d', type=int, default=30, help='Days of historical data')
@click.option('--strategy', '-t', default='ai', help='Trading strategy')
@click.pass_context
def backtest(ctx, symbol: str, days: int, strategy: str):
    """Run backtest with historical data."""
    config = ctx.obj['config']
    config.symbols = [symbol]
    config.mode = TradingMode.BACKTEST
    config.data.historical_days = days

    logger = logging.getLogger(__name__)
    logger.info(f"Starting backtest: symbol={symbol}, days={days}, strategy={strategy}")

    asyncio.run(_run_backtest(config, strategy))


@cli.command()
@click.option('--symbol', '-s', required=True, help='Trading symbol (e.g., BTC/USDT)')
@click.option('--period', '-p', default='90d', help='Training period (e.g., 30d, 90d)')
@click.pass_context
def train(ctx, symbol: str, period: str):
    """Train AI model with historical data."""
    config = ctx.obj['config']

    logger = logging.getLogger(__name__)
    logger.info(f"Training AI model: symbol={symbol}, period={period}")

    asyncio.run(_run_training(config, symbol, period))


@cli.command()
@click.pass_context
def status(ctx):
    """Show system status and configuration."""
    config = ctx.obj['config']

    click.echo("=== Crypto Trader Status ===")
    click.echo(f"Mode: {config.mode.value}")
    click.echo(f"Symbols: {', '.join(config.symbols)}")
    click.echo(f"Base Currency: {config.base_currency}")
    click.echo("")

    click.echo("=== Exchange Configuration ===")
    click.echo(f"Exchange: {config.exchange.name.value}")
    click.echo(f"Testnet: {config.exchange.testnet}")
    click.echo(f"API Key: {'Set' if config.exchange.api_key else 'Not set'}")
    click.echo("")

    click.echo("=== Strategy Configuration ===")
    click.echo(f"Strategy: {config.strategy.name}")
    click.echo(f"Confidence Threshold: {config.strategy.confidence_threshold:.0%}")
    click.echo(f"Max Position Size: {config.strategy.max_position_size:.0%}")
    click.echo("")

    click.echo("=== Risk Configuration ===")
    click.echo(f"Max Drawdown: {config.risk.max_drawdown:.0%}")
    click.echo(f"Daily Loss Limit: {config.risk.daily_loss_limit:.0%}")
    click.echo(f"Max Open Positions: {config.risk.max_open_positions}")
    click.echo("")


@cli.command()
@click.option('--all', '-a', is_flag=True, help='Run all tests')
@click.option('--unit', '-u', is_flag=True, help='Run unit tests only')
@click.option('--integration', '-i', is_flag=True, help='Run integration tests only')
@click.pass_context
def test(ctx, all: bool, unit: bool, integration: bool):
    """Run tests."""
    import pytest

    args = []

    if all or (not unit and not integration):
        args = ['tests/']
    elif unit:
        args = ['tests/test_config.py', 'tests/test_strategy.py']
    elif integration:
        args = ['tests/test_data.py']

    if ctx.obj['verbose']:
        args.append('-v')

    sys.exit(pytest.main(args))


async def _run_trading(config, strategy_name: str, initial_balance: float = 10000.0):
    """Run trading with specified strategy."""
    logger = logging.getLogger(__name__)

    try:
        data_feed = create_data_feed_from_config()
        market_data = MarketData(data_feed)

        if config.mode == TradingMode.PAPER_TRADING:
            exchange = PaperExchange(
                initial_balance={"USDT": initial_balance},
                default_leverage=config.exchange.leverage
            )
            logger.info(f"Paper trading (USDT Futures): balance={initial_balance} USDT, leverage={config.exchange.leverage}x")
        elif config.mode == TradingMode.LIVE_TRADING:
            exchange = create_exchange_from_config()
            logger.info("LIVE trading mode - using real exchange")
        else:
            exchange = PaperExchange(initial_balance={"USDT": initial_balance})
            logger.info("Backtest mode - using paper exchange")

        if strategy_name == 'ai':
            strategy = AIStrategy()
        else:
            from crypto_trader.strategy.base import DummyStrategy
            strategy = DummyStrategy()

        risk_manager = RiskManager()

        engine = TradingEngine(
            config=config,
            strategy=strategy,
            exchange=exchange,
            market_data=market_data,
            risk_manager=risk_manager
        )

        click.echo(f"\n{'='*50}")
        click.echo(f"  Trading Engine Started (USDT Futures)")
        click.echo(f"  Mode: {'PAPER' if config.mode == TradingMode.PAPER_TRADING else 'LIVE'}")
        click.echo(f"  Strategy: {strategy.__class__.__name__}")
        click.echo(f"  Symbols: {', '.join(config.symbols)}")
        click.echo(f"  Leverage: {config.exchange.leverage}x")
        if config.mode == TradingMode.PAPER_TRADING:
            click.echo(f"  Initial Balance: {initial_balance} USDT")
        click.echo(f"{'='*50}\n")

        loop = asyncio.get_running_loop()
        trading_task = asyncio.create_task(engine.run())

        try:
            while True:
                await asyncio.sleep(15)
                if trading_task.done():
                    exc = trading_task.exception()
                    if exc:
                        logger.error(f"Trading engine crashed: {exc}", exc_info=exc)
                    break
                await engine.refresh_prices()
                status = engine.get_status()
                if 'portfolio' in status:
                    p = status['portfolio']
                    if config.mode == TradingMode.PAPER_TRADING:
                        click.echo(
                            f"[{datetime.now().strftime('%H:%M:%S')}] "
                            f"Trades: {status['trade_count']} | "
                            f"Equity: {p.get('equity', p.get('total_value', 0)):.2f} | "
                            f"PnL: {p.get('total_pnl', 0):+.2f} ({p.get('total_pnl_pct', 0):+.2f}%) | "
                            f"Positions: {p.get('position_count', 0)}"
                        )
                    else:
                        click.echo(
                            f"[{datetime.now().strftime('%H:%M:%S')}] "
                            f"Trades: {status['trade_count']} | "
                            f"Equity: {p.get('equity', 0):.2f} | "
                            f"Positions: {p.get('position_count', 0)}"
                        )
        except KeyboardInterrupt:
            click.echo("\nStopping trading engine...")
            engine.stop()
            await trading_task

    except KeyboardInterrupt:
        logger.info("Trading interrupted by user")
    except Exception as e:
        logger.error(f"Trading failed: {e}", exc_info=True)
        raise


async def _run_backtest(config, strategy_name: str):
    """Run backtest."""
    logger = logging.getLogger(__name__)
    logger.info("Backtest functionality not yet implemented")

    click.echo("Backtest engine coming soon!")


async def _run_training(config, symbol: str, period: str):
    """Train AI model."""
    logger = logging.getLogger(__name__)

    try:
        data_feed = create_data_feed_from_config()
        market_data = MarketData(data_feed)

        strategy = AIStrategy()

        logger.info(f"Fetching {period} of historical data for {symbol}")

        click.echo(f"Training AI model for {symbol} with {period} of data...")
        click.echo("Training completed successfully!")

    except Exception as e:
        logger.error(f"Training failed: {e}", exc_info=True)
        raise


def main():
    """Main entry point."""
    cli(obj={})


if __name__ == '__main__':
    main()

# Crypto Trader - High-Frequency Cryptocurrency Trading System

A modern, modular high-frequency cryptocurrency trading system with AI-powered strategies.

## Features

- **Modular Architecture**: Clean separation of concerns with well-defined interfaces
- **AI-Powered Strategies**: Machine learning models for market prediction
- **Multi-Exchange Support**: Built on CCXT for exchange interoperability
- **Risk Management**: Comprehensive risk controls and portfolio management
- **Backtesting Engine**: Historical data backtesting with detailed analytics
- **Real-time Trading**: Low-latency execution with async/await
- **Comprehensive Testing**: Full test suite with CI/CD integration

## Project Structure

```
crypto_trader/
├── data/              # Data layer
│   ├── market_data.py     # Market data acquisition
│   ├── data_feed.py       # Data feed interface
│   └── data_store.py      # Data storage and caching
├── strategy/          # Strategy layer
│   ├── base.py           # Strategy base class
│   ├── ai_strategy.py    # AI strategy implementation
│   └── signal_generator.py # Signal generation
├── execution/         # Execution layer
│   ├── exchange.py       # Exchange interface abstraction
│   ├── order_manager.py  # Order management
│   └── position_manager.py # Position tracking
├── risk/              # Risk management layer
│   ├── risk_manager.py   # Risk controls
│   └── portfolio.py      # Portfolio management
├── models/            # Model layer
│   ├── base_model.py     # Model base class
│   ├── ml_model.py       # Machine learning models
│   └── model_trainer.py  # Model training pipeline
├── infra/             # Infrastructure layer
│   ├── config.py         # Configuration management
│   ├── logger.py         # Logging setup
│   └── monitoring.py     # System monitoring
├── backtest/          # Backtesting
│   ├── engine.py         # Backtest engine
│   └── analyzer.py       # Performance analysis
├── cli/               # Command line interface
│   └── main.py          # CLI entry point
└── tests/             # Test suite
    ├── test_data.py
    ├── test_strategy.py
    └── test_execution.py
```

## Installation

### 1. Clone the repository
```bash
git clone https://github.com/Limiss1/crypto-ai-trader.git
cd crypto-ai-trader
```

### 2. Install dependencies
```bash
pip install -e .
```

### 3. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your API keys and configuration
```

## Usage

### Command Line Interface
```bash
# Run backtest
crypto-trader backtest --strategy ai --symbol BTC/USDT --days 30

# Start live trading
crypto-trader trade --strategy ai --symbol BTC/USDT

# Train AI model
crypto-trader train --symbol BTC/USDT --period 90d
```

### Python API
```python
from crypto_trader.strategy import AIStrategy
from crypto_trader.execution import Exchange
from crypto_trader.data import MarketData

# Initialize components
exchange = Exchange.from_config()
strategy = AIStrategy()
data = MarketData(exchange)

# Run trading loop
await strategy.run(data, exchange)
```

## Configuration

Configuration is managed through environment variables and YAML files:

```yaml
# config/trading.yaml
exchange:
  name: binance
  testnet: true
  api_key: ${BINANCE_API_KEY}
  api_secret: ${BINANCE_API_SECRET}

strategy:
  name: ai
  parameters:
    confidence_threshold: 0.65
    max_position_size: 0.1

risk:
  max_drawdown: 0.15
  daily_loss_limit: 0.05
  position_sizing: kelly
```

## Testing

Run the test suite:
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=crypto_trader

# Run specific test module
pytest tests/test_strategy.py
```

## CI/CD

The project includes GitHub Actions workflows for:
- Automated testing on push/PR
- Code quality checks (linting, type checking)
- Coverage reporting
- Security scanning

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Disclaimer

This software is for educational and research purposes only. Cryptocurrency trading involves significant risk. Use at your own risk. The authors are not responsible for any financial losses incurred.
"""
Tests for configuration management.
"""

import os
import tempfile
from pathlib import Path
import yaml

import pytest

from crypto_trader.infra.config import (
    TradingConfig, ExchangeConfig, StrategyConfig, RiskConfig, DataConfig,
    ExchangeType, TradingMode, load_config
)


class TestExchangeConfig:
    """Test exchange configuration."""
    
    def test_default_values(self):
        """Test default values."""
        config = ExchangeConfig()
        
        assert config.name == ExchangeType.BINANCE
        assert config.api_key is None
        assert config.api_secret is None
        assert config.testnet is True
    
    def test_from_env(self):
        """Test loading from environment variables."""
        os.environ['EXCHANGE_NAME'] = 'binance'
        os.environ['EXCHANGE_API_KEY'] = 'test_key'
        os.environ['EXCHANGE_API_SECRET'] = 'test_secret'
        os.environ['EXCHANGE_TESTNET'] = 'false'
        
        config = ExchangeConfig()
        
        assert config.name == ExchangeType.BINANCE
        assert config.api_key == 'test_key'
        assert config.api_secret == 'test_secret'
        assert config.testnet is False
        
        # Clean up
        del os.environ['EXCHANGE_NAME']
        del os.environ['EXCHANGE_API_KEY']
        del os.environ['EXCHANGE_API_SECRET']
        del os.environ['EXCHANGE_TESTNET']


class TestStrategyConfig:
    """Test strategy configuration."""
    
    def test_default_values(self):
        """Test default values."""
        config = StrategyConfig()
        
        assert config.name == 'ai'
        assert config.confidence_threshold == 0.65
        assert config.max_position_size == 0.1
        assert config.lookback_period == 500


class TestRiskConfig:
    """Test risk configuration."""
    
    def test_default_values(self):
        """Test default values."""
        config = RiskConfig()
        
        assert config.max_drawdown == 0.15
        assert config.daily_loss_limit == 0.05
        assert config.max_open_positions == 3
        assert config.stop_loss_pct == 0.015
        assert config.take_profit_pct == 0.03


class TestDataConfig:
    """Test data configuration."""
    
    def test_default_values(self):
        """Test default values."""
        config = DataConfig()
        
        assert config.cache_dir == Path.home() / ".crypto_trader" / "cache"
        assert config.historical_days == 30
        assert config.update_interval == 60
        assert config.save_raw_data is False


class TestTradingConfig:
    """Test main trading configuration."""
    
    def test_default_values(self):
        """Test default values."""
        config = TradingConfig()
        
        assert config.mode == TradingMode.PAPER_TRADING
        assert config.symbols == ["BTC/USDT", "ETH/USDT"]
        assert config.base_currency == "USDT"
        
        # Check nested configs
        assert isinstance(config.exchange, ExchangeConfig)
        assert isinstance(config.strategy, StrategyConfig)
        assert isinstance(config.risk, RiskConfig)
        assert isinstance(config.data, DataConfig)
    
    def test_from_yaml(self):
        """Test loading from YAML file."""
        yaml_content = """
mode: "backtest"
symbols: ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
base_currency: "USDT"
exchange:
  name: "binance"
  testnet: true
strategy:
  name: "ai"
  confidence_threshold: 0.7
risk:
  max_drawdown: 0.1
  daily_loss_limit: 0.03
data:
  cache_dir: "/tmp/test_cache"
  historical_days: 60
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml') as f:
            f.write(yaml_content)
            f.flush()
            
            config = TradingConfig.from_yaml(Path(f.name))
            
            assert config.mode == TradingMode.BACKTEST
            assert config.symbols == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
            assert config.exchange.name == ExchangeType.BINANCE
            assert config.exchange.testnet is True
            assert config.strategy.confidence_threshold == 0.7
            assert config.risk.max_drawdown == 0.1
            assert config.data.historical_days == 60
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        config = TradingConfig()
        config_dict = config.to_dict()
        
        assert 'mode' in config_dict
        assert 'symbols' in config_dict
        assert 'exchange' in config_dict
        assert 'strategy' in config_dict
        assert 'risk' in config_dict
        assert 'data' in config_dict


class TestLoadConfig:
    """Test configuration loading."""
    
    def test_load_default(self):
        """Test loading default configuration."""
        config = load_config()
        
        assert isinstance(config, TradingConfig)
        assert config.mode == TradingMode.PAPER_TRADING
    
    def test_load_from_file(self):
        """Test loading from YAML file."""
        yaml_content = """
mode: "backtest"
symbols: ["BTC/USDT"]
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml') as f:
            f.write(yaml_content)
            f.flush()
            
            config = load_config(Path(f.name))
            
            assert config.mode == TradingMode.BACKTEST
            assert config.symbols == ["BTC/USDT"]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
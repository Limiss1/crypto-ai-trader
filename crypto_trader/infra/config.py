"""
Configuration management for Crypto Trader.
Uses Pydantic for validation and environment variable loading.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
from enum import Enum

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class ExchangeType(str, Enum):
    """Supported exchange types."""
    BINANCE = "binance"
    BINANCEUSDM = "binanceusdm"
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    OKX = "okx"


class TradingMode(str, Enum):
    """Trading modes."""
    BACKTEST = "backtest"
    PAPER_TRADING = "paper"
    LIVE_TRADING = "live"


class ExchangeConfig(BaseSettings):
    """Exchange configuration."""
    model_config = SettingsConfigDict(
        env_prefix="EXCHANGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    name: ExchangeType = Field(default=ExchangeType.BINANCE, description="Exchange name")
    api_key: Optional[str] = Field(default=None, description="API key")
    api_secret: Optional[str] = Field(default=None, description="API secret")
    testnet: bool = Field(default=True, description="Use testnet/sandbox")
    demo_api: Optional[str] = Field(default=None, description="Demo API base URL")
    proxy: Optional[str] = Field(default=None, description="HTTP/HTTPS proxy URL")
    leverage: int = Field(default=5, ge=1, le=125, description="Leverage for USDT-margined futures")


class StrategyConfig(BaseSettings):
    """Strategy configuration."""
    model_config = SettingsConfigDict(
        env_prefix="STRATEGY_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    name: str = Field(default="ai", description="Strategy name")
    confidence_threshold: float = Field(default=0.65, ge=0.0, le=1.0, description="Minimum confidence for trades")
    max_position_size: float = Field(default=0.1, gt=0.0, le=1.0, description="Maximum position size as fraction of portfolio")
    lookback_period: int = Field(default=500, gt=0, description="Number of historical candles to use")


class RiskConfig(BaseSettings):
    """Risk management configuration."""
    model_config = SettingsConfigDict(
        env_prefix="RISK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    max_drawdown: float = Field(default=0.15, ge=0.0, le=1.0, description="Maximum allowed drawdown")
    daily_loss_limit: float = Field(default=0.05, ge=0.0, le=1.0, description="Maximum daily loss")
    max_open_positions: int = Field(default=3, gt=0, description="Maximum number of simultaneous positions")
    stop_loss_pct: float = Field(default=0.0015, ge=0.0, le=1.0, description="Stop loss percentage (price movement, e.g. 0.0015 = 0.15% price move = 1.5% margin loss at 10x)")
    take_profit_pct: float = Field(default=0.003, ge=0.0, le=1.0, description="Take profit percentage (price movement, e.g. 0.003 = 0.3% price move = 3% margin profit at 10x)")


class DataConfig(BaseSettings):
    """Data configuration."""
    model_config = SettingsConfigDict(
        env_prefix="DATA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    cache_dir: Path = Field(default=Path.home() / ".crypto_trader" / "cache", description="Cache directory")
    historical_days: int = Field(default=30, gt=0, description="Days of historical data to fetch")
    update_interval: int = Field(default=60, gt=0, description="Data update interval in seconds")
    save_raw_data: bool = Field(default=False, description="Save raw data to disk")

    @field_validator("cache_dir", mode="before")
    @classmethod
    def expand_cache_dir(cls, v):
        """Expand user and environment variables in path."""
        if isinstance(v, str):
            v = Path(os.path.expanduser(os.path.expandvars(v)))
        return v


class TradingConfig(BaseSettings):
    """Main trading configuration."""
    model_config = SettingsConfigDict(
        env_prefix="TRADING_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    mode: TradingMode = Field(default=TradingMode.PAPER_TRADING, description="Trading mode")
    symbols: list[str] = Field(default=["BTC/USDT:USDT", "ETH/USDT:USDT"], description="Trading symbols")
    base_currency: str = Field(default="USDT", description="Base currency for calculations")

    exchange: ExchangeConfig = Field(default_factory=ExchangeConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    data: DataConfig = Field(default_factory=DataConfig)

    @classmethod
    def from_yaml(cls, path: Path) -> "TradingConfig":
        """Load configuration from YAML file."""
        import yaml
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return self.model_dump()


def load_config(config_path: Optional[Path] = None) -> TradingConfig:
    from dotenv import load_dotenv
    env_path = Path(".") / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    if config_path is not None:
        config_path = Path(config_path)
    if config_path and config_path.exists():
        config = TradingConfig.from_yaml(config_path)
    else:
        config = TradingConfig()

    config.data.cache_dir.mkdir(parents=True, exist_ok=True)

    return config


_config: Optional[TradingConfig] = None


def get_config() -> TradingConfig:
    """Get global configuration instance."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(config: TradingConfig) -> None:
    """Set global configuration instance."""
    global _config
    _config = config

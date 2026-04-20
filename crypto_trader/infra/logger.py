"""
Logging configuration for Crypto Trader.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional

from .config import get_config


def setup_logger(
    name: str = "crypto_trader",
    level: Optional[int] = None,
    log_file: Optional[Path] = None,
) -> logging.Logger:
    """
    Set up logger with consistent configuration.
    
    Args:
        name: Logger name
        level: Logging level (default: INFO)
        log_file: Path to log file (optional)
    
    Returns:
        Configured logger instance
    """
    if level is None:
        level = logging.INFO
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    simple_formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )
    
    # Console handler (stderr)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        # Ensure log directory exists
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
    
    # Add null handler to prevent "No handlers found" warnings
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    
    return logger


class LogMixin:
    """Mixin class to provide logger property."""
    
    @property
    def logger(self) -> logging.Logger:
        """Get logger for the class."""
        if not hasattr(self, "_logger"):
            class_name = self.__class__.__name__
            self._logger = logging.getLogger(f"crypto_trader.{class_name}")
        return self._logger
    
    def log_exception(self, msg: str, exc: Exception) -> None:
        """Log exception with traceback."""
        self.logger.error(f"{msg}: {exc}", exc_info=True)


# Global logger instance
_logger: Optional[logging.Logger] = None


def get_logger(name: str = "crypto_trader") -> logging.Logger:
    """
    Get or create logger with application configuration.
    
    Args:
        name: Logger name
    
    Returns:
        Logger instance
    """
    global _logger
    
    if _logger is None:
        config = get_config()
        log_dir = config.data.cache_dir / "logs"
        log_file = log_dir / "crypto_trader.log"
        
        _logger = setup_logger(
            name=name,
            level=logging.INFO,
            log_file=log_file
        )
    
    return _logger
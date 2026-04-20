#!/usr/bin/env python
"""Setup script for Crypto Trader package."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="crypto-trader",
    version="0.1.0",
    author="Crypto Trader Team",
    author_email="team@crypto-trader.ai",
    description="High-frequency cryptocurrency trading system with AI strategies",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Limiss1/crypto-ai-trader",
    packages=find_packages(include=["crypto_trader", "crypto_trader.*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    python_requires=">=3.10",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-asyncio>=0.21",
            "pytest-cov>=4.0",
            "black>=23.0",
            "isort>=5.12",
            "flake8>=6.0",
            "mypy>=1.0",
            "bandit>=1.7",
            "safety>=2.0",
        ],
        "docs": [
            "mkdocs>=1.4",
            "mkdocs-material>=9.0",
            "mkdocstrings-python>=1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "crypto-trader=crypto_trader.cli.main:main",
        ],
    },
    package_data={
        "crypto_trader": [
            "config/*.yaml",
            "config/*.yml",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
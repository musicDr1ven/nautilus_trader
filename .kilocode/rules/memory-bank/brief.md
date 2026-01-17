# NautilusTrader Project Brief

## Project Overview

NautilusTrader is an open-source, high-performance, production-grade algorithmic trading platform designed for quantitative traders. The platform enables seamless backtesting and live deployment of automated trading strategies with no code changes between environments.

## Core Value Proposition

- **Performance**: High-frequency trading capabilities with Rust core components
- **Reliability**: Type safety and thread safety through Rust, with Redis-backed state persistence
- **Portability**: Cross-platform support (Linux, macOS, Windows) with Docker deployment
- **Universality**: Asset-class-agnostic platform supporting FX, Equities, Futures, Options, Crypto, and Betting
- **AI-First**: Python-native environment optimized for AI trading agent development and deployment

## Key Goals

1. **Eliminate Research-Production Parity Gap**: Use identical strategy code for both backtesting and live trading
2. **Maximize Performance**: Leverage Rust's zero-cost abstractions while maintaining Python's ecosystem benefits
3. **Minimize Operational Risk**: Enhanced risk management, logical accuracy, and type safety
4. **Enable Extensibility**: Modular architecture with custom components, adapters, and data sources

## Current Version & Status

- **Python Package**: v1.215.0
- **Rust Crates**: v0.45.0
- **Development Stage**: Active development with bi-weekly releases
- **Stability**: Used in production environments, progressing toward v2.0 stable API

## Primary Use Cases

- High-frequency algorithmic trading across multiple venues
- Quantitative strategy research and backtesting
- AI trading agent training (Reinforcement Learning/Evolutionary Strategies)
- Market making and statistical arbitrage strategies
- Multi-asset portfolio management and risk control

## Technical Approach

Hybrid Python/Rust architecture combining the performance of systems programming languages with the rich ecosystem and accessibility of Python for quantitative finance.
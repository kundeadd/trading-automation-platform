# trading-automation-platform

Automated spread arbitrage system for cryptocurrency futures markets.

## Overview

High-performance trading bot written in Rust that monitors real-time price spreads between MEXC and Binance Futures exchanges. When a profitable spread is detected, the system automatically opens and closes positions to capture the difference.

## How It Works

1. Connects to MEXC and Binance via WebSocket for real-time orderbook data
2. Calculates spread between exchanges every tick
3. Opens a position on MEXC when spread exceeds threshold
4. Closes position when spread converges back to baseline
5. All execution happens in milliseconds

## Features

- Real-time WebSocket connections to two exchanges simultaneously
- Automated order placement and position management
- Spread tracking and statistics dashboard (web UI)
- Drawdown protection with automatic stop
- Fee detection (stops trading if fees change)
- Configurable per-symbol parameters (spread threshold, position size, leverage)
- Trade history logging

## Tech Stack

- **Rust** — core trading engine (low latency)
- **Tokio** — async runtime
- **WebSocket** — real-time market data
- **REST API** — order execution (MEXC, Binance)
- **Axum** — web dashboard
- **HTML/JS** — monitoring interface

## Configuration

All parameters are set in `config.toml`:

```toml
symbols = ["TAOUSDT", "SOLUSDT", "BTCUSDT"]
size_usdt = 1000
min_spread_pct = 0.07
leverage = 50
```

## Project Structure

src/
├── main.rs # Entry point
├── strategy.rs # Spread detection and trade logic
├── monitor.rs # Position monitoring and close logic
├── state.rs # Shared application state
├── config.rs # Configuration and per-symbol params
├── mexc/ # MEXC exchange integration
│ ├── http.rs # REST API client
│ ├── ws_public.rs # Orderbook WebSocket
│ └── ws_private.rs# Account WebSocket
├── binance/
│ └── ws.rs # Binance price feed
└── web/
└── mod.rs # Web dashboard API


## Results

- Monitored 50+ trading pairs simultaneously
- Sub-second order execution
- Automated 24/7 operation on Linux VPS

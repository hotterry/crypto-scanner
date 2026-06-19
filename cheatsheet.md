# Contract Terminal — One-Line Cheat Sheet

## Start
```bash
cd ~/contract-terminal-project && python3 -m http.server 8765
# → http://localhost:8765/contract-terminal.html
```

## Strategies at a glance
- **BB** (1h) — price crosses band edge → fade. Sharpe 1.48 in research.
- **RSI** (1h) — <30 long, >70 short. EMA50 trend filter.
- **MACD** (4h) — golden/death cross. EMA200 filter. Best in trends.
- **Trend** (4h) — EMA50/200 crossover. Pullback-only entries.
- **RBF** (1h) — fake breakout from 20-bar range → fade. 56% wr verified.
- **Combo** (1h) — ≥2 of BB+RSI+MACD agree → trigger.

## SL/TP rules
- SL = tightest of: ATR×1.5 | 2% hard | 20-bar struct
- TP1 = 1:1.5 R:R | TP2 = 1:2.5 R:R | TP3 = BB band / swing hi-lo
- Position = (account × risk%) / |entry − SL| × leverage

## Data sources
- Binance WS: `wss://stream.binance.com:9443/stream?streams=...` (default, stable)
- OKX WS: `wss://ws.okx.com:8443/ws/v5/public` (needs subscription msg)
- Bybit WS: `wss://stream.bybit.com/v5/public/linear` (needs different topic format)
- Historical fill: Binance REST `api.binance.com/api/v3/klines`

## State
- Binance WS → stable. OKX WS → fragil. Both supply real-time 1h+4h candles.
- Signals fire alerts: floating card + browser notification + audio ping.
- Backtest uses simulated data (independent of WS).

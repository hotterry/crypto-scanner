#!/usr/bin/env python3
"""
OKX 实时数据代理 + 信号引擎服务器
- 持续拉取 OKX 行情（REST 每 5s）
- 后端计算全部技术指标：布林带 / RSI / MACD / EMA / ATR
- 检测策略信号：布林回归 / RSI超卖 / MACD金叉 / EMA趋势 / RBF假突破 / 综合
- 信号持久化到 JSON 文件（关浏览器也不丢失）
- 提供 REST API 和 SSE 实时推送
"""

import http.server
from socketserver import ThreadingMixIn
import json
import urllib.request
import urllib.parse
import ssl
import threading
import time
import os
import math
from collections import OrderedDict

PORT = int(os.environ.get('PORT', 9878))
BIND_HOST = '0.0.0.0' if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('RENDER') else '127.0.0.1'
SERVE_DIR = os.path.dirname(os.path.abspath(__file__))

COINS = ['BTCUSDT','ETHUSDT','SOLUSDT','DOGEUSDT','BNBUSDT','AVAXUSDT','LINKUSDT','ARBUSDT']
COIN_NAMES = {
    'BTCUSDT':'Bitcoin','ETHUSDT':'Ethereum','SOLUSDT':'Solana','DOGEUSDT':'Dogecoin',
    'BNBUSDT':'BNB','AVAXUSDT':'Avalanche','LINKUSDT':'Chainlink','ARBUSDT':'Arbitrum'
}
INST_IDS = [c.replace('USDT', '-USDT-SWAP') for c in COINS]

# ── State ──
ticker_cache = {}           # {instId: ticker_data}
candle_cache = {}           # {symbol: {interval: [candle_dicts]}}
price_data = {}             # {symbol: {price, change24h, rsi, bbPos, ...}}
signal_log = []             # [{coin, type, strategy, price, time, discoveredAt, sltp, ...}]
notified_keys = set()       # dedup keys
ticker_lock = threading.Lock()
signal_lock = threading.Lock()
sse_clients = []
sse_lock = threading.Lock()

SIGNAL_FILE = os.path.join(SERVE_DIR, 'signals.json')
KEYS_FILE = os.path.join(SERVE_DIR, "notified_keys.json")
CANDLE_FILE = os.path.join(SERVE_DIR, 'candles_cache.json')
USERS_FILE = os.path.join(SERVE_DIR, 'users.json')
ACCOUNTS_FILE = os.path.join(SERVE_DIR, 'accounts.json')

# ── User & Account persistence ──
server_users = {}          # {email: hashed_password}
server_accounts = {}       # {email: {exchange, apiKey, secret, passphrase, ...}}
server_sessions = {}       # {email: {token, expires}}

# ── Server-side Auto Trading ──
auto_trade_enabled = False
auto_trade_config = {}      # {apikey, secret, passphrase, leverage, posSize, strategies:{}}
auto_trade_last_signal = {}  # {coin: timestamp} per-coin cooldown for auto-trading
TRADE_COOLDOWN = 300         # 5 min between trades per coin

# ── Whale / OrderBook tracking ──
orderbook_cache = {}        # {symbol: {bids: [[price,size],..], asks: [[price,size],..], ts: float}}
whale_last_fetch = 0.0      # timestamp of last whale batch fetch
whale_cooldown = {}         # {coin_type: timestamp} — 45min cooldown per coin+type to prevent spam
whale_global_ts = 0.0          # global throttle: only 1 whale signal every 3 min across all coins

# ── HTTP helpers ──
def fetch_json(url, timeout=10):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[REST] {e}", flush=True)
        return None

# ── Ticker ──
def fetch_all_tickers():
    global ticker_cache
    url = f'https://www.okx.com/api/v5/market/tickers?instType=SWAP&instId={",".join(INST_IDS)}'
    data = fetch_json(url)
    if data and data.get('code') == '0':
        with ticker_lock:
            for item in data['data']:
                ticker_cache[item['instId']] = item
        return data['data']
    return None

# ── Candles ──
def fetch_candles(symbol, bar='1H', limit=300):
    inst = symbol.replace('USDT', '-USDT-SWAP')
    url = f'https://www.okx.com/api/v5/market/history-candles?instId={inst}&bar={bar}&limit={limit}'
    data = fetch_json(url)
    if data and data.get('code') == '0' and data.get('data'):
        rows = []
        for r in data['data']:
            rows.append({
                'time': int(r[0]) / 1000,
                'open': float(r[1]), 'high': float(r[2]), 'low': float(r[3]),
                'close': float(r[4]), 'volume': float(r[5])
            })
        rows.reverse()
        return rows
    return None

def fetch_all_candles():
    for coin in COINS:
        for bar in ['1H', '4H', '15m']:
            interval = bar.replace('H','h').lower()
            rows = fetch_candles(coin, bar, 300)
            if rows:
                if coin not in candle_cache:
                    candle_cache[coin] = {}
                candle_cache[coin][interval] = rows
                print(f"[candle] {coin} {bar}: {len(rows)} bars", flush=True)
        time.sleep(0.3)  # rate limit

# ── Whale order book ──
def fetch_order_book(symbol, depth=20):
    inst = symbol.replace('USDT', '-USDT-SWAP')
    url = f'https://www.okx.com/api/v5/market/books?instId={inst}&sz={depth}'
    data = fetch_json(url)
    if data and data.get('code') == '0' and data.get('data'):
        item = data['data'][0]
        return {
            'bids': [[float(b[0]), float(b[1])] for b in item.get('bids', [])],
            'asks': [[float(a[0]), float(a[1])] for a in item.get('asks', [])],
            'ts': float(item.get('ts', 0)) / 1000
        }
    return None

def fetch_all_orderbooks():
    global orderbook_cache, whale_last_fetch
    now = time.time()
    if now - whale_last_fetch < 25:
        return  # throttle to ~30s
    for coin in COINS:
        ob = fetch_order_book(coin, 20)
        if ob:
            orderbook_cache[coin] = ob
        time.sleep(0.15)
    whale_last_fetch = now
    print(f"[whale] orderbooks updated for {len(orderbook_cache)} coins", flush=True)

def execute_okx_trade(coin, side, sz, leverage):
    """Execute market order on OKX using auto_trade_config credentials."""
    cfg = auto_trade_config
    inst = coin.replace('USDT', '-USDT-SWAP')
    apikey = cfg.get('apikey', '')
    secret = cfg.get('secret', '')
    passphrase = cfg.get('passphrase', '')
    if not apikey or not secret:
        return False

    try:
        import hmac, base64, hashlib
        # Set leverage first
        ts = str(int(time.time() * 1000))
        body = json.dumps({'instId': inst, 'lever': str(leverage), 'mgnMode': 'cross'})
        sign_input = ts + 'POST' + '/api/v5/account/set-leverage' + body
        signature = base64.b64encode(hmac.new(secret.encode(), sign_input.encode(), hashlib.sha256).digest()).decode()

        req = urllib.request.Request('https://www.okx.com/api/v5/account/set-leverage',
            data=body.encode(),
            headers={'OK-ACCESS-KEY': apikey, 'OK-ACCESS-SIGN': signature,
                     'OK-ACCESS-TIMESTAMP': ts, 'OK-ACCESS-PASSPHRASE': passphrase,
                     'Content-Type': 'application/json'})
        ctx = ssl.create_default_context()
        urllib.request.urlopen(req, timeout=10, context=ctx)

        # Place market order
        ts2 = str(int(time.time() * 1000))
        order_body = json.dumps({
            'instId': inst, 'tdMode': 'cross',
            'side': 'buy' if side == 'LONG' else 'sell',
            'ordType': 'market', 'sz': str(sz)
        })
        sign_input2 = ts2 + 'POST' + '/api/v5/trade/order' + order_body
        signature2 = base64.b64encode(hmac.new(secret.encode(), sign_input2.encode(), hashlib.sha256).digest()).decode()

        req2 = urllib.request.Request('https://www.okx.com/api/v5/trade/order',
            data=order_body.encode(),
            headers={'OK-ACCESS-KEY': apikey, 'OK-ACCESS-SIGN': signature2,
                     'OK-ACCESS-TIMESTAMP': ts2, 'OK-ACCESS-PASSPHRASE': passphrase,
                     'Content-Type': 'application/json'})
        resp = urllib.request.urlopen(req2, timeout=10, context=ctx)
        result = json.loads(resp.read().decode())
        if result.get('code') == '0':
            print(f"[trade] EXECUTED {coin} {side} {sz} contracts", flush=True)
            return True
        else:
            print(f"[trade] FAILED {coin} {side}: {result.get('msg','unknown')}", flush=True)
            return False
    except Exception as e:
        print(f"[trade] ERROR {coin} {side}: {e}", flush=True)
        return False

def process_auto_trades():
    """Check new signals and execute auto-trades."""
    global auto_trade_last_signal
    if not auto_trade_enabled:
        return
    cfg = auto_trade_config
    strategies = cfg.get('strategies', {})
    if not strategies:
        return
    leverage = int(cfg.get('leverage', 5))
    pos_pct = float(cfg.get('posSize', 10))
    now = time.time()

    for coin in COINS:
        pd = price_data.get(coin, {})
        sigs = pd.get('allSignals', [])
        if not sigs:
            continue
        # Check last signal for this coin
        last_key = f'{coin}_last_trade'
        if now - auto_trade_last_signal.get(last_key, 0) < TRADE_COOLDOWN:
            continue
        ls = sigs[-1]
        sname = ls.get('strategy', '')
        # Map strategy names to strategy keys
        strat_map = {
            '布林带均值回归': 'bollinger', 'RSI超卖反弹': 'rsi', 'RSI超买回落': 'rsi',
            'MACD金叉': 'macd', 'MACD死叉': 'macd',
            'EMA趋势金叉': 'trend', 'EMA趋势死叉': 'trend',
            'RBF假突破': 'rbf', 'RBF假跌破': 'rbf',
            '综合多策略': 'combo', '综合+巨鲸共振': 'combo',
            '巨鲸追踪': 'whale'
        }
        skey = strat_map.get(sname, None)
        if not skey or not strategies.get(skey):
            continue
        side = ls.get('type', 'LONG')
        sltp = ls.get('sltp')
        if not sltp:
            continue
        # Calculate position size
        entry = ls.get('price', 0)
        sl_price = sltp.get('sl', 0)
        if entry <= 0 or sl_price <= 0:
            continue
        risk_per_unit = abs(entry - sl_price)
        if risk_per_unit <= 0:
            continue
        # Use a default account balance for sizing (10000 USDT)
        account = 10000.0
        contracts = (account * pos_pct / 100) / risk_per_unit * leverage
        # Round contracts based on coin
        if coin == 'BTCUSDT':
            contracts = max(1, int(contracts * 100) / 100)
        elif coin == 'ETHUSDT':
            contracts = max(1, int(contracts * 10) / 10)
        else:
            contracts = max(1, int(contracts))
        if contracts <= 0:
            continue

        # Execute trade
        print(f"[trade] Signal: {coin} {sname} {side} @ {entry} — executing {contracts} contracts", flush=True)
        ok = execute_okx_trade(coin, side, contracts, leverage)
        if ok:
            auto_trade_last_signal[last_key] = now

def get_whale_thresholds(coin):
    """Return (strong_buy, buy, strong_sell, sell) thresholds based on volume tier."""
    high_vol = ['BTCUSDT', 'ETHUSDT']
    mid_vol  = ['SOLUSDT', 'BNBUSDT', 'LINKUSDT', 'AVAXUSDT']
    low_vol  = ['DOGEUSDT', 'ARBUSDT']
    if coin in high_vol:
        return (4.0, 2.5, 0.25, 0.4)   # BTC/ETH: need very strong imbalance
    elif coin in mid_vol:
        return (3.2, 2.0, 0.3, 0.5)    # SOL/BNB/LINK/AVAX: moderate
    else:
        return (2.8, 1.8, 0.35, 0.55)  # DOGE/ARB: looser

def detect_whale_signals(coin, orderbook):
    """Detect whale direction signals from order book with coin-specific thresholds."""
    global whale_cooldown, whale_global_ts
    if not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
        return []
    now = time.time()
    # Global throttle: only 1 whale signal per 3 min across all coins
    if now - whale_global_ts < 180:
        return []
    # Cooldown: skip if any whale signal for this coin was sent within 45min
    for key, ts in list(whale_cooldown.items()):
        if key.startswith(coin + '_') and now - ts < 2700:
            return []
        elif now - ts >= 5400:
            del whale_cooldown[key]
    bids = orderbook['bids']
    asks = orderbook['asks']
    if len(bids) < 5 or len(asks) < 5:
        return []

    signals = []
    strong_buy, buy_th, strong_sell, sell_th = get_whale_thresholds(coin)

    # Calculate total bid/ask volume at top levels
    bid_vol_5 = sum(b[1] for b in bids[:5])
    ask_vol_5 = sum(a[1] for a in asks[:5])
    bid_vol_10 = sum(b[1] for b in bids[:10])
    ask_vol_10 = sum(a[1] for a in asks[:10])
    total_bid = bid_vol_5 + bid_vol_10
    total_ask = ask_vol_5 + ask_vol_10

    if total_ask <= 0:
        return []

    ratio = total_bid / total_ask

    # Check for whale walls — single price level with outsized volume
    avg_bid_vol_5 = bid_vol_5 / 5
    avg_ask_vol_5 = ask_vol_5 / 5
    bid_whale = any(b[1] >= avg_bid_vol_5 * 4 for b in bids[:5])
    ask_whale = any(a[1] >= avg_ask_vol_5 * 4 for a in asks[:5])

    detail_parts = []

    # Strong buy — requires ratio ≥ strong_buy AND whale wall
    if ratio >= strong_buy and bid_whale:
        detail_parts.append(f'买盘碾压 {ratio:.1f}x 有大单')
        signals.append({'type':'LONG','strategy':'巨鲸追踪','strength':88,'price':bids[0][0],'time':now,'detail':' · '.join(detail_parts)})
    elif ratio >= buy_th and bid_whale:
        detail_parts.append(f'买盘优势 {ratio:.1f}x 有大单')
        signals.append({'type':'LONG','strategy':'巨鲸追踪','strength':75,'price':bids[0][0],'time':now,'detail':' · '.join(detail_parts)})
    elif ratio >= buy_th:
        detail_parts.append(f'买盘偏强 {ratio:.1f}x')
        signals.append({'type':'LONG','strategy':'巨鲸追踪','strength':65,'price':bids[0][0],'time':now,'detail':' · '.join(detail_parts)})

    # Strong sell — requires ratio ≤ strong_sell AND whale wall
    if ratio <= strong_sell and ask_whale:
        detail_parts.append(f'卖盘碾压 {1/ratio:.1f}x 有大单')
        signals.append({'type':'SHORT','strategy':'巨鲸追踪','strength':88,'price':asks[0][0],'time':now,'detail':' · '.join(detail_parts)})
    elif ratio <= sell_th and ask_whale:
        detail_parts.append(f'卖盘优势 {1/ratio:.1f}x 有大单')
        signals.append({'type':'SHORT','strategy':'巨鲸追踪','strength':75,'price':asks[0][0],'time':now,'detail':' · '.join(detail_parts)})
    elif ratio <= sell_th:
        detail_parts.append(f'卖盘偏强 {1/ratio:.1f}x')
        signals.append({'type':'SHORT','strategy':'巨鲸追踪','strength':65,'price':asks[0][0],'time':now,'detail':' · '.join(detail_parts)})

    # Set cooldown + global throttle for emitted signals
    if signals:
        whale_global_ts = now
        for s in signals:
            whale_cooldown[f'{coin}_{s["type"]}'] = now
    return signals

# ── Indicator engine (Python) ──
def sma(values, period):
    if len(values) < period:
        return [None]*len(values)
    out = [None]*len(values)
    for i in range(period-1, len(values)):
        out[i] = sum(values[i-period+1:i+1]) / period
    return out

def ema(values, period):
    if len(values) < 2:
        return [None]*len(values)
    out = [None]*len(values)
    k = 2/(period+1)
    out[0] = values[0]
    for i in range(1, len(values)):
        out[i] = values[i]*k + out[i-1]*(1-k)
    return out

def bollinger(values, period=20, std_mult=2):
    n = len(values)
    sma_vals = sma(values, period)
    upper = [None]*n
    lower = [None]*n
    for i in range(period-1, n):
        chunk = values[i-period+1:i+1]
        mean = sma_vals[i]
        std = math.sqrt(sum((x-mean)**2 for x in chunk)/period)
        upper[i] = mean + std_mult*std
        lower[i] = mean - std_mult*std
    return sma_vals, upper, lower

def rsi(closes, period=14):
    n = len(closes)
    if n < period+1:
        return [None]*n
    out = [None]*n
    gains = 0
    losses = 0
    for i in range(1, period+1):
        ch = closes[i] - closes[i-1]
        if ch >= 0: gains += ch
        else: losses -= ch
    avg_gain = gains/period
    avg_loss = losses/period if losses > 0 else 0.0001
    out[period] = 100 - 100/(1 + avg_gain/avg_loss)
    for i in range(period+1, n):
        ch = closes[i] - closes[i-1]
        gain = ch if ch > 0 else 0
        loss = -ch if ch < 0 else 0
        avg_gain = (avg_gain*(period-1) + gain)/period
        avg_loss = (avg_loss*(period-1) + loss)/period
        out[i] = 100 - 100/(1 + avg_gain/(avg_loss if avg_loss > 0 else 0.0001))
    return out

def macd(closes, fast=12, slow=26, signal=9):
    ef = ema(closes, fast)
    es = ema(closes, slow)
    n = len(closes)
    ml = [None]*n
    sl = [None]*n
    hist = [None]*n
    for i in range(n):
        if ef[i] is not None and es[i] is not None:
            ml[i] = ef[i] - es[i]
    ml_valid = [v for v in ml if v is not None]
    if len(ml_valid) >= signal:
        em = ema(ml_valid, signal)
        k = 0
        for i in range(n):
            if ml[i] is not None and k < len(em):
                sl[i] = em[k]
                hist[i] = ml[i] - sl[i]
                k += 1
    return ml, sl, hist

def atr(candles, period=14):
    n = len(candles)
    if n < 2:
        return [None]*n
    out = [None]*n
    tr = max(candles[1]['high']-candles[1]['low'],
             abs(candles[1]['high']-candles[0]['close']),
             abs(candles[1]['low']-candles[0]['close']))
    out[1] = tr
    for i in range(2, n):
        tr = max(candles[i]['high']-candles[i]['low'],
                 abs(candles[i]['high']-candles[i-1]['close']),
                 abs(candles[i]['low']-candles[i-1]['close']))
        out[i] = (out[i-1]*(period-1) + tr)/period
    return out

# ── Strategy signals ──
def detect_signals(coin, candles):
    if len(candles) < 30:
        return []
    closes = [c['close'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]
    i = len(candles) - 1
    p = closes[i]

    sma_vals, bb_upper, bb_lower = bollinger(closes)
    rsi_vals = rsi(closes)
    ml, sl, hist = macd(closes)
    ema50 = ema(closes, 50)
    ema200 = ema(closes, 200)
    atr_vals = atr(candles)

    signals = []
    t = candles[i]['time']

    # Bollinger mean reversion
    if bb_lower[i] and bb_upper[i] and i >= 1:
        pp = closes[i-1]
        if pp <= (bb_lower[i-1] or 0) and p > bb_lower[i] and (rsi_vals[i] or 50) < 40:
            signals.append({'type':'LONG','strategy':'布林带均值回归','strength':min(90,50+int((40-(rsi_vals[i] or 50))*2)),'price':p,'time':t,'detail':f'下轨反弹 RSI={rsi_vals[i]:.1f}'})
        if pp >= (bb_upper[i-1] or 1e9) and p < bb_upper[i] and (rsi_vals[i] or 50) > 60:
            signals.append({'type':'SHORT','strategy':'布林带均值回归','strength':min(90,50+int(((rsi_vals[i] or 50)-60)*2)),'price':p,'time':t,'detail':f'上轨回落 RSI={rsi_vals[i]:.1f}'})

    # RSI
    if rsi_vals[i] and rsi_vals[i-1]:
        if rsi_vals[i-1] <= 30 and rsi_vals[i] > 30 and ema50[i] and p > ema50[i]:
            signals.append({'type':'LONG','strategy':'RSI超卖反弹','strength':int(60+(30-rsi_vals[i-1])*1.5),'price':p,'time':t,'detail':f'RSI {rsi_vals[i-1]:.1f}→{rsi_vals[i]:.1f}'})
        if rsi_vals[i-1] >= 70 and rsi_vals[i] < 70 and ema50[i] and p < ema50[i]:
            signals.append({'type':'SHORT','strategy':'RSI超买回落','strength':int(60+(rsi_vals[i-1]-70)*1.5),'price':p,'time':t,'detail':f'RSI {rsi_vals[i-1]:.1f}→{rsi_vals[i]:.1f}'})

    # MACD
    if ml[i] and sl[i] and ml[i-1] and sl[i-1] and hist[i] and hist[i-1]:
        if ml[i-1] <= sl[i-1] and ml[i] > sl[i] and hist[i] > 0:
            signals.append({'type':'LONG','strategy':'MACD金叉','strength':70,'price':p,'time':t,'detail':f'MACD {ml[i]:.2f} > Signal'})
        if ml[i-1] >= sl[i-1] and ml[i] < sl[i] and hist[i] < 0:
            signals.append({'type':'SHORT','strategy':'MACD死叉','strength':70,'price':p,'time':t,'detail':f'MACD {ml[i]:.2f} < Signal'})

    # EMA trend
    if ema50[i] and ema200[i] and ema50[i-1] and ema200[i-1]:
        if ema50[i-1] <= ema200[i-1] and ema50[i] > ema200[i]:
            signals.append({'type':'LONG','strategy':'EMA趋势金叉','strength':80,'price':p,'time':t,'detail':'EMA50↑EMA200'})
        if ema50[i-1] >= ema200[i-1] and ema50[i] < ema200[i]:
            signals.append({'type':'SHORT','strategy':'EMA趋势死叉','strength':80,'price':p,'time':t,'detail':'EMA50↓EMA200'})

    # RBF (range break fakeout)
    if i >= 23:
        rp = 20
        rh = max(highs[i-rp:i])
        rl = min(lows[i-rp:i])
        if closes[i-2] > rh*1.01 and closes[i-1] > rh and p < rh:
            signals.append({'type':'SHORT','strategy':'RBF假突破','strength':65,'price':p,'time':t,'detail':f'假突破回归 {rl:.1f}-{rh:.1f}'})
        if closes[i-2] < rl*0.99 and closes[i-1] < rl and p > rl:
            signals.append({'type':'LONG','strategy':'RBF假跌破','strength':65,'price':p,'time':t,'detail':f'假跌破回归 {rl:.1f}-{rh:.1f}'})

    # Combo
    b = r = m = 0
    if bb_lower[i] and p <= bb_lower[i]*1.02: b=1
    elif bb_upper[i] and p >= bb_upper[i]*0.98: b=-1
    if rsi_vals[i] and rsi_vals[i] < 35: r=1
    elif rsi_vals[i] and rsi_vals[i] > 65: r=-1
    if hist[i] and hist[i-1] and hist[i] > 0 and hist[i] > hist[i-1]: m=1
    elif hist[i] and hist[i-1] and hist[i] < 0 and hist[i] < hist[i-1]: m=-1
    total = b+r+m
    if total >= 2:
        signals.append({'type':'LONG','strategy':'综合多策略','strength':55+abs(total)*20,'price':p,'time':t,'detail':f'{total}策略共振 B:{b} R:{r} M:{m}'})
    if total <= -2:
        signals.append({'type':'SHORT','strategy':'综合多策略','strength':55+abs(total)*20,'price':p,'time':t,'detail':f'{abs(total)}策略共振 B:{b} R:{r} M:{m}'})

    # SL/TP
    for s in signals:
        s['sltp'] = compute_sltp(s['price'], s['type'], candles)

    return signals

def compute_sltp(entry, side, candles):
    if len(candles) < 20:
        return None
    atr_vals = atr(candles)
    cur_atr = atr_vals[-1] if atr_vals[-1] else None
    if not cur_atr:
        return None
    closes = [c['close'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]
    _, bb_upper, bb_lower = bollinger(closes)
    slice20_high = max(highs[-20:])
    slice20_low = min(lows[-20:])

    if side == 'LONG':
        atr_sl = entry - cur_atr*1.5
        hard_sl = entry*(1-0.03)
        struct_sl = slice20_low*(1-0.005)
        sl = min(atr_sl, hard_sl, struct_sl)
        risk = entry - sl
        tp1 = entry + risk*1.5
        tp2 = entry + risk*2.5
        tp3 = bb_upper[-1] if bb_upper[-1] else entry + risk*3
    else:
        atr_sl = entry + cur_atr*1.5
        hard_sl = entry*(1+0.03)
        struct_sl = slice20_high*(1+0.005)
        sl = min(atr_sl, hard_sl, struct_sl)
        risk = sl - entry
        tp1 = entry - risk*1.5
        tp2 = entry - risk*2.5
        tp3 = bb_lower[-1] if bb_lower[-1] else entry - risk*3

    risk_val = abs(entry - sl)

    def fmt_p(v):
        if abs(v) < 1: return math.floor(abs(v)*1e6)/1e6 * (1 if v>=0 else -1)
        if abs(v) < 100: return math.floor(abs(v)*1e3)/1e3 * (1 if v>=0 else -1)
        return math.floor(abs(v)*100)/100 * (1 if v>=0 else -1)
    return {
        'sl': fmt_p(sl), 'slPct': round(risk_val/entry*100, 2),
        'tp1': fmt_p(tp1), 'tp1Pct': round(abs(tp1-entry)/entry*100, 2),
        'tp2': fmt_p(tp2), 'tp2Pct': round(abs(tp2-entry)/entry*100, 2),
        'tp3': fmt_p(tp3), 'tp3Pct': round(abs(tp3-entry)/entry*100, 2),
        'rr1': round(abs(tp1-entry)/risk_val, 1),
        'rr2': round(abs(tp2-entry)/risk_val, 1),
        'risk': fmt_p(risk_val),
        'atr': fmt_p(cur_atr)
    }

# ── Analysis engine (runs continuously) ──
def analyze_all():
    global price_data, signal_log
    for coin in COINS:
        inst = coin.replace('USDT', '-USDT-SWAP')
        with ticker_lock:
            ticker = ticker_cache.get(inst, {})
        if not ticker:
            continue

        # Get live price from ticker (always available)
        try:
            live_price = float(ticker.get('last', 0))
            if live_price <= 0:
                continue
        except (ValueError, TypeError):
            continue

        # 24h change from ticker
        ch24h = 0
        try:
            open24h = float(ticker.get('open24h', 0))
            if open24h > 0:
                ch24h = (live_price - open24h)/open24h*100
        except:
            pass

        # Candle data for indicators (may be empty on first run)
        bars = candle_cache.get(coin, {}).get('1h', [])
        has_indicators = len(bars) >= 30

        sigs = []
        rsi_str = '—'
        bb_pos = '加载中'
        bb_pct = 50
        macd_signal = '⚪'
        ema_sig = '⚪'

        if has_indicators:
            # Update close from ticker
            bars[-1]['close'] = live_price
            bars[-1]['high'] = max(bars[-1]['high'], live_price)
            bars[-1]['low'] = min(bars[-1]['low'], live_price)

            closes = [b['close'] for b in bars]
            _, bb_upper, bb_lower = bollinger(closes)
            rsi_vals = rsi(closes)
            ml_val, sl_val, hist_val = macd(closes)
            ema50_arr = ema(closes, 50)
            ema200_arr = ema(closes, 200)
            i = len(bars) - 1
            p = closes[i]

            if bb_lower[i] and bb_upper[i]:
                bb_pct = (p - bb_lower[i])/(bb_upper[i]-bb_lower[i])*100
                bb_pos = '🟢超卖' if bb_pct<5 else '🔴超买' if bb_pct>95 else '🟢偏低' if bb_pct<20 else '🔴偏高' if bb_pct>80 else '⚪中轨'

            rv = rsi_vals[i]
            if rv is not None:
                rsi_str = f'{rv:.1f}'

            if hist_val[i] and hist_val[i-1]:
                macd_signal = '🟢' if hist_val[i]>0 and hist_val[i]>hist_val[i-1] else '🔴' if hist_val[i]<0 and hist_val[i]<hist_val[i-1] else '⚪'

            if ema50_arr[i] and ema200_arr[i]:
                ema_sig = '🟢多头' if ema50_arr[i] > ema200_arr[i] else '🔴空头'

            sigs = detect_signals(coin, bars)
            # Whale signal detection (from orderbook)
            ob = orderbook_cache.get(coin)
            whale_bias = 0  # -1 bearish, 0 neutral, 1 bullish
            if ob:
                ws = detect_whale_signals(coin, ob)
                sigs.extend(ws)
                # Compute whale bias for combo enhancement
                bv = sum(b[1] for b in ob['bids'][:5])
                av = sum(a[1] for a in ob['asks'][:5])
                if av > 0:
                    wr = bv / av
                    if wr > 2.5: whale_bias = 1
                    elif wr < 0.4: whale_bias = -1
            # Enhance combo signals with whale direction
            for s in sigs:
                if '综合多策略' in s['strategy']:
                    if (s['type'] == 'LONG' and whale_bias == 1) or (s['type'] == 'SHORT' and whale_bias == -1):
                        s['strength'] = min(100, s['strength'] + 20)
                        s['detail'] += ' +🐳共鸣'
                        s['strategy'] = '综合+巨鲸共振'
                    elif (s['type'] == 'LONG' and whale_bias == -1) or (s['type'] == 'SHORT' and whale_bias == 1):
                        s['strength'] = max(20, s['strength'] - 15)
                        s['detail'] += ' ⚠️鲸鱼反向'

        # Whale direction indicator
        whale_dir = '—'
        ob = orderbook_cache.get(coin)
        if ob and ob.get('bids') and ob.get('asks'):
            bv = sum(b[1] for b in ob['bids'][:5])
            av = sum(a[1] for a in ob['asks'][:5])
            if av > 0:
                wr = bv / av
                whale_dir = '🐳买' if wr > 2.5 else ('🐻卖' if wr < 0.4 else ('📈偏买' if wr > 1.8 else ('📉偏卖' if wr < 0.55 else '⚖️平衡')))
        price_data[coin] = {
            'price': live_price, 'change24h': ch24h,
            'rsiVal': rsi_str, 'bbPos': bb_pos, 'bbPct': bb_pct,
            'macdSignal': macd_signal, 'emaSig': ema_sig,
            'whaleDir': whale_dir,
            'allSignals': sigs,
            'ohlcv': bars if has_indicators else [],
            'lastInterval': '1h'
        }

        # New signal detection
        if has_indicators:
            now = time.time()
            for s in sigs:
                key = f"{coin}_{s['strategy']}_{s['type']}_{s['time']}"
                if key not in notified_keys and now - s['time'] <= 4*3600:
                    notified_keys.add(key)
                    s['discoveredAt'] = now
                    s['coin'] = coin
                    with signal_lock:
                        signal_log.insert(0, s)
                        signal_log = signal_log[:200]
                    broadcast_signal(s)

    # Persist
    save_state()

def broadcast_signal(sig):
    data = json.dumps({'type':'signal','data':sig}, ensure_ascii=False)
    with sse_lock:
        dead = []
        for c in sse_clients:
            try:
                c.write(f"data: {data}\n\n".encode())
                c.flush()
            except:
                dead.append(c)
        for d in dead:
            sse_clients.remove(d)

# ── Main loop ──
def engine_loop():
    while True:
        try:
            fetch_all_tickers()
            fetch_all_orderbooks()
            analyze_all()
            process_auto_trades()
        except Exception as e:
            print(f"[engine] {e}", flush=True)
            import traceback; traceback.print_exc()
        time.sleep(5)

# ── Persistence ──
def save_state():
    try:
        with open(SIGNAL_FILE, 'w') as f:
            json.dump(signal_log[:100], f, ensure_ascii=False)
    except:
        pass
    try:
        with open(KEYS_FILE, 'w') as f:
            json.dump(list(notified_keys), f)
    except:
        pass
    try:
        cd = {}
        for coin, intervals in candle_cache.items():
            cd[coin] = {}
            for interval, bars in intervals.items():
                cd[coin][interval] = bars[-300:]
        with open(CANDLE_FILE, 'w') as f:
            json.dump(cd, f)
    except:
        pass
    try:
        with open(USERS_FILE, 'w') as f:
            json.dump(server_users, f, ensure_ascii=False)
    except:
        pass
    # Save accounts WITHOUT API keys for security
    safe_accounts = {}
    for e, a in server_accounts.items():
        safe_accounts[e] = {k: v for k, v in a.items() if k not in ('apiKey', 'secret', 'passphrase')}
    try:
        with open(ACCOUNTS_FILE, 'w') as f:
            json.dump(safe_accounts, f, ensure_ascii=False)
    except:
        pass

def load_state():
    global signal_log, notified_keys, candle_cache
    try:
        with open(SIGNAL_FILE) as f:
            signal_log = json.load(f)
            for s in signal_log:
                key = f"{s['coin']}_{s['strategy']}_{s['type']}_{s.get('time',0)}"
                notified_keys.add(key)
    except:
        pass
    try:
        with open(KEYS_FILE) as f:
            for k in json.load(f):
                notified_keys.add(k)
        print(f"[init] loaded {len(notified_keys)} notified keys", flush=True)
    except:
        pass
    try:
        with open(CANDLE_FILE) as f:
            candle_cache = json.load(f)
    except:
        pass
    try:
        with open(USERS_FILE) as f:
            server_users.update(json.load(f))
    except:
        pass
    try:
        with open(ACCOUNTS_FILE) as f:
            server_accounts.update(json.load(f))
        # Strip API keys from loaded accounts for security display
        for e, a in server_accounts.items():
            a.pop('apiKey', None); a.pop('secret', None); a.pop('passphrase', None)
    except:
        pass

# ── HTTP Handler ──
class ThreadingHTTPServer(ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

class ComboHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        content_len = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_len).decode() if content_len > 0 else '{}'
        try:
            data = json.loads(body)
        except:
            data = {}

        if path == '/api/trade/config':
            global auto_trade_enabled, auto_trade_config, auto_trade_last_signal
            auto_trade_enabled = data.get('enabled', False)
            if auto_trade_enabled:
                auto_trade_config = data.get('config', {})
                auto_trade_last_signal = {}
                print(f"[trade] Auto-trade ENABLED. Strategies: {list(auto_trade_config.get('strategies',{}).keys())}", flush=True)
            else:
                auto_trade_enabled = False
                auto_trade_config = {}
                print("[trade] Auto-trade DISABLED", flush=True)
            self._json({'ok': True, 'enabled': auto_trade_enabled})
            return
        if path == '/api/trade/status':
            self._json({'enabled': auto_trade_enabled, 'strategies': auto_trade_config.get('strategies', {}),
                        'timestamp': time.time()})
            return
        if path == '/api/user/register':
            email = data.get('email', '').strip().lower()
            pw = data.get('password', '')
            if not email or not pw:
                self._json({'ok': False, 'error': '邮箱和密码必填'})
                return
            if len(pw) < 6:
                self._json({'ok': False, 'error': '密码至少6位'})
                return
            if email in server_users:
                self._json({'ok': False, 'error': '该邮箱已注册'})
                return
            # Simple hash
            import hashlib
            h = hashlib.sha256((email + ':' + pw).encode()).hexdigest()[:24]
            server_users[email] = h
            save_state()
            self._json({'ok': True, 'email': email})
            return
        if path == '/api/user/login':
            email = data.get('email', '').strip().lower()
            pw = data.get('password', '')
            if not email or not pw:
                self._json({'ok': False, 'error': '邮箱和密码必填'})
                return
            import hashlib
            h = hashlib.sha256((email + ':' + pw).encode()).hexdigest()[:24]
            if server_users.get(email) != h:
                self._json({'ok': False, 'error': '邮箱或密码错误'})
                return
            self._json({'ok': True, 'email': email})
            return
        if path == '/api/user/accounts/save':
            email = data.get('email', '').strip().lower()
            acct = data.get('account', {})
            if not email or not acct:
                self._json({'ok': False, 'error': '参数不完整'})
                return
            server_accounts[email] = acct
            save_state()
            self._json({'ok': True})
            return
        if path == '/api/user/accounts/delete':
            email = data.get('email', '').strip().lower()
            if email in server_accounts:
                del server_accounts[email]
                save_state()
            self._json({'ok': True})
            return
        self._json({'error': 'not found'}, 404)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == '/api/okx/tickers':
            self._json({'code':'0','data': list(ticker_cache.values())})
            return
        if path == '/api/okx/candles':
            inst_id = query.get('instId', [None])[0]
            bar = query.get('bar', ['1H'])[0]
            limit = int(query.get('limit', ['300'])[0])
            symbol = inst_id.replace('-USDT-SWAP', 'USDT') if inst_id else None
            interval = bar.replace('H','h').lower()
            if not symbol:
                self._json({'error':'missing instId'}, 400)
                return
            bars = candle_cache.get(symbol, {}).get(interval, [])[-limit:]
            self._json({'code':'0','data': bars})
            return
        if path == '/api/user/accounts/get':
            email = query.get('email', [None])[0]
            if email:
                acct = server_accounts.get(email.strip().lower(), {})
                self._json({'ok': True, 'email': email, 'account': acct})
            else:
                self._json({'ok': True, 'accounts': server_accounts})
            return
        if path == '/api/okx/ping':
            self._json({'pong':True,'tickers':len(ticker_cache),'signals':len(signal_log)})
            return
        if path == '/api/priceData':
            # Lightweight: price + indicators only, no candle data
            pd = {}
            for k, v in price_data.items():
                pd[k] = {
                    'price': v.get('price'), 'change24h': v.get('change24h'),
                   'rsiVal': v.get('rsiVal'), 'bbPos': v.get('bbPos'), 'bbPct': v.get('bbPct'),
                    'whaleDir': v.get('whaleDir'),
                   'macdSignal': v.get('macdSignal'), 'emaSig': v.get('emaSig'),
                    'allSignals': v.get('allSignals'), 'lastInterval': v.get('lastInterval')
                }
            self._json(pd)
            return
        if path == '/api/state':
            # Full state dump for frontend
            self._json({
                'priceData': price_data,
                'signalLog': signal_log[:200],
                'ohlcvCache': candle_cache
            })
            return
        if path == '/api/signals':
            self._json(signal_log[:200])
            return
        if path == '/api/stream':
            self.send_response(200)
            self.send_header('Content-Type','text/event-stream')
            self.send_header('Cache-Control','no-cache')
            self.send_header('Connection','keep-alive')
            self.send_header('Access-Control-Allow-Origin','*')
            self.end_headers()

            # Send initial state
            data = json.dumps({'type':'init','priceData':price_data,'signalLog':signal_log[:200]}, ensure_ascii=False)
            self.wfile.write(f"data: {data}\n\n".encode()); self.wfile.flush()

            with sse_lock:
                sse_clients.append(self.wfile)
            try:
                while True:
                    self.wfile.write(f": heartbeat\n\n".encode()); self.wfile.flush()
                    time.sleep(15)
            except:
                pass
            finally:
                with sse_lock:
                    if self.wfile in sse_clients:
                        sse_clients.remove(self.wfile)
            return

        # Static file serving
        file_path = os.path.join(SERVE_DIR, path.lstrip('/'))
        if not os.path.exists(file_path) or path == '/':
            file_path = os.path.join(SERVE_DIR, 'contract-terminal.html')
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
        except:
            self._text('Not Found', 404)
            return

        ct = 'text/html; charset=utf-8' if file_path.endswith('.html') else \
             'application/javascript' if file_path.endswith('.js') else 'text/css'
        # Inject initial data for HTML pages so they work even when fetch is blocked
        if file_path.endswith('.html'):
            content_str = content.decode('utf-8')
            import json as _json
            init_data = _json.dumps({
                'priceData': price_data,
                'signalLog': signal_log[:200],
                'tickers': list(ticker_cache.values())
            }, ensure_ascii=False)
            inject_script = f'\n<script>window.__INIT_DATA__ = {init_data};</script>\n'
            content_str = content_str.replace('</head>', inject_script + '</head>')
            content = content_str.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', ct)
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Cache-Control','no-cache')
        self.end_headers()
        self.wfile.write(content)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','GET,OPTIONS')
        self.send_header('Access-Control-Allow-Headers','*')
        self.end_headers()

    def _json(self, data, code=200):
        self.send_response(code)
        self.send_header('Content-Type','application/json')
        self.send_header('Access-Control-Allow-Origin','*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

    def _text(self, text, code=200):
        self.send_response(code)
        self.send_header('Content-Type','text/plain')
        self.end_headers()
        self.wfile.write(text.encode())

    def log_message(self, fmt, *args):
        if 'api' not in args[0]:
            print(f"[http] {args[0]}", flush=True)

if __name__ == '__main__':
    print(f"[server] {BIND_HOST}:{PORT}", flush=True)
    load_state()

    # Start engine immediately (ticker poll starts filling priceData)
    # Candles load asynchronously to avoid Render startup timeout
    print("[init] Starting engine...", flush=True)
    threading.Thread(target=engine_loop, daemon=True).start()

    # Load candles in background thread
    def deferred_candle_load():
        time.sleep(2)
        print("[init] Loading candles...", flush=True)
        fetch_all_candles()
        print("[init] Candles loaded", flush=True)
    threading.Thread(target=deferred_candle_load, daemon=True).start()

    server = ThreadingHTTPServer((BIND_HOST, PORT), ComboHandler)
    print(f"[server] http://{BIND_HOST}:{PORT}/", flush=True)
    server.serve_forever()

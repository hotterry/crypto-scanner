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
BIND_HOST = '0.0.0.0' if os.environ.get('RAILWAY_ENVIRONMENT') else '127.0.0.1'
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
CANDLE_FILE = os.path.join(SERVE_DIR, 'candles_cache.json')

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
        for bar in ['1H', '4H']:
            interval = bar.replace('H','h').lower()
            rows = fetch_candles(coin, bar, 300)
            if rows:
                if coin not in candle_cache:
                    candle_cache[coin] = {}
                candle_cache[coin][interval] = rows
                print(f"[candle] {coin} {bar}: {len(rows)} bars", flush=True)
        time.sleep(0.3)  # rate limit

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
    return {
        'sl': round(sl, 2), 'slPct': round(risk_val/entry*100, 2),
        'tp1': round(tp1, 2), 'tp1Pct': round(abs(tp1-entry)/entry*100, 2),
        'tp2': round(tp2, 2), 'tp2Pct': round(abs(tp1-entry)/entry*100, 2),
        'tp3': round(tp3, 2), 'tp3Pct': round(abs(tp3-entry)/entry*100, 2),
        'rr1': round(abs(tp1-entry)/risk_val, 1),
        'rr2': round(abs(tp2-entry)/risk_val, 1),
        'risk': round(risk_val, 2),
        'atr': round(cur_atr, 2)
    }

# ── Analysis engine (runs continuously) ──
def analyze_all():
    global price_data
    for coin in COINS:
        bars = candle_cache.get(coin, {}).get('1h', [])
        if len(bars) < 30:
            continue
        # Update close from ticker
        inst = coin.replace('USDT', '-USDT-SWAP')
        with ticker_lock:
            ticker = ticker_cache.get(inst, {})
        if ticker:
            try:
                live = float(ticker.get('last', 0))
                if live > 0:
                    bars[-1]['close'] = live
                    bars[-1]['high'] = max(bars[-1]['high'], live)
                    bars[-1]['low'] = min(bars[-1]['low'], live)
            except (ValueError, TypeError):
                pass

        closes = [b['close'] for b in bars]
        _, bb_upper, bb_lower = bollinger(closes)
        rsi_vals = rsi(closes)
        ml_val, sl_val, hist_val = macd(closes)
        ema50 = ema(closes, 50)
        ema200 = ema(closes, 200)
        i = len(bars) - 1
        p = closes[i]

        # BB position
        bb_pct = 50
        bb_pos = '中性'
        if bb_lower[i] and bb_upper[i]:
            bb_pct = (p - bb_lower[i])/(bb_upper[i]-bb_lower[i])*100
            bb_pos = '🟢超卖' if bb_pct<5 else '🔴超买' if bb_pct>95 else '🟢偏低' if bb_pct<20 else '🔴偏高' if bb_pct>80 else '⚪中轨'

        rsi_val = rsi_vals[i]
        macd_signal = '⚪'
        if hist_val[i] and hist_val[i-1]:
            macd_signal = '🟢' if hist_val[i]>0 and hist_val[i]>hist_val[i-1] else '🔴' if hist_val[i]<0 and hist_val[i]<hist_val[i-1] else '⚪'

        ema_sig = '⚪'
        if ema50[i] and ema200[i]:
            ema_sig = '🟢多头' if ema50[i] > ema200[i] else '🔴空头'

        # 24h change from ticker
        ch24h = 0
        try:
            open24h = float(ticker.get('open24h', 0))
            if open24h > 0:
                ch24h = (p - open24h)/open24h*100
        except (ValueError, TypeError):
            pass

        # Live price from ticker
        live_price = p
        try:
            lp = float(ticker.get('last', 0))
            if lp > 0:
                live_price = lp
        except (ValueError, TypeError):
            pass

        # Signals
        sigs = detect_signals(coin, bars)

        price_data[coin] = {
            'price': live_price, 'change24h': ch24h,
            'rsiVal': f'{rsi_val:.1f}' if rsi_val else '—',
            'bbPos': bb_pos, 'bbPct': bb_pct,
            'macdSignal': macd_signal, 'emaSig': ema_sig,
            'allSignals': sigs, 'ohlcv': bars, 'lastInterval': '1h'
        }

        # New signal detection
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
            analyze_all()
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
        cd = {}
        for coin, intervals in candle_cache.items():
            cd[coin] = {}
            for interval, bars in intervals.items():
                cd[coin][interval] = bars[-300:]
        with open(CANDLE_FILE, 'w') as f:
            json.dump(cd, f)
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
        with open(CANDLE_FILE) as f:
            candle_cache = json.load(f)
    except:
        pass

# ── HTTP Handler ──
class ComboHandler(http.server.BaseHTTPRequestHandler):
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
        if path == '/api/okx/ping':
            self._json({'pong':True,'tickers':len(ticker_cache),'signals':len(signal_log)})
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

        self.send_response(200)
        ct = 'text/html; charset=utf-8' if file_path.endswith('.html') else \
             'application/javascript' if file_path.endswith('.js') else 'text/css'
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

    # Initial full candle load
    print("[init] Loading candles...", flush=True)
    fetch_all_candles()
    print("[init] Starting engine...", flush=True)
    threading.Thread(target=engine_loop, daemon=True).start()

    server = http.server.HTTPServer((BIND_HOST, PORT), ComboHandler)
    print(f"[server] http://{BIND_HOST}:{PORT}/", flush=True)
    server.serve_forever()

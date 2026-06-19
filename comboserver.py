#!/usr/bin/env python3
"""
Combined HTTP server: serves static files AND proxies OKX API data.
Deployable on Railway or any PaaS.

Local:  python3 comboserver.py
Railway: detected automatically via PORT env var.
"""

import http.server
import json
import urllib.request
import urllib.parse
import ssl
import threading
import time
import os

# Railway sets PORT env var; use 9878 locally
PORT = int(os.environ.get('PORT', 9878))
# Bind to 0.0.0.0 for Railway, 127.0.0.1 for local
BIND_HOST = '0.0.0.0' if os.environ.get('RAILWAY_ENVIRONMENT') else '127.0.0.1'

COINS = ['BTCUSDT','ETHUSDT','SOLUSDT','DOGEUSDT','BNBUSDT','AVAXUSDT','LINKUSDT','ARBUSDT']
INST_IDS = [c.replace('USDT', '-USDT-SWAP') for c in COINS]

ticker_cache = {}
ticker_cache_lock = threading.Lock()

def fetch_json(url, timeout=10):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json'
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[server] REST failed: {e}", flush=True)
        return None

def fetch_all_tickers():
    global ticker_cache
    inst_ids_str = ','.join(INST_IDS)
    url = f'https://www.okx.com/api/v5/market/tickers?instType=SWAP&instId={inst_ids_str}'
    data = fetch_json(url)
    if data and data.get('code') == '0' and data.get('data'):
        with ticker_cache_lock:
            for item in data['data']:
                ticker_cache[item['instId']] = item
        return data['data']
    return None

def ticker_refresh_loop():
    while True:
        try:
            fetch_all_tickers()
        except Exception as e:
            print(f"[server] ticker error: {e}", flush=True)
        time.sleep(5)

class ComboHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == '/api/okx/tickers':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            with ticker_cache_lock:
                data = list(ticker_cache.values())
            self.wfile.write(json.dumps({'code':'0','data':data}, ensure_ascii=False).encode())
            return

        if path == '/api/okx/candles':
            inst_id = query.get('instId', [None])[0]
            bar = query.get('bar', ['1H'])[0]
            limit = int(query.get('limit', ['300'])[0])
            if not inst_id:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"error":"missing instId"}')
                return
            data = fetch_candles(inst_id, bar, limit)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
            return

        if path == '/api/okx/ping':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(b'{"pong":true,"tickers":' + str(len(ticker_cache)).encode() + b'}')
            return

        # Serve static files from script directory
        serve_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(serve_dir, path.lstrip('/'))
        if not file_path or path == '/' or not os.path.exists(file_path):
            file_path = os.path.join(serve_dir, 'contract-terminal.html')
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
        except (IOError, IsADirectoryError):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')
            return

        self.send_response(200)
        if file_path.endswith('.html'):
            self.send_header('Content-Type', 'text/html; charset=utf-8')
        elif file_path.endswith('.js'):
            self.send_header('Content-Type', 'application/javascript')
        elif file_path.endswith('.css'):
            self.send_header('Content-Type', 'text/css')
        else:
            self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.end_headers()
        self.wfile.write(content)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.end_headers()

    def log_message(self, format, *args):
        if 'api/okx' not in args[0]:
            print(f"[server] {args[0]}", flush=True)

def fetch_candles(inst_id, bar='1H', limit=300):
    url = f'https://www.okx.com/api/v5/market/history-candles?instId={inst_id}&bar={bar}&limit={limit}'
    return fetch_json(url)

if __name__ == '__main__':
    print(f"[server] Serving on {BIND_HOST}:{PORT}", flush=True)
    fetch_all_tickers()
    threading.Thread(target=ticker_refresh_loop, daemon=True).start()

    server = http.server.HTTPServer((BIND_HOST, PORT), ComboHandler)
    print(f"[server] http://{BIND_HOST}:{PORT}/contract-terminal.html", flush=True)
    server.serve_forever()

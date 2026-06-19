#!/usr/bin/env python3
"""
OKX Proxy Server — fetches real-time OKX data and relays it to local frontend.
Handles:
  - REST: /api/okx/tickers — current prices for all tracked coins
  - REST: /api/okx/candles?instId=BTC-USDT-SWAP&bar=1H&limit=300 — historical candles
  - Server-sent events: /api/okx/stream — real-time WebSocket relay via SSE
"""

import http.server
import json
import urllib.request
import urllib.parse
import ssl
import threading
import time
import asyncio
import sys
import os

PORT = 9877  # proxy port (different from the file server on 9876)

COINS = ['BTCUSDT','ETHUSDT','SOLUSDT','DOGEUSDT','BNBUSDT','AVAXUSDT','LINKUSDT','ARBUSDT']
INST_IDS = [c.replace('USDT', '-USDT-SWAP') for c in COINS]

# Cache for latest ticker data
ticker_cache = {}
ticker_cache_lock = threading.Lock()

# Cache for SSE clients
sse_clients = []
sse_clients_lock = threading.Lock()

def fetch_json(url, timeout=10):
    """Fetch JSON from OKX REST API."""
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json'
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[proxy] REST fetch failed: {url} — {e}", flush=True)
        return None

def fetch_all_tickers():
    """Fetch tickers for all tracked coins from OKX."""
    global ticker_cache
    inst_ids_str = ','.join(INST_IDS)
    url = f'https://www.okx.com/api/v5/market/tickers?instType=SWAP&instId={inst_ids_str}'
    data = fetch_json(url)
    if data and data.get('code') == '0' and data.get('data'):
        with ticker_cache_lock:
            for item in data['data']:
                ticker_cache[item['instId']] = item
        # Broadcast to SSE clients
        broadcast_sse({
            'type': 'tickers',
            'data': data['data']
        })
        return data['data']
    return None

def fetch_candles(inst_id, bar='1H', limit=300):
    """Fetch historical candles from OKX."""
    url = f'https://www.okx.com/api/v5/market/history-candles?instId={inst_id}&bar={bar}&limit={limit}'
    data = fetch_json(url)
    return data

def broadcast_sse(event):
    """Send an SSE event to all connected clients."""
    json_str = json.dumps(event, ensure_ascii=False).replace('\n', '')
    with sse_clients_lock:
        dead = []
        for client in sse_clients:
            try:
                client.write(f"data: {json_str}\n\n".encode())
                client.flush()
            except Exception:
                dead.append(client)
        for d in dead:
            sse_clients.remove(d)

class ProxyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        # CORS headers for local dev
        self.send_response(200)

        if path == '/api/okx/tickers':
            with ticker_cache_lock:
                data = list(ticker_cache.values())
            if not data:
                # Force fetch if cache is empty
                fetch_all_tickers()
                with ticker_cache_lock:
                    data = list(ticker_cache.values())
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'code':'0','data':data}, ensure_ascii=False).encode())

        elif path == '/api/okx/candles':
            inst_id = query.get('instId', [None])[0]
            bar = query.get('bar', ['1H'])[0]
            limit = int(query.get('limit', ['300'])[0])
            if not inst_id:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"error":"missing instId"}')
                return
            data = fetch_candles(inst_id, bar, limit)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

        elif path == '/api/okx/stream':
            # SSE endpoint — keep connection open and stream ticker updates
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            # Send initial data
            with ticker_cache_lock:
                for item in ticker_cache.values():
                    event_data = json.dumps({'type': 'tickers', 'data': [item]}, ensure_ascii=False).replace('\n', '')
                    self.wfile.write(f"data: {event_data}\n\n".encode())
                    self.wfile.flush()

            # Register client
            with sse_clients_lock:
                sse_clients.append(self.wfile)

            # Keep connection alive
            try:
                while True:
                    # Send heartbeat
                    self.wfile.write(f": heartbeat\n\n".encode())
                    self.wfile.flush()
                    time.sleep(15)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with sse_clients_lock:
                    if self.wfile in sse_clients:
                        sse_clients.remove(self.wfile)

        elif path == '/health':
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error":"not found"}')

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.end_headers()

    def log_message(self, format, *args):
        # Quieter logging
        if '/api/okx/stream' not in args[0]:
            print(f"[proxy] {args[0]}", flush=True)

def ticker_refresh_loop():
    """Fetch tickers every 5 seconds."""
    while True:
        try:
            fetch_all_tickers()
        except Exception as e:
            print(f"[proxy] ticker refresh error: {e}", flush=True)
        time.sleep(5)

def start_server():
    """Start the proxy HTTP server."""
    # Initial ticker fetch
    print("[proxy] Fetching initial ticker data...", flush=True)
    fetch_all_tickers()
    print(f"[proxy] Cached {len(ticker_cache)} tickers", flush=True)

    # Start background ticker refresh
    threading.Thread(target=ticker_refresh_loop, daemon=True).start()

    # Start HTTP server
    server = http.server.HTTPServer(('127.0.0.1', PORT), ProxyHandler)
    print(f"[proxy] OKX proxy running on http://127.0.0.1:{PORT}", flush=True)
    print(f"[proxy] Endpoints:", flush=True)
    print(f"  GET /api/okx/tickers", flush=True)
    print(f"  GET /api/okx/candles?instId=BTC-USDT-SWAP&bar=1H&limit=300", flush=True)
    print(f"  GET /api/okx/stream (SSE)", flush=True)
    print(f"  GET /health", flush=True)
    server.serve_forever()

if __name__ == '__main__':
    start_server()

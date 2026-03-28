"""One-time script: place limit sell (SL) orders for open NO positions on BTC and SOL."""
import os, time, base64, httpx, json
from pathlib import Path
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# Load .env
for line in Path('/opt/kalshi-bot/.env').read_text().splitlines():
    line = line.strip()
    if '=' in line and not line.startswith('#'):
        k, _, v = line.partition('=')
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

key_id = os.environ.get('KALSHI_API_KEY', '')
with open('/opt/kalshi-bot/kalshi_private_key.pem', 'rb') as f:
    key = serialization.load_pem_private_key(f.read(), password=None)

BASE = 'https://api.elections.kalshi.com/trade-api/v2'

def auth_headers(method, path):
    ts = str(int(time.time() * 1000))
    msg = (ts + method.upper() + path).encode()
    sig = key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    return {
        'Content-Type': 'application/json',
        'KALSHI-ACCESS-KEY': key_id,
        'KALSHI-ACCESS-TIMESTAMP': ts,
        'KALSHI-ACCESS-SIGNATURE': base64.b64encode(sig).decode(),
    }

def get(path):
    r = httpx.get(BASE + path, headers=auth_headers('GET', '/trade-api/v2' + path))
    r.raise_for_status()
    return r.json()

def place_sell(ticker, side, price_cents):
    path = '/trade-api/v2/portfolio/orders'
    body = {
        'ticker': ticker,
        'action': 'sell',
        'side': side,
        'type': 'limit',
        'count': 1,  # will be overridden per position
    }
    if side == 'yes':
        body['yes_price'] = price_cents
    else:
        body['no_price'] = price_cents
    r = httpx.post(BASE + '/portfolio/orders', headers=auth_headers('POST', path), json=body)
    print(f"  -> {r.status_code}: {r.text[:300]}")
    return r

# Get open positions
print("Fetching open positions...")
data = get('/portfolio/positions')
positions = data.get('market_positions', [])

print(f"Found {len(positions)} positions:")
for p in positions:
    ticker = p.get('market_id', '')
    side = 'yes' if p.get('position', 0) > 0 else 'no'
    qty = abs(p.get('position', 0))
    print(f"  {ticker}  side={side}  qty={qty}")

# Find BTC and SOL NO positions
btc_pos = [p for p in positions if 'KXBTC' in p.get('market_id','') and p.get('position',0) < 0]
sol_pos = [p for p in positions if 'KXSOL' in p.get('market_id','') and p.get('position',0) < 0]

print(f"\nBTC NO positions: {len(btc_pos)}")
print(f"SOL NO positions: {len(sol_pos)}")

# SL levels — set above avg entry to lock in profit
# BTC avg entry 60.43c → SL at 65c (locks in profit above entry)
# SOL avg entry 76.17c → SL at 78c (locks in profit above entry)
BTC_SL = 65
SOL_SL = 78

for p in btc_pos:
    ticker = p['market_id']
    qty = abs(p['position'])
    print(f"\nPlacing BTC NO sell x{qty} @ {BTC_SL}c on {ticker}")
    body = {
        'ticker': ticker,
        'action': 'sell',
        'side': 'no',
        'type': 'limit',
        'count': qty,
        'no_price': BTC_SL,
    }
    path = '/trade-api/v2/portfolio/orders'
    r = httpx.post(BASE + '/portfolio/orders', headers=auth_headers('POST', path), json=body)
    print(f"  -> {r.status_code}: {r.text[:300]}")

for p in sol_pos:
    ticker = p['market_id']
    qty = abs(p['position'])
    print(f"\nPlacing SOL NO sell x{qty} @ {SOL_SL}c on {ticker}")
    body = {
        'ticker': ticker,
        'action': 'sell',
        'side': 'no',
        'type': 'limit',
        'count': qty,
        'no_price': SOL_SL,
    }
    path = '/trade-api/v2/portfolio/orders'
    r = httpx.post(BASE + '/portfolio/orders', headers=auth_headers('POST', path), json=body)
    print(f"  -> {r.status_code}: {r.text[:300]}")

print("\nDone.")

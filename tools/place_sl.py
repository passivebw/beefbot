"""Place SL limit sell orders for ALL open positions."""
import os, time, base64, httpx
from pathlib import Path
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

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

def place_order(ticker, side, action, price_cents, qty):
    path = '/trade-api/v2/portfolio/orders'
    body = {
        'ticker': ticker,
        'action': action,
        'side': side,
        'type': 'limit',
        'count': qty,
    }
    if side == 'yes':
        body['yes_price'] = price_cents
    else:
        body['no_price'] = price_cents
    r = httpx.post(BASE + '/portfolio/orders', headers=auth_headers('POST', path), json=body)
    print(f"  -> {r.status_code}: {r.text[:300]}")
    return r

# SL levels
YES_SL = 50  # sell YES at 50c if position reverses
NO_SL  = 50  # sell NO at 50c if position reverses

print("Fetching open positions...")
data = get('/portfolio/positions')
print("RAW RESPONSE:", str(data)[:2000])
positions = data.get('market_positions', data.get('positions', data.get('holdings', [])))

active = [(p['market_id'], p['position']) for p in positions if p.get('position', 0) != 0]
print(f"Found {len(active)} active positions:")
for ticker, pos in active:
    side = 'yes' if pos > 0 else 'no'
    qty = abs(pos)
    print(f"  {ticker}  {side.upper()} x{qty}")

if not active:
    print("No open positions — nothing to do.")
else:
    for ticker, pos in active:
        if pos > 0:
            qty = pos
            print(f"\nPlacing SL: sell YES x{qty} @ {YES_SL}c on {ticker}")
            place_order(ticker, 'yes', 'sell', YES_SL, qty)
        else:
            qty = abs(pos)
            print(f"\nPlacing SL: sell NO x{qty} @ {NO_SL}c on {ticker}")
            place_order(ticker, 'no', 'sell', NO_SL, qty)

print("\nDone.")

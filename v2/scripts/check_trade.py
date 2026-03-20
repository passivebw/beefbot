"""Print raw trade data for one KXBTC15M contract to find correct field names."""
import base64, time, os, json
from pathlib import Path
import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

for line in Path(".env").read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, _, v = line.partition("=")
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

api_key  = os.environ.get("KALSHI_API_KEY", "b16ab35a-2d26-4bd3-93c6-a1e8fd6c7a95")
key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "/opt/kalshi-bot/kalshi_private_key.pem")

with open(key_path, "rb") as f:
    key = serialization.load_pem_private_key(f.read(), password=None)

def auth_headers(method, path):
    ts = str(int(time.time() * 1000))
    msg = (ts + method.upper() + path).encode()
    sig = key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": api_key, "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}

BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Get a recently closed market ticker from DB
import sqlite3
conn = sqlite3.connect("analysis/market_data.db")
row = conn.execute("SELECT ticker FROM contracts WHERE result IN ('yes','no') LIMIT 1").fetchone()
conn.close()

if not row:
    print("No settled contracts in DB yet")
    exit()

ticker = row[0]
print(f"Fetching trades for: {ticker}")

path = f"/trade-api/v2/markets/{ticker}/trades"
r = httpx.get(BASE + f"/markets/{ticker}/trades",
              headers=auth_headers("GET", path),
              params={"limit": 5}, timeout=10)

print(f"HTTP {r.status_code}")
data = r.json()
trades = data.get("trades", [])
print(f"Trades returned: {len(trades)}")
if trades:
    print("\nFirst trade raw fields:")
    for k, v in trades[0].items():
        print(f"  {k:30} = {v}")
    print("\nAll trades (price fields only):")
    for t in trades:
        print({k: v for k, v in t.items() if any(x in k.lower() for x in ["price", "count", "side", "time"])})

"""Find crypto 15-min series tickers on Kalshi."""
import base64, time, os
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

# Try guessed tickers directly by fetching open markets
guesses = ["KXDOGE15M", "KXBNB15M", "KXHYPE15M",
           "KXETH15M", "KXSOL15M", "KXXRP15M",
           "KXDOGE", "KXBNB", "KXHYPE"]

print("=== Testing series tickers (open markets) ===")
for ticker in guesses:
    path = "/trade-api/v2/markets"
    r = httpx.get(BASE + "/markets", headers=auth_headers("GET", path),
                  params={"series_ticker": ticker, "status": "open", "limit": 1}, timeout=10)
    markets = r.json().get("markets", []) if r.status_code == 200 else []
    if markets:
        m = markets[0]
        print(f"  FOUND: {ticker:20} → {m.get('ticker')}  status={m.get('status')}")
    else:
        print(f"  {ticker:20} → HTTP {r.status_code}  (no markets)")
    time.sleep(0.3)

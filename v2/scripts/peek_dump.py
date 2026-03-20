"""Find all 7 crypto 15-min series on Kalshi."""
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
key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", str(Path("kalshi_private_key.pem").resolve()))

with open(key_path, "rb") as f:
    key = serialization.load_pem_private_key(f.read(), password=None)

def auth_headers(method, path):
    ts = str(int(time.time() * 1000))
    msg = (ts + method.upper() + path).encode()
    sig = key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": api_key, "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}

BASE  = "https://api.elections.kalshi.com/trade-api/v2"
MPATH = "/trade-api/v2/markets"

candidates = [
    "KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M",
    "KXDOGE15M", "KXBNB15M", "KXHYPE15M",
    "KXBTC-15M", "KXETH-15M", "KXSOL-15M",
    "BTC15M", "ETH15M", "SOL15M", "XRP15M",
    "KXDOGE", "KXBNB", "KXHYPE", "KXSOL", "KXETH", "KXXRP",
]

print(f"{'Series':>15}  Result")
print(f"{'─'*15}  {'─'*40}")
found = []
for ticker in candidates:
    r = httpx.get(BASE + "/markets", headers=auth_headers("GET", MPATH),
                  params={"series_ticker": ticker, "limit": 1}, timeout=10)
    markets = r.json().get("markets", []) if r.status_code == 200 else []
    if markets:
        m = markets[0]
        print(f"  {ticker:>13}  FOUND → {m.get('ticker')}  status={m.get('status')}")
        found.append(ticker)
    else:
        print(f"  {ticker:>13}  HTTP {r.status_code} / no markets")
    time.sleep(0.3)

print(f"\nFound {len(found)}: {found}")

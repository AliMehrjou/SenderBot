import requests

resp = requests.get(
    "https://databay.com/api/v1/proxy-list",
    params={"ssl": "strict", "protocol": "socks5", "format": "json"},
    timeout=10,
)
proxies = resp.json()["data"]

for p in proxies:
    print(f"{p['protocol']}://{p['ip']}:{p['port']}  ({p['iso']}, {p['latency']}ms, {p['anonymity']})")
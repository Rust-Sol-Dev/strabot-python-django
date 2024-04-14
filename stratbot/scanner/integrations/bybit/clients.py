from __future__ import annotations

from ccxt import bybit
from ccxt.async_support import bybit as async_bybit

proxy_url = 'http://165.227.33.133:3128'
proxies = {
  'http': proxy_url,
  'https': proxy_url,
}

client = bybit()
client.proxies = proxies

async_client = async_bybit()
async_client.proxies = proxies

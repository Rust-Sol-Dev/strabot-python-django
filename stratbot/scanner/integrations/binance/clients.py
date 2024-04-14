from __future__ import annotations

from binance.um_futures import UMFutures


proxies = {
  'https': 'http://138.197.142.106:3128'
}
client = UMFutures(proxies=proxies)

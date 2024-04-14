from __future__ import annotations

from tda.client.synchronous import Client

from .clients import client


class TDABridge:
    def __init__(self, client: Client):
        self.client = client


tda_bridge = TDABridge(client)

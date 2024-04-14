from __future__ import annotations
import logging
from datetime import datetime


from stratbot.scanner.models.symbols import SymbolType, SymbolRec, SymbolTypeManager
from stratbot.scanner.ops import historical
from .base import LiveLoop


logger = logging.getLogger(__name__)


class CryptoLoop(LiveLoop):
    symbol_type = SymbolType.CRYPTO
    intraday_timeframes = SymbolTypeManager.intraday_timeframes(symbol_type)
    scan_timeframes = SymbolTypeManager.scan_timeframes(symbol_type)
    display_timeframes = SymbolTypeManager.display_timeframes(symbol_type)
    tfc_timeframes = SymbolTypeManager.tfc_timeframes(symbol_type)
    logger = logger

    def _refresh_latest_prices(self) -> None:
        self._quotes = historical.refresh_crypto_quotes()

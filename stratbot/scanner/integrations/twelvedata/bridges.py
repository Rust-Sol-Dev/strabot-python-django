from __future__ import annotations
import logging
from datetime import datetime
from time import perf_counter

import pandas as pd
from twelvedata import TDClient, exceptions

from stratbot.scanner.models.timeframes import Timeframe
from . import exchange
from .clients import client


log = logging.getLogger(__name__)


class TwelveDataBridge:
    def __init__(self, client: TDClient):
        self.client = client
        self.session = None
        self.exchange_id = exchange.ID

    def _parse_df(self, symbol: str, df: tuple[dict]) -> pd.DataFrame:
        df = pd.DataFrame(data=df)
        df['symbol'] = symbol
        # df['exchange'] = self.exchange_id
        df.index = df.index.tz_localize('UTC')
        df.index.name = 'time'
        return df

    def historical(
            self,
            symbol: str,
            tf: Timeframe,
            start_date: datetime,
            end_date: datetime = None,
    ) -> pd.DataFrame:

        interval = exchange.INTERVALS[tf]
        end_date = end_date if end_date else datetime.utcnow()

        s = perf_counter()
        try:
            ts = self.client.time_series(
                symbol=symbol,
                interval=interval,
                outputsize=5000,
                start_date=start_date,
                end_date=end_date,
                timezone="UTC",
                order='asc',
            )
            df = ts.as_pandas()
            df = self._parse_df(symbol, df)
        except exceptions.BadRequestError as e:
            log.error(f'{self.exchange_id} exception [{tf=}]: {symbol} - {e}')
            return pd.DataFrame()

        elapsed = perf_counter() - s
        elapsed_ms = elapsed * 1000
        log.info(
            f'{self.exchange_id}: {symbol} [{tf}] queried {len(df)} records in {elapsed_ms:.4f} ms ({elapsed:.2f} s)')

        return df


twelvedata_bridge = TwelveDataBridge(client)

class CoinbasePro(DataSource):
    def __init__(self):
        super().__init__()
        self.client = coinbasepro_client
        self.async_client = coinbasepro_async_client

    @staticmethod
    def _json_to_df(symbol, json_data, tf):
        df = pd.DataFrame(json_data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df['symbol'] = symbol
        df['timestamp'] = pd.to_datetime(df['time'], unit='ms', utc=True)
        del df['time']
        df.set_index('timestamp', inplace=True)
        return symbol, stratify_df(df, tf)

    @func_timer_async
    async def historical(self, symbols: list, tf: str = '5', start_date: datetime = None) -> dict:
        timeframe_mapping = {
            '5': '5m',
            'D': '1d',
        }
        if start_date:
            start_date = int(start_date.timestamp()) * 1000
        tasks = []
        s = time.perf_counter()
        for symbol in symbols:
            tasks.append(self._async_fetch_ohlcv(symbol, timeframe_mapping[tf], since=start_date))
        json_responses = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - s
        logging.info(f'CBP: {len(symbols)} symbols queried in {elapsed:0.2f} seconds')
        parsed_dfs = self._threaded_json_to_df(json_responses, tf=tf)
        return {symbol: df for symbol, df in parsed_dfs}

    def markets(self):
        return self.client.fetch_markets()

    # async def async_fetch_ticker(self, symbol):
    #     return await self.async_client.fetch_ticker(symbol)

    async def markets_by_volume(self, min_usd: int = 5_000_000) -> list:
        excludes = ['USDT/USD']
        tasks = []
        s = time.perf_counter()
        for row in self.markets():
            ticker = row['symbol']
            active = row['active']
            if active and ticker.endswith('USD') and ticker not in excludes:
                tasks.append(self.async_client.fetch_ticker(ticker))
        tickers = []
        responses = await asyncio.gather(*tasks)
        for resp in responses:
            try:
                ticker = resp['symbol']
                volume = float(resp['info']['volume'])
                price = float(resp['info']['price'])
                if volume * price > min_usd:
                    print(f'{ticker=}, {price=}, {volume=}, $volume={volume * price}')
                    tickers.append(f'{ticker}')
            except (KeyError, NameError):
                pass
        elapsed = time.perf_counter() - s
        logging.info(f"initialized in {elapsed:0.2f} seconds")
        return tickers

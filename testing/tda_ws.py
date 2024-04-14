import asyncio
import pprint
import os
import sys
sys.path.append('/Users/TLK3/PycharmProjects/stratbot2')

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
from django.conf import settings
django.setup()

from tda.streaming import StreamClient

from stratbot.scanner.models.symbols import SymbolRec, SymbolType
from stratbot.scanner.integrations.tda.clients import client as tda_client


class MyStreamConsumer:
    """
    We use a class to enforce good code organization practices
    """

    def __init__(self, symbols: list, queue_size=0):
        """
        We're storing the configuration variables within the class for easy
        access later in the code!
        """
        self.tda_client = tda_client
        self.stream_client = None
        self.symbols = symbols
        # Create a queue so we can queue up work gathered from the client
        self.queue = asyncio.Queue(queue_size)

    def initialize(self):
        """
        Create the clients and log in. Using easy_client, we can get new creds
        from the user via the web browser if necessary
        """
        self.stream_client = StreamClient(
            self.tda_client)

        # The streaming client wants you to add a handler for every service type
        self.stream_client.add_timesale_equity_handler(
            self.handle_timesale_equity)
        self.stream_client.add_chart_equity_handler(
            self.handle_chart_equity)

    async def stream(self):
        await self.stream_client.login()  # Log into the streaming service
        await self.stream_client.quality_of_service(StreamClient.QOSLevel.EXPRESS)
        await self.stream_client.timesale_equity_subs(self.symbols)
        # await self.stream_client.chart_equity_subs(self.symbols)

        # Kick off our handle_queue function as an independent coroutine
        asyncio.ensure_future(self.handle_queue())

        # Continuously handle inbound messages
        while True:
            await self.stream_client.handle_message()

    async def handle_timesale_equity(self, msg):
        """
        This is where we take msgs from the streaming client and put them on a
        queue for later consumption. We use a queue to prevent us from wasting
        resources processing old data, and falling behind.
        """
        # if the queue is full, make room
        if self.queue.full():  # This won't happen if the queue doesn't have a max size
            print('Handler queue is full. Awaiting to make room... Some messages might be dropped')
            await self.queue.get()
        await self.queue.put(msg)

    async def handle_chart_equity(self, msg):
        """
        This is where we take msgs from the streaming client and put them on a
        queue for later consumption. We use a queue to prevent us from wasting
        resources processing old data, and falling behind.
        """
        # if the queue is full, make room
        if self.queue.full():  # This won't happen if the queue doesn't have a max size
            print('Handler queue is full. Awaiting to make room... Some messages might be dropped')
            await self.queue.get()
        await self.queue.put(msg)

    async def handle_queue(self):
        """
        Here we pull messages off the queue and process them.
        """
        while True:
            msg = await self.queue.get()
            pprint.pprint(msg)


async def main(symbols: list):
    """
    Create and instantiate the consumer, and start the stream
    """
    consumer = MyStreamConsumer(symbols)
    consumer.initialize()
    await consumer.stream()


if __name__ == '__main__':
    stock_symbols = list(SymbolRec.objects.filter(symbol_type='stock').values_list('symbol', flat=True)[0:301])
    # ['TSLA', 'PFE', 'T', 'NVDA', 'AAPL', 'OXY', 'AAL', 'F', 'AMD', 'MO', 'INTC', 'MMM', 'CSX', 'AMZN', 'KEY', 'WFC', 'HPE', 'DG', 'JNJ', 'TFC', 'CSCO', 'VZ', 'BAC', 'NFLX', 'GOOG', 'LUV', 'BKR', 'DAL', 'ORCL', 'KHC', 'GLW', 'RTX', 'KVUE', 'HPQ', 'ICE', 'CCL', 'XOM', 'GOOGL', 'BA', 'MS', 'DVN', 'PYPL', 'GM', 'JCI', 'HBAN', 'RF', 'NEM', 'D', 'NEE', 'META', 'CTVA', 'EBAY', 'NCLH', 'BMY', 'CMCSA', 'NKE', 'BSX', 'BWA', 'C', 'SLB', 'BK', 'FOXA', 'EL', 'UAL', 'MSFT', 'USB', 'NI', 'PPL', 'MU', 'CARR', 'MRK', 'EXC', 'IR', 'CPRT', 'PCAR', 'ABBV', 'MGM', 'MDT', 'CVS', 'MTCH', 'BRK.B', 'KDP', 'ATVI', 'TMUS', 'COP', 'AXP', 'APA', 'PCG', 'AEP', 'SO', 'XEL', 'V', 'LOW', 'LNC', 'WBD', 'HON', 'LLY', 'CBRE', 'CNC', 'FIS', 'MNST', 'MRNA', 'KO', 'ROL', 'ON', 'PM', 'TJX', 'WMB', 'ABT', 'TXN', 'FSLR', 'OKE', 'DIS', 'CMA', 'DUK', 'MET', 'IBM', 'HAL', 'NRG', 'PXD', 'CNP', 'UPS', 'K', 'SEE', 'HRL', 'KMI', 'FTV', 'AIG', 'EA', 'JPM', 'WMT', 'GNRC', 'PANW', 'FCX', 'AMCR', 'CTRA', 'ZTS', 'WBA', 'ZION', 'CRM', 'UNH', 'EW', 'EMR', 'SBUX', 'DHI', 'WRK', 'VTRS', 'CFG', 'FMC', 'FANG', 'ES', 'HLT', 'BBWI', 'ILMN', 'OGN', 'AES', 'ALL', 'SYF', 'CVX', 'HWM', 'QCOM', 'ALK', 'TGT', 'DD', 'NDAQ', 'NXPI', 'GIS', 'MPC', 'PEG', 'SEDG', 'GEHC', 'ANET', 'VFC', 'CTSH', 'PARA', 'SCHW', 'CAH', 'FI', 'HOLX', 'JNPR', 'COR', 'ACN', 'FAST', 'ENPH', 'VLO', 'PNC', 'A', 'PG', 'EXPE', 'PHM', 'BALL', 'UNP', 'FITB', 'PSX', 'ADBE', 'NWL', 'KMB', 'WYNN', 'MCHP', 'AMGN', 'STT', 'APH', 'EQT', 'DOW', 'GILD', 'CAT', 'LEN', 'DXCM', 'ADI', 'IVZ', 'MA', 'GE', 'SRE', 'MRO', 'KR', 'ED', 'MOS', 'IPG', 'LYB', 'ETSY', 'CDNS', 'ISRG', 'CL', 'FE', 'WDC', 'MAS', 'CSGP', 'MAR', 'IFF', 'AVGO', 'STX', 'CB', 'COST', 'FTNT', 'SWKS', 'MDLZ', 'KMX', 'AMAT', 'ETR', 'TPR', 'HAS', 'PAYX', 'BAX', 'INCY', 'BEN', 'PRU', 'IP', 'TAP', 'ALB', 'BBY', 'ETN', 'PGR', 'OMC', 'CTLT', 'PEP', 'AFL', 'CEG', 'ADM', 'ACGL', 'TER', 'HD', 'GEN', 'LVS', 'CF', 'LYV', 'EOG', 'TSN', 'NWSA', 'HES', 'RCL', 'CPB', 'OTIS', 'GS', 'COF', 'SYY', 'ROST', 'DHR', 'ADP', 'DXC', 'CMS', 'GPN', 'DLTR', 'NTAP', 'FDX', 'CAG', 'PPG', 'DFS', 'CZR', 'MCD', 'APTV']
    asyncio.run(main(stock_symbols))

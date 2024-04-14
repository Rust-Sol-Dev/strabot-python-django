import pandas as pd
from django.db import models
from django_pandas.managers import DataFrameManager, DataFrameQuerySet
from timescale.db.models.models import TimescaleModel
from timescale.db.models.fields import TimescaleDateTimeField
from timescale.db.models.managers import TimescaleManager

from .timeframes import Timeframe


class PriceRecQuerySet(DataFrameQuerySet):
    def as_df(self) -> pd.DataFrame:
        df = self.to_timeseries(index='bucket')
        df.index.rename('time', inplace=True)
        return df


class StockPriceRecMaterializedView(models.Model):
    class Meta:
        abstract = True
        # TODO: decide on common ordering for views and DataFrames. Cannot negative index in django so newest
        #  first must be used to get the latest data. stratify_df() expects oldest first.
        ordering = ["bucket"]
        get_latest_by = ["bucket"]

    bucket = models.DateTimeField("Bucket", primary_key=True)
    symbol = models.CharField("Symbol", max_length=15)
    open = models.FloatField("Open")
    high = models.FloatField("High")
    low = models.FloatField("Low")
    close = models.FloatField("Close")
    volume = models.BigIntegerField("Volume")

    objects = models.Manager()
    df = PriceRecQuerySet.as_manager()

    def __str__(self):
        return f"[{self.symbol}] {self.bucket} | {self.open} | {self.high} | {self.low} | {self.close}"


class StockPriceRec(TimescaleModel):
    time = TimescaleDateTimeField(interval="1 year")
    symbol = models.CharField("Symbol", max_length=32, db_index=True)
    # exchange = models.CharField("Exchange", max_length=32, db_index=True)
    open = models.DecimalField(max_digits=10, decimal_places=4)
    high = models.DecimalField(max_digits=10, decimal_places=4)
    low = models.DecimalField(max_digits=10, decimal_places=4)
    close = models.DecimalField(max_digits=10, decimal_places=4)
    volume = models.DecimalField(max_digits=20, decimal_places=1)
    transactions = models.IntegerField(null=True, blank=True)
    vwap = models.DecimalField(max_digits=10, decimal_places=4, null=True, blank=True)

    timescale = TimescaleManager()
    df = DataFrameManager()

    class Meta:
        db_table = 'stock_pricerec'
        constraints = [
            models.UniqueConstraint(
                # fields=["time", "symbol", "exchange"],
                fields=["time", "symbol"],
                name="stock_time_sym_idx",
            )
        ]
        indexes = [
            models.Index(fields=['time', 'symbol']),
            models.Index(fields=['symbol']),
        ]
        ordering = ["time"]
        get_latest_by = ["time"]

    def __str__(self):
        return f"[{self.symbol}] {self.time} | {self.open} | {self.high} | {self.low} | {self.close}"


class StockPriceRecView5(StockPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "stock_pricerec_5"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class StockPriceRecView15(StockPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "stock_pricerec_15"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class StockPriceRecView30(StockPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "stock_pricerec_30"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class StockPriceRecViewD(StockPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "stock_pricerec_d"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class StockPriceRecViewW(StockPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "stock_pricerec_w"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class StockPriceRecViewM(StockPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "stock_pricerec_m"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class StockPriceRecViewQ(StockPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "stock_pricerec_q"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class StockPriceRecViewY(StockPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "stock_pricerec_y"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


# ===============================================================


class CryptoPriceRec(TimescaleModel):
    time = TimescaleDateTimeField(interval="1 year")
    symbol = models.CharField("Symbol", max_length=20, db_index=True)
    exchange = models.CharField("Exchange", max_length=32, db_index=True)
    open = models.DecimalField(max_digits=20, decimal_places=8)
    high = models.DecimalField(max_digits=20, decimal_places=8)
    low = models.DecimalField(max_digits=20, decimal_places=8)
    close = models.DecimalField(max_digits=20, decimal_places=8)
    volume = models.DecimalField(max_digits=20, decimal_places=8, null=True)
    contract_type = models.CharField(max_length=32, null=True, blank=True)

    timescale = TimescaleManager()
    df = DataFrameManager()

    class Meta:
        db_table = 'crypto_pricerec'
        constraints = [
            models.UniqueConstraint(
                fields=["time", "symbol", "exchange"],
                name="crypto_time_sym_ex_idx",
            )
        ]
        indexes = [
            models.Index(fields=['time', 'symbol']),
            models.Index(fields=['symbol']),
        ]
        ordering = ["time"]
        get_latest_by = ["time"]

    def __str__(self):
        return f"[{self.symbol}] {self.time} | {self.open} | {self.high} | {self.low} | {self.close}"


class CryptoPriceRecMaterializedView(models.Model):
    class Meta:
        abstract = True
        # TODO: decide on common ordering for views and DataFrames. Cannot negative index in django so newest
        #  first must be used to get the latest data. stratify_df() expects oldest first.
        ordering = ["bucket"]
        get_latest_by = ["bucket"]

    bucket = models.DateTimeField("Bucket", primary_key=True)
    symbol = models.CharField("Symbol", max_length=15)
    exchange = models.CharField("Exchange", max_length=32)
    open = models.FloatField("Open")
    high = models.FloatField("High")
    low = models.FloatField("Low")
    close = models.FloatField("Close")
    volume = models.BigIntegerField("Volume")

    objects = models.Manager()
    df = PriceRecQuerySet.as_manager()

    def __str__(self):
        return f"[{self.symbol}] {self.bucket} | {self.open} | {self.high} | {self.low} | {self.close}"


class CryptoPriceRecView5(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_5"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecView15(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_15"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecView30(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_30"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecView60(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_60"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecView4H(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_4h"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecView6H(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_6h"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecView12H(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_12h"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecViewD(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_d"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecViewW(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_w"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecViewM(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_m"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecViewQ(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_q"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


class CryptoPriceRecViewY(CryptoPriceRecMaterializedView):
    class Meta:
        managed = False
        db_table = "crypto_pricerec_y"
        ordering = ['bucket']
        get_latest_by = ["bucket"]


STOCK_TF_PRICEREC_MODEL_MAP = {
    Timeframe.MINUTES_5: StockPriceRecView5,
    Timeframe.MINUTES_15: StockPriceRecView15,
    Timeframe.MINUTES_30: StockPriceRecView30,
    Timeframe.MINUTES_60: StockPriceRecView30,
    Timeframe.HOURS_4: StockPriceRecView30,
    Timeframe.DAYS_1: StockPriceRecViewD,
    Timeframe.WEEKS_1: StockPriceRecViewW,
    Timeframe.MONTHS_1: StockPriceRecViewM,
    Timeframe.QUARTERS_1: StockPriceRecViewQ,
    Timeframe.YEARS_1: StockPriceRecViewY,
}

CRYPTO_TF_PRICEREC_MODEL_MAP = {
    Timeframe.MINUTES_5: CryptoPriceRecView5,
    Timeframe.MINUTES_15: CryptoPriceRecView15,
    Timeframe.MINUTES_30: CryptoPriceRecView30,
    Timeframe.MINUTES_60: CryptoPriceRecView60,
    Timeframe.HOURS_4: CryptoPriceRecView4H,
    Timeframe.HOURS_6: CryptoPriceRecView6H,
    Timeframe.HOURS_12: CryptoPriceRecView12H,
    Timeframe.DAYS_1: CryptoPriceRecViewD,
    Timeframe.WEEKS_1: CryptoPriceRecViewW,
    Timeframe.MONTHS_1: CryptoPriceRecViewM,
    Timeframe.QUARTERS_1: CryptoPriceRecViewQ,
    Timeframe.YEARS_1: CryptoPriceRecViewY,
}


# class CryptoTrade(TimescaleModel):
#     """
#     {
#       "e": "aggTrade",  // Event type
#       "E": 123456789,   // Event time
#       "s": "BTCUSDT",    // Symbol
#       "a": 5933014,     // Aggregate trade ID
#       "p": "0.001",     // Price
#       "q": "100",       // Quantity
#       "f": 100,         // First trade ID
#       "l": 105,         // Last trade ID
#       "T": 123456785,   // Trade time
#       "m": true,        // Is the buyer the market maker?
#     }
#     """
#
#     event_type = models.CharField(max_length=20, verbose_name="Event Type")
#     event_time = models.BigIntegerField(verbose_name="Event Time")
#     symbol = models.CharField("Symbol", max_length=32, db_index=True)
#     agg_trade_id = models.IntegerField(verbose_name="Aggregate Trade ID")
#     price = models.DecimalField(max_digits=10, decimal_places=4, verbose_name="Price")
#     quantity = models.DecimalField(max_digits=10, decimal_places=4, verbose_name="Quantity")
#     first_trade_id = models.IntegerField(verbose_name="First Trade ID")
#     last_trade_id = models.IntegerField(verbose_name="Last Trade ID")
#     # T = models.BigIntegerField(verbose_name="Trade time")
#     time = TimescaleDateTimeField(interval="7 days")
#     is_market_maker = models.BooleanField(default=False, verbose_name="Is buyer the market maker?")
#
#     timescale = TimescaleManager()
#     df = DataFrameManager()
#
#     class Meta:
#         verbose_name = "Trade Event"
#         verbose_name_plural = "Trade Events"
#
#         db_table = 'crypto_trades'
#
#         # constraints = [
#         #     models.UniqueConstraint(
#         #         # fields=["time", "symbol", "exchange"],
#         #         fields=["time", "symbol"],
#         #         name="stock_time_sym_idx",
#         #     )
#         # ]
#
#         indexes = [
#             models.Index(fields=['time', 'symbol']),
#             models.Index(fields=['symbol']),
#         ]
#
#         ordering = ["time"]
#         get_latest_by = ["time"]
#
#     # def __str__(self):
#     #     return f"[{self.symbol}] {self.time} | {self.open} | {self.high} | {self.low} | {self.close}"


class StockTrade(TimescaleModel):
    """
    https://docs.alpaca.markets/docs/real-time-stock-pricing-data#data-point-schemas
    {
        "T":"t"
        "S":"MPC"
        "i":52983558721364
        "x":"N"
        "p":148.12
        "s":30
        "c":[" ", "I"]
        "z":"A"
        "t":"2023-12-15T19:57:15.696265728Z"
    }
    """
    time = TimescaleDateTimeField(interval="1 day")
    symbol = models.CharField("Symbol", max_length=32, db_index=True)
    trade_id = models.BigIntegerField(verbose_name="Trade ID")
    exchange_code = models.CharField(max_length=1, verbose_name="Exchange Code")
    price = models.DecimalField(max_digits=10, decimal_places=4, verbose_name="Price")
    size = models.IntegerField(verbose_name="Size")
    conditions = models.JSONField(verbose_name="Conditions")
    tape = models.CharField(max_length=1, verbose_name="Tape")

    timescale = TimescaleManager()
    df = DataFrameManager()

    class Meta:
        verbose_name = "Trade"
        verbose_name_plural = "Trades"

        db_table = 'stock_trades'

        constraints = [
            models.UniqueConstraint(
                fields=["time", "symbol", "trade_id"],
                name="stock_time_sym_trade_idx",
            )
        ]

        indexes = [
            models.Index(fields=['time', 'symbol']),
            models.Index(fields=['symbol']),
        ]

        ordering = ["time"]
        get_latest_by = ["time"]


class CryptoTrade(TimescaleModel):
    """
    {
        'e': 'aggTrade',
        'E': 1703448795188,
        'a': 180219672,
        's': 'SUSHIUSDT',
        'p': '1.2418',
        'q': '28',
        'f': 391127953,
        'l': 391127953,
        'T': 1703448795036,
        'm': True
    }
    """
    time = TimescaleDateTimeField(interval="1 day")
    symbol = models.CharField("Symbol", max_length=32, db_index=True)
    trade_id = models.BigIntegerField(verbose_name="Trade ID")
    price = models.DecimalField(max_digits=20, decimal_places=8, verbose_name="Price")
    size = models.DecimalField(max_digits=20, decimal_places=8, verbose_name="Size")
    market_maker = models.BooleanField(verbose_name="Market Maker")

    timescale = TimescaleManager()
    df = DataFrameManager()

    class Meta:
        db_table = 'crypto_trades'

        constraints = [
            models.UniqueConstraint(
                fields=["time", "symbol", "trade_id"],
                name="crypto_time_sym_trade_idx",
            )
        ]

        indexes = [
            models.Index(fields=['time', 'symbol']),
            models.Index(fields=['symbol']),
        ]

        ordering = ["time"]
        get_latest_by = ["time"]


class StagingOHLCV(models.Model):
    id = models.BigAutoField(primary_key=True)
    symbol = models.CharField(max_length=64)
    tf = models.CharField(max_length=3)
    ohlcv = models.JSONField()

    class Meta:
        managed = False
        db_table = 'staging_ohlcv'

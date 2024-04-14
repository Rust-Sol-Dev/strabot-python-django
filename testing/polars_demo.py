import polars as pl

# Load the CSV into a Polars DataFrame
df_polars = pl.read_csv('sample.csv')

# Convert the 'time' column to datetime format
# Perform the resampling and aggregation
df_polars = (
    df_polars
    .sort("time")
    .groupby(pl.col("time").cast(pl.Datetime).dt.round("30 min"))
    .agg([
        pl.col("open").first().alias("open"),
        pl.col("high").max().alias("high"),
        pl.col("low").min().alias("low"),
        pl.col("close").last().alias("close"),
        pl.col("volume").sum().alias("volume"),
        pl.col("transactions").sum().alias("transactions"),
    ])
)
print(df_polars.head())


# ======================================================================================================================


from time import perf_counter
import polars_demo as pl

queryset = CryptoTrade.objects.filter(symbol='BTCUSDT').order_by('time')
data = queryset.values('time', 'symbol', 'trade_id', 'price', 'size', 'market_maker')
data_list = list(data)
df = pl.DataFrame(data_list)

s = perf_counter()
ohlcv = (
    df.lazy()
    .sort('time')
    .group_by_dynamic(index_column="time", every="1m", period="1m", offset="0ns", include_boundaries=False)
    .agg([
        pl.col("price").first().alias("open"),
        pl.col("price").max().alias("high"),
        pl.col("price").min().alias("low"),
        pl.col("price").last().alias("close"),
        pl.col("size").sum().alias("volume")
    ])
    .collect()
)
len(ohlcv)
elapsed = perf_counter() - s
print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

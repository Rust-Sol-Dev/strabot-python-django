from django.db import models

from .symbols import SymbolType


class LiveLoop(models.Model):
    symbol_type = models.CharField(max_length=7, choices=SymbolType.choices)

    start_datetime = models.DateTimeField()
    start_perf = models.FloatField()
    last_datetime = models.DateTimeField()
    last_perf = models.FloatField()

    last_stats_flush_at = models.DateTimeField()

    last_run_number = models.PositiveBigIntegerField()
    num_setups_examined = models.PositiveBigIntegerField()
    num_setups_updated = models.PositiveBigIntegerField()
    num_setups_triggered = models.PositiveBigIntegerField()
    num_alerts_attempted = models.PositiveBigIntegerField()


class LiveLoopRun(models.Model):
    loop = models.ForeignKey(LiveLoop, on_delete=models.CASCADE)

    start_datetime = models.DateTimeField()
    start_perf = models.FloatField()
    end_datetime = models.DateTimeField()
    end_perf = models.FloatField()

    run_number = models.PositiveBigIntegerField()
    fully_ran = models.BooleanField()
    exit_reason = models.TextField()
    exit_reason_detail = models.TextField()
    symbols_refreshed = models.BooleanField()
    symbols_refreshed_duration = models.FloatField(blank=True, null=True, default=None)
    num_symbols = models.PositiveIntegerField(blank=True, null=True, default=None)
    setups_refreshed = models.BooleanField()
    setups_refresh_duration = models.FloatField(blank=True, null=True, default=None)
    num_setups = models.PositiveIntegerField(blank=True, null=True, default=None)
    price_records_refreshed = models.BooleanField()
    price_records_refreshed_duration = models.FloatField(
        blank=True, null=True, default=None
    )
    num_price_records = models.PositiveIntegerField(blank=True, null=True, default=None)
    num_setups_examined = models.PositiveBigIntegerField()
    num_setups_updated = models.PositiveBigIntegerField()
    num_setups_triggered = models.PositiveBigIntegerField()
    num_alerts_attempted = models.PositiveBigIntegerField()

    flushed_at = models.DateTimeField()

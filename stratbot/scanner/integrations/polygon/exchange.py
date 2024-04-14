from stratbot.scanner.models.timeframes import Timeframe


ID = 'POLYGON'

INTERVALS = {
    Timeframe.MINUTES_1: (1, 'minute'),
    Timeframe.MINUTES_30: (30, 'minute'),
    Timeframe.DAYS_1: (1, 'day'),
}

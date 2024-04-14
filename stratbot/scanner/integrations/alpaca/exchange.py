from alpaca.data import TimeFrame, TimeFrameUnit

from stratbot.scanner.models.timeframes import Timeframe


ID = 'ALPACA'

INTERVALS = {
    Timeframe.MINUTES_1: TimeFrame.Minute,
    Timeframe.MINUTES_30: TimeFrame(30, TimeFrameUnit.Minute),
    Timeframe.DAYS_1: TimeFrame.Day,
}

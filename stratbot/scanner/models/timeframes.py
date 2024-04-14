from functools import cached_property
from datetime import timedelta
from typing import Any, Final
import re
import logging

from django.db import models


logger = logging.getLogger(__name__)


class TimeframeUnit(models.TextChoices):
    MINUTE = "minute", "Minute"
    HOUR = "hour", "Hour"
    DAY = "day", "Day"
    WEEK = "week", "Week"
    MONTH = "month", "Month"
    QUARTER = "quarter", "Quarter"
    YEAR = "year", "Year"

    __empty__ = ""

    @cached_property
    def td(self) -> timedelta:
        return _timeframe_unit_duration_mapping[str(self.value)]

    @cached_property
    def is_td_approx(self) -> bool:
        return self not in (self.MINUTE, self.HOUR, self.DAY, self.WEEK)

    @cached_property
    def singular_str(self) -> str:
        return str(self.value)

    @cached_property
    def plural_str(self) -> str:
        return f"{self.value}s"

    def __eq__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td == other.td
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td != other.td
        return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td < other.td
        return NotImplemented

    def __le__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td <= other.td
        return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td > other.td
        return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td >= other.td
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)


_timeframe_unit_char_mapping: Final[dict[str, TimeframeUnit]] = {
    "": TimeframeUnit.MINUTE,
    "H": TimeframeUnit.HOUR,
    "D": TimeframeUnit.DAY,
    "W": TimeframeUnit.WEEK,
    "M": TimeframeUnit.MONTH,
    "Q": TimeframeUnit.QUARTER,
    "Y": TimeframeUnit.YEAR,
}


_timeframe_unit_duration_mapping: Final[dict[str, timedelta]] = {
    str(TimeframeUnit.MINUTE.value): timedelta(minutes=1),
    str(TimeframeUnit.HOUR.value): timedelta(hours=1),
    str(TimeframeUnit.DAY.value): timedelta(days=1),
    str(TimeframeUnit.WEEK.value): timedelta(days=7),
    str(TimeframeUnit.MONTH.value): timedelta(days=30),
    str(TimeframeUnit.QUARTER.value): timedelta(days=90),
    str(TimeframeUnit.YEAR.value): timedelta(days=365),
}


_timeframe_string_regex: Final[re.Pattern] = re.compile(
    rf"^(\d*)([{''.join(u for u in _timeframe_unit_char_mapping if u)}]{{0,1}})$"
)


def _parse_timeframe_string(value: str) -> tuple[TimeframeUnit, int]:
    if value not in {timeframe_enum.value for timeframe_enum in Timeframe}:
        # NOTE: Currently just logging an error here, which you'd see in Sentry or on
        # your console, etc. However, you could be more aggressive and throw a
        # `ValueError` here if the timeframe string is unexpected (which might be good
        # if you want the program to immediately error out if an invalid timeframe
        # string is ever seen).
        logger.error("Unexpected timeframe string seen: '%s'.", value, stack_info=True)
    match = _timeframe_string_regex.match(value)
    assert match is not None, "Pre-condition"
    quantity: str = match.group(1) or ""
    unit: str = match.group(2) or ""
    unit_enum = _timeframe_unit_char_mapping[unit]
    quantity_int = int(quantity or "1")
    return unit_enum, quantity_int


class Timeframe(models.TextChoices):
    MINUTES_1 = "1", "1 Minute"
    MINUTES_5 = "5", "5 Minutes"
    MINUTES_15 = "15", "15 Minutes"
    MINUTES_30 = "30", "30 Minutes"
    MINUTES_60 = "60", "60 Minutes"
    HOURS_4 = "4H", "4 Hours"
    HOURS_6 = "6H", "6 Hours"
    HOURS_12 = "12H", "12 Hours"
    DAYS_1 = "D", "1 Day"
    WEEKS_1 = "W", "1 Week"
    MONTHS_1 = "M", "1 Month"
    QUARTERS_1 = "Q", "1 Quarter"
    YEARS_1 = "Y", "1 Year"

    @classmethod
    def get_case_when_with_durations(cls, field_name: str):
        """
        Sorting `Timeframe`s is straightforward in Python using and overriding `__lt__`
        (and the other ones as well) below.

        For sorting or annotating values in the database with the equivalent of the `td`
        property, we can use a `Case`/`When` statement to map the underlying DB string
        value to a `timedelta` (which Django calls a `DurationField` and PostgreSQL
        calls an `interval`).

        NOTE that `timedelta()` is equivalent to `timedelta(seconds=0)`.
        """
        when_statements: list[models.When] = [
            models.When(
                **{
                    field_name: tf,
                    "then": models.Value(tf.td, output_field=models.DurationField()),
                }
            )
            for tf in sorted(cls)
        ]
        return models.Case(
            *when_statements,
            default=models.Value(timedelta(), output_field=models.DurationField()),
        )

    @cached_property
    def components(self) -> tuple[TimeframeUnit, int]:
        return _parse_timeframe_string(str(self.value))

    @cached_property
    def unit(self) -> TimeframeUnit:
        return self.components[0]

    @cached_property
    def quantity(self) -> int:
        return self.components[1]

    @cached_property
    def td(self) -> timedelta:
        return self.unit.td * self.quantity

    @cached_property
    def is_td_approx(self) -> bool:
        return self.unit.is_td_approx

    def __eq__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td == other.td
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td != other.td
        return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td < other.td
        return NotImplemented

    def __le__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td <= other.td
        return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td > other.td
        return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if self.__class__ is other.__class__:
            return self.td >= other.td
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)


TIMEFRAMES_INTRADAY: Final[set[Timeframe]] = {
    Timeframe.MINUTES_1,
    Timeframe.MINUTES_5,
    Timeframe.MINUTES_15,
    Timeframe.MINUTES_30,
    Timeframe.MINUTES_60,
    Timeframe.HOURS_4,
    Timeframe.HOURS_6,
    Timeframe.HOURS_12,
}

TIMEFRAMES_INTERDAY: Final[set[Timeframe]] = {
    Timeframe.DAYS_1,
    Timeframe.WEEKS_1,
    Timeframe.MONTHS_1,
    Timeframe.QUARTERS_1,
    Timeframe.YEARS_1,
}

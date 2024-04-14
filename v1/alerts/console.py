from datetime import datetime
import pytz
from pytz import timezone

from rich.table import Table

from stratbot.alerts.models import SetupAlert
from stratbot.scanner.models.symbols import SymbolRec, Setup


class ConsoleAlert:
    def __init__(self):
        # now_utc = datetime.utcnow().strftime("%c")
        # now_est = datetime.now().strftime("%c")
        now_utc_short = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")
        now_est_short = datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
        table = Table(title=f'{now_utc_short} UTC ({now_est_short} EST)', width=200)
        table.add_column('SYMBOL', width=12)
        table.add_column('TFC', width=20)
        # table.add_column('FTFC %')
        table.add_column('TF', width=2)
        table.add_column('STRAT', width=17)
        # table.add_column('TFC', width=4)
        # table.add_column('IF', width=1)
        table.add_column('PMG', width=3)
        table.add_column('TRIGGER', width=10)
        table.add_column('LAST', width=10)
        table.add_column('T1', width=28)
        table.add_column('T2', width=30)
        # table.add_column('STOP', width=10)
        table.add_column('RR', width=4)
        # table.add_column('TIMESTAMP', width=26)
        table.add_column('EXPIRES', width=25)
        self.table = table

    @staticmethod
    def yellow_checkmark(value):
        checkmark = u'\u2713'
        return f'[yellow][b]{checkmark}[/b][/yellow]' if value else ''

    def add_row(self, symbolrec: SymbolRec, setup: Setup):
        alert = SetupAlert(symbolrec, setup, route='console')
        magnitude = self.yellow_checkmark(alert.setup.hit_magnitude)

        self.table.add_row(
            alert.prioritized_symbol,
            alert.timeframes_colored,
            # str(tfc_percent),
            alert.setup.tf,
            alert.strat_text,
            # str(tfc_text),
            # in_force_checkmark,
            alert.pmg_text,
            alert.directional_value_colored(alert.setup.trigger),
            str(alert.symbolrec.price),
            f'{alert.directional_value_colored(alert.setup.target)} {setup.magnitude_percent}%, ${setup.magnitude_dollars} {magnitude}',
            f'{alert.setup.targets[1:3]}',
            # str(alert.setup.stop),
            str(alert.setup.rr),
            # str(setup.timestamp),
            str(alert.setup.expires),
        )


class ConsoleStockAlert(ConsoleAlert):
    def __init__(self):
        super().__init__()
        now_utc_short = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        now_est_short = datetime.now(timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
        table = Table(title=f'{now_utc_short} UTC ({now_est_short} EST)', width=170)
        table.add_column('SYMBOL', width=5)
        table.add_column('TFC', width=12)
        # table.add_column('FTFC %')
        table.add_column('TF', width=2)
        table.add_column('STRAT', width=12)
        # table.add_column('TFC', width=4)
        # table.add_column('IF', width=1)
        table.add_column('PMG', width=3)
        table.add_column('TRIGGER', width=8)
        table.add_column('LAST', width=8)
        table.add_column('T1', width=25)
        table.add_column('T2', width=10)
        # table.add_column('STOP', width=10)
        table.add_column('RR', width=4)
        table.add_column('EXPIRES', width=25)
        self.table = table

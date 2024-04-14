# myapp/management/commands/migrate_meta.py
from django.core.management.base import BaseCommand
from stratbot.scanner.models.symbols import SymbolRec, ProviderMeta


class Command(BaseCommand):
    help = 'Migrate metadata to the new ProviderMeta model'

    def handle(self, *args, **kwargs):

        # iterate over all the instances of MainModel
        for symbolrec in SymbolRec.objects.all():

            # Check and migrate Polygon meta data
            if symbolrec.polygon_meta:
                ProviderMeta.objects.create(
                    provider_name='Polygon',
                    main_model=symbolrec,
                    meta=symbolrec.polygon_meta,
                )

            # Check and migrate Schwab meta data
            if symbolrec.schwab_meta:
                ProviderMeta.objects.create(
                    provider_name='Schwab',
                    main_model=symbolrec,
                    meta=symbolrec.schwab_meta,
                )

            # Check and migrate YFinance meta data
            if symbolrec.yfinance_meta:
                ProviderMeta.objects.create(
                    provider_name='YFinance',
                    main_model=symbolrec,
                    meta=symbolrec.yfinance_meta,
                )

        self.stdout.write(self.style.SUCCESS('Successfully migrated metadata'))

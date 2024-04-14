from __future__ import annotations

import django_filters
import redis
from django_filters.constants import EMPTY_VALUES
from django.db.models import Q, Subquery
from django.core.cache import caches
from redis.commands.search.query import Query

from stratbot.scanner.models.symbols import SymbolRec, Setup, ProviderMeta, SymbolType


cache = caches['markets']
r = cache.client.get_client(write=True)


class PatternFilterField(django_filters.CharFilter):
    def filter(self, qs, value):
        if value in EMPTY_VALUES:
            return qs

        if self.distinct:
            qs = qs.distinct()

        prev_candle, cur_candle = value.split("-")

        if prev_candle == 'x' and cur_candle == '1':
            q = Q(pattern__1='1')
        elif prev_candle == 'x' and cur_candle == '3':
            q = Q(pattern__1='3')
        elif prev_candle.startswith('2') and cur_candle.startswith('2'):
            q = Q(pattern__exact=['2U', '2D']) | Q(pattern__exact=['2D', '2U'])
        elif prev_candle.startswith('2'):
            q = Q(pattern__exact=['2U', cur_candle]) | Q(pattern__exact=['2D', cur_candle])
        elif cur_candle.startswith('2'):
            q = Q(pattern__exact=[prev_candle, '2U']) | Q(pattern__exact=[prev_candle, '2D'])
        else:
            q = Q(pattern__exact=[prev_candle, cur_candle])

        # qs = self.get_method(qs)(**{lookup: array_value})

        return qs.filter(q)


class CandleTagFilterField(django_filters.CharFilter):
    def filter(self, qs, value):
        if value in EMPTY_VALUES:
            return qs
        if self.distinct:
            qs = qs.distinct()
        match value:
            case "hammer" | "shooter":
                q = Q(candle_tag__exact=value)
            case "hammer shooter":
                q = Q(target_candle__candle_shape="hammer") & Q(trigger_candle__candle_shape="shooter")
            case "shooter hammer":
                q = Q(target_candle__candle_shape="shooter") & Q(trigger_candle__candle_shape="hammer")
            case "hammer OR shooter":
                q = Q(candle_tag__exact="hammer") | Q(candle_tag__exact="shooter")
            case "2d green hammer":
                q = Q(trigger_candle__strat_id='2D') & Q(trigger_candle__green=True) & Q(candle_tag__exact="hammer")
            case "2u red shooter":
                q = Q(trigger_candle__strat_id='2U') & Q(trigger_candle__red=True) & Q(candle_tag__exact="shooter")
            case "2d green":
                q = Q(trigger_candle__strat_id='2D') & Q(trigger_candle__green=True)
            case "2u red":
                q = Q(trigger_candle__strat_id='2U') & Q(trigger_candle__red=True)
            case _:
                q = Q()
        return qs.filter(q)


class SetupFilter(django_filters.FilterSet):
    pattern = PatternFilterField(field_name="pattern", lookup_expr="exact")
    candle_tag = CandleTagFilterField(field_name="candle_tag", lookup_expr="exact")
    sector = django_filters.CharFilter(method='filter_by_sector')
    industry = django_filters.CharFilter(method='filter_by_industry')
    market_cap = django_filters.NumberFilter(method='filter_by_market_cap')
    current_candle = django_filters.CharFilter(method='filter_by_current_candle')
    tfc = django_filters.CharFilter(method='filter_by_tfc')
    spread = django_filters.NumberFilter(method='filter_by_spread')
    min_price = django_filters.NumberFilter(field_name="symbol_rec__price", lookup_expr='gte')
    max_price = django_filters.NumberFilter(field_name="symbol_rec__price", lookup_expr='lte')

    class Meta:
        model = Setup
        fields = {
            "tf": ["exact"],
            "direction": ["exact"],
            "rr": ["gte"],
            "pmg": ["gte"],
            "potential_outside": ["exact"],
            "has_triggered": ["exact"],
            "in_force": ["exact"],
            "hit_magnitude": ["exact"],
            "gapped": ["exact"],
            "negated": ["exact"],
            "symbol_rec__atr": ["gte"],
            "symbol_rec__atr_percentage": ["gte"],
            "symbol_rec__price": ["gte", "lte"],
        }

    def __init__(self, *args, symbol_type: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.symbol_type = symbol_type

    def filter_by_sector(self, queryset, name, value):
        providermeta_ids = ProviderMeta.objects.filter(
            name='yfinance',
            meta__sectorKey=value,
        ).values('id')

        return queryset.filter(symbol_rec__provider_metas__id__in=Subquery(providermeta_ids))

    def filter_by_industry(self, queryset, name, value):
        providermeta_ids = ProviderMeta.objects.filter(
            name='yfinance',
            meta__industryKey__icontains=value,
        ).values('id')

        return queryset.filter(symbol_rec__provider_metas__id__in=Subquery(providermeta_ids))

    def filter_by_market_cap(self, queryset, name, value):
        if value is None:
            market_cap_filter = Q(name='yfinance', meta__marketCap__gte=50_000_000_000) | Q(name='yfinance', meta__marketCap__isnull=True)
        else:
            market_cap_filter = Q(name='yfinance', meta__marketCap__gte=float(value)) | Q(name='yfinance', meta__marketCap__isnull=True)

        providermeta_ids = ProviderMeta.objects.filter(market_cap_filter).values('id')
        return queryset.filter(symbol_rec__provider_metas__id__in=Subquery(providermeta_ids))

    def filter_by_current_candle(self, queryset, name, value):
        if not value:
            return queryset

        tf = self.form.cleaned_data['tf']
        query = Query(f"@sid{tf}_cur:({value})").no_content().paging(0, 1000)
        index = 'sidStockIndex' if self.symbol_type == SymbolType.STOCK else 'sidCryptoIndex'
        results = r.ft(index).search(query)
        symbols = [doc.id.split(':')[-1] for doc in results.docs]

        return queryset.filter(symbol_rec__symbol__in=symbols)

    def filter_by_tfc(self, queryset, name, value):
        if not value:
            return queryset

        if value == 'BULL':
            query = Query(f"@D:[0 1] @W:[0 1] @M:[0, 1] @Q:[0 1] @Y:[0, 1]").no_content().paging(0, 1000)
        elif value == 'BEAR':
            query = Query(f"@D:[-1 0] @W:[-1 0] @M:[-1 0] @Q:[-1 0] @Y:[-1 0]").no_content().paging(0, 1000)
        else:
            return queryset

        index = 'tfcStockIndex' if self.symbol_type == SymbolType.STOCK else 'tfcCryptoIndex'
        try:
            results = r.ft(index).search(query)
        except redis.exceptions.ResponseError:
            return queryset
        symbols = [doc.id.split(':')[-1] for doc in results.docs]

        return queryset.filter(symbol_rec__symbol__in=symbols)

    def filter_by_spread(self, queryset, name, value):
        if not value:
            return queryset

        query = Query(f"@spread_percentage:[0 {value}]").no_content().paging(0, 1000)
        try:
            results = r.ft('spreadIndex').search(query)
        except redis.exceptions.ResponseError:
            return queryset
        symbols = [doc.id.split(':')[-1] for doc in results.docs]

        return queryset.filter(symbol_rec__symbol__in=symbols)

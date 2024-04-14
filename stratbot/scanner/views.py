from __future__ import annotations
import json

from django.core.paginator import Paginator
from django.template.response import TemplateResponse
# from datetime import datetime, timezone
from django.utils import timezone
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import Q, Subquery
from django.http import HttpResponse, JsonResponse
from django.views.generic.list import ListView
from django.views.generic.detail import DetailView
from django.shortcuts import render, redirect
from rest_framework import viewsets
from rest_framework.parsers import JSONParser
from django.views.decorators.csrf import csrf_exempt
from django.core.cache import caches
from redis.commands.search.query import Query

from .models.symbols import Setup, SymbolRec, ProviderMeta
from .models.timeframes import Timeframe
from .filters import SetupFilter
from .forms import SetupFilterForm
from .ops.live_loop.crypto import CryptoLoop
from .ops.live_loop.stocks import StocksLoop
from .serializers import SymbolRecSerializer


def flowbite(request):
    return render(request, 'flowbite/dash.html')


def liveprice(request):
    symbols_list = (SymbolRec.objects
                    .filter(symbol_type='crypto')
                    .order_by('symbol')
                    .values_list('symbol', flat=True)
                    )
    symbols_json = json.dumps(list(symbols_list))
    context = {
        'symbols_list': list(symbols_list),
        'symbols_json': symbols_json,
    }
    return render(request, 'flowbite/liveprice.html', context)


# class SymbolDetailView(LoginRequiredMixin, DetailView):
class SymbolListView(ListView):
    model = SymbolRec
    template_name = "dashboard/symbols_list.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['stock_symbols'] = SymbolRec.objects.filter(symbol_type='stock').order_by('symbol')
        context['crypto_symbols'] = SymbolRec.objects.filter(symbol_type='crypto').order_by('symbol')
        return context


# class SymbolDetailView(LoginRequiredMixin, DetailView):
class SymbolDetailView(DetailView):
    model = SymbolRec
    template_name = "dashboard/symbol_detail.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['setups'] = (
            Setup.objects
            .filter(symbol_rec=self.object)
            .filter(expires__gt=timezone.now())
            .annotate(tf_sorted=Timeframe.get_case_when_with_durations('tf'))
            .order_by('tf_sorted', 'direction', 'pattern__1')
        )
        return context


# class SetupListView(LoginRequiredMixin, ListView):
class SetupListView(ListView):
    model = Setup
    # ordering = ["symbol_rec__symbol"]
    # paginate_by = 100
    tf = None

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        symbol_type = self.kwargs.get("symbol_type") or "stock"
        timeframes = StocksLoop.scan_timeframes if symbol_type == "stock" else CryptoLoop.scan_timeframes
        request = self.request.GET or {}

        search = request.get("q", '')
        if search:
            search_query = Q(symbol_rec__symbol__icontains=search)
        else:
            search_query = Q()

        context['symbol_type'] = symbol_type
        context["timeframes"] = timeframes
        context["q"] = request
        context["selected_tf"] = request.get("tf", 'D')
        context["setup_filter_form"] = self.get_crispy_form()
        return context

    def get_crispy_form(self):
        form: SetupFilterForm
        is_stock = True if 'stock' in self.request.path else False
        if self.request.GET:
            form = SetupFilterForm(request=self.request, data=self.request.GET, is_stock=is_stock)
        else:
            form = SetupFilterForm(request=self.request, is_stock=is_stock)
        return form

    def get_queryset(self):
        symbol_type = self.kwargs.get("symbol_type") or "stock"

        tf = self.request.GET.get("tf")
        match tf:
            case 'all': tf_q = Q()
            case None: tf_q = Q(tf='D')
            case _: tf_q = Q(tf=tf)

        market_cap_q = Q()
        if symbol_type == "stock":
            if self.request.GET.get('market_cap') is None:
                market_cap_filter = Q(name='yfinance', meta__marketCap__gte=50_000_000_000) | Q(name='yfinance', meta__marketCap__isnull=True)
                providermeta_ids = ProviderMeta.objects.filter(market_cap_filter).values('id')
                market_cap_q = Q(symbol_rec__provider_metas__id__in=Subquery(providermeta_ids))

        if negated := self.request.GET.get("negated"):
            negated_q = Q(negated=negated)
        else:
            negated_q = Q(negated=False)

        queryset = (
            super()
            .get_queryset()
            .filter(expires__gt=timezone.now())
            .filter(tf_q, market_cap_q, negated_q)
            .select_related("symbol_rec")
            .prefetch_related("negated_reasons")
            .filter(symbol_rec__symbol_type=symbol_type)
            .order_by('direction', 'pattern__1', 'symbol_rec__symbol')
            # .annotate(tf_sorted=Timeframe.get_case_when_with_durations('tf'))
            # .order_by('tf_sorted', 'direction', 'pattern__1', 'symbol_rec__symbol')
        )

        # https://django-filter.readthedocs.io/en/stable/guide/usage.html#the-view
        # filter_data = self.request.GET.copy()
        # if filter_data.get("potential_outside") == "on":
        #     filter_data["potential_outside"] = "true"
        # if filter_data.get("potential_outside") == "off":
        #     filter_data["potential_outside"] = "false"
        # f = SetupFilter(filter_data, queryset=queryset)

        f = SetupFilter(self.request.GET, symbol_type=symbol_type, queryset=queryset)
        return f.qs


def filter_setups(request, symbol_type: str = 'stock'):
    if request.htmx:
        redirect("/")

    search = request.GET.get("q", '')
    if search:
        search_query = Q(symbol_rec__symbol__icontains=search)
    else:
        search_query = Q()

    tf = request.GET.get("tf", 'D')
    match tf:
        case 'all': tf_q = Q()
        case None: tf_q = Q(tf='D')
        case _: tf_q = Q(tf=tf)

    rr = request.GET.get("rr", 1)
    rr_q = Q(rr__gt=rr)

    timeframes: list[Timeframe]
    if symbol_type == "stock":
        timeframes = [t for t in StocksLoop.scan_timeframes]
    else:
        timeframes = [t for t in CryptoLoop.scan_timeframes]

    form: SetupFilterForm
    is_stock = True if 'stock' in request.path else False
    if request.GET:
        form = SetupFilterForm(data=request.GET, is_stock=is_stock)
    else:
        form = SetupFilterForm(is_stock=is_stock)

    setups = (
        Setup.objects
        .select_related("symbol_rec")
        .filter(expires__gt=timezone.now())
        .filter(symbol_rec__symbol_type=symbol_type)
        .filter(tf_q, rr_q, search_query)
        .annotate(tf_sorted=Timeframe.get_case_when_with_durations('tf'))
        .order_by('tf_sorted', 'direction', 'pattern__1', 'symbol_rec__symbol')
    )
    # paginator = Paginator(setups, 100)

    # for setup in setups:
    #     setup.is_positive = (setup.symbol_rec.price > setup.trigger and setup.direction == 1) or (
    #             setup.symbol_rec.price < setup.trigger and setup.direction == -1)

    context = {
        "timeframes": timeframes,
        "setup_filter_form": form,
        "setups": setups,
        'q': search,
        "selected_tf": request.GET.get("tf", 'D'),
        'tf': tf,
        'form': form,
        # 'page_obj': paginator.get_page(request.GET.get('page')),
    }
    return TemplateResponse(request, "flowbite/partials/setups.html", context)


def search_setups(request):
    query = request.GET.get("q")
    if query:
        setups = (
            Setup.objects
            .filter(expires__gt=timezone.now())
            .filter(symbol_rec__symbol__icontains=query)
            .select_related("symbol_rec")
            .order_by('symbol_rec__symbol', 'expires')
        )
    else:
        setups = Setup.objects.none()
    return TemplateResponse(request, "flowbite/partials/setups.html", {"setups": setups, 'q': query})


def setups_view(request):
    symbol_type = request.GET.get("symbol_type") or "stock"
    tf = request.GET.get("tf", 'D')
    rr = request.GET.get("rr", 1)

    qs = (
        Setup.objects
        .filter(expires__gt=timezone.now(), symbol_rec__symbol_type=symbol_type, negated=False)
        .filter(tf=tf, rr__gte=rr)
        .select_related("symbol_rec")
        .prefetch_related("negated_reasons")
        .annotate(tf_sorted=Timeframe.get_case_when_with_durations('tf'))
        .order_by('tf_sorted', 'direction', 'pattern__1', 'symbol_rec__symbol')
    )
    setup_filter = SetupFilter(request.GET, queryset=qs)

    timeframes: list[Timeframe]
    if symbol_type == "stock":
        timeframes = StocksLoop.scan_timeframes
    else:
        timeframes = CryptoLoop.scan_timeframes

    form: SetupFilterForm
    is_stock = True if 'stock' in request.path else False
    if request.GET:
        form = SetupFilterForm(data=request.GET, is_stock=is_stock)
    else:
        form = SetupFilterForm(is_stock=is_stock)

    context = {
        'symbol_type': symbol_type,
        'timeframes': timeframes,
        'form': setup_filter.form,
        # 'form': form,
        'setup_qs': setup_filter.qs,
        'selected_tf': request.GET.get("tf", 'D'),
    }
    # context["setup_filter_form"] = self.get_crispy_form()

    return TemplateResponse(request, "flowbite/scanner_v2.html", context)


def grid(request):
    return TemplateResponse(request, "flowbite/grid.html")


class SymbolRecViewSet(viewsets.ModelViewSet):
    queryset = SymbolRec.objects.filter(symbol_type='crypto')
    serializer_class = SymbolRecSerializer


def advancing_declining(request):
    cache = caches['markets']
    r = cache.client.get_client(write=True)

    advancing_q = Query("@D:[0, 1]").no_content()
    a_results = r.ft('tfcStockIndex').search(advancing_q)

    declining_q = Query("@D:[-1 0]").no_content()
    d_results = r.ft('tfcStockIndex').search(declining_q)

    metrics = f"advancing_stocks {a_results.total}\n"
    metrics += f"declining_stocks {d_results.total}"

    return HttpResponse(metrics, content_type='text/plain')


def sector_metrics(request):
    cache = caches['markets']
    r = cache.client.get_client(write=True)

    symbols = [
        'XLC',
        # 'XLY',
        'XLP',
        'XLE',
        'XLF',
        'XLV',
        'XLI',
        'XLB',
        'XLRE',
        'XLK',
        'XLU',
    ]

    with r.pipeline() as pipe:
        for symbol in symbols:
            pipe.json().get(f'barHistory:stock:{symbol}')
        responses = pipe.execute()
    metrics = ''

    for symbol, data in zip(symbols, responses):
        yesterday_close = data['D'][-2]['c']
        today_close = data['D'][-1]['c']
        percentage_change = ((today_close - yesterday_close) / yesterday_close) * 100
        metrics += f'stock_sector_change{{sector="{symbol}"}} {percentage_change}\n'

    return HttpResponse(metrics, content_type='text/plain')


def candle_metrics(request):
    cache = caches['markets']
    r = cache.client.get_client(write=True)

    symbols = SymbolRec.objects.filter(symbol_type='stock').values_list('symbol', flat=True)

    with r.pipeline() as pipe:
        for symbol in symbols:
            pipe.json().get(f'barHistory:stock:{symbol}')
        responses = pipe.execute()

    candle_sids = {
        '1': 0,
        '2U': 0,
        '2D': 0,
        '3': 0,
    }
    for symbol, data in zip(symbols, responses):
        sid = data['D'][-1]['sid']
        candle_sids[sid] += 1

    metrics = ''
    for sid, count in candle_sids.items():
        metrics += f'stock_sid{{sid="{sid}"}} {count}\n'

    return HttpResponse(metrics, content_type='text/plain')


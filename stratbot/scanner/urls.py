from __future__ import annotations

from django.urls import path
from django.shortcuts import redirect
from django_filters.views import FilterView
from django.views.generic import TemplateView

from stratbot.scanner import views
from stratbot.scanner.filters import SetupFilter


app_name = "scanner"


urlpatterns = [
    # path("", SetupListView.as_view(template_name="dashboard/scanner.html"), name="scanner"),
    path("", lambda request: redirect('/scanner/stock'), name="default-redirect"),
    path("liveprice/", views.liveprice, name="liveprice"),
    path("flowbite/", views.flowbite, name="flowbite"),
    path("setups/", views.setups_view, name="setups-view"),
    path("grid/", views.grid, name="grid"),

    path('metrics/', views.advancing_declining, name='metrics'),
    path('sector-metrics/', views.sector_metrics, name='sector-metrics'),
    path('candle-metrics/', views.candle_metrics, name='sector-metrics'),
    # path("search/", FilterView.as_view(filterset_class=UserFilter, template_name="pages/search.html"), name="search"),
    # path("realtime/", TemplateView.as_view(template_name="flowbite/realtime.html"), name="realtime"),
    path("detail/<pk>/", views.SymbolDetailView.as_view(template_name="dashboard/symbol_detail.html"), name="symbol-detail"),
    path("symbols/", views.SymbolListView.as_view(template_name="dashboard/symbols_list.html"), name="symbols"),
    path("<symbol_type>/", views.SetupListView.as_view(template_name="flowbite/scanner.html"), name="scanner"),
    # path("<symbol_type>/", views.setup_list, name="scanner"),
]

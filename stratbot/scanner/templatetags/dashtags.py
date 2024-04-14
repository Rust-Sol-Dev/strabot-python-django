from __future__ import annotations

import re

import redis
from django import template
from django.conf import settings
from django.core.cache import caches
from django.urls import NoReverseMatch, reverse
from django.utils.safestring import mark_safe
from stratbot.scanner.models.timeframes import Timeframe


cache = caches['markets']
r = cache.client.get_client(write=True)

register = template.Library()

@register.simple_tag(takes_context=True)
def active(context, pattern_or_urlname):
    try:
        pattern = "^" + reverse(pattern_or_urlname)
    except NoReverseMatch:
        pattern = pattern_or_urlname
    path = context["request"].path
    if re.search(pattern, path):
        return "active"
    return ""


@register.simple_tag
def bg_color_based_on_direction(setup):
    return 'bg-success-lighten' if setup.direction == 1 else 'bg-danger-lighten'


@register.simple_tag
def tfc_to_html(last_price, tfc_state):
    # TODO: I feel like this should be done somewhere else. SymbolRec.tfc_state() should be sorted, but
    #  it's not. Maybe needs db flush? Does django return ordered JSON objects?
    tfc_state_as_cls = {Timeframe(key): value for key, value in tfc_state.items()}
    tfc_html = '<table><tr>'
    for tf, data in sorted(tfc_state_as_cls.items()):
        if float(last_price) > data['open']:
            color_class = 'text-green-500 dark:text-green-400'
        elif float(last_price) < data['open']:
            color_class = 'text-red-500 dark:text-red-400'
        else:
            color_class = 'text-gray-200'
        tfc_html += f'<td class="px-1 {color_class}">{tf}</td>'
    tfc_html += '</tr></table>'
    return mark_safe(tfc_html)


@register.simple_tag
def rvol(setup):
    return setup.symbol_rec.rvol(setup.tf)


@register.filter
def divide(value, arg):
    if value is None:
        return 0
    try:
        return int(value) / int(arg)
    except (ValueError, ZeroDivisionError):
        return None


@register.simple_tag
def current_bar(symbol, symbol_type, tf):
    try:
        key = f'barHistory:{symbol_type}:{symbol}'
        ohlc = r.json().get(key).get(tf)[-1]
        sid = ohlc.get('sid', '-')
        o = ohlc.get('o')
        c = ohlc.get('c')
    except (AttributeError, TypeError, IndexError):
        return '', '-'

    if c > o:
        color = 'text-green-500 dark:text-green-400'
    elif c < o:
        color = 'text-red-500 dark:text-red-400'
    else:
        color = ''

    return color, sid


def human_readable_number(value):
    for unit in ['', 'K', 'M', 'B', 'T']:
        if abs(value) < 1000:
            return f"{value:.1f}{unit}"
        value /= 1000
    return f"{value:.1f}P"  # Peta is the last unit


@register.filter(name='humanize')
def humanize_number(value):
    try:
        value = float(value)
        return human_readable_number(value)
    except (TypeError, ValueError):
        return value


COLOR_MAP = {
    'hammer': 'bg-green-500',
    'shooter': 'bg-red-500',
    'doji': 'bg-orange-500',
}


@register.filter
def get_color(candle_tag):
    return COLOR_MAP.get(candle_tag, '')


@register.filter
def percentage_diff(price, trigger):
    return (abs(price - trigger) / trigger) * 100


@register.filter
def replace_interval(value):
    return value.replace('Q', '3M').replace('Y', '12M')

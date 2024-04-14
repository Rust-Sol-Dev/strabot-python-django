from __future__ import annotations

from django.utils import timezone
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes, OpenApiResponse, OpenApiExample
from rest_framework.exceptions import ValidationError, ParseError
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.mixins import ListModelMixin, RetrieveModelMixin
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet
from rest_framework.views import APIView
import pandas_market_calendars as mcal

from stratbot.scanner.models.symbols import SymbolRec, Setup
from .serializers import SymbolRecSerializer, SetupSerializer, DateRangeSerializer


class SetupViewSet(RetrieveModelMixin, ListModelMixin, GenericViewSet):
    serializer_class = SetupSerializer
    queryset = Setup.objects.filter(expires__gt=timezone.now())

    @extend_schema(
        parameters=[
            OpenApiParameter(
                name='symbol',
                description='Symbol to filter setups, or "all" for all symbols',
                required=True,
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='tf',
                description='Timeframe to filter setups: 15, 30, 60, 4H, 6H, 12H, D, W, M, Q',
                required=False,
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY
            )
        ]
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def get_queryset(self, *args, **kwargs):
        symbol = self.request.query_params.get('symbol')
        timeframe = self.request.query_params.get('tf')
        if not symbol:
            raise ValidationError({"error": "Invalid symbol parameter"})
        if symbol == 'all':
            return self.queryset if not timeframe else self.queryset.filter(tf=timeframe)
        filters = {'symbol_rec__symbol': symbol}
        if timeframe:
            filters['tf'] = timeframe
        return self.queryset.filter(**filters)


class SymbolRecViewSet(RetrieveModelMixin, ListModelMixin, GenericViewSet):
    serializer_class = SymbolRecSerializer
    queryset = SymbolRec.objects.all()

    @extend_schema(
        parameters=[
            OpenApiParameter(
                name='symbol',
                description='Stock or crypto symbol',
                required=False,
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='symbol_type',
                description='Symbol type "stock", "crypto", or "all"',
                required=False,
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY
            ),
        ]
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def get_queryset(self, *args, **kwargs):
        # symbol_type = self.request.query_params.get('symbol_type', None)
        symbol_type = 'crypto'
        if symbol_type is not None:
            return self.queryset.filter(symbol_type=symbol_type, price__gt=0)
        else:
            return self.queryset


class MarketCalendarView(APIView):
    serializer_class = DateRangeSerializer

    @extend_schema(
        parameters=[
            OpenApiParameter(
                name='start_date',
                description='Start date in YYYY-MM-DD format',
                required=True,
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name='end_date',
                description='Start date in YYYY-MM-DD format',
                required=True,
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY
            ),
        ],
        responses={
            200: OpenApiResponse(
                description="Returns a list of market hours between start_date and end_date",
                response=OpenApiTypes.OBJECT,
                examples=[
                    OpenApiExample(
                        name="Market Calendar",
                        value=[
                            {
                                "index": "2023-06-01T00:00:00",
                                "market_open": "2023-06-01T13:30:00Z",
                                "market_close": "2023-06-01T20:00:00Z",
                            },
                            {
                                "index": "2023-06-02T00:00:00",
                                "market_open": "2023-06-02T13:30:00Z",
                                "market_close": "2023-06-02T20:00:00Z"}
                        ],
                        summary="Market Calendar Example",
                        description="Example market calendar with open and close times during regular trading hours."
                    ),
                ]
            )
        },
    )
    def get(self, request):
        serializer = self.serializer_class(data=request.query_params)
        serializer.is_valid(raise_exception=True)

        start_date = serializer.validated_data['start_date']
        end_date = serializer.validated_data['end_date']

        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=start_date, end_date=end_date)

        schedule_json = schedule.reset_index().to_dict(orient='records')
        return Response(schedule_json)

from __future__ import annotations

from rest_framework import serializers

from stratbot.scanner.models.symbols import SymbolRec, Setup


class SetupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Setup
        fields = '__all__'

        # extra_kwargs = {"url": {"view_name": "api:user-detail", "lookup_field": "pk"}}


class SymbolRecSerializer(serializers.ModelSerializer):
    class Meta:
        model = SymbolRec
        fields = '__all__'


class DateRangeSerializer(serializers.Serializer):
    start_date = serializers.DateField()
    end_date = serializers.DateField()

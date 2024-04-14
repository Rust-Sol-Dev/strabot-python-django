from django.http import JsonResponse
from rest_framework import routers, serializers, viewsets
from rest_framework import serializers

from .models.symbols import SymbolRec


class SymbolRecSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SymbolRec
        # fields = ['symbol', 'symbol_type', 'price']
        fields = '__all__'

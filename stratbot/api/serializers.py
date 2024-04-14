from rest_framework import routers, serializers, viewsets
from ..scanner.models.symbols import SymbolRec, Setup


class SymbolRecSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SymbolRec
        fields = ['symbol', 'symbol_type', 'price']


class SetupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Setup
        fields = '__all__'

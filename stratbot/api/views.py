from django.shortcuts import render
from django.utils import timezone

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework import permissions
from ..scanner.models import SymbolRec, Setup
from .serializers import SetupSerializer


class SetupListApiView(APIView):
    # add permission to check if user is authenticated
    # permission_classes = [permissions.IsAuthenticated]

    # 1. List all
    def get(self, request, *args, **kwargs):
        """
        List all setups for a given symbol
        """
        symbol = kwargs.get('symbol', None)
        if symbol is not None:
            if symbol == 'all':
                setups = Setup.objects.filter(expires__gt=timezone.now())
            else:
                setups = Setup.objects.filter(symbol_rec__symbol=symbol, expires__gt=timezone.now())
            serializer = SetupSerializer(setups, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        else:
            return Response({"error": "No symbol provided"}, status=status.HTTP_400_BAD_REQUEST)

from datetime import datetime

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, TakeProfitRequest, StopLossRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
from alpaca.common import exceptions as alpaca_exceptions


trading_client = TradingClient('PKZ52UFQJJ5UCKU6B2ZC', '6Qpd4h2PGxacRMM2BLlB06tqQ0zsm9kiPQe5jsPb')


def enter_trade(symbol: str, take_profit, stop_loss, direction):
    if direction == 1:
        side = OrderSide.BUY
    else:
        side = OrderSide.SELL

    try:
        market_order_data = MarketOrderRequest(
            symbol=symbol,
            qty=1,
            side=side,
            order_class=OrderClass.BRACKET,
            time_in_force=TimeInForce.GTC,
            take_profit=TakeProfitRequest(limit_price=round(float(take_profit), 2)),
            stop_loss=StopLossRequest(stop_price=round(float(stop_loss), 2))
        )
        market_bracket_order = trading_client.submit_order(
            order_data=market_order_data
        )
        print(f'{datetime.now()}: ENTERED TRADE')
        return market_bracket_order
    except alpaca_exceptions.APIError as e:
        print(f'ERROR: {e}')


def exit_trade(symbol: str):
    market_order_data = MarketOrderRequest(
        symbol=symbol,
        qty=1,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC
    )
    market_order = trading_client.submit_order(
        order_data=market_order_data
    )


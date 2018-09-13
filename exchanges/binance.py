"""Module for interaction with the binance API."""

# Base classes
from exchanges.base import ExchangeData, ExchangeOrders

# Binance API
from binance.client import Client
from binance.enums import *

from time import time, sleep
import pandas as pd

# Custom
from config import config
import utils.toolbox as tb


class BinanceData(ExchangeData):
    '''Handle data calls to binance.'''

    def __init__(self, config = config.binance):
        self.client = Client(config['api_key'], config['api_secret'])

    def ticker(self, symbol):
        return self.client.get_symbol_ticker(symbol = symbol)

    def all_tickers(self):
        cols = ['symbol','price']
        return pd.DataFrame.from_dict(self.client.get_all_tickers())[cols]

    def candle(self, symbol, limit = None, startTime = None, endTime = None):

        if startTime:
            startTime = tb.DateConvert(startTime).timestamp*1000
        elif not limit:
            limit = 1

        if endTime:
            endTime = tb.DateConvert(endTime).timestamp*1000

        candle = self.client.get_klines(
                    symbol = symbol,
                    interval = self.client.KLINE_INTERVAL_1HOUR,
                    limit = limit,
                    startTime = startTime,
                    endTime = endTime
                    )

        cols = [
            'open_date', 'open', 'high', 'low', 'close', 'volume',
            'close_date', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
            'ignore'
        ]

        df = pd.DataFrame(candle, columns = cols)
        df = df.drop('ignore', axis=1)
        df['symbol'] = symbol

        reorder = [
            'symbol', 'open_date', 'open', 'high', 'low', 'close', 'volume',
            'close_date', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
        ]

        # Convert timestamp to datetime
        f = lambda x: tb.DateConvert(x).date
        df.open_date = df.open_date.map(f)
        df.close_date = df.close_date.map(f)

        return df[reorder]

    def symbols(self):
        info = self.client.get_exchange_info()['symbols']

        symbols = []
        for symbol in info:
            symbols.append({
                'symbol':symbol['symbol'],
                'from_symbol':symbol['baseAsset'],
                'to_symbol':symbol['quoteAsset']
                })

        cols = ['symbol','from_symbol','to_symbol']
        s =  pd.DataFrame.from_dict(symbols)[cols]
        s.index = s.symbol
        return s



class BinanceOrders(ExchangeOrders):
    '''Handle order calls to binance.'''
    def __init__(self, config = config.binance):
        self.client = Client(config['api_key'], config['api_secret'])

    def buy_order(self, symbol, quantity, test = True):

        if test:
            order = self.client.create_test_order(symbol=symbol,
                                                  side=SIDE_BUY,
                                                  type=ORDER_TYPE_MARKET,
                                                  quantity=quantity)
        else:
            order = self.client.create_order(symbol=symbol,
                                             side=SIDE_BUY,
                                             type=ORDER_TYPE_MARKET,
                                             quantity=quantity)
        return True if order else False


    def sell_order(self, symbol, quantity, test = True):

        if test:
            order = self.client.create_test_order(symbol=symbol,
                                                  side=SIDE_SELL,
                                                  type=ORDER_TYPE_MARKET,
                                                  quantity=quantity)
        else:
            order = self.client.create_order(symbol=symbol,
                                             side=SIDE_sell,
                                             type=ORDER_TYPE_MARKET,
                                             quantity=quantity)
        return True if order else False


    def account_balance(self):
        return self.client.get_account()['balances']


    def coin_balance(self, symbol):
        return self.client.get_asset_balance(symbol)


    def account_status(self):
        status = self.client.get_account_status()
        if status['msg'] == 'Normal' and status['success']:
            return True
        else:
            return False

    def all_orders(self, symbol):
        return self.client.get_all_orders(symbol=symbol)


    def trades(self, symbol):
        return self.client.get_my_trades(symbol = symbol)



def check_binance_server_time_diff(verbose=True, ret=False):
    '''
    Check the difference between the binance server time and local machine
    time.
    '''

    client = Client(config.binance['api_key'], config.binance['api_secret'])

    diffs = []
    for i in range(1, 3):
        local_time1 = int(time() * 1000)
        server_time = client.get_server_time()
        diff1 = server_time['serverTime'] - local_time1
        local_time2 = int(time() * 1000)
        diff2 = local_time2 - server_time['serverTime']
        diffs.append(diff2)
        print("local1: {} server: {} local2: {} diff1: {} diff2: {}" \
              .format(local_time1, server_time['serverTime'],
                      local_time2, diff1, diff2))
        sleep(2)

    if ret:
        return diffs
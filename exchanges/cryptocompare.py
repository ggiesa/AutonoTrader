import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from exchanges.base import ExchangeData
from utils.toolbox import decode_api_response, DateConvert


class CryptocompareData(ExchangeData):
    def __init__(self, exchange):
        """
        Get data from Cryptocompare for a given exchange.
        """
        self.exchange = exchange
        self._symbols = self._get_symbols()


    def _get_symbols(self):
        """
        To maintain a consistent API between exchanges, gather symbols on
        intilization so <symbol> can be used rather than <from_symbol> and
        <to_symbol> in calls to candle and ticker.
        """
        url = 'https://min-api.cryptocompare.com/data/all/exchanges'
        exchange_info = decode_api_response(url)[1][self.exchange]

        symbols = []
        for symbol in exchange_info:
            for to_coin in exchange_info[symbol]:
                symbols.append({
                    'symbol':symbol+to_coin,
                    'from_symbol':symbol,
                    'to_symbol':to_coin
                })

        cols = ['symbol','from_symbol','to_symbol']
        symbols = pd.DataFrame.from_dict(symbols)[cols]
        symbols.index = symbols.symbol
        return symbols


    def ticker(self):
        pass


    def candle(self, symbol, limit = 1, startTime = None, endTime = None):
        """
        Get hourly candles for a single symbol from an exchange.

        Parameters:
        -----------
        symbol: string
            A valid cryptocurreny symbol for the given exchange.

        limit: int; min 1, max 500
            The max amount of candles to return.

        startTime: datetime object
            The start of candle interval

        endTime: datetime object
            The end of candle interval


        Returns:
        -----------
        data: pandas.DataFrame

            Columns:

            'symbol', 'open_date', 'open', 'high', 'low', 'close', 'volume',
            'close_date', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',

        """

        # Parse symbol into from_symbol and to_symbol for API call
        try:
            from_symbol = self._symbols.loc[symbol].from_symbol
            to_symbol = self._symbols.loc[symbol].to_symbol
        except KeyError as e:
            print(f'''
            {symbol} is not a valid symbol for {self.exchange} within the
            Cryptocompare system.
            Options:
                {sorted(list(self._symbols.symbol))}

            ''')
            raise e

        # Parse startTime and endTime
        if startTime:

            if endTime:
                endTime = DateConvert(endTime).datetime
            else:
                endTime = datetime.utcnow()

            if endTime.minute:
                endTime -= timedelta(hours=1)
                endTime = endTime.replace(minute=0, second=0, microsecond=0)

            startTime = DateConvert(startTime).datetime
            date_range = pd.date_range(startTime, endTime, freq='1H')
            limit = len(date_range)

        elif not endTime:
            endTime = datetime.utcnow()

        endTime = DateConvert(endTime).timestamp

        # URL parameters
        params = {'fsym':from_symbol, 'tsym':to_symbol, 'e':self.exchange,
                  'toTs':endTime,     'limit':limit}

        # Build URL
        url = 'https://min-api.cryptocompare.com/data/histohour?'
        count = 0
        for param, value in params.items():
            if value:
                sym = '&' if count > 0 else ''
                url += f'{sym}{param}={value}'
                count+=1

        # Get Response from GET call
        data = decode_api_response(url)[1]
        data = pd.DataFrame(data['Data'])

        # Convert from timestamp to datetime, set as index
        data['open_date'] = data.time.map(lambda x: DateConvert(x).date)
        data['symbol'] = symbol
        data = data.drop('time', axis=1)

        # TODO Figure out volume config
        reorder = [
            'symbol', 'open_date', 'open', 'high', 'low', 'close', 'volume',
            'close_date', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
        ]
        print(limit)
        if limit == 1:
            return data.iloc[-1].T
        else:
            return data


    def symbols(self):
        """
        Return a pandas.DataFrame with all symbols for an exchange.

        Returns:
        ---------
        symbols: pd.DataFrame
            Columns:
            | 'symbol' | 'from_symbol' | 'to_symbol' |
        """
        return self._symbols

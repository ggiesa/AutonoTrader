import numpy as np
import pandas as pd
from exchanges.base import ExchangeData, ExchangeOrders
from exchanges.cryptocompare import CryptocompareData



class TestData:

    # def test_coinlist(self):
    #     symbols = CryptocompareData('Binance').symbols()
    #     assert not symbols.empty, 'Nothing returned from Binance.symbols call'
    #     assert np.isin(['symbol','from_symbol', 'to_symbol'], symbols.columns).all()
    #
    # def test_ticker(self):
    #     symbol = 'BTCUSDT'
    #     assert 'price' in CryptocompareData().ticker(symbol).keys()

    # def test_candle_format(self):
    #     symbol = 'BTCUSDT'
    #     assert not CryptocompareData('Binance').candle(symbol).empty

    def test_candle_date_range(self):
        symbol = 'BTCUSDT'
        start = '2018-09-10 12:00:00'
        end = '2018-09-11 12:00:00'

        check_dates = CryptocompareData('Binance').candle(
            symbol, startTime=start, endTime=end
            )
        assert check_dates.open_date.iloc[0] == start
        assert check_dates.open_date.iloc[-1] == end

    # def test_candle_date_range_w_limit(self):
    #     symbol = 'BTCUSDT'
    #     start = '2018-09-10 12:00:00'
    #     end = '2018-09-11 12:00:00'
    #     limit = 5
    #
    #     check_dates = CryptocompareData('Binance').candle(
    #         symbol, startTime=start, endTime=end, limit=5
    #         )
    #     assert len(check_dates) == limit
    #
    #     limit=1
    #     check_dates = CryptocompareData('Binance').candle(
    #         symbol, startTime=start, endTime=end, limit=5
    #         )
    #     assert len(check_dates) == limit

    # def test_all_tickers(self):
    #     all_tickers = CryptocompareData().all_tickers()
    #     assert not all_tickers.empty

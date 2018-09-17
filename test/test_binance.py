import numpy as np
import pandas as pd
from exchanges.base import ExchangeData, ExchangeOrders
from exchanges.binance import (
    BinanceData, BinanceOrders, check_binance_server_time_diff
)



class TestData:

    def test_ticker(self):
        symbol = 'BTCUSDT'
        assert 'price' in BinanceData().ticker(symbol).keys()

    def test_coinlist(self):
        symbols = BinanceData().symbols()
        assert not symbols.empty, 'Nothing returned from Binance.symbols call'

    def test_candle_format(self):
        symbol = 'BTCUSDT'
        assert not BinanceData().candle(symbol).empty

    def test_historical_candles(self):
        symbol = 'BTCUSDT'
        start = '2018-09-10 12:00:00'
        end = '2018-09-11 12:00:00'

        check_dates = BinanceData().candle(
            symbol, startTime=start, endTime=end
            )
        assert check_dates.open_date.iloc[0] == start
        assert check_dates.open_date.iloc[-1] == end

        limit = 5
        check_dates = BinanceData().candle(
            symbol, startTime=start, endTime=end, limit=5
            )
        assert len(check_dates) == limit

    def test_all_tickers(self):
        all_tickers = BinanceData().all_tickers()
        assert not all_tickers.empty



class TestOrders:
    pass


class TestBaseClass:
    def test_ExchangeData_inheritance(self):
        class ImproperDataImplementation(ExchangeData):
            pass

        try:
            should_fail = ImproperDataImplementation()
        except NotImplementedError:
            return True

    def test_ExchangeOrder_inheritance(self):
        class ImproperOrderImplementation(ExchangeOrders):
            pass

        try:
            should_fail = ImproperOrderImplementation()
        except NotImplementedError:
            return True

# Check server time sync -- takes too long
# def test_server_time():
#     diff = check_binance_server_time_diff(verbose=False, ret=True)
#     assert np.abs(sum(diff)) < 1000, 'Binance server time dissagreement'

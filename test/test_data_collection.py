from ingestion import data_collection as dc
import numpy as np
import pandas as pd

from exchanges.binance import BinanceData
from utils.toolbox import format_records, DateConvert, chunker
from utils.database import Database, get_symbols
from datetime import datetime, timedelta


class TestCandles:

    def test_startTime_with_no_endTime(self):
        # A range of candles from startTime to most recent
        startTime = datetime.utcnow()-timedelta(hours=5)
        symbol = 'BTCUSDT'

        candles = dc.insert_hourly_candles(
            symbol, startTime=startTime, debug=True
        )
        assert len(candles) == 5

        symbol = ['BTCUSDT','ETHUSDT']

        candles = dc.insert_hourly_candles(
            symbol, startTime=startTime, debug=True
        )
        assert len(candles) == 10


    def test_startTime_with_endTime(self):
        # Range of candles within interval StartTime-endTime

        startTime = '2018-09-09 22:05:13'
        endTime = '2018-09-10 22:05:13'

        symbol = 'BTCUSDT'

        candles = dc.insert_hourly_candles(
            symbol, startTime=startTime, endTime=endTime, debug=True
        )
        assert len(candles) == 24

        symbol = ['BTCUSDT', 'ETHUSDT']

        candles = dc.insert_hourly_candles(
            symbol, startTime=startTime, endTime=endTime, debug=True
        )
        assert len(candles) == 48


    def test_endTime_with_no_startTime(self):

        endTime = '2018-09-11 22:05:13'
        symbol = 'BTCUSDT'

        candles = dc.insert_hourly_candles(
            symbol, endTime=endTime, debug=True
        )
        assert len(candles) == 1

        symbol = ['BTCUSDT','ETHUSDT']

        candles = dc.insert_hourly_candles(
            symbol, endTime=endTime, debug=True
        )
        assert len(candles) == 2


    def test_expected_date(self):
        """Function should return the last complete candle."""

        endTime = '2018-09-11 22:05:13'
        symbol = 'BTCUSDT'

        candles = dc.insert_hourly_candles(
            symbol, endTime=endTime, debug=True
        )
        assert len(candles) == 1

        symbol = ['BTCUSDT','ETHUSDT']

        candles = dc.insert_hourly_candles(
            symbol, endTime=endTime, debug=True
        )
        assert candles.open_date.iloc[0] == '2018-09-11 22:00:00'

        #----------------------------------------------------------------

        endTime = '2018-09-11 22:00:00'
        symbol = 'BTCUSDT'

        candles = dc.insert_hourly_candles(
            symbol, endTime=endTime, debug=True
        )
        assert len(candles) == 1

        symbol = ['BTCUSDT','ETHUSDT']

        candles = dc.insert_hourly_candles(
            symbol, endTime=endTime, debug=True
        )
        assert candles.open_date.iloc[0] == '2018-09-11 22:00:00'

        #----------------------------------------------------------------

        endTime = '2018-09-11 21:59:00'
        symbol = 'BTCUSDT'

        candles = dc.insert_hourly_candles(
            symbol, endTime=endTime, debug=True
        )
        assert len(candles) == 1

        symbol = ['BTCUSDT','ETHUSDT']

        candles = dc.insert_hourly_candles(
            symbol, endTime=endTime, debug=True
        )
        assert candles.open_date.iloc[0] == '2018-09-11 21:00:00'


    def test_no_endTime_and_no_startTime(self):
        symbol = 'BTCUSDT'

        candles = dc.insert_hourly_candles(
            symbol, debug=True
        )

        now = datetime.utcnow()
        nearest_complete = now.replace(minute=0,second=0,microsecond=0)
        hour_ago = now - timedelta(hours=1)
        now, hour_ago = DateConvert(now).date, DateConvert(hour_ago).date
        assert len(candles) == 1
        assert hour_ago < candles.open_date.iloc[0] < now
        assert candles.open_date.iloc[0] == DateConvert(nearest_complete).date

        symbol = ['BTCUSDT','ETHUSDT']
        candles = dc.insert_hourly_candles(
            symbol, debug=True
        )
        assert len(candles) == 2


    def test_multi_chunk(self):
        startTime = datetime.utcnow()-timedelta(days=26)
        endTime = datetime.utcnow()-timedelta(days=1)
        symbol = ['BTCUSDT']

        candles = dc.insert_hourly_candles(
            symbol, startTime=startTime, endTime=endTime, debug=True
        )

        hour_before = startTime - timedelta(hours=1)
        hour_after = startTime + timedelta(hours=1)
        assert hour_before < DateConvert(candles.open_date.min()).datetime < hour_after

        hour_before = endTime - timedelta(hours=1)
        hour_after = endTime + timedelta(hours=1)
        assert hour_before < DateConvert(candles.open_date.max()).datetime < hour_after

        dates = list(candles.open_date)
        min_date = min(dates)
        max_date = max(dates)
        complete_range = pd.date_range(min_date,max_date,freq='1H')
        complete_range = list(map(lambda x: DateConvert(x).date, complete_range))
        assert np.isin(complete_range, dates).all()

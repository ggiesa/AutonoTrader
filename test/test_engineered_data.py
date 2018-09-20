
from datetime import datetime, timedelta

from ingestion import core
from ingestion import live
from utils.database import get_max_open_date, get_symbols

from importlib import reload
core = reload(core)
live = reload(live)



class TestEngineeredData:
    def test_num_candles(self):
        # Number of candles should be proportional to number of hours from from_date
        num_symbols = len(get_symbols())
        num_candles = 11*num_symbols
        max_date = get_max_open_date()
        from_date = max_date - timedelta(hours=10)

        engineered_data = core.engineer_data(from_date=from_date)
        assert len(engineered_data) == num_candles

    def test_earliest_date(self):
        # Earliest engineered date should be from_date
        num_symbols = len(get_symbols())
        num_candles = 10*num_symbols
        max_date = get_max_open_date()
        from_date = max_date - timedelta(hours=10)

        engineered_data = core.engineer_data(from_date=from_date)
        assert engineered_data.open_date.min() == from_date

    def test_most_recent_date(self):
        # Most recent date should be the most recent candle in the db
        num_symbols = len(get_symbols())
        num_candles = 10*num_symbols
        max_date = get_max_open_date()
        from_date = max_date - timedelta(hours=10)

        engineered_data = core.engineer_data(from_date=from_date)
        assert engineered_data.open_date.max() == max_date

    def test_most_recent_date(self):
        # Most recent date should be the most recent candle in the db
        max_date = get_max_open_date()
        engineered_data = core.engineer_data()
        assert engineered_data.open_date.max() == max_date

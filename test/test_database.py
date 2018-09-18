from utils.database import (
    Database, get_symbols, get_pairs, get_symbols_and_pairs, Candles,
    get_most_recent_dates
    )
from utils.toolbox import DateConvert
from pymysql.err import OperationalError
from datetime import datetime, timedelta

class TestDatabase:

    def test_database_existence(self):
        db = Database(db = '')
        databases = list(db.execute('show databases;').Database)
        assert 'autonotrader' in databases, 'there is no autonotrader database'
        assert 'test' in databases, 'there is no test database'

    def test_wrong_database(self):
        db = Database(db='')
        try:
            db.connection.select_db('scoob')
        except:
            return True


def test_get_symbols():
    symbols = get_symbols()
    assert symbols
    assert isinstance(symbols, list)

def test_get_pairs():
    symbols = get_pairs()
    assert not symbols.empty
    assert 'from_symbol' in symbols.columns
    assert 'to_symbol' in symbols.columns

def test_get_symbols_and_pairs():
    symbols = get_symbols_and_pairs()
    assert not symbols.empty
    assert 'from_symbol' in symbols.columns
    assert 'to_symbol' in symbols.columns
    assert 'symbol' in symbols.columns


def test_get_most_recent_dates():
    pass


def test_get_oldest_dates():
    pass



class TestRawCandles:

    def test_from_date(self):

        symbol = get_symbols()[0]
        date = get_most_recent_dates(symbol)[symbol]
        from_date = date - timedelta(hours=10)

        candles = Candles().get_raw(
            symbol = symbol,
            from_date = from_date
            )

        date = DateConvert(date).datetime
        most_recent_date = DateConvert(candles.open_date.iloc[0]).datetime

        from_date = DateConvert(from_date).datetime
        oldest_date = DateConvert(candles.open_date.iloc[-1]).datetime

        assert date == most_recent_date
        assert from_date == oldest_date

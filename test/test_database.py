from utils.database import (
    Database, get_symbols, get_pairs, get_symbols_and_pairs
    )
from pymysql.err import OperationalError

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

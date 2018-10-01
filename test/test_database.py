from utils.database import (
    Database, get_symbols, get_pairs, get_symbols_and_pairs, Candles,
    get_most_recent_dates, CreateTable, check_table_existence
    )
from utils.toolbox import DateConvert
from pymysql.err import OperationalError, ProgrammingError
from datetime import datetime, timedelta
import pandas as pd

DB = 'test'

class TestDatabase:

    def test_database_existence(self):
        db = Database(db = '')
        databases = list(db.execute('show databases;').Database)
        assert 'autonotrader' in databases, 'there is no autonotrader database'
        assert DB in databases, 'there is no test database'

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
        date = get_most_recent_dates(symbols=symbol)[symbol]
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

class TestCreateTable:

    def test_discover_table_true(self):
        # Test handling of table that already exists
        data = Database(db=DB).execute('SELECT * FROM candles LIMIT 1;')
        table_name = 'candles'
        c = CreateTable('candles', data, db=DB)
        assert c.table_exists, 'discover table failed to find candles'

    def test_create_table(self):
        # Test handling of a non-existant table
        integer = 1000
        floatt = 99.9
        date = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        string = 'something'

        table_name = 'test_create_table'
        data = pd.DataFrame([[date, integer, floatt, string]],
                            columns = ['date','integer', 'floatt', 'string'])

        try:
            c = CreateTable(table_name, data, db=DB)
            sql = f'SHOW TABLES;'
            tables = Database(db=DB).execute(sql)
            assert table_name in set(tables.iloc[:,0]), 'Table not created'
        except Exception as e:
            raise e
        finally:
            sql = f'DROP TABLE {table_name};'
            Database(db=DB).execute(sql)


def test_check_table_existence():
    table='candles'
    assert check_table_existence(table)

import setup
from utils import setup_utilities as su
from utils.database import Database
from errors.exceptions import ImplementationError
from datetime import timedelta
from errors.exceptions import ImplementationError


def test_db_creation():
    pass

def test_all_symbols_population():
    pass

def test_custom_symbol_population():
    pass

class TestParseandValidateSymbols:

    def test_invalid_symbol(self):
        user_symbols = ['skag/leaf']
        try:
            su.parse_and_validate_symbols(user_symbols, 'binance')
        except ImplementationError:
            return True
        return False


class TestParseHistoricalCollectionPeriod():

    def test_year(self):
        collection_period = '1Y'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=365)

        collection_period = '2Y'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=2*365)

        collection_period = '23Y'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=23*365)


    def test_month(self):
        collection_period = '1M'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=30)

        collection_period = '2M'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=2*30)

        collection_period = '23M'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=23*30)


    def test_day(self):
        collection_period = '10D'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=10)

        collection_period = '1D'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=1)

        collection_period = '13D'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=13)


    def test_invalid_format(self):
        try:
            collection_period = '1'
            td = su.parse_datestring(collection_period)
        except ImplementationError:
            True

        try:
            collection_period = '#'
            td = su.parse_datestring(collection_period)
        except ImplementationError:
            True

        collection_period = '1d'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=1)


        collection_period = '1y'
        td = su.parse_datestring(collection_period)
        assert td == timedelta(days=365)

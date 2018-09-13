"""Setup initial MySQL schema, populate with historical data."""

from utils.database import Database, get_symbols
from utils import setup_utilities as su
from utils.toolbox import progress_bar
from exchanges.binance import BinanceData
from config.data_collection import historical_config, user_symbols
from ingestion.data_collection import insert_hourly_candles
from datetime import datetime


def setup_databases():
    """Setup initial database schema."""

    print('Building databases','\n','--------------------------')
    schema_path = 'data/MySQL_scripts/build_tables.sql'
    su.create_db('autonotrader', schema_path)
    su.create_db('test', schema_path)


def populate_all_db_symbols():
    """Get complete list of possible symbols from each exchange, add to db."""

    print('Acquiring symbols from exchanges','\n','--------------------------')

    # Get full list of Binance symbols
    binance_symbols = BinanceData().symbols()
    binance_symbols['exchange'] = 'binance'

    # Populate all_symbols tables
    Database().insert(
        'all_symbols',
        binance_symbols,
        auto_format=True
    )
    Database(db='test').insert(
        'all_symbols',
        binance_symbols,
        auto_format=True
    )


def populate_custom_db_symbols():
    """Populate user_symbols table with user symbols."""

    print('Adding user symbols to database','\n','--------------------------')

    for exchange, symbols in user_symbols.items():
        if symbols:
            ins = su.parse_and_validate_symbols(symbols, exchange)
            Database().insert('user_symbols', ins, auto_format=True)
            Database(db='test').insert('user_symbols', ins, auto_format=True)


def collect_historical_data():
    """Populate the candles table with historical data."""

    collection_period = historical_config['historical_collection_period']
    collection_period = su.parse_historical_collection_period(collection_period)

    user_symbols = get_symbols()
    startTime = datetime.utcnow() - collection_period

    print('Collecting historical data','\n','--------------------------')
    insert_hourly_candles(
        user_symbols, startTime=startTime, db='autonotrader', verbose=True
        )



if __name__ == '__main__':
    setup_databases()
    populate_all_db_symbols()
    populate_custom_db_symbols()
    collect_historical_data()
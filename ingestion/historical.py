"""Data ingestion tasks to populate database with historical data."""

from datetime import datetime, timedelta

from utils.toolbox import parse_datestring, DateConvert
from utils.database import get_oldest_dates
from ingestion.core import insert_hourly_candles


def insert_historical_candles(symbols, datestring, min_date=None, verbose=True):
    """
    Insert <datestring> historical candles for a symbol(s) beyond the oldest
    found candle in the database.

    Example:
        insert_historical_candles('BTCUSDT', '1M') will insert 1 month of
        historical data beyond what is found in the database.

    Parameters:
    ---------------
    symbols: string | list of strings
        Symbols of the cryptos to insert data for. Example: ['BTCUSDT', 'ETHBTC']

    datestring: string
        String designating how much data to collect. Format: integer followed by
        'D', 'M', or 'Y'.

        Examples:
        '10D' ---> 10 days
        '5M' ---> 5 months
        '1Y' ---> 1 year

    debug: boolean
        Setting to true will return the data that would have been inserted into
        the database.

    """

    if isinstance(symbols, str):
        symbols = [symbols]

    # Convert datestring to timedelta object
    dt = parse_datestring(datestring)

    # Oldest dates from the DB as a dict, with None values if nothing's there
    oldest_dates = get_oldest_dates(symbols=symbols)

    for i, symbol in enumerate(symbols):
        endTime = oldest_dates[symbol]

        if endTime:
            endTime = DateConvert(endTime).datetime
        else:
            endTime = datetime.utcnow()

        startTime = endTime-dt

        insert_hourly_candles(
            symbol, endTime=endTime, startTime=startTime, verbose=verbose
            )

        if i != len(symbols)-1:
            if verbose:
                print('Sleeping...')
            sleep(10)

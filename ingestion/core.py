"""Core data ingestion functionality."""

import pandas as pd
from datetime import datetime, timedelta
from errors.exceptions import DiscontinuousError

from utils import toolbox as tb
from exchanges.binance import BinanceData
from utils.database import Database, Candles, get_symbols, get_max_from_column
from ingestion.custom_indicators import CustomIndicator

# TODO break into live and historical components
# TODO Chunk number doesnt update in progress bar
def insert_hourly_candles(symbols, startTime=None,    endTime=None,
                                   db='autonotrader', debug=False,
                                   verbose=False,     datasource=None):
    """
    Get candles from the binance API, insert into the database.
        - If no startTime or endTime is provided, inserts the most recent
            candles.
        - If startTime but not endTime is provided, inserts startTime to
            most recent candles.
        - If endTime is provided but not startTime, inserts the closest candles
            to endTime.


    Parameters:
    -------------
    symbols: string | list of strings
        Valid symbols for the given exchange. Example: BTCUSDT, ETHBTC

    startTime: python datetime object | date string like '%Y-%m-%d %H:%M:%S'
        The date at which to begin data collection.

    endTime: python datetime object | date string like '%Y-%m-%d %H:%M:%S'
        The date at which to end data collection.

    db: string
        The name of the database to insert to.

    debug: boolean
        Setting to True will return the DataFrame that was to be inserted into
        the database.

    verbose: boolean
        Setting to True will display a progress bar for data collection and
        database inserts.

    datasource: initialized exchanges.base.ExchangeData object


    """

    if isinstance(symbols, str):
        symbols = [symbols]

    # From startTime to most recent candle
    if startTime and not endTime:
        startTime = tb.DateConvert(startTime).datetime
        endTime = datetime.utcnow()

    # Interval startTime-endTime
    elif startTime and endTime:
        startTime = tb.DateConvert(startTime).datetime
        endTime = tb.DateConvert(endTime).datetime

    # Single candle(s) closest to endTime
    elif endTime and not startTime:
        endTime = tb.DateConvert(endTime).datetime

    # TODO generalize
    if not datasource:
        datasource = BinanceData()

    if startTime and endTime:
        daterange = pd.date_range(startTime, endTime, freq='1H')

        # Calculate total num of chunks and loop iterations for progress bar
        total_chunks = len(daterange)//500
        if len(daterange) % 500:
            total_chunks+=1
        total_iterations = total_chunks*len(symbols)

        iteration = 0
        chunk_num = 1
        to_insert = pd.DataFrame()

        # API limited to 500 candles, so split date range into chunks if needed
        for subrange in tb.chunker(daterange, 500):
            sub_startTime = min(subrange)
            sub_endTime = max(subrange)

            if total_chunks > 1:
                sub_endTime+=timedelta(hours=1)

            for symbol in symbols:

                # if datasource:
                candles = datasource.candle(
                    symbol, startTime=sub_startTime, endTime=sub_endTime
                    )
                to_insert = pd.concat([to_insert, candles])

                iteration+=1
                if verbose:
                    tb.progress_bar(
                        iteration, total_iterations,
                        f'Getting {symbol}: chunk {chunk_num} of {total_iterations}'
                    )

            chunk_num+=1

            # To avoid losing data, insert in chunks if df becomes large
            if len(to_insert) >= 4000 and not debug:
                if verbose:
                    tb.progress_bar(
                        iteration, total_iterations,
                        'Inserting into db....................'
                    )
                Database(db=db).insert(
                    'candles', to_insert, auto_format=True, verbose=False
                    )
                to_insert = pd.DataFrame()

        if debug:
            return to_insert
        else:
            Database(db=db).insert(
                'candles', to_insert, auto_format=True, verbose=False
                )

    else:
        to_insert = pd.DataFrame()
        for symbol in symbols:
            candles = datasource.candle(
                symbol, startTime = startTime, endTime = endTime
            )
            to_insert = pd.concat([to_insert, candles])
        if debug:
            return to_insert
        else:
            Database(db=db).insert(
                'candles', to_insert, auto_format=True, verbose=verbose
                )



def engineer_data(from_date = None, verbose=False):
    """
    Get candles from database, add custom indicators.

    Parameters:
    ---------------
    from_date: UTC datetime, datestring, or second timestamp.
        The date at which to pull data from

    verbose: boolean
        True to print a progress bar.
    """

    def interpolate_nulls(candles):
        candles['interpolated'] = False
        null_inds = pd.isnull(candles).any(1).nonzero()[0]
        if null_inds.size:
            candles = candles.fillna(method='ffill')
            candles.interpolated.iloc[null_inds] = True

            # If nulls still in df, drop and check for continuity
            if pd.isnull(candles).any().any():
                candles = candles.dropna()
                continuous = pd.date_range(start=candles.open_date.min(),
                                           end=candles.open_date.max(),
                                           freq = '1H')

                if not (continuous == candles.open_date).all():
                    raise DiscontinuousError(
                        '''DataFrame doesn't form a continuous date
                            sequence after interpolation''' )

        return candles

    def indicators():
        for indicator in CustomIndicator.__subclasses__():
            yield indicator

    # Get timedelta for data acquisition from DB
    td = []
    num_indicators = 0
    for indicator in indicators():
        num_indicators+=1
        delta = indicator.get_timedelta()
        if delta:
            td.append(delta)

    # HACK? using max(td) will return one candle older than needed
    td = max(td)-timedelta(hours=1)

    # Get most recent date in candles
    if not from_date:
        from_date = get_max_from_column(column='open_date')
    else:
        from_date = tb.DateConvert(from_date).datetime

    from_date -= td

    if verbose:
        print('Fetching data from database...')

    # Get raw candles for transformation
    candles = Candles().get_raw(from_date = from_date)
    candles = interpolate_nulls(candles)
    candles.index = candles.symbol

    symbols = get_symbols()
    num_symbols = len(symbols)
    total_iterations = num_symbols*num_indicators

    # Calculate indicators
    ins = pd.DataFrame()
    count = 0
    for indicator in indicators():

        indicator_data = pd.DataFrame()
        for symbol in symbols:

            indicator_name = indicator.__name__

            sub_candles = candles.loc[symbol]
            sub_candles.index = sub_candles.open_date
            sub_candles = sub_candles.sort_index()

            transformed = pd.DataFrame(indicator()._transform(sub_candles))
            transformed['symbol'] = symbol
            transformed.columns = [indicator_name, 'symbol']

            indicator_data = pd.concat([indicator_data, transformed])

            count+=1
            if verbose:
                tb.progress_bar(
                    count, total_iterations,
                    f'Calculating {indicator_name}'
                )

        # return indicator_data
        if ins.empty:
            ins = indicator_data
        else:
            ins = ins.merge(indicator_data, on = ['symbol', 'open_date'])

    candles = candles.reset_index(drop=True)
    ins = ins.merge(candles, on = ['symbol', 'open_date'])
    return ins.dropna()


def repair_data(symbol = 'all', verbose=True):
    '''Iterate though candles, find missing dates, replace with Binance data.'''

    if verbose:
        print('Repairing...')

    TIME_RES = '1H' # If changed, must change timedelta parameters as well.

    # Get data for symbol
    if symbol == 'all':
        symbols = get_symbols()
    else:
        symbols = [symbol]

    for symbol in symbols:
        if verbose:
            print(symbol)
            print('------------------')

        sql = f"select * from candles where symbol = '{symbol}'"
        candles = Database().execute(sql)
        candles.index = candles.open_date

        # Get min, max date
        start = candles.open_date.min()
        end = candles.open_date.max()

        # Build date range
        daterange = pd.date_range(start, end, freq=TIME_RES)

        # Find holes in data, throw away non-nulls
        missing = []
        for date in daterange:
            if date not in candles.open_date:
                missing.append(date)

        # Find chunks of continuous dates for Binance API call
        chunks = []
        chunk = []
        for i, current_candle in enumerate(missing, start=1):
            if i < len(missing):

                next_candle = missing[i]

                if next_candle == current_candle + timedelta(hours=1):
                    chunk.append(current_candle)
                else:
                    chunk.append(current_candle)
                    chunks.append(chunk)
                    chunk = []

                if i == len(missing)-1:
                    chunk.append(next_candle)
                    chunks.append(chunk)

        if verbose:
            if missing:
                print(f'{len(missing)} missing dates found in {len(chunks)} chunks.')
            else:
                print('No missing dates found!')

        if not missing:
            print()
            continue

        # Date conversion functions for mapping
        to_binance_ts = lambda x: tb.DateConvert(x).timestamp*1000
        to_date = lambda x: tb.DateConvert(x).date

        # Get Binance data
        add = []
        for chunk in chunks:
            # Pad to be safe
            startTime = tb.DateConvert(min(chunk) - timedelta(hours=10)).date
            endTime = tb.DateConvert(max(chunk) + timedelta(hours=10)).date

            limit = len(pd.date_range(startTime, endTime, freq=TIME_RES))

            add += BinanceData().candle(
                symbol = symbol,       limit = limit,
                startTime = startTime, endTime = endTime
                ).to_dict('records')

        cols =  ['open_date', 'open', 'high', 'low', 'close', 'volume',
                'close_date', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                'ignore']


        add = pd.DataFrame(add, columns = cols).drop('ignore', axis=1)
        add['symbol'] = symbol
        add.open_date = add.open_date.map(to_date)
        add.close_date = add.close_date.map(to_date)

        missing = pd.DataFrame(missing, columns = ['open_date'])
        missing.open_date = missing.open_date.map(to_date)
        missing['symbol'] = symbol
        for col in candles:
            if col not in ['open_date', 'symbol']:
                missing[col] = None

        success = 0
        for i, row in missing.iterrows():
            missing_date = row.open_date
            if add.open_date.isin([missing_date]).any():
                missing.iloc[i,:] = add[add.open_date == missing_date].iloc[0,:]
                success+=1

        if verbose:
            print(f'Binance call returned {success} missing dates.')
            print(f'Inserting {len(missing)} items into db.')
            print()

        # return add, missing, endTime, startTime

        Database().insert('candles', missing)

# TODO test
def check_data_continuity(symbol='all', table='candles', verbose=True):
    '''Check that candles form a fully continuous date range.'''

    if symbol == 'all':
        symbols = get_symbols()
    else:
        if isinstance(symbol, str):
            symbols = [symbol]

    for symbol in symbols:
        sql = f"SELECT open_date FROM {table} WHERE symbol = '{symbol}';"
        dates = Database().execute(sql).open_date
        min_date = dates.min()
        max_date = dates.max()

        continuous_date_range = pd.date_range(min_date, max_date, freq='1H')

        discontinuous = 0
        for i, date in enumerate(dates.iloc[1:], start=1):
                prev_date = tb.DateConvert(dates.iloc[i-1]).datetime
                date = tb.DateConvert(date).datetime

                if date != prev_date + timedelta(hours=1):
                    discontinuous+=1
                    if verbose:
                        print(date)

        if discontinuous:
            discontinuous = int(discontinuous/2)+1
            print(f'There are {discontinuous} discontinuous dates in {symbol}')
        else:
            print(f'No discontinuous dates found in {symbol}')

# TODO test
def clean_candles(symbol='all', table='candles', verbose=True):
    '''Iterate through candles, deleting ones that don't start on the hour.'''

    if symbol == 'all':
        symbols = get_symbols()
    else:
        if isinstance(symbol, str):
            symbols = [symbol]

    for symbol in symbols:
        sql = f"SELECT open_date FROM {table} WHERE symbol = '{symbol}';"
        dates = Database().execute(sql).open_date

        deleted = 0
        for date in dates:
            if date.minute and date.second:
                d = tb.DateConvert(date).date
                sql = f"DELETE FROM {table} WHERE open_date = '{d}' " + \
                        f"AND symbol = '{symbol}';"
                Database().write(sql)
                deleted+=1

        if verbose:
            print(f"Deleted {deleted} items from {symbol}")


def check_ingestion_status():
    '''Print max dates in each table.'''

    sql = "SELECT NOW();"
    now = Database().execute(sql)
    msg = 'Now: '
    space = (22 - len(msg))*' '
    print(msg, space, tb.DateConvert(now.values[0][0]).date)

    sql = 'SELECT MAX(date) FROM ticker;'
    ticker_date = Database().execute(sql)
    msg = 'Max ticker date: '
    space = (22 - len(msg))*' '
    print(msg, space, tb.DateConvert(ticker_date.values[0][0]).date)

    sql = 'SELECT MAX(open_date) FROM candles;'
    candle_date = Database().execute(sql)
    msg = 'Max candle date: '
    space = (22 - len(msg))*' '
    print(msg, space, tb.DateConvert(candle_date.values[0][0]).date)

    sql = 'SELECT MAX(open_date) FROM engineered_data;'
    eng_date = Database().execute(sql)
    msg = 'Max engineered date: '
    space = (22 - len(msg))*' '
    print(msg, space, tb.DateConvert(eng_date.values[0][0]).date)

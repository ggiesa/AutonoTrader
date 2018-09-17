'''Perform data collection tasks from external sources.'''

# Basics
import pandas as pd
import numpy as np
from pathlib import Path
import time
from datetime import datetime, timedelta
from collections import OrderedDict
from copy import copy

# Custom
from utils import toolbox as tb
from utils.toolbox import DateConvert, chunker, progress_bar
from utils.database import Database, get_oldest_dates
from exchanges.binance import BinanceData
from exchanges.cryptocompare import CryptocompareData
from errors.exceptions import DiscontinuousError
from utils.toolbox import parse_datestring
from time import sleep


# TODO Avoid duplicates?
# TODO Add support for selecting based on exchange
def insert_hourly_candles(symbols, startTime=None, endTime=None,
                          db='autonotrader', debug=False, verbose=False,
                          datasource=None):
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
        for subrange in chunker(daterange, 500):
            sub_startTime = min(subrange)
            sub_endTime = max(subrange)

            if total_chunks > 1:
                sub_endTime+=timedelta(hours=1)


            for symbol in symbols:

                # if datasource:
                candles = datasource.candle(
                    symbol, startTime = sub_startTime, endTime = sub_endTime
                )

                to_insert = pd.concat([to_insert, candles])

                iteration+=1
                if verbose:
                    progress_bar(
                        iteration, total_iterations,
                        f'Getting {symbol}: chunk {chunk_num} of {total_chunks}'
                    )

            chunk_num+=1

            # To avoid losing data, insert in chunks if df becomes large
            if len(to_insert) >= 4000 and not debug:

                if verbose:
                    progress_bar(
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


def insert_historical_candles(symbols, datestring,
                              min_date = None, verbose=True):
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
    oldest_dates = get_oldest_dates(symbols)

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



def insert_tickers():
    pass


def get_ticker(from_symbol, to_symbol,
               insert = False):
    '''Get most recent price for currency pair from Binance API.'''

    # Call Binance API
    ticker = Binance().ticker(symbol = from_symbol+to_symbol)
    date = tb.DateConvert(datetime.utcnow()).date

    df = pd.DataFrame(ticker, columns = ['symbol', 'price'], index = [0])
    df['date'] = date
    df = df[['date', 'symbol', 'price']]
    df.price = pd.to_numeric(df.price)

    # Insert into MySQL server
    if insert:
        t = OrderedDict(pd.Series(df.T[0]).to_dict())
        for key in t:
            t[key] = f"'{t[key]}'" if isinstance(t[key], str) else t[key]

        Database().insert('ticker', t)
    else:
        return df


def get_all_tickers(insert = False):
    '''Get current ticker price for all currency pairs in DB. Return as
        DataFrame or insert into DB.'''

    # Get symbols and ticker data
    pairs = get_pairs()
    tickers = Binance().all_tickers()
    symbols = [row[1].from_symbol+row[1].to_symbol for row in pairs.iterrows()]
    date = tb.DateConvert(datetime.utcnow()).date

    # Filter tickers
    temp = []
    for ticker in tickers:
        if ticker['symbol'] in symbols:
            temp.append(ticker)
    tickers = temp

    df = pd.DataFrame(tickers, index = range(len(tickers)))
    df['date'] = date
    df = df[['date', 'symbol', 'price']]

    if insert:

        df.date = df.date.astype(str)
        df.symbol = df.symbol.astype(str)
        df.price = pd.to_numeric(df.price)
        df = df.to_dict(orient='records')

        for i, ticker in enumerate(df):
            for key in ticker:
                if isinstance(ticker[key], str):
                    ticker[key] = f"'{ticker[key]}'"

        Database().insert('ticker', df)
    else:
        return df


def engineer_features(candles, symbol, dropnull = True):
    '''Calculate moving averages and other derived features.'''

    assert len(candles) > 366, 'Length of candles must be greater than 366 ' \
                                + 'in order to take moving averages.'
    # Mean normalization
    def scale(col):
        ret  = (col - col.mean())/(col.max()-col.min())
        return ret

    # Find slope of an array
    def slope(narray):
        coefs = np.polyfit(range(len(narray)), narray, 1)
        return coefs[0] # Slope

    # Try converting columns to numerical
    def convert_to_numerical(candles):
        for col in candles:
            if col not in ['open_date', 'close_date']:
                try:
                    candles[col] = pd.to_numeric(candles[col])
                except:
                    continue
        return candles

    def add_last_base(candles):
        # Get bases from db
        sql = "select * from bases where symbol = '{}' and type = 'base';" \
                .format(symbol)
        bases = Database().execute(sql)
        candles = candles[candles.open_date > bases.date.min()].copy()

        last_base = []
        last_base_date = []
        for row in candles.iterrows():
            b = bases[bases.date < row[1].open_date].copy()
            b['difference'] = bases.date - row[1].open_date
            last = b[b.difference == max(b.difference)]
            last_base_date.append(tb.DateConvert(last.date.values[0]).datetime)
            last_base.append(last.price.values[0])

        candles['last_base'] = last_base
        candles['last_base_date'] = last_base_date
        return candles

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

    # Ensure correct ordering
    candles.index = candles.open_date
    candles = candles.sort_index()
    candles = candles.reset_index(drop=True)

    candles = convert_to_numerical(copy(candles))
    candles = interpolate_nulls(candles)

    # 4-hour tangent line
    df = candles[['open', 'close', 'high', 'low']].copy()
    df['avg'] = (df.high + df.low + df.open + df.close)/4
    trend_4h = df.avg.rolling(4).apply(slope, raw=True)

    # Trends of various moving windows
    candles['trend_4h'] = scale(trend_4h)

    # Gadients for each trend (the change in the slope)
    candles['trend_4h_grad'] = np.gradient(candles['trend_4h'])

    # The moving 2 week standard deviation of each trend
    candles['trend_4h_2W_STD'] = candles.trend_4h.rolling(366).std()

    # The 48-hour moving avg
    candles['moving_avg_48'] = candles.close.rolling(48).mean()
    candles['moving_std_1W'] = candles.close.rolling(168).std()
    candles['moving_std_72H'] = candles.close.rolling(72).std()

    # Find last bases
    candles = add_last_base(candles)
    candles = candles.dropna() if dropnull else candles

    return None if candles.empty else candles


def insert_all_engineered_features(verbose=True):
    '''Update the engineered_data table with most recent candles.'''

    # The number of rows needed to calculate all moving averages
    padding = 50
    M_AVG_PARAM = 366 + padding

    # Get list of symbols
    pairs = get_pairs()
    symbols = [row[1].from_symbol+row[1].to_symbol for row in pairs.iterrows()]

    if verbose:
        print('Engineering Features for...')
    for symbol in symbols:
        try:
            if verbose:
                print(symbol)

            # Find most recent engineered candle
            sql = f"select * from engineered_data where symbol ='{symbol}' " + \
                    "order by open_date desc limit 1;"
            e_candle = Database().execute(sql)

            if not e_candle.empty:
                mre_candle = e_candle.open_date.values[0]
                mre_candle = tb.DateConvert(mre_candle).datetime
                offset = timedelta(hours=M_AVG_PARAM)
                min_date = tb.DateConvert(mre_candle - offset).date

                # Check to see if engineered_data is up to date
                sql = "select max(open_date) from candles where " + \
                        f"symbol = '{symbol}';"
                max_candle = Database().execute(sql)
                max_candle = max_candle['max(open_date)'][0]
                if max_candle == mre_candle:
                    if verbose:
                        print(f'Engineered data for {symbol} is up to date')
                    continue

                # Select appropriate range of candles for calculations
                sql = f"select * from candles where symbol = " + \
                        f"'{symbol}' and open_date >= '{min_date}';"

            else:
                # If there's nothing in engineered_data, select all
                sql = f"select * from candles where symbol ='{symbol}';"

            candles = Database().execute(sql)
            candles = engineer_features(candles, symbol)

            if not e_candle.empty:
                candles = candles[candles.open_date > mre_candle]
                min_insert_date = candles.open_date.min()
                min_insert_date = tb.DateConvert(min_insert_date).datetime

                assert min_insert_date == mre_candle + timedelta(hours=1), \
                    "Dates being inserted don't form a continuous range " \
                    "with existing data."

            insert = []
            scandles = candles.to_dict(orient='records')
            for t in scandles:
                for key in t:
                    try:
                        if np.isnan(t[key]):
                            t[key] = 'NULL'
                        else:
                            t[key] = pd.to_numeric(t[key])
                    except:
                        try:
                            t[key] = tb.DateConvert(t[key]).date
                        except:
                            pass
                    if isinstance(t[key], str) and t[key] != 'NULL':
                        t[key] = f"'{t[key]}'"

                insert.append(t)
            if verbose:
                rows = len(insert)
                print(f'Inserting {rows} rows for {symbol}')
            Database().insert('engineered_data', insert)

        except Exception as err:
            print(f'Failed with {symbol}')
            raise err


def repair_data(symbol = 'all', verbose=True):
    '''Iterate though candles, find missing dates, replace with Binance data.'''

    if verbose:
        print('Repairing...')

    TIME_RES = '1H' # If changed, must change timedelta parameters as well.

    # Get data for symbol
    if symbol == 'all':
        pairs = get_pairs()
        symbols = [row[1].from_symbol+row[1].to_symbol for row in pairs.iterrows()]
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
            startTime = to_binance_ts(startTime)
            endTime = to_binance_ts(endTime)

            add += Binance().candle(symbol = symbol, limit = limit,
                                    startTime = startTime, endTime = endTime)

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


        missing = missing.to_dict(orient='records')
        for t in missing:
            for key in t:
                try:
                    if not t[key]:
                        t[key] = 'NULL'
                    else:
                        t[key] = pd.to_numeric(t[key])
                except:
                    t[key] = \
                        f"'{t[key]}'" if isinstance(t[key], str) else t[key]

        if verbose:
            print(f'Inserting {len(missing)} items into db.')
            print()

        Database().insert('candles', missing)


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

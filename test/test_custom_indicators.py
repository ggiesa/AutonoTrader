
import pandas as pd
from ingestion.custom_indicators import CustomIndicator
from utils.database import (
    Candles, get_symbols, get_most_recent_dates, Database, add_column
    )
from utils.toolbox import progress_bar, DateConvert
from datetime import datetime, timedelta


def engineer_data(from_date = None, verbose=False):

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

    td = max(td)

    # Get most recent date in candles
    if not from_date:
        sql = 'SELECT MAX(open_date) FROM candles;'
        from_date = Database().execute(sql).iloc[0][0]
    else:
        from_date = DateConvert(from_date).datetime
    from_date -= td

    if verbose:
        print('Fetching data from database...')

    # Get raw candles for transformation
    candles = Candles().get_raw(from_date = from_date)
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
                progress_bar(
                    count, total_iterations,
                    f'Calculating {indicator_name} for {symbol}'
                )

        # return indicator_data
        if ins.empty:
            ins = indicator_data
        else:
            ins = ins.merge(indicator_data, on = ['symbol', 'open_date'])

    candles = candles.reset_index(drop=True)
    ins = ins.merge(candles, on = ['symbol', 'open_date'])
    return ins.dropna()



def insert_engineered_data(verbose = True):

    verbose=True

    # Get indicators from subclasses
    indicators = list(CustomIndicator.__subclasses__())

    # Get columns for comparison to incoming indicators
    sql = 'SHOW COLUMNS IN engineered_data;'
    candle_cols = list(Database().execute(sql).Field)

    for indicator in indicators:
        if indicator.__name__ not in candle_cols:
            add_column('engineered_data', indicator.__name__, 'float(20,9)')

    # Get starting date for insert
    sql = 'SELECT MAX(open_date) FROM engineered_data;'
    from_date = Database().execute(sql)['MAX(open_date)'].iloc[0]

    # If there's nothing in the table, populate the entire thing
    if not from_date:
        sql = 'SELECT MIN(open_date) FROM candles;'
        from_date = Database().execute(sql)['MIN(open_date)'].iloc[0]

    ins = engineer_data(from_date=from_date, verbose=verbose)

    # Convert to sql-friendly dates
    convert_to_sql = lambda x: DateConvert(x).date
    ins.open_date = ins.open_date.map(convert_to_sql)
    ins.close_date = ins.close_date.map(convert_to_sql)


    if verbose:
        print('Inserting engineered data into database...')

    Database().insert('engineered_data', ins, verbose=verbose, auto_format=True)

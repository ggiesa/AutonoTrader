"""Data ingestion tasks to be scheduled for regular execution."""


def update_candles(debug=False):
    """
    Insert candles from the most recent open_date in the database to
    the current time.
    """

    symbols = get_symbols()
    startTime = get_max_open_date()

    if debug:
        return insert_hourly_candles(symbols, startTime=startTime, debug=True)
    else:
        insert_hourly_candles(
            symbols, startTime=startTime, debug=False, verbose=True
        )


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


def get_ticker(from_symbol, to_symbol, insert = False):
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


# TODO test
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

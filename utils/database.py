"""Module for handling interaction with the MySQL database."""

import pymysql
import pandas as pd
from config import config
from pymysql.err import OperationalError, InternalError
from utils.toolbox import format_records, progress_bar, chunker, DateConvert


class Database:
    '''Connect to MySQL database.'''
    def __init__(self, config = config.mysql, db='autonotrader'):

        # Establish connection to MySQL server
        self.config = config
        self.connection = \
                pymysql.connect(
                     host=self.config['host'],
                     user=self.config['user'],
                     password=self.config['password'],
                     port=self.config['port'],
                     charset='utf8mb4',
                     cursorclass=pymysql.cursors.DictCursor
                     )
        if db:
            try:
                self.connection.select_db(db)
            except InternalError as err:
                print(f'Cannot access database {db}. Most likely, it does not exist')
                raise err

        self.cursor = self.connection.cursor()


    def check_connection(self):
        '''Check to see if connection to database is active'''
        return True if self.connection.open else False


    def close_connection(self):
        '''Close the connection to the database.'''
        self.connection.close()


    def write(self, sql):
        '''
        Perform any command that requires commiting a change to the database.

        Parameters:
        -----------
        sql: string
            A valid MySQL command.

        '''
        try:
            with self.cursor as cursor:
                cursor.execute(sql)
            self.connection.commit()

        except Exception as err:
            print(sql)
            raise err


    def insert(self, table, ins, auto_format=False, verbose=False):
        '''
        Insert a new row or set of rows into a table.

        Parameters:
        ------------
        table: string
            The name of the SQL table to be inserted into.

        ins: dict | list of dicts | pandas.DataFrame
            Data to be inserted. Dicts should be structured with keys
            corresponding to column names in the sql table.

            Examples:

                Single dict:
                {'<column1>':<value1>, '<column2>':<value2>}

                List of dicts (records):
                [{'<column1>':<value1>, '<column2>':<value2>},
                {'<column1>':<value1>, '<column2>':<value2>}]

                DataFrame:
                | column1 | column2 | column3 |
                -------------------------------
                | value1  | value2  | value3  |
                | value4  | value5  | value6  |

        auto_format: boolean
            True ---> Format dict items for insert into database:
                - Add parentheses around strings
                - Convert None values to NULL
                - Format dates to be friendly with SQL

        verbose: boolean
            True ---> If insert is large, display a progress bar.

        '''

        sql = None
        if ins is None:
            return
        elif isinstance(ins, pd.DataFrame):
            ins = ins.to_dict('records')

        if isinstance(ins, list):
            if len(ins) == 1:
                ins = ins[0]
            elif auto_format:
                ins = format_records(ins)

        num_chunks = len(ins)//1000
        if num_chunks and len(ins)%1000:
            num_chunks+=1

        try:
            iteration=0
            with self.cursor as cursor:
                if isinstance(ins, list):

                    # Parse dict keys into sql-formatted columns
                    columns = str(tuple(ins[0].keys())).replace("'", '')

                    # Insert items into db in chunks of 1000 if necessary
                    for chunk in chunker(ins, 1000):
                        insert = ''
                        for i, item in enumerate(chunk):
                            add = str(tuple(item.values())).replace("'", '')
                            add = add + ', ' if i < len(chunk)-1 else add
                            insert += add

                        sql = f"INSERT IGNORE INTO {table} {columns} VALUES {insert};"
                        cursor.execute(sql)
                        self.connection.commit()

                        iteration+=1
                        if verbose and num_chunks:
                            progress_bar(
                                iteration, num_chunks,
                                f'Inserting chunk {iteration} of {num_chunks}'
                            )

                else:
                    insert = str(tuple(ins.values())).replace("'", '')
                    columns = str(tuple(ins.keys())).replace("'", '')
                    if len(ins.values()) == 1:
                        insert = insert.replace(',', '')
                    if len(ins.keys()) == 1:
                        columns = columns.replace(',', '')

                    sql = f"INSERT IGNORE INTO {table} {columns} VALUES {insert};"
                    cursor.execute(sql)
                    self.connection.commit()

                    iteration+=1
                    if verbose and num_chunks:
                        progress_bar(
                            iteration, num_chunks,
                            f'Inserting chunk {iteration} of {num_chunks}'
                        )


        except Exception as err:
            if sql:
                print(sql)
            raise err


    def execute(self, sql):
        '''Return a pandas.DataFrame containing data a sql SELECT command.'''

        try:
            with self.cursor as cursor:
                cursor.execute(sql)
            result = self.cursor.fetchall()
            if result:
                if as_df:
                    result = pd.DataFrame(result)
                    if 'id' in result.columns:
                        result = result.drop('id', axis=1)
                    return result
                else:
                    return result
            else:
                return pd.DataFrame()

        except Exception as err:
            print(sql)
            raise err


    def delete(self):
        pass


# TODO Should probably be moved to utils.toolbox
class AssembleSQL:
    """Compose a SQL query given a table and set of logical conditions."""

    def _assemble_sql(self, table, conditions = None):
        """
        Parameters:
        ----------
        table: string
            A valid name of a SQL table

        conditions: dict | list of dicts
            dict or list of dicts containing SQL WHERE conditions.
            Format:
                {'column':<column name>,
                'operator':<logical_operator>,
                'value':<comparison value>}
            Example:
                {'column':close,
                'operator':>,
                'value':2000}

        Returns:
        ---------
        sql: string
            Formatted SQL query.
        """

        sql = f"SELECT * FROM {table}"
        if conditions:

            if isinstance(conditions, dict):
                conditions = [conditions]

            add = []
            for c in conditions:
                if isinstance(c['value'], str):
                    add.append(f"{c['column']} {c['operator']} '{c['value']}'")
                else:
                    add.append(f"{c['column']} {c['operator']} {c['value']}")

            add = ' AND '.join(add)
            sql += f" WHERE {add}"

        return sql


class Candles(AssembleSQL):
    """Get candles from an SQL database."""

    def get_raw(self, symbol = None, from_date = None, to_date = None):
        """
        Get raw candles from the database.

        Parameters:
        -----------
        symbol: string
            A valid cryptocurreny symbol.

        from_date, to_date: string, format '%Y-%m-%d %H:%M:%S'
            Dates for query, resulting in expression:
            from_date < open_date < to_date

        Returns
        -----------
        candles: pd.DataFrame
            The results of the composed query.
        """

        conditions = []
        if from_date:
            from_date = DateConvert(from_date).date
            conditions.append(dict(column = 'open_date',
                                   operator = '>',
                                   value = from_date))
        if to_date:
            to_date = DateConvert(to_date).date
            conditions.append(dict(column = 'open_date',
                                   operator = '<',
                                   value = to_date))
        if symbol:
            conditions.append(dict(column = 'symbol',
                                   operator = '=',
                                   value = symbol))

        sql = self._assemble_sql('candles', conditions = conditions)
        sql += ' ORDER BY open_date DESC;'

        return Database().execute(sql)


    def get_engineered(self, symbol = None, from_date = None, to_date = None):
        """
        Get engineered candles from the database.

        Parameters:
        -----------
        symbol: string
            A valid cryptocurreny symbol.

        from_date, to_date: string, format '%Y-%m-%d %H:%M:%S'
            Dates for query, resulting in expression:
            from_date < open_date < to_date

        Returns
        -----------
        candles: pd.DataFrame
            The results of the composed query.
        """

        conditions = []
        if from_date:
            from_date = DateConvert(from_date).date
            conditions.append(dict(column = 'open_date',
                                   operator = '>',
                                   value = from_date))
        if to_date:
            to_date = DateConvert(to_date).date
            conditions.append(dict(column = 'open_date',
                                   operator = '<',
                                   value = to_date))
        if symbol:
            conditions.append(dict(column = 'symbol',
                                   operator = '=',
                                   value = symbol))

        sql = self._assemble_sql('engineered_data', conditions = conditions)
        sql += ' ORDER BY open_date DESC;'

        return Database().execute(sql)


class Trades(AssembleSQL):

    def get_trades(self, symbol = None, from_date = None,
                   to_date = None,  type = None):

        """
        Get bot trades from the database.

        Parameters:
        -----------
        symbol: string
            A valid cryptocurreny symbol.

        from_date, to_date: string, format '%Y-%m-%d %H:%M:%S'
            Dates for query, resulting in expression:
            from_date < date < to_date

        type: string; 'buy' | 'sell'
            Filter trades by type

        Returns
        -----------
        candles: pd.DataFrame
            The results of the composed query.
        """

        conditions = []
        if from_date:
            from_date = DateConvert(from_date).date
            conditions.append(dict(column = 'date',
                                   operator = '>',
                                   value = from_date))
        if to_date:
            to_date = DateConvert(to_date).date
            conditions.append(dict(column = 'date',
                                   operator = '<',
                                   value = to_date))
        if symbol:
            conditions.append(dict(column = 'symbol',
                                   operator = '=',
                                   value = symbol))
        if type:
            conditions.append(dict(column = 'type',
                                   operator = '=',
                                   value = type))

        sql = self._assemble_sql('trades', conditions = conditions)
        sql += ' ORDER BY date DESC;'

        return Database().execute(sql)


def get_symbols():
    """Return a list of user symbols from the user_symbols table."""
    ret =  Database().execute(
        'SELECT symbol FROM user_symbols;'
        )
    return list(ret.symbol)


def get_pairs():
    """
    Return a pandas.DataFrame of user symbols from the user_symbols table, like:
    | from_symbol | to_symbol |
    """
    ret = Database().execute(
        'SELECT from_symbol, to_symbol FROM user_symbols;'
        )
    return ret[['from_symbol','to_symbol']]


def get_symbols_and_pairs():
    """
    Return a pandas.DataFrame of user symbols from the user_symbols table, like:
    | symbol | from_symbol | to_symbol |
    """
    ret =  Database().execute(
        'SELECT * FROM user_symbols;'
        )
    ret.index=ret.symbol
    return ret[['symbol','from_symbol','to_symbol']]


def get_most_recent_dates(symbols, db='autonotrader'):
    """
    Get the most recent candle date for a given symbol or list of symbols in the
    database.

    Parameters:
    -------------
    symbol: string
        A valid cryptocurreny symbol

    Returns:
    -------------
    most_recent_dates: dict
        dict like {'<symbol>':<most_recent_date>}
    """

    if isinstance(symbols, str):
        symbols = [symbols]

    most_recent_dates = {}
    for symbol in symbols:
        sql = f"SELECT MAX(open_date) FROM candles WHERE symbol = '{symbol}'"
        date = Database(db=db).execute(sql)['MAX(open_date)'].iloc[0]

        if date:
            date = DateConvert(date).datetime

        most_recent_dates[symbol] = date

    return most_recent_dates


def get_oldest_dates(symbols, db='autonotrader'):
    """
    Get the earliest candle date for a given symbol or list of symbols in the
    database.

    Parameters:
    -------------
    symbol: string
        A valid cryptocurreny symbol

    Returns:
    -------------
    most_recent_dates: dict
        dict like {'<symbol>':<earliest_date>}
    """

    if isinstance(symbols, str):
        symbols = [symbols]

    oldest_dates = {}
    for symbol in symbols:
        sql = f"SELECT MIN(open_date) FROM candles WHERE symbol = '{symbol}'"
        date = Database(db=db).execute(sql)['MIN(open_date)'].iloc[0]

        if date:
            date = DateConvert(date).datetime

        oldest_dates[symbol] = date

    return oldest_dates

"""Module for handling interaction with the MySQL database."""

import pymysql
import pandas as pd
from config import config
from pymysql.err import OperationalError, InternalError, ProgrammingError
from utils.toolbox import format_records, progress_bar, chunker, DateConvert


class Database:
    '''Connect to MySQL database.'''
    def __init__(self, config = config.mysql, db=None):

        if not db:
            db = 'autonotrader'

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


    def insert(self, table, ins, auto_format=True, verbose=False):
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

        if isinstance(ins, pd.DataFrame):
            if ins.empty:
                return
            ins = ins.to_dict('records')
        elif not isinstance(ins, [list, dict]):
            raise TypeError(f'''
                    Data to insert should be a Pandas DataFrame, dict, or list
                    of dicts. Instead Database.insert recieved type {type(ins)}
                ''')

        if not ins:
            return

        if auto_format:
            if verbose:
                print('Formatting input...')
            ins = format_records(ins)

        if isinstance(ins, list):
            if len(ins) == 1:
                ins = ins[0]

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
        '''Return a DataFrame containing data from a sql SELECT command.'''

        try:
            with self.cursor as cursor:
                cursor.execute(sql)
            result = self.cursor.fetchall()
            if result:
                result = pd.DataFrame(result)
                if 'id' in result.columns:
                    result = result.drop('id', axis=1)
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
        Get raw candles from the database. With no parameters, returns entire
        table.

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
                                   operator = '>=',
                                   value = from_date))
        if to_date:
            to_date = DateConvert(to_date).date
            conditions.append(dict(column = 'open_date',
                                   operator = '<=',
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
                                   operator = '>=',
                                   value = from_date))
        if to_date:
            to_date = DateConvert(to_date).date
            conditions.append(dict(column = 'open_date',
                                   operator = '<=',
                                   value = to_date))
        if symbol:
            conditions.append(dict(column = 'symbol',
                                   operator = '=',
                                   value = symbol))

        sql = self._assemble_sql('engineered_data', conditions = conditions)
        sql += ' ORDER BY open_date DESC;'

        return Database().execute(sql)


class Trades(AssembleSQL):

    def get_trades(self, symbol = None,   from_date = None,
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


def get_most_recent_dates(symbols=None, db='autonotrader'):
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
    if not symbols:
        symbols = get_symbols()

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


def get_oldest_dates(symbols=None, db='autonotrader'):
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

    if not symbols:
        symbols = get_symbols()

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


def add_column(table, column, datatype, db=None):
    """
    Add a column to a MySQL table.

    Parameters:
    -----------------
    table: str
        The name of the table to alter

    column: str
        The name of the column to create

    datatype: str
        A valid MySQL data type.
            Examples:
                - 'datetime'
                - 'varchar(100)'
                - 'int(10)'
    """
    sql = f'ALTER TABLE {table} ADD {column} {datatype};'
    Database(db=db).execute(sql)


# TODO convert to generalized table object
class CreateTable:

    def __init__(self, table_name, dataframe,
                 db=None, fail_on_duplicate=False, verbose=False):
        """
        Automatically build a table from a dataframe.

        Parameters:
        --------------
        table_name: str
            The name of the table to be created

        dataframe: pandas.DataFrame
            A DataFrame with columns corresponding to the SQL table column names,
            and data to be interpreted and automatically assigned a SQL type.

        db: str
            The name of the database to create the table in.
        """
        self.table_name = table_name
        self.verbose = verbose
        self.data = dataframe
        self.db = db

        self.table_exists = None
        self._discover_table()

    def _discover_table(self):
        """Check to see if table exists in db. If it does, get information."""

        # Check to see if table exists
        sql = f'SHOW COLUMNS IN {self.table_name};'
        columns = None
        try:
            columns = Database(db=self.db).execute(sql)
        except ProgrammingError as e:
            if e.args[0] != 1146:
                raise e

        self.table_exists = True if columns is not None else False
        if self.table_exists:
            if self.verbose:
                print('Table already exists. Exiting.')
            return
        else:
            self._compose_sql()


    def _compose_sql(self):
        """Compose the create table statement."""

        if self.verbose:
            print('Creating build table statement.')

        # Conversion from pd to sql dtypes
        dtype_eq = {
            'datetime': 'datetime',
            'float': 'float(20,9)',
            'int': 'int(15)',
            'bool': 'boolean',
            'object': 'varchar(40)'
            }

        # Detect dtypes
        dtypes = self.data.dtypes
        columns = {column:str(dtypes[f'{column}']) for column in dtypes.index}

        # Swap pandas dtype to sql dtype
        for column, pd_dtype in columns.items():
            found = False
            for basetype in dtype_eq:
                if basetype in pd_dtype:
                    columns[column] = dtype_eq[basetype]
                    found = True
                    continue

            if not found:
                raise TypeError(f'''Cannot find a way to convert from pandas
                                    dtype {pd_dtype} to SQL.''')

        # Compose column creation SQL
        self.row_dec = ''
        i = 0
        for column, dtype in columns.items():
            self.row_dec += f'`{column}` {dtype}'
            self.row_dec += ', ' if i < len(columns)-1 else ''
            i+=1

        self._create_table()

    def _create_table(self):
        """Execute the create table SQL query."""

        if self.verbose:
            print('Creating table.')

        sql = f"""
        CREATE TABLE `{self.table_name}` (
        {self.row_dec}
        );
        """
        try:
            Database(db=self.db).execute(sql)
        except Exception as e:
            print(f'Failed to create table with statement: {sql}')
            raise e


def get_max_from_column(table='candles', column='open_date', db='autonotrader'):
    sql = f'SELECT MAX({column}) FROM {table};'
    date = Database(db=db).execute(sql)[f'MAX({column})'].iloc[0]
    return DateConvert(date).datetime


def get_min_from_column(table='candles', column='open_date', db='autonotrader'):
    sql = f'SELECT MIN({column}) FROM {table};'
    date = Database(db=db).execute(sql)[f'MIN({column})'].iloc[0]
    return DateConvert(date).datetime


def check_table_existence(table, db=None):
    """
    Check to see if <table> exists in <db>.

    Parameters:
    -------------
    table: str
        The name of the table to check the existence of.

    db: str
        The name of the database to use in query.

    Returns:
    -------------
    table_exists: boolean
        True if the table is found in the database, False if not.
    """

    sql = 'SHOW TABLES;'
    tables = set(Database(db=db).execute(sql).iloc[:,0])
    return True if table in tables else False

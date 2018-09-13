"""Helper functions for initial setup tasks."""

from utils.database import Database
from config.data_collection import historical_config
from errors.exceptions import ImplementationError
from datetime import timedelta


def create_db(db_name, schema_path):
    """
    Create a new database with a set of table schemas.

    Parameters:
    ------------
    db_name: str
        The name of the new database to be created

    schema_path: str
        The path to the .sql file containing table schema.
    """
    table_schemas = open('data/MySQL_scripts/build_tables.sql')
    table_schemas = ' '.join(table_schemas.readlines()).replace('\n','')

    databases = list(Database(db=None).execute('SHOW databases;').Database)
    if db_name not in databases:
        sql = f'CREATE DATABASE {db_name};'
        Database(db=None).execute(sql)
        Database(db=db_name).execute(table_schemas)
    else:
        print(f'{db_name} already exists. Continuing.')


def parse_and_validate_symbols(user_symbols, exchange):
    """
    Parse symbols from <from_symbol>/<to_symbol> format to list of dicts like:
        [{
        symbol :       <from_symbol><to_symbol>
        from_symbol :   <from_symbol>
        to_symbol :     <to_symbol>
        exchange :      <exchange>
        }]

    Verify that symbols are valid by comparing to full set of exchange symbols.

    Parameters:
    -------------------
    user_symbols: list of strings
        Market symbols provided by user. Example: [BTC/USDT, LTC/BTC]
    """

    ex_symbols = f"SELECT symbol FROM all_symbols WHERE exchange = '{exchange}';"
    ex_symbols = list(Database().execute(ex_symbols).symbol)

    ins = []
    for symbol in user_symbols:

        # Fail hard if any symbol is invalid.
        if symbol.replace('/','') not in ex_symbols:
            raise ImplementationError(f'''
                {symbol} is not traded on the {exchange} exchange. Check
                user_symbols in config/data_collection.py.
                Possible symbols are:
                {ex_symbols}
            ''')

        # Format for DB insert
        ind = symbol.find('/')
        add = {
            'symbol' : symbol.replace('/',''),
            'from_symbol' : symbol[:ind],
            'to_symbol' : symbol[ind+1:],
            'exchange' : exchange
        }
        ins.append(add)

    return ins


def parse_historical_collection_period(collection_period):
    """
    Parse user provided historical_collection_period string into timedelta obj.

    Parameters:
    -----------
    collection_period: string
        Example: '1Y', '2Y'

    Returns:
    -----------
    diff: datetime.timedelta
        Example: '1Y' ---> datetime.timedelta(days = 365)
        Example: '1Y' ---> datetime.timedelta(days = 365)
    """

    collection_period = collection_period.upper()
    num = ''
    char = ''
    for c in collection_period:
        if c.isdigit():
            num+=c
        else:
            char+=c

    try:
        num = int(num)
    except ValueError as e:
        raise ImplementationError('''
            Must include an integer in input string. Check
            config/data_collection.historical_config
        ''')

    if char == 'Y':
        return timedelta(days=num*365)
    elif char == 'M':
        return timedelta(days=num*30)
    elif char == 'D':
        return timedelta(days=num)
    else:
        raise ImplementationError('''
            Must designate month/day/year as M/D/Y. Check
            config/data_collection.historical_config
        ''')

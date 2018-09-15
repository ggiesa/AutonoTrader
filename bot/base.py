"""Internal workings of Backtest, Forwardtest, and Live bot operations."""

# Basics
import pandas as pd
import numpy as np
from copy import copy
from pathlib import Path
from datetime import datetime, timedelta
import math
from yattag import Doc
from copy import copy, deepcopy
import time
from uuid import uuid4
from logging import warning

# Plotting
import plotly.graph_objs as go
import plotting
import pickle

# Custom
from utils import toolbox as tb
from ingestion import data_collection as dc
from errors.exceptions import DiscontinuousError, ImplementationError


# TODO Restructure SQL parameter design
class Core:
    def __init__(self, sql_config = {}, verbose = True):
        """
        Core uses user supplied data and algorithms to generate trading signals.

        Parameters:
        -------------
        sql_config: dict with optional keys. Empty to operate without DB.
            key options:
            'truncate_tables':boolean
                True ---> truncate buy/sell/pending tables in the database
                before insert.
            'test':boolean
                True ---> use the test db rather than production db

        verbose: boolean
            True ---> display a progress bar with profit stats.


        """

        self.sql_config = sql_config
        self.verbose = verbose
        self._setup()

    def _setup(self):
        """Acquire data and initialize portfolio and state variables."""

        # Get user data
        self.symbols = self._get_symbols()
        self.data_dict = self._get_data()
        self.portfolio = self.initialize_portfolio()

        if 'slippage' in self.portfolio:
            self.slippage = self.portfolio['slippage']
        else:
            self.slippage = None

        # Keep track of all trades
        self.trade_manager = TradeManager(
            self.symbols, self.portfolio, self.sql_config
            )

        # Initialize state variables that are updated each iteration
        self.date = None
        self.data = None
        self.symbol = None
        self.currency = None
        self.last_buy = None
        self.num_unresolved = 0
        self.unresolved_trade = False


    def _get_data(self):
        """Parse user-supplied data into dict of DataEngine objects."""

        data = self.get_data()

        required_data = ['open','close','open_date','high','low']
        if not np.isin(required_data, data.columns).all():
            raise ImplementationError(f'''
                Data must contain columns: {required_data}
            ''')

        data = data.sort_values('open_date')
        data.index = data.open_date

        temp_dates = pd.unique(data.open_date)
        self.total_candles = len(temp_dates)
        self.start_date, self.end_date = min(temp_dates), max(temp_dates)

        # Divide df based on symbol, create DataEngine object, add to dict.
        data_dict = {}
        for symbol in self.symbols.symbol:
            try:
                data_dict[symbol] = DataEngine(data[data.symbol == symbol])
            except DiscontinuousError as err:
                print(f'There are missing dates in data for {symbol}')
                raise err
            except ValueError as err:
                print(f'No data for provided for symbol: {symbol}')
                self.symbols = self.symbols.drop(symbol)

        return data_dict


    # TODO Automatically find symbols from DB?
    def _get_symbols(self):
        """Ensure that user-supplied symbols are formatted correctly."""

        symbols = self.get_symbols()

        if isinstance(symbols, dict):
            keys = ['symbol', 'from_symbol', 'to_symbol']
            correct_keys = np.isin(keys, list(symbols.keys())).all()

            if not correct_keys:
                raise ImplementationError('''
                    Dict should be in the form:
                        {'symbol':[], 'from_symbol':[], 'to_symbol':[]}
                ''')
            else:
                symbols = pd.DataFrame(symbols, index = [symbols['symbol']])

        symbols.index = symbols.symbol

        return symbols


    def get_data(self):
        """
        If no data is provided at initialization, the get_data method
        must be implemented to return a pandas DataFrame with market
        data candles and any other features needed in the trading strategy.
        The DataFrame should contain columns:

        | symbol | open_date | close_date | open | close | high | low |
        """

        raise NotImplementedError('''
            Must Implement get_data. Call help() for details.
        ''')


    def get_symbols(self):
        """
        If no symbols are provided at initialization, the get_symbols method
        must be implemented to return a pandas DataFrame containing all
        portfolio symbols in the form:

            | symbol | from_symbol | to_symbol |

            For example:

            | BTCUSDT | BTC | USDT |

        Alternatively, a dict can be returned in the form:

            {'symbol':[], 'from_symbol':[], 'to_symbol':[]}
        """

        raise NotImplementedError('''
            Must implement get_symbols. Call help() for details.
        ''')


    def initialize_portfolio(self):
        """
        Must implement initialize_portfolio method that sets
        self.purse, self.max_buy_p and self.holdout_p.
        """

        raise NotImplementedError('''
            Must implement initialize_portfolio. Call help() for details.
        ''')

    def generate_signals(self):
        """
        Implement generate_signals method that determines whether to buy,
            sell, or do nothing.
            - If buying, call the buy() method.
            - If selling, call the sell() method.
        """

        raise NotImplementedError('''
            Must implement generate_signals. Call help() for details.
        ''')


    def report(self):
        """
        Print a report of generated profits containing:
            - Initial Portfolio Value
            - Final Portfolio Value
            - Portfolio Value Change
            - Portfolio Value Percent Change
            - Total Trades
            - Trades Per Week
            - Overall Market Change
            - Max Consecutive Losses
            - Win Percent
        """
        metrics = Metrics(self)
        metrics.report()

    def buy(self):
        """Simulate a buy order on the current price using TradeManager."""

        from_symbol = self.symbol
        to_symbol = self.currency
        price = self.data[0].close
        amount = self.portfolio['buy_sell_amount'][self.currency]
        date = self.date

        if self.slippage:
            slip_factor = (self.data[-1].high - self.data[-1].close)*self.slippage
            price += np.abs(slip_factor)

        self.trade_manager.buy(from_symbol, to_symbol, price, amount, date)


    def sell(self):
        """
        Simulate a 1:1 market sell order. TradeManager searches for the pending
        trade with the lowest price, sells the same amount that was bought, and
        records profits.
        """

        from_symbol = self.symbol
        to_symbol = self.currency
        price = self.data[0].close
        amount = self.portfolio['buy_sell_amount'][self.currency]
        date = self.date

        if self.slippage:
            slip_factor = (self.data[-1].high - self.data[-1].close)*self.slippage
            price -= np.abs(slip_factor)

        self.trade_manager.sell(from_symbol, to_symbol, price, amount, date)


    def sell_all(self):
        """Simulate a complete exit in position with TradeManager."""

        from_symbol = self.symbol
        to_symbol = self.currency
        price = self.data[0].close
        amount = self.portfolio['buy_sell_amount'][self.currency]
        date = self.date

        if self.slippage:
            slip_factor = (self.data[-1].high - self.data[-1].close)*self.slippage
            price -= np.abs(slip_factor)

        self.trade_manager.sell_all(from_symbol, to_symbol, price, amount, date)


    def run(self):
        """
        Iterate through available market data, generating signals and buying/
        selling accordingly.
        """
        self._run()

        if self.sql_config:
            if self.verbose:
                print('Inserting trades')
            self.trade_manager.sqlm.insert_trades()


    def _run(self):

        # Keep track of iterations for progress bar
        count = 1

        # Initialize symbol and DataEngine
        symbol = self.symbols.symbol.iloc[0]
        self.data = self.data_dict[symbol]

        while not self.data.finished:
            for symbol in self.symbols.symbol:

                self.data = self.data_dict[symbol]
                if self.data.finished:
                    continue

                # Update state variables
                self.symbol = symbol
                self.date = self.data[0].open_date
                self.currency = self.symbols.loc[symbol].to_symbol
                self.unresolved_trade = self.trade_manager.unresolved_trade[symbol]
                self.last_buy = self.trade_manager.last_buy[self.symbol]
                self.num_unresolved = self.trade_manager.num_unresolved[self.symbol]

                self.generate_signals()
                self.data_dict[symbol].increment()

            # Update progress bar
            if self.verbose:
                m = self.trade_manager.currency_earned.items()
                msg = ', '.join([f"{cur}: {round(amt,4)}" for cur,amt in m])

                status = f'Currency earned: ' + msg
                tb.progress_bar(count, self.total_candles, status = status)
                count += 1


class TradeManager:
    """Keeps track of bot trades."""
    def __init__(self, symbols, portfolio, sql_config):

        self.symbols = symbols
        self.purse = portfolio['purse']
        self.initial_purse = copy(self.purse)
        self.currencies = list(self.symbols.to_symbol.unique())

        if 'holdout' in portfolio:
            self.holdout = portfolio['holdout']
        else:
            self.holdout = {c:0 for c in self.currencies}

        if 'trading_fee' in portfolio:
            self.trading_fee = portfolio['trading_fee']
        else:
            self.trading_fee = None

        self.currency_earned = {cur:0 for cur in self.currencies}
        self.unresolved_trade = {s:False for s in self.symbols.index}
        self.num_unresolved = {s:0 for s in self.symbols.index}
        self.last_buy = {s:None for s in self.symbols.index}
        self.all_buys =  pd.DataFrame()
        self.all_sells = pd.DataFrame()

        # Track trades for individual coins
        self.trades = {

            symbol:{

                'buys':{
                     'id':[],
                     'date':[],
                     'price':[],
                     'amount_fs':[],
                     'amount_ts':[]
                     },

                'sells':{
                     'id':[],
                     'date':[],
                     'price':[],
                     'amount_fs':[],
                     'amount_ts':[],
                     'profit':[],
                     'percent_profit':[],
                     'trades_resolved':[]
                     },

                'profit':{
                     'from_symbol':0,
                     'to_symbol':0
                     },

                'unresolved_trades':[],

                'total_invested':0

                } for symbol in self.symbols.symbol}

        if sql_config:
            self.sqlm = SQLManager(sql_config)
        else:
            self.sqlm = None

    def __repr__(self):
        # Assume the representation of the trades dict
        return self.trades.__repr__()

    def buy(self, from_symbol, to_symbol, price, amount, date):
        """Simulate market buy."""

        # Don't buy if we'd go over the holdout amount
        if self.purse[to_symbol] > self.holdout[to_symbol]:

            # Amount bought, from symbol and to symbol
            amount_ts = amount
            amount_fs = amount_ts/price
            self.purse[to_symbol] -= amount_ts

            update = {
                'id':uuid4().hex[:10],
                'date':date,
                'price':price,
                'amount_ts': amount_ts,
                'amount_fs': amount_fs
                }

            for key, value in update.items():
                self.trades[from_symbol]['buys'][key].append(value)

            self.trades[from_symbol]['total_invested'] += amount_fs
            self.unresolved_trade[from_symbol] = True
            self.last_buy[from_symbol] = {'date':date, 'price':price}

            self._append_unresolved(update, from_symbol, date, price)
            self._append_all_buys(update, from_symbol, date)


    def sell(self, from_symbol, to_symbol, price, amount, date):
        """Simulate market sell."""

        if self.unresolved_trade[from_symbol]:
            self.trades[from_symbol]['sells']['date'].append(date)
            self.trades[from_symbol]['sells']['price'].append(price)

            self._resolve_trades(from_symbol, to_symbol, date, price)


    def sell_all(self, from_symbol, to_symbol, price, amount, date):
        """Simulate market sell, selling entire position."""

        if self.unresolved_trade[from_symbol]:
            self.trades[from_symbol]['sells']['date'].append(date)
            self.trades[from_symbol]['sells']['price'].append(price)

            self._resolve_trades(
                from_symbol, to_symbol, date, price, sell_all= True
                )


    def _resolve_trades(self, from_symbol, to_symbol, date, price, sell_all=False):
        """
        Calculate and record the profits generated from a sell, factoring in
        slippage and trading fees.
        """

        # Calculate profits on past trades
        trades = deepcopy(self.trades[from_symbol])
        unresolved_trades = trades['unresolved_trades']

        resolving = {
            'amount_fs':0,
            'amount_ts':0,
            'profit':0,
            'percent_profit':[],
            'trades_resolved':[]
            }

        if sell_all:

            for trade in unresolved_trades:

                diff = trade['amount_fs']*price - trade['amount_ts']
                if self.trading_fee:
                    diff -= self.trading_fee*trade['amount_ts']

                percent_diff = 100*diff/trade['amount_ts']

                resolving['amount_fs'] += trade['amount_fs']
                resolving['amount_ts'] += trade['amount_fs']*price
                resolving['trades_resolved'].append(trade['id'])
                resolving['profit'] += diff
                resolving['percent_profit'].append(percent_diff)
                self._remove_unresolved(trade, from_symbol)

        else:

            # Find lowest pending trade, calculate profit based on that
            lowest_price = unresolved_trades[0]['price']
            id = unresolved_trades[0]['id']

            for trade in unresolved_trades:
                if trade['price'] < lowest_price:
                    id = trade['id']

            for trade in unresolved_trades:

                if trade['id'] == id:

                    diff = trade['amount_fs']*price - trade['amount_ts']
                    if self.trading_fee:
                        diff -= self.trading_fee*trade['amount_ts']

                    percent_diff = 100*diff/trade['amount_ts']

                    resolving['amount_fs'] = trade['amount_fs']
                    resolving['amount_ts'] = trade['amount_fs']*price
                    resolving['trades_resolved'] = trade['id']
                    resolving['profit'] = diff
                    resolving['percent_profit'] = percent_diff
                    self._remove_unresolved(trade, from_symbol)



        resolving['percent_profit'] = np.average(resolving['percent_profit'])
        resolving['trades_resolved'] = ';'.join(resolving['trades_resolved'])
        self.currency_earned[to_symbol] += resolving['profit']

        # Update sells
        for key, value in resolving.items():
            self.trades[from_symbol]['sells'][key].append(value)

        resolving['id'] = uuid4().hex[:10]
        self._append_all_sells(resolving, from_symbol, date, price)

        # Update overall
        trades['total_invested'] -= resolving['amount_fs']
        self.purse[to_symbol] += resolving['amount_ts']

        if self.trades[from_symbol]['unresolved_trades']:
            self.unresolved_trade[from_symbol] = True
        else:
            self.unresolved_trade[from_symbol] = False


    def _append_all_buys(self, buy, from_symbol, date):
        """Append buy results to the all_buys DataFrame."""

        buy['symbol'] = from_symbol
        buy['date'] = tb.DateConvert(buy['date']).date
        self.all_buys = pd.concat([pd.DataFrame(buy, index=[0]),
                                  self.all_buys],
                                  ignore_index=True)

        if self.sqlm:
            self.sqlm.add_buy(buy)


    def _append_all_sells(self, sell, from_symbol, date, price):
        """Append sell results to the all_sells DataFrame."""

        sell['symbol'] = from_symbol
        sell['date'] = date
        sell['price'] = price

        self.all_sells = pd.concat([pd.DataFrame(sell, index=[0]),
                                   self.all_sells],
                                   ignore_index=True)

        if self.sqlm:
            self.sqlm.add_sell(sell)


    def _append_unresolved(self, buy, from_symbol, date, price):
        """Append buy results to unresolved_trades for a given symbol."""

        temp = {
            'id':buy['id'],
            'symbol':from_symbol,
            'date':date,
            'price':price,
            'amount_ts': buy['amount_ts'],
            'amount_fs': buy['amount_fs']
            }
        self.trades[from_symbol]['unresolved_trades'].append(temp)
        self.num_unresolved[from_symbol] += 1

        if self.sqlm:
            self.sqlm.add_pending(temp)


    def _remove_unresolved(self, trade, from_symbol):
        """Remove a resolved trade from a symbol's unresolved_trades list."""
        self.trades[from_symbol]['unresolved_trades'].remove(trade)
        self.num_unresolved[from_symbol] -= 1

        if self.sqlm:
            self.sqlm.remove_pending(trade)


class DataEngine:
    """Manages candles such that the 'current' candle is always at index 0."""
    def __init__(self, data):
        if data.empty:
            raise ValueError('DataEngine was given an empty dataframe')

        self.data = data
        self._check_data_continuity()
        self.length = len(self.data)
        self.increments = 0
        self.finished = False


    def _check_data_continuity(self):
        """Ensure that candles provided form a continuous timeseries."""
        dates = list(self.data.open_date.unique())
        dates.sort()

        f = lambda x : tb.DateConvert(x).datetime
        dates = list(map(f, dates))

        delta = []
        for i, _ in enumerate(dates, start=1):
            if i < len(dates):
                delta.append(dates[i]-dates[i-1])

        if len(pd.unique(delta)) > 1:
            raise DiscontinuousError(
                'There appear to be missing dates in the market data.'
            )

    def __getitem__(self, ind):
        """
        Always access the most recent candle at index 0. Previous candles
        are accessed with negative indices, future candles can be accessed with
        positive indices.
        """
        try:

            if isinstance(ind, slice):
                if ind.start is None:
                    start = self.increments
                else:
                    start = ind.start + self.increments

                if ind.stop is not None:
                    stop = ind.stop + self.increments

                ind = slice(start, stop)
            else:
                ind += self.increments

            return self.data.iloc[ind,:]

        except IndexError:
            warning('DataEngine: Index out of bounds')
            return None

    def __len__(self):
        return self.length

    def __repr__(self):
        return self.data.__repr__()

    def increment(self):
        """Increment the current candle forward in time by one candle."""
        self.increments += 1
        if self.increments == self.length:
            self.finished = True

    def reset_index(self):
        """Reset candle index to the begining."""
        self.increments = 0



class SQLManager:
    """Manages SQL database operations."""
    def __init__(self, sql_config):

        # Print info about inserts
        self.verbose = True

        # Truncate buy/sell/pending tables before insert
        self.truncate_tables = False

        # Use the test database instead of production database
        self.test = True

        if 'test' in sql_config:
            self.test = sql_config['test']
        if 'verbose' in sql_config:
            self.verbose = sql_config['verbose']
        if 'truncate_tables' in sql_config:
            self.truncate_tables = sql_config['truncate_tables']

        self.buy_table = 'buys' if not self.test else 'test_buys'
        self.sell_table = 'sells' if not self.test else 'test_sells'
        self.pending_table = 'pending'if not self.test else 'test_pending'

        self.buys = {}
        self.sells = {}
        self.fields = {}
        self.pending = {}
        self._get_fields()


    def _format_sql(self, trade, table):
        """Format dict items for insert into database.

            - Add parentheses around strings
            - Convert None values to NULL
            - Format dates to be friendly with SQL

        """

        trade = copy(trade)
        for key, value in trade.items():

            if value is None:
                trade[key] = 'NULL'
            elif key == 'date':
                value = tb.DateConvert(value).date

            if isinstance(value, str):
                trade[key] = f"'{value}'"

        return {k:v for k,v in trade.items() if k in self.fields[table]}

    def _get_fields(self):
        """Acquire column names from tables."""
        tables = [self.sell_table, self.buy_table, self.pending_table]
        for table in tables:
            sql = f'SHOW COLUMNS in {table}'
            self.fields[table] = list(tb.Database().read(sql).Field)

    def add_buy(self, trade):
        """Add buy to pending database inserts."""
        trade = self._format_sql(trade, self.buy_table)
        self.buys[trade['id']] = trade

    def add_sell(self, trade):
        """Add sell to pending database inserts."""
        trade = self._format_sql(trade, self.sell_table)
        self.sells[trade['id']] = trade

    def add_pending(self, trade):
        """Add unresolved trade to pending database inserts."""
        trade = self._format_sql(trade, self.pending_table)
        self.pending[trade['id']] = trade

    def remove_pending(self, trade):
        """Remove unresolved trade from pending database inserts."""
        trade = self._format_sql(trade, self.pending_table)
        del self.pending[trade['id']]


    def insert_trades(self):
        """Insert trades into database."""

        if self.truncate_tables:
            if self.verbose:
                a,b,c = self.buy_table, self.sell_table, self.pending_table
                print(f"Truncating tables {a}, {b}, and {c}")

            sql = f'TRUNCATE TABLE {self.buy_table};'
            tb.Database().write(sql)
            sql = f'TRUNCATE TABLE {self.pending_table};'
            tb.Database().write(sql)
            sql = f'TRUNCATE TABLE {self.sell_table};'
            tb.Database().write(sql)

        tb.Database().insert(self.buy_table, list(self.buys.values()))
        tb.Database().insert(self.sell_table, list(self.sells.values()))
        tb.Database().insert(self.pending_table, list(self.pending.values()))

        if self.verbose:
            print("Insert successful")



class Metrics:
    """Manages the calculation of summary performance metrics."""
    def __init__(self, bot, sql=False):

        self.bot = bot
        self.trade_manager = self.bot.trade_manager
        self.trades = self.trade_manager.trades

        # Value of portfolio at the start of trading
        self.initial_portfolio_value = None

        # Value of portfolio at the end of trading period
        self.final_portfolio_value = None

        # Difference between starting and ending values
        self.portfolio_value_change = None
        self.portfolio_value_change_percent = None

        # Overall change in market
        self.overall_market_change = None

        # 100*(wins/losses)
        self.percent_wins = None
        self.max_consecutive_losses = None

        self.total_trades = None
        self.trades_per_week = None

        self.portfolio_value()
        self.num_trades()
        self.market_change()
        self.win_loss_ratio()
        self.consecutive_losses()

    def avg_trade_duration(self):
        pass

    def num_trades(self):

        min_date = tb.DateConvert(self.bot.start_date).datetime
        max_date = tb.DateConvert(self.bot.end_date).datetime
        date_range = max_date-min_date
        weeks = date_range/timedelta(weeks=1)

        if not self.trade_manager.all_buys.empty:
            self.total_trades = len(self.trade_manager.all_buys['date'])
            self.trades_per_week = round(self.total_trades/weeks,1)
        else:
            self.total_trades = 0
            self.trades_per_week = 0

    def portfolio_value(self):

        currency_sums = {c:0 for c in self.trade_manager.currencies}
        currency_change = {c:0 for c in self.trade_manager.currencies}
        currency_change_percent = {c:0 for c in self.trade_manager.currencies}
        wins = 0
        losses = 0

        # Iterate through all unresolved trades, summing their currency values
        # at the most recent price
        for symbol in self.bot.symbols.symbol:
            currency = self.bot.symbols.loc[symbol].to_symbol
            unresolved = self.trades[symbol]['unresolved_trades']

            if unresolved:
                most_recent_price = self.bot.data_dict[symbol].data.close.iloc[-1]

                for trade in unresolved:
                    curr_val = trade['amount_fs']*most_recent_price
                    currency_sums[currency] += curr_val

        for currency, amount in self.trade_manager.purse.items():
            currency_sums[currency] += amount

        for currency in currency_change:
            iv = self.trade_manager.initial_purse[currency]
            fv = currency_sums[currency]
            currency_change[currency] = fv - iv
            currency_change_percent[currency] = 100*(fv - iv)/iv

        avg_currency_change = \
            round(np.average(list(currency_change_percent.values())),3)

        for c in currency_change:
            currency_sums[c] = round(currency_sums[c],3)
            currency_change[c] = round(currency_change[c],3)
            currency_change_percent[c] = round(currency_change_percent[c],3)


        self.initial_portfolio_value = self.trade_manager.initial_purse
        self.final_portfolio_value = currency_sums
        self.portfolio_value_change = currency_change
        self.portfolio_value_change_percent = currency_change_percent


    def consecutive_losses(self):
        max_consecutive_losses = 0
        running_sum = 0
        completed_trades = self.trade_manager.all_sells
        for profit in completed_trades.percent_profit:

            if profit < 0:
                running_sum += 1
                if running_sum > max_consecutive_losses:
                    max_consecutive_losses = running_sum
            else:
                running_sum = 0

        self.max_consecutive_losses = max_consecutive_losses


    def win_loss_ratio(self):
        wins = 0
        losses = 0

        for symbol in self.bot.symbols.symbol:
            currency = self.bot.symbols.loc[symbol].to_symbol
            unresolved = self.trades[symbol]['unresolved_trades']

            if unresolved:
                most_recent_price = self.bot.data_dict[symbol].data.close.iloc[-1]

                for trade in unresolved:
                    buy_price = trade['price']
                    if buy_price < most_recent_price:
                        losses += 1
                    else:
                        wins += 1

        completed_trades = self.trade_manager.all_sells
        for profit in completed_trades.percent_profit:

            if profit > 0:
                wins += 1
            else:
                losses += 1

        self.win_percent = round(100*wins/(wins+losses),2)


    def market_change(self):

        self.instrument_change = []
        for symbol in self.bot.symbols.symbol:
            temp = self.bot.data_dict[symbol].data
            ip = temp.loc[temp.open_date.min()].close # Initial price
            fp = temp.loc[temp.open_date.max()].close # Final price
            self.instrument_change.append(100*((fp-ip)/fp))

        self.overall_market_change = round(np.average(self.instrument_change),4)

    def report(self):
        print('Initial Portfolio Value: ', self.initial_portfolio_value)
        print('Final Portfolio Value: ', self.final_portfolio_value)
        print('Portfolio Value Change: ', self.portfolio_value_change)
        print('Portfolio Value Percent Change: ', self.portfolio_value_change_percent)
        print('Total Trades: ', self.total_trades)
        print('Trades Per Week: ', self.trades_per_week)
        print('Overall Market Change: ', self.overall_market_change)
        print('Max Consecutive Losses: ', self.max_consecutive_losses)
        print('Win Percent: ', self.win_percent)



class Forwardtest:
    """Simulate trading in a live environment."""
    def __init__(self):
        pass

class Livetrade:
    """Trade with real money, using exchange APIs"""
    def __init__(self):
        pass

class Backtest(Core):
    """Backtest completely offline or with results inserted into database."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

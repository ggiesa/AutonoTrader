import sys
sys.path.append('./')

import numpy as np
import bot
from bot import backtest, base
import unittest
from unittest import TestCase
import toolbox as tb
from ingestion import data_collection as dc
from importlib import reload


class JustBTC(base.Backtest):
    def __init__(self):
        super().__init__(sql_config = {'truncate_tables':True, 'test':True})

    def get_data(self):
        sql = "SELECT * FROM engineered_data where symbol = 'BTCUSDT'"
        return tb.Database().read(sql)

    def get_symbols(self):
        return {'symbol':'BTCUSDT',
                'from_symbol':'BTC',
                'to_symbol':'USDT'}

    def initialize_portfolio(self):
        purse = {'USDT':5000}
        buy_sell_amount = {'USDT':75}
        holdout = {'USDT':.1}
        return {
            'purse':purse,
            'holdout':holdout,
            'buy_sell_amount':buy_sell_amount
            }

    def generate_signals(self):
        if self.data[0].close*1.1 < self.data[0].moving_avg_48:
            self.buy()
        elif self.data[0].close > self.data[0].moving_avg_48:
            self.sell()


class JustADA(base.Backtest):

    def get_data(self):
        sql = "SELECT * FROM engineered_data WHERE symbol = 'ADABTC'"
        return tb.Database().read(sql)

    def get_symbols(self):
        return {'symbol':'ADABTC',
                'from_symbol':'ADA',
                'to_symbol':'BTC'}

    def initialize_portfolio(self):
        purse = {'BTC':5}
        buy_sell_amount = {'BTC':.05}
        holdout = {'BTC':.1}
        return {
            'purse':purse,
            'holdout':holdout,
            'buy_sell_amount':buy_sell_amount
            }

    def generate_signals(self):
        if self.data[0].close*1.1 < self.data[0].moving_avg_48:
            self.buy()
        elif self.data[0].close > self.data[0].moving_avg_48:
            self.sell()


class BuySell(base.Backtest):

    def get_data(self):
        sql = "SELECT * FROM engineered_data ORDER BY open_date DESC LIMIT 1000"
        return tb.Database().read(sql)

    def get_symbols(self):
        return dc.get_symbols_and_pairs(as_df=True)

    def initialize_portfolio(self):
        purse = {'USDT':1000, 'BTC':1.5}
        buy_sell_amount = {'BTC':.05, 'USDT':75}
        holdout = {'USDT':.2, 'BTC':.2}
        slippage = .05
        trading_fee = .001
        return {
            'purse':purse,
            'holdout':holdout,
            'buy_sell_amount':buy_sell_amount,
            'slippage':slippage,
            'trading_fee':trading_fee
            }

    def generate_signals(self):
        self.buy()
        self.sell()


class TestBot10(base.Backtest):
    def __init__(self):
        super().__init__(sql_config = {'test':True, 'truncate_tables':True})

    def get_data(self):
        if self.verbose:
            print('Acquiring data')
        sql = "SELECT * FROM engineered_data order by open_date desc limit 10000;"
        # sql = "SELECT * FROM engineered_data;"
        return tb.Database().read(sql)

    def get_symbols(self):
        return dc.get_symbols_and_pairs(as_df=True)

    def initialize_portfolio(self):
        purse = {'USDT':1000, 'BTC':1.5}
        holdout = {'USDT':200, 'BTC':.2}
        buy_sell_amount = {'BTC':.05, 'USDT':75}
        slippage = .05
        trading_fee = .001

        return {
            'purse':purse,
            'holdout':holdout,
            'buy_sell_amount':buy_sell_amount,
            'slippage':slippage,
            'trading_fee':trading_fee
            }

    def generate_signals(self):
        if self.data[0].close*1.1 < self.data[0].moving_avg_48 \
        and self.data[0].close < self.data[0].last_base \
        and self.num_unresolved <= 5:
            self.buy()
        elif self.data[0].close > self.data[0].moving_avg_48:
            self.sell_all()


def test_duplicate_trades(bot):

    # Test that there are no duplicate trades
    ids = set()
    for symbol in bot.symbols.symbol:
        for res in bot.trade_manager.trades[symbol]['sells']['trades_resolved']:
            x = res.split(';')
            for id in x:
                if id in ids:
                    raise ValueError(f'Duplicate trades detected in {symbol}')
                else:
                    ids.add(id)

    num_unresolved = 0
    for symbol in bot.symbols.symbol:
        num_unresolved += bot.trade_manager.num_unresolved[symbol]

    total_buys = len(bot.trade_manager.all_buys)

    # Test num_unresolved state variable
    assert total_buys-len(ids) == num_unresolved, '''
        Number of unresolved does not agree with number of unique trades
    '''

def test_number_of_trades(bot):
    all_unresolved = []
    for symbol in bot.symbols.symbol:
        for unresolved in bot.trade_manager.trades[symbol]['unresolved_trades']:
            all_unresolved.append(unresolved)

    all_resolved = []
    for resolved in bot.trade_manager.all_sells.trades_resolved:
        for trade in resolved.split(';'):
            all_resolved.append(trade)

    total_unresolved = len(all_unresolved)
    total_resolved = len(all_resolved)
    total_buys = len(bot.trade_manager.all_buys)
    assert total_resolved + total_unresolved == total_buys


class BuySell(base.Backtest):

    def get_data(self):
        sql = "SELECT * FROM engineered_data ORDER BY open_date DESC LIMIT 1000"
        return tb.Database().read(sql)

    def get_symbols(self):
        return dc.get_symbols_and_pairs(as_df=True)

    def initialize_portfolio(self):
        purse = {'USDT':1000, 'BTC':1.5}
        buy_sell_amount = {'BTC':.05, 'USDT':75}
        holdout = {'USDT':.2, 'BTC':.2}
        slippage = .05
        trading_fee = .001
        return {
            'purse':purse,
            'holdout':holdout,
            'buy_sell_amount':buy_sell_amount,
            'slippage':slippage,
            'trading_fee':trading_fee
            }

    def generate_signals(self):
        self.buy()
        self.sell()


class TestBacktest(unittest.TestCase):
    def setUp(self):
        self.tb1 = JustADA() # Symbol 1
        self.tb2 = JustBTC() # Symbol 2
        self.tb3 = BuySell() # Multiple symbols, buy/sell simultaneously

    def tearDown(self):
        del self.tb1
        del self.tb2
        del self.tb3

    def test_run(self):
        self.tb1.run()
        self.tb2.run()
        self.tb3.run()

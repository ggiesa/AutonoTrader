from bot import Backtest
from utils.database import Database


class ADABot(Backtest):
    """An example of a complete implementation of the Backtest class."""

    def get_data(self):
        """User returns hourly candles for the backtest to iterate through."""
        sql = "SELECT * FROM engineered_data WHERE symbol = 'ADABTC'"
        return tb.Database().read(sql)


    def get_symbols(self):
        """User returns the symbol metadata for the test."""
        return {'symbol':'ADABTC', 'from_symbol':'ADA', 'to_symbol':'BTC'}


    def initialize_portfolio(self):
        # Amount of currency to start the backtest with
        purse = {'BTC':5}

        # Default amount to buy when a buy signal is generated
        buy_sell_amount = {'BTC':.05}

        # Minimum amount to reserve in BTC at all times
        holdout = {'BTC':.1}

        # Simulates trade slippage of 5%
        slippage = .05

        # Factors in the Binance trading fee
        trading_fee = .001

        return {
            'purse':purse,
            'holdout':holdout,
            'buy_sell_amount':buy_sell_amount
            'slippage':slippage,
            'trading_fee':trading_fee
            }

    def generate_signals(self):
        """A mock trading strategy."""

        if self.data[0].close < self.data[0].moving_avg_48:
            self.buy()
        elif self.data[0].close > self.data[0].moving_avg_48:
            self.sell()


# Initializes by collecting data from the database
bot = ADABot()

# Iterates through candles, displaying progress with the progress bar
bot.run()

# Prints a performance summary
bot.report()

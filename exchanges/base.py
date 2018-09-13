"""Base classes for exchange data and order endpoints"""


class ExchangeData:
    def ticker(self, symbol):
        """Return the current market price for a given symbol."""
        raise NotImplementedError('Must implement ticker method.')

    def candle(self, limit, from_date=None, to_date=None):
        """Return candles for a given symbol."""
        raise NotImplementedError('Must implement candle method.')

    def symbols(self):
        """Return a list of all symbols traded on the exchange."""
        raise NotImplementedError('Must implement symbols method.')


class ExchangeOrders:
    def buy_order(self, symbol, quantity, test=True):
        """Place a buy order on a given exchange."""
        raise NotImplementedError('Must implement buy_order method.')

    def sell_order(self, symbol, quantity, test=True):
        """Place a sell order on a given exchange."""
        raise NotImplementedError('Must implement sell_order method.')

    def account_balance(self):
        """Return the total account balanace for an exchange."""
        raise NotImplementedError('Must implement account_balance method.')

    def coin_balance(self, symbol):
        """Return the balance for a given asset."""
        raise NotImplementedError('Must implement coin_balance method.')

    def all_orders(self):
        """Return a record of all orders for all coins."""
        raise NotImplementedError('Must implement all_orders method.')

    def trades(self, symbol):
        """Return a record of all orders for a given coin."""
        raise NotImplementedError('Must implement trades method.')

    def account_status(self):
        """Return the status of the user account."""
        raise NotImplementedError('Must implement account_status method.')

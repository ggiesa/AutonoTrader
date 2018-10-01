"""Add custom data sources that will be schedule for automatic collection."""

import pandas as pd
import numpy as np
from datetime import timedelta
from utils import toolbox as tb
from utils.database import Database
from utils import database as db
from errors.exceptions import ImplementationError

class CustomData:
    """
    Base class for custom data source implementation.
    """

    def __init__(self):
        self.table_name = self.set_table_name()
        self.automation_frequency = self.set_automation_frequency()

    def set_automation_frequency(self):
        """
        Return a timedelta object denoting the frequency to collect data for the
        custom datasource. For example, if you want the custom datasource process
        to be run every hours, return timedelta(hours=1)
        """
        raise NotImplementedError("""
            Must implement set_automation_frequency method. See docs for details.
        """)

    def set_table_name(self):
        """
        Return a string with the name of the MySQL table to be inserted into.
        If a table doesn't exist with the name returned, AutonoTrader will
        create one.
        """
        raise NotImplementedError("""
            Must implement set_table_name method. See docs for details.
        """)

    def get_data(self):
        """
        Return a pandas dataframe or series with the custom data to be inserted.
        Columns should
        """
        raise NotImplementedError("""
            Must implement get_data method. See docs for details.
        """)

    def _get_data(self):
        pass



class BaseData(CustomData):

    def set_automation_frequency(self):
        return timedelta(hours=1)

    def set_table_name(self):
        return 'bases'

    def get_data(self, debug=False):

        pairs = db.get_pairs()
        ret = pd.DataFrame()
        convert_dates = lambda x: tb.DateConvert(x).datetime

        for pair in pairs.iterrows():

            from_symbol = pair[1].from_symbol
            to_symbol = pair[1].to_symbol

            # URL parameters
            exchange = 'binance'

            # Get data from Hodloo API
            url = 'https://qft.hodloo.com/api/{}/points/60/{}-{}'.format(
                        exchange, from_symbol.lower(), to_symbol.lower()
                    )
            df = pd.DataFrame.from_records(tb.decode_api_response(url)[1])
            df = df.rename(columns={'o':'type', 't':'date', 'val':'price'})

            # Convert price and date to proper format, add symbol column
            df.price = df.price/1e8
            df.date = pd.to_datetime(df.date, unit='s')
            df['symbol'] = from_symbol + to_symbol
            df = df[['date', 'symbol', 'type', 'price']]
            df.date = df.date.map(convert_dates)

            ret = pd.concat([ret, df], ignore_index=True)

            # TODO Check for existing data in table, truncate if so
            if debug:
                return ret
        return ret

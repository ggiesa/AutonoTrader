"""Add custom indicators that will be calculated each time a candle is acquired."""

import pandas as pd
import numpy as np
from datetime import timedelta
from utils import toolbox as tb
from utils.database import Database
from errors.exceptions import ImplementationError


class CustomIndicator:
    """
    Base class for custom indicators.

    initialize with self.timedelta

    """

    @staticmethod
    def get_timedelta():
        """
        Return a timedelta object representing the minimum number of rows needed
        to calculate the indicator.

        For example, if the custom indicator is a 24-hour moving average, return
        timedelta(hours=24). Return None if rows are irrelevent.
        """
        raise NotImplementedError("""
            Must implement get_timedelta method. See docs for details.
        """)

    def transform(self):
        """
        Return a pandas dataframe or series with the custom indicator. self.data
        gives access to market data for calculations. Series or dataframe
        returned should be the same length as self.data.
        """
        raise NotImplementedError("""
            Must implement transform method. See docs for details.
        """)

    def _transform(self, candles):
        self.candles = candles
        custom_indicator = self.transform()

        if len(self.candles) != len(custom_indicator):
            raise ImplementationError(f"""
                Length of input to transform method is different from length of
                output. They should be the same.

                len(input): {len(self.candles)}
                len(output): {len(custom_indicator)}
            """)
        return custom_indicator


class MA_48H(CustomIndicator):
    def get_timedelta():
        return timedelta(hours=48)

    def transform(self):
        return self.candles.close.rolling(48).mean()

class MA_72H(CustomIndicator):
    def get_timedelta():
        return timedelta(hours=72)

    def transform(self):
        return self.candles.close.rolling(72).mean()

class AVG(CustomIndicator):
    def get_timedelta():
        return None

    def transform(self):
        a = self.candles.close + self.candles.open
        b = self.candles.high + self.candles.low
        return a+b/4

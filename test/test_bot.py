from unittest import TestCase
from bot.base import DataEngine
from datetime import datetime, timedelta

class TestDataEngine(TestCase):

    def setUp(self):
        date = tb.DateConvert(datetime.utcnow()-timedelta(weeks=1)).date
        self.data = tb.Candles().get_engineered(from_date = date,
                                                symbol = 'BTCUSDT')

    def test_indexing(self):
        e = DataEngine(self.data)

        self.assertTrue((e[0] == self.data.iloc[0]).all())
        e.increment()
        self.assertTrue((e[0] == self.data.iloc[1]).all())
        e.increment()
        self.assertTrue((e[0] == self.data.iloc[2]).all())

        e.reset_index()
        self.assertTrue((e[0] == e.data.iloc[0]).all())
        self.assertTrue((e[:10] == e.data.iloc[:10]).all().all())

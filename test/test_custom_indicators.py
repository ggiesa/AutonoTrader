
import pandas as pd
from ingestion.custom_indicators import CustomIndicator
from utils.database import Candles, get_symbols, get_most_recent_dates
from datetime import datetime, timedelta



def indicators():
    for indicator in CustomIndicator.__subclasses__():
        yield indicator

# Get timedelta for data acquisition from DB
td = []
for indicator in indicators():
    delta = indicator.get_timedelta()
    if delta:
        td.append(delta)


# TODO utcnow doesnt work if there aren't recent candles in the db
td = max(td)
from_date = datetime.utcnow()-td
from_date -= timedelta(hours=10)

candles = Candles().get_raw(from_date = from_date)
candles.index = candles.symbol
symbols = get_symbols()

Calculate indicators
ins = pd.DataFrame()
for indicator in indicators():
    for symbol in symbols:
        subset_candles = candles.loc[symbol]
        subset_candles = indicator()._transform(subset_candles)
        ins = pd.concat([ins, subset_candles], axis=1)

indicator()._transform(candles.loc[symbol])

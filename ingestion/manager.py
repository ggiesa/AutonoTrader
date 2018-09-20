"""Data ingestion tasks for automation."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import OrderedDict
import logging

from utils import toolbox as tb
from utils.database import get_symbols, Database
from exchanges.binance import BinanceData
from ingestion import data_collection as dc
from ingestion.live import get_all_candles,
from bot import backtest, base

# Set up logging
logging.basicConfig(
    filename = './data/logs/tasks.log',
    level = logging.WARNING,
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
    )
logger = logging.getLogger(__name__)


class Tasks:

    def __init__(self):
        # self.pairs = dc.get_pairs()
        # self.symbols = [row[1].from_symbol+row[1].to_symbol \
        #                 for row in self.pairs.iterrows()]
        self.symbols = get_symbols()

    # TODO Get working with new system
    def insert_ticker(self, verbose=False):
        try:
            if verbose:
                print(f'Attempting bulk ticker insert')
            dc.get_all_tickers(insert=True)

        except Exception as err:
            logger.error('insert_ticker failed')
            logger.error(err)

    def insert_candle(self, verbose=False):
        try:
            if verbose:
                print('Attempting bulk candle insert')

            # Get date of last complete candle
            # date = datetime.utcnow() - timedelta(hours=1)
            # date = tb.DateConvert(date).timestamp*1000
            # dc.get_all_candles(insert=True, endTime=date)


        except Exception as err:
            logger.error(f'Insert_candle failed')
            logger.error(err)


    def insert_engineered_features(self, verbose=False):
        try:
            if verbose:
                print(f'Inserting engineered data.')
            dc.insert_all_engineered_features(verbose=verbose)
        except Exception as err:
            logger.error('Insert_engineered_features failed.')
            logger.error(err)


    # TODO Setup metrics SQL table
    def run_backtest(self, verbose=False, test=False,
                           metrics=False, ret=False,
                           sql=False, truncate_tables=False):
        try:
            if verbose:
                print('Starting backtest')

            bot = backtest.TestBot()
            bot.run()

            if metrics:
                if verbose:
                    print('Calculating metrics')
                # metrics = base.Metrics(bt)

            if verbose:
                print('Done')

            if ret:
                return (bt, metrics)

        except Exception as err:
            logger.error('Backtest process failed.')
            logger.error(err)


    def repair_data(self, verbose = False):
        try:
            if verbose:
                print('Running repair_data process')
            dc.repair_data()
        except Exception as err:
            logger.error('Backtest process failed.')
            logger.error(err)

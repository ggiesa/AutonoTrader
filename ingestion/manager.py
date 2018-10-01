"""Data ingestion tasks for automation."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from utils import toolbox as tb
from utils.database import get_symbols, Database
from ingestion import live, core
from bot import base

# Set up logging
logging.basicConfig(
    filename = './data/logs/tasks.log',
    level = logging.WARNING,
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
    )
logger = logging.getLogger(__name__)


class Tasks:

    def __init__(self):
        self.symbols = get_symbols()

    # TODO Get working with new file structure
    def insert_ticker(self, verbose=False):
        try:
            if verbose:
                print(f'Attempting bulk ticker insert')
            dc.get_all_tickers(insert=True)

        except Exception as err:
            logger.error('insert_ticker failed')
            logger.error(err)

    def insert_candles(self, verbose=False):
        try:
            if verbose:
                print('Attempting bulk candle insert')
            live.update_candles()
        except Exception as err:
            logger.error(f'Insert_candle failed')
            logger.error(err)


    def insert_engineered_features(self, verbose=False):
        try:
            if verbose:
                print(f'Inserting engineered data.')
            live.insert_engineered_data(verbose=verbose)
        except Exception as err:
            logger.error('Insert_engineered_features failed.')
            logger.error(err)


    def insert_custom_data(self, verbose=False):
        try:
            if verbose:
                print('Inserting custom data sources.')
            live.insert_custom_data(verbose=verbose)
        except Exception as err:
            logger.error('insert_custom_data failed.')
            logger.error(err)


    # TODO Setup metrics SQL table
    # TODO Get working with current file structure
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
            core.repair_data(verbose=True)
        except Exception as err:
            logger.error('Backtest process failed.')
            logger.error(err)

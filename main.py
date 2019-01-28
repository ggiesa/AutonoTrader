from sys import argv
import logging
from ingestion.manager import Tasks

# Set up logging
logging.basicConfig(filename = './data/logs/main.log',
                    level = logging.WARNING,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    args = argv
    verbose = 'verbose' in args

    # TODO not implemented
    if 'insert_ticker' in args:
        Tasks().insert_ticker(verbose=verbose)
        logger.info('Sucessfully inserted ticker data')

    if 'insert_candle' in args:
        Tasks().insert_candles(verbose=verbose)
        logger.info('Sucessfully inserted candle data')

    if 'insert_engineered_features' in args:
        Tasks().insert_engineered_features(verbose=verbose)
        logger.info('Sucessfully inserted engineered data')

    # TODO Not working with new file structure
    if 'run_backtest' in args:
        Tasks().run_backtest(verbose=verbose, sql=True, truncate_tables=True)
        logger.info('Sucessfully ran backtest and generated summary')

    if 'repair_data' in args:
        Tasks().repair_data(verbose=verbose)
        logger.info('Sucessfully ran repair process')


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        logger.error(err)

"""Configuration for data collection tasks."""


"""
Configuration for historical data collection.

Parameters:
-------------
historical_collection_period: string
    Historical time period to collect data for.
        Options:
            nY, nM, nD, where Y, M, D corresponds to
            years, months, days, and n is the number of years, months, etc.
        Example:
            historical_collection_period = '2Y' --> collect historical data
            from the most recent candle to two years prior.

"""
historical_config = dict(
    historical_collection_period = '1Y'
)


"""
Symbols to collect data for. Currently supported: Binance.
       Format:  <exchange> : [<from_symbol>/<to_symbol>]
       Example: 'binance' : ['BTC/USDT', 'ETH/BTC']
"""
user_symbols = {

    'binance' : [
         'ETH/BTC',
         'EOS/BTC',
         'ADA/BTC',
         'BCC/BTC',
         'NANO/BTC',
         'WAVES/BTC',
    ]
}

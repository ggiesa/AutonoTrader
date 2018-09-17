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
    historical_collection_period = '3M'
)


"""
Symbols to collect data for. Currently supported: Binance.
       Format:  <exchange> : [<from_symbol>/<to_symbol>]
       Example: 'binance' : ['BTC/USDT', 'ETH/BTC']
"""
user_symbols = {
    # Symbols to collect data for. Currently supported: Binance.
    #       Format:  <exchange> : [<from_symbol>/<to_symbol>]
    'binance' : [
         'EOS/BTC',
         'ONT/BTC',
         'BCD/BTC',
         'XLM/BTC',
         'ETC/BTC',
         'ADA/BTC',
         'BCC/BTC',
         'NANO/BTC',
         'WAVES/BTC',
         'NEO/BTC',
         'LTC/BTC',
         'VET/BTC',
         'TRX/BTC',
         'DASH/BTC',
         'ICX/BTC',
         'BNB/BTC',
         'HOT/BTC',
         'XMR/BTC',
         'TRIG/BTC',
         'ZRX/BTC',
         'TUSD/BTC',
         'MTL/BTC',
         'QKC/BTC',
         'NPXS/BTC',
         'WAN/BTC',
         'CVC/BTC',
         'DOCK/BTC',
         'GVT/BTC',
         'RCN/BTC',
         'BAT/BTC',
         'ENG/BTC',
         'GTO/BTC',
         'NULS/BTC',
         'QTUM/BTC',
         'OMG/BTC',
         'ZIL/BTC',
         'MCO/BTC',
         'LSK/BTC',
         'ZEC/BTC',
         'BQX/BTC',
         'ELF/BTC',
         'ADX/BTC',
         'VIBE/BTC',
         'XEM/BTC',
         'KEY/BTC',
         'DNT/BTC',
         'MDA/BTC',
         'NAS/BTC',
         'ARN/BTC',
         'GAS/BTC',
         'VIB/BTC',
         'STRAT/BTC',
         'NCASH/BTC',
         'OST/BTC',
         'MFT/BTC',
         'REP/BTC',
         'DENT/BTC',
         'NXS/BTC',
         'CHAT/BTC',
         'BTG/BTC',
         'BCN/BTC',
         'STORM/BTC',
         'CLOAK/BTC',
         'LINK/BTC',
         'BRD/BTC',
         'ENJ/BTC',
         'IOST/BTC',
         'SUB/BTC',
         'MANA/BTC',
         'SC/BTC',
         'BCPT/BTC',
         'LOOM/BTC',
         'RDN/BTC',
         'LUN/BTC',
         'PPT/BTC',
         'PIVX/BTC',
         'MTH/BTC',
         'TNT/BTC',
         'ZEN/BTC',
         'XZC/BTC',
         'THETA/BTC',
         'EVX/BTC',
         'WINGS/BTC',
         'BTS/BTC',
         'WABI/BTC',
         'POWR/BTC',
         'GNT/BTC',
         'NEBL/BTC',
         'AION/BTC',
         'KNC/BTC'
    ]
}

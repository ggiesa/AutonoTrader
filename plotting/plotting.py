"""Offline plotting functionality"""

# Basics
import pandas as pd
import numpy as np
from datetime import timedelta
from pathlib import Path

# Plotting
import plotly
from plotly import tools
import plotly.plotly as py
import plotly.graph_objs as go



def plot_candles(data, symbol,

                 exchange = None,       show_moving_avg = True,
                 show_volume = True,    show_support = False,
                 buy_data = None,       sell_data = None,
                 filename = None,       auto_open = False,
                 filename_tag = None,   custom_data = None,
                 show_vol_avg = True

                 ):

    if symbol[-4:] == 'USDT':
        to_coin = 'USDT'
        from_coin = symbol[:-4]
    else:
        to_coin = 'BTC'
        from_coin = symbol[:-3]

    exchange = 'Binance' if not exchange else exchange

    def parse_data(data):
        # Seperate bases and market data for individual plotting

        temp = data['last_base']
        market_data = data.drop('last_base', axis=1)

        base_data = {'date':[], 'price':[]}
        for i in range(len(temp)-1):
            if temp.iloc[i] != temp.iloc[i+1]:
                base_data['date'].append(temp.index[i+1])
                base_data['price'].append(temp.iloc[i+1])

        base_data = pd.DataFrame(base_data)
        base_data.index = pd.to_datetime(base_data.date)
        base_data = base_data.drop('date', axis=1)
        return base_data, market_data


    base_data, market_data = parse_data(data)

    INCREASING_COLOR = 'rgb(37, 163, 107)' # Greenish
    DECREASING_COLOR = 'rgb(155, 64, 48)' # Red-orangeish

    # Plot market data
    price = go.Candlestick(
        x = list(market_data.index),
        open = market_data.open,
        high = market_data.high,
        close = market_data.close,
        low = market_data.low,
        name = from_coin,
        line = {'width':1},
        yaxis = 'y1',
        increasing = {'line':{'color':INCREASING_COLOR}},
        decreasing = {'line':{'color':DECREASING_COLOR}}
    )

    # Plot bases
    base = go.Scatter(
        x = base_data.index,
        y = base_data.price,
        mode = 'markers',
        marker = {'color':'yellow'},
        name = 'Bases',
        yaxis = 'y1'
    )

    # Plot buy signals
    if buy_data:
        buy_signals = go.Scatter(
            x = buy_data['date'],
            y = buy_data['price'],
            mode = 'markers',
            marker = {'color':'blue', 'symbol': 'triangle-up', 'size':12},
            name = 'Buy signals',
            yaxis = 'y1'
        )

    # Plot sell signals, with percent profit annotations if available
    if sell_data:
        sell_signals = go.Scatter(
            x = sell_data['date'],
            y = sell_data['price'],
            mode = 'markers',
            marker = {'color':'red', 'symbol':'triangle-down', 'size':12},
            name = 'Sell signals',
            yaxis = 'y1'
        )

        # Take the average of profits for each sell
        if 'percent_profit' in sell_data:
            text = []
            for i, prof in enumerate(sell_data['percent_profit']):
                t = np.round(np.mean(sell_data['percent_profit'][i]),decimals=2)
                t = '+' + str(t) if t > 0 else str(t)
                text.append(t+'%')

            sell_signals['mode'] = 'markers+text'
            sell_signals['textfont'] = {'size':12, 'color':'white'}
            sell_signals['textposition'] = 'top'
            sell_signals['text'] = text

    # Plot 48-hour min line
    if show_support:
        support = go.Scatter(
                  x = market_data.index,
                  y = market_data.low.rolling(48).min(),
                 line = {'dash':'solid',
                         'color':'rgb(91, 44, 44)'},
                  name = '48-hr Minimum',
                  yaxis = 'y1'
                )

    # Plot volume
    if show_volume:
        colors = []
        for row in market_data.iterrows():
            if row[1].open >= row[1].close:
                colors.append(DECREASING_COLOR)
            else:
                colors.append(INCREASING_COLOR)
        vol = market_data.volume

        volume = go.Bar(
            x = list(market_data.index),
            y = (vol - vol.mean())/(vol.max()-vol.min()),
            name = 'Normalized Volume',
            yaxis = 'y2',
            marker = {'color':colors},
        )

        # Plot 48-hour moving avg of volume
        if show_vol_avg:
            vol_std = market_data.volume.rolling(48).mean()
            volume_std = go.Scatter(
                x = list(market_data.index),
                y = (vol_std - vol_std.mean())/(vol_std.max()-vol_std.min()),
                name = 'Normalized 48-hour Volume Moving Average',
                yaxis = 'y2',
                marker = {'color':'#7095d1'},
            )

    # Plot 48-hour moving avg
    if show_moving_avg:
        rolling_avg_48 = go.Scatter(
            name = '48-hour Moving Average',
            x = market_data.index,
            y = market_data.close.rolling(48).mean(),
            hoverinfo = 'skip',
            line = {'dash':'dashdot',
                    'color':'#7095d1'},
            yaxis = 'y1'
        )


    # Create horizontal lines for bases and/or ceilings
    lines = []
    for row in base_data.iterrows():
        lines.append(
            {
            'type': 'line',
            'x0': row[0] - timedelta(hours=40),
            'y0': row[1].price,
            'x1': row[0] + timedelta(hours=100),
            'y1': row[1].price,
            'line': {
                'color': 'yellow',
                'width': 1.5,
                },
            }
        )

    layout = go.Layout(

        title = f'{symbol} | {exchange}',

        xaxis = dict(
            tickmode = 'auto',
            nticks = 20,
            rangeslider = {'visible':False},
            gridcolor = '#707070',
            showgrid = False,
            showspikes = True,
            spikecolor = 'white',
            spikethickness = '1',
            spikedash = 'solid',
            spikemode = 'across',
            spikesnap = 'cursor'
            ),

        yaxis = dict(
            tickmode = 'auto',
            nticks = 20,
            tickwidth = .5,
            title = to_coin,
            gridcolor = '#828282',
            showgrid = False
            ),

        hovermode = 'closest',
        shapes = lines,
        plot_bgcolor = 'rgb(40, 43, 50)',
        paper_bgcolor = 'rgb(40, 43, 50)',
        font = {'color':'rgb(93, 101, 119)'}

    )

    # Initialize plot
    data = [price, base]

    if show_moving_avg:
        data.append(rolling_avg_48)
    if show_support:
        data.append(support)
    if buy_data:
        data.append(buy_signals)
    if sell_data:
        data.append(sell_signals)
    if show_volume:
        # Normalize so we can easily put other stuff on the same axis
        data.append(volume)
        if show_vol_avg:
            data.append(volume_std)
        layout['yaxis']['domain'] = [0.3, 1]
        layout['yaxis2'] = dict(domain = [0, 0.3], showticklabels = False,
                                gridcolor = '#9e9e9e')
                                # showgrid = False)

    # Plot custom data d
    if custom_data:
        if not isinstance(custom_data, list):
            custom_data = [custom_data]
        for trace in custom_data:
            data.append(trace)

    fig = go.Figure(data = data, layout = layout)
    if not filename:
        filename = f'./data/plots/{symbol}_{exchange}'
    if filename_tag:
        filename += filename_tag
    if filename[-5:] != '.html':
        filename += '.html'

    plotly.offline.plot(fig, auto_open=auto_open, filename=filename)



def plot_all(path):
    '''Generate plots for all data files in data/live'''

    files = Path(path)
    files = [str(f).replace('\\', '/') for f in files.iterdir()]
    files.sort()

    for i in range(len(files)):

        b = files[i].split('/')[-1].split('_')
        from_coin = b[0]
        to_coin = b[1]
        exchange = b[2].split('.')[0]

        data = pd.read_csv(files[i], index_col='time')
        print(f'Plotting {from_coin}-{to_coin}-{exchange}')

        plot_candles(data = data,
                     from_coin = from_coin, to_coin = to_coin,
                     exchange = exchange, show_volume = True,
                     show_moving_avg = True, show_support = True)

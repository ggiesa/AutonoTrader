'''Miscellaneous utility functions to be used project-wide.'''

# Basics
import pandas as pd
import numpy as np
import json
import urllib.request as request
import requests
import ndjson
import pymysql
from datetime import datetime, timedelta
import time
import sys

# Binance API
from binance.client import Client
from binance.enums import *

# Custom
import config.config as config


def decode_api_response(url):
    '''Return JSON from url response.'''

    response = requests.get(url)
    status = response.status_code

    try:
        response = response.json()
    except:
        try:
            response = ndjson.loads(response.text)
        except:
            print("Bad response")
            response = None

    return (status, response)


class DateConvert:
    '''Handle conversions between different date formats. All dates should be
        in UTC.'''
    def __init__(self, date):

        if isinstance(date, int) or isinstance(date, np.int64):
            self.timestamp = date

            digits = len(str(self.timestamp))
            if digits == 10:
                option = 's'
            elif digits == 13:
                option = 'ms'
            else:
                raise TypeError('Unrecognized timestamp format')

            self.datetime = pd.to_datetime(self.timestamp,unit=option,utc=True)

            # Constrain datetime to second resolution
            if self.datetime.microsecond:
                self.datetime = self.datetime.floor('S')
                self.datetime = self.datetime.to_pydatetime()
            self.date = self.datetime.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(date, str):
            if 'T' and 'Z' in date:
                self.date = date.replace('T', ' ')
                self.date = self.date.replace('Z', '')
            else:
                self.date = date
            self.datetime = datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S')
            self.timestamp = int(pd.Timestamp(self.datetime).timestamp())

        elif isinstance(date, pd.Timestamp):
            self.datetime = date.to_pydatetime()
            self.date = self.datetime.strftime('%Y-%m-%d %H:%M:%S')
            self.timestamp =  int(pd.Timestamp(self.datetime).timestamp())

        elif isinstance(date, datetime):
            self.datetime = date
            self.date = self.datetime.strftime('%Y-%m-%d %H:%M:%S')
            self.timestamp = int(pd.Timestamp(self.datetime).timestamp())

        elif isinstance(date, np.datetime64):
            t = (date - np.datetime64('1970-01-01 00:00:00'))
            self.timestamp = int(t / np.timedelta64(1, 's'))
            self.datetime = datetime.utcfromtimestamp(self.timestamp)
            self.date = self.datetime.strftime('%Y-%m-%d %H:%M:%S')

        else:
            raise TypeError(f'''
                {type(date)} is not in a format supported for conversion
            ''')


def progress_bar(count, total, status):
    """
    Print refreshing progress bar to console.
    From https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
    """

    bar_len = 50
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))

    if count == total:
        print()
    else:
        sys.stdout.flush()

def format_records(records, exclude=[]):
    """
    Edit a records object (list of dicts) into something friendly for SQL-insert
    by converting numeric-like types into numbers and enclosing strings
    in additional apostophes. Examples:

        "12" --> 12
        "red" --> "'red'"

    Parameters:
    ----------------
    exclude: list of strings
        Strings correspond to dict keys that should be excluded from alteration.
    """
    if isinstance(records, dict):
        records = [records]

    ret=[]
    for dictionary in records:
        for key in dictionary:
            if key not in exclude:
                try:
                    dictionary[key] = pd.to_numeric(dictionary[key])
                except:
                    if isinstance(dictionary[key], str):
                        dictionary[key] = f"'{dictionary[key]}'"
        ret.append(dictionary)
    return ret


def chunker(array, chunk_size):
    for i in range(0, len(array), chunk_size):
        yield array[i:i+chunk_size]


def parse_datestring(collection_period):
    """
    Parse user provided historical_collection_period string into timedelta obj.

    Parameters:
    -----------
    collection_period: string
        Example: '1Y', '2Y'

    Returns:
    -----------
    diff: datetime.timedelta
        Example: '1Y' ---> datetime.timedelta(days = 365)
        Example: '1Y' ---> datetime.timedelta(days = 365)
    """

    collection_period = collection_period.upper()
    num = ''
    char = ''
    for c in collection_period:
        if c.isdigit():
            num+=c
        else:
            char+=c

    try:
        num = int(num)
    except ValueError as e:
        raise ImplementationError('''
            Must include an integer in input string. Check
            config/data_collection.historical_config
        ''')

    if char == 'Y':
        return timedelta(days=num*365)
    elif char == 'M':
        return timedelta(days=num*30)
    elif char == 'D':
        return timedelta(days=num)
    else:
        raise ImplementationError('''
            Must designate month/day/year as M/D/Y. Check
            config/data_collection.historical_config
        ''')

import yfinance as yf
import json
import os
import pandas as pd
from datetime import datetime

from utilities import Utils
from QuordataSqlManager import QuordataSqlManager


def filter_symbols(df):

    # Create a dictionary to define the priority order
    priority_order = {'Q': 1, 'N': 2, 'B': 3}

    # Create a new column 'Priority' based on the Market column
    df['Priority'] = df['Market'].map(priority_order).fillna(4)

    # Sort the DataFrame by Symbol and Priority in ascending order
    df = df.sort_values(by=['Symbol', 'Priority'])

    # Drop duplicates in the Symbol column, keeping the first occurrence based on priority
    df = df.drop_duplicates(subset='Symbol', keep='first')

    # Remove the 'Priority' column if no longer needed
    df = df.drop(columns='Priority')

    # Drop rows where 'Symbol' contains a forward slash
    df = df[~df['Symbol'].str.contains('/')]

    # Drop rows where 'Symbol' contains a lowercase p
    df = df[~df['Symbol'].str.contains('p')]

    # Drop rows where 'Symbol' contains a lowercase r
    df = df[~df['Symbol'].str.contains('r')]

    return df


def write_yf_json(jsonfilename, ticker_info):
    with open(jsonfilename, 'w') as jsonfile:
        json.dump(ticker_info, jsonfile, indent=4)


def load_yf_json(jsonfilename):
    with open(jsonfilename, 'r') as jsonfile:
        data = json.load(jsonfile)
    return data


def get_yfinance_ticker_info(tickers):

    ts = yf.Tickers(tickers)

    ticker_data = {}

    for symbol in ts.symbols:

        print(f'Collecting data on symbol {symbol}')
        try:
            ticker = ts.tickers[symbol]
            ticker_info = ticker.info
            if ticker_info['quoteType'] != 'EQUITY':
                continue

            ticker_data[symbol] = ticker_info
        except Exception as e:
            print(f'Error {e} on key {symbol}')
            continue

    return ticker_data


def get_sp_russell_list():

    sp = pd.read_csv('../data/MarketData/sp_500.csv')
    russell = pd.read_csv('../data/MarketData/russell_2000.csv')

    return sp['Symbol'].to_list() + russell['Ticker'].to_list()


def get_historical_tickers_data(tickers, start_date, end_date):
    data = yf.download(tickers, start=start_date, end=end_date)
    return data


def get_sp_russell_historical_data(start_date, end_date=None):
    """Gets historical stock data for a ticker given a range of dates. Data includes open, high, low, close, and
    volume for a ticker on a specific day. Calls one ticker at a time as TD API does not support multiple tickers
    for a single call.

    :param start_date: Start date to get ticker data from (ex. 2023-01-01)
    :type start_date: str
    :param end_date: Start date to get ticker data from (ex. 2023-12-31)
    :type end_date: str

    :return: A dictionary of :class:`pandas.core.frame.DataFrame`
    """

    # TODO update so that start_date can be NULL and is the day after the newest data stored in DB

    if not end_date:
        end_date = datetime.today().date().strftime('%Y-%m-%d')

    sp_russell_tickers = get_sp_russell_list()
    return get_historical_tickers_data(sp_russell_tickers, start_date, end_date)


def update_companies_database():
    yf_json = '../data/MarketData/yf_stock_info.json'
    if not os.path.exists(yf_json):

        ticker_df = Utils.get_all_tickers()
        ticker_df = filter_symbols(ticker_df)
        market_tickers = ticker_df['Symbol'].to_list()

        yf_ticker_info = get_yfinance_ticker_info(market_tickers)

        # Write out to file to store all the data collection
        write_yf_json(yf_json, yf_ticker_info)
    else:
        yf_ticker_info = load_yf_json(yf_json)

    qsm = QuordataSqlManager()
    qsm.update_companies_table(yf_ticker_info)


if __name__ == '__main__':

    stock_data = get_sp_russell_historical_data('2000-01-01')
    qsm = QuordataSqlManager()
    qsm.update_stock_data(stock_data)

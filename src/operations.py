import datetime
import os
from os import path
import math
import numpy as np
import pandas as pd
import yfinance as yf
from operator import itemgetter
import uuid
import random
import json


def get_etf_histories():
  etf_details = pd.read_csv('./etfs_details_type_fund_flow.csv')
  symbols = etf_details['Symbol'].tolist()

  # get list of symbols
  etf_histories_path = './etf_histories.csv'
  df_histories = None
  if path.exists(etf_histories_path):
      df_histories = pd.read_csv(etf_histories_path)
  else:
      etf_details = pd.read_csv('./etfs_details_type_fund_flow.csv')
      symbols = etf_details['Symbol'].tolist()


      histories = yf.download(symbols, period='max', interval='1d')

      df_histories = histories

      df_histories = (df_histories
                      .drop(['Adj Close', 'Close', 'Volume', 'High', 'Low'], axis=1)
                      .transpose()
                      .reset_index()
                      .drop(['level_0'], axis=1)
                      .rename(columns={ 'level_1': 'Symbol' })
                      .set_index('Symbol')
                    )
      df_histories.to_csv('./etf_histories.csv')

  return df_histories.set_index('Symbol')



def discover_etfs_past(df, end_date, period):
  # convert end_date to datetime format for date manipulation
  end_date = np.datetime64(end_date).astype('datetime64[D]')
  end_date_str = end_date.astype(str) + ' 00:00:00'

  # get start_date based on period
  # period is in months
  start_date = end_date - np.timedelta64(period, 'M').astype('timedelta64[D]')
  start_date_str = start_date.astype(str) + ' 00:00:00'

  if isinstance(df, pd.DataFrame):
    # find first available start_date going back
    while not start_date_str in df.columns.tolist():
      start_date = start_date - np.timedelta64(1, 'D')
      start_date_str = start_date.astype(str) + ' 00:00:00'

    # find first available end_date going back
    while not end_date_str in df.columns.tolist():
      end_date = end_date - np.timedelta64(1, 'D')
      end_date_str = end_date.astype(str) + ' 00:00:00'


    start_prices = df[start_date_str]
    end_prices = df[end_date_str]
    return (end_prices - start_prices) / start_prices


def choose_etfs(df, configs):
  config_options, etf_samples_size, purchase_date = itemgetter('etf_history_options', 'max_etf_samples_size', 'purchase_date')(configs)
  # config_options is list of config_option
  # config_option has end_date, period, method
  # method is 'top', 'bottom', 'random'

  purchase_date_str = purchase_date.astype(str)
  
  candidates = []
  for config_option in config_options:
    period = config_option['period']
    method = config_option['method']

    etfs_changed = discover_etfs_past(df, purchase_date_str, period)

    current_candidates = []
    if method == 'top':
      etfs_changed = etfs_changed.sort_values(ascending=False)
    elif method == 'bottom':
      etfs_changed = etfs_changed.sort_values(ascending=True)
    else:
      etfs_changed = etfs_changed.sample(n=etf_samples_size)

    current_candidates = etfs_changed.head(etf_samples_size).index.tolist()

    if len(candidates) == 0:
      candidates = current_candidates
    else:
      candidates = list(set(candidates) & set(current_candidates))

  return candidates



portfolio_cols = ['ID', 'Symbol', 'Purchase Date', 'Purchase Price', 'Spent', 'Quantity', 'Sell Date', 'Sell Price', 'Gained', 'ROI', 'Current Value', 'Hold Length']

def purchase_etfs(df, configs):
  purchase_date, monthly_budget, purchase_frequency = itemgetter('purchase_date', 'monthly_budget', 'purchase_frequency')(configs)
  purchase_budget = monthly_budget * purchase_frequency

  candidates = choose_etfs(df, configs)

  purchase_date_str = purchase_date.astype(str) + ' 00:00:00'

  # go backwards each day until a purchase date is found
  while not purchase_date_str in df.columns.tolist():
    purchase_date = purchase_date - np.timedelta64(1, 'D')
    purchase_date_str = purchase_date.astype(str) + ' 00:00:00'

  
  global portfolio_cols
  df_purchase_session = pd.DataFrame([], columns=portfolio_cols)

  def purchase_candidate(df, df_purchase_session, symbol, purchase_date_str, budget):
    purchase_price = df[purchase_date_str][symbol]

    if math.isnan(purchase_price):
      return None

    quantity = float(math.floor(budget / purchase_price))
    spent = purchase_price * quantity

    if quantity > 0:
      purchase_id = uuid.uuid1().hex

      global portfolio_cols

      df_purchase = pd.DataFrame([
        [purchase_id, symbol, purchase_date, purchase_price, spent, quantity, None, None, None, None, None, None]
      ],
        columns=portfolio_cols
      )

      return df_purchase

  
  # purchase candidates from budget
  if len(candidates) > 0:
    budget = purchase_budget / len(candidates)
    for symbol in candidates:
      df_purchase = purchase_candidate(df, df_purchase_session, symbol, purchase_date_str, budget)
      if isinstance(df_purchase, pd.DataFrame):
        df_purchase_session = pd.concat([df_purchase_session, df_purchase])

    # calculate how much budget is left over, and assign it to first candidate
    remaining_budget = purchase_budget
    remaining_budget -= df_purchase_session.sum()['Spent']
    

    if remaining_budget > 0:
      df_purchase = purchase_candidate(df, df_purchase_session, candidates[0], purchase_date_str, remaining_budget)
      if isinstance(df_purchase, pd.DataFrame):
        df_purchase_session = pd.concat([df_purchase_session, df_purchase])


  return df_purchase_session




def purchase_etfs_across_dates(df, configs):
  purchase_dates = itemgetter('purchase_dates')(configs)

  global portfolio_cols
  df_portfolio = pd.DataFrame([], columns=portfolio_cols)

  for date in purchase_dates:
    configs['purchase_date'] = date
    purchase_session = purchase_etfs(df, configs)
    
    df_portfolio = pd.concat([df_portfolio, purchase_session])

  return df_portfolio.reset_index().drop('index', axis=1)



def sell_purchases(df_portfolio, df, config):
  roi_threshold = itemgetter('roi_threshold')(config)

  df_transposed = df.transpose()

  for index, purchase in df_portfolio.iterrows():
    purchase_quantity = purchase['Quantity']
    purchase_price = purchase['Purchase Price']
    purchase_symbol = purchase['Symbol']
    purchase_date = purchase['Purchase Date']
    min_sell_date = (
      np.datetime64(purchase_date).astype('datetime64[D]') + np.timedelta64(config['min_sell_period'], 'Y').astype('timedelta64[D]')
    ).astype(str)

    etf_prices = df_transposed[purchase_symbol]
    etf_prices = etf_prices.reset_index().rename(columns={ 'index': 'Date', purchase_symbol: 'Price' })
    etf_prices = (
      etf_prices
        .where((etf_prices['Date'] > min_sell_date) & (etf_prices['Price'] > purchase_price * (1 + roi_threshold)))
        .dropna()
        .reset_index()
        .drop('index', axis=1)
      )

    purchase_end_date = None
    if etf_prices.size > 0:
      sale = etf_prices.iloc[0]
      sell_price = sale['Price']
      purchase['Sell Price'] = sell_price
      purchase['Sell Date'] = sale['Date']
      purchase['ROI'] = (sell_price - purchase_price) / purchase_price
      purchase['Gained'] = (sell_price - purchase_price) * purchase_quantity
      purchase_end_date = np.datetime64(sale['Date'])
    else:
      purchase['Current Value'] = df_transposed[purchase_symbol].iloc[-1] * purchase_quantity
      purchase_end_date = np.datetime64('now')

    purchase['Hold Length'] = (purchase_end_date - np.datetime64(purchase['Purchase Date'])).astype('timedelta64[Y]') / np.timedelta64(1, 'Y')

    df_portfolio.iloc[index] = purchase
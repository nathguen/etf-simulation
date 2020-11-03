# import packages
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
from src.config import generate_config_key, run_config
from src.utils import Merge, start_session, log_running_time
from src.operations import purchase_etfs_across_dates, sell_purchases, get_etf_histories


operation_start_time = None

histories = get_etf_histories()

config_keys = {}







summary_cols = [
  'ID', 
  'Start Date', 
  'Years Run', 
  'Avg Hold Length', 
  'ETFS Purchased', 
  'Total Unique ETFS Purchased', 
  'ETFS Sold', 
  'Total Unique ETFS Sold', 
  'History Method (Direction)', 
  'Purchase Frequency', 
  'Max Samples Size', 
  'Avg Sample Size', 
  'Monthly Budget', 
  'Min Sell Period', 
  'Spent / Year', 
  'Gained', 
  'ROI', 
  'Current Value', 
  'Net Worth / Year', 
  'ROI Threshold'
  ]

def run_configs():
  runs = 400

  global summary_cols

  df_summaries = pd.DataFrame([], columns=summary_cols)

  global config_keys
  global histories

  i = 0
  config_key = generate_config_key()
  collisions = 0
  while (i < runs) and collisions < 3:
    if (config_key['id'] not in config_keys):
      config_keys[config_key['id']] = True
    else:
      collisions += 1
      continue

    print('run # ', str(i + 1))
    start_date, purchase_frequency, history_direction, etf_history_options, max_etf_samples_size, monthly_budget, min_sell_period, roi_threshold = itemgetter(
      'start_date',
      'purchase_frequency',
      'history_direction',
      'etf_history_options',
      'max_etf_samples_size',
      'monthly_budget',
      'min_sell_period',
      'roi_threshold'
    )(config_key)

    # purchase config
    purchase_config = {
      'purchase_dates': np.arange(
            np.datetime64(start_date),
            np.datetime64('now'),
            np.timedelta64(purchase_frequency, 'M'),
            dtype='datetime64[M]'
      ).astype('datetime64[D]'),
      'etf_history_options': etf_history_options,
      'max_etf_samples_size': max_etf_samples_size,
      'monthly_budget': monthly_budget,
      'purchase_frequency': purchase_frequency
    }

    # sell config
    sell_config = {
      'min_sell_period': min_sell_period,
      'roi_threshold': roi_threshold
    }

    configs = {
      'purchase_config': purchase_config,
      'sell_config': sell_config
    }

    df_portfolio = run_config(histories, configs)

    # Portfolio ID
    portfolio_id = uuid.uuid1().hex

    # General Averages
    means = df_portfolio.mean()
    
    # General Sums
    sums = df_portfolio.sum()

    # ROI
    roi_sums = df_portfolio.where(df_portfolio['Sell Price'] > 0).sum()
    roi = roi_sums['Gained'] / roi_sums['Spent']

    # Net Worth
    net_worth = sums['Current Value'] + sums['Gained']

    # Years
    years = (np.datetime64('now') - np.datetime64(start_date)).astype('timedelta64[Y]') / np.timedelta64(1, 'Y')

    # Net Worth per Year
    net_worth_per_year = net_worth / years

    # ETFS Purchased
    etfs_purchased = ', '.join(
      df_portfolio['Symbol'].unique()
    )

    # ETFS Sold
    etfs_sold = ', '.join(
      df_portfolio.where(df_portfolio['Sell Price'] > 0).dropna(how='all')['Symbol'].unique()
    )

    # Avg Sample Size
    avg_sample_size = df_portfolio.groupby('Purchase Date').count().mean()['Symbol']

    # add 'avg hold length', 'etfs purchased', 'etfs sold', 'years', 'actual sample sizes', 'spent per year', 'net worth per year'
    df_summary = pd.DataFrame([[
        portfolio_id,
        start_date,
        years,
        means['Hold Length'],
        etfs_purchased,
        len(etfs_purchased),
        etfs_sold,
        len(etfs_sold),
        history_direction,
        purchase_frequency,
        max_etf_samples_size,
        avg_sample_size,
        monthly_budget,
        min_sell_period,
        sums['Spent'] / years,
        sums['Gained'],
        roi,
        sums['Current Value'],
        net_worth_per_year,
        roi_threshold
      ]],
      columns=summary_cols
    )

    df_summaries = pd.concat([df_summaries, df_summary])

    config_key = generate_config_key()
    i += 1


    # assign configs to portoflio
    writer = pd.ExcelWriter('./portfolios/' + portfolio_id + '.xlsx')
    # pylint: disable=abstract-class-instantiated

    df_summary.to_excel(writer, 'Summary')
    df_portfolio.to_excel(writer, 'Portfolio')

    writer.save()

  
  df_summaries.to_csv('./summaries.csv')



run_configs()


# import packages
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


session_start_time = None

# # configurable dials
# max_etf_samples_size = 5

# # etf history options
# etf_history_options = [
#   {
#     'period': 12,
#     'method': 'bottom'
#   },
#   {
#     'period': 6,
#     'method': 'bottom'
#   }
# ]

# # purchase config
# purchase_config = {
#   'purchase_dates': np.arange(
#         np.datetime64('2010-01-01'),
#         np.datetime64('now'),
#         np.timedelta64(1, 'M'),
#         dtype='datetime64[M]'
#     ).astype('datetime64[D]'),
#   'etf_history_options': etf_history_options,
#   'max_etf_samples_size': max_etf_samples_size,
#   'monthly_budget': 4000
# }

# # sell config
# sell_config = {
#   'min_sell_period': 8
# }

# # def generate_config_key():




# Python code to merge dict using a single 
# expression
def Merge(dict1, dict2):
  res = {**dict1, **dict2}
  return res


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


histories = get_etf_histories()


def discover_etfs_past(df, end_date, period):
  # if not type(end_date) == str:
  #   print('start_date must be a string, but received ' + type(end_date))
  #   return

  # convert end_date to datetime format for date manipulation
  end_date = np.datetime64(end_date).astype('datetime64[D]')
  end_date_str = end_date.astype(str) + ' 00:00:00'

  # get start_date based on period
  # period is in months
  start_date = end_date - np.timedelta64(period, 'M').astype('timedelta64[D]')
  start_date_str = start_date.astype(str) + ' 00:00:00'

  if isinstance(df, pd.DataFrame):
    # df_etfs = df.copy()

    # find first available start_date going back
    while not start_date_str in df.columns.tolist():
      start_date = start_date - np.timedelta64(1, 'D')
      start_date_str = start_date.astype(str) + ' 00:00:00'

    # find first available end_date going back
    while not end_date_str in df.columns.tolist():
      end_date = end_date - np.timedelta64(1, 'D')
      end_date_str = end_date.astype(str) + ' 00:00:00'

    # print(df_etfs.columns.tolist())
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



portfolio_cols = ['ID', 'Symbol', 'Purchase Date', 'Purchase Price', 'Spent', 'Quantity', 'Sell Date', 'Sell Price', 'Gained', 'ROI', 'Current Value']

def purchase_etfs(df, configs):
  purchase_date, monthly_budget = itemgetter('purchase_date', 'monthly_budget')(configs)

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
        [purchase_id, symbol, purchase_date, purchase_price, spent, quantity, None, None, None, None, None]
      ],
        columns=portfolio_cols
      )

      return df_purchase

      

  
  # purchase candidates from budget
  if len(candidates) > 0:
    budget = monthly_budget / len(candidates)
    for symbol in candidates:
      df_purchase = purchase_candidate(df, df_purchase_session, symbol, purchase_date_str, budget)
      if isinstance(df_purchase, pd.DataFrame):
        df_purchase_session = pd.concat([df_purchase_session, df_purchase])

    # calculate how much budget is left over, and assign it to first candidate
    remaining_budget = monthly_budget
    remaining_budget -= df_purchase_session.sum()['Spent']
    

    if remaining_budget > 0:
      df_purchase = purchase_candidate(df, df_purchase_session, candidates[0], purchase_date_str, remaining_budget)
      if isinstance(df_purchase, pd.DataFrame):
        df_purchase_session = pd.concat([df_purchase_session, df_purchase])

  # if df_purchase_session.size > 0:
  #   log_running_time('made purchase on ' + str(purchase_date))
  return df_purchase_session




def purchase_etfs_across_dates(df, configs):
  purchase_dates = itemgetter('purchase_dates')(configs)

  global portfolio_cols
  df_portfolio = pd.DataFrame([], columns=portfolio_cols)

  for date in purchase_dates:
    configs['purchase_date'] = date
    purchase_session = purchase_etfs(df, configs)
    
    df_portfolio = pd.concat([df_portfolio, purchase_session])
    # log_running_time('made purchase for ' + date.astype(str))
    # portfolio = Merge(portfolio, purchase_session)

  return df_portfolio.reset_index().drop('index', axis=1)

# discover_etfs_past(histories, '2020-10-01', 6)


def sell_purchase(df, purchase, config):
  purchase_date = purchase['Purchase Date']
  purchase_price = purchase['Purchase Price']
  symbol = purchase['Symbol']
  quantity = purchase['Quantity']

  min_sell_period = itemgetter('min_sell_period')(config)

  min_sell_date = (
    np.datetime64(purchase_date).astype('datetime64[D]') + 
    np.timedelta64(min_sell_period, 'Y').astype('timedelta64[D]')
  )
  min_sell_date_str = min_sell_date.astype(str) + ' 00:00:00'

  # determines first date after min_sell_period
  while (
    min_sell_date_str not in df.columns.tolist() and
    min_sell_date < np.datetime64('now')
  ):
    min_sell_date = min_sell_date + np.timedelta64(1, 'D')
    min_sell_date_str = min_sell_date.astype(str) + ' 00:00:00'

  df_sell = df.copy()

  df_sell = df_sell.loc[symbol, min_sell_date_str:]
  df_sell = df_sell.where(df_sell > purchase_price).dropna()
  
  if len(df_sell.index.tolist()) > 0:
    sell_date = df_sell.index.values[0]
    sell_price = df_sell[sell_date]

    purchase['Sell Date'] = sell_date
    purchase['Sell Price'] = sell_price
    purchase['Gained'] = (sell_price - purchase_price) * quantity
    purchase['ROI'] = purchase['Gained'] / purchase['Spent']
  else:
    today_price = df[df.columns.tolist()[-1]][symbol]
    current_value = today_price * purchase['Quantity']
    purchase['Current Value'] = current_value

  return purchase



def log_running_time(event_name):
  global session_start_time
  print(event_name + ': ', (np.datetime64('now') - session_start_time).astype(str))



config_keys = {}
def generate_config_key():
  # start_date
  config_start_date = np.datetime64(
    str(random.randint(1995, 2010)) + '-01-01'
  ).astype(str)

  # @TODO figure out why 1994 and less causes script to hang
  # config_start_date = '1994-01-01'

  # etf_history_options
  history_directions = ['top', 'bottom', 'none']
  direction = random.choice(history_directions)
  
  # global etf_history_options
  config_etf_history_options = [
    {
      'period': 12,
      'method': direction
    },
    {
      'period': 6,
      'method': direction
    },
    {
      'period': 3,
      'method': direction
    }
  ]

  # max_etf_samples_size
  config_max_etf_samples_size = random.randint(5, 20)

  # purchase_frequency
  config_purchase_frequency = 1 # every num of months

  # monthly_budget
  config_monthly_budget = 4000

  # min_sell_period
  config_min_sell_period = random.randint(1, 10)

  config = {
    'start_date': config_start_date,
    'etf_history_options': config_etf_history_options,
    'max_etf_samples_size': config_max_etf_samples_size,
    'purchase_frequency': config_purchase_frequency,
    'monthly_budget': config_monthly_budget,
    'min_sell_period': config_min_sell_period
  }

  config['id'] = (
    config_start_date +
    ':' +
    str(config_max_etf_samples_size) +
    ':' +
    str(config_monthly_budget) +
    ':' +
    str(config_min_sell_period)
  )

  # global config_keys
  # config_keys[config['id']] = True


  return config


def run_config(df, configs):
  global session_start_time
  session_start_time = np.datetime64('now')

  log_running_time('starting session')

  purchase_config, sell_config = itemgetter('purchase_config', 'sell_config')(configs)

  log_running_time('creating portfolio')
  df_portfolio = purchase_etfs_across_dates(df, purchase_config)
  log_running_time('created portfolio')

  # attempt to sell portfolio items
  log_running_time('processing for sales or holds')
  for index, purchase in df_portfolio.iterrows():
    updated_purchase = sell_purchase(df, purchase, sell_config)
    df_portfolio.iloc[index] = updated_purchase

  log_running_time('sold portfolio')

  return df_portfolio

  # sums = df_portfolio.sum()

  # update ROI for sum to ensure weight is considered
  # roi = sums['Gained'] / sums['Spent']
  # sums['ROI'] = roi

  # return sums


# summary_cols = portfolio_cols.append('ROI')
summary_cols = ['Start Date', 'History', 'Purchase Frequency', 'Max Samples Size', 'Min Sell Period', 'Gained', 'ROI', 'Current Value']

def run_configs():
  runs = 2

  global summary_cols
  global portfolio_cols

  df_summaries = pd.DataFrame([], columns=summary_cols)
  # df_portfolios = pd.DataFrame([], columns=portfolio_cols)

  global config_keys
  global histories

  i = 0
  config_key = generate_config_key()
  while (i < runs) and (config_key['id'] not in config_keys):
    config_keys[config_key['id']] = True

    print('run # ', str(i + 1))
    start_date, purchase_frequency, etf_history_options, max_etf_samples_size, monthly_budget, min_sell_period = itemgetter(
      'start_date',
      'purchase_frequency',
      'etf_history_options',
      'max_etf_samples_size',
      'monthly_budget',
      'min_sell_period'
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
      'monthly_budget': monthly_budget
    }

    # sell config
    sell_config = {
      'min_sell_period': min_sell_period
    }

    configs = {
      'purchase_config': purchase_config,
      'sell_config': sell_config
    }

    df_portfolio = run_config(histories, configs)

    df_portfolio.to_csv('./portfolios/' + config_key['id'] + '.csv')

    # determine holdings
    # final_values = histories[histories.columns.tolist()[-1]]
    # df_portfolio = df_portfolio.set_index('ID')
    # print(df_portfolio)
    # print(final_values)

    # print(df_portfolio[df_portfolio.index.duplicated()])
    # print(final_values[final_values.index.duplicated()])

    # df_portfolio['Current Value'] = df_portfolio['Quantity'] * final_values

    # determin overall ROI
    sums = df_portfolio.sum()
    roi = sums['Gained'] / sums['Spent']
    sums['ROI'] = roi

    summary_history_list = []
    for option in etf_history_options:
      summary_history_list.append(str(option['period']) + option['method'])


    df_summary = pd.DataFrame([[
        start_date,
        ', '.join(summary_history_list),
        purchase_frequency,
        max_etf_samples_size,
        min_sell_period,
        # summary['Purchase Price'],
        # summary['Spent'],
        sums['Gained'],
        sums['ROI'],
        sums['Current Value'],
      ]],
      columns=summary_cols
    )

    df_summaries = pd.concat([df_summaries, df_summary])

    config_key = generate_config_key()
    i += 1

  
  df_portfolio.to_csv('./portfolios/' + config_key['id'] + '.csv')
  df_summaries.to_csv('./summaries.csv')



run_configs()


# configs = {
#   'purchase_config': purchase_config,
#   'sell_config': sell_config
# }

# run_config(histories, configs) 
import random
import numpy as np
from src.utils import start_session, log_running_time
from src.operations import purchase_etfs_across_dates, sell_purchases
from operator import itemgetter

def generate_config_key():
  # start_date
  start_month_str = ''
  start_month = random.randint(1, 12)
  if start_month >= 10:
    start_month_str = str(start_month)
  else:
    start_month_str = '0' + str(start_month)
  config_start_date = np.datetime64(
    str(random.randint(1995, 2015)) +
    '-' +
    start_month_str +
    '-01'
  ).astype(str)

  # @TODO figure out why 1994 and less causes script to hang
  # config_start_date = '1994-01-01'

  # etf_history_options
  global history_direction
  history_directions = ['top', 'bottom', 'none']
  # history_direction = random.choice(history_directions)
  history_direction = 'top'
  
  # global etf_history_options
  config_etf_history_options = [
    {
      'period': 12,
      'method': history_direction
    },
    {
      'period': 6,
      'method': history_direction
    },
    {
      'period': 3,
      'method': history_direction
    }
  ]

  # max_etf_samples_size
  config_max_etf_samples_size = random.randint(5, 20)
  # config_max_etf_samples_size = 10

  # purchase_frequency
  config_purchase_frequency = 1 # every num of months
  # config_purchase_frequency = random.randint(1, 24)

  # monthly_budget
  config_monthly_budget = 4000

  # min_sell_period
  # config_min_sell_period = random.randint(5, 10)
  config_min_sell_period = 1

  # roi_threshold
  # global roi_theshold
  # roi_threshold = float(random.randint(1, 100)) / 100
  config_roi_threshold = float(random.randint(1, 400)) / 100

  config = {
    'start_date': config_start_date,
    'history_direction': history_direction,
    'etf_history_options': config_etf_history_options,
    'max_etf_samples_size': config_max_etf_samples_size,
    'purchase_frequency': config_purchase_frequency,
    'monthly_budget': config_monthly_budget,
    'min_sell_period': config_min_sell_period,
    'roi_threshold': config_roi_threshold
    # 'roi_theshold': config_roi_threshold
  }

  config['id'] = (
    config_start_date +
    ':' +
    history_direction +
    ':' +
    str(config_max_etf_samples_size) +
    ':' +
    str(config_monthly_budget) +
    ':' +
    str(config_min_sell_period) +
    ':' +
    str(config_purchase_frequency) +
    ':' +
    str(config_roi_threshold)
  )


  return config




def run_config(df, configs):
  start_session()

  purchase_config, sell_config = itemgetter('purchase_config', 'sell_config')(configs)

  log_running_time('creating portfolio')
  df_portfolio = purchase_etfs_across_dates(df, purchase_config)
  log_running_time('created portfolio')

  # attempt to sell portfolio items
  sell_purchases(df_portfolio, df, sell_config)

  log_running_time('sold portfolio')
  log_running_time('run time', True)

  return df_portfolio
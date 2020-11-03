from src.operations import get_etf_histories, choose_etfs
import pandas as pd
import datetime
import numpy as np

df_histories = get_etf_histories()

configs = {
  'etf_history_options': [
    {
      'period': 12,
      'method': 'top'
    },
    {
      'period': 6,
      'method': 'top'
    },
    {
      'period': 3,
      'method': 'top'
    }
  ],
  'max_etf_samples_size': 10,
  'purchase_date': np.datetime64('now').astype(str)
}

candidates = choose_etfs(df_histories, configs)

print(candidates)
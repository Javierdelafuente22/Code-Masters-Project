"""
Loads the orderbook data and splits it into train and test sets.
The split is by date: the first 80% of days train, the last 20% test.
"""

import pandas as pd
import numpy as np


TARGET_AGENT = '1_Prosumer'
OTHER_AGENTS = ['2_Prosumer', '3_Prosumer', '4_Prosumer', '5_Prosumer',
                '6_Buyer', '7_Buyer', '8_Seller', '9_Seller', '10_Seller']

REQUIRED_COLUMNS = [
    'timestamp', 'time_year_sin', 'time_year_cos', 'time_day_sin', 'time_day_cos',
    'is_working_day', 'import_price', 'export_price', 'spread', 'net_community',
    TARGET_AGENT
] + OTHER_AGENTS


def load_and_split(csv_path, train_ratio=0.8, episode_length=24):
    """Loads the orderbook CSV and splits it into train and test DataFrames by day."""
    df = pd.read_csv(csv_path)

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in dataset: {missing}")

    df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=True)

    df = df.sort_values('timestamp').reset_index(drop=True)

    total_rows = len(df)
    total_days = total_rows // episode_length

    # Drop any incomplete final day
    usable_rows = total_days * episode_length
    df = df.iloc[:usable_rows].reset_index(drop=True)

    train_days = int(total_days * train_ratio)
    test_days = total_days - train_days
    train_rows = train_days * episode_length
    
    df_train = df.iloc[:train_rows].reset_index(drop=True)
    df_test = df.iloc[train_rows:].reset_index(drop=True)
    
    info = {
        'total_rows': len(df),
        'total_days': total_days,
        'train_days': train_days,
        'test_days': test_days,
        'train_rows': len(df_train),
        'test_rows': len(df_test),
        'train_start': df_train['timestamp'].iloc[0],
        'train_end': df_train['timestamp'].iloc[-1],
        'test_start': df_test['timestamp'].iloc[0],
        'test_end': df_test['timestamp'].iloc[-1],
    }
    
    return df_train, df_test, info


def print_split_info(info):
    """Pretty-print the train/test split metadata."""
    print("=" * 55)
    print("TRAIN / TEST SPLIT")
    print("=" * 55)
    print(f"Total days:  {info['total_days']}")
    print(f"Train days:  {info['train_days']}  ({info['train_rows']} rows)")
    print(f"  Period:    {info['train_start']} → {info['train_end']}")
    print(f"Test days:   {info['test_days']}  ({info['test_rows']} rows)")
    print(f"  Period:    {info['test_start']} → {info['test_end']}")
    print("=" * 55)

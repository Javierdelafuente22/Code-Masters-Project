"""
Runs the trained Q-learning battery agent over the data.
Loads the saved policy and applies it hour by hour.
"""

import pandas as pd
import numpy as np
from collections import deque
import time

from trading_algorithms.Qlearning.battery_alg_qlearning import QLearningBattery

def run_rl_market_simulation(input_file, target_users):
    df = pd.read_csv(input_file)
    agent_ids = [c for c in df.columns if c not in ['timestamp', 'time_year_sin', 'time_year_cos', 'time_day_sin', 'time_day_cos', 'import_price', 'export_price']]

    # Load the brain we trained earlier (epsilon 0 means it never explores, just uses what it learned)
    rl_agent = QLearningBattery(epsilon=0.0)
    try:
        rl_agent.q_table = np.load('trained_q_table.npy')
        print("Successfully loaded trained RL brain.")
    except FileNotFoundError:
        print("Error: trained_q_table.npy not found! Run the training script first.")
        return

    prices = deque(maxlen=48)

    # Go through the data one hour at a time
    for idx, row in df.iterrows():
        tou = row['import_price']
        hour = pd.to_datetime(row['timestamp']).hour
        prices.append(tou)

        # Recent price levels the agent uses to judge cheap vs expensive (same as in training)
        f_p, m_p, c_p = None, None, None
        if len(prices) >= 24:
            f_p, m_p, c_p = np.percentile(prices, 20), np.percentile(prices, 50), np.percentile(prices, 80)

        # Let the trained battery act for each target user
        for u in target_users:
            if u in agent_ids:
                row[u], _ = rl_agent.optimize_demand(
                    row[u], tou, f_p, c_p, m_p, hour, is_training=False
                )

if __name__ == "__main__":
    start = time.time()
    run_rl_market_simulation(
        input_file='data/orderbook.csv',
        target_users=['1_Prosumer']
    )
    print(f"Total Simulation Time: {time.time() - start:.2f}s")

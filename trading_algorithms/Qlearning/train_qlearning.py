"""
Trains the Q-learning battery agent and saves its brain to trained_q_table.npy.
"""

import pandas as pd
import numpy as np
from collections import deque
from trading_algorithms.Qlearning.battery_alg_qlearning import QLearningBattery

def train_agent(episodes=500):
    df = pd.read_csv('data/orderbook.csv')
    agent = QLearningBattery()
    target_user = '1_Prosumer'

    print(f"Starting training for {target_user}...")
    for ep in range(episodes):
        agent.soc = 0.0  # start each run with an empty battery
        prices = deque(maxlen=48)
        # Explore a lot at first, then gradually rely on what it has learned
        agent.epsilon = max(0.01, 1.0 - (ep / (episodes * 0.85)))

        for _, row in df.iterrows():
            tou = row['import_price']
            hour = pd.to_datetime(row['timestamp']).hour
            prices.append(tou)

            f_p, c_p, m_p = None, None, None
            if len(prices) >= 24:
                f_p, c_p, m_p = np.percentile(prices, 20), np.percentile(prices, 80), np.percentile(prices, 50)

            agent.optimize_demand(row[target_user], tou, f_p, c_p, m_p, hour, is_training=True)

        if ep % 50 == 0:
            print(f"Episode {ep}/{episodes} | Epsilon: {agent.epsilon:.2f}")

    np.save('trained_q_table.npy', agent.q_table)
    print("Training Complete. Brain saved as 'trained_q_table.npy'.")

if __name__ == "__main__":
    train_agent(500)

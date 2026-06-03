"""
Q-learning battery agent.
Learns by trial and error when to charge, hold or discharge to save money.
"""

import numpy as np
import random

class QLearningBattery:
    """A battery that learns a charging strategy with Q-learning."""
    def __init__(self, max_rate=0.4, efficiency=0.95, min_soc=0.0, max_soc=1.0,
                 learning_rate=0.1, discount_factor=0.99, epsilon=1.0):
        self.max_rate, self.efficiency = max_rate, efficiency
        self.min_soc, self.max_soc = min_soc, max_soc
        self.soc = min_soc

        self.alpha = learning_rate      # how fast it learns
        self.gamma = discount_factor    # how much it values future rewards
        self.epsilon = epsilon          # how often it tries random actions

        # The Q-table holds a score for each action in every (battery level, price, hour) situation
        self.soc_bins = np.linspace(0, 1, 11)
        self.price_bins = 5
        self.hours = 24
        self.q_table = np.zeros((len(self.soc_bins), self.price_bins, self.hours, 3))

        self.last_state = None
        self.last_action = None

    def get_state_bins(self, soc, price, floor_p, ceil_p, hour):
        """Turn the current situation into a simple state for the Q-table."""
        s_bin = np.digitize(soc, self.soc_bins) - 1
        if floor_p is None or ceil_p is None: p_bin = 2
        else:
            if price <= floor_p: p_bin = 0
            elif price >= ceil_p: p_bin = 4
            elif price < (floor_p + ceil_p)/2: p_bin = 1
            else: p_bin = 3
        return (s_bin, p_bin, int(hour))

    def choose_action(self, state):
        """Either explore a random action or use the best one learned so far."""
        if random.uniform(0, 1) < self.epsilon:
            return random.randint(0, 2)
        return np.argmax(self.q_table[state])

    def update_q_table(self, old_state, action, reward, new_state):
        """Update the score for the action we just took based on the reward."""
        best_future_q = np.max(self.q_table[new_state])
        old_q = self.q_table[old_state + (action,)]
        self.q_table[old_state + (action,)] = old_q + self.alpha * (reward + self.gamma * best_future_q - old_q)

    def optimize_demand(self, raw_demand, current_price, floor_p, ceil_p, median_p, hour, is_training=True):
        """Pick the battery action for this hour and, while training, learn from the result."""
        rem_rate = self.max_rate
        state = self.get_state_bins(self.soc, current_price, floor_p, ceil_p, hour)

        # Always store spare solar first
        if raw_demand < 0:
            charge = min(abs(raw_demand), rem_rate, (self.max_soc - self.soc) / self.efficiency)
            self.soc += charge * self.efficiency
            rem_rate -= charge
            raw_demand += charge

        # Then let the agent choose: charge, hold or discharge
        action = self.choose_action(state)
        reward = 0

        if rem_rate > 0:
            if action == 0: # Charge
                actual_charge = min(rem_rate, (self.max_soc - self.soc) / self.efficiency)
                if actual_charge < 0.01: # can't charge, battery is full
                    reward = -20
                else:
                    self.soc += actual_charge * self.efficiency
                    raw_demand += actual_charge
                    reward = -(actual_charge * current_price) * 10 - 1

            elif action == 2: # Discharge
                actual_discharge = min(rem_rate, self.soc - self.min_soc)
                if actual_discharge < 0.01: # can't discharge, battery is empty
                    reward = -20
                else:
                    self.soc -= actual_discharge
                    raw_demand -= actual_discharge
                    reward = (actual_discharge * current_price) * 100 - 1

            else: # Hold
                if ceil_p and current_price >= ceil_p and self.soc > 0.8:
                    reward = -15 # penalise sitting on a full battery when prices are high
                else:
                    reward = 0

        # Learn from what just happened
        if is_training and self.last_state is not None:
            self.update_q_table(self.last_state, self.last_action, reward, state)

        self.last_state, self.last_action = state, action
        return raw_demand, self.soc

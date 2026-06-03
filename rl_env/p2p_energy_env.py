"""
Gymnasium environment for P2P energy trading.
The agent controls a battery for 1_Prosumer over a day and tries to lower energy costs.
Each episode is one day of 24 hourly steps.
"""

import gymnasium as gym
from gymnasium import spaces
import numpy as np
import pandas as pd

from rl_env.battery import Battery
from rl_env.rl_orderbook_simp import clear_market_for_agent


# Action in [-1, 1] scales to battery power in [-MAX_RATE, MAX_RATE].
# Must match the Battery's max_rate.
MAX_RATE = 0.4

# Market feature columns taken from the dataset.
MARKET_FEATURES = [
    'import_price', 'export_price', 'spread', 'net_community',
    'time_day_sin', 'time_day_cos', 'time_year_sin', 'time_year_cos',
    'is_working_day'
]

TARGET_AGENT = '1_Prosumer'
OTHER_AGENTS = ['2_Prosumer', '3_Prosumer', '4_Prosumer', '5_Prosumer',
                '6_Buyer', '7_Buyer', '8_Seller', '9_Seller', '10_Seller']


class P2PEnergyTradingEnv(gym.Env):
    """Gymnasium environment for P2P energy trading with a battery."""
    metadata = {"render_modes": []}

    def __init__(self, df, reward_scale=10.0, episode_length=24):
        """Set up the environment from the dataset (24 steps make one day)."""
        super().__init__()

        self.df = df.reset_index(drop=True)
        self.reward_scale = reward_scale
        self.episode_length = episode_length

        # Turn the columns we need into numpy arrays for fast access during training.
        self.raw_demands = self.df[TARGET_AGENT].values.astype(np.float32)
        self.market_features = self.df[MARKET_FEATURES].values.astype(np.float32)
        self.import_prices = self.df['import_price'].values.astype(np.float32)
        self.export_prices = self.df['export_price'].values.astype(np.float32)
        self.others_demands = self.df[OTHER_AGENTS].values.astype(np.float32)

        # Each episode is 24 consecutive hours starting at midnight.
        total_rows = len(self.df)
        self.episode_starts = list(range(0, total_rows - episode_length + 1, episode_length))

        if len(self.episode_starts) == 0:
            raise ValueError(f"Dataset too small ({total_rows} rows) for episode_length={episode_length}")

        self.battery = Battery(capacity=1.0, max_rate=0.4, efficiency=0.95, initial_soc=0.0)

        # Action: a value in [-1, 1] where -1 is full discharge, 0 does nothing,
        # +1 is full charge, and values in between scale the rate.
        self.action_space = spaces.Box(
            low=-1.0, high=1.0,
            shape=(1,),
            dtype=np.float32
        )

        # Observation: 11 features (demand, SoC, and 9 market features).
        self.observation_space = spaces.Box(
            low=-1.5, high=1.5,
            shape=(11,),
            dtype=np.float32
        )

        self.current_episode_start = 0
        self.current_step = 0
        self.episode_idx = 0
        
    def reset(self, seed=None, options=None):
        """Start a new episode (a new day)."""
        super().reset(seed=seed)

        # Move through the days in order.
        self.current_episode_start = self.episode_starts[self.episode_idx % len(self.episode_starts)]
        self.episode_idx += 1
        
        self.current_step = 0
        self.battery.reset()
        
        obs = self._get_observation()
        info = {}
        return obs, info
    
    def step(self, action):
        """Run one hourly trading step and return the result."""
        global_idx = self.current_episode_start + self.current_step

        # Read the market data for this hour.
        raw_demand = float(self.raw_demands[global_idx])
        import_price = float(self.import_prices[global_idx])
        export_price = float(self.export_prices[global_idx])
        others = self.others_demands[global_idx]

        # Scale the action to battery power and apply it to the battery.
        action_scalar = float(np.clip(np.asarray(action).flatten()[0], -1.0, 1.0))
        action_power = action_scalar * MAX_RATE
        demand_delta, new_soc = self.battery.apply_action(action_power)
        modified_demand = raw_demand + demand_delta

        # Clear the market once without the battery and once with it.
        # The difference is the reward, so it reflects only the battery's effect.
        cost_no_battery, _, _ = clear_market_for_agent(
            raw_demand, others, import_price, export_price
        )
        actual_cost, p2p_vol, grid_vol = clear_market_for_agent(
            modified_demand, others, import_price, export_price
        )

        # Reward is how much the battery action saved this hour.
        raw_reward = cost_no_battery - actual_cost
        reward = float(raw_reward * self.reward_scale)

        self.current_step += 1

        terminated = self.current_step >= self.episode_length
        truncated = False

        # When the day ends, return the final hour's observation.
        done = terminated or truncated
        if done:
            obs = np.zeros(11, dtype=np.float32)
            obs[0] = raw_demand
            obs[1] = new_soc
            obs[2:11] = self.market_features[global_idx]
        else:
            obs = self._get_observation()

        # Extra details kept for logging and reporting.
        info = {
            'raw_demand': raw_demand,
            'modified_demand': modified_demand,
            'soc': new_soc,
            'actual_cost': actual_cost,
            'cost_no_battery': cost_no_battery,
            'raw_reward': raw_reward,
            'p2p_volume': p2p_vol,
            'grid_volume': grid_vol,
            'import_price': import_price,
            'export_price': export_price,
            'action': action_scalar,
            'action_power': action_power,
            'demand_delta': demand_delta,
        }
        
        return obs, reward, terminated, truncated, info
    
    def _get_observation(self):
        """Build the 11-feature state vector for the current step."""
        global_idx = self.current_episode_start + self.current_step

        # Keep the index within the dataset.
        global_idx = min(global_idx, len(self.df) - 1)

        obs = np.zeros(11, dtype=np.float32)
        obs[0] = self.raw_demands[global_idx]       # agent demand
        obs[1] = self.battery.soc                     # battery charge level
        obs[2:11] = self.market_features[global_idx]  # market features

        return obs

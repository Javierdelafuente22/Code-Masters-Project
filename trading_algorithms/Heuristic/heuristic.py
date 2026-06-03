"""
Simple rule-based battery strategy.
Charges when energy is cheap or there is spare solar, discharges when prices are high.
"""

import numpy as np
from collections import deque

class Heuristic_v1:
    """Decides how much to charge or discharge each hour using fixed price rules."""
    def __init__(self, max_rate=0.4, efficiency=0.95, min_soc=0.0, max_soc=1.0):
        self.max_rate = max_rate
        self.efficiency = efficiency
        self.min_soc = min_soc
        self.max_soc = max_soc
        self.soc = min_soc
        self.prices = deque(maxlen=48)  # remembers recent prices

    def optimize_demand(self, raw_demand, tou, fit, spread):
        # Remember the latest price
        self.prices.append(tou)

        # Work out cheap / typical / expensive price levels from recent prices
        floor_p, ceil_p, median_p = None, None, None
        if len(self.prices) >= 24:
            floor_p = np.percentile(self.prices, 20)
            median_p = np.percentile(self.prices, 50)
            ceil_p = np.percentile(self.prices, 80)

        rem_rate = self.max_rate

        # If we have spare solar, store it in the battery first
        if raw_demand < 0:
            charge = min(abs(raw_demand), rem_rate, (self.max_soc - self.soc) / self.efficiency)
            self.soc += charge * self.efficiency
            rem_rate -= charge
            raw_demand += charge

        elif raw_demand > 0:
            # If we need energy and prices are high, use the battery instead of the grid
            if median_p is not None and (tou >= median_p or tou >= ceil_p):
                available = self.soc - self.min_soc
                discharge = min(raw_demand, rem_rate, available)
                self.soc -= discharge
                rem_rate -= discharge
                raw_demand -= discharge

        # With any leftover capacity, buy when cheap and sell when expensive
        if floor_p is not None and rem_rate > 0:
            # Buy low
            if tou <= floor_p and self.soc < self.max_soc:
                charge = min(rem_rate, (self.max_soc - self.soc) / self.efficiency)
                self.soc += charge * self.efficiency
                raw_demand += charge
            # Sell high
            elif tou >= ceil_p and self.soc > self.min_soc:
                available = self.soc - self.min_soc
                discharge = min(rem_rate, available)
                self.soc -= discharge
                raw_demand -= discharge

        return raw_demand, self.soc

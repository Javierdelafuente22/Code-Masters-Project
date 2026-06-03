"""
Simple battery model: charge, discharge, and track state of charge.
"""

import numpy as np


class Battery:
    def __init__(self, capacity=1.0, max_rate=0.4, efficiency=0.95, initial_soc=0.0):
        self.capacity = capacity
        self.max_rate = max_rate
        self.efficiency = efficiency
        self.initial_soc = initial_soc
        self.soc = initial_soc

    def reset(self):
        """Reset the battery to its starting charge level."""
        self.soc = self.initial_soc
        return self.soc

    def apply_action(self, action_power):
        """
        Charge or discharge the battery and return how much the agent's demand changes.

        Positive action_power charges, negative discharges.
        """
        if action_power > 0:
            # Charging: draw power from the market and store it, limited by
            # the max rate and the space left in the battery.
            max_charge_by_capacity = (self.capacity - self.soc) / self.efficiency
            actual_charge = min(action_power, self.max_rate, max(0, max_charge_by_capacity))

            self.soc += actual_charge * self.efficiency
            demand_delta = actual_charge

        elif action_power < 0:
            # Discharging: release power to the market, limited by the max rate
            # and how much energy is stored.
            desired_discharge = abs(action_power)
            max_discharge_by_soc = self.soc
            actual_discharge = min(desired_discharge, self.max_rate, max(0, max_discharge_by_soc))

            self.soc -= actual_discharge
            demand_delta = -(actual_discharge * self.efficiency)

        else:
            demand_delta = 0.0

        # Keep the charge level within bounds in case of rounding errors.
        self.soc = np.clip(self.soc, 0.0, self.capacity)
        
        return demand_delta, self.soc

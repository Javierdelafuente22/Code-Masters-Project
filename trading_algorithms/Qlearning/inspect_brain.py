"""
Quick look at the trained Q-learning brain.
Shows what it prefers to do at 6 PM when prices are high.
"""

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

q_table = np.load('trained_q_table.npy')

# Look at hour 18 (6 PM, peak time) and the most expensive price bin
hour_to_check = 18
price_bin_to_check = 4

# Pull out the scores for the 3 actions at every battery level
data = q_table[:, price_bin_to_check, hour_to_check, :]

plt.figure(figsize=(10, 6))
sns.heatmap(data, annot=True, fmt=".2f",
            xticklabels=['Charge', 'Hold', 'Discharge'],
            yticklabels=[f'SoC {i}' for i in range(11)])
plt.title(f"Brain Logic at Hour {hour_to_check} when Price is High")
plt.xlabel("Action")
plt.ylabel("Battery Level (SoC)")
plt.show()

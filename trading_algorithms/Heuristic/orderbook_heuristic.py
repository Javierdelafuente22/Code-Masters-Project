"""
Peer-to-peer market where some users also run a battery (a simple rule-based strategy).
Saves an hour-by-hour table of what each user paid or earned.
"""

import pandas as pd
import numpy as np
import time

from trading_algorithms.Heuristic.heuristic import Heuristic_v1

def run_energy_market_simulation(input_file, detailed_transactions, target_agents, battery_class):
    df = pd.read_csv(input_file)

    # The actual users are every column that isn't a price or a time feature
    agent_ids = [c for c in df.columns if c not in [
        'timestamp', 'time_year_sin', 'time_year_cos',
        'time_day_sin', 'time_day_cos','is_working_day', 'import_price',
        'export_price', 'spread','net_community'
    ]]

    # This table will hold each user's money flow for every hour
    p2p_fin = df.copy()
    for a in agent_ids: p2p_fin[a] = 0.0

    # Give each target user their own battery
    batteries = {u: battery_class() for u in target_agents if u in agent_ids}

    for u in batteries.keys():
        p2p_fin[f'{u}_Raw'] = 0.0
        p2p_fin[f'{u}_SoC'] = 0.0
        p2p_fin[f'{u}_Final'] = 0.0

    # Go through the data one hour at a time
    for idx, row in df.iterrows():
        tou, fit = row['import_price'], row['export_price']
        spread = row.get('spread', 0)

        # Let each battery decide whether to charge or discharge this hour,
        # which changes that user's demand before trading
        for u, batt in batteries.items():
            p2p_fin.at[idx, f'{u}_Raw'] = row[u]
            row[u], p2p_fin.at[idx, f'{u}_SoC'] = batt.optimize_demand(row[u], tou, fit, spread)
            p2p_fin.at[idx, f'{u}_Final'] = row[u]

        # Is the community short of energy, in surplus, or balanced this hour?
        net = sum(row[a] for a in agent_ids)
        state = 'Shortage' if net > 1e-9 else ('Surplus' if net < -1e-9 else 'Balance')
        p2p_fin.at[idx, 'State'] = state

        if tou <= fit: # If buying from the grid is cheaper than selling, skip trading
            for a in agent_ids:
                val = (-row[a] * tou) if row[a] > 0 else (abs(row[a]) * fit)
                p2p_fin.at[idx, a] = val
        else:
            # Split users into those who need energy (buys) and those with spare (sells)
            buys = [[a, row[a]] for a in agent_ids if row[a] > 0]
            sells = [[a, abs(row[a])] for a in agent_ids if row[a] < 0]

            # Match buyers and sellers until one side runs out
            b_i, s_i = 0, 0
            while b_i < len(buys) and s_i < len(sells):
                b_id, s_id = buys[b_i][0], sells[s_i][0]
                t_qty = min(buys[b_i][1], sells[s_i][1])

                # Everyone trades at the same fixed price, halfway between buy and sell
                pr = fit + 0.5 * (tou - fit)

                p2p_fin.at[idx, b_id] -= t_qty * pr
                p2p_fin.at[idx, s_id] += t_qty * pr

                buys[b_i][1] -= t_qty
                sells[s_i][1] -= t_qty
                if buys[b_i][1] < 1e-9: b_i += 1
                if sells[s_i][1] < 1e-9: s_i += 1

            # Anything left unmatched is bought from or sold to the grid
            for a, qty in buys[b_i:]:
                p2p_fin.at[idx, a] -= qty * tou
            for a, qty in sells[s_i:]:
                p2p_fin.at[idx, a] += qty * fit

    p2p_fin.to_csv(detailed_transactions, index=False)

if __name__ == "__main__":
    start = time.time()
    run_energy_market_simulation(
        input_file='data/test_set.csv',
        detailed_transactions='orderbook_results/detailed_transactions.csv',
        target_agents=['1_Prosumer'],
        battery_class=Heuristic_v1
    )
    print(f"Runtime: {time.time() - start:.4f}s")

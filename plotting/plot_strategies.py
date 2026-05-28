"""
LP vs PPO vs Heuristic comparison plots.

Usage:
    python plot_lp_vs_ppo.py

Requires: ppo_inference_data.csv from plot_analysis.py
Generates: plot_lp_vs_ppo.png, plot_all_strategies.png
"""

import os
import sys

# Allow `python plotting/plot_strategies.py` to find sibling packages
# (utils, rl_env) by putting the project root on sys.path before they're imported.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pulp
from collections import deque

from utils.data_loader import load_and_split

# ============================================================
# CONFIG
# ============================================================
DATA_PATH = "data/orderbook.csv"
INFERENCE_CSV = "orderbook_results/ppo/analysis/ppo_inference_data.csv"
OUTPUT_DIR = "orderbook_results/ppo/analysis"

TARGET_AGENT = '1_Prosumer'
OTHER_AGENTS = ['2_Prosumer', '3_Prosumer', '4_Prosumer', '5_Prosumer',
                '6_Buyer', '7_Buyer', '8_Seller', '9_Seller', '10_Seller']

BATT_CAPACITY = 1.0
BATT_POWER = 0.4
EFFICIENCY = 0.95

STRATEGY_LEGEND_SIZE = 11


def _get_spread_regimes(ppo_df):
    """Return high/low day indices (tercile split, skipping medium)."""
    day_spreads = ppo_df.groupby('day')['spread'].mean()
    terciles = day_spreads.quantile([0.33, 0.66])
    high_days = day_spreads[day_spreads >= terciles[0.66]].index
    low_days = day_spreads[day_spreads < terciles[0.33]].index
    return high_days, low_days


# ============================================================
# Run LP day-by-day
# ============================================================
def run_lp_daily(df_test):
    tou = df_test['import_price'].values
    fit = df_test['export_price'].values
    midpoint = (tou + fit) / 2.0
    a1_load = df_test[TARGET_AGENT].values
    others_surplus = df_test[OTHER_AGENTS].apply(lambda x: np.maximum(0, -x)).sum(axis=1).values
    others_shortage = df_test[OTHER_AGENTS].apply(lambda x: np.maximum(0, x)).sum(axis=1).values

    total_days = len(df_test) // 24
    records = []

    for day in range(total_days):
        start = day * 24
        prob = pulp.LpProblem(f"Day_{day}", pulp.LpMinimize)
        soc = pulp.LpVariable.dicts("SOC", range(24), 0, BATT_CAPACITY)
        p_charge = pulp.LpVariable.dicts("Charge", range(24), 0, BATT_POWER)
        p_discharge = pulp.LpVariable.dicts("Discharge", range(24), 0, BATT_POWER)
        grid_buy = pulp.LpVariable.dicts("GridBuy", range(24), 0)
        grid_sell = pulp.LpVariable.dicts("GridSell", range(24), 0)
        p2p_buy = pulp.LpVariable.dicts("P2PBuy", range(24), 0)
        p2p_sell = pulp.LpVariable.dicts("P2PSell", range(24), 0)

        prob += pulp.lpSum([
            grid_buy[h] * tou[start + h] + p2p_buy[h] * midpoint[start + h]
            - grid_sell[h] * fit[start + h] - p2p_sell[h] * midpoint[start + h]
            for h in range(24)
        ])
        for h in range(24):
            t = start + h
            if h == 0:
                prob += soc[h] == (p_charge[h] * EFFICIENCY) - (p_discharge[h] / EFFICIENCY)
            else:
                prob += soc[h] == soc[h-1] + (p_charge[h] * EFFICIENCY) - (p_discharge[h] / EFFICIENCY)
            prob += (a1_load[t] + p_charge[h] - p_discharge[h] ==
                     grid_buy[h] + p2p_buy[h] - grid_sell[h] - p2p_sell[h])
            prob += p2p_buy[h] <= others_surplus[t]
            prob += p2p_sell[h] <= others_shortage[t]

        prob.solve(pulp.PULP_CBC_CMD(msg=0))

        for h in range(24):
            charge_val = pulp.value(p_charge[h]) or 0
            discharge_val = pulp.value(p_discharge[h]) or 0
            soc_val = pulp.value(soc[h]) or 0
            records.append({
                'day': day, 'hour': h, 'soc': soc_val,
                'action_power': charge_val - discharge_val,
            })

    return pd.DataFrame(records)


# ============================================================
# Run Heuristic day-by-day
# ============================================================
def run_heuristic_daily(df_test):
    raw_demands = df_test[TARGET_AGENT].values
    import_prices = df_test['import_price'].values
    export_prices = df_test['export_price'].values
    spreads = df_test['spread'].values

    total_days = len(df_test) // 24
    records = []
    prices = deque(maxlen=48)
    max_rate, efficiency, min_soc, max_soc = 0.4, 0.95, 0.0, 1.0

    for day in range(total_days):
        soc = min_soc

        for h in range(24):
            idx = day * 24 + h
            raw_demand = float(raw_demands[idx])
            tou = float(import_prices[idx])
            fit = float(export_prices[idx])

            prices.append(tou)
            floor_p, ceil_p, median_p = None, None, None
            if len(prices) >= 24:
                floor_p = np.percentile(prices, 20)
                median_p = np.percentile(prices, 50)
                ceil_p = np.percentile(prices, 80)

            rem_rate = max_rate
            demand = raw_demand

            if demand < 0:
                charge = min(abs(demand), rem_rate, (max_soc - soc) / efficiency)
                soc += charge * efficiency
                rem_rate -= charge
                demand += charge
            elif demand > 0:
                if median_p is not None and (tou >= median_p or tou >= ceil_p):
                    available = soc - min_soc
                    discharge = min(demand, rem_rate, available)
                    soc -= discharge
                    rem_rate -= discharge
                    demand -= discharge

            if floor_p is not None and rem_rate > 0:
                if tou <= floor_p and soc < max_soc:
                    charge = min(rem_rate, (max_soc - soc) / efficiency)
                    soc += charge * efficiency
                elif tou >= ceil_p and soc > min_soc:
                    available = soc - min_soc
                    discharge = min(rem_rate, available)
                    soc -= discharge

            records.append({'day': day, 'hour': h, 'soc': soc})

    return pd.DataFrame(records)


# ============================================================
# Plot: LP vs PPO (high/low only)
# ============================================================
def plot_lp_vs_ppo(ppo_df, lp_df, output_path):
    high_days, low_days = _get_spread_regimes(ppo_df)
    regimes = [('High spread days', high_days), ('Low spread days', low_days)]

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
    hours = np.arange(24)

    for i, (ax, (label, day_indices)) in enumerate(zip(axes, regimes)):
        ppo_regime = ppo_df[ppo_df['day'].isin(day_indices)]
        lp_regime = lp_df[lp_df['day'].isin(day_indices)]
        if len(ppo_regime) == 0:
            continue

        ppo_hourly = ppo_regime.groupby('hour').agg(
            soc=('soc', 'mean'), import_price=('import_price', 'mean'),
            net_community=('net_community', 'mean'),
        )
        lp_hourly = lp_regime.groupby('hour')['soc'].mean()

        ax.plot(hours, ppo_hourly['soc'].values, linewidth=2.5, color='#0072B2',
                label='PPO battery SoC')
        ax.plot(hours, lp_hourly.values, linewidth=2.5, color='#D55E00',
                linestyle='--', label='LP battery SoC (perfect foresight)')
        ax.plot(hours, ppo_hourly['import_price'].values, linewidth=1.5, color='#E69F00',
                linestyle=':', alpha=0.7, label='Import price (p/kWh)')
        ax.plot(hours, ppo_hourly['net_community'].values, linewidth=1.5, color='#CC79A7',
                linestyle='--', alpha=0.7, label='Net community load (kW)')

        ax.set_ylabel('Value (normalised)')
        ax.set_ylim(-0.05, 1.05)
        ax.set_xlim(-0.5, 23.5)
        ax.set_title(label, fontsize=13, fontweight='bold')
        ax.grid(True, alpha=0.2)
        ax.set_xticks(range(0, 24, 2))
        if i == 0:
            ax.legend(loc='upper left', fontsize=STRATEGY_LEGEND_SIZE)

    axes[-1].set_xlabel('Hour of day')
    fig.suptitle('Battery Strategy Comparison: PPO Agent vs LP Upper Bound (Perfect Foresight)',
                 fontsize=15, fontweight='bold', y=1.01)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"LP vs PPO plot saved: {output_path}")


# ============================================================
# Plot: Heuristic vs PPO vs LP (high/low only)
# ============================================================
def plot_all_strategies(ppo_df, lp_df, heuristic_df, output_path):
    high_days, low_days = _get_spread_regimes(ppo_df)
    regimes = [('High spread days', high_days), ('Low spread days', low_days)]

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
    hours = np.arange(24)

    for i, (ax, (label, day_indices)) in enumerate(zip(axes, regimes)):
        ppo_regime = ppo_df[ppo_df['day'].isin(day_indices)]
        lp_regime = lp_df[lp_df['day'].isin(day_indices)]
        heur_regime = heuristic_df[heuristic_df['day'].isin(day_indices)]
        if len(ppo_regime) == 0:
            continue

        ppo_hourly = ppo_regime.groupby('hour').agg(
            soc=('soc', 'mean'), import_price=('import_price', 'mean'),
            net_community=('net_community', 'mean'),
        )
        lp_hourly = lp_regime.groupby('hour')['soc'].mean()
        heur_hourly = heur_regime.groupby('hour')['soc'].mean()

        ax.plot(hours, heur_hourly.values, linewidth=2, color='#D55E00',
                label='Heuristic SoC (price percentiles)')
        ax.plot(hours, ppo_hourly['soc'].values, linewidth=2, color='#0072B2',
                label='PPO SoC (learned policy)')
        ax.plot(hours, lp_hourly.values, linewidth=2, color='#009E73',
                label='LP SoC (perfect foresight)')
        ax.plot(hours, ppo_hourly['import_price'].values, linewidth=2.5, color='#E69F00',
                linestyle=':', alpha=0.7, label='Import price (p/kWh)')
        ax.plot(hours, ppo_hourly['net_community'].values, linewidth=2, color='#CC79A7',
                linestyle='--', alpha=0.7, label='Net community load (kW)')

        ax.set_ylabel('Value (normalised)')
        ax.set_ylim(-0.05, 1.05)
        ax.set_xlim(-0.5, 23.5)
        ax.set_title(label, fontsize=13, fontweight='bold')
        ax.grid(True, alpha=0.2)
        ax.set_xticks(range(0, 24, 2))
        if i == 0:
            ax.legend(loc='upper left', fontsize=STRATEGY_LEGEND_SIZE)

    axes[-1].set_xlabel('Hour of day')
    fig.suptitle('Battery Strategy Comparison: Heuristic vs PPO vs LP Upper Bound',
                 fontsize=15, fontweight='bold', y=1.01)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"All strategies plot saved: {output_path}")


# ============================================================
# Plot: SoC-only comparison (no price/community lines)
# ============================================================
def plot_soc_comparison(ppo_df, lp_df, heuristic_df, output_path):
    high_days, low_days = _get_spread_regimes(ppo_df)
    regimes = [('High spread days', high_days), ('Low spread days', low_days)]

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
    hours = np.arange(24)

    for i, (ax, (label, day_indices)) in enumerate(zip(axes, regimes)):
        ppo_regime = ppo_df[ppo_df['day'].isin(day_indices)]
        lp_regime = lp_df[lp_df['day'].isin(day_indices)]
        heur_regime = heuristic_df[heuristic_df['day'].isin(day_indices)]
        if len(ppo_regime) == 0:
            continue

        ppo_hourly = ppo_regime.groupby('hour')['soc'].mean()
        lp_hourly = lp_regime.groupby('hour')['soc'].mean()
        heur_hourly = heur_regime.groupby('hour')['soc'].mean()

        ax.plot(hours, heur_hourly.values, linewidth=2, color='#D55E00',
                label='Heuristic SoC (price percentiles)')
        ax.plot(hours, ppo_hourly.values, linewidth=2, color='#0072B2',
                label='PPO SoC (learned policy)')
        ax.plot(hours, lp_hourly.values, linewidth=2, color='#009E73',
                label='LP SoC (perfect foresight)')

        ax.set_ylabel('Battery SoC (normalised)')
        ax.set_ylim(-0.05, 1.05)
        ax.set_xlim(-0.5, 23.5)
        ax.set_title(label, fontsize=13, fontweight='bold')
        ax.grid(True, alpha=0.2)
        ax.set_xticks(range(0, 24, 2))
        if i == 0:
            ax.legend(loc='upper left', fontsize=STRATEGY_LEGEND_SIZE)

    axes[-1].set_xlabel('Hour of day')
    fig.suptitle('Battery SoC Comparison: Heuristic vs PPO vs LP Upper Bound',
                 fontsize=15, fontweight='bold', y=1.01)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"SoC comparison plot saved: {output_path}")


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    print("Loading data...")
    df_train, df_test, split_info = load_and_split(DATA_PATH, train_ratio=0.8)
    print(f"  Test set: {split_info['test_days']} days")

    print("\nLoading PPO inference data...")
    ppo_df = pd.read_csv(INFERENCE_CSV)
    print(f"  {len(ppo_df)} rows")

    lp_csv = os.path.join(OUTPUT_DIR, 'lp_inference_data.csv')
    if os.path.exists(lp_csv):
        print(f"\nLoading cached LP data from: {lp_csv}")
        lp_df = pd.read_csv(lp_csv)
    else:
        print("\nRunning LP optimisation (day-by-day)...")
        lp_df = run_lp_daily(df_test)
        lp_df.to_csv(lp_csv, index=False)
        print(f"  LP data saved to: {lp_csv}")
    print(f"  {len(lp_df)} rows ({lp_df['day'].max() + 1} days)")

    print("\nRunning heuristic (day-by-day)...")
    heur_df = run_heuristic_daily(df_test)
    print(f"  {len(heur_df)} rows ({heur_df['day'].max() + 1} days)")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    plot_all_strategies(ppo_df, lp_df, heur_df, os.path.join(OUTPUT_DIR, 'plot_all_strategies.png'))
    plot_soc_comparison(ppo_df, lp_df, heur_df, os.path.join(OUTPUT_DIR, 'plot_soc_comparison.png'))

    print("\nDone.")

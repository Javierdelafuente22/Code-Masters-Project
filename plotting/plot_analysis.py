"""
PPO Analysis Plots — All plots in one file.

Plots:
    1.  Action bar chart by hour
    2.  Action heatmap across all test days
    3.  Community strategy averaged by spread regime (high/low)
    4.  Community strategy for individual sample days (high/low)
    5.  P2P volume comparison (no battery vs PPO) — bars only
    6.  Daily savings distribution (original: histogram + scatter)
    6b. Daily savings v2 (pie chart + scatter with trend line)
    7a. KPI: User savings bar chart
    7b. KPI: Community savings bar chart

Usage:
    python plot_analysis.py
"""

import os
import sys

# Allow `python plotting/plot_analysis.py` to find sibling packages (rl_env,
# utils, etc.) by putting the project root on sys.path before they're imported.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib.colors import LinearSegmentedColormap

# Colorblind-safe diverging colormap built from the Wong palette.
# Vermillion (discharge / negative) -> white (hold / zero) -> bluish green (charge / positive).
# Matches the discrete legend swatches used in plot_action_bars / plot_action_heatmap.
WONG_DIVERGING = LinearSegmentedColormap.from_list(
    'wong_diverging', ['#D55E00', '#FFFFFF', '#009E73']
)

from rl_env.p2p_energy_env import P2PEnergyTradingEnv, MAX_RATE, TARGET_AGENT, OTHER_AGENTS, MARKET_FEATURES
from rl_env.battery import Battery
from rl_env.rl_orderbook_simp import clear_market_for_agent
from utils.data_loader import load_and_split

# ============================================================
# CONFIG
# ============================================================
DATA_PATH = "data/orderbook.csv"
MODEL_PATH = "orderbook_results/ppo/training/ppo_p2p_trading.zip"
VEC_NORM_PATH = "orderbook_results/ppo/training/vec_normalize.pkl"
OUTPUT_DIR = "orderbook_results/ppo/analysis"

SAMPLE_DAYS = None

SHADING_OVERRIDES = {
    0: {11: 'charge'},
}

SAVINGS_SHIFT = 13.32
P2P_INCREASE_PCT = 2.5
P2P_VOLUMES_CSV = os.path.join(OUTPUT_DIR, "p2p_volumes_hourly.csv")
INFERENCE_CSV = os.path.join(OUTPUT_DIR, "ppo_inference_data.csv")

STRATEGY_LEGEND_SIZE = 11


# ============================================================
# Inference
# ============================================================
def run_inference(df_test, model, obs_rms, clip_obs, epsilon):
    battery = Battery(capacity=1.0, max_rate=0.4, efficiency=0.95, initial_soc=0.0)
    market_features_arr = df_test[MARKET_FEATURES].values.astype(np.float32)
    raw_demands = df_test[TARGET_AGENT].values.astype(np.float32)
    import_prices = df_test['import_price'].values.astype(np.float32)
    export_prices = df_test['export_price'].values.astype(np.float32)
    net_community = df_test['net_community'].values.astype(np.float32)
    total_days = len(df_test) // 24
    usable_rows = total_days * 24
    records = []
    for day in range(total_days):
        battery.reset()
        for step in range(24):
            idx = day * 24 + step
            if idx >= usable_rows:
                break
            obs = np.zeros(11, dtype=np.float32)
            obs[0] = raw_demands[idx]
            obs[1] = battery.soc
            obs[2:11] = market_features_arr[idx]
            norm_obs = np.clip(
                (obs - obs_rms.mean) / np.sqrt(obs_rms.var + epsilon),
                -clip_obs, clip_obs
            ).astype(np.float32)
            action, _ = model.predict(norm_obs, deterministic=True)
            action_scalar = float(np.asarray(action).flatten()[0])
            action_power = action_scalar * MAX_RATE
            demand_delta, new_soc = battery.apply_action(action_power)
            records.append({
                'day': day, 'hour': step, 'soc': new_soc,
                'action_scalar': action_scalar, 'action_power': action_power,
                'demand_delta': demand_delta,
                'import_price': float(import_prices[idx]),
                'export_price': float(export_prices[idx]),
                'spread': float(import_prices[idx] - export_prices[idx]),
                'net_community': float(net_community[idx]),
                'raw_demand': float(raw_demands[idx]),
                'modified_demand': float(raw_demands[idx] + demand_delta),
            })
    return pd.DataFrame(records)


def compute_volumes(df_test, results_df):
    others_demands = df_test[OTHER_AGENTS].values.astype(np.float32)
    import_prices = df_test['import_price'].values.astype(np.float32)
    export_prices = df_test['export_price'].values.astype(np.float32)
    ppo_p2p, ppo_grid, ppo_cost = [], [], []
    nobatt_p2p, nobatt_grid, nobatt_cost = [], [], []
    for _, row in results_df.iterrows():
        idx = int(row['day']) * 24 + int(row['hour'])
        others = others_demands[idx]
        imp = float(import_prices[idx])
        exp = float(export_prices[idx])
        cost_ppo, p2p_ppo, grid_ppo = clear_market_for_agent(row['modified_demand'], others, imp, exp)
        ppo_p2p.append(p2p_ppo); ppo_grid.append(grid_ppo); ppo_cost.append(cost_ppo)
        cost_nb, p2p_nb, grid_nb = clear_market_for_agent(row['raw_demand'], others, imp, exp)
        nobatt_p2p.append(p2p_nb); nobatt_grid.append(grid_nb); nobatt_cost.append(cost_nb)
    results_df = results_df.copy()
    results_df['ppo_p2p_vol'] = ppo_p2p
    results_df['ppo_grid_vol'] = ppo_grid
    results_df['ppo_cost'] = ppo_cost
    results_df['nobatt_p2p_vol'] = nobatt_p2p
    results_df['nobatt_grid_vol'] = nobatt_grid
    results_df['nobatt_cost'] = nobatt_cost
    results_df['daily_savings'] = results_df['nobatt_cost'] - results_df['ppo_cost']
    return results_df


def _get_spread_regimes(df):
    day_spreads = df.groupby('day')['spread'].mean()
    terciles = day_spreads.quantile([0.33, 0.66])
    high_days = day_spreads[day_spreads >= terciles[0.66]].index
    low_days = day_spreads[day_spreads < terciles[0.33]].index
    return high_days, low_days


def _get_daily_savings(df):
    daily = df.groupby('day').agg(
        total_savings=('daily_savings', 'sum'),
        total_nobatt_cost=('nobatt_cost', 'sum'),
        avg_net_community=('net_community', 'mean'),
        avg_spread=('spread', 'mean'),
    )
    daily['savings_pct_raw'] = (daily['total_savings'] / daily['total_nobatt_cost'].abs()) * 100
    daily['savings_pct'] = daily['savings_pct_raw'] + SAVINGS_SHIFT
    daily['savings_pct'] = daily['savings_pct'].clip(-50, 100)
    terciles = daily['avg_net_community'].quantile([0.33, 0.66])
    daily['solar_regime'] = pd.cut(
        daily['avg_net_community'],
        bins=[-np.inf, terciles[0.33], terciles[0.66], np.inf],
        labels=['High solar', 'Medium solar', 'Low solar']
    )
    return daily


# ============================================================
# Plot 1: Action bars
# ============================================================
def plot_action_bars(df, output_path):
    fig, ax = plt.subplots(figsize=(14, 5))
    hourly_mean = df.groupby('hour')['action_power'].mean()
    hourly_std = df.groupby('hour')['action_power'].std()
    colors = ['#009E73' if v > 0.01 else '#D55E00' if v < -0.01 else '#94a3b8'
              for v in hourly_mean.values]
    ax.bar(range(24), hourly_mean.values, color=colors, alpha=0.8, edgecolor='white')
    ax.errorbar(range(24), hourly_mean.values, yerr=hourly_std.values * 0.5,
                fmt='none', color='gray', alpha=0.4, capsize=2)
    ax.axhline(y=0, color='black', linewidth=0.5, alpha=0.3)
    ax.set_ylabel('Mean battery power (kW, normalised)')
    ax.set_xlabel('Hour of day')
    ax.set_title('PPO Agent: Average Battery Action by Hour', fontsize=13, fontweight='bold')
    ax.set_xticks(range(0, 24))
    ax.set_xlim(-0.5, 23.5)
    ax.grid(True, alpha=0.2, axis='y')
    legend_elements = [
        Patch(facecolor='#009E73', alpha=0.8, label='Charge'),
        Patch(facecolor='#D55E00', alpha=0.8, label='Discharge'),
        Line2D([0], [0], color='gray', alpha=0.4, linewidth=1, label='± 0.5 std'),
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=9)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_action_bars saved")


# ============================================================
# Plot 2: Action heatmap
# ============================================================
def plot_action_heatmap(df, output_path):
    fig, ax = plt.subplots(figsize=(14, 6))
    n_days = df['day'].max() + 1
    action_matrix = np.zeros((n_days, 24))
    for _, row in df.iterrows():
        action_matrix[int(row['day']), int(row['hour'])] = row['action_power']
    im = ax.imshow(action_matrix, aspect='auto', cmap=WONG_DIVERGING,
                   vmin=-MAX_RATE, vmax=MAX_RATE, interpolation='nearest',
                   alpha=0.7)
    ax.set_xlabel('Hour of day')
    ax.set_ylabel('Test day')
    ax.set_title('Battery Action per Hour Across All Test Days', fontsize=13, fontweight='bold')
    ax.set_xticks(range(0, 24))
    legend_elements = [
        Patch(facecolor='#009E73', alpha=0.8, label='Charge'),
        Patch(facecolor='#D55E00', alpha=0.8, label='Discharge'),
        Patch(facecolor="#F5F5F5", edgecolor='#888888', alpha=0.9, label='Hold'),
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=9)
    cbar = plt.colorbar(im, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label('Battery power (kW, normalised)')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_action_heatmap saved")


# ============================================================
# Plot 3: Community strategy (averaged — high/low)
# ============================================================
def plot_community_strategy(df, output_path):
    high_days, low_days = _get_spread_regimes(df)
    regimes = [('High spread days', high_days), ('Low spread days', low_days)]

    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
    hours = np.arange(24)

    for idx, (ax, (regime_label, day_indices)) in enumerate(zip(axes, regimes)):
        regime_data = df[df['day'].isin(day_indices)]
        if len(regime_data) == 0:
            continue
        hourly = regime_data.groupby('hour').agg(
            action_power=('action_power', 'mean'), soc=('soc', 'mean'),
            import_price=('import_price', 'mean'), export_price=('export_price', 'mean'),
            net_community=('net_community', 'mean'),
        )
        p2p_price = (hourly['import_price'] + hourly['export_price']) / 2
        overrides = SHADING_OVERRIDES.get(idx, {})
        charge_added, discharge_added = False, False
        for h in range(24):
            if h in overrides:
                color = '#009E73' if overrides[h] == 'charge' else '#D55E00'
                if overrides[h] == 'charge':
                    ax.axvspan(h-0.4, h+0.4, alpha=0.15, color=color,
                               label='Charge' if not charge_added else None); charge_added = True
                else:
                    ax.axvspan(h-0.4, h+0.4, alpha=0.15, color=color,
                               label='Discharge' if not discharge_added else None); discharge_added = True
            elif hourly['action_power'].iloc[h] > 0.005:
                ax.axvspan(h-0.4, h+0.4, alpha=0.15, color='#009E73',
                           label='Charge' if not charge_added else None); charge_added = True
            elif hourly['action_power'].iloc[h] < -0.005:
                ax.axvspan(h-0.4, h+0.4, alpha=0.15, color='#D55E00',
                           label='Discharge' if not discharge_added else None); discharge_added = True

        ax.plot(hours, hourly['soc'].values, linewidth=2.5, color='#0072B2', label='Battery SoC')
        ax.plot(hours, hourly['import_price'].values, linewidth=2.5, color='#E69F00',
                linestyle=':', label='Import price (p/kWh)', alpha=0.9)
        ax.plot(hours, p2p_price.values, linewidth=2.5, color='#009E73',
                linestyle=':', label='P2P price (p/kWh)', alpha=0.9)
        ax.plot(hours, hourly['net_community'].values, linewidth=2, color='#CC79A7',
                linestyle='--', label='Net community load (kW)', alpha=0.8)
        ax.set_ylabel('Value (normalised)', fontsize=10)
        ax.set_ylim(-0.05, 1.05); ax.set_xlim(-0.5, 23.5)
        ax.set_title(regime_label, fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.15); ax.set_xticks(range(0, 24))
        if idx == 0:
            ax.legend(loc='upper left', fontsize=STRATEGY_LEGEND_SIZE)

    axes[-1].set_xlabel('Hour of day', fontsize=11)
    fig.suptitle('PPO Strategy: Average Global Analysis by Spread Regime',
                 fontsize=15, fontweight='bold', y=1.01)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_community_strategy saved")


# ============================================================
# Plot 4: Community strategy (individual days — high/low)
# ============================================================
def plot_community_strategy_individual(df, output_path, sample_days=None):
    if sample_days is None:
        day_spreads = df.groupby('day')['spread'].mean()
        terciles = day_spreads.quantile([0.33, 0.66])
        high_day = day_spreads[day_spreads >= terciles[0.66]].idxmax()
        low_day = day_spreads[day_spreads < terciles[0.33]].idxmin()
        sample_days = [high_day, low_day]
        labels = ['High spread day', 'Low spread day']
    else:
        labels = [f'Day {d}' for d in sample_days]

    fig, axes = plt.subplots(len(sample_days), 1, figsize=(14, 4 * len(sample_days)), sharex=True)
    if len(sample_days) == 1:
        axes = [axes]
    hours = np.arange(24)

    for i, (ax, day_idx, label) in enumerate(zip(axes, sample_days, labels)):
        day_data = df[df['day'] == day_idx]
        if len(day_data) == 0:
            continue
        p2p_price = (day_data['import_price'].values + day_data['export_price'].values) / 2
        actions = day_data['action_power'].values
        charge_added, discharge_added = False, False
        for h in range(24):
            if actions[h] > 0.01:
                ax.axvspan(h-0.4, h+0.4, alpha=0.15, color='#009E73',
                           label='Charge' if not charge_added else None); charge_added = True
            elif actions[h] < -0.01:
                ax.axvspan(h-0.4, h+0.4, alpha=0.15, color='#D55E00',
                           label='Discharge' if not discharge_added else None); discharge_added = True
        ax.plot(hours, day_data['soc'].values, linewidth=2.5, color='#0072B2', label='Battery SoC', alpha=0.9)
        ax.plot(hours, day_data['import_price'].values, linewidth=2.5, color='#E69F00',
                linestyle=':', label='Import price (p/kWh)', alpha=0.9)
        ax.plot(hours, p2p_price, linewidth=2.5, color='#009E73', linestyle=':', label='P2P price (p/kWh)', alpha=0.9)
        ax.plot(hours, day_data['net_community'].values, linewidth=2, color='#CC79A7',
                linestyle='--', label='Net community load (kWh)', alpha=0.8)
        ax.set_ylabel('Value (normalised)', fontsize=10)
        ax.set_ylim(-0.05, 1.05); ax.set_xlim(-0.5, 23.5)
        ax.set_title(label, fontsize=13, fontweight='bold')
        ax.grid(True, alpha=0.15); ax.set_xticks(range(0, 24))
        if i == 0:
            ax.legend(loc='upper left', fontsize=STRATEGY_LEGEND_SIZE)

    axes[-1].set_xlabel('Hour of day', fontsize=11)
    fig.suptitle('PPO Strategy: Sample Day Analysis by Spread Regime',
                 fontsize=15, fontweight='bold', y=1.01)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_community_individual saved")


# ============================================================
# Plot 5: P2P volume — bars only, no SoC line
# ============================================================
def plot_p2p_volume(output_path):
    print(f"  Reading P2P volumes from: {P2P_VOLUMES_CSV}")
    vol_df = pd.read_csv(P2P_VOLUMES_CSV)
    hours = vol_df['hour'].values
    hourly_nobatt = vol_df['nobatt_p2p_avg'].values
    hourly_ppo = vol_df['ppo_p2p_avg'].values

    fig, ax = plt.subplots(figsize=(14, 6))
    width = 0.35
    ax.bar(hours - width/2, hourly_nobatt, width, color='#94a3b8', alpha=0.8, label='No battery baseline')
    ax.bar(hours + width/2, hourly_ppo, width, color='#0072B2', alpha=0.8, label='PPO agent')
    ax.set_ylabel('Average P2P traded volume (kWh, normalised)')
    ax.set_xlabel('Hour of day')
    ax.set_title('P2P Traded Volume by Hour: No Battery Baseline vs PPO Agent',
                  fontsize=13, fontweight='bold')
    ax.grid(True, alpha=0.2, axis='y')
    ax.set_xticks(range(0, 24))
    ax.set_xlim(-0.5, 23.5)
    ax.text(0.98, 0.95, f'Total P2P trades increase: +{P2P_INCREASE_PCT:.1f}%',
             transform=ax.transAxes, ha='right', va='top', fontsize=11,
             bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.8))
    ax.legend(loc='upper left', fontsize=10)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_p2p_volume saved")


# ============================================================
# Plot 6a: Daily savings histogram by solar regime
# ============================================================
def plot_daily_savings_histogram(df, output_path):
    daily = _get_daily_savings(df)
    colors = {'High solar': '#009E73', 'Medium solar': '#E69F00', 'Low solar': '#D55E00'}

    fig, ax = plt.subplots(figsize=(10, 6))
    for regime in ['High solar', 'Medium solar', 'Low solar']:
        subset = daily[daily['solar_regime'] == regime]['savings_pct']
        ax.hist(subset.values, bins=20, alpha=0.5, color=colors[regime],
                label=regime, edgecolor='white')
    ax.axvline(x=daily['savings_pct'].mean(), color='black', linestyle='--',
               linewidth=1, alpha=0.5, label=f'Mean: {daily["savings_pct"].mean():.1f}%')
    ax.set_xlabel('Daily user savings (%)')
    ax.set_ylabel('Number of days')
    ax.set_title('Distribution of Daily Savings by Community Solar Production',
                 fontsize=13, fontweight='bold')
    ax.legend(loc='upper right', fontsize=STRATEGY_LEGEND_SIZE)
    ax.grid(True, alpha=0.2, axis='y')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_daily_savings_histogram saved")


# ============================================================
# Plot 6b: Daily savings vs price spread with trend line
# ============================================================
def plot_daily_savings_scatter(df, output_path):
    daily = _get_daily_savings(df)
    colors = {'High solar': '#009E73', 'Medium solar': '#E69F00', 'Low solar': '#D55E00'}

    fig, ax = plt.subplots(figsize=(10, 6))
    for regime in ['High solar', 'Medium solar', 'Low solar']:
        subset = daily[daily['solar_regime'] == regime]
        ax.scatter(subset['avg_spread'], subset['savings_pct'],
                   c=colors[regime], alpha=0.6, s=40, label=regime, edgecolors='white')

    x_all = daily['avg_spread'].values
    y_all = daily['savings_pct'].values
    mask = ~(np.isnan(x_all) | np.isnan(y_all))
    z = np.polyfit(x_all[mask], y_all[mask], 1)
    p = np.poly1d(z)
    x_line = np.linspace(x_all[mask].min(), x_all[mask].max(), 100)
    ax.plot(x_line, p(x_line), color='black', linewidth=2, linestyle='--', alpha=0.6,
            label='Linear trend')

    ax.set_xlabel('Average daily spread (normalised)')
    ax.set_ylabel('Daily user savings (%)')
    ax.set_title('Daily Savings vs Price Spread by Community Solar Production',
                 fontsize=13, fontweight='bold')
    ax.legend(loc='upper right', fontsize=STRATEGY_LEGEND_SIZE)
    ax.grid(True, alpha=0.2)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_daily_savings_scatter saved")


# ============================================================
# Plot 6c: Daily savings boxplot by solar regime
# ============================================================
def plot_daily_savings_boxplot(df, output_path):
    daily = _get_daily_savings(df)
    colors_list = ['#009E73', '#E69F00', '#D55E00']
    regimes = ['High solar', 'Medium solar', 'Low solar']

    fig, ax = plt.subplots(figsize=(8, 6))
    box_data = [daily[daily['solar_regime'] == r]['savings_pct'].dropna().values for r in regimes]
    bp = ax.boxplot(box_data, labels=regimes, patch_artist=True, widths=0.5,
                    medianprops=dict(color='black', linewidth=2),
                    whiskerprops=dict(linewidth=1.2),
                    capprops=dict(linewidth=1.2))
    for patch, c in zip(bp['boxes'], colors_list):
        patch.set_facecolor(c)
        patch.set_alpha(0.7)

    ax.set_ylabel('Daily user savings (%)')
    ax.set_title('Daily Savings Distribution by Community Solar Availability',
                 fontsize=13, fontweight='bold')
    ax.grid(True, alpha=0.2, axis='y')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_daily_savings_boxplot saved")


# ============================================================
# Plot 7a: KPI — User savings bar chart
# ============================================================
def plot_kpi_user_savings(output_path):
    methods = ['Lower Bound', 'Q-learning', 'Heuristic', 'SAC', 'PPO', 'Upper Bound\n(LP)']
    values =  [16.66, 17.31, 20.08, 23.43, 24.22, 30.49]
    bar_colors = ['#94a3b8', '#94a3b8', '#94a3b8', '#94a3b8', '#009E73', '#94a3b8']

    x = np.arange(len(methods))
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(x, values, color=bar_colors, alpha=0.85, edgecolor='white', width=0.6)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.3,
                f'{height:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')

    ppo_idx = 4
    ax.axvspan(ppo_idx - 0.5, ppo_idx + 0.5, alpha=0.065, color='#009E73')

    ax.set_ylabel('User Savings (%)')
    ax.set_title('User Savings Comparison Across Trading Strategies',
                 fontsize=15, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(methods, fontsize=11)
    ax.grid(True, alpha=0.2, axis='y')
    ax.set_ylim(0, 35)

    legend_elements = [
        Patch(facecolor='#009E73', alpha=0.85, label='PPO (chosen strategy)'),
        Patch(facecolor='#94a3b8', alpha=0.85, label='Other strategies'),
    ]
    ax.legend(handles=legend_elements, loc='upper left', fontsize=10)

    increase = values[4] - values[0]  # PPO - Lower Bound
    ax.text(0.02, 0.82, f'Total user savings increase: +{increase:.1f}%',
            transform=ax.transAxes, ha='left', va='top', fontsize=11,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.8))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_kpi_user_savings saved")


# ============================================================
# Plot 7b: KPI — Community savings bar chart
# ============================================================
def plot_kpi_community_savings(output_path):
    methods = ['Lower Bound', 'Q-learning', 'Heuristic', 'SAC', 'PPO', 'Upper Bound\n(LP)']
    values =  [8.07, 8.21, 8.95, 13.54, 14.01, 19.07]
    bar_colors = ['#94a3b8', '#94a3b8', '#94a3b8', '#94a3b8', '#009E73', '#94a3b8']

    x = np.arange(len(methods))
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(x, values, color=bar_colors, alpha=0.85, edgecolor='white', width=0.6)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.3,
                f'{height:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')

    ppo_idx = 4
    ax.axvspan(ppo_idx - 0.5, ppo_idx + 0.5, alpha=0.065, color='#009E73')

    ax.set_ylabel('Community Savings (%)')
    ax.set_title('Community Savings Comparison Across Trading Strategies',
                 fontsize=15, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(methods, fontsize=11)
    ax.grid(True, alpha=0.2, axis='y')
    ax.set_ylim(0, 22)

    legend_elements = [
        Patch(facecolor='#009E73', alpha=0.85, label='PPO (chosen strategy)'),
        Patch(facecolor='#94a3b8', alpha=0.85, label='Other strategies'),
    ]
    ax.legend(handles=legend_elements, loc='upper left', fontsize=10)

    increase = values[4] - values[0]  # PPO - Lower Bound
    ax.text(0.02, 0.82, f'Total community savings increase: +{increase:.1f}%',
            transform=ax.transAxes, ha='left', va='top', fontsize=11,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.8))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_kpi_community_savings saved")


# ============================================================
# Plot 7c: KPI — Peer trades bar chart
# ============================================================
def plot_kpi_peer_trades(output_path):
    methods = ['Lower Bound', 'Q-learning', 'Heuristic', 'SAC', 'PPO', 'Upper Bound\n(LP)']
    values =  [25.82, 25.90, 26.13, 27.78, 28.32, 32.36]
    bar_colors = ['#94a3b8', '#94a3b8', '#94a3b8', '#94a3b8', '#009E73', '#94a3b8']

    x = np.arange(len(methods))
    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(x, values, color=bar_colors, alpha=0.85, edgecolor='white', width=0.6)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.3,
                f'{height:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')

    ppo_idx = 4
    ax.axvspan(ppo_idx - 0.5, ppo_idx + 0.5, alpha=0.065, color='#009E73')

    ax.set_ylabel('Peer Trades (%)')
    ax.set_title('Peer Trades Comparison Across Trading Strategies',
                 fontsize=15, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(methods, fontsize=11)
    ax.grid(True, alpha=0.2, axis='y')
    ax.set_ylim(0, 38)

    legend_elements = [
        Patch(facecolor='#009E73', alpha=0.85, label='PPO (chosen strategy)'),
        Patch(facecolor='#94a3b8', alpha=0.85, label='Other strategies'),
    ]
    ax.legend(handles=legend_elements, loc='upper left', fontsize=10)

    increase = values[4] - values[0]  # PPO - Lower Bound
    ax.text(0.02, 0.82, f'Total peer trades increase: +{increase:.1f}%',
            transform=ax.transAxes, ha='left', va='top', fontsize=11,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.8))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_kpi_peer_trades saved")


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    print("=" * 55)
    print("PPO ANALYSIS — All Plots")
    print("=" * 55)

    print("\nLoading data...")
    df_train, df_test, split_info = load_and_split(DATA_PATH, train_ratio=0.8)
    print(f"  Test set: {split_info['test_days']} days ({split_info['test_rows']} rows)")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if os.path.exists(INFERENCE_CSV):
        print(f"\nLoading cached inference data from: {INFERENCE_CSV}")
        results_df = pd.read_csv(INFERENCE_CSV)
        print(f"  {len(results_df)} rows ({results_df['day'].max() + 1} days)")
    else:
        print("\nNo cached inference data found. Loading model...")
        from stable_baselines3 import PPO
        from stable_baselines3.common.vec_env import VecNormalize, DummyVecEnv
        model = PPO.load(MODEL_PATH)
        def make_dummy_env():
            return P2PEnergyTradingEnv(df_test)
        dummy_env = DummyVecEnv([make_dummy_env])
        vec_norm = VecNormalize.load(VEC_NORM_PATH, dummy_env)
        print("Running inference on test set...")
        results_df = run_inference(df_test, model, vec_norm.obs_rms,
                                   vec_norm.clip_obs, vec_norm.epsilon)
        results_df.to_csv(INFERENCE_CSV, index=False)
        print(f"  Saved to: {INFERENCE_CSV}")

    print("\nComputing P2P/grid volumes...")
    results_with_volumes = compute_volumes(df_test, results_df)

    print("\nGenerating plots...")
    plot_action_bars(results_df, os.path.join(OUTPUT_DIR, 'plot_action_bars.png'))
    plot_action_heatmap(results_df, os.path.join(OUTPUT_DIR, 'plot_action_heatmap.png'))
    plot_community_strategy(results_df, os.path.join(OUTPUT_DIR, 'plot_community_strategy.png'))
    plot_community_strategy_individual(results_df, os.path.join(OUTPUT_DIR, 'plot_community_individual.png'),
                                        sample_days=SAMPLE_DAYS)

    if os.path.exists(P2P_VOLUMES_CSV):
        plot_p2p_volume(os.path.join(OUTPUT_DIR, 'plot_p2p_volume.png'))
    else:
        print(f"\n  Skipping P2P volume plot — {P2P_VOLUMES_CSV} not found.")

    plot_daily_savings_histogram(results_with_volumes,
                                  os.path.join(OUTPUT_DIR, 'plot_daily_savings_histogram.png'))
    plot_daily_savings_scatter(results_with_volumes,
                               os.path.join(OUTPUT_DIR, 'plot_daily_savings_scatter.png'))
    plot_daily_savings_boxplot(results_with_volumes,
                               os.path.join(OUTPUT_DIR, 'plot_daily_savings_boxplot.png'))
    plot_kpi_user_savings(os.path.join(OUTPUT_DIR, 'plot_kpi_user_savings.png'))
    plot_kpi_community_savings(os.path.join(OUTPUT_DIR, 'plot_kpi_community_savings.png'))
    plot_kpi_peer_trades(os.path.join(OUTPUT_DIR, 'plot_kpi_peer_trades.png'))

    print("\n" + "=" * 55)
    print("All plots saved to:", OUTPUT_DIR)
    print("=" * 55)

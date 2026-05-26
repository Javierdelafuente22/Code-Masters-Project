"""
Demand profile plots.

Plots:
    1. Daily consumption  - one day, all 9 SSEN profiles, by hour
    2. Yearly consumption - 3 SSEN profiles, weekly-mean across 2 years

Usage:
    python plotting/plot_demand.py
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ============================================================
# CONFIG
# ============================================================
DATA_PATH = "data/demand.csv"
OUTPUT_DIR = "data/plots"

# Daily plot: mid-winter weekday - pronounced evening peak.
DAILY_DATE = "2024-01-23"

# Yearly plot: first three SSEN profiles, weekly-mean aggregation.
YEARLY_PROFILES = [
    "SSEN-2506608140",
    "SSEN-2803012140",
    "SSEN-2521012110",
]
YEARLY_RESAMPLE = "W"

# 9 distinguishable colors, consistent with the existing palette
# (greens/reds/blues/oranges/purples used in plot_analysis.py).
DAILY_COLORS = [
    '#16a34a',  # green
    '#dc2626',  # red
    '#2563eb',  # blue
    '#f97316',  # orange
    '#7c3aed',  # purple
    '#0891b2',  # teal
    '#ca8a04',  # amber
    '#e11d48',  # rose
    '#475569',  # slate
]

YEARLY_COLORS = ['#16a34a', '#2563eb', '#f97316']

SSEN_COLUMNS = [
    "SSEN-2506608140", "SSEN-2518012120", "SSEN-2521012110",
    "SSEN-2522012150", "SSEN-2803012140", "SSEN-7900001010",
    "SSEN-7900004087", "SSEN-7991001005", "SSEN-8034001010",
]


def load_demand(path):
    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d/%m/%Y %H:%M')
    df = df.set_index('timestamp').sort_index()
    return df


# ============================================================
# Plot 1: Daily consumption - all 9 SSEN profiles
# ============================================================
def plot_daily_consumption(df, output_path):
    day = pd.Timestamp(DAILY_DATE)
    day_df = df.loc[day.strftime('%Y-%m-%d')]
    if len(day_df) != 24:
        raise ValueError(f"Expected 24 hours for {DAILY_DATE}, got {len(day_df)}")

    hours = day_df.index.hour.values

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for col, color in zip(SSEN_COLUMNS, DAILY_COLORS):
        ax.plot(hours, day_df[col].values, linewidth=1.5, color=color,
                label=col.replace('SSEN-', ''), alpha=0.9)

    ax.set_xlabel('Hour of day', fontsize=11)
    ax.set_ylabel('Demand (kWh, normalised)', fontsize=11)
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlim(-0.5, 23.5)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.2)
    ax.legend(loc='lower right', fontsize=8, ncol=3, title='Substation code',
              title_fontsize=8)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_daily_consumption saved -> {output_path}")


# ============================================================
# Plot 2: Yearly consumption - 3 profiles, weekly mean
# ============================================================
def plot_yearly_consumption(df, output_path):
    weekly = df[YEARLY_PROFILES].resample(YEARLY_RESAMPLE).mean()

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for col, color in zip(YEARLY_PROFILES, YEARLY_COLORS):
        ax.plot(weekly.index, weekly[col].values, linewidth=1.5, color=color,
                label=col.replace('SSEN-', ''), alpha=0.9)

    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())

    ax.set_xlabel('Date', fontsize=11)
    ax.set_ylabel('Demand (kWh, normalised)', fontsize=11)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.2)
    ax.legend(loc='lower right', fontsize=9, title='Substation code',
              title_fontsize=9)

    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha='center')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_yearly_consumption saved -> {output_path}")


# ============================================================
# Plot 3: Yearly consumption - all 9 SSEN profiles, weekly mean
# ============================================================
def plot_yearly_consumption_all(df, output_path):
    weekly = df[SSEN_COLUMNS].resample(YEARLY_RESAMPLE).mean()

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for col, color in zip(SSEN_COLUMNS, DAILY_COLORS):
        ax.plot(weekly.index, weekly[col].values, linewidth=1.5, color=color,
                label=col.replace('SSEN-', ''), alpha=0.9)

    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())

    ax.set_xlabel('Date', fontsize=11)
    ax.set_ylabel('Demand (kWh, normalised)', fontsize=11)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.2)
    ax.legend(loc='lower right', fontsize=8, ncol=3, title='Substation code',
              title_fontsize=8)

    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha='center')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  plot_yearly_consumption_all saved -> {output_path}")


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    print("=" * 55)
    print("DEMAND PROFILE PLOTS")
    print("=" * 55)

    print("\nLoading data...")
    df = load_demand(DATA_PATH)
    print(f"  {len(df)} rows, {df.index.min()} -> {df.index.max()}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("\nGenerating plots...")
    plot_daily_consumption(df, os.path.join(OUTPUT_DIR, 'plot_daily_demand.png'))
    plot_yearly_consumption(df, os.path.join(OUTPUT_DIR, 'plot_yearly_demand.png'))
    plot_yearly_consumption_all(df, os.path.join(OUTPUT_DIR, 'plot_yearly_demand_all.png'))

    print("\n" + "=" * 55)
    print(f"All plots saved to: {OUTPUT_DIR}")
    print("=" * 55)

"""
Makes the reward plots from the saved PPO training results.
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def _get_rolling_stats(episodes_df, source, window=5):
    """Return the rolling mean and std of the reward for one data source."""
    subset = episodes_df[episodes_df['source'] == source].copy()
    if len(subset) < 2:
        return None
    subset = subset.sort_values('timestep').reset_index(drop=True)
    subset['reward_mean'] = subset['reward'].rolling(window, min_periods=1, center=True).mean()
    subset['reward_std'] = subset['reward'].rolling(window, min_periods=1, center=True).std().fillna(0)
    return subset


def plot_lines(df, output_path, train_window=5, eval_window=5):
    """Plot the train and test reward as simple lines."""
    fig, ax = plt.subplots(figsize=(12, 6))
    train_stats = _get_rolling_stats(df, 'train', window=train_window)
    eval_stats = _get_rolling_stats(df, 'eval', window=eval_window)
    if train_stats is not None:
        ax.plot(train_stats['timestep'], train_stats['reward_mean'],
                linewidth=2, color='#E69F00', label='Training')
    if eval_stats is not None:
        ax.plot(eval_stats['timestep'], eval_stats['reward_mean'],
                linewidth=2, color='#0072B2', label='Test (unseen data)')
    ax.set_xlabel('Training Timesteps')
    ax.set_ylabel('Episode Reward')
    ax.set_title('PPO Training: Train vs Test Reward', fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='lower right')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"Plot 1 saved: {output_path}")


def plot_shaded_both(df, output_path, train_window=5, eval_window=5, band_factor=0.5):
    """Plot train and test reward with a shaded band around each line."""
    fig, ax = plt.subplots(figsize=(12, 6))
    for source, color, label, w in [
        ('train', '#E69F00', 'Training', train_window),
        ('eval', '#0072B2', 'Test (unseen data)', eval_window),
    ]:
        stats = _get_rolling_stats(df, source, w)
        if stats is None or len(stats) < 2:
            continue
        low = stats['reward_mean'] - band_factor * stats['reward_std']
        high = stats['reward_mean'] + band_factor * stats['reward_std']
        ax.fill_between(stats['timestep'], low, high, alpha=0.2, color=color)
        ax.plot(stats['timestep'], stats['reward_mean'],
                linewidth=2.5, color=color, label=f'{label} (± {band_factor} std)')
    ax.set_xlabel('Training Timesteps')
    ax.set_ylabel('Episode Reward')
    ax.set_title('PPO Training: Reward Range & Trend (Train + Test)', fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='lower right')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"Plot 2 saved: {output_path}")


def plot_shaded_test_only(df, output_path, eval_window=5, band_factor=0.5):
    """Plot only the test reward with a shaded band around the line."""
    stats = _get_rolling_stats(df, 'eval', eval_window)
    if stats is None or len(stats) < 2:
        print("Not enough eval data for plot 3.")
        return
    fig, ax = plt.subplots(figsize=(12, 6))
    low = stats['reward_mean'] - band_factor * stats['reward_std']
    high = stats['reward_mean'] + band_factor * stats['reward_std']
    ax.fill_between(stats['timestep'], low, high, alpha=0.25, color='#0072B2')
    ax.plot(stats['timestep'], stats['reward_mean'],
            linewidth=2.5, color='#0072B2', label=f'Test (± {band_factor} std)')
    ax.set_xlabel('Training Timesteps')
    ax.set_ylabel('Episode Reward')
    ax.set_title('PPO Training: Test Set Reward Range & Trend', fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='lower right')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"Plot 3 saved: {output_path}")


if __name__ == "__main__":
    CSV_PATH = "orderbook_results/ppo/training/training_episodes.csv"
    OUTPUT_DIR = "orderbook_results/ppo/training"
    TRAIN_WINDOW = 5
    EVAL_WINDOW = 5
    BAND_FACTOR = 0.5

    df = pd.read_csv(CSV_PATH)
    n_train = len(df[df['source'] == 'train'])
    n_eval = len(df[df['source'] == 'eval'])
    print(f"Loaded {len(df)} rows ({n_train} train, {n_eval} eval)")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    plot_lines(df, os.path.join(OUTPUT_DIR, 'plot1_lines.png'),
               TRAIN_WINDOW, EVAL_WINDOW)
    plot_shaded_both(df, os.path.join(OUTPUT_DIR, 'plot2_shaded_both.png'),
                     TRAIN_WINDOW, EVAL_WINDOW, BAND_FACTOR)
    plot_shaded_test_only(df, os.path.join(OUTPUT_DIR, 'plot3_shaded_test.png'),
                          EVAL_WINDOW, BAND_FACTOR)

    print("Done.")

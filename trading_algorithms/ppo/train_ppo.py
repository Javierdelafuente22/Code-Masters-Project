"""
Trains a PPO agent to control the battery for peer-to-peer energy trading.
Saves the trained model, the reward history, and the training plots.
"""

import os
from types import SimpleNamespace
from typing import Callable

import numpy as np
import pandas as pd

from stable_baselines3 import PPO
from stable_baselines3.common.callbacks import BaseCallback
from stable_baselines3.common.monitor import Monitor
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize

from rl_env.p2p_energy_env import P2PEnergyTradingEnv, MAX_RATE
from utils.data_loader import load_and_split, print_split_info


def linear_schedule(initial_value: float) -> Callable[[float], float]:
    """Learning rate that starts at initial_value and falls to 0 over training."""
    def func(progress_remaining: float) -> float:
        return progress_remaining * initial_value
    return func


class RewardTrackingCallback(BaseCallback):
    """Records the training and test rewards during training so we can plot them later."""

    def __init__(self, eval_env, eval_freq=10_000, n_eval_episodes=10,
                 episode_log_freq=1000, verbose=1):
        super().__init__(verbose)
        self.eval_env = eval_env
        self.eval_freq = eval_freq
        self.n_eval_episodes = n_eval_episodes
        self.episode_log_freq = episode_log_freq
        self.episode_log = []
        self._total_train_episodes = 0
        self.best_reward = -np.inf
        # Start the log at zero so the plots begin at the origin
        self.episode_log.append({
            'timestep': 0, 'episode': 0, 'reward': 0.0, 'source': 'train',
        })
        self.episode_log.append({
            'timestep': 0, 'episode': 0, 'reward': 0.0, 'source': 'eval',
        })

    def _on_step(self):
        # Record the reward whenever a training episode finishes
        infos = self.locals.get('infos', [])
        for info in infos:
            if 'episode' in info:
                self._total_train_episodes += 1
                ep_reward = float(info['episode']['r'])

                if self._total_train_episodes % self.episode_log_freq == 0:
                    self.episode_log.append({
                        'timestep': self.num_timesteps,
                        'episode': self._total_train_episodes,
                        'reward': ep_reward,
                        'source': 'train',
                    })

        # Every so often, test the agent on the unseen test set
        if self.n_calls % self.eval_freq == 0:
            rewards = []
            for _ in range(self.n_eval_episodes):
                obs = self.eval_env.reset()
                done = False
                episode_reward = 0.0
                while not done:
                    action, _ = self.model.predict(obs, deterministic=True)
                    obs, reward, dones, info = self.eval_env.step(action)
                    raw_reward = self.eval_env.get_original_reward()[0]
                    episode_reward += float(raw_reward)
                    done = bool(dones[0])
                rewards.append(episode_reward)

            # Log one point per test checkpoint: the average reward
            mean_eval_reward = float(np.mean(rewards))
            self.episode_log.append({
                'timestep': self.num_timesteps,
                'episode': self._total_train_episodes,
                'reward': mean_eval_reward,
                'source': 'eval',
            })

            if self.verbose:
                recent_train = [e['reward'] for e in self.episode_log
                                if e['source'] == 'train'][-10:]
                mean_train = float(np.mean(recent_train)) if recent_train else np.nan
                print(f"  Step {self.num_timesteps:>7d} | Train: {mean_train:7.3f} | Eval: {mean_eval_reward:7.3f}")

            self.logger.record("eval/mean_reward", mean_eval_reward)

            if mean_eval_reward > self.best_reward:
                self.best_reward = mean_eval_reward

        return True


def train(args):
    print("\n" + "=" * 55)
    print("P2P ENERGY TRADING - PPO TRAINING")
    print("=" * 55)

    os.makedirs(args.output_dir, exist_ok=True)

    # Load the data and split it into training and test sets
    print("\n[1/4] Loading data...")
    df_train, df_test, split_info = load_and_split(args.data, train_ratio=0.8)
    print_split_info(split_info)

    print("\n[2/4] Creating environments...")

    # Training environment (Monitor tracks rewards; VecNormalize scales the inputs)
    def make_train_env():
        return Monitor(P2PEnergyTradingEnv(df_train, reward_scale=args.reward_scale))

    train_env = DummyVecEnv([make_train_env])
    train_env = VecNormalize(
        train_env,
        norm_obs=True,
        norm_reward=True,
        clip_obs=10.0,
        gamma=args.gamma,
    )

    # Test environment (must use the same scaling as the training environment)
    def make_eval_env():
        return P2PEnergyTradingEnv(df_test, reward_scale=args.reward_scale)

    eval_env = DummyVecEnv([make_eval_env])
    eval_env = VecNormalize(
        eval_env,
        norm_obs=True,
        norm_reward=True,
        clip_obs=10.0,
        gamma=args.gamma,
        training=False,  # use the training stats, don't update them
    )

    print(f"  Train env: {split_info['train_days']} episodes (days)")
    print(f"  Test env:  {split_info['test_days']} episodes (days)")
    print(f"  Action space: continuous [-1, 1], scaled to battery power [-{MAX_RATE}, {MAX_RATE}]")
    print(f"  VecNormalize: observations normalised with running mean/std")

    print(f"\n[3/4] Training PPO for {args.timesteps:,} timesteps...")
    print(f"  Learning rate:  CONSTANT {args.lr} (no decay)")
    print(f"  Entropy coef:   {args.ent_coef}")
    print(f"  Gamma:          {args.gamma}")
    print(f"  Network arch:   [256, 256]")
    print(f"  Initial log_std: {args.log_std_init} (std = {np.exp(args.log_std_init):.3f})")

    model = PPO(
        "MlpPolicy",
        train_env,
        learning_rate=args.lr,
        n_steps=2048,
        batch_size=256,
        n_epochs=10,
        gamma=args.gamma,
        gae_lambda=0.95,
        clip_range=0.2,
        ent_coef=args.ent_coef,        # higher value = more exploration
        max_grad_norm=0.5,
        verbose=1,
        tensorboard_log=args.log_dir,
        seed=args.seed,
        device='cpu',
        policy_kwargs=dict(
            log_std_init=args.log_std_init,   # how random the actions are at the start
            net_arch=dict(pi=[256, 256], vf=[256, 256]),   # network size
        ),
    )

    # Make the test environment share the training environment's scaling stats
    eval_env.obs_rms = train_env.obs_rms
    eval_env.ret_rms = train_env.ret_rms

    reward_callback = RewardTrackingCallback(
        eval_env=eval_env,
        eval_freq=args.eval_freq,
        n_eval_episodes=10,
        verbose=1,
    )

    try:
        model.learn(
            total_timesteps=args.timesteps,
            callback=reward_callback,
            progress_bar=True,
        )
    except KeyboardInterrupt:
        print("\n\nTraining interrupted — saving progress so far...")

    # Save the reward history (training + test) to one CSV
    episode_df = pd.DataFrame(reward_callback.episode_log)
    csv_path = os.path.join(args.output_dir, 'training_episodes.csv')
    episode_df.to_csv(csv_path, index=False)
    n_train = len(episode_df[episode_df['source'] == 'train'])
    n_eval = len(episode_df[episode_df['source'] == 'eval'])
    print(f"Episode log saved: {n_train} train + {n_eval} eval = {len(episode_df)} total")

    # Draw the training curves
    from plotting.plot_training import plot_lines, plot_shaded_both, plot_shaded_test_only
    plot_lines(episode_df, os.path.join(args.output_dir, 'plot1_lines.png'))
    plot_shaded_both(episode_df, os.path.join(args.output_dir, 'plot2_shaded_both.png'))
    plot_shaded_test_only(episode_df, os.path.join(args.output_dir, 'plot3_shaded_test.png'))

    # Save the trained model and its scaling stats so it can be reused later
    model_path = os.path.join(args.output_dir, "ppo_p2p_trading")
    model.save(model_path)
    vec_normalize_path = os.path.join(args.output_dir, "vec_normalize.pkl")
    train_env.save(vec_normalize_path)
    print(f"\nModel saved to:         {model_path}.zip")
    print(f"VecNormalize stats to:  {vec_normalize_path}")

    print("\n" + "=" * 55)
    print("FILES SAVED in", args.output_dir)
    print("=" * 55)
    print("  training_episodes.csv      (per-episode reward data)")
    print("  plot1_lines.png            (train + test lines)")
    print("  plot2_shaded_both.png      (shaded range, train + test)")
    print("  plot3_shaded_test.png      (shaded range, test only)")
    print("  ppo_p2p_trading.zip        (trained model)")
    print("  vec_normalize.pkl          (normalization stats)")
    print("=" * 55)


if __name__ == "__main__":
    # Settings for a training run
    args = SimpleNamespace(
        data="data/orderbook.csv",
        timesteps=500_000,
        lr=3e-4,
        reward_scale=10.0,
        ent_coef=0.05,          # higher = more exploration
        log_std_init=-1.0,      # how random the actions start out
        gamma=0.99,             # how much future rewards matter
        eval_freq=10_000,
        seed=42,
        output_dir="orderbook_results/ppo",
        log_dir="logs/tensorboard",
    )
    train(args)

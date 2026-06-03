# Ampeer

**AI-automated peer-to-peer energy trading for everyday households.**

**Live app: [www.ampeerenergy.com](https://www.ampeerenergy.com)**

Ampeer lets neighbours buy and sell their solar and battery energy directly with
each other, and uses AI to run the whole thing automatically — so people get the
savings of a local energy market without having to manage one.

---

## Overview

Energy used to flow one way: big power stations generated it, the grid delivered it,
and households consumed it. Today millions of homes have solar panels, batteries and
electric vehicles, turning consumers into **prosumers** who both produce and use power.

**Peer-to-peer (P2P) trading** lets these prosumers trade locally instead of only
through the grid. The benefit comes from the gap between the grid's two prices:

> A household selling surplus solar to the grid might get **~10p/kWh**, while a
> neighbour buying from the grid pays **~30p/kWh**. Trading directly at a midpoint of
> **~20p/kWh** leaves the seller better off *and* the buyer better off.

The economics and technology already work — the real barrier is **effort**. Managing a
battery and actively trading is more than most people want to do. Ampeer removes that
effort by automating the trading with AI and wrapping it in a simple, friendly app.

## How it works

Ampeer has three parts that together deliver "hands-off" P2P trading:

1. **Automated trading engine** — AI agents decide when to charge or discharge each
   home's battery and match buyers with sellers in a local market, aiming to capture
   the best price for the user every hour.
2. **Insight dashboard** — a mobile-style web app that turns the trading activity into
   clear financial and carbon savings, so users can see what the AI is doing for them.
3. **Conversational assistant** — a chatbot that lets users adjust their strategy in
   plain language (e.g. *"I'm going on holiday next week"* or *"I'll be working from
   home"*), which updates their energy profile automatically.

The trading is tested on a simulated community of **10 homes plus the grid**, using
real half-hourly price and demand data. Several agents are compared — a rule-based
heuristic, tabular Q-learning, and the deep RL methods PPO and SAC — against
theoretical best- and worst-case benchmarks.

## Repository structure

```
Ampeer/
├── index.html                # App entry point (loads the React frontend)
├── tokens.css                # Shared styling (colours, fonts, spacing)
├── start_demo.bat            # One-click local launcher (server + browser)
├── requirements.txt          # Python dependencies
│
├── app/                      # Main app screens (Dashboard, Home, Community, Assistant, Profile)
├── components/               # Reusable UI pieces and the onboarding screens
├── vendor/                   # Bundled browser libraries (React, Babel, pdf.js)
│
├── rl_env/                   # Reinforcement learning environment
│   ├── p2p_energy_env.py     #   trading environment (one day = one episode)
│   ├── battery.py            #   simple battery model
│   └── rl_orderbook_simp.py  #   fast market clearing used during training
│
├── trading_algorithms/       # The trading agents
│   ├── orderbook_basic.py    #   P2P market engine (no battery)
│   ├── Heuristic/            #   rule-based battery strategy
│   ├── Qlearning/            #   tabular Q-learning agent
│   ├── ppo/                  #   PPO agent (Stable-Baselines3)
│   └── sac/                  #   SAC agent (Stable-Baselines3)
│
├── benchmark_calcs/          # Theoretical bounds for comparison
│   ├── calc_theoretical_min.py    #   lower bound (no battery)
│   └── calc_theoretical_max_lp.py #   upper bound (linear programming)
│
├── chatbot/                  # Conversational assistant
│   ├── server.py             #   Flask server (serves the app + chatbot API)
│   ├── chatbot_API.py        #   talks to Google Gemini
│   └── chatbot_data.py       #   turns lifestyle requests into demand changes
│
├── data/                     # Datasets and the case-study generator
├── plotting/                 # Figures for the analysis and report
└── utils/                    # Data loading / train-test splitting helpers
```

## Tech stack

- **AI / trading:** Python, [Stable-Baselines3](https://stable-baselines3.readthedocs.io)
  (PPO, SAC), [Gymnasium](https://gymnasium.farama.org), PyTorch, NumPy, Pandas,
  [PuLP](https://coin-or.github.io/pulp/) (linear-programming benchmark)
- **Assistant:** Flask, Google Gemini (`google-genai`)
- **Frontend:** React (loaded in-browser via Babel — no build step), HTML, CSS
- **Plots:** Matplotlib, Seaborn

## Getting started

The easiest way to try Ampeer is the live app: **[www.ampeerenergy.com](https://www.ampeerenergy.com)**.

To run it locally:

### 1. Install dependencies

```bash
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt   # Windows
# (use .venv/bin/pip on macOS/Linux)
```

### 2. Add a Gemini API key (for the chatbot)

Create a `.env` file in the project root:

```
GEMINI_API_KEY=your_key_here
```

### 3. Launch the app

```bash
start_demo.bat
```

This starts the local server and opens the app at `http://localhost:5000`.
(On macOS/Linux, run `python chatbot/server.py` and open that address manually.)

## Usage

Run these from the project root.

**Train a trading agent**

```bash
python -m trading_algorithms.ppo.train_ppo     # PPO
python -m trading_algorithms.sac.train_sac     # SAC
python -m trading_algorithms.Qlearning.train_qlearning   # Q-learning
```

Each run saves the trained model, a reward history, and training plots under
`orderbook_results/`.

**Run the market simulations and benchmarks**

```bash
python -m trading_algorithms.orderbook_basic        # baseline P2P market
python -m benchmark_calcs.calc_theoretical_min      # lower bound (no battery)
python -m benchmark_calcs.calc_theoretical_max_lp   # upper bound (perfect battery)
```

**Generate the analysis figures**

```bash
python -m plotting.plot_analysis
```
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time

# Import your trading strategy
from trading_algorithms.Heuristic.heuristic import Heuristic_v1

def run_single_simulation(input_file, alpha_file, target_agents, battery_class):
    """
    Your original simulation engine, modified slightly to return KPIs 
    instead of just printing them.
    """
    df = pd.read_csv(input_file)
    agent_ids = [c for c in df.columns if c not in [
        'timestamp', 'time_year_sin', 'time_year_cos', 
        'time_day_sin', 'time_day_cos','is_working_day', 'import_price', 
        'export_price', 'spread','net_community'
    ]]
    
    df_alphas = pd.read_csv(alpha_file)
    df_alphas.columns = agent_ids
    
    # Trackers
    batteries = {u: battery_class() for u in target_agents if u in agent_ids}
    metrics = {a: {'p2p': 0.0, 'grid': 0.0, 'base': 0.0, 'p2p_n': 0.0} for a in agent_ids}

    # Simulation Loop
    for idx, row in df.iterrows():
        tou, fit = row['import_price'], row['export_price']
        spread = row.get('spread', 0)
        
        # Baseline
        for a in agent_ids:
            metrics[a]['base'] += (-row[a] * tou) if row[a] > 0 else (abs(row[a]) * fit)

        # Battery Optimization
        for u, batt in batteries.items():
            row[u], _ = batt.optimize_demand(row[u], tou, fit, spread)

        # Market Clearing
        if tou > fit:
            buys = sorted([[a, row[a], df_alphas.at[idx, a]] for a in agent_ids if row[a] > 0], key=lambda x: x[2], reverse=True)
            sells = sorted([[a, abs(row[a]), df_alphas.at[idx, a]] for a in agent_ids if row[a] < 0], key=lambda x: x[2])
            
            b_i, s_i = 0, 0
            pr = fit + 0.5 * (tou - fit) # Midpoint
            
            while b_i < len(buys) and s_i < len(sells):
                t_qty = min(buys[b_i][1], sells[s_i][1])
                metrics[buys[b_i][0]]['p2p_n'] -= t_qty * pr
                metrics[sells[s_i][0]]['p2p_n'] += t_qty * pr
                metrics[buys[b_i][0]]['p2p'] += t_qty
                metrics[sells[s_i][0]]['p2p'] += t_qty
                
                buys[b_i][1] -= t_qty
                sells[s_i][1] -= t_qty
                if buys[b_i][1] < 1e-9: b_i += 1
                if sells[s_i][1] < 1e-9: s_i += 1
            
            # Grid Imbalance
            for a, qty, _ in buys[b_i:]:
                metrics[a]['p2p_n'] -= qty * tou
                metrics[a]['grid'] += qty
            for a, qty, _ in sells[s_i:]:
                metrics[a]['p2p_n'] += qty * fit
                metrics[a]['grid'] += qty
        else:
            # Arbitrage check fail
            for a in agent_ids:
                metrics[a]['p2p_n'] += (-row[a] * tou) if row[a] > 0 else (abs(row[a]) * fit)
                metrics[a]['grid'] += abs(row[a])

    # Aggregate Results
    a1_sav = ((metrics['1_Prosumer']['p2p_n'] - metrics['1_Prosumer']['base']) / abs(metrics['1_Prosumer']['base'])) * 100
    
    total_base = sum(m['base'] for m in metrics.values())
    total_p2p_n = sum(m['p2p_n'] for m in metrics.values())
    comm_sav = ((total_p2p_n - total_base) / abs(total_base)) * 100
    
    total_p2p_kwh = sum(m['p2p'] for m in metrics.values())
    total_grid_kwh = sum(m['grid'] for m in metrics.values())
    pt_pct = (total_p2p_kwh / (total_p2p_kwh + total_grid_kwh)) * 100
    
    return round(a1_sav, 2), round(comm_sav, 2), round(pt_pct, 2)

def generate_saturation_study():
    input_file = 'data/orderbook.csv'
    alpha_file = 'data/alphas.csv'
    
    # Identify agent names
    df_temp = pd.read_csv(input_file)
    all_agents = [c for c in df_temp.columns if c not in [
        'timestamp', 'time_year_sin', 'time_year_cos', 
        'time_day_sin', 'time_day_cos','is_working_day', 'import_price', 
        'export_price', 'spread','net_community'
    ]]
    
    results = []
    
    # Loop for Adoption Density 1 to 10
    for n in range(1, 11):
        target_list = all_agents[:n]
        print(f"Running simulation for {n} battery-enabled agent(s)...")
        
        a1_s, comm_s, pt_p = run_single_simulation(input_file, alpha_file, target_list, Heuristic_v1)
        
        results.append({
            'n': n,
            'Agent 1': a1_s,
            'Community': comm_s,
            'Peer Trades': pt_p
        })
    
    res_df = pd.DataFrame(results)
    res_df.to_csv('final_saturation_results.csv', index=False)
    
    # --- PLOTTING ---
    plt.rcParams.update({'font.size': 14}) # Base font size
    fig, ax1 = plt.subplots(figsize=(12, 7), dpi=200)

    # Left Axis: Savings
    ln1 = ax1.plot(res_df['n'], res_df['Agent 1'], marker='o', color='#0072B2', linewidth=3, markersize=10, label='Agent 1')
    ln2 = ax1.plot(res_df['n'], res_df['Community'], marker='s', color='#009E73', linewidth=3, markersize=10, label='Community')
    
    ax1.set_xlabel('Number of Agents with Batteries (Adoption Density)', fontweight='bold', fontsize=16)
    ax1.set_ylabel('Savings (%)', fontweight='bold', fontsize=16)
    ax1.tick_params(axis='both', labelsize=14)
    ax1.set_xticks(range(1, 11))
    ax1.grid(True, linestyle='--', alpha=0.6)

    # Right Axis: Peer Trades
    ax2 = ax1.twinx()
    ln3 = ax2.plot(res_df['n'], res_df['Peer Trades'], marker='^', color='#d62728', linestyle='--', linewidth=2.5, markersize=10, label='Peer Trades')
    ax2.set_ylabel('Peer Trades (%)', color='#d62728', fontweight='bold', fontsize=16)
    ax2.tick_params(axis='y', labelcolor='#d62728', labelsize=14)

    # Combined Legend: Top Right
    lns = ln1 + ln2 + ln3
    labs = [l.get_label() for l in lns]
    ax1.legend(lns, labs, loc='upper right', frameon=True, shadow=True, fontsize=14)

    plt.tight_layout()
    plt.savefig('final_saturation_plot.png')
    print("\nProcess complete. Plot saved as 'final_saturation_plot.png'")

if __name__ == "__main__":
    generate_saturation_study()
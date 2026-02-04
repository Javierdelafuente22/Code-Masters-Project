import pandas as pd
import numpy as np

def run_energy_market_simulation(input_file, p2p_output, report_output, alpha=0.5):
    # 1. Load Data
    # Note: Use pd.read_excel(input_file) for .xlsx files
    df = pd.read_excel(input_file)

    metadata_cols = ['timestamp', 'export price', 'import price']
    agent_ids = [col for col in df.columns if col not in metadata_cols]
    
    # Containers for results
    p2p_financials = df.copy()
    for agent in agent_ids: 
        p2p_financials[agent] = 0.0
    
    # Metrics tracking for the summary report
    metrics = {agent: {'p2p_kWh': 0.0, 'grid_kWh': 0.0, 'baseline_net': 0.0, 'p2p_net': 0.0} for agent in agent_ids}

    # 2. Simulation Loop
    for index, row in df.iterrows():
        fit = row['export price']
        tou = row['import price']
        
        # Calculate Baseline (Grid-Only) for this period
        for agent in agent_ids:
            qty = row[agent]
            if qty > 0: # Agent buys from Grid
                metrics[agent]['baseline_net'] -= qty * tou
            else: # Agent sells to Grid
                metrics[agent]['baseline_net'] += abs(qty) * fit

        # --- P2P Market Logic ---
        if tou <= fit:
            # Rationality Guard: If no benefit exists, everyone uses the Grid
            for agent in agent_ids:
                qty = row[agent]
                val = -qty * tou if qty > 0 else abs(qty) * fit
                p2p_financials.at[index, agent] = val
                metrics[agent]['p2p_net'] += val
                metrics[agent]['grid_kWh'] += abs(qty)
        else:
            # Calculate P2P Price (Will be 0 if FiT/ToU are centered around 0)
            p2p_price = fit + alpha * (tou - fit)

            buy_orders = [[a, row[a]] for a in agent_ids if row[a] > 0]
            sell_orders = [[a, abs(row[a])] for a in agent_ids if row[a] < 0]

            # FCFS Matching
            b_idx, s_idx = 0, 0
            while b_idx < len(buy_orders) and s_idx < len(sell_orders):
                b_id, b_qty = buy_orders[b_idx]
                s_id, s_qty = sell_orders[s_idx]
                
                trade_qty = min(b_qty, s_qty)
                
                # Financials (P2P Trade)
                p2p_financials.at[index, b_id] -= trade_qty * p2p_price
                p2p_financials.at[index, s_id] += trade_qty * p2p_price
                metrics[b_id]['p2p_net'] -= trade_qty * p2p_price
                metrics[s_id]['p2p_net'] += trade_qty * p2p_price
                
                # Volume Tracking
                metrics[b_id]['p2p_kWh'] += trade_qty
                metrics[s_id]['p2p_kWh'] += trade_qty
                
                buy_orders[b_idx][1] -= trade_qty
                sell_orders[s_idx][1] -= trade_qty
                if buy_orders[b_idx][1] < 1e-9: b_idx += 1
                if sell_orders[s_idx][1] < 1e-9: s_idx += 1
            
            # Grid Settlement for unmatched quantities
            for i in range(b_idx, len(buy_orders)):
                agent, rem_qty = buy_orders[i]
                cost = rem_qty * tou
                p2p_financials.at[index, agent] -= cost
                metrics[agent]['p2p_net'] -= cost
                metrics[agent]['grid_kWh'] += rem_qty
                
            for i in range(s_idx, len(sell_orders)):
                agent, rem_qty = sell_orders[i]
                rev = rem_qty * fit
                p2p_financials.at[index, agent] += rev
                metrics[agent]['p2p_net'] += rev
                metrics[agent]['grid_kWh'] += rem_qty

    # 3. Build Savings Report
    report_list = []
    for agent in agent_ids:
        m = metrics[agent]
        total_vol = m['p2p_kWh'] + m['grid_kWh']
        report_list.append({
            'Agent': agent,
            'Baseline Net ($)': round(m['baseline_net'], 4),
            'P2P Net ($)': round(m['p2p_net'], 4),
            'Savings ($)': round(m['p2p_net'] - m['baseline_net'], 4),
            'P2P Traded (kWh)': round(m['p2p_kWh'], 2),
            'Grid Traded (kWh)': round(m['grid_kWh'], 2),
            'Peer Trade %': round((m['p2p_kWh'] / total_vol * 100), 2) if total_vol > 0 else 0
        })

    # 4. Save to files
    pd.DataFrame(report_list).to_csv(report_output, index=False)
    p2p_financials.to_csv(p2p_output, index=False)
    print(f"Success. Files generated: {p2p_output}, {report_output}")

# Execution
run_energy_market_simulation('order_book_fake.xlsx', 'p2p_financial_results.csv', 'savings_report.csv')
"""
Best-case benchmark using linear programming.
Works out the most a perfectly-optimised battery could ever save, as an upper bound.
"""

import pandas as pd
import numpy as np
import pulp

def lp_benchmarks():
    df = pd.read_csv('data/test_set.csv')

    agent_ids = [c for c in df.columns if c not in [
        'timestamp', 'time_year_sin', 'time_year_cos', 'is_working_day',
        'import_price', 'export_price', 'spread', 'net_community'
    ]]
    a1_col = '1_Prosumer'
    other_agents_cols = [c for c in agent_ids if c != a1_col]

    tou, fit = df['import_price'].values, df['export_price'].values
    midpoint = (tou + fit) / 2.0
    a1_load = df[a1_col].values

    # How much spare energy and unmet demand the neighbours have each hour
    others_surplus = df[other_agents_cols].apply(lambda x: np.maximum(0, -x)).sum(axis=1).values
    others_shortage = df[other_agents_cols].apply(lambda x: np.maximum(0, x)).sum(axis=1).values

    T = len(df)
    BATT_CAPACITY, BATT_POWER, EFFICIENCY = 1.0, 0.4, 0.95

    # Set up and solve the optimisation for Agent 1's battery (minimise its cost)
    prob = pulp.LpProblem("Proper_Report", pulp.LpMinimize)
    soc = pulp.LpVariable.dicts("SOC", range(T), 0, BATT_CAPACITY)
    p_charge = pulp.LpVariable.dicts("Charge", range(T), 0, BATT_POWER)
    p_discharge = pulp.LpVariable.dicts("Discharge", range(T), 0, BATT_POWER)
    grid_buy = pulp.LpVariable.dicts("GridBuy", range(T), 0)
    grid_sell = pulp.LpVariable.dicts("GridSell", range(T), 0)
    p2p_buy = pulp.LpVariable.dicts("P2PBuy", range(T), 0)
    p2p_sell = pulp.LpVariable.dicts("P2PSell", range(T), 0)

    prob += pulp.lpSum([
        grid_buy[t]*tou[t] + p2p_buy[t]*midpoint[t] - grid_sell[t]*fit[t] - p2p_sell[t]*midpoint[t]
        for t in range(T)
    ])

    for t in range(T):
        if t == 0:
            prob += soc[t] == (p_charge[t] * EFFICIENCY) - (p_discharge[t] / EFFICIENCY)
        else:
            prob += soc[t] == soc[t-1] + (p_charge[t] * EFFICIENCY) - (p_discharge[t] / EFFICIENCY)
        prob += a1_load[t] + p_charge[t] - p_discharge[t] == grid_buy[t] + p2p_buy[t] - grid_sell[t] - p2p_sell[t]
        prob += p2p_buy[t] <= others_surplus[t]
        prob += p2p_sell[t] <= others_shortage[t]

    prob.solve(pulp.PULP_CBC_CMD(msg=0))

    # After Agent 1 has traded, see how much the neighbours can still trade with each other
    neighbor_to_neighbor_vol = 0
    neighbor_p2p_savings = 0

    for t in range(T):
        rem_surplus = others_surplus[t] - pulp.value(p2p_buy[t])
        rem_shortage = others_shortage[t] - pulp.value(p2p_sell[t])

        natural_trade = min(rem_surplus, rem_shortage)
        neighbor_to_neighbor_vol += natural_trade
        # Both sides of each trade save money
        neighbor_p2p_savings += natural_trade * (tou[t] - midpoint[t]) # buyer side
        neighbor_p2p_savings += natural_trade * (midpoint[t] - fit[t]) # seller side

    # Turn everything into the final percentages
    # Agent 1's own savings vs paying full grid prices
    a1_base = (a1_load[a1_load > 0]*tou[a1_load > 0]).sum() - (np.abs(a1_load[a1_load < 0])*fit[a1_load < 0]).sum()
    user_savings_pct = ((a1_base - pulp.value(prob.objective)) / abs(a1_base)) * 100

    # Share of all energy that was traded peer-to-peer (each trade counts for both sides)
    total_p2p_volume = (sum(pulp.value(p2p_buy[t]) + pulp.value(p2p_sell[t]) for t in range(T)) + (neighbor_to_neighbor_vol * 2))
    total_energy_vol = df[agent_ids].abs().sum().sum()
    total_pt_pct = (total_p2p_volume / total_energy_vol) * 100

    # Whole-community savings
    comm_baseline_cost = 0
    for a in agent_ids:
        load = df[a].values
        comm_baseline_cost += (load[load > 0]*tou[load > 0]).sum() - (np.abs(load[load < 0])*fit[load < 0]).sum()

    # Total community profit = Agent 1 + neighbours trading with Agent 1 + neighbours trading with each other
    a1_profit = a1_base - pulp.value(prob.objective)
    a1_to_neighbor_profit = sum(pulp.value(p2p_buy[t])*(midpoint[t]-fit[t]) + pulp.value(p2p_sell[t])*(tou[t]-midpoint[t]) for t in range(T))

    total_community_profit = a1_profit + a1_to_neighbor_profit + neighbor_p2p_savings
    total_comm_savings_pct = (total_community_profit / abs(comm_baseline_cost)) * 100

    print("\n" + "="*55)
    print("UPPER BOUND 3 (LP STRATEGIC CEILING)")
    print("="*55)
    print(f"% Peer Trades:      {total_pt_pct:.2f}%")
    print(f"% Community Saving: {total_comm_savings_pct:.2f}%")
    print(f"% User Savings:     {user_savings_pct:.2f}%")
    print("="*55)

if __name__ == "__main__":
    lp_benchmarks()

"""
Simple market clearing for the RL environment.
Works out how much the agent trades peer-to-peer versus with the grid, and the cost.
"""

import numpy as np


def clear_market_for_agent(modified_demand, others_demands, import_price, export_price):
    """
    Work out the agent's cost after clearing, plus how much it traded P2P and with the grid.
    Positive demand means buying, negative means selling.
    """
    midpoint = (import_price + export_price) / 2.0

    # If buying from the grid isn't dearer than selling, P2P gives no benefit, so use the grid.
    if import_price <= export_price:
        if modified_demand > 0:
            return modified_demand * import_price, 0.0, modified_demand
        elif modified_demand < 0:
            return -(abs(modified_demand) * export_price), 0.0, abs(modified_demand)
        else:
            return 0.0, 0.0, 0.0
    
    # How much the other agents are selling and buying, available for P2P trades.
    others_supply = np.sum(np.abs(others_demands[others_demands < 0]))
    others_demand = np.sum(others_demands[others_demands > 0])

    if modified_demand > 0:
        # Agent is buying: take what it can from other sellers, rest from the grid.
        p2p_bought = min(modified_demand, others_supply)
        grid_bought = modified_demand - p2p_bought
        agent_cost = p2p_bought * midpoint + grid_bought * import_price
        return agent_cost, p2p_bought, grid_bought

    elif modified_demand < 0:
        # Agent is selling: sell what it can to other buyers, rest to the grid.
        sell_amount = abs(modified_demand)
        p2p_sold = min(sell_amount, others_demand)
        grid_sold = sell_amount - p2p_sold
        agent_cost = -(p2p_sold * midpoint + grid_sold * export_price)
        return agent_cost, p2p_sold, grid_sold
        
    else:
        return 0.0, 0.0, 0.0


def compute_baseline_cost(raw_demand, import_price, export_price):
    """
    What the agent would pay or earn using only the grid, with no battery and no P2P trading.
    """
    if raw_demand > 0:
        return raw_demand * import_price
    elif raw_demand < 0:
        return -(abs(raw_demand) * export_price)
    else:
        return 0.0

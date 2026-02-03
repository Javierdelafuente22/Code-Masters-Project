import pandas as pd
import numpy as np
import os

def run_double_auction(input_file_path, output_file_path, alpha=0.5):
    """
    Simulates a Double Auction P2P energy market based on the specifications.

    Args:
        input_file_path (str): Path to the input Excel file.
        output_file_path (str): Path to save the output CSV file.
        alpha (float): Scaling factor for P2P price calculation (0.0 to 1.0).
                       P2P Price = FiT + alpha * (ToU - FiT).
    """

    # 1. Load Input Data
    # Expecting: 'timestamp', 'export price' (FiT), 'import price' (ToU), followed by agent columns.
    try:
        df = pd.read_excel(input_file_path)
    except Exception as e:
        print(f"Error reading input file: {e}")
        return

    # Identify agent columns (all columns after the first 3 metadata columns)
    metadata_cols = ['timestamp', 'export price', 'import price']
    # Check if headers match expected lower case; if not, adjust logic or standardise input
    # Assuming input follows the prompt strictly:
    agent_ids = [col for col in df.columns if col not in metadata_cols]
    
    # Initialize a DataFrame to store financial results (Profit/Cost)
    # Copy structure from input to ensure timestamp alignment
    financial_results = df.copy()
    
    # Reset agent columns to 0.0 to accumulate financial results
    for agent in agent_ids:
        financial_results[agent] = 0.0

    print(f"Starting simulation for {len(df)} time periods with {len(agent_ids)} agents...")

    # 2. Iterate through each timestamp (Trading Period)
    for index, row in df.iterrows():
        timestamp = row['timestamp']
        fit = row['export price']
        tou = row['import price']
        
        # --- P2P Pricing Rule ---
        # Price = FiT + alpha * (ToU - FiT)
        # [cite_start]This creates a mid-market price beneficial to both buyer and seller [cite: 31, 203]
        p2p_price = fit + alpha * (tou - fit)

        # --- Order Book Creation ---
        # Separate agents into Buy Book and Sell Book
        # Positive profile = Buying (Demand), Negative profile = Selling (Supply)
        buy_orders = []
        sell_orders = []

        for agent in agent_ids:
            quantity = row[agent]
            if quantity > 0:
                # Buyer: needs 'quantity' energy
                # Storing as list: [Agent ID, Quantity Remaining]
                buy_orders.append([agent, quantity]) 
            elif quantity < 0:
                # Seller: has excess 'quantity' energy 
                # Store absolute value for matching logic
                sell_orders.append([agent, abs(quantity)])
        
        # --- Matching Logic (Double Auction - FCFS) ---
        # We iterate through the lists sequentially (First-Come-First-Served based on ID order).
        
        buyer_idx = 0
        seller_idx = 0

        # Create dictionary to track period financials (Revenue - Cost)
        period_financials = {agent: 0.0 for agent in agent_ids}

        # Match while both books have available orders
        while buyer_idx < len(buy_orders) and seller_idx < len(sell_orders):
            buyer_id = buy_orders[buyer_idx][0]
            buyer_qty = buy_orders[buyer_idx][1]
            
            seller_id = sell_orders[seller_idx][0]
            seller_qty = sell_orders[seller_idx][1]

            # Determine trade quantity (min of available buy/sell)
            trade_qty = min(buyer_qty, seller_qty)

            # Execute P2P Transaction
            # Buyer pays: trade_qty * p2p_price (Negative impact on profit)
            period_financials[buyer_id] -= trade_qty * p2p_price
            
            # Seller earns: trade_qty * p2p_price (Positive impact on profit)
            period_financials[seller_id] += trade_qty * p2p_price

            # Update remaining quantities in the temp lists
            buy_orders[buyer_idx][1] -= trade_qty
            sell_orders[seller_idx][1] -= trade_qty

            # Move to next order if fully satisfied
            # Using small epsilon for float comparison safety
            if buy_orders[buyer_idx][1] < 1e-9: 
                buyer_idx += 1
            
            if sell_orders[seller_idx][1] < 1e-9:
                seller_idx += 1

        # --- Grid Settlement (Unsettled Quantities) ---
        
        # 1. Grid Import: Remaining Buy Orders
        # [cite_start]Buyers must buy remainder from Grid at ToU (import price) [cite: 34]
        for b_idx in range(buyer_idx, len(buy_orders)):
            b_agent = buy_orders[b_idx][0]
            rem_qty = buy_orders[b_idx][1]
            if rem_qty > 0:
                cost = rem_qty * tou
                period_financials[b_agent] -= cost

        # 2. Grid Export: Remaining Sell Orders
        # [cite_start]Sellers must sell remainder to Grid at FiT (export price) [cite: 31]
        for s_idx in range(seller_idx, len(sell_orders)):
            s_agent = sell_orders[s_idx][0]
            rem_qty = sell_orders[s_idx][1]
            if rem_qty > 0:
                revenue = rem_qty * fit
                period_financials[s_agent] += revenue

        # Store calculated profit/cost into the results DataFrame
        for agent in agent_ids:
            financial_results.at[index, agent] = period_financials[agent]

    # 5. Output Generation
    try:
        financial_results.to_csv(output_file_path, index=False)
        print(f"Simulation complete. Results saved to: {output_file_path}")
    except Exception as e:
        print(f"Error saving output file: {e}")

# --- Example Usage Block ---
if __name__ == "__main__":
    # Define file names
    input_file = 'prosumer_data.xlsx'
    output_file = 'p2p_market_results.csv'

    # Run the market simulation
    run_double_auction(input_file, output_file)
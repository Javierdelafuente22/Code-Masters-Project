"""
Builds the case-study orderbook.csv from the raw price, demand and supply files.
Each of the 10 users is given a role and a net-load profile is worked out for it.
"""
import pandas as pd
import numpy as np
import holidays

# The role for each of the 10 user IDs (Prosumer, Buyer or Seller)
ROLES = [
    "Prosumer", # ID 1 (the user)
    "Prosumer", # ID 2
    "Prosumer", # ID 3
    "Prosumer", # ID 4
    "Prosumer", # ID 5
    "Buyer",    # ID 6
    "Buyer",    # ID 7
    "Seller",   # ID 8
    "Seller",   # ID 9
    "Seller"    # ID 10 (the wind farm)
]

def generate_case_study():
    print("--- Starting Case Study Generator ---")

    if len(ROLES) != 10:
        raise ValueError(f"Configuration Error: You defined {len(ROLES)} roles, but there should be 10.")

    print("Loading raw files...")
    # dayfirst=True so DD/MM/YYYY dates are read correctly
    df_prices = pd.read_csv('prices.csv')
    df_demand = pd.read_csv('demand.csv')
    df_supply = pd.read_csv('supply.csv')

    # Use the timestamp column from the prices file as the master timeline
    timestamp_col = pd.to_datetime(df_prices.iloc[:, 0], dayfirst=True)

    print("Generating Time Features...")

    # Time of day as sin/cos so the model sees it as a repeating cycle
    hours_float = timestamp_col.dt.hour + timestamp_col.dt.minute / 60.0
    day_sin = np.sin(2 * np.pi * hours_float / 24.0).round(4)
    day_cos = np.cos(2 * np.pi * hours_float / 24.0).round(4)

    # Time of year as sin/cos
    year_sin = np.sin(2 * np.pi * timestamp_col.dt.dayofyear / 365.25).round(4)
    year_cos = np.cos(2 * np.pi * timestamp_col.dt.dayofyear / 365.25).round(4)

    # 1 on UK working days, 0 on weekends and holidays
    uk_holidays = holidays.UK()
    is_weekend = timestamp_col.dt.dayofweek >= 5
    is_holiday = timestamp_col.dt.date.apply(lambda d: d in uk_holidays)
    is_working_day = (~is_weekend & ~is_holiday).astype(int)

    df_features = pd.DataFrame({
        'time_year_sin': year_sin,
        'time_year_cos': year_cos,
        'time_day_sin': day_sin,
        'time_day_cos': day_cos,
        'is_working_day': is_working_day
    })

    print("Calculating profiles based on roles...")

    profile_data = {}

    # Column i+1 in the raw files, since column 0 is the timestamp
    for i, role in enumerate(ROLES):
        demand_vals = df_demand.iloc[:, i+1].values
        supply_vals = df_supply.iloc[:, i+1].values

        column_name = f"{i+1}_{role}"

        if role == "Prosumer":
            profile_data[column_name] = demand_vals - supply_vals

        elif role == "Buyer":
            profile_data[column_name] = demand_vals

        elif role == "Seller":
            profile_data[column_name] = -supply_vals

        else:
            raise ValueError(f"Unknown role: {role}")

    df_profiles = pd.DataFrame(profile_data)

    # Net load of the rest of the community, excluding the user (agent 1)
    print("Calculating Community Net Load (excluding Agent 1)...")
    raw_community_net = df_profiles.iloc[:, 1:].sum(axis=1)
    # Scale to [-1, 1] while keeping 0 as the balance point
    max_abs_net = raw_community_net.abs().max()
    norm_community_net = (raw_community_net / max_abs_net).round(4)

    print("Assembling final file...")

    # Import price, export price and spread
    df_price_data = df_prices.iloc[:, 1:4]

    final_df = pd.concat([
        timestamp_col.rename("timestamp"),
        df_features,
        df_price_data,
        norm_community_net.rename('net_community'),
        df_profiles
    ], axis=1)

    output_filename = 'orderbook.csv'
    final_df.to_csv(output_filename, index=False)
    
    print("SUCCESS!")
    print(f"File saved as: {output_filename}")
    print("\nGenerated Columns:")
    print(list(final_df.columns))

if __name__ == "__main__":
    generate_case_study()
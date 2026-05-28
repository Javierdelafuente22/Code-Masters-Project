import pandas as pd
import numpy as np
import os
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# demand.csv covers two years. We always run the mask against 2024 data and
# always re-target the plot's x-axis to the current calendar year, regardless
# of what year the LLM picked. This makes the demo behave consistently whether
# the LLM emits 2024 dates (the prompt's default) or 2026 dates ("next week").
DATA_START_YEAR = 2024
DATA_END_YEAR = 2025
DISPLAY_YEAR = datetime.now().year  # auto-advances year-on-year


def _mask_shift_years(start_date):
    """How many years to add to LLM dates so start_date lands in DATA_START_YEAR.
    Negative when the LLM picked a future year, positive when in the past."""
    return DATA_START_YEAR - start_date.year


def _shift_years(dt, years):
    """Leap-day-safe year shift via DateOffset."""
    return dt if years == 0 else (dt + pd.DateOffset(years=years))


def apply_lifestyle_update(json_payload, input_csv="data/demand.csv", output_csv="data/chatbot/updated_demand.csv"):
    """
    Takes the JSON from the chatbot, applies the modifications to the demand time-series,
    and outputs a detailed CSV for verification and graphing.
    """
    print("\nApplying coefficients...")
    
    # 1. Load the data
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"Error: Could not find {input_csv}.")
        return None

    # 2. Parse the timestamp securely 
    # (dayfirst=True ensures DD/MM/YYYY is read correctly, even if hours are missing)
    df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=True)

    # 3. Setup our Output/Audit columns
    df['pre_demand'] = df['User']
    df['post_demand'] = df['User'].copy()
    df['multiplier_applied'] = np.nan # NaN by default (easier to read in CSV)
    df['is_masked'] = False           # Will turn True only for rows we modify

    # 4. Extract logic from the JSON payload
    mod_type = json_payload['modification']['type']
    mod_value = float(json_payload['modification']['value'])
    
    timing = json_payload['timing']
    days_of_week = timing['days_of_week']
    start_hour = int(timing['start_hour'])
    end_hour = int(timing['end_hour'])
    
    start_date = pd.to_datetime(timing['start_date'], dayfirst=True)
    end_date = pd.to_datetime(timing['end_date'], dayfirst=True)

    # Always pin start_date to DATA_START_YEAR for masking, and always
    # re-target the display to DISPLAY_YEAR. This way the plot's x-axis shows
    # the current year regardless of what year the LLM emits.
    mask_shift = _mask_shift_years(start_date)
    if mask_shift != 0:
        print(f"  Shifting LLM dates by {mask_shift:+d}y so start lands in {DATA_START_YEAR}")
        start_date = _shift_years(start_date, mask_shift)
        end_date = _shift_years(end_date, mask_shift)
    json_payload['year_shift'] = DISPLAY_YEAR - DATA_START_YEAR

    # Push the end_date to 23:59 so it includes the entirety of the final day
    if end_date.hour == 0 and end_date.minute == 0:
         end_date = end_date.replace(hour=23, minute=59)

    # 5. Create the Masks (The Filters)
    date_mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
    day_mask = df['timestamp'].dt.dayofweek.isin(days_of_week)
    hour_mask = (df['timestamp'].dt.hour >= start_hour) & (df['timestamp'].dt.hour <= end_hour)

    # Combine all 3 filters together
    final_mask = date_mask & day_mask & hour_mask
    
    # Tag the rows that made it through the filter
    df.loc[final_mask, 'is_masked'] = True

    # 6. Apply the Modification Math
    if mod_type == 'scale':
        df.loc[final_mask, 'post_demand'] = df.loc[final_mask, 'pre_demand'] * mod_value
        df.loc[final_mask, 'multiplier_applied'] = mod_value
        
        # Apply your constraint: Clip the new values so nothing exceeds 1.0
        df['post_demand'] = df['post_demand'].clip(upper=1.0)
        
    elif mod_type == 'fixed':
        df.loc[final_mask, 'post_demand'] = mod_value
        df.loc[final_mask, 'multiplier_applied'] = mod_value # Tracking the fixed value applied

    # 7. Format the Final DataFrame for the Output CSV
    # Re-order columns to exactly what you requested
    output_df = df[['timestamp', 'pre_demand', 'post_demand', 'multiplier_applied', 'is_masked']]
    
    # Create the output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    
    # Save the CSV
    output_df.to_csv(output_csv, index=False)
    print(f"Success! Modified data saved to: {output_csv}")
    
    return output_df

def get_plot_window(df, json_payload):
    """
    Intelligently slices the dataframe to find the best window to plot.
    """
    if not df['is_masked'].any():
        return df.head(100)

    category = json_payload.get('category', '')
    first_masked_time = df[df['is_masked']]['timestamp'].iloc[0]

    if category == 'Vacation':
        # Re-apply the same mask-shift apply_lifestyle_update used, so the
        # window slice lands on the actual CSV rows (not the display year).
        raw_start = pd.to_datetime(json_payload['timing']['start_date'], dayfirst=True)
        raw_end = pd.to_datetime(json_payload['timing']['end_date'], dayfirst=True)
        shift = _mask_shift_years(raw_start)
        start_date = _shift_years(raw_start, shift)
        end_date = _shift_years(raw_end, shift)

        plot_start = start_date.normalize() - pd.Timedelta(days=1)
        plot_end = end_date.normalize() + pd.Timedelta(days=2)
        
    else:
        # EV and Worker show a Mon-Fri week containing the first masked row
        data_monday = first_masked_time.normalize() - pd.Timedelta(days=first_masked_time.dayofweek)
        plot_start = data_monday
        plot_end = data_monday + pd.Timedelta(days=4, hours=23)  # Friday 23:00

    plot_df = df[(df['timestamp'] >= plot_start) & (df['timestamp'] <= plot_end)].copy()
    return plot_df


def plot_demand_comparison(plot_df, category, year_shift=0):
    """
    Creates a clean, readable line chart comparing pre and post demand.
    `year_shift` is added back to the x-axis timestamps so the chart shows the
    user's originally-requested year, even though the masking used analogue
    rows from inside the dataset.
    """
    print("Generating comparison graph...")

    plot_df = plot_df.copy()

    if category == 'Vacation':
        # Vacation: shift by year_shift years (dates come from the LLM payload)
        if year_shift > 0:
            plot_df['timestamp'] = plot_df['timestamp'] + pd.DateOffset(years=year_shift)
    else:
        # Worker / EV: re-anchor the data's Monday to this week's Monday so the
        # x-axis reads as "today's week", even though the underlying values come
        # from the 2024/2025 dataset.
        data_monday = plot_df['timestamp'].iloc[0].normalize()
        data_monday = data_monday - pd.Timedelta(days=data_monday.dayofweek)
        today = pd.Timestamp.now().normalize()
        next_monday = today + pd.Timedelta(days=(7 - today.dayofweek) % 7 or 7)
        plot_df['timestamp'] = plot_df['timestamp'] + (next_monday - data_monday)

    plt.figure(figsize=(10, 5))

    # 1. Plot the continuous New Demand (Wong blue) everywhere
    plt.plot(plot_df['timestamp'], plot_df['post_demand'],
             label='Updated Demand', color='#0072B2', linewidth=2)

    # Create the "Visual Bridge" mask
    visual_mask = (
        plot_df['is_masked'] |
        plot_df['is_masked'].shift(1).fillna(False) |
        plot_df['is_masked'].shift(-1).fillna(False)
    )

    # 2. Mask the Original Demand so it ONLY shows during the change
    original_masked = plot_df['pre_demand'].where(visual_mask, np.nan)

    # Plot Original Demand "Ghost" Line (Grey, Dashed)
    plt.plot(plot_df['timestamp'], original_masked,
             label='Original Demand', color="#9E9E9E", linewidth=2, linestyle='--')

    # 3. Highlight the area between the lines to show the delta
    plt.fill_between(plot_df['timestamp'],
                     plot_df['pre_demand'],
                     plot_df['post_demand'],
                     where=visual_mask,
                     interpolate=True,
                     color='#E69F00', alpha=0.3, label='Net Change')

    plt.title(f"User Electricity Consumption: {category} Profile", fontsize=14)

    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
    plt.xlabel("Date (DD/MM/YYYY)", fontsize=12)
    plt.xticks(rotation=30)
    plt.ylabel("User demand (kWh, normalised)", fontsize=12)
    plt.ylim(0, 1.05)
    plt.grid(True, alpha=0.3)
    
    # Legend in the top right corner
    plt.legend(loc='upper right')
    
    plt.tight_layout()
    
    plt.show()

# --- Local Testing Block ---
if __name__ == "__main__":
    # If you run this file directly, it tests the logic with our mock JSON
    mock_payload = {
        "mission_check": "Scale demand for Working from Home?",
        "category": "Worker",
        "modification": {"type": "scale", "value": 1.2},
        "timing": {
            "repeat": "weekly",
            "days_of_week": [4], # Friday
            "start_date": "01/01/2024",
            "end_date": "31/12/2025",
            "start_hour": 9,
            "end_hour": 17
        }
    }
    
    # Ensure you have a 'data/demand.csv' file before running this!
    test_run = apply_lifestyle_update(mock_payload)
                # 1. Update the Data
    modified_df = apply_lifestyle_update(json_payload=mock_payload)
            
    if modified_df is not None:
                # 2. Extract the specific window to plot
        plot_data = get_plot_window(modified_df, mock_payload)
                
                # 3. Show the graph!
        plot_demand_comparison(plot_data, mock_payload['category'])
    
    if test_run is not None:
        print("\nPreview of modified rows:")
        print(test_run[test_run['is_masked'] == True].head())